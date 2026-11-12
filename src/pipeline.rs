//! 请求管线：规范请求 -> 上传双附件（上下文+工具定义）-> 新建上游会话（conversation_id 留空）
//! -> 消费 SSE -> 工具块拆分 -> 结构化事件流。
//!
//! 每次调用都新建上游对话；得到回复后即结束（下次调用又是全新会话），
//! 上游并发由信号量限制（默认最高 3）。

use crate::context;
use crate::error::GatewayError;
use crate::proto::{CanonRequest, CanonToolCall};
use crate::state::AppState;
use crate::tools::{SplitPiece, ToolStreamSplitter};
use crate::upstream::{self, SseDecoder, UploadedFile, UpstreamParser};
use crate::util::{estimate_tokens, now_ms, uuid_hex};
use futures_util::StreamExt;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// 管线产物事件。协议层负责把事件渲染为各协议的 SSE / JSON。
#[derive(Debug, Clone)]
pub enum StreamItem {
    Text(String),
    Reasoning(String),
    ToolCall(CanonToolCall),
    Done(PipelineDone),
}

#[derive(Debug, Clone)]
pub struct PipelineDone {
    /// stop | tool_calls | content_filter
    pub finish_reason: String,
    pub usage_in: u32,
    pub usage_out: u32,
}

#[derive(Debug, Clone, Default)]
pub struct Collected {
    pub text: String,
    pub reasoning: String,
    pub calls: Vec<CanonToolCall>,
    pub finish_reason: String,
    pub usage_in: u32,
    pub usage_out: u32,
}

/// 运行完整管线，把事件发入 tx。函数返回即代表管线结束（Done / Err 已发出）。
pub async fn run_pipeline(
    state: Arc<AppState>,
    canon: CanonRequest,
    tx: mpsc::Sender<Result<StreamItem, GatewayError>>,
) {
    let result = pipeline_inner(&state, &canon, &tx).await;
    match result {
        Ok(done) => {
            let _ = tx.send(Ok(StreamItem::Done(done))).await;
        }
        Err(e) => {
            let _ = tx.send(Err(e)).await;
        }
    }
}

/// 非流式路径：运行管线并聚合结果。
pub async fn collect_pipeline(
    state: Arc<AppState>,
    canon: CanonRequest,
) -> Result<Collected, GatewayError> {
    let (tx, mut rx) = mpsc::channel::<Result<StreamItem, GatewayError>>(64);
    tokio::spawn(run_pipeline(state, canon, tx));
    let mut out = Collected::default();
    while let Some(item) = rx.recv().await {
        match item {
            Ok(StreamItem::Text(t)) => out.text.push_str(&t),
            Ok(StreamItem::Reasoning(t)) => out.reasoning.push_str(&t),
            Ok(StreamItem::ToolCall(c)) => out.calls.push(c),
            Ok(StreamItem::Done(d)) => {
                out.finish_reason = d.finish_reason;
                out.usage_in = d.usage_in;
                out.usage_out = d.usage_out;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

async fn pipeline_inner(
    state: &Arc<AppState>,
    canon: &CanonRequest,
    tx: &mpsc::Sender<Result<StreamItem, GatewayError>>,
) -> Result<PipelineDone, GatewayError> {
    let cfg = state.config();

    let tool_instr = crate::tools::instructions(
        &canon.tools,
        &canon.tool_choice_kind,
        &canon.tool_choice_name,
    );
    // 附件模式下的格式约定（仅格式，Schema 在工具定义附件中）。
    let tool_format_rules = if canon.tools.is_empty() {
        String::new()
    } else {
        crate::tools::format_rules(&canon.tool_choice_kind, &canon.tool_choice_name)
    };
    let tools_doc = if canon.tools.is_empty() {
        String::new()
    } else {
        crate::tools::tools_document(&canon.tools)
    };
    let doc = context::build_context_document(canon, cfg.upstream.max_context_chars);
    let selected_model = cfg.resolve_model(&canon.model);
    // 思考强度优先级：面板强制覆盖 > 客户端显式请求 > 面板默认。
    let effort = if !cfg.upstream.effort_override.is_empty() {
        cfg.upstream.effort_override.clone()
    } else if canon.effort.is_empty() {
        cfg.upstream.default_effort.clone()
    } else {
        canon.effort.clone()
    };

    // 1. 上游并发闸门（超限排队）。
    let _permit = state.acquire().await?;

    // 2. 取访问令牌（账号池轮转 / 游客回退）。
    let (token, slot, source) = state.tokens.get_access_token(state).await?;
    eprintln!("[zmfd] upstream slot={slot} source={source} model={selected_model} effort={effort}");

    // 3. 上下文投递（双附件）：附件 1 = 对话上下文，附件 2 = 工具定义（仅当有工具）。
    //    槽位被上游拒绝过附件或配置为内联时直接内联（Schema 随文本携带）。
    let can_attach = state.tokens.slot_can_attach(slot).await;
    let mut uploaded: Vec<UploadedFile> = Vec::new();
    if can_attach && cfg.upstream.context_send_mode != "inline" {
        let ts = now_ms();
        match upstream::upload_context_file(
            state,
            &token,
            &format!("context-{ts}.md"),
            doc.clone().into_bytes(),
        )
        .await
        {
            Ok(f) => {
                uploaded.push(f);
                if !tools_doc.is_empty() {
                    match upstream::upload_context_file(
                        state,
                        &token,
                        &format!("tools-{ts}.md"),
                        tools_doc.clone().into_bytes(),
                    )
                    .await
                    {
                        Ok(f) => uploaded.push(f),
                        Err(e) => {
                            eprintln!("[zmfd] 上传工具定义文件失败，Schema 改为内联: {e}");
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[zmfd] 上传上下文文件失败，改用内联模式: {e}");
            }
        }
    }

    let inline_instruction = build_instruction_inline(&tool_instr, &doc);
    let mut instruction = if uploaded.is_empty() {
        inline_instruction.clone()
    } else {
        build_instruction_with_files(&uploaded, &tool_format_rules)
    };
    let body = upstream::build_chat_body(
        &cfg.upstream,
        &instruction,
        &uploaded,
        &effort,
        &selected_model,
    );

    // 4. 发起对话流。上游对附件的拒绝有两种形态（都自动降级内联重试一次）：
    //    a) HTTP 400（chat_stream 返回 Err）
    //    b) HTTP 200 + 裸 JSON 业务错误帧（对象或数组形态，可能不带换行结尾）
    let mut used_file = !uploaded.is_empty();
    let mut resp_slot: Option<reqwest::Response> = {
        let first = upstream::chat_stream(state, &token, body).await;
        match first {
            Ok(r) => Some(r),
            Err(e) => {
                if used_file && e.status == 400 {
                    state.tokens.set_slot_attach(slot, false).await;
                    eprintln!(
                        "[zmfd] 上游拒绝消息附件（HTTP 400），槽位改用内联上下文重试: {}",
                        e.message
                    );
                    used_file = false;
                    instruction = inline_instruction.clone();
                    let retry_body = upstream::build_chat_body(
                        &cfg.upstream,
                        &instruction,
                        &[],
                        &effort,
                        &selected_model,
                    );
                    Some(upstream::chat_stream(state, &token, retry_body).await?)
                } else {
                    return Err(e);
                }
            }
        }
    };

    let idle = Duration::from_secs(cfg.upstream.stream_idle_timeout_secs);
    let deadline = Instant::now() + Duration::from_secs(cfg.upstream.total_timeout_secs);
    let debug = cfg.debug_log;

    let mut full_text = String::new();
    let mut full_reasoning = String::new();
    let mut calls: Vec<CanonToolCall> = Vec::new();
    let mut conversation_id = String::new();
    let mut finish_reason = String::from("stop");
    let mut got_finish = false;
    let mut upstream_error: Option<GatewayError> = None;
    // 流异常结束时的原始帧样本（前 3 行），随错误信息透出便于定位。
    let mut raw_sample: Vec<String> = Vec::new();

    // 5. 统一的行处理循环：先逐块读流，流结束后衔接 decoder.finish() 的残余行
    //    （上游错误体可能不带换行结尾），所有行走同一套解析 / 重试 / 样本逻辑。
    let mut parser = UpstreamParser::new();
    let mut splitter = ToolStreamSplitter::new(canon.known_tool_names());
    let mut decoder = SseDecoder::new();
    let mut stream = resp_slot.take().unwrap().bytes_stream();
    let mut drained: VecDeque<String> = VecDeque::new();
    let mut stream_done = false;

    'outer: loop {
        if Instant::now() > deadline {
            upstream_error = Some(GatewayError::timeout("上游响应超过总时长上限"));
            break 'outer;
        }
        let line: String = if !stream_done {
            match tokio::time::timeout(idle, stream.next()).await {
                Err(_) => {
                    upstream_error = Some(GatewayError::timeout("上游流空闲超时"));
                    break 'outer;
                }
                Ok(None) => {
                    // 流关闭：切换到残余行处理阶段。
                    stream_done = true;
                    continue 'outer;
                }
                Ok(Some(Err(e))) => {
                    upstream_error = Some(GatewayError::upstream(format!("上游连接中断: {e}")));
                    break 'outer;
                }
                Ok(Some(Ok(bytes))) => {
                    let mut lines = decoder.push(&bytes);
                    if lines.is_empty() {
                        continue 'outer;
                    }
                    for l in lines.drain(1..) {
                        drained.push_back(l);
                    }
                    lines.remove(0)
                }
            }
        } else {
            match drained.pop_front() {
                Some(l) => l,
                None => break 'outer,
            }
        };

        if raw_sample.len() < 3 {
            let t: String = line.chars().take(200).collect();
            raw_sample.push(t);
        }
        if debug {
            debug_line(&line);
        }
        let Some(ev) = parser.parse_line(&line) else {
            continue;
        };
        if let Some(cid) = ev.conversation_id {
            conversation_id = cid;
        }
        if !ev.reasoning.is_empty() {
            full_reasoning.push_str(&ev.reasoning);
            if tx
                .send(Ok(StreamItem::Reasoning(ev.reasoning.clone())))
                .await
                .is_err()
            {
                return Err(client_closed());
            }
        }
        if !ev.text.is_empty() {
            full_text.push_str(&ev.text);
            for piece in splitter.feed(&ev.text) {
                match piece {
                    SplitPiece::Text(t) => {
                        if !t.is_empty() && tx.send(Ok(StreamItem::Text(t))).await.is_err() {
                            return Err(client_closed());
                        }
                    }
                    SplitPiece::Tool(c) => {
                        let call = CanonToolCall {
                            id: format!("call_{}", &uuid_hex()[..12]),
                            name: c.name,
                            arguments: c.arguments_json,
                        };
                        calls.push(call.clone());
                        if tx.send(Ok(StreamItem::ToolCall(call))).await.is_err() {
                            return Err(client_closed());
                        }
                    }
                }
            }
        }
        if let Some((ended, reason)) = upstream::status_reason(&ev.status) {
            if ended {
                if reason == "error" {
                    if full_text.is_empty() && calls.is_empty() && used_file {
                        // 流内业务错误且尚未产出内容：附件被拒，降级内联重试一次。
                        state.tokens.set_slot_attach(slot, false).await;
                        eprintln!(
                            "[zmfd] 流内业务错误拒绝附件（status={}），槽位改用内联上下文重试: {}",
                            ev.status, ev.error_text
                        );
                        used_file = false;
                        instruction = inline_instruction.clone();
                        let retry_body = upstream::build_chat_body(
                            &cfg.upstream,
                            &instruction,
                            &[],
                            &effort,
                            &selected_model,
                        );
                        resp_slot = Some(upstream::chat_stream(state, &token, retry_body).await?);
                        stream = resp_slot.take().unwrap().bytes_stream();
                        parser = UpstreamParser::new();
                        splitter = ToolStreamSplitter::new(canon.known_tool_names());
                        decoder = SseDecoder::new();
                        drained.clear();
                        stream_done = false;
                        continue 'outer;
                    }
                    let code = ev.error_code.unwrap_or(0);
                    upstream_error = Some(GatewayError::upstream(format!(
                        "上游返回错误（code={code}）: {}",
                        ev.error_text
                    )));
                    got_finish = true;
                    break 'outer;
                }
                finish_reason = reason.to_string();
                got_finish = true;
                // 结束状态后上游可能还有快照帧，继续排空到流关闭或 [DONE]。
            }
        }
        if ev.done {
            got_finish = true;
            break 'outer;
        }
    }

    // 冲刷拆分器缓冲（可能有未闭合的块开头按正文交付）。
    for piece in splitter.flush() {
        match piece {
            SplitPiece::Text(t) => {
                if !t.is_empty() {
                    full_text.push_str(&t);
                    let _ = tx.send(Ok(StreamItem::Text(t))).await;
                }
            }
            SplitPiece::Tool(c) => {
                let call = CanonToolCall {
                    id: format!("call_{}", &uuid_hex()[..12]),
                    name: c.name,
                    arguments: c.arguments_json,
                };
                full_text.push_str(&call.arguments);
                calls.push(call.clone());
                let _ = tx.send(Ok(StreamItem::ToolCall(call))).await;
            }
        }
    }

    if let Some(e) = upstream_error {
        return Err(e);
    }
    if !got_finish {
        let raw = if raw_sample.is_empty() {
            String::new()
        } else {
            format!("（上游响应前几行: {}）", raw_sample.join(" | "))
        };
        return Err(GatewayError::upstream(format!(
            "上游连接结束但未收到完成标记{raw}"
        )));
    }

    let finish = if !calls.is_empty() {
        "tool_calls"
    } else if finish_reason == "content_filter" {
        "content_filter"
    } else {
        "stop"
    };

    // usage 为本地估算（上游不返回真实值）。
    let mut input_text = instruction.clone();
    input_text.push_str(&doc);
    let usage_in = estimate_tokens(&input_text);
    let mut output_text = full_text.clone();
    output_text.push_str(&full_reasoning);
    let usage_out = estimate_tokens(&output_text);

    // 6. 尽力清理上游会话。
    if cfg.upstream.delete_conversation && !conversation_id.is_empty() {
        let st = state.clone();
        let tok = token.clone();
        let cid = conversation_id.clone();
        tokio::spawn(async move {
            upstream::delete_conversation(&st, &tok, &cid).await;
        });
    }

    Ok(PipelineDone {
        finish_reason: finish.to_string(),
        usage_in,
        usage_out,
    })
}

fn client_closed() -> GatewayError {
    GatewayError::new(499, "client_closed", "客户端已断开，已中止上游读取")
}

/// 消息正文（附件模式）：工具调用格式约定 + 附件清单与阅读顺序引导。
/// files[0] = 对话上下文，files[1]（如存在）= 工具定义。
fn build_instruction_with_files(files: &[UploadedFile], tool_format_rules: &str) -> String {
    let mut s = String::new();
    if !tool_format_rules.is_empty() {
        s.push_str(tool_format_rules);
        s.push_str("\n\n");
    }
    s.push_str(&format!(
        "本次请求共附带 {} 个文件，请按以下顺序通读：\n",
        files.len()
    ));
    for (i, f) in files.iter().enumerate() {
        let desc = if i == 0 {
            "对话上下文：本次调用的完整对话历史，文末「下一步」为当前任务"
        } else {
            "可用工具定义：所有可用工具的名称、说明与入参 Schema"
        };
        s.push_str(&format!("{}. 《{}》—— {}\n", i + 1, f.file_name, desc));
    }
    s.push_str(
        "\n阅读顺序：先通读第 1 个文件明确当前任务；再查阅第 2 个文件（如存在）了解可用工具及各工具的入参 Schema；然后决定直接作答，或按上述约定输出工具调用块。\n除这些文件外没有其他上下文信息。\n",
    );
    s
}

/// 消息正文（内联模式）：工具约定（含 Schema 清单）+ 上下文全文。
fn build_instruction_inline(tool_instr: &str, doc: &str) -> String {
    let mut s = String::new();
    if !tool_instr.is_empty() {
        s.push_str(tool_instr);
        s.push_str("\n\n");
    }
    s.push_str("以下是对话上下文全文，请通读后按文末「下一步」要求直接给出回复：\n\n");
    s.push_str(doc);
    s
}

fn debug_line(line: &str) {
    let trimmed: String = line.chars().take(240).collect();
    eprintln!("[zmfd][debug] {trimmed}");
}
