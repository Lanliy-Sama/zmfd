//! 与 agentmore.chatglm.cn 的实际网络交互：签名头、文件上传、对话流、会话清理、SSE 解析。
//!
//! 端点（均相对上游基址，官方前端同源调用）：
//!   POST /chatglm/user-api/guest/access                    游客令牌
//!   POST /chatglm/user-api/user/refresh                    刷新访问令牌
//!   POST /chatglm/productivity-api/file/chat_upload        聊天附件上传（multipart）
//!   POST /chatglm/agent-api/chat/stream                    对话模式流式端点
//!   DELETE /chatglm/agent-api/conversation/delete          会话删除（best-effort）

use crate::config::UpstreamConfig;
use crate::error::{map_upstream_status, GatewayError};
use crate::state::AppState;
use crate::util::{deep_find_str, deep_find_u64};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct UploadedFile {
    pub file_id: String,
    pub file_name: String,
    pub file_type: String,
    pub file_url: String,
    pub file_size: u64,
}

/// 构造上游请求头（官方 J0() 形态 + 浏览器指纹）。
/// referer_suffix 为 None 时不发 Referer；json_content=false 时不设置 Content-Type（multipart 由 reqwest 自带）。
pub fn upstream_headers(
    secret: &str,
    device_id: &str,
    origin: &str,
    referer_suffix: Option<&str>,
    accept_stream: bool,
    json_content: bool,
    auth: Option<&str>,
) -> HeaderMap {
    let mut map = HeaderMap::new();
    let insert = |map: &mut HeaderMap, name: &'static str, value: String| {
        if let (Ok(n), Ok(v)) = (name.parse::<HeaderName>(), HeaderValue::from_str(&value)) {
            map.insert(n, v);
        }
    };

    insert(&mut map, "Accept", if accept_stream {
        "text/event-stream".to_string()
    } else {
        "application/json, text/plain, */*".to_string()
    });
    insert(&mut map, "Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8".to_string());
    insert(&mut map, "Origin", origin.to_string());
    if let Some(suffix) = referer_suffix {
        insert(&mut map, "Referer", format!("{origin}{suffix}"));
    }
    if json_content {
        insert(&mut map, "Content-Type", "application/json;charset=utf-8".to_string());
    }
    if let Some(token) = auth {
        insert(&mut map, "Authorization", format!("Bearer {token}"));
    }
    for (k, v) in crate::sign::sign_headers(secret, device_id) {
        if let (Ok(n), Ok(v)) = (HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(&v)) {
            map.insert(n, v);
        }
    }
    map
}

fn headers_for(state: &AppState, accept_stream: bool, json_content: bool, auth: Option<&str>) -> HeaderMap {
    let cfg = state.config();
    let origin = cfg.upstream.base_url.clone();
    let secret = if cfg.upstream.sign_secret.trim().is_empty() {
        crate::sign::FALLBACK_SIGN_SECRET.to_string()
    } else {
        cfg.upstream.sign_secret.clone()
    };
    let device_id = if cfg.device_id.trim().is_empty() {
        "zmfd-gateway".to_string()
    } else {
        cfg.device_id.clone()
    };
    upstream_headers(
        &secret,
        &device_id,
        &origin,
        Some("/"),
        accept_stream,
        json_content,
        auth,
    )
}

/// 上游业务响应校验：status 字段若存在，必须是 0 / 200（数字或字符串形态）。
fn check_business_ok(parsed: &Value, what: &str) -> Result<(), GatewayError> {
    if let Some(status) = parsed.get("status") {
        let ok = status
            .as_i64()
            .map(|s| s == 0 || s == 200)
            .or_else(|| {
                status
                    .as_str()
                    .map(|s| s == "0" || s == "200" || s == "success")
            })
            .unwrap_or(false);
        if !ok {
            let msg = parsed
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("（无错误消息）");
            return Err(GatewayError::upstream(format!("{what}业务失败: {msg}")));
        }
    }
    Ok(())
}

/// 游客令牌：免账号获取 access_token（官方前端登录前即调用此接口）。
pub async fn guest_access(state: &AppState) -> Result<(String, String), GatewayError> {
    let cfg = state.config();
    let base = cfg.upstream.base_url.clone();
    let url = format!("{base}/chatglm/user-api/guest/access");
    let headers = headers_for(state, false, true, None);

    let client = state.http();
    let resp = client
        .post(&url)
        .headers(headers)
        .json(&json!({}))
        .send()
        .await
        .map_err(|e| GatewayError::upstream(format!("游客令牌请求失败: {e}")))?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    if status != 200 {
        return Err(map_upstream_status(status, &body));
    }
    let parsed: Value = serde_json::from_str(&body)
        .map_err(|_| GatewayError::upstream("游客令牌返回非 JSON"))?;
    check_business_ok(&parsed, "游客令牌")?;
    let access = deep_find_str(&parsed, "access_token")
        .ok_or_else(|| GatewayError::upstream("游客令牌响应缺少 access_token"))?;
    let refresh = deep_find_str(&parsed, "refresh_token").unwrap_or_default();
    Ok((access, refresh))
}

/// 上传上下文文件（multipart），返回可直接放入消息 content 的文件块信息。
pub async fn upload_context_file(
    state: &AppState,
    token: &str,
    file_name: &str,
    bytes: Vec<u8>,
) -> Result<UploadedFile, GatewayError> {
    let cfg = state.config();
    let base = cfg.upstream.base_url.clone();
    let url = format!("{base}/chatglm/productivity-api/file/chat_upload");
    let headers = headers_for(state, false, false, Some(token));

    let part = reqwest::multipart::Part::bytes(bytes.clone())
        .file_name(file_name.to_string())
        .mime_str("text/markdown")
        .map_err(|e| GatewayError::new(500, "internal_error", format!("构造上传体失败: {e}")))?;
    let form = reqwest::multipart::Form::new()
        .text("from", cfg.upstream.upload_from.clone())
        .text("assistant_id", cfg.upstream.assistant_id.clone())
        .part("file", part);

    let client = state.http();
    let resp = client
        .post(&url)
        .headers(headers)
        .multipart(form)
        .send()
        .await
        .map_err(|e| GatewayError::upstream(format!("上传上下文文件失败: {e}")))?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    if status != 200 {
        return Err(map_upstream_status(status, &body));
    }
    let parsed: Value = serde_json::from_str(&body)
        .map_err(|_| GatewayError::upstream("上传响应非 JSON"))?;
    check_business_ok(&parsed, "上传")?;

    let file_id = deep_find_str(&parsed, "file_id")
        .ok_or_else(|| GatewayError::upstream("上传响应缺少 file_id"))?;
    let file_url = deep_find_str(&parsed, "url")
        .or_else(|| deep_find_str(&parsed, "file_url"))
        .unwrap_or_default();
    let resp_name = deep_find_str(&parsed, "file_name").unwrap_or_else(|| file_name.to_string());
    let file_type = deep_find_str(&parsed, "file_type").unwrap_or_else(|| "md".to_string());
    let file_size = deep_find_u64(&parsed, "file_size").unwrap_or(bytes.len() as u64);

    Ok(UploadedFile {
        file_id,
        file_name: resp_name,
        file_type,
        file_url,
        file_size,
    })
}

/// 构造对话模式流式请求体（与官方 ChatList 的 Che 函数构造一致的字段集）。
/// files 为按阅读顺序排列的附件（多文件合入同一个 file 块数组，order 递增）。
pub fn build_chat_body(
    cfg: &UpstreamConfig,
    instruction: &str,
    files: &[UploadedFile],
    effort: &str,
    selected_model: &str,
) -> Value {
    let mut content = vec![json!({"type": "text", "text": instruction})];
    if !files.is_empty() {
        // 附件块形态实测（官方前端 Che 函数）：file 是数组，元素带
        // url / cover_images / order / width / height 字段，缺任何一个都会
        // 被上游以 {"status":400,"message":"invalid param"} 拒绝。
        let items: Vec<Value> = files
            .iter()
            .enumerate()
            .map(|(i, f)| {
                json!({
                    "file_id": f.file_id,
                    "file_name": f.file_name,
                    "file_url": f.file_url,
                    "file_size": f.file_size,
                    "url": f.file_url,
                    "cover_images": Value::Null,
                    "order": i,
                    "width": 0,
                    "height": 0,
                })
            })
            .collect();
        content.push(json!({"type": "file", "file": items}));
    }
    json!({
        "assistant_id": cfg.assistant_id,
        "conversation_id": "",
        "chat_type": "user_chat",
        "meta_data": {
            "cogview": {"rm_label_watermark": true},
            "is_test": false,
            "input_question_type": "xxxx",
            "channel": "",
            "chat_mode": cfg.chat_mode,
            "reasoning_effort": effort,
            "selected_model": selected_model,
            "is_networking": cfg.is_networking,
            "platform": "pc",
        },
        "messages": [
            {"role": "user", "content": content}
        ],
    })
}

/// 发起对话流请求，返回原始响应（SSE 字节流）。
pub async fn chat_stream(
    state: &AppState,
    token: &str,
    body: Value,
) -> Result<reqwest::Response, GatewayError> {
    let cfg = state.config();
    let base = cfg.upstream.base_url.clone();
    let url = format!("{base}/chatglm/agent-api/chat/stream");
    let headers = headers_for(state, true, true, Some(token));

    let client = state.http();
    let resp = client
        .post(&url)
        .headers(headers)
        .json(&body)
        .send()
        .await
        .map_err(|e| GatewayError::upstream(format!("上游网络连接失败: {e}")))?;
    let status = resp.status().as_u16();
    if status != 200 {
        let text = resp.text().await.unwrap_or_default();
        return Err(map_upstream_status(status, &text));
    }
    Ok(resp)
}

/// 删除上游会话（best-effort，失败不影响主流程）。
pub async fn delete_conversation(state: &AppState, token: &str, conversation_id: &str) {
    if conversation_id.trim().is_empty() {
        return;
    }
    let cfg = state.config();
    let base = cfg.upstream.base_url.clone();
    let url = format!("{base}/chatglm/agent-api/conversation/delete");
    let headers = headers_for(state, false, true, Some(token));
    let client = state.http();
    let result = client
        .delete(&url)
        .headers(headers)
        .query(&[("conversation_ids", conversation_id)])
        .timeout(Duration::from_secs(30))
        .send()
        .await;
    if let Err(e) = result {
        eprintln!("[zmfd] 清理上游会话 {conversation_id} 失败（忽略）: {e}");
    }
}

// ---------------------------------------------------------------------------
// SSE 解码与上游事件解析
// ---------------------------------------------------------------------------

/// 把 SSE 字节流切成完整行（跨 chunk 的多字节字符安全：只在完整行上解码）。
pub struct SseDecoder {
    buf: Vec<u8>,
}

impl SseDecoder {
    pub fn new() -> Self {
        SseDecoder { buf: Vec::new() }
    }

    pub fn push(&mut self, data: &[u8]) -> Vec<String> {
        self.buf.extend_from_slice(data);
        let mut lines = Vec::new();
        while let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
            let raw: Vec<u8> = self.buf.drain(..=pos).collect();
            let mut end = raw.len() - 1; // 去掉 \n
            if end > 0 && raw[end - 1] == b'\r' {
                end -= 1;
            }
            lines.push(String::from_utf8_lossy(&raw[..end]).to_string());
        }
        lines
    }

    pub fn finish(&mut self) -> Vec<String> {
        if self.buf.is_empty() {
            return Vec::new();
        }
        let rest = std::mem::take(&mut self.buf);
        vec![String::from_utf8_lossy(&rest).to_string()]
    }
}

/// 上游一行 SSE 解析出的增量事件。
#[derive(Debug, Default, Clone)]
pub struct UpParsed {
    pub conversation_id: Option<String>,
    pub history_id: Option<String>,
    pub status: String,
    /// 正文增量。
    pub text: String,
    /// 思考链增量。
    pub reasoning: String,
    pub error_text: String,
    pub error_code: Option<i64>,
    /// 收到 data: [DONE] 终止标记。
    pub done: bool,
}

impl UpParsed {
    fn empty() -> Self {
        UpParsed::default()
    }
}

/// 上游 SSE 流解析器：处理「增量帧 + 累积快照帧」混合推送，产出干净的增量。
///
/// 帧语义（完整帧捕获确认，确定性规则）：
///   part.status == "init"  => 帧内容为增量（追加）；
///   part.status == "finish" => 帧内容为该 logic_id 的累积全量（替换）；
///   帧 meta_data 的 if_increase_push / full 显式标志优先于 part.status。
pub struct UpstreamParser {
    order: Vec<String>,
    text: HashMap<String, String>,
    reasoning: HashMap<String, String>,
    sent_text: HashMap<String, usize>,
    sent_reasoning: HashMap<String, usize>,
}

impl UpstreamParser {
    pub fn new() -> Self {
        UpstreamParser {
            order: Vec::new(),
            text: HashMap::new(),
            reasoning: HashMap::new(),
            sent_text: HashMap::new(),
            sent_reasoning: HashMap::new(),
        }
    }

    pub fn parse_line(&mut self, line: &str) -> Option<UpParsed> {
        let line = line.trim();
        if line.is_empty() {
            return None;
        }
        // 接受三种形态：SSE "data:" 帧、HTTP 200 内裸 JSON 对象行、
        // 以及数组形态错误体（实测附件被拒时为 [{"status":400,...}]，取首个对象元素）。
        let payload = if let Some(rest) = line.strip_prefix("data:") {
            rest.trim()
        } else if line.starts_with('{') || line.starts_with('[') {
            line
        } else {
            return None;
        };
        if payload.is_empty() {
            return None;
        }
        if payload == "[DONE]" {
            let mut ev = UpParsed::empty();
            ev.done = true;
            return Some(ev);
        }
        let obj: Value = {
            let parsed: Value = match serde_json::from_str(payload) {
                Ok(v) => v,
                Err(_) => return None,
            };
            match parsed {
                Value::Array(items) => match items.into_iter().find(|v| v.is_object()) {
                    Some(v) => v,
                    None => return None,
                },
                other => other,
            }
        };

        let mut ev = UpParsed::empty();

        // 会话 / 消息 ID。
        for key in ["conversation_id", "conv_id"] {
            if let Some(v) = obj.get(key).and_then(Value::as_str) {
                if !v.is_empty() {
                    ev.conversation_id = Some(v.to_string());
                    break;
                }
            }
        }
        if ev.conversation_id.is_none() {
            if let Some(v) = obj
                .get("meta_data")
                .and_then(|m| m.get("conversation_id"))
                .and_then(Value::as_str)
            {
                if !v.is_empty() {
                    ev.conversation_id = Some(v.to_string());
                }
            }
        }
        if let Some(v) = obj.get("id").and_then(Value::as_str) {
            if !v.is_empty() {
                ev.history_id = Some(v.to_string());
            }
        }

        // 状态：字符串形态（finish/init/intervene/success…）或数字形态
        // （如 -1001 积分不足、400 invalid param）。数字判定对两种形态都生效：
        // 实测上游会用 HTTP 200 + 裸 JSON 帧承载业务错误，非 0/200 的数值状态都是错误帧。
        let status_val = obj.get("status");
        ev.status = status_val
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let mut numeric_status = status_val.and_then(Value::as_i64);
        if numeric_status.is_none() && !ev.status.is_empty() {
            numeric_status = ev.status.parse::<i64>().ok();
        }
        if let Some(n) = numeric_status {
            if ev.status.is_empty() {
                ev.status = n.to_string();
            }
            if n != 0 && n != 200 {
                if let Some(t) = str_field(&obj, "message") {
                    ev.error_text = t;
                }
                ev.error_code = Some(n);
            }
        }

        // 增量/快照判定：帧 meta_data 的显式标志优先；
        // 缺省时按 part.status 判定（finish=快照替换，init=增量追加）。
        let meta = obj.get("meta_data");
        let increase_mode: Option<bool> = match meta.and_then(|m| m.get("if_increase_push")) {
            Some(Value::Bool(b)) => Some(*b),
            _ => match meta.and_then(|m| m.get("full")) {
                Some(v) => Some(!json_truthy(v)),
                None => None,
            },
        };

        // 本帧内容按 logic_id 归组（保持出现顺序，并记录 part.status）。
        // 实测语义（完整帧捕获确认）：part.status=="init" 的帧内容是增量（追加）；
        // part.status=="finish" 的帧内容是该 logic_id 的累积全量快照（替换）。
        let mut frame_text: Vec<(String, String, bool)> = Vec::new();
        let mut frame_reasoning: Vec<(String, String, bool)> = Vec::new();
        if let Some(parts) = obj.get("parts").and_then(Value::as_array) {
            for part in parts {
                let logic_id = part
                    .get("logic_id")
                    .and_then(Value::as_str)
                    .or_else(|| part.get("id").and_then(Value::as_str))
                    .unwrap_or("default")
                    .to_string();
                let is_snapshot = part.get("status").and_then(Value::as_str) == Some("finish");
                let Some(blocks) = part.get("content").and_then(Value::as_array) else {
                    continue;
                };
                for block in blocks {
                    let btype = block.get("type").and_then(Value::as_str).unwrap_or("");
                    match btype {
                        "text" => {
                            let t = str_field(block, "text").or_else(|| str_field(block, "content"));
                            if let Some(t) = t {
                                frame_append(&mut frame_text, &logic_id, &t, is_snapshot);
                            }
                        }
                        "think" => {
                            let t = str_field(block, "think").or_else(|| str_field(block, "content"));
                            if let Some(t) = t {
                                frame_append(&mut frame_reasoning, &logic_id, &t, is_snapshot);
                            }
                        }
                        "execution_output" => {
                            if let Some(t) = str_field(block, "content") {
                                frame_append(&mut frame_text, &logic_id, &t, is_snapshot);
                            }
                        }
                        "code" => {
                            if let Some(t) = str_field(block, "code") {
                                let fenced = format!("```python\n{t}\n```");
                                frame_append(&mut frame_text, &logic_id, &fenced, is_snapshot);
                            }
                        }
                        // tool_calls / tool_result / image / file / dir 等平台内部块不消费。
                        _ => {}
                    }
                }
            }
        }

        // 帧内容并入全量状态：显式 meta 标志优先；否则按 part.status 判定
        // （finish=累积快照替换，init=增量追加）。
        for (id, val, part_snapshot) in &frame_text {
            if !self.order.contains(id) {
                self.order.push(id.clone());
            }
            let cur = self.text.get(id).cloned().unwrap_or_default();
            let merged = match increase_mode {
                Some(true) => format!("{cur}{val}"),
                Some(false) => val.clone(),
                None => {
                    if *part_snapshot {
                        val.clone()
                    } else {
                        format!("{cur}{val}")
                    }
                }
            };
            self.text.insert(id.clone(), merged);
        }
        for (id, val, part_snapshot) in &frame_reasoning {
            if !self.order.contains(id) {
                self.order.push(id.clone());
            }
            let cur = self.reasoning.get(id).cloned().unwrap_or_default();
            let merged = match increase_mode {
                Some(true) => format!("{cur}{val}"),
                Some(false) => val.clone(),
                None => {
                    if *part_snapshot {
                        val.clone()
                    } else {
                        format!("{cur}{val}")
                    }
                }
            };
            self.reasoning.insert(id.clone(), merged);
        }

        // 差分出增量（按字符，与官方前端一致）。
        // 快照收缩（模型重写/纠错使累积内容变短）时对齐已发送计数，
        // 否则后续增长在追上旧计数之前会被整体吞掉。
        for id in &self.order {
            if let Some(full) = self.text.get(id) {
                let total = full.chars().count();
                let prev = self.sent_text.get(id).copied().unwrap_or(0);
                if total > prev {
                    let delta: String = full.chars().skip(prev).collect();
                    self.sent_text.insert(id.clone(), total);
                    ev.text.push_str(&delta);
                } else if total < prev {
                    self.sent_text.insert(id.clone(), total);
                }
            }
            if let Some(full) = self.reasoning.get(id) {
                let total = full.chars().count();
                let prev = self.sent_reasoning.get(id).copied().unwrap_or(0);
                if total > prev {
                    let delta: String = full.chars().skip(prev).collect();
                    self.sent_reasoning.insert(id.clone(), total);
                    ev.reasoning.push_str(&delta);
                } else if total < prev {
                    self.sent_reasoning.insert(id.clone(), total);
                }
            }
        }

        // 错误信息。
        if let Some(le) = obj.get("last_error") {
            ev.error_text = str_field(le, "intervene_text")
                .or_else(|| str_field(le, "message"))
                .unwrap_or_default();
            ev.error_code = le.get("error_code").and_then(Value::as_i64);
        }
        if ev.error_text.is_empty() {
            if let Some(parts) = obj.get("parts").and_then(Value::as_array) {
                for part in parts {
                    if part.get("status").and_then(Value::as_str) == Some("intervene") {
                        if let Some(err) = part.get("error") {
                            if let Some(t) = str_field(err, "intervene_text") {
                                ev.error_text = t;
                                break;
                            }
                        }
                    }
                }
            }
        }

        Some(ev)
    }
}

fn str_field(v: &Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

fn frame_append(
    target: &mut Vec<(String, String, bool)>,
    id: &str,
    val: &str,
    is_snapshot: bool,
) {
    if val.is_empty() {
        return;
    }
    for entry in target.iter_mut() {
        if entry.0 == id {
            entry.1.push_str(val);
            return;
        }
    }
    target.push((id.to_string(), val.to_string(), is_snapshot));
}

/// 与 JS 布尔真值语义一致（用于 meta_data.full）。
fn json_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// 结束状态判定：(是否结束, finish_reason)。None 表示尚未结束。
/// 数值状态码（如 -1001 积分不足、400 invalid param，除 0/200 外）按错误结束处理。
pub fn status_reason(status: &str) -> Option<(bool, &'static str)> {
    match status {
        "finish" | "stop" => Some((true, "stop")),
        "intervene" => Some((true, "content_filter")),
        "error" => Some((true, "error")),
        _ => {
            if let Ok(n) = status.parse::<i64>() {
                if n != 0 && n != 200 {
                    return Some((true, "error"));
                }
            }
            None
        }
    }
}
