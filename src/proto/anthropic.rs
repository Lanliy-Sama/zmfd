//! Anthropic Messages 协议适配（/v1/messages、/v1/messages/count_tokens）。

use crate::config::Config;
use crate::error::GatewayError;
use crate::pipeline::{self, StreamItem};
use crate::proto::*;
use crate::state::AppState;
use crate::tools::ToolDef;
use crate::util::{estimate_tokens, uuid_hex};
use axum::body::Bytes;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use bytes::Bytes as ByteBuf;
use serde_json::{json, Map, Value};
use std::io;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub fn parse(body: &Value, cfg: &Config) -> Result<CanonRequest, GatewayError> {
    let mut canon = CanonRequest::new_empty(
        body.get("model")
            .and_then(Value::as_str)
            .unwrap_or("glm-5.3"),
    );
    canon.stream = body
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    canon.effort = resolve_effort(body, &canon.model, &cfg.upstream.default_effort);

    // 顶层 system。
    match body.get("system") {
        Some(Value::String(s)) => {
            if !s.is_empty() {
                canon
                    .messages
                    .push(CanonMessage::simple(Role::System, s.clone()));
            }
        }
        Some(Value::Array(items)) => {
            let mut text = String::new();
            for item in items {
                if let Some(t) = item.get("text").and_then(Value::as_str) {
                    text.push_str(t);
                    text.push('\n');
                }
            }
            if !text.trim().is_empty() {
                canon.messages.push(CanonMessage::simple(Role::System, text));
            }
        }
        _ => {}
    }

    let messages = body
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| GatewayError::bad_request("缺少 messages 数组"))?;
    if messages.is_empty() {
        return Err(GatewayError::bad_request("messages 不能为空"));
    }
    for msg in messages {
        // 角色归一化（trim + 小写），容忍 system/developer/tool 等变体：
        // 官方协议 system 在顶层，但部分客户端会把 system 角色放进 messages。
        let role_raw = msg.get("role").and_then(Value::as_str).unwrap_or("");
        let role = role_raw.trim().to_lowercase();
        match role.as_str() {
            "user" | "system" | "developer" => {
                let canon_role = if role == "user" { Role::User } else { Role::System };
                let mut text_buf = String::new();
                match msg.get("content") {
                    Some(Value::String(s)) => text_buf.push_str(s),
                    Some(Value::Array(blocks)) => {
                        for block in blocks {
                            let btype = block.get("type").and_then(Value::as_str).unwrap_or("");
                            match btype {
                                "text" => {
                                    if let Some(t) = block.get("text").and_then(Value::as_str) {
                                        text_buf.push_str(t);
                                    }
                                }
                                "tool_result" => {
                                    // Anthropic 的工具结果位于 user 消息内容里。
                                    let mut m = CanonMessage::simple(
                                        Role::Tool,
                                        tool_result_text(block.get("content")),
                                    );
                                    m.tool_call_id = block
                                        .get("tool_use_id")
                                        .and_then(Value::as_str)
                                        .unwrap_or("")
                                        .to_string();
                                    canon.messages.push(m);
                                }
                                // image 等不支持的内容在 content_to_text 语义下报错更诚实，
                                // 但 tool_result 已单独处理，这里忽略其余类型。
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
                if !text_buf.is_empty() {
                    canon.messages.push(CanonMessage::simple(canon_role, text_buf));
                }
            }
            "assistant" => {
                let mut text_buf = String::new();
                let mut m = CanonMessage::simple(Role::Assistant, String::new());
                match msg.get("content") {
                    Some(Value::String(s)) => text_buf.push_str(s),
                    Some(Value::Array(blocks)) => {
                        for block in blocks {
                            let btype = block.get("type").and_then(Value::as_str).unwrap_or("");
                            match btype {
                                "text" => {
                                    if let Some(t) = block.get("text").and_then(Value::as_str) {
                                        text_buf.push_str(t);
                                    }
                                }
                                "tool_use" => {
                                    let id = block
                                        .get("id")
                                        .and_then(Value::as_str)
                                        .unwrap_or("")
                                        .to_string();
                                    let name = block
                                        .get("name")
                                        .and_then(Value::as_str)
                                        .unwrap_or("")
                                        .to_string();
                                    let input = block.get("input").cloned().unwrap_or(json!({}));
                                    m.tool_calls.push(CanonToolCall {
                                        id,
                                        name,
                                        arguments: serde_json::to_string(&input)
                                            .unwrap_or("{}".to_string()),
                                    });
                                }
                                "thinking" | "redacted_thinking" => {}
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
                m.content = text_buf;
                canon.messages.push(m);
            }
            "tool" | "function" => {
                // 容忍 tool 角色消息：视为工具结果文本。
                let mut m = CanonMessage::simple(
                    Role::Tool,
                    content_to_text(msg.get("content")).unwrap_or_default(),
                );
                m.tool_call_id = msg
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                canon.messages.push(m);
            }
            other => {
                return Err(GatewayError::bad_request(format!(
                    "不支持的消息角色: {other}"
                )));
            }
        }
    }

    if let Some(tools) = body.get("tools").and_then(Value::as_array) {
        for t in tools {
            let name = t.get("name").and_then(Value::as_str).unwrap_or("").to_string();
            if name.is_empty() {
                continue;
            }
            canon.tools.push(ToolDef {
                name,
                description: t
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                parameters: t
                    .get("input_schema")
                    .cloned()
                    .unwrap_or(Value::Object(Map::new())),
            });
        }
    }

    // tool_choice: {"type":"auto"|"any"|"tool","name"}。
    if let Some(tc) = body.get("tool_choice") {
        let kind = tc.get("type").and_then(Value::as_str).unwrap_or("auto");
        match kind {
            "any" => canon.tool_choice_kind = "required".to_string(),
            "tool" => {
                canon.tool_choice_kind = "named".to_string();
                canon.tool_choice_name = tc
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
            }
            "none" => {
                canon.tools.clear();
                canon.tool_choice_kind = "none".to_string();
            }
            _ => canon.tool_choice_kind = "auto".to_string(),
        }
    }
    Ok(canon)
}

fn estimate_prompt_tokens(canon: &CanonRequest) -> u32 {
    let mut total: u64 = 0;
    for m in &canon.messages {
        total += estimate_tokens(&m.content) as u64;
        for c in &m.tool_calls {
            total += estimate_tokens(&c.arguments) as u64;
        }
    }
    for t in &canon.tools {
        total += estimate_tokens(&t.description) as u64;
    }
    total as u32
}

pub async fn handle(state: Arc<AppState>, body_bytes: Bytes) -> Response {
    let cfg = state.config();
    let body: Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(e) => return json_error(&GatewayError::bad_request(format!("请求体不是合法 JSON: {e}"))),
    };
    let canon = match parse(&body, &cfg) {
        Ok(c) => c,
        Err(e) => return json_error(&e),
    };
    let prompt_est = estimate_prompt_tokens(&canon);

    if canon.stream {
        let (out_tx, out_rx) = mpsc::channel::<Result<ByteBuf, io::Error>>(64);
        tokio::spawn(stream_task(state, canon, out_tx, prompt_est));
        let body = axum::body::Body::from_stream(ReceiverStream::new(out_rx));
        (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "text/event-stream; charset=utf-8"),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            body,
        )
            .into_response()
    } else {
        let model = canon.model.clone();
        match pipeline::collect_pipeline(state, canon).await {
            Ok(collected) => {
                let usage_in = if prompt_est > 0 { prompt_est } else { collected.usage_in };
                Json(build_message(&collected, usage_in, &model)).into_response()
            }
            Err(e) => json_error(&e),
        }
    }
}

fn stop_reason(finish: &str) -> &'static str {
    match finish {
        "tool_calls" => "tool_use",
        "content_filter" => "refusal",
        _ => "end_turn",
    }
}

fn build_message(collected: &pipeline::Collected, usage_in: u32, model: &str) -> Value {
    let mut content: Vec<Value> = Vec::new();
    if !collected.reasoning.is_empty() {
        content.push(json!({
            "type": "thinking",
            "thinking": collected.reasoning,
            "signature": "sig_zmfd",
        }));
    }
    if !collected.text.is_empty() {
        content.push(json!({"type": "text", "text": collected.text}));
    }
    for c in &collected.calls {
        let input: Value = serde_json::from_str(&c.arguments).unwrap_or(json!({}));
        content.push(json!({
            "type": "tool_use",
            "id": c.id,
            "name": c.name,
            "input": input,
        }));
    }
    if content.is_empty() {
        content.push(json!({"type": "text", "text": ""}));
    }
    json!({
        "id": format!("msg_{}", uuid_hex()),
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        "stop_reason": stop_reason(&collected.finish_reason),
        "stop_sequence": Value::Null,
        "usage": {
            "input_tokens": usage_in,
            "output_tokens": collected.usage_out,
        }
    })
}

async fn send_event(
    out: &mpsc::Sender<Result<ByteBuf, io::Error>>,
    event: &str,
    data: &Value,
) -> bool {
    let mut s = String::from("event: ");
    s.push_str(event);
    s.push_str("\ndata: ");
    s.push_str(&data.to_string());
    s.push_str("\n\n");
    out.send(Ok(ByteBuf::from(s))).await.is_ok()
}

#[derive(PartialEq, Clone, Copy, Debug)]
enum Block {
    None,
    Text,
    Thinking,
}

async fn stream_task(
    state: Arc<AppState>,
    canon: CanonRequest,
    out: mpsc::Sender<Result<ByteBuf, io::Error>>,
    prompt_est: u32,
) {
    let id = format!("msg_{}", uuid_hex());
    let model = canon.model.clone();

    let (ptx, mut prx) = mpsc::channel::<Result<StreamItem, GatewayError>>(64);
    tokio::spawn(pipeline::run_pipeline(state, canon, ptx));

    let message_start = json!({
        "type": "message_start",
        "message": {
            "id": id,
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [],
            "stop_reason": Value::Null,
            "stop_sequence": Value::Null,
            "usage": {"input_tokens": prompt_est, "output_tokens": 0},
        }
    });
    if !send_event(&out, "message_start", &message_start).await {
        return;
    }

    let mut index: i64 = 0;
    let mut current = Block::None;
    let mut finished = false;

    // 关闭当前打开的内容块。
    async fn close_block(
        out: &mpsc::Sender<Result<ByteBuf, io::Error>>,
        current: &mut Block,
        index: i64,
    ) -> bool {
        match *current {
            Block::None => true,
            Block::Text => {
                *current = Block::None;
                send_event(
                    out,
                    "content_block_stop",
                    &json!({"type": "content_block_stop", "index": index}),
                )
                .await
            }
            Block::Thinking => {
                *current = Block::None;
                let _ = send_event(
                    out,
                    "content_block_delta",
                    &json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": {"type": "signature_delta", "signature": "sig_zmfd"},
                    }),
                )
                .await;
                send_event(
                    out,
                    "content_block_stop",
                    &json!({"type": "content_block_stop", "index": index}),
                )
                .await
            }
        }
    }

    while let Some(item) = prx.recv().await {
        match item {
            Ok(StreamItem::Reasoning(t)) => {
                if current != Block::Thinking {
                    // 关闭旧块后索引前移：客户端按 index 聚合内容块，
                    // 复用已关闭块的 index 会导致新旧块互相覆盖。
                    let had_open = current != Block::None;
                    if !close_block(&out, &mut current, index).await {
                        break;
                    }
                    if had_open {
                        index += 1;
                    }
                    let start = json!({
                        "type": "content_block_start",
                        "index": index,
                        "content_block": {"type": "thinking", "thinking": ""},
                    });
                    if !send_event(&out, "content_block_start", &start).await {
                        break;
                    }
                    current = Block::Thinking;
                }
                let delta = json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {"type": "thinking_delta", "thinking": t},
                });
                if !send_event(&out, "content_block_delta", &delta).await {
                    break;
                }
            }
            Ok(StreamItem::Text(t)) => {
                if current != Block::Text {
                    let had_open = current != Block::None;
                    if !close_block(&out, &mut current, index).await {
                        break;
                    }
                    if had_open {
                        index += 1;
                    }
                    let start = json!({
                        "type": "content_block_start",
                        "index": index,
                        "content_block": {"type": "text", "text": ""},
                    });
                    if !send_event(&out, "content_block_start", &start).await {
                        break;
                    }
                    current = Block::Text;
                }
                let delta = json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {"type": "text_delta", "text": t},
                });
                if !send_event(&out, "content_block_delta", &delta).await {
                    break;
                }
            }
            Ok(StreamItem::ToolCall(c)) => {
                let had_open = current != Block::None;
                if !close_block(&out, &mut current, index).await {
                    break;
                }
                if had_open {
                    index += 1;
                }
                let start = json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {"type": "tool_use", "id": c.id, "name": c.name, "input": {}},
                });
                if !send_event(&out, "content_block_start", &start).await {
                    break;
                }
                let delta = json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {"type": "input_json_delta", "partial_json": c.arguments},
                });
                let _ = send_event(&out, "content_block_delta", &delta).await;
                let stop = json!({"type": "content_block_stop", "index": index});
                let _ = send_event(&out, "content_block_stop", &stop).await;
                index += 1;
            }
            Ok(StreamItem::Done(d)) => {
                if !close_block(&out, &mut current, index).await {
                    break;
                }
                let delta = json!({
                    "type": "message_delta",
                    "delta": {"stop_reason": stop_reason(&d.finish_reason), "stop_sequence": Value::Null},
                    "usage": {"output_tokens": d.usage_out},
                });
                let _ = send_event(&out, "message_delta", &delta).await;
                let _ = send_event(&out, "message_stop", &json!({"type": "message_stop"})).await;
                finished = true;
                break;
            }
            Err(e) => {
                let _ = close_block(&out, &mut current, index).await;
                let err = json!({
                    "type": "error",
                    "error": {"type": e.anthropic_type(), "message": e.message},
                });
                let _ = send_event(&out, "error", &err).await;
                finished = true;
                break;
            }
        }
    }
    if !finished {
        // 管线异常退出：补发正常收尾，避免客户端悬挂。
        let _ = close_block(&out, &mut current, index).await;
        let delta = json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": Value::Null},
            "usage": {"output_tokens": 0},
        });
        let _ = send_event(&out, "message_delta", &delta).await;
        let _ = send_event(&out, "message_stop", &json!({"type": "message_stop"})).await;
    }
}

pub fn json_error(e: &GatewayError) -> Response {
    let status = StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, axum::Json(e.anthropic_json())).into_response()
}

/// /v1/messages/count_tokens：本地估算，不调用上游。
pub async fn handle_count_tokens(state: Arc<AppState>, body_bytes: Bytes) -> Response {
    let cfg = state.config();
    let body: Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(e) => return json_error(&GatewayError::bad_request(format!("请求体不是合法 JSON: {e}"))),
    };
    let canon = match parse(&body, &cfg) {
        Ok(c) => c,
        Err(e) => return json_error(&e),
    };
    let total = estimate_prompt_tokens(&canon);
    Json(json!({"input_tokens": total})).into_response()
}
