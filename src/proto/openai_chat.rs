//! OpenAI Chat Completions 协议适配（/v1/chat/completions、/v1/models）。

use crate::config::Config;
use crate::error::GatewayError;
use crate::pipeline::{self, StreamItem};
use crate::proto::*;
use crate::state::AppState;
use crate::tools::ToolDef;
use crate::util::{estimate_tokens, now_secs, uuid_hex};
use axum::extract::Bytes;
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
    canon.include_usage = body
        .get("stream_options")
        .and_then(|s| s.get("include_usage"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    canon.effort = resolve_effort(body, &canon.model, &cfg.upstream.default_effort);

    let messages = body
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| GatewayError::bad_request("缺少 messages 数组"))?;
    if messages.is_empty() {
        return Err(GatewayError::bad_request("messages 不能为空"));
    }
    for msg in messages {
        let role = msg.get("role").and_then(Value::as_str).unwrap_or("");
        match role {
            "system" | "developer" => {
                let content = content_to_text(msg.get("content"))?;
                canon.messages.push(CanonMessage::simple(Role::System, content));
            }
            "user" => {
                let content = content_to_text(msg.get("content"))?;
                canon.messages.push(CanonMessage::simple(Role::User, content));
            }
            "assistant" => {
                let content = content_to_text(msg.get("content"))?;
                let mut m = CanonMessage::simple(Role::Assistant, content);
                if let Some(calls) = msg.get("tool_calls").and_then(Value::as_array) {
                    for call in calls {
                        let id = call
                            .get("id")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        if let Some(f) = call.get("function") {
                            let name = f
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string();
                            if name.is_empty() {
                                continue;
                            }
                            m.tool_calls.push(CanonToolCall {
                                id,
                                name,
                                arguments: args_field_to_string(f.get("arguments")),
                            });
                        }
                    }
                }
                canon.messages.push(m);
            }
            "tool" | "function" => {
                let content = content_to_text(msg.get("content"))?;
                let mut m = CanonMessage::simple(Role::Tool, content);
                m.tool_call_id = msg
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                m.name = msg
                    .get("name")
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
            if t.get("type").and_then(Value::as_str) != Some("function") {
                continue;
            }
            // 兼容 {type:"function", function:{...}} 与平铺 {type:"function", name, ...}。
            let f = t.get("function").unwrap_or(t);
            let name = f
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                continue;
            }
            canon.tools.push(ToolDef {
                name,
                description: f
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                parameters: f
                    .get("parameters")
                    .cloned()
                    .unwrap_or(Value::Object(Map::new())),
            });
        }
    }

    match body.get("tool_choice") {
        Some(Value::String(s)) => match s.as_str() {
            "none" => {
                canon.tools.clear();
                canon.tool_choice_kind = "none".to_string();
            }
            "required" => canon.tool_choice_kind = "required".to_string(),
            _ => canon.tool_choice_kind = "auto".to_string(),
        },
        Some(Value::Object(o)) => {
            let name = o
                .get("function")
                .and_then(|f| f.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if !name.is_empty() {
                canon.tool_choice_kind = "named".to_string();
                canon.tool_choice_name = name.to_string();
            } else {
                canon.tool_choice_kind = "auto".to_string();
            }
        }
        _ => {}
    }
    Ok(canon)
}

fn args_field_to_string(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::Null) => "{}".to_string(),
        Some(Value::String(s)) => {
            if s.trim().is_empty() {
                "{}".to_string()
            } else {
                s.clone()
            }
        }
        Some(Value::Object(_)) => serde_json::to_string(v.unwrap()).unwrap_or("{}".to_string()),
        Some(other) => serde_json::to_string(other).unwrap_or("{}".to_string()),
    }
}

/// 客户端输入的 token 估算（用于 usage 回填）。
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
        tokio::spawn(stream_task(state, canon, out_tx));
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
                Json(build_completion(&collected, usage_in, &model)).into_response()
            }
            Err(e) => json_error(&e),
        }
    }
}

fn build_completion(collected: &pipeline::Collected, usage_in: u32, model: &str) -> Value {
    let mut message = json!({"role": "assistant", "content": collected.text});
    if !collected.reasoning.is_empty() {
        message["reasoning_content"] = json!(collected.reasoning);
    }
    if !collected.calls.is_empty() {
        message["tool_calls"] = Value::Array(
            collected
                .calls
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    json!({
                        "id": c.id,
                        "type": "function",
                        "function": {"name": c.name, "arguments": c.arguments},
                        "index": i,
                    })
                })
                .collect(),
        );
    }
    json!({
        "id": format!("chatcmpl-{}", uuid_hex()),
        "object": "chat.completion",
        "created": now_secs(),
        "model": model,
        "choices": [
            {"index": 0, "message": message, "finish_reason": collected.finish_reason}
        ],
        "usage": {
            "prompt_tokens": usage_in,
            "completion_tokens": collected.usage_out,
            "total_tokens": usage_in + collected.usage_out,
        }
    })
}

async fn send_chunk(
    out: &mpsc::Sender<Result<ByteBuf, io::Error>>,
    chunk: &Value,
) -> bool {
    let mut data = String::from("data: ");
    data.push_str(&chunk.to_string());
    data.push_str("\n\n");
    out.send(Ok(ByteBuf::from(data))).await.is_ok()
}

async fn send_raw(out: &mpsc::Sender<Result<ByteBuf, io::Error>>, s: &str) -> bool {
    out.send(Ok(ByteBuf::from(s.to_string()))).await.is_ok()
}

async fn stream_task(
    state: Arc<AppState>,
    canon: CanonRequest,
    out: mpsc::Sender<Result<ByteBuf, io::Error>>,
) {
    let id = format!("chatcmpl-{}", uuid_hex());
    let created = now_secs();
    let model = canon.model.clone();
    let include_usage = canon.include_usage;

    let (ptx, mut prx) = mpsc::channel::<Result<StreamItem, GatewayError>>(64);
    tokio::spawn(pipeline::run_pipeline(state, canon, ptx));

    let base_chunk = || -> Value {
        json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [],
        })
    };

    let mut first = base_chunk();
    first["choices"] = json!([{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": Value::Null}]);
    if !send_chunk(&out, &first).await {
        return;
    }

    let mut tool_index = 0usize;
    let mut finished = false;
    while let Some(item) = prx.recv().await {
        match item {
            Ok(StreamItem::Text(t)) => {
                let mut chunk = base_chunk();
                chunk["choices"] = json!([{"index": 0, "delta": {"content": t}, "finish_reason": Value::Null}]);
                if !send_chunk(&out, &chunk).await {
                    break;
                }
            }
            Ok(StreamItem::Reasoning(t)) => {
                let mut chunk = base_chunk();
                chunk["choices"] = json!([{"index": 0, "delta": {"reasoning_content": t}, "finish_reason": Value::Null}]);
                if !send_chunk(&out, &chunk).await {
                    break;
                }
            }
            Ok(StreamItem::ToolCall(c)) => {
                let idx = tool_index;
                tool_index += 1;
                let mut chunk = base_chunk();
                chunk["choices"] = json!([{"index": 0, "delta": {"tool_calls": [{
                    "index": idx, "id": c.id, "type": "function",
                    "function": {"name": c.name, "arguments": c.arguments},
                }]}, "finish_reason": Value::Null}]);
                if !send_chunk(&out, &chunk).await {
                    break;
                }
            }
            Ok(StreamItem::Done(d)) => {
                let mut chunk = base_chunk();
                chunk["choices"] = json!([{"index": 0, "delta": {}, "finish_reason": d.finish_reason}]);
                let _ = send_chunk(&out, &chunk).await;
                if include_usage {
                    let mut chunk = base_chunk();
                    chunk["usage"] = json!({
                        "prompt_tokens": d.usage_in,
                        "completion_tokens": d.usage_out,
                        "total_tokens": d.usage_in + d.usage_out,
                    });
                    let _ = send_chunk(&out, &chunk).await;
                }
                finished = true;
                break;
            }
            Err(e) => {
                let err = json!({"error": {"message": e.message, "type": e.openai_type(), "code": e.code}});
                let mut data = String::from("data: ");
                data.push_str(&err.to_string());
                data.push_str("\n\n");
                let _ = send_raw(&out, &data).await;
                finished = true;
                break;
            }
        }
    }
    if !finished {
        // 管线异常退出（接收端提前关闭等）：补一个正常结束，避免客户端悬挂。
        let mut chunk = base_chunk();
        chunk["choices"] = json!([{"index": 0, "delta": {}, "finish_reason": "stop"}]);
        let _ = send_chunk(&out, &chunk).await;
    }
    let _ = send_raw(&out, "data: [DONE]\n\n").await;
}

/// OpenAI 风格错误响应。
pub fn json_error(e: &GatewayError) -> Response {
    let status = StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, axum::Json(e.openai_json())).into_response()
}

/// /v1/models 响应。
pub fn models_response(cfg: &Config) -> Value {
    let data: Vec<Value> = cfg
        .models
        .iter()
        .map(|m| {
            json!({
                "id": m,
                "object": "model",
                "created": 0,
                "owned_by": "agentmore",
            })
        })
        .collect();
    json!({"object": "list", "data": data})
}
