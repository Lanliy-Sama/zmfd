//! OpenAI Responses 协议适配（/v1/responses）。

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
    canon.effort = resolve_effort(body, &canon.model, &cfg.upstream.default_effort);

    // instructions -> 顶层系统消息。
    if let Some(Value::String(s)) = body.get("instructions") {
        if !s.is_empty() {
            canon.messages.push(CanonMessage::simple(Role::System, s.clone()));
        }
    }

    let input = body
        .get("input")
        .ok_or_else(|| GatewayError::bad_request("缺少 input 字段"))?;
    match input {
        Value::String(s) => {
            canon.messages.push(CanonMessage::simple(Role::User, s.clone()));
        }
        Value::Array(items) => {
            if items.is_empty() {
                return Err(GatewayError::bad_request("input 不能为空"));
            }
            for item in items {
                let itype = item.get("type").and_then(Value::as_str).unwrap_or("message");
                match itype {
                    "message" => {
                        let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
                        let content = item.get("content");
                        match role {
                            "user" | "system" | "developer" => {
                                let text = content_to_text(content)?;
                                let r = match role {
                                    "user" => Role::User,
                                    _ => Role::System,
                                };
                                canon.messages.push(CanonMessage::simple(r, text));
                            }
                            "assistant" => {
                                let text = content_to_text(content)?;
                                canon
                                    .messages
                                    .push(CanonMessage::simple(Role::Assistant, text));
                            }
                            other => {
                                return Err(GatewayError::bad_request(format!(
                                    "不支持的消息角色: {other}"
                                )));
                            }
                        }
                    }
                    "function_call" => {
                        let mut m = CanonMessage::simple(Role::Assistant, String::new());
                        m.tool_calls.push(CanonToolCall {
                            id: item
                                .get("call_id")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            name: item
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            arguments: item
                                .get("arguments")
                                .and_then(Value::as_str)
                                .unwrap_or("{}")
                                .to_string(),
                        });
                        canon.messages.push(m);
                    }
                    "function_call_output" => {
                        let mut m = CanonMessage::simple(Role::Tool, String::new());
                        m.tool_call_id = item
                            .get("call_id")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        m.content = output_to_text(item.get("output"));
                        canon.messages.push(m);
                    }
                    // reasoning / item_reference / web_search_call 等回显项：忽略。
                    _ => {}
                }
            }
        }
        _ => {
            return Err(GatewayError::bad_request(
                "input 必须是字符串或 item 数组",
            ));
        }
    }

    if canon.messages.iter().all(|m| m.role != Role::User) {
        return Err(GatewayError::bad_request("input 中缺少用户消息"));
    }

    // Responses API 的 tools 是平铺形态：{type:"function", name, description, parameters}。
    if let Some(tools) = body.get("tools").and_then(Value::as_array) {
        for t in tools {
            if t.get("type").and_then(Value::as_str) != Some("function") {
                continue;
            }
            let name = t
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
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
            if o.get("type").and_then(Value::as_str) == Some("function") {
                let name = o.get("name").and_then(Value::as_str).unwrap_or("");
                if !name.is_empty() {
                    canon.tool_choice_kind = "named".to_string();
                    canon.tool_choice_name = name.to_string();
                }
            }
        }
        _ => {}
    }
    Ok(canon)
}

/// function_call_output 的 output 归一化为文本。
fn output_to_text(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Object(map)) => {
            // 兼容 {output: "..."} / {content: "..."} 包裹。
            for key in ["output", "content", "text"] {
                if let Some(Value::String(s)) = map.get(key) {
                    return s.clone();
                }
            }
            serde_json::to_string(v.unwrap()).unwrap_or_default()
        }
        Some(Value::Array(items)) => {
            let mut parts: Vec<String> = Vec::new();
            for item in items {
                match item {
                    Value::String(s) => parts.push(s.clone()),
                    Value::Object(map) => {
                        if let Some(t) = map.get("text").and_then(Value::as_str) {
                            parts.push(t.to_string());
                        }
                    }
                    _ => {}
                }
            }
            parts.join("\n")
        }
        Some(other) => serde_json::to_string(other).unwrap_or_default(),
    }
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
                Json(build_response(&collected, usage_in, &model)).into_response()
            }
            Err(e) => json_error(&e),
        }
    }
}

fn base_response(id: &str, model: &str, status: &str) -> Value {
    json!({
        "id": id,
        "object": "response",
        "created_at": now_secs(),
        "status": status,
        "model": model,
        "output": [],
    })
}

fn reasoning_item(id: &str, summary_text: &str, complete: bool) -> Value {
    let mut item = json!({
        "id": id,
        "type": "reasoning",
        "summary": [{"type": "summary_text", "text": summary_text}],
    });
    if complete {
        item["status"] = json!("completed");
    }
    item
}

fn message_item(id: &str, text: &str, complete: bool) -> Value {
    let mut item = json!({
        "id": id,
        "type": "message",
        "role": "assistant",
        "content": [{"type": "output_text", "text": text, "annotations": []}],
    });
    if complete {
        item["status"] = json!("completed");
    }
    item
}

fn function_call_item(id: &str, call_id: &str, name: &str, arguments: &str) -> Value {
    json!({
        "id": id,
        "type": "function_call",
        "call_id": call_id,
        "name": name,
        "arguments": arguments,
        "status": "completed",
    })
}

fn build_response(collected: &pipeline::Collected, usage_in: u32, model: &str) -> Value {
    let id = format!("resp_{}", uuid_hex());
    let mut resp = base_response(&id, model, "completed");
    let mut output: Vec<Value> = Vec::new();
    if !collected.reasoning.is_empty() {
        output.push(reasoning_item(&format!("rs_{}", uuid_hex()), &collected.reasoning, true));
    }
    if !collected.text.is_empty() {
        output.push(message_item(&format!("msg_{}", uuid_hex()), &collected.text, true));
    }
    for c in &collected.calls {
        output.push(function_call_item(
            &format!("fc_{}", uuid_hex()),
            &c.id,
            &c.name,
            &c.arguments,
        ));
    }
    resp["output"] = Value::Array(output);
    resp["usage"] = json!({
        "input_tokens": usage_in,
        "output_tokens": collected.usage_out,
        "total_tokens": usage_in + collected.usage_out,
        "output_tokens_details": {"reasoning_tokens": collected.usage_out},
    });
    resp
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

async fn stream_task(
    state: Arc<AppState>,
    canon: CanonRequest,
    out: mpsc::Sender<Result<ByteBuf, io::Error>>,
    prompt_est: u32,
) {
    let model = canon.model.clone();
    let resp_id = format!("resp_{}", uuid_hex());

    let (ptx, mut prx) = mpsc::channel::<Result<StreamItem, GatewayError>>(64);
    tokio::spawn(pipeline::run_pipeline(state, canon, ptx));

    let mut resp = base_response(&resp_id, &model, "in_progress");
    let _ = send_event(&out, "response.created", &json!({"type": "response.created", "response": resp.clone()})).await;
    let _ = send_event(&out, "response.in_progress", &json!({"type": "response.in_progress", "response": resp.clone()})).await;

    let mut output_items: Vec<Value> = Vec::new();
    let mut output_index: i64 = 0;

    // 当前打开的输出项：None / Reasoning / Message。
    #[derive(PartialEq, Clone, Copy, Debug)]
    enum Open {
        None,
        Reasoning,
        Message,
    }
    let mut open = Open::None;
    let mut reasoning_id = String::new();
    let mut reasoning_buf = String::new();
    let mut message_id = String::new();
    let mut message_buf = String::new();
    let mut finished = false;

    async fn close_reasoning(
        out: &mpsc::Sender<Result<ByteBuf, io::Error>>,
        output_items: &mut Vec<Value>,
        output_index: &mut i64,
        reasoning_id: &mut String,
        reasoning_buf: &mut String,
        open: &mut Open,
    ) -> bool {
        if *open != Open::Reasoning {
            return true;
        }
        *open = Open::None;
        let idx = *output_index;
        let id = reasoning_id.clone();
        let full = reasoning_buf.clone();
        let _ = send_event(out, "response.reasoning_summary_text.done",
            &json!({"type": "response.reasoning_summary_text.done", "item_id": id, "output_index": idx, "summary_index": 0, "text": full})).await;
        let _ = send_event(out, "response.reasoning_summary_part.done",
            &json!({"type": "response.reasoning_summary_part.done", "item_id": id, "output_index": idx, "summary_index": 0, "part": {"type": "summary_text", "text": full}})).await;
        let item = reasoning_item(&id, &full, true);
        let ok = send_event(out, "response.output_item.done",
            &json!({"type": "response.output_item.done", "output_index": idx, "item": item})).await;
        output_items.push(item);
        *output_index += 1;
        *reasoning_id = String::new();
        *reasoning_buf = String::new();
        ok
    }

    async fn close_message(
        out: &mpsc::Sender<Result<ByteBuf, io::Error>>,
        output_items: &mut Vec<Value>,
        output_index: &mut i64,
        message_id: &mut String,
        message_buf: &mut String,
        open: &mut Open,
    ) -> bool {
        if *open != Open::Message {
            return true;
        }
        *open = Open::None;
        let idx = *output_index;
        let id = message_id.clone();
        let full = message_buf.clone();
        let _ = send_event(out, "response.output_text.done",
            &json!({"type": "response.output_text.done", "item_id": id, "output_index": idx, "content_index": 0, "text": full})).await;
        let _ = send_event(out, "response.content_part.done",
            &json!({"type": "response.content_part.done", "item_id": id, "output_index": idx, "content_index": 0, "part": {"type": "output_text", "text": full, "annotations": []}})).await;
        let item = message_item(&id, &full, true);
        let ok = send_event(out, "response.output_item.done",
            &json!({"type": "response.output_item.done", "output_index": idx, "item": item})).await;
        output_items.push(item);
        *output_index += 1;
        *message_id = String::new();
        *message_buf = String::new();
        ok
    }

    async fn close_open(
        out: &mpsc::Sender<Result<ByteBuf, io::Error>>,
        output_items: &mut Vec<Value>,
        output_index: &mut i64,
        reasoning_id: &mut String,
        reasoning_buf: &mut String,
        message_id: &mut String,
        message_buf: &mut String,
        open: &mut Open,
    ) -> bool {
        if !close_message(out, output_items, output_index, message_id, message_buf, open).await {
            return false;
        }
        close_reasoning(out, output_items, output_index, reasoning_id, reasoning_buf, open).await
    }

    while let Some(item) = prx.recv().await {
        match item {
            Ok(StreamItem::Reasoning(t)) => {
                if open != Open::Reasoning {
                    if !close_open(&out, &mut output_items, &mut output_index, &mut reasoning_id, &mut reasoning_buf, &mut message_id, &mut message_buf, &mut open).await {
                        break;
                    }
                    reasoning_id = format!("rs_{}", uuid_hex());
                    let item_added = reasoning_item(&reasoning_id, "", false);
                    if !send_event(&out, "response.output_item.added",
                        &json!({"type": "response.output_item.added", "output_index": output_index, "item": item_added})).await {
                        break;
                    }
                    let _ = send_event(&out, "response.reasoning_summary_part.added",
                        &json!({"type": "response.reasoning_summary_part.added", "item_id": reasoning_id, "output_index": output_index, "summary_index": 0, "part": {"type": "summary_text", "text": ""}})).await;
                    open = Open::Reasoning;
                }
                reasoning_buf.push_str(&t);
                if !send_event(&out, "response.reasoning_summary_text.delta",
                    &json!({"type": "response.reasoning_summary_text.delta", "item_id": reasoning_id, "output_index": output_index, "summary_index": 0, "delta": t})).await {
                    break;
                }
            }
            Ok(StreamItem::Text(t)) => {
                if open != Open::Message {
                    if !close_open(&out, &mut output_items, &mut output_index, &mut reasoning_id, &mut reasoning_buf, &mut message_id, &mut message_buf, &mut open).await {
                        break;
                    }
                    message_id = format!("msg_{}", uuid_hex());
                    let item_added = message_item(&message_id, "", false);
                    if !send_event(&out, "response.output_item.added",
                        &json!({"type": "response.output_item.added", "output_index": output_index, "item": item_added})).await {
                        break;
                    }
                    let _ = send_event(&out, "response.content_part.added",
                        &json!({"type": "response.content_part.added", "item_id": message_id, "output_index": output_index, "content_index": 0, "part": {"type": "output_text", "text": "", "annotations": []}})).await;
                    open = Open::Message;
                }
                message_buf.push_str(&t);
                if !send_event(&out, "response.output_text.delta",
                    &json!({"type": "response.output_text.delta", "item_id": message_id, "output_index": output_index, "content_index": 0, "delta": t})).await {
                    break;
                }
            }
            Ok(StreamItem::ToolCall(c)) => {
                if !close_open(&out, &mut output_items, &mut output_index, &mut reasoning_id, &mut reasoning_buf, &mut message_id, &mut message_buf, &mut open).await {
                    break;
                }
                let fc_id = format!("fc_{}", uuid_hex());
                let item_added = function_call_item(&fc_id, &c.id, &c.name, "");
                if !send_event(&out, "response.output_item.added",
                    &json!({"type": "response.output_item.added", "output_index": output_index, "item": item_added})).await {
                    break;
                }
                let _ = send_event(&out, "response.function_call_arguments.delta",
                    &json!({"type": "response.function_call_arguments.delta", "item_id": fc_id, "output_index": output_index, "delta": c.arguments})).await;
                let _ = send_event(&out, "response.function_call_arguments.done",
                    &json!({"type": "response.function_call_arguments.done", "item_id": fc_id, "output_index": output_index, "arguments": c.arguments})).await;
                let done_item = function_call_item(&fc_id, &c.id, &c.name, &c.arguments);
                let _ = send_event(&out, "response.output_item.done",
                    &json!({"type": "response.output_item.done", "output_index": output_index, "item": done_item.clone()})).await;
                output_items.push(done_item);
                output_index += 1;
            }
            Ok(StreamItem::Done(d)) => {
                if !close_open(&out, &mut output_items, &mut output_index, &mut reasoning_id, &mut reasoning_buf, &mut message_id, &mut message_buf, &mut open).await {
                    break;
                }
                resp["output"] = Value::Array(output_items.clone());
                resp["status"] = json!("completed");
                resp["usage"] = json!({
                    "input_tokens": prompt_est,
                    "output_tokens": d.usage_out,
                    "total_tokens": prompt_est + d.usage_out,
                    "output_tokens_details": {"reasoning_tokens": d.usage_out},
                });
                let _ = send_event(&out, "response.completed",
                    &json!({"type": "response.completed", "response": resp})).await;
                finished = true;
                break;
            }
            Err(e) => {
                let mut resp = base_response(&resp_id, &model, "failed");
                resp["error"] = json!({
                    "code": e.code,
                    "message": e.message,
                });
                let _ = send_event(&out, "response.failed",
                    &json!({"type": "response.failed", "response": resp})).await;
                finished = true;
                break;
            }
        }
    }
    if !finished {
        let mut resp = base_response(&resp_id, &model, "completed");
        resp["output"] = Value::Array(output_items.clone());
        let _ = send_event(&out, "response.completed",
            &json!({"type": "response.completed", "response": resp})).await;
    }
}

pub fn json_error(e: &GatewayError) -> Response {
    let status = StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, axum::Json(e.openai_json())).into_response()
}
