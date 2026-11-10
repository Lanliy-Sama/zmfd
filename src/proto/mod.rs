//! 协议层共享的规范请求/消息类型与通用提取函数。

pub mod anthropic;
pub mod openai_chat;
pub mod responses_api;

use crate::error::GatewayError;
use crate::tools::ToolDef;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone)]
pub struct CanonToolCall {
    pub id: String,
    pub name: String,
    /// 紧凑 JSON 字符串。
    pub arguments: String,
}

#[derive(Debug, Clone)]
pub struct CanonMessage {
    pub role: Role,
    pub content: String,
    /// assistant 消息携带的工具调用。
    pub tool_calls: Vec<CanonToolCall>,
    /// tool 消息对应的调用 ID。
    pub tool_call_id: String,
    /// tool 消息对应的函数名。
    pub name: String,
}

impl CanonMessage {
    pub fn simple(role: Role, content: impl Into<String>) -> Self {
        CanonMessage {
            role,
            content: content.into(),
            tool_calls: Vec::new(),
            tool_call_id: String::new(),
            name: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CanonRequest {
    /// 客户端原始模型名（响应需原样回显）。
    pub model: String,
    pub messages: Vec<CanonMessage>,
    pub tools: Vec<ToolDef>,
    /// auto | required | named | none
    pub tool_choice_kind: String,
    /// tool_choice 指定的工具名（kind = named 时）。
    pub tool_choice_name: String,
    pub stream: bool,
    /// 上游思考强度：low | high | max（已映射）。
    pub effort: String,
    /// OpenAI chat：stream_options.include_usage。
    pub include_usage: bool,
}

impl CanonRequest {
    pub fn new_empty(model: impl Into<String>) -> Self {
        CanonRequest {
            model: model.into(),
            messages: Vec::new(),
            tools: Vec::new(),
            tool_choice_kind: "auto".to_string(),
            tool_choice_name: String::new(),
            stream: false,
            effort: "high".to_string(),
            include_usage: false,
        }
    }

    pub fn known_tool_names(&self) -> Vec<String> {
        self.tools.iter().map(|t| t.name.clone()).collect()
    }
}

/// 把消息 content（字符串或 part 数组）提取为纯文本。
/// 遇到图片/文件等多媒体 part 时报错 —— 本网关只支持文本输入。
pub fn content_to_text(content: Option<&Value>) -> Result<String, GatewayError> {
    let Some(content) = content else {
        return Ok(String::new());
    };
    match content {
        Value::Null => Ok(String::new()),
        Value::String(s) => Ok(s.clone()),
        Value::Array(items) => {
            let mut parts: Vec<String> = Vec::new();
            for item in items {
                match item {
                    Value::String(s) => parts.push(s.clone()),
                    Value::Object(map) => {
                        let kind = map.get("type").and_then(Value::as_str).unwrap_or("");
                        match kind {
                            "text" | "input_text" | "output_text" | "summary_text" => {
                                if let Some(t) = map.get("text").and_then(Value::as_str) {
                                    parts.push(t.to_string());
                                }
                            }
                            // Anthropic tool_result 内容块出现在 user 消息里，
                            // 由各协议适配器单独处理，这里跳过。
                            "tool_use" | "tool_result" | "thinking" | "redacted_thinking" => {}
                            "" => {
                                if let Some(t) = map.get("text").and_then(Value::as_str) {
                                    parts.push(t.to_string());
                                }
                            }
                            _ => {
                                return Err(GatewayError::bad_request(
                                    "该网关当前只支持文本输入，不支持图片 / 文件等多媒体内容",
                                ));
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(parts.join("\n"))
        }
        _ => Ok(String::new()),
    }
}

/// Anthropic tool_result 的 content 提取（字符串或 [{type:"text",text}] 或 [{type:"tool_result"...}]）。
pub fn tool_result_text(content: Option<&Value>) -> String {
    match content {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
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
        Some(Value::Object(map)) => map
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        Some(other) => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// 思考强度归一化：客户端字符串 -> low|high|max。
pub fn normalize_effort(effort: &str) -> String {
    match effort.trim().to_lowercase().as_str() {
        "minimal" | "low" | "fast" => "low".to_string(),
        "medium" | "high" => "high".to_string(),
        "max" | "xhigh" => "max".to_string(),
        _ => String::new(),
    }
}

/// 是否表达了「要思考」的意图（OpenAI/Anthropic 各种思考参数）。
pub fn detect_reasoning_intent(body: &Value) -> bool {
    if body.get("enable_thinking").and_then(Value::as_bool).unwrap_or(false) {
        return true;
    }
    if body.get("thinking_enabled").and_then(Value::as_bool).unwrap_or(false) {
        return true;
    }
    if let Some(t) = body.get("thinking") {
        match t {
            Value::Bool(true) => return true,
            Value::Object(map) => {
                if map.get("enabled").and_then(Value::as_bool).unwrap_or(false) {
                    return true;
                }
                if let Some(k) = map.get("type").and_then(Value::as_str) {
                    if matches!(k, "enabled" | "on" | "adaptive") {
                        return true;
                    }
                }
            }
            _ => {}
        }
    }
    false
}

/// 合并解析 effort：显式 reasoning_effort 优先；其次模型名 / 思考意图 -> high；最后网关默认。
pub fn resolve_effort(body: &Value, model: &str, default_effort: &str) -> String {
    if let Some(e) = body.get("reasoning_effort").and_then(Value::as_str) {
        let mapped = normalize_effort(e);
        if !mapped.is_empty() {
            return mapped;
        }
        // "none" 等无法映射的值：按是否有思考意图回落。
        if detect_reasoning_intent(body) {
            return "high".to_string();
        }
        return default_effort.to_string();
    }
    if let Some(e) = body
        .get("reasoning")
        .and_then(|r| r.get("effort"))
        .and_then(Value::as_str)
    {
        let mapped = normalize_effort(e);
        if !mapped.is_empty() {
            return mapped;
        }
    }
    let model_lower = model.to_lowercase();
    if model_lower.contains("think") || model_lower.contains("reasoner") {
        return "high".to_string();
    }
    if detect_reasoning_intent(body) {
        return "high".to_string();
    }
    default_effort.to_string()
}
