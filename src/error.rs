//! 网关内部错误类型与三种协议的错误响应渲染。

use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct GatewayError {
    pub status: u16,
    /// 内部错误码，例如 invalid_api_key / rate_limit_error / upstream_error
    pub code: String,
    pub message: String,
}

impl GatewayError {
    pub fn new(status: u16, code: &str, message: impl Into<String>) -> Self {
        GatewayError {
            status,
            code: code.to_string(),
            message: message.into(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(400, "invalid_request", message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(401, "invalid_api_key", message)
    }

    pub fn rate_limited(message: impl Into<String>) -> Self {
        Self::new(429, "rate_limit_error", message)
    }

    pub fn upstream(message: impl Into<String>) -> Self {
        Self::new(502, "upstream_error", message)
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(504, "upstream_timeout", message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(503, "service_unavailable", message)
    }

    /// OpenAI 系错误里的 error.type。
    pub fn openai_type(&self) -> String {
        match self.code.as_str() {
            "invalid_api_key" | "invalid_request" => "invalid_request_error".to_string(),
            "rate_limit_error" => "rate_limit_error".to_string(),
            _ => "api_error".to_string(),
        }
    }

    /// Anthropic 错误里的 error.type。
    pub fn anthropic_type(&self) -> String {
        match self.code.as_str() {
            "invalid_request" => "invalid_request_error".to_string(),
            "invalid_api_key" => "authentication_error".to_string(),
            "rate_limit_error" => "rate_limit_error".to_string(),
            "service_unavailable" => "overloaded_error".to_string(),
            _ => "api_error".to_string(),
        }
    }

    pub fn openai_json(&self) -> Value {
        json!({
            "error": {
                "message": self.message,
                "type": self.openai_type(),
                "code": self.code,
            }
        })
    }

    pub fn anthropic_json(&self) -> Value {
        json!({
            "type": "error",
            "error": {
                "type": self.anthropic_type(),
                "message": self.message,
            }
        })
    }
}

impl std::fmt::Display for GatewayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{} {}] {}", self.status, self.code, self.message)
    }
}

impl std::error::Error for GatewayError {}

/// 把上游 HTTP 错误映射为网关错误。body 片段进入消息便于排障。
pub fn map_upstream_status(status: u16, body: &str) -> GatewayError {
    let snippet: String = body.chars().take(300).collect();
    if status == 401 || status == 403 {
        return GatewayError::unauthorized(format!(
            "上游鉴权失败（HTTP {status}）: {snippet}"
        ));
    }
    if status == 429 {
        return GatewayError::rate_limited(format!("上游限流（HTTP 429）: {snippet}"));
    }
    if (400..500).contains(&status) {
        return GatewayError::bad_request(format!(
            "上游拒绝请求（HTTP {status}）: {snippet}"
        ));
    }
    GatewayError::upstream(format!("上游错误（HTTP {status}）: {snippet}"))
}
