//! 管理面板后端：配置读写、状态视图、登录校验、上游测试。

use crate::error::GatewayError;
use crate::pipeline;
use crate::proto::{CanonMessage, CanonRequest, Role};
use crate::state::AppState;
use axum::extract::Bytes;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;

/// 面板鉴权：设置了密码时要求 X-Panel-Password 头匹配。
pub fn check_panel_auth(state: &AppState, headers: &HeaderMap) -> Result<(), GatewayError> {
    let cfg = state.config();
    if cfg.panel_password.is_empty() {
        return Ok(());
    }
    let got = headers
        .get("x-panel-password")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if got == cfg.panel_password {
        Ok(())
    } else {
        Err(GatewayError::unauthorized("面板密码错误"))
    }
}

fn panel_json_error(e: GatewayError) -> Response {
    let status = StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, axum::Json(json!({"ok": false, "error": e.message}))).into_response()
}

pub async fn api_config_get(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = check_panel_auth(&state, &headers) {
        return panel_json_error(e);
    }
    axum::Json(state.config()).into_response()
}

pub async fn api_config_put(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(e) = check_panel_auth(&state, &headers) {
        return panel_json_error(e);
    }
    let incoming: crate::config::Config = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return panel_json_error(GatewayError::bad_request(format!(
                "配置格式错误: {e}"
            )));
        }
    };
    match state.apply_config(incoming) {
        Ok(()) => axum::Json(json!({"ok": true})).into_response(),
        Err(e) => panel_json_error(GatewayError::new(400, "invalid_request", e)),
    }
}

pub async fn api_login(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    body: Bytes,
) -> Response {
    let v: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => Value::Null,
    };
    let password = v
        .get("password")
        .and_then(Value::as_str)
        .unwrap_or("");
    let cfg = state.config();
    if cfg.panel_password.is_empty() {
        return axum::Json(json!({"ok": true, "message": "未设置面板密码"})).into_response();
    }
    if password == cfg.panel_password {
        axum::Json(json!({"ok": true})).into_response()
    } else {
        (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"ok": false, "error": "密码错误"})),
        )
            .into_response()
    }
}

pub async fn api_status(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = check_panel_auth(&state, &headers) {
        return panel_json_error(e);
    }
    let cfg = state.config();
    let accounts = state.tokens.snapshot().await;
    let sem = state
        .semaphore
        .read()
        .expect("semaphore lock poisoned")
        .clone();
    let available = sem.available_permits();
    axum::Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "started_at": state.started_at,
        "accounts": accounts,
        "concurrency": {"limit": cfg.max_concurrency, "available": available},
        "upstream": {
            "base_url": cfg.upstream.base_url,
            "assistant_id": cfg.upstream.assistant_id,
            "chat_mode": cfg.upstream.chat_mode,
        },
        "guest_fallback": cfg.guest_fallback,
        "config_file": state.cfg_path.display().to_string(),
    }))
    .into_response()
}

/// 上游连通性测试：mode=tokens 刷新全部账号；mode=chat 做一次最小对话往返。
pub async fn api_test(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(e) = check_panel_auth(&state, &headers) {
        return panel_json_error(e);
    }
    let v: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let mode = v.get("mode").and_then(Value::as_str).unwrap_or("tokens");

    match mode {
        "tokens" => {
            let n = state.tokens.account_count().await;
            let mut results: Vec<Value> = Vec::new();
            let mut all_ok = n > 0;
            for i in 0..n {
                let (ok, msg) = state.tokens.refresh_slot(&state, i).await;
                if !ok {
                    all_ok = false;
                }
                results.push(json!({"slot": i + 1, "ok": ok, "error": msg}));
            }
            if n == 0 {
                // 无账号：尝试游客通道。
                match state.tokens.get_access_token(&state).await {
                    Ok((_t, slot, source)) => {
                        all_ok = true;
                        results.push(json!({"slot": slot + 1, "ok": true, "source": source, "error": ""}));
                    }
                    Err(e) => {
                        all_ok = false;
                        results.push(json!({"slot": 0, "ok": false, "error": e.message}));
                    }
                }
            }
            axum::Json(json!({"ok": all_ok, "results": results})).into_response()
        }
        "chat" => {
            let cfg = state.config();
            let mut canon = CanonRequest::new_empty(cfg.upstream.selected_model.clone());
            canon
                .messages
                .push(CanonMessage::simple(Role::User, "请只回复四个字：测试成功"));
            let started = Instant::now();
            match pipeline::collect_pipeline(state, canon).await {
                Ok(collected) => axum::Json(json!({
                    "ok": true,
                    "reply": collected.text,
                    "elapsed_ms": started.elapsed().as_millis() as u64,
                }))
                .into_response(),
                Err(e) => axum::Json(json!({
                    "ok": false,
                    "error": e.message,
                    "elapsed_ms": started.elapsed().as_millis() as u64,
                }))
                .into_response(),
            }
        }
        other => panel_json_error(GatewayError::bad_request(format!(
            "未知测试模式: {other}"
        ))),
    }
}
