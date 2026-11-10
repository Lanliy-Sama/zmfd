//! agentmore2api —— agentmore.chatglm.cn（对话模式）反向网关。
//!
//! 提供三种协议入口：
//!   POST /v1/chat/completions   OpenAI Chat Completions
//!   POST /v1/responses          OpenAI Responses
//!   POST /v1/messages           Anthropic Messages
//! 每次调用都会新建上游对话，完整上下文以 Markdown 文件附件形式发送；
//! 上游并发最高 3（可在面板调整 1..=3）。

mod config;
mod context;
mod error;
mod panel;
mod pipeline;
mod proto;
mod sign;
mod state;
mod tokens;
mod tools;
mod upstream;
mod util;

use axum::body::Bytes;
use axum::extract::DefaultBodyLimit;
use axum::http::HeaderMap;
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use error::GatewayError;
use state::AppState;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

static INDEX_HTML: &str = include_str!("../static/index.html");

fn main() {
    let mut host: Option<String> = None;
    let mut port: Option<u16> = None;
    let mut cfg_path = PathBuf::from("config.json");
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--host" => host = args.next(),
            "--port" => port = args.next().and_then(|v| v.parse().ok()),
            "--config" => {
                if let Some(p) = args.next() {
                    cfg_path = PathBuf::from(p);
                }
            }
            "--version" => {
                println!("agentmore2api {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            other => {
                eprintln!("未知参数: {other}（支持 --host --port --config --version）");
                std::process::exit(2);
            }
        }
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("构建 tokio runtime 失败");
    runtime.block_on(async_main(host, port, cfg_path));
}

async fn async_main(host_override: Option<String>, port_override: Option<u16>, cfg_path: PathBuf) {
    let mut cfg = match config::Config::load_or_create(&cfg_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[zmfd] {e}");
            std::process::exit(1);
        }
    };
    if let Some(h) = host_override {
        cfg.host = h;
    }
    if let Some(p) = port_override {
        cfg.port = p;
    }
    if cfg.device_id.trim().is_empty() {
        cfg.device_id = util::uuid_hex();
        let _ = cfg.save_to(&cfg_path);
    }

    let addr_host = cfg.host.clone();
    let addr_port = cfg.port;
    let state = Arc::new(AppState::new(cfg, cfg_path));

    let app = Router::new()
        .route("/", get(panel_index))
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/api/config",
            get(panel::api_config_get).put(panel::api_config_put),
        )
        .route("/api/login", post(panel::api_login))
        .route("/api/status", get(panel::api_status))
        .route("/api/test", post(panel::api_test))
        .route("/v1/models", get(api_models))
        .route("/v1/chat/completions", post(api_chat_completions))
        .route("/v1/responses", post(api_responses))
        .route("/v1/messages", post(api_messages))
        .route("/v1/messages/count_tokens", post(api_messages_count))
        .layer(middleware::from_fn(log_mw))
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state.clone());

    let listener = match tokio::net::TcpListener::bind((addr_host.as_str(), addr_port)).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[zmfd] 监听 {addr_host}:{addr_port} 失败: {e}");
            std::process::exit(1);
        }
    };
    eprintln!(
        "[zmfd] agentmore2api v{} 已启动: http://{}:{}（配置面板同地址）",
        env!("CARGO_PKG_VERSION"),
        addr_host,
        addr_port
    );
    let serve = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());
    if let Err(e) = serve.await {
        eprintln!("[zmfd] 服务退出: {e}");
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("[zmfd] 收到退出信号");
}

async fn log_mw(req: axum::extract::Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let start = Instant::now();
    let resp = next.run(req).await;
    eprintln!(
        "[zmfd] {} {} -> {} ({}ms)",
        method,
        path,
        resp.status().as_u16(),
        start.elapsed().as_millis()
    );
    resp
}

async fn panel_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// 本网关 API 鉴权：Authorization: Bearer <key> 或 x-api-key: <key>。
fn check_api_auth(state: &AppState, headers: &HeaderMap) -> Result<(), GatewayError> {
    let cfg = state.config();
    if cfg.api_keys.is_empty() {
        return Ok(());
    }
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    let xkey = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let hit = (!bearer.is_empty() && cfg.api_keys.iter().any(|k| k == bearer))
        || (!xkey.is_empty() && cfg.api_keys.iter().any(|k| k == xkey));
    if hit {
        Ok(())
    } else {
        Err(GatewayError::unauthorized(
            "缺少或错误的 API Key（Authorization: Bearer <key> 或 x-api-key: <key>）",
        ))
    }
}

async fn api_models(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = check_api_auth(&state, &headers) {
        return proto::openai_chat::json_error(&e);
    }
    Json(proto::openai_chat::models_response(&state.config())).into_response()
}

async fn api_chat_completions(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(e) = check_api_auth(&state, &headers) {
        return proto::openai_chat::json_error(&e);
    }
    proto::openai_chat::handle(state, body).await
}

async fn api_responses(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(e) = check_api_auth(&state, &headers) {
        return proto::responses_api::json_error(&e);
    }
    proto::responses_api::handle(state, body).await
}

async fn api_messages(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(e) = check_api_auth(&state, &headers) {
        return proto::anthropic::json_error(&e);
    }
    proto::anthropic::handle(state, body).await
}

async fn api_messages_count(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(e) = check_api_auth(&state, &headers) {
        return proto::anthropic::json_error(&e);
    }
    proto::anthropic::handle_count_tokens(state, body).await
}
