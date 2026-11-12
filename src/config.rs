//! 网关配置：持久化到 config.json，面板可在线修改并热生效。

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const DEFAULT_SIGN_SECRET: &str = "8a1317a7468aa3ad86e997d08f3f31cb";
pub const DEFAULT_ASSISTANT_ID: &str = "65940acff94777010aa6b796";
pub const DEFAULT_BASE_URL: &str = "https://agentmore.chatglm.cn";

/// 并发硬上限：上游对并发敏感，最高 3。
pub const MAX_CONCURRENCY_CAP: usize = 3;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct UpstreamConfig {
    /// 上游站点基址（不带末尾斜杠）。
    pub base_url: String,
    /// X-Sign 协议指纹密钥（官方网页端 JS 公开常量）。
    pub sign_secret: String,
    /// 对话模式使用的 assistant_id（网页 URL /chat/<agent_id>/<conversation_id> 里的 agent_id）。
    pub assistant_id: String,
    /// 流式端点的 chat_mode，官方对话模式为 chat_agent。
    pub chat_mode: String,
    /// 默认思考强度：low | high | max（客户端未指定时使用）
    pub default_effort: String,
    /// 强制思考强度：非空时覆盖客户端请求的 reasoning_effort。
    pub effort_override: String,
    /// 是否允许上游联网搜索。
    pub is_networking: bool,
    /// 默认上游模型（selected_model）。
    pub selected_model: String,
    /// 请求结束后删除上游会话（保持服务端干净）。
    pub delete_conversation: bool,
    /// 上传文件时的 from 字段（官方前端固定为 claw）。
    pub upload_from: String,
    /// 上下文投递方式：file = 上传为附件；inline = 压平进消息文本。
    pub context_send_mode: String,
    /// 上下文文件最大字符数（超出截断）。
    pub max_context_chars: usize,
    /// SSE 相邻数据包最大空闲秒数（超时判定流已死）。
    pub stream_idle_timeout_secs: u64,
    /// 单次请求总时长上限秒数。
    pub total_timeout_secs: u64,
}

impl UpstreamConfig {
    pub fn with_defaults() -> Self {
        UpstreamConfig {
            base_url: DEFAULT_BASE_URL.to_string(),
            sign_secret: DEFAULT_SIGN_SECRET.to_string(),
            assistant_id: DEFAULT_ASSISTANT_ID.to_string(),
            chat_mode: "chat_agent".to_string(),
            default_effort: "high".to_string(),
            effort_override: String::new(),
            is_networking: true,
            selected_model: "glm-5.3".to_string(),
            delete_conversation: true,
            upload_from: "claw".to_string(),
            context_send_mode: "file".to_string(),
            max_context_chars: 200_000,
            stream_idle_timeout_secs: 90,
            total_timeout_secs: 900,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// 监听地址。
    pub host: String,
    /// 监听端口。
    pub port: u16,
    /// 本网关 API Key（Bearer / x-api-key 任一匹配）。空列表 = 不鉴权。
    pub api_keys: Vec<String>,
    /// 面板访问密码。空 = 不鉴权（仅建议本机监听时使用）。
    pub panel_password: String,
    /// 上游最大并发（1..=3）。
    pub max_concurrency: usize,
    /// 上游参数。
    pub upstream: UpstreamConfig,
    /// refresh_token 账号池（浏览器 Cookie chatglm_refresh_token）。
    pub accounts: Vec<String>,
    /// 无账号时是否回退到游客令牌。
    pub guest_fallback: bool,
    /// /v1/models 暴露的模型列表。
    pub models: Vec<String>,
    /// 模型别名映射：客户端模型名 -> 上游 selected_model。
    pub model_aliases: BTreeMap<String, String>,
    /// 未知模型策略：default = 映射到 selected_model；passthrough = 原样传给上游。
    pub unknown_model_policy: String,
    /// 发往上游使用的代理（可选），如 http://127.0.0.1:7897。
    pub proxy: String,
    /// 调试日志：打印上游原始 SSE 行（截断）。
    pub debug_log: bool,
    /// 上游请求使用的稳定设备 ID（自动生成）。
    pub device_id: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: "127.0.0.1".to_string(),
            port: 8795,
            api_keys: Vec::new(),
            panel_password: String::new(),
            max_concurrency: MAX_CONCURRENCY_CAP,
            upstream: UpstreamConfig::with_defaults(),
            accounts: Vec::new(),
            guest_fallback: true,
            models: vec!["glm-5.3".to_string(), "glm-5.3-flash".to_string()],
            model_aliases: BTreeMap::new(),
            unknown_model_policy: "default".to_string(),
            proxy: String::new(),
            debug_log: false,
            device_id: String::new(),
        }
    }
}

impl Config {
    /// 加载配置文件；不存在时写入默认配置。
    pub fn load_or_create(path: &PathBuf) -> Result<Config, String> {
        if path.exists() {
            let raw = std::fs::read_to_string(path)
                .map_err(|e| format!("读取配置失败: {e}"))?;
            let mut cfg: Config = serde_json::from_str(&raw)
                .map_err(|e| format!("解析配置失败: {e}"))?;
            cfg.normalize();
            Ok(cfg)
        } else {
            let cfg = Config::default();
            cfg.save_to(path)?;
            Ok(cfg)
        }
    }

    /// 规范化字段值（截断/钳制/去重），保存前与加载后都要调用。
    pub fn normalize(&mut self) {
        self.host = self.host.trim().to_string();
        if self.host.is_empty() {
            self.host = "127.0.0.1".to_string();
        }
        if self.port == 0 {
            self.port = 8795;
        }
        if self.max_concurrency < 1 {
            self.max_concurrency = 1;
        }
        if self.max_concurrency > MAX_CONCURRENCY_CAP {
            self.max_concurrency = MAX_CONCURRENCY_CAP;
        }
        self.api_keys = clean_lines(&self.api_keys);
        self.accounts = clean_lines(&self.accounts);
        self.models = clean_lines(&self.models);
        if self.models.is_empty() {
            self.models = vec!["glm-5.3".to_string(), "glm-5.3-flash".to_string()];
        }
        self.model_aliases.retain(|k, v| !k.trim().is_empty() && !v.trim().is_empty());
        let p = self.unknown_model_policy.as_str();
        if p != "default" && p != "passthrough" {
            self.unknown_model_policy = "default".to_string();
        }
        let e = self.upstream.default_effort.as_str();
        if e != "low" && e != "high" && e != "max" {
            self.upstream.default_effort = "high".to_string();
        }
        let eo = self.upstream.effort_override.as_str();
        if eo != "low" && eo != "high" && eo != "max" {
            self.upstream.effort_override = String::new();
        }
        if self.upstream.base_url.trim().is_empty() {
            self.upstream.base_url = DEFAULT_BASE_URL.to_string();
        }
        if self.upstream.sign_secret.trim().is_empty() {
            self.upstream.sign_secret = DEFAULT_SIGN_SECRET.to_string();
        }
        if self.upstream.assistant_id.trim().is_empty() {
            self.upstream.assistant_id = DEFAULT_ASSISTANT_ID.to_string();
        }
        if self.upstream.chat_mode.trim().is_empty() {
            self.upstream.chat_mode = "chat_agent".to_string();
        }
        if self.upstream.upload_from.trim().is_empty() {
            self.upstream.upload_from = "claw".to_string();
        }
        let m = self.upstream.context_send_mode.as_str();
        if m != "file" && m != "inline" {
            self.upstream.context_send_mode = "file".to_string();
        }
        if self.upstream.max_context_chars < 2_000 {
            self.upstream.max_context_chars = 2_000;
        }
        if self.upstream.stream_idle_timeout_secs < 10 {
            self.upstream.stream_idle_timeout_secs = 10;
        }
        if self.upstream.total_timeout_secs < 30 {
            self.upstream.total_timeout_secs = 30;
        }
        self.upstream.base_url = self.upstream.base_url.trim_end_matches('/').to_string();
    }

    pub fn save_to(&self, path: &PathBuf) -> Result<(), String> {
        // normalize 需要 &mut；对快照做规范化，保持 &self 签名不变。
        let mut snapshot = self.clone();
        snapshot.normalize();
        let json = serde_json::to_string_pretty(&snapshot).map_err(|e| e.to_string())?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, json.as_bytes()).map_err(|e| format!("写入配置失败: {e}"))?;
        std::fs::rename(&tmp, path).map_err(|e| format!("替换配置失败: {e}"))?;
        Ok(())
    }

    /// 把客户端模型名解析为上游 selected_model。
    pub fn resolve_model(&self, model: &str) -> String {
        let model = model.trim();
        if let Some(v) = self.model_aliases.get(model) {
            let v = v.trim();
            if !v.is_empty() {
                return v.to_string();
            }
        }
        if model.starts_with("glm-") {
            return model.to_string();
        }
        if self.unknown_model_policy == "passthrough" && !model.is_empty() {
            return model.to_string();
        }
        self.upstream.selected_model.clone()
    }
}

/// 清理字符串列表：trim、去空、去重。
fn clean_lines(items: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for item in items {
        let t = item.trim().to_string();
        if !t.is_empty() && !out.contains(&t) {
            out.push(t);
        }
    }
    out
}
