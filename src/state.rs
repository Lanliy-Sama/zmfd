//! 全局共享状态：配置、并发闸门、HTTP 客户端、令牌管理。

use crate::config::Config;
use crate::error::GatewayError;
use crate::tokens::TokenManager;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub struct AppState {
    /// 运行时配置（面板可热改）。锁内不做任何 await。
    pub config: RwLock<Config>,
    /// 配置文件路径。
    pub cfg_path: PathBuf,
    /// 上游并发闸门（可随配置替换）。
    pub semaphore: RwLock<Arc<Semaphore>>,
    /// 发往上游的 HTTP 客户端（代理变更时重建）。
    pub http: RwLock<reqwest::Client>,
    /// 上游令牌管理。
    pub tokens: TokenManager,
    pub started_at: u64,
}

impl AppState {
    pub fn new(config: Config, cfg_path: PathBuf) -> Self {
        let concurrency = config.max_concurrency;
        let http = build_http_client(&config);
        AppState {
            config: RwLock::new(config),
            cfg_path,
            semaphore: RwLock::new(Arc::new(Semaphore::new(concurrency))),
            http: RwLock::new(http),
            tokens: TokenManager::new(),
            started_at: crate::util::now_secs(),
        }
    }

    /// 配置快照（克隆）。
    pub fn config(&self) -> Config {
        self.config.read().expect("config lock poisoned").clone()
    }

    /// HTTP 客户端句柄（reqwest Client 克隆廉价）。
    pub fn http(&self) -> reqwest::Client {
        self.http.read().expect("http lock poisoned").clone()
    }

    /// 获取一个上游并发许可；超限时排队等待而非报错。
    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, GatewayError> {
        let sem = self.semaphore.read().expect("semaphore lock poisoned").clone();
        sem.acquire_owned()
            .await
            .map_err(|_| GatewayError::upstream("并发闸门已关闭"))
    }

    /// 保存配置并让可热更的组件生效（并发闸门 / HTTP 客户端 / 账号池）。
    pub fn apply_config(&self, mut cfg: Config) -> Result<(), String> {
        cfg.normalize();
        let old = self.config();
        // 面板提交的配置不含 device_id，保留现有设备指纹避免漂移。
        if cfg.device_id.trim().is_empty() {
            cfg.device_id = old.device_id.clone();
        }
        let concurrency_changed = old.max_concurrency != cfg.max_concurrency;
        let proxy_changed = old.proxy != cfg.proxy;

        {
            let mut w = self.config.write().expect("config lock poisoned");
            *w = cfg.clone();
        }
        cfg.save_to(&self.cfg_path)?;

        if concurrency_changed {
            *self.semaphore.write().expect("semaphore lock poisoned") =
                Arc::new(Semaphore::new(cfg.max_concurrency));
        }
        if proxy_changed {
            *self.http.write().expect("http lock poisoned") = build_http_client(&cfg);
        }
        self.tokens.sync_accounts(&cfg.accounts);
        Ok(())
    }

    /// 把当前配置写回磁盘（token 轮换时调用）。
    pub fn persist_config(&self) -> Result<(), String> {
        let cfg = self.config();
        cfg.save_to(&self.cfg_path)
    }

    /// 用给定配置替换内存配置并写盘（token 轮换写回用）。
    pub fn persist_config_with(&self, cfg: Config) -> Result<(), String> {
        {
            let mut w = self.config.write().expect("config lock poisoned");
            *w = cfg.clone();
        }
        cfg.save_to(&self.cfg_path)
    }
}

pub fn build_http_client(cfg: &Config) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(15))
        .user_agent(crate::sign::BROWSER_UA);
    if !cfg.proxy.trim().is_empty() {
        if let Ok(p) = reqwest::Proxy::all(cfg.proxy.trim()) {
            builder = builder.proxy(p);
        }
    }
    builder.build().unwrap_or_default()
}
