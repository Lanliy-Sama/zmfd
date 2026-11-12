//! 上游访问令牌管理：refresh_token 账号池轮转、自动刷新、游客回退。
//!
//! refresh_token（长期，浏览器 Cookie 里的 chatglm_refresh_token）
//!   │  POST /chatglm/user-api/user/refresh  (Authorization: Bearer <refresh_token>, body {})
//!   ▼
//! { access_token, refresh_token }  ← refresh_token 会轮换，必须写回
//!   │  Authorization: Bearer <access_token>
//!   ▼
//! /chatglm/agent-api/chat/stream

use crate::error::GatewayError;
use crate::state::AppState;
use crate::upstream;
use crate::util::now_ms;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct Account {
    pub refresh_token: String,
    pub access_token: String,
    /// access_token 过期时刻（Unix ms）。
    pub expires_at: u64,
    pub health: String,
    pub checked_at: u64,
    /// account = 用户导入的 refresh_token；guest = 游客令牌。
    pub source: &'static str,
    /// 该槽位是否可在流式请求中携带文件附件（上游 400 拒绝后降级为 false）。
    pub can_attach: bool,
}

struct Inner {
    accounts: Vec<Account>,
    index: usize,
}

pub struct TokenManager {
    inner: Mutex<Inner>,
}

impl TokenManager {
    pub fn new() -> Self {
        TokenManager {
            inner: Mutex::new(Inner {
                accounts: Vec::new(),
                index: 0,
            }),
        }
    }

    /// 面板保存配置后同步账号池：保留已缓存的 access_token，删除失效槽位。
    pub async fn sync_accounts(&self, cfg_accounts: &[String]) {
        let mut inner = self.inner.lock().await;
        let mut next: Vec<Account> = Vec::new();
        for token in cfg_accounts {
            if let Some(existing) = inner
                .accounts
                .iter()
                .find(|a| a.source == "account" && a.refresh_token == *token)
            {
                next.push(existing.clone());
            } else {
                next.push(Account {
                    refresh_token: token.clone(),
                    access_token: String::new(),
                    expires_at: 0,
                    health: "unknown".to_string(),
                    checked_at: 0,
                    source: "account",
                    can_attach: true,
                });
            }
        }
        inner.accounts = next;
        inner.index = 0;
    }

    /// 按轮转顺序返回一个可用 access_token：(token, 槽位下标, 来源)。
    pub async fn get_access_token(
        &self,
        state: &AppState,
    ) -> Result<(String, usize, &'static str), GatewayError> {
        let mut inner = self.inner.lock().await;

        let len = inner.accounts.len();
        for _ in 0..len {
            let idx = inner.index % len;
            inner.index += 1;
            let cached_ok = {
                let a = &inner.accounts[idx];
                !a.access_token.is_empty() && a.expires_at > now_ms() + 10_000
            };
            if cached_ok {
                let a = &inner.accounts[idx];
                return Ok((a.access_token.clone(), idx, a.source));
            }
            match refresh_account(state, &mut inner.accounts[idx]).await {
                Ok((token, rotated)) => {
                    let mut rotated_changed = false;
                    {
                        let a = &mut inner.accounts[idx];
                        a.access_token = token.clone();
                        a.expires_at = now_ms() + TOKEN_TTL_MS;
                        a.health = "healthy".to_string();
                        a.checked_at = now_ms();
                        if a.source == "account" {
                            if let Some(new_refresh) = rotated {
                                if !new_refresh.is_empty() && new_refresh != a.refresh_token {
                                    a.refresh_token = new_refresh;
                                    rotated_changed = true;
                                }
                            }
                        }
                    }
                    if rotated_changed {
                        persist_accounts(state, &inner.accounts);
                    }
                    let src = inner.accounts[idx].source;
                    return Ok((token, idx, src));
                }
                Err(_e) => {
                    let a = &mut inner.accounts[idx];
                    a.health = "refresh_failed".to_string();
                    a.checked_at = now_ms();
                    continue;
                }
            }
        }

        // 全部账号不可用：按配置回退游客令牌。
        let cfg = state.config();
        if cfg.guest_fallback {
            match ensure_guest_slot(state, &mut inner).await {
                Ok(idx) => {
                    let acct = &inner.accounts[idx];
                    return Ok((acct.access_token.clone(), idx, acct.source));
                }
                Err(e) => return Err(e),
            }
        }

        Err(GatewayError::unavailable(
            "没有可用的上游账号：请先在面板（/）导入 refresh_token，或开启游客回退",
        ))
    }

    /// 立即刷新指定槽位（面板测试用）。返回 (是否成功, 错误信息)。
    pub async fn refresh_slot(&self, state: &AppState, slot: usize) -> (bool, String) {
        let mut inner = self.inner.lock().await;
        if slot >= inner.accounts.len() {
            return (false, "槽位不存在".to_string());
        }
        let acct = &mut inner.accounts[slot];
        match refresh_account(state, acct).await {
            Ok((token, rotated)) => {
                let mut rotated_changed = false;
                {
                    let a = &mut inner.accounts[slot];
                    a.access_token = token;
                    a.expires_at = now_ms() + TOKEN_TTL_MS;
                    a.health = "healthy".to_string();
                    a.checked_at = now_ms();
                    if a.source == "account" {
                        if let Some(new_refresh) = rotated {
                            if !new_refresh.is_empty() && new_refresh != a.refresh_token {
                                a.refresh_token = new_refresh;
                                rotated_changed = true;
                            }
                        }
                    }
                }
                if rotated_changed {
                    persist_accounts(state, &inner.accounts);
                }
                (true, String::new())
            }
            Err(e) => {
                acct.health = "refresh_failed".to_string();
                acct.checked_at = now_ms();
                (false, e.message)
            }
        }
    }

    /// 面板状态视图。
    pub async fn snapshot(&self) -> Vec<serde_json::Value> {
        let inner = self.inner.lock().await;
        let now = now_ms();
        inner
            .accounts
            .iter()
            .enumerate()
            .map(|(i, a)| {
                serde_json::json!({
                    "slot": i + 1,
                    "source": a.source,
                    "health": a.health,
                    "token_masked": mask_token(&a.refresh_token),
                    "cached": !a.access_token.is_empty() && a.expires_at > now,
                    "checked_at": a.checked_at,
                    "can_attach": a.can_attach,
                })
            })
            .collect()
    }

    pub async fn account_count(&self) -> usize {
        self.inner.lock().await.accounts.len()
    }

    /// 指定槽位当前是否允许携带文件附件。
    pub async fn slot_can_attach(&self, slot: usize) -> bool {
        self.inner
            .lock()
            .await
            .accounts
            .get(slot)
            .map(|a| a.can_attach)
            .unwrap_or(false)
    }

    /// 上游拒绝附件后把槽位降级为内联模式。
    pub async fn set_slot_attach(&self, slot: usize, can: bool) {
        let mut inner = self.inner.lock().await;
        if let Some(a) = inner.accounts.get_mut(slot) {
            a.can_attach = can;
        }
    }
}

/// access_token 缓存时长。上游未稳定返回 expires_in，用保守值避免缓存过期令牌。
const TOKEN_TTL_MS: u64 = 300_000;

async fn refresh_account(
    state: &AppState,
    acct: &mut Account,
) -> Result<(String, Option<String>), GatewayError> {
    if acct.source == "guest" {
        let (access, _refresh) = upstream::guest_access(state).await?;
        return Ok((access, None));
    }

    let cfg = state.config();
    let base = cfg.upstream.base_url.clone();
    let secret = cfg.upstream.sign_secret.clone();
    let device_id = cfg.device_id.clone();
    let mut last_err = GatewayError::upstream("未尝试任何刷新端点");

    for url in [
        format!("{base}/chatglm/user-api/user/refresh"),
        "https://chatglm.cn/chatglm/user-api/user/refresh".to_string(),
    ] {
        let headers = upstream::upstream_headers(&secret, &device_id, &base, None, false, true, Some(&acct.refresh_token));
        let client = state.http();
        let resp = match client
            .post(&url)
            .headers(headers)
            .json(&serde_json::json!({}))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                last_err = GatewayError::upstream(format!("刷新请求失败（{url}）: {e}"));
                continue;
            }
        };
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        if status != 200 {
            last_err = crate::error::map_upstream_status(status, &body);
            continue;
        }
        let parsed: serde_json::Value = match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(_) => {
                last_err = GatewayError::upstream("令牌刷新返回非 JSON");
                continue;
            }
        };
        let access = crate::util::deep_find_str(&parsed, "access_token");
        let Some(access) = access else {
            last_err = GatewayError::upstream("令牌刷新响应缺少 access_token");
            continue;
        };
        let rotated = crate::util::deep_find_str(&parsed, "refresh_token");
        return Ok((access, rotated));
    }
    Err(last_err)
}

/// 获取（或复用）游客令牌槽位，返回其下标。
async fn ensure_guest_slot(state: &AppState, inner: &mut Inner) -> Result<usize, GatewayError> {
    let (access, refresh) = upstream::guest_access(state).await?;
    if let Some(pos) = inner.accounts.iter().position(|a| a.source == "guest") {
        let acct = &mut inner.accounts[pos];
        acct.access_token = access;
        acct.refresh_token = if refresh.is_empty() {
            acct.refresh_token.clone()
        } else {
            refresh
        };
        acct.expires_at = now_ms() + TOKEN_TTL_MS;
        acct.health = "healthy".to_string();
        acct.checked_at = now_ms();
        Ok(pos)
    } else {
        inner.accounts.push(Account {
            refresh_token: refresh,
            access_token: access,
            expires_at: now_ms() + TOKEN_TTL_MS,
            health: "healthy".to_string(),
            checked_at: now_ms(),
            source: "guest",
            // 游客实测：流式请求携带文件附件会被上游以 HTTP 400 拒绝，直接走内联。
            can_attach: false,
        });
        Ok(inner.accounts.len() - 1)
    }
}

/// 把账号池当前 refresh_token 写回配置文件（发生轮换时调用）。
fn persist_accounts(state: &AppState, accounts: &[Account]) {
    let mut cfg = state.config();
    cfg.accounts = accounts
        .iter()
        .filter(|a| a.source == "account" && !a.refresh_token.is_empty())
        .map(|a| a.refresh_token.clone())
        .collect();
    if let Err(e) = state.persist_config_with(cfg) {
        eprintln!("[zmfd] 写回轮换后的 refresh_token 失败: {e}");
    }
}

pub fn mask_token(token: &str) -> String {
    let chars: Vec<char> = token.chars().collect();
    if chars.len() <= 8 {
        return "*".repeat(chars.len());
    }
    let head: String = chars[..4].iter().collect();
    let tail: String = chars[chars.len() - 4..].iter().collect();
    format!("{head}****{tail}")
}
