//! 上游请求签名与请求头构造。
//!
//! X-Sign = MD5("{timestamp}-{nonce}-{secret}")。timestamp 为毫秒时间戳字符串，
//! 倒数第二位数字替换为校验位：校验位 = (各位数字之和 - 原倒数第二位) % 10。
//! 签名密钥是官方网页端 JS 里的公开常量（协议指纹），可在面板覆盖。

use crate::util::{now_ms, uuid_hex};

/// 官方网页端内置签名密钥（公开协议指纹，可被面板覆盖）。
pub const FALLBACK_SIGN_SECRET: &str = crate::config::DEFAULT_SIGN_SECRET;

/// 伪装成 PC 端 Edge 浏览器。
pub const BROWSER_UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0";

/// 生成带校验位的时间戳字符串。
pub fn sign_timestamp(now: u64) -> String {
    let s = now.to_string();
    let digits: Vec<u32> = s
        .chars()
        .map(|c| c.to_digit(10).unwrap_or(0))
        .collect();
    if digits.len() < 2 {
        return s;
    }
    let n = digits.len();
    let sum: u32 = digits.iter().sum();
    let checksum = (sum - digits[n - 2]) % 10;
    let mut out = String::with_capacity(n);
    for (i, d) in digits.iter().enumerate() {
        if i == n - 2 {
            out.push_str(&checksum.to_string());
        } else {
            out.push_str(&d.to_string());
        }
    }
    out
}

pub fn md5_hex(input: &str) -> String {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    let mut out = String::with_capacity(32);
    for b in result.iter() {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// 一组签名头（不含 Authorization / Content-Type / Accept）。
pub fn sign_headers(secret: &str, device_id: &str) -> Vec<(String, String)> {
    let ts = sign_timestamp(now_ms());
    let nonce = uuid_hex();
    let sign = md5_hex(&format!("{}-{}-{}", ts, nonce, secret));
    vec![
        ("App-Name".to_string(), "chatglm".to_string()),
        ("X-Device-Id".to_string(), device_id.to_string()),
        ("X-Request-Id".to_string(), uuid_hex()),
        ("X-App-Platform".to_string(), "pc".to_string()),
        ("X-App-Version".to_string(), "0.0.1".to_string()),
        ("X-Timestamp".to_string(), ts),
        ("X-Nonce".to_string(), nonce),
        ("X-Sign".to_string(), sign),
    ]
}
