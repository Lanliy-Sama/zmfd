//! 通用小工具：时间、ID、token 估算。

use std::time::{SystemTime, UNIX_EPOCH};

/// 当前 Unix 毫秒时间戳。
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 当前 Unix 秒时间戳。
pub fn now_secs() -> u64 {
    now_ms() / 1000
}

/// 32 位无连字符的 uuid hex（对应官方前端 $d() 的生成方式）。
pub fn uuid_hex() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// 本地估算 token 数：约 4 个 ASCII 字符 = 1 token，每个非 ASCII 字符 = 1 token。
/// 上游不返回真实 usage，仅用于回填 usage 字段与 count_tokens。
pub fn estimate_tokens(text: &str) -> u32 {
    if text.is_empty() {
        return 0;
    }
    let mut ascii: usize = 0;
    let mut total: usize = 0;
    for c in text.chars() {
        total += 1;
        if c.is_ascii() {
            ascii += 1;
        }
    }
    let non_ascii = total - ascii;
    let est = (ascii + 3) / 4 + non_ascii;
    est.max(1) as u32
}

/// JSON 深度查找某个数字字段。
pub fn deep_find_u64(value: &serde_json::Value, key: &str) -> Option<u64> {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(v) = map.get(key) {
                if let Some(n) = v.as_u64() {
                    return Some(n);
                }
            }
            for (_k, v) in map {
                if let Some(found) = deep_find_u64(v, key) {
                    return Some(found);
                }
            }
            None
        }
        serde_json::Value::Array(items) => {
            for item in items {
                if let Some(found) = deep_find_u64(item, key) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

/// JSON 深度查找某个字符串/数字字段（响应包裹层级在不同接口间不完全一致）。
pub fn deep_find_str(value: &serde_json::Value, key: &str) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(v) = map.get(key) {
                if let Some(s) = v.as_str() {
                    if !s.is_empty() {
                        return Some(s.to_string());
                    }
                }
            }
            for (_k, v) in map {
                if let Some(found) = deep_find_str(v, key) {
                    return Some(found);
                }
            }
            None
        }
        serde_json::Value::Array(items) => {
            for item in items {
                if let Some(found) = deep_find_str(item, key) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}
