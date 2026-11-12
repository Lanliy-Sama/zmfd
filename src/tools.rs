//! 工具调用桥。
//!
//! 请求侧：把结构化 tools[] 序列化为注入提示词的文本契约，约定模型用
//!   <tool>{"name": "...", "arguments": {...}}</tool>
//! 块描述调用；响应侧：从流式文本中增量剥离这些块，还原为结构化工具调用。

use serde_json::Value;

/// 提示词与解析两端必须一致的标记。
pub const TOOL_BLOCK_OPEN: &str = "<tool>";
pub const TOOL_BLOCK_CLOSE: &str = "</tool>";
pub const TOOL_CALL_OPEN: &str = "<tool_call";
pub const TOOL_CALL_CLOSE: &str = "</tool_call>";

#[derive(Debug, Clone)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

/// 把工具定义序列化为注入提示词的指令段（完整版：格式约定 + Schema 清单）。
/// 用于内联模式。bias_kind/bias_name 描述 tool_choice 的倾向。
pub fn instructions(tools: &[ToolDef], bias_kind: &str, bias_name: &str) -> String {
    if tools.is_empty() {
        return String::new();
    }
    let mut lines: Vec<String> = format_rules_lines(bias_kind, bias_name);
    lines.push(String::new());
    lines.push("可用工具：".to_string());
    for tool in tools {
        lines.push(format!("- 名称: {}", tool.name));
        if !tool.description.is_empty() {
            lines.push(format!("  说明: {}", tool.description));
        }
        let schema = compact_json(&tool.parameters);
        lines.push(format!("  入参 Schema: {schema}"));
    }
    lines.push("[/TOOL USE INSTRUCTIONS]".to_string());
    lines.join("\n")
}

fn format_rules_lines(bias_kind: &str, bias_name: &str) -> Vec<String> {
    let mut lines: Vec<String> = vec![
        "[TOOL USE INSTRUCTIONS]".to_string(),
        "你可以调用工具。当且仅当需要调用工具时，你的回复中输出如下格式的调用块（可多个，每个一块）：".to_string(),
        String::new(),
        format!("{TOOL_BLOCK_OPEN}{{\"name\": \"<工具名>\", \"arguments\": {{ ... }}}}{TOOL_BLOCK_CLOSE}"),
        String::new(),
        "规则：".to_string(),
        "1. 只有确实要调用工具时才输出调用块；不需要调用工具时正常作答，不要提及本约定。".to_string(),
        "2. arguments 必须是合法 JSON 对象，并符合对应工具的入参 Schema。".to_string(),
        "3. 不要把调用块包在代码块或其他标记里。".to_string(),
    ];
    match bias_kind {
        "required" => lines.push("4. 本次要求必须调用一个工具，不要直接给出普通回答。".to_string()),
        "named" => lines.push(format!(
            "4. 本次必须调用名为「{bias_name}」的工具，不要直接给出普通回答。"
        )),
        _ => {}
    }
    lines.push("[/TOOL USE INSTRUCTIONS]".to_string());
    lines
}

/// 工具调用格式约定（仅格式，不含工具清单）。附件模式下 Schema 单独作为附件 2 上传，
/// 正文只携带格式约定与文件阅读引导。
pub fn format_rules(bias_kind: &str, bias_name: &str) -> String {
    if bias_kind == "none" {
        return String::new();
    }
    format_rules_lines(bias_kind, bias_name).join("\n")
}

/// 工具定义文档（上传为附件 2）：名称、说明与入参 Schema。
pub fn tools_document(tools: &[ToolDef]) -> String {
    let mut s = String::new();
    s.push_str("# 可用工具定义\n\n");
    s.push_str(&format!(
        "共 {} 个工具。调用任何工具前，请先核对本文件中对应工具的入参 Schema；调用块的 arguments 必须是符合 Schema 的合法 JSON 对象。\n\n",
        tools.len()
    ));
    for (i, tool) in tools.iter().enumerate() {
        s.push_str(&format!("## 工具 {}：{}\n", i + 1, tool.name));
        if !tool.description.is_empty() {
            s.push_str(&format!("- 说明：{}\n", tool.description));
        }
        s.push_str(&format!(
            "- 入参 Schema：\n\n```json\n{}\n```\n\n",
            compact_json(&tool.parameters)
        ));
    }
    s
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
}

#[derive(Debug, Clone)]
pub struct ParsedToolCall {
    pub name: String,
    pub arguments_json: String,
}

/// 流拆分产物：正文片段或一个完整的工具调用。
#[derive(Debug, Clone)]
pub enum SplitPiece {
    Text(String),
    Tool(ParsedToolCall),
}

/// 增量地把模型输出拆成「正文」与「工具调用」。
/// 工具块可能跨流式分片，未闭合的块开头会保留在缓冲区。
pub struct ToolStreamSplitter {
    known_names: Vec<String>,
    buffer: String,
}

impl ToolStreamSplitter {
    pub fn new(known_names: Vec<String>) -> Self {
        ToolStreamSplitter {
            known_names,
            buffer: String::new(),
        }
    }

    pub fn feed(&mut self, text: &str) -> Vec<SplitPiece> {
        self.buffer.push_str(text);
        self.drain(false)
    }

    pub fn flush(&mut self) -> Vec<SplitPiece> {
        self.drain(true)
    }

    fn drain(&mut self, flush_rest: bool) -> Vec<SplitPiece> {
        let mut out: Vec<SplitPiece> = Vec::new();
        loop {
            // 找最早出现的块开头："<tool>" 或 "<tool_call"。
            let open_exact = self.buffer.find(TOOL_BLOCK_OPEN);
            let open_call = self.buffer.find(TOOL_CALL_OPEN);
            let open_pos = match (open_exact, open_call) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            let Some(open_pos) = open_pos else {
                break;
            };

            // 先把开标签之前的正文安全发出，再从开标签处截断缓冲区。
            // 这样即使闭标签迟迟未到，正文也不会滞留，且标签本身不会泄漏。
            if open_pos > 0 {
                out.push(SplitPiece::Text(self.buffer[..open_pos].to_string()));
                self.buffer.drain(..open_pos);
            }

            // 此时缓冲区以开标签开头。
            let is_call = self.buffer.starts_with(TOOL_CALL_OPEN);
            let (open_tag_end, close_tag) = if is_call {
                match self.buffer.find('>') {
                    Some(gt) => (gt + 1, TOOL_CALL_CLOSE),
                    None => break, // 开标签尚未接收完整，等待更多数据
                }
            } else {
                (TOOL_BLOCK_OPEN.len(), TOOL_BLOCK_CLOSE)
            };

            let Some(close_rel) = self.buffer[open_tag_end..].find(close_tag) else {
                break; // 闭标签未到，继续等
            };
            let close_start = open_tag_end + close_rel;
            let close_end = close_start + close_tag.len();

            let body = self.buffer[open_tag_end..close_start].to_string();
            match parse_tool_block(&body, &self.known_names) {
                Some(call) => out.push(SplitPiece::Tool(call)),
                None => {
                    // 块内 JSON 无法解析或工具名未知：把整个块按正文保留，避免静默丢失。
                    let raw = self.buffer[..close_end].to_string();
                    out.push(SplitPiece::Text(raw));
                }
            }
            self.buffer.drain(..close_end);
        }

        if flush_rest {
            if !self.buffer.is_empty() {
                out.push(SplitPiece::Text(std::mem::take(&mut self.buffer)));
            }
        } else {
            // 无完整块且无块开头时，保留可能是块前缀的尾部（最长 "<tool_call" 10 字符）。
            let tags = [TOOL_BLOCK_OPEN, TOOL_CALL_OPEN, TOOL_BLOCK_CLOSE];
            let mut hold = 0usize;
            let chars: Vec<char> = self.buffer.chars().collect();
            for k in 1..=10usize {
                if k > chars.len() {
                    break;
                }
                let tail: String = chars[chars.len() - k..].iter().collect();
                if tags.iter().any(|t| t.starts_with(&tail)) {
                    hold = k;
                }
            }
            if hold > 0 {
                let keep: String = chars[chars.len() - hold..].iter().collect();
                let emit_len = chars.len() - hold;
                let emit: String = chars[..emit_len].iter().collect();
                if !emit.is_empty() {
                    out.push(SplitPiece::Text(emit));
                }
                self.buffer = keep;
            }
        }
        out
    }
}

/// 解析一个工具块体：{"name": ..., "arguments": ...}。
fn parse_tool_block(body: &str, known: &[String]) -> Option<ParsedToolCall> {
    let body = body.trim();
    if body.is_empty() {
        return None;
    }
    let parsed = loose_json_parse(body)?;
    match parsed {
        Value::Object(map) => {
            let name = map.get("name").and_then(Value::as_str)?.to_string();
            let matched = match_tool_name(&name, known)?;
            let args_json = arguments_to_json(map.get("arguments"));
            Some(ParsedToolCall {
                name: matched,
                arguments_json: args_json,
            })
        }
        Value::Array(items) => {
            // 数组形态不支持单块返回多个调用，取第一个合法元素。
            for item in items {
                if let Value::Object(map) = item {
                    if let Some(name) = map.get("name").and_then(Value::as_str) {
                        if let Some(matched) = match_tool_name(name, known) {
                            let args_json = arguments_to_json(map.get("arguments"));
                            return Some(ParsedToolCall {
                                name: matched,
                                arguments_json: args_json,
                            });
                        }
                    }
                }
            }
            None
        }
        _ => None,
    }
}

/// arguments 归一化为紧凑 JSON 字符串。
fn arguments_to_json(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "{}".to_string(),
        Some(Value::Object(_)) => compact_json(value.unwrap()),
        Some(Value::String(s)) => {
            // 字符串形态的 arguments：尝试按 JSON 对象解析。
            if let Some(v) = loose_json_parse(s) {
                if v.is_object() {
                    return compact_json(&v);
                }
            }
            // 无法解析时包一层，保证始终是合法 JSON 对象。
            serde_json::to_string(&json_object_with("value", s)).unwrap_or("{}".to_string())
        }
        Some(other) => serde_json::to_string(&json_object_with("value", other))
            .unwrap_or("{}".to_string()),
    }
}

fn json_object_with(key: &str, value: &Value) -> Value {
    let mut map = serde_json::Map::new();
    map.insert(key.to_string(), value.clone());
    Value::Object(map)
}

/// 工具名匹配：精确 -> 忽略大小写 -> 相似度 >= 0.72 的模糊匹配。
fn match_tool_name(requested: &str, known: &[String]) -> Option<String> {
    for name in known {
        if name == requested {
            return Some(name.clone());
        }
    }
    let lower = requested.to_lowercase();
    for name in known {
        if name.to_lowercase() == lower {
            return Some(name.clone());
        }
    }
    for name in known {
        if similarity(&name.to_lowercase(), &lower) >= 0.72 {
            return Some(name.clone());
        }
    }
    None
}

/// 基于 Levenshtein 距离的相似度（difflib ratio 的近似）。
fn similarity(a: &str, b: &str) -> f64 {
    let x: Vec<char> = a.chars().collect();
    let y: Vec<char> = b.chars().collect();
    if x.is_empty() && y.is_empty() {
        return 1.0;
    }
    let max_len = x.len().max(y.len()) as f64;
    let dist = levenshtein(&x, &y) as f64;
    1.0 - dist / max_len
}

fn levenshtein(a: &[char], b: &[char]) -> usize {
    let (rows, cols) = (a.len() + 1, b.len() + 1);
    let mut prev: Vec<usize> = (0..cols).collect();
    let mut cur = vec![0usize; cols];
    for i in 1..rows {
        cur[0] = i;
        for j in 1..cols {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            cur[j] = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[cols - 1]
}

/// 宽松 JSON 解析：标准解析失败后做有限修复再试。
pub fn loose_json_parse(text: &str) -> Option<Value> {
    if let Ok(v) = serde_json::from_str::<Value>(text) {
        return Some(v);
    }
    let fixed = repair_json(text)?;
    serde_json::from_str::<Value>(&fixed).ok()
}

/// 对无法直接解析的 JSON 做少量修复：去尾逗号、单引号替换、补对象括号。
fn repair_json(text: &str) -> Option<String> {
    let s = text.trim();
    if s.is_empty() {
        return None;
    }
    let chars: Vec<char> = s.chars().collect();
    let mut no_trailing = String::with_capacity(s.len());
    for i in 0..chars.len() {
        if chars[i] == ',' {
            let mut j = i + 1;
            while j < chars.len() && chars[j].is_whitespace() {
                j += 1;
            }
            if j < chars.len() && (chars[j] == '}' || chars[j] == ']') {
                continue;
            }
        }
        no_trailing.push(chars[i]);
    }
    let quoted = if !no_trailing.contains('"') {
        no_trailing.replace('\'', "\"")
    } else {
        no_trailing
    };
    let t = quoted.trim().to_string();
    let t = if (t.starts_with('{') && t.ends_with('}'))
        || (t.starts_with('[') && t.ends_with(']'))
    {
        t
    } else {
        format!("{{{t}}}")
    };
    Some(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitter_extracts_tool_and_text() {
        let mut sp = ToolStreamSplitter::new(vec!["get_weather".to_string()]);
        let mut pieces = sp.feed("好的<tool>{\"name\":\"get_weather\",\"arguments\":{\"city\":\"北京\"}}</tool>还有");
        pieces.extend(sp.flush());
        let tools: Vec<&ParsedToolCall> = pieces
            .iter()
            .filter_map(|p| if let SplitPiece::Tool(t) = p { Some(t) } else { None })
            .collect();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "get_weather");
        assert!(tools[0].arguments_json.contains("北京"));
        let text: String = pieces
            .iter()
            .filter_map(|p| if let SplitPiece::Text(t) = p { Some(t.clone()) } else { None })
            .collect();
        assert_eq!(text, "好的还有");
    }

    #[test]
    fn splitter_handles_cross_chunk_tag() {
        let mut sp = ToolStreamSplitter::new(vec!["f".to_string()]);
        let a = sp.feed("abc<to");
        let b = sp.feed("ol>{\"name\":\"f\",\"arguments\":{}}</tool>xyz");
        let c = sp.flush();
        let all: Vec<SplitPiece> = [a, b, c].concat();
        let text: String = all
            .iter()
            .filter_map(|p| if let SplitPiece::Text(t) = p { Some(t.clone()) } else { None })
            .collect();
        assert_eq!(text, "abcxyz");
        let tools = all.iter().filter(|p| matches!(p, SplitPiece::Tool(_))).count();
        assert_eq!(tools, 1);
    }

    #[test]
    fn splitter_does_not_leak_open_tag_before_close_arrives() {
        let mut sp = ToolStreamSplitter::new(vec!["f".to_string()]);
        // 第一片在完整开标签处截断：标签不得作为正文泄漏。
        let a = sp.feed("abc<tool>");
        let b = sp.feed("{\"name\":\"f\",\"arguments\":{}}</tool>xyz");
        let c = sp.flush();
        let all: Vec<SplitPiece> = [a, b, c].concat();
        for piece in &all {
            if let SplitPiece::Text(t) = piece {
                assert!(!t.contains("<tool>"), "开标签泄漏为正文: {t:}");
            }
        }
        let text: String = all
            .iter()
            .filter_map(|p| if let SplitPiece::Text(t) = p { Some(t.clone()) } else { None })
            .collect();
        assert_eq!(text, "abcxyz");
        let tools = all.iter().filter(|p| matches!(p, SplitPiece::Tool(_))).count();
        assert_eq!(tools, 1);
    }

    #[test]
    fn fuzzy_name_match() {
        assert_eq!(match_tool_name("get-weather", &["get_weather".to_string()]).is_some(), true);
        assert_eq!(match_tool_name("totally_other", &["get_weather".to_string()]).is_none(), true);
    }

    #[test]
    fn loose_parse_repairs_trailing_comma() {
        let v = loose_json_parse("{\"a\":1,}");
        assert!(v.is_some());
    }
}
