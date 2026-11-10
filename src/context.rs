//! 把协议层的规范消息序列化为「上下文文件」（Markdown）。
//!
//! 上游对话模式的入口是单条 user 消息 + 可选文件附件。这里把完整多轮历史
//! （含工具调用与工具结果）压平成一个 Markdown 文档作为附件上传，模型无沙箱、
//! 附件内容直接可见；消息正文只放任务说明与工具调用格式约定。

use crate::proto::{CanonRequest, Role};

/// 构建上下文文档。超过 max_chars 时做中段截断（保头保尾）。
pub fn build_context_document(canon: &CanonRequest, max_chars: usize) -> String {
    let mut sections: Vec<String> = Vec::new();

    for msg in &canon.messages {
        let head = match msg.role {
            Role::System => "[system]".to_string(),
            Role::User => "[user]".to_string(),
            Role::Assistant => "[assistant]".to_string(),
            Role::Tool => format!(
                "[tool 结果 call_id={} name={}]",
                msg.tool_call_id, msg.name
            ),
        };
        let mut body = String::new();
        body.push_str(&msg.content);
        if msg.role == Role::Assistant && !msg.tool_calls.is_empty() {
            if !msg.content.is_empty() {
                body.push('\n');
            }
            for call in &msg.tool_calls {
                body.push_str(&format!(
                    "> 调用工具 {name} (call_id={id}) 入参: {args}\n",
                    name = call.name,
                    id = call.id,
                    args = call.arguments
                ));
            }
        }
        sections.push(format!("## {head}\n{body}", head = head, body = body.trim_end()));
    }

    let mut doc = String::new();
    doc.push_str("# 对话上下文\n\n");
    doc.push_str("> 本文件是一次 API 调用传入的完整对话历史（可能包含多轮）。请通读全部内容。\n\n");
    doc.push_str("---\n\n");
    doc.push_str(&sections.join("\n\n"));
    doc.push_str("\n\n---\n\n");
    doc.push_str("## 下一步\n");
    doc.push_str("请基于以上完整对话历史，以 assistant 身份生成下一条回复：\n");
    doc.push_str("1. 直接输出回复内容本身，不要复述历史，不要解释本文件的机制。\n");
    doc.push_str("2. 最后一条消息是 [user] 时，回复该消息；是 [tool 结果] 时，基于结果继续。\n");
    doc.push_str("3. 若消息正文中有 [TOOL USE INSTRUCTIONS] 且本次确实需要调用工具，按其约定的 <tool> 块格式输出；否则正常作答。\n");

    truncate_middle(&doc, max_chars)
}

/// 超长时截断中段：保留开头 10% 与结尾 88%，并插入省略标记。
fn truncate_middle(doc: &str, max_chars: usize) -> String {
    let chars: Vec<char> = doc.chars().collect();
    if chars.len() <= max_chars {
        return doc.to_string();
    }
    let head = max_chars / 10;
    let tail = max_chars - head - 64; // 预留标记长度
    let mut out = String::with_capacity(max_chars + 128);
    out.extend(chars[..head].iter());
    out.push_str("\n\n[……中间过长内容已截断……]\n\n");
    out.extend(chars[chars.len() - tail..].iter());
    out
}
