# agentmore2api

`agentmore.chatglm.cn`（智谱清言 AgentMore · 网页端对话模式）反向网关。把网页端聊天封装成标准 API：

- **POST `/v1/chat/completions`** — OpenAI Chat Completions（流式 / 非流式）
- **POST `/v1/responses`** — OpenAI Responses（流式 / 非流式）
- **POST `/v1/messages`** — Anthropic Messages（流式 / 非流式）
- **POST `/v1/messages/count_tokens`** — Anthropic token 计数（本地估算）
- **GET `/v1/models`** — 模型列表
- **GET `/`** — 浏览器配置面板（运行后直接打开配置）

## 工作方式

1. 每次网关调用都会**新建一次上游对话**（`conversation_id` 留空，由上游自动建会话）；
2. 客户端传入的完整多轮上下文（含工具调用与工具结果）会被序列化为 Markdown，**以文件附件形式上传**（`POST /chatglm/productivity-api/file/chat_upload`），消息正文只携带任务说明；上传失败自动回退为内联文本；
3. 上游是 agentmore 的 `agent-api/chat/stream`（`chat_mode=chat_agent`，无沙箱，不需要上游 Read 工具读取附件，模型直接可见文件内容）；
4. 收到回复后本次会话即结束（可选自动删除上游会话），**下一次调用又是全新会话**；
5. 上游并发由信号量限制，**最高 3**（面板可调 1~3），超限排队而非报错。

### 工具调用桥

上游没有结构化工具调用通道，网关通过提示词约定让模型用文本输出调用块：

```
<tool>{"name": "<工具名>", "arguments": { ... }}</tool>
```

网关流式解析这些块并还原为协议原生的 `tool_calls`（OpenAI）/ `function_call`（Responses）/ `tool_use`（Anthropic）；客户端回传的工具结果会在下一次调用的上下文文件中渲染为 `[tool 结果]` 段落，形成多轮工具循环。工具名支持精确 / 忽略大小写 / 相似度 ≥ 0.72 的模糊匹配。

## 运行

```bash
# Windows
agentmore2api.exe --host 127.0.0.1 --port 8795 --config config.json
# Linux
./agentmore2api --host 0.0.0.0 --port 8795
```

首次运行会生成默认 `config.json`，浏览器打开 `http://127.0.0.1:8795/` 即为配置面板。

## 配置账号

两种方式（面板「账号凭据」页可在线维护）：

1. **refresh_token（推荐）**：浏览器登录 agentmore.chatglm.cn 后，DevTools → Application → Cookies → 复制 `chatglm_refresh_token`，粘贴进面板账号列表。令牌轮换会自动写回配置文件；支持多账号轮转。
2. **游客回退**：不配置账号时自动走 `POST /chatglm/user-api/guest/access` 免登录令牌（配置 `guest_fallback: true`，默认开启）。

X-Sign 签名密钥是官方网页端 JS 中的公开协议指纹常量，已内置默认值，可在面板覆盖。

## 主要配置项

| 字段 | 说明 | 默认 |
|---|---|---|
| `host` / `port` | 监听地址端口（改后需重启） | `127.0.0.1:8795` |
| `api_keys` | 本网关鉴权 Key（Bearer / x-api-key），留空不鉴权 | 空 |
| `panel_password` | 面板访问密码，留空不鉴权 | 空 |
| `max_concurrency` | 上游并发上限，硬性 1~3 | 3 |
| `accounts` | refresh_token 账号池 | 空 |
| `guest_fallback` | 无账号时游客回退 | true |
| `upstream.base_url` | 上游基址 | `https://agentmore.chatglm.cn` |
| `upstream.assistant_id` | 对话模式 assistant_id | 内置默认值 |
| `upstream.context_send_mode` | `file` 上传附件 / `inline` 内联 | file |
| `upstream.max_context_chars` | 上下文文件最大字符数 | 200000 |
| `upstream.delete_conversation` | 用完删除上游会话 | true |
| `upstream.default_effort` | 默认思考强度 low/high/max | high |
| `upstream.is_networking` | 上游联网开关 | true |
| `model_aliases` | 客户端模型名 → 上游 selected_model | 空 |
| `unknown_model_policy` | 未知模型映射策略 default / passthrough | default |

## 面板功能

- 运行状态：版本、并发占用、账号健康度（8 秒轮询）
- 在线修改全部配置并**热生效**（监听地址/端口除外）
- 连通性测试：**测试令牌**（逐账号刷新）与**测试对话**（一次最小上游往返）

## 构建

推送到 GitHub 后由 `.github/workflows/build.yml` 自动构建（不要本地构建时可直接下载 Actions 产物）：

- `agentmore2api-linux-x64.tar.gz`
- `agentmore2api-windows-x64.zip`

打 `v*` 标签或手动 workflow_dispatch 时会同时创建 Release。

## 调试

`debug_log: true` 时会把上游原始 SSE 行（截断）打印到 stderr，便于上游协议漂移时定位。所有请求与响应状态都会记录到控制台日志。

> 注意：这是面向个人自动化使用的网关工具，上游接口随官方前端版本可能变化；签名密钥、端点、请求体均已做成配置项，漂移时无需改代码。
