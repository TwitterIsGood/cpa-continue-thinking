# cpa-continue-thinking

[English](./README.md)

CLIProxyAPI（CPA）插件，为 Codex 响应添加"继续思考"（continue-thinking）能力。当 Codex 响应的 `reasoning_tokens` 匹配截断公式 `518*n - 2` 时，插件会自动追加推理输出与隐藏注释标记，删除 `previous_response_id`，并重新请求 Codex —— 将多次上游 SSE 轮次折叠为一次下游流式响应。

支持所有下游协议：OpenAI Responses、OpenAI Chat Completions、Claude Messages、Gemini、Interactions 以及原生 Codex。

## 工作原理

1. CPA 将符合条件的 Codex 流式请求路由至此插件
2. 插件代理 SSE 帧，缓冲最终的 `response.completed` 事件
3. 若 `reasoning_tokens == 518*n - 2`（min-n ≤ n ≤ max-n），将推理内容重新追加并开始新一轮上游请求
4. 若公式不匹配，响应保持原样透传
5. 内置内存聚合指标面板（不收集任何提示词、响应、密钥或账号信息）

## 构建

需要 Go 1.26+、zig CC（用于交叉编译）以及 CLIProxyAPI SDK 模块。

```bash
# Linux arm64（例如 Apple Silicon 上的 Docker）
CGO_ENABLED=1 GOOS=linux GOARCH=arm64 \
  CC="zig cc -target aarch64-linux-gnu" \
  go build -buildmode=c-shared -o codex-continue-thinking.so .

# Linux amd64（生产环境 x86_64）
CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
  CC="zig cc -target x86_64-linux-gnu" \
  go build -buildmode=c-shared -o codex-continue-thinking.so .
```

## 配置

```yaml
plugins:
  enabled: true
  dir: /path/to/plugins
  configs:
    codex-continue-thinking:
      enabled: true
      priority: 1
      fail-open: true
      max-continue: 3
      truncation-step: 518
      min-n: 1
      max-n: 6
      marker-text: "Continue thinking..."
      model-prefixes:
        - "codex/"
      source-formats:
        - "openai"
        - "openai-response"
        - "claude"
        - "gemini"
        - "interactions"
        - "codex"
      models:
        - "gpt-5.5"
```

### 配置字段

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| enabled | bool | true | 为 false 时路由器拒绝所有请求 |
| fail-open | bool | true | 继续失败时，发送当前完成事件而不是让流失败 |
| models | array | [] | 需要路由至此插件的精确模型名称 |
| model-prefixes | array | ["gpt-5", "codex"] | 当 Codex 认证可用时按前缀匹配路由的模型 |
| source-formats | array | [全部] | 符合条件的客户端协议格式 |
| truncation-step | int | 518 | 截断检测的推理步长 |
| max-continue | int | 3 | 最大额外上游 Codex 轮次 |
| min-n | int | 1 | 公式 reasoning_tokens = step*n - 2 的最小 n |
| max-n | int | 6 | 公式 reasoning_tokens = step*n - 2 的最大 n |
| marker-text | string | "Continue thinking..." | 继续轮次前追加的隐藏注释文本 |
| log-decisions | bool | false | 为 true 时将每轮继续决策写入主机日志 |

## 指标面板

插件提供内存聚合指标面板：

- **JSON 接口**：`GET /v0/resource/plugins/codex-continue-thinking/metrics?format=json`
- **HTML 页面**：`GET /v0/resource/plugins/codex-continue-thinking/metrics`

面板展示请求计数、继续率、停止原因、推理 token 分布以及滚动 24 小时数据桶。不收集任何提示词、响应、API 密钥、账号或认证标识。

## 许可

MIT
