# cpa-continue-thinking

[中文文档](./README_CN.md)

CLIProxyAPI (CPA) plugin that adds Codex "continue-thinking" support. When a Codex response's `reasoning_tokens` matches the truncation formula `518*n - 2`, the plugin appends the reasoning output and hidden commentary, deletes `previous_response_id`, and re-requests Codex — folding multiple upstream rounds into a single downstream stream.

Supports all downstream protocols: OpenAI Responses, OpenAI Chat Completions, Claude Messages, Gemini, Interactions, and native Codex.

## How it works

1. CPA routes eligible Codex streaming requests through this plugin
2. The plugin proxies SSE frames, buffering the final `response.completed` event
3. If `reasoning_tokens == 518*n - 2` (where min-n ≤ n ≤ max-n), the reasoning item is re-appended and a new upstream round begins
4. If the formula doesn't match, the response is passed through unmodified
5. Built-in in-memory aggregate metrics dashboard (no prompts/responses/keys collected)

## Build

Requires Go 1.26+, zig CC (for cross-compilation), and the CLIProxyAPI SDK module.

```bash
# Linux arm64 (e.g., Docker on Apple Silicon)
CGO_ENABLED=1 GOOS=linux GOARCH=arm64 \
  CC="zig cc -target aarch64-linux-gnu" \
  go build -buildmode=c-shared -o codex-continue-thinking.so .

# Linux amd64 (production x86_64)
CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
  CC="zig cc -target x86_64-linux-gnu" \
  go build -buildmode=c-shared -o codex-continue-thinking.so .
```

## Configuration

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

### Config fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| enabled | bool | true | When false, the router declines all requests |
| fail-open | bool | true | On continue-specific failures, emit current completed event instead of failing the stream |
| models | array | [] | Exact model names to route through this plugin |
| model-prefixes | array | ["gpt-5", "codex"] | Lowercase model prefixes routed when Codex auth is available |
| source-formats | array | [all] | Client protocol formats eligible for routing |
| truncation-step | int | 518 | Reasoning token step for truncation detection |
| max-continue | int | 3 | Maximum additional upstream Codex rounds |
| min-n | int | 1 | Minimum n for reasoning_tokens = step*n - 2 |
| max-n | int | 6 | Maximum n for reasoning_tokens = step*n - 2 |
| marker-text | string | "Continue thinking..." | Hidden commentary text appended before continue rounds |
| log-decisions | bool | false | When true, writes per-round continue decisions to host logs |

## Metrics

The plugin exposes an in-memory aggregate metrics dashboard:

- **JSON**: `GET /v0/resource/plugins/codex-continue-thinking/metrics?format=json`
- **HTML**: `GET /v0/resource/plugins/codex-continue-thinking/metrics`

The dashboard shows request counts, continue rates, stop reasons, reasoning token distribution, and rolling 24-hour buckets. No prompts, responses, API keys, accounts, or auth identifiers are collected.

## Community

Friendly link: [LINUX DO](https://linux.do/) — a Chinese developer community.

This plugin was inspired by the community discussion and solution notes around Codex continue-thinking: [解决 Codex 516 的截断降智](https://linux.do/t/topic/2504036?u=ricksanchezzz12301).

## License

MIT
