package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"github.com/tidwall/gjson"
)

func TestDecideCodexContinueDetectsTruncation(t *testing.T) {
	cfg := defaultPluginConfig()
	payload := []byte(`data: {"type":"response.completed","response":{"usage":{"output_tokens_details":{"reasoning_tokens":516}},"output":[{"type":"reasoning","encrypted_content":"enc-1"}]}}`)

	decision := decideCodexContinue(payload, cfg)
	if !decision.ShouldContinue {
		t.Fatalf("ShouldContinue = false, stop=%s", decision.StopReason)
	}
	if decision.N != 1 || decision.ReasoningToken != 516 {
		t.Fatalf("decision = %#v, want n=1 reasoning=516", decision)
	}
	if string(decision.ReasoningItem) != `{"type":"reasoning","encrypted_content":"enc-1"}` {
		t.Fatalf("ReasoningItem = %s", decision.ReasoningItem)
	}
}

func TestDecideCodexContinueRejectsMissingEncryptedContent(t *testing.T) {
	cfg := defaultPluginConfig()
	payload := []byte(`data: {"type":"response.completed","response":{"usage":{"output_tokens_details":{"reasoning_tokens":516}},"output":[{"type":"reasoning"}]}}`)

	decision := decideCodexContinue(payload, cfg)
	if decision.ShouldContinue {
		t.Fatal("ShouldContinue = true, want false")
	}
	if decision.StopReason != "no_encrypted_content" {
		t.Fatalf("StopReason = %q", decision.StopReason)
	}
}

func TestDecideCodexContinueRejectsNonFormulaReasoningTokens(t *testing.T) {
	cfg := defaultPluginConfig()
	payload := []byte(`data: {"type":"response.completed","response":{"usage":{"output_tokens_details":{"reasoning_tokens":517}},"output":[{"type":"reasoning","encrypted_content":"enc-1"}]}}`)

	decision := decideCodexContinue(payload, cfg)
	if decision.ShouldContinue {
		t.Fatal("ShouldContinue = true, want false")
	}
	if decision.StopReason != "not_truncated" {
		t.Fatalf("StopReason = %q", decision.StopReason)
	}
}

func TestAppendCodexContinueInput(t *testing.T) {
	body := []byte(`{"model":"gpt-5.5","previous_response_id":"resp_old","input":[{"role":"user","content":"hi"}]}`)
	reasoning := []byte(`{"type":"reasoning","encrypted_content":"enc-1"}`)

	updated, ok := appendCodexContinueInput(body, reasoning, "Continue thinking...")
	if !ok {
		t.Fatal("appendCodexContinueInput ok = false")
	}
	if !json.Valid(updated) {
		t.Fatalf("updated body is invalid JSON: %s", updated)
	}
	input := gjson.GetBytes(updated, "input")
	if got := len(input.Array()); got != 3 {
		t.Fatalf("input length = %d, want 3; body=%s", got, updated)
	}
	if got := input.Array()[1].Get("encrypted_content").String(); got != "enc-1" {
		t.Fatalf("reasoning encrypted_content = %q", got)
	}
	if got := input.Array()[2].Get("phase").String(); got != "commentary" {
		t.Fatalf("marker phase = %q", got)
	}
	if gjson.GetBytes(updated, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id still exists: %s", updated)
	}
}

func TestMatchesConfiguredModel(t *testing.T) {
	cfg := defaultPluginConfig()
	if !matchesConfiguredModel("gpt-5.5", nil, cfg) {
		t.Fatal("gpt-5.5 should match default gpt-5 prefix")
	}
	if !matchesConfiguredModel("codex/gpt-5.5", nil, cfg) {
		t.Fatal("codex/gpt-5.5 should match base model")
	}
	if matchesConfiguredModel("claude-opus-4-7", nil, cfg) {
		t.Fatal("claude-opus-4-7 should not match")
	}
}

func TestCodexExecutionModel(t *testing.T) {
	if got := codexExecutionModel("gpt-5.5"); got != "codex/gpt-5.5" {
		t.Fatalf("codexExecutionModel() = %q", got)
	}
	if got := codexExecutionModel("codex/gpt-5.5"); got != "codex/gpt-5.5" {
		t.Fatalf("codexExecutionModel() = %q", got)
	}
	if got := codexExecutionModel("other/gpt-5.5"); got != "codex/gpt-5.5" {
		t.Fatalf("codexExecutionModel() = %q", got)
	}
}

func TestCodexSSEEventHeaderPairing(t *testing.T) {
	event := []byte("event: response.completed")
	data := []byte(`data: {"type":"response.completed","response":{"usage":{"output_tokens_details":{"reasoning_tokens":516}}}}`)
	combined := prependPendingEvent(event, data)
	if got := string(combined); got != string(event)+"\n"+string(data) {
		t.Fatalf("combined = %q", got)
	}
	if got := codexEventType(combined); got != "response.completed" {
		t.Fatalf("codexEventType() = %q", got)
	}
	if !isCodexSSEEventHeader(event) {
		t.Fatal("event header was not detected")
	}
	if isCodexSSEEventHeader(combined) {
		t.Fatal("combined event+data frame should not be treated as header-only")
	}
	if shouldEmitRoundEventHeader([]byte("event: response.created"), 1) {
		t.Fatal("second round response.created header should be suppressed")
	}
	if !shouldEmitRoundEventHeader([]byte("event: response.completed"), 1) {
		t.Fatal("second round response.completed header should be emitted with final data")
	}
}

func TestContinueMetricsRecordNoCompleted(t *testing.T) {
	previous := continueMetrics
	continueMetrics = newContinueMetricsStore()
	defer func() { continueMetrics = previous }()

	cfg := defaultPluginConfig()
	metric := newContinueRequestMetric(pluginapi.ExecutorRequest{}, cfg)
	metric.markNoCompleted()
	metric.finish(fmt.Errorf("missing completed"))

	snapshot := continueMetrics.snapshot(cfg)
	if snapshot.RequestsStarted != 1 || snapshot.RequestsFinished != 1 || snapshot.RequestsFailed != 1 {
		t.Fatalf("snapshot counts = started %d finished %d failed %d", snapshot.RequestsStarted, snapshot.RequestsFinished, snapshot.RequestsFailed)
	}
	if snapshot.NoCompleted != 1 {
		t.Fatalf("NoCompleted = %d", snapshot.NoCompleted)
	}
	if snapshot.FinalStopReasons["no_completed_payload"] != 1 {
		t.Fatalf("final no_completed_payload = %d", snapshot.FinalStopReasons["no_completed_payload"])
	}
}

func TestCodexTranslatedStreamUsesDataOnlyPayloads(t *testing.T) {
	event := []byte("event: response.output_text.delta")
	data := []byte(`data: {"type":"response.output_text.delta","delta":"x"}`)
	if got := string(codexRoundEmitPayload(event, data, false)); got != string(data) {
		t.Fatalf("translated emit payload = %q", got)
	}
	combined := append(append([]byte(nil), event...), '\n')
	combined = append(combined, data...)
	if got := string(codexDataOnlyPayload(combined)); got != string(data) {
		t.Fatalf("data-only combined payload = %q", got)
	}
	if shouldEmitCodexEventHeaders(pluginapi.ExecutorRequest{Format: "codex", SourceFormat: "openai"}) {
		t.Fatal("openai-sourced codex output should omit event headers for host translation")
	}
	if !shouldEmitCodexEventHeaders(pluginapi.ExecutorRequest{Format: "codex", SourceFormat: "codex"}) {
		t.Fatal("codex-sourced codex output should keep event headers")
	}
	if shouldEmitCodexEventHeaders(pluginapi.ExecutorRequest{Format: "codex", SourceFormat: "codex", Metadata: map[string]any{"request_path": "/v1/chat/completions"}}) {
		t.Fatal("chat path should omit event headers even after source format is normalized to codex")
	}
	if !shouldEmitCodexEventHeaders(pluginapi.ExecutorRequest{Format: "codex", SourceFormat: "codex", Metadata: map[string]any{"request_path": "/backend-api/codex/responses"}}) {
		t.Fatal("native codex path should keep event headers")
	}
	if !shouldEmitCodexEventHeaders(pluginapi.ExecutorRequest{Format: "openai-response", SourceFormat: "openai-response"}) {
		t.Fatal("openai-response output should keep event headers")
	}
}
