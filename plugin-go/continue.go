package main

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type continueDecision struct {
	ShouldContinue bool
	ReasoningItem  []byte
	ReasoningToken int64
	N              int64
	StopReason     string
}

func decideCodexContinue(completedPayload []byte, cfg pluginConfig) continueDecision {
	data := codexEventData(completedPayload)
	if len(data) == 0 || !json.Valid(data) {
		return continueDecision{StopReason: "invalid_completed_event"}
	}
	root := gjson.ParseBytes(data)
	if strings.TrimSpace(root.Get("type").String()) != "response.completed" {
		return continueDecision{StopReason: "not_completed"}
	}
	reasoningTokens := root.Get("response.usage.output_tokens_details.reasoning_tokens")
	if !reasoningTokens.Exists() {
		return continueDecision{StopReason: "no_reasoning_tokens"}
	}
	step := int64(cfg.TruncationStep)
	if step <= 0 {
		step = 518
	}
	tokens := reasoningTokens.Int()
	if tokens < 0 || (tokens+2)%step != 0 {
		return continueDecision{ReasoningToken: tokens, StopReason: "not_truncated"}
	}
	n := (tokens + 2) / step
	if n < int64(cfg.MinN) || n > int64(cfg.MaxN) {
		return continueDecision{ReasoningToken: tokens, N: n, StopReason: "truncation_n_out_of_range"}
	}
	reasoningItem := codexCompletedReasoningItem(root)
	if len(reasoningItem) == 0 {
		return continueDecision{ReasoningToken: tokens, N: n, StopReason: "no_encrypted_content"}
	}
	return continueDecision{ShouldContinue: true, ReasoningItem: reasoningItem, ReasoningToken: tokens, N: n}
}

func codexCompletedReasoningItem(root gjson.Result) []byte {
	output := root.Get("response.output")
	if !output.IsArray() {
		return nil
	}
	for _, item := range output.Array() {
		if strings.TrimSpace(item.Get("type").String()) != "reasoning" {
			continue
		}
		if strings.TrimSpace(item.Get("encrypted_content").String()) == "" {
			continue
		}
		return []byte(item.Raw)
	}
	return nil
}

func appendCodexContinueInput(body []byte, reasoningItem []byte, markerText string) ([]byte, bool) {
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() || len(reasoningItem) == 0 || !json.Valid(reasoningItem) {
		return body, false
	}
	markerItem, ok := codexContinueMarkerItem(markerText)
	if !ok {
		return body, false
	}
	items := make([]string, 0, len(input.Array())+2)
	for _, item := range input.Array() {
		items = append(items, item.Raw)
	}
	items = append(items, string(reasoningItem), string(markerItem))
	updated, errSet := sjson.SetRawBytes(body, "input", []byte("["+strings.Join(items, ",")+"]"))
	if errSet != nil {
		return body, false
	}
	updated, _ = sjson.DeleteBytes(updated, "previous_response_id")
	return updated, true
}

func codexContinueMarkerItem(markerText string) ([]byte, bool) {
	markerText = strings.TrimSpace(markerText)
	if markerText == "" {
		markerText = "Continue thinking..."
	}
	item := map[string]any{
		"type":  "message",
		"role":  "assistant",
		"phase": "commentary",
		"content": []map[string]string{
			{"type": "output_text", "text": markerText},
		},
	}
	raw, errMarshal := json.Marshal(item)
	if errMarshal != nil {
		return nil, false
	}
	return raw, true
}

func isCodexCompletedEvent(payload []byte) bool {
	return codexEventType(payload) == "response.completed"
}

func codexEventType(payload []byte) string {
	data := codexEventData(payload)
	if len(data) == 0 {
		return ""
	}
	return strings.TrimSpace(gjson.GetBytes(data, "type").String())
}

func codexEventData(payload []byte) []byte {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return nil
	}
	for _, line := range bytes.Split(payload, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(line[len("data:"):])
		if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
			return nil
		}
		return data
	}
	if json.Valid(payload) {
		return payload
	}
	return nil
}
