package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

type rpcStreamEmitRequest struct {
	StreamID string `json:"stream_id"`
	Payload  []byte `json:"payload,omitempty"`
	Error    string `json:"error,omitempty"`
}

type rpcStreamCloseRequest struct {
	StreamID string `json:"stream_id"`
	Error    string `json:"error,omitempty"`
}

type roundResult struct {
	CompletedPayload []byte
	Emitted          bool
}

func executeStream(raw []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	return startExecutorStream(req, runCodexContinueStream, closePluginStream)
}

type streamRunner func(context.Context, pluginapi.ExecutorRequest, string, string) error

type pluginStreamCloser func(string, string)

func startExecutorStream(req rpcExecutorRequest, runner streamRunner, closeStream pluginStreamCloser) ([]byte, error) {
	streamID := strings.TrimSpace(req.StreamID)
	if streamID == "" {
		return errorEnvelope("executor_error", "stream_id is required for executor.execute_stream"), nil
	}
	if runner == nil {
		return errorEnvelope("executor_error", "stream runner is unavailable"), nil
	}
	if closeStream == nil {
		closeStream = func(string, string) {}
	}
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				closeStream(streamID, fmt.Sprintf("stream panic: %v", recovered))
			}
		}()
		errRun := runner(backgroundContext(), req.ExecutorRequest, req.HostCallbackID, streamID)
		if errRun != nil {
			closeStream(streamID, errRun.Error())
			return
		}
		closeStream(streamID, "")
	}()
	return okEnvelope(map[string]any{
		"headers": http.Header{"Content-Type": []string{"text/event-stream"}},
	})
}

func runCodexContinueStream(ctx context.Context, exec pluginapi.ExecutorRequest, hostCallbackID, pluginStreamID string) (errRun error) {
	cfg := loadedConfig()
	metric := newContinueRequestMetric(exec, cfg)
	defer func() {
		metric.finish(errRun)
	}()

	body := cloneBytes(exec.Payload)
	if len(body) == 0 {
		body = cloneBytes(exec.OriginalRequest)
	}
	if len(body) == 0 {
		return fmt.Errorf("codex continue stream: empty codex request body")
	}

	continues := 0
	var lastCompleted []byte
	for round := 0; ; round++ {
		result, errRound := runCodexRound(ctx, exec, hostCallbackID, pluginStreamID, body, round)
		if errRound != nil {
			if cfg.FailOpen && len(lastCompleted) > 0 && !result.Emitted {
				metric.markFailOpen()
				return emitPluginStreamChunk(pluginStreamID, lastCompleted)
			}
			return errRound
		}
		if len(result.CompletedPayload) == 0 {
			metric.markNoCompleted()
			if cfg.FailOpen && len(lastCompleted) > 0 && !result.Emitted {
				metric.markFailOpen()
				return emitPluginStreamChunk(pluginStreamID, lastCompleted)
			}
			return fmt.Errorf("codex continue stream: upstream ended without response.completed")
		}
		lastCompleted = cloneBytes(result.CompletedPayload)

		decision := decideCodexContinue(result.CompletedPayload, cfg)
		metric.recordDecision(decision)
		if cfg.LogDecisions {
			logContinueDecision(hostCallbackID, round, continues, cfg.MaxContinue, decision)
		}
		if !decision.ShouldContinue || continues >= cfg.MaxContinue {
			return emitPluginStreamChunk(pluginStreamID, result.CompletedPayload)
		}
		nextBody, ok := appendCodexContinueInput(body, decision.ReasoningItem, cfg.MarkerText)
		if !ok {
			metric.markAppendFailed()
			if cfg.FailOpen {
				metric.markFailOpen()
				return emitPluginStreamChunk(pluginStreamID, result.CompletedPayload)
			}
			return fmt.Errorf("codex continue stream: failed to append continue input")
		}
		body = nextBody
		continues++
	}
}

func runCodexRound(ctx context.Context, exec pluginapi.ExecutorRequest, hostCallbackID, pluginStreamID string, body []byte, round int) (roundResult, error) {
	raw, errCall := callHost(pluginabi.MethodHostModelExecuteStream, hostModelExecutionRequest{
		HostModelExecutionRequest: pluginapi.HostModelExecutionRequest{
			EntryProtocol: "codex",
			ExitProtocol:  "codex",
			Model:         codexExecutionModel(exec.Model),
			Stream:        true,
			Body:          body,
			Headers:       cloneHeader(exec.Headers),
			Query:         exec.Query,
			Alt:           exec.Alt,
		},
		HostCallbackID: hostCallbackID,
	})
	if errCall != nil {
		return roundResult{}, errCall
	}
	var resp pluginapi.HostModelStreamResponse
	if errDecode := json.Unmarshal(raw, &resp); errDecode != nil {
		return roundResult{}, errDecode
	}
	if resp.StatusCode >= 400 {
		_ = closeHostModelStream(resp.StreamID)
		return roundResult{}, fmt.Errorf("host model status %d", resp.StatusCode)
	}
	if strings.TrimSpace(resp.StreamID) == "" {
		return roundResult{}, fmt.Errorf("host model stream: empty stream_id")
	}
	defer func() { _ = closeHostModelStream(resp.StreamID) }()

	var result roundResult
	var pendingEvent []byte
	emitEventHeaders := shouldEmitCodexEventHeaders(exec)
	for {
		chunkRaw, errRead := callHost(pluginabi.MethodHostModelStreamRead, pluginapi.HostModelStreamReadRequest{StreamID: resp.StreamID})
		if errRead != nil {
			return result, errRead
		}
		var chunk pluginapi.HostModelStreamReadResponse
		if errDecode := json.Unmarshal(chunkRaw, &chunk); errDecode != nil {
			return result, errDecode
		}
		if chunk.Error != "" {
			return result, fmt.Errorf("%s", chunk.Error)
		}
		if len(chunk.Payload) > 0 {
			payload := chunk.Payload
			if isCodexSSEEventHeader(payload) {
				if emitEventHeaders && len(pendingEvent) > 0 && shouldEmitRoundEventHeader(pendingEvent, round) {
					if errEmit := emitPluginStreamChunk(pluginStreamID, pendingEvent); errEmit != nil {
						return result, errEmit
					}
					result.Emitted = true
				}
				pendingEvent = bytes.Clone(payload)
			} else {
				emitPayload := codexRoundEmitPayload(pendingEvent, payload, emitEventHeaders)
				pendingEvent = nil
				if isCodexCompletedEvent(payload) {
					result.CompletedPayload = emitPayload
				} else if shouldEmitRoundPayload(payload, round) {
					if errEmit := emitPluginStreamChunk(pluginStreamID, emitPayload); errEmit != nil {
						return result, errEmit
					}
					result.Emitted = true
				}
			}
		}
		if chunk.Done {
			break
		}
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}
	}
	if emitEventHeaders && len(pendingEvent) > 0 && shouldEmitRoundEventHeader(pendingEvent, round) {
		if errEmit := emitPluginStreamChunk(pluginStreamID, pendingEvent); errEmit != nil {
			return result, errEmit
		}
		result.Emitted = true
	}
	return result, nil
}

func codexExecutionModel(model string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(model), "codex/") {
		return model
	}
	if slash := strings.LastIndex(model, "/"); slash >= 0 && slash < len(model)-1 {
		model = model[slash+1:]
	}
	return "codex/" + model
}

func shouldEmitRoundPayload(payload []byte, round int) bool {
	if round <= 0 {
		return true
	}
	switch codexEventType(payload) {
	case "response.created", "response.in_progress", "response.queued":
		return false
	default:
		return true
	}
}

func shouldEmitCodexEventHeaders(exec pluginapi.ExecutorRequest) bool {
	format := strings.ToLower(strings.TrimSpace(exec.Format))
	if format != "codex" {
		return true
	}
	sourceFormat := strings.ToLower(strings.TrimSpace(exec.SourceFormat))
	if sourceFormat != "" && sourceFormat != "codex" {
		return false
	}
	path := executorRequestPath(exec)
	if path == "" {
		return true
	}
	return strings.Contains(path, "/backend-api/codex/") || strings.HasSuffix(path, "/responses")
}

func executorRequestPath(exec pluginapi.ExecutorRequest) string {
	if exec.Metadata == nil {
		return ""
	}
	raw := exec.Metadata["request_path"]
	if raw == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(fmt.Sprint(raw)))
}

func codexRoundEmitPayload(eventPayload, dataPayload []byte, emitEventHeaders bool) []byte {
	if emitEventHeaders {
		return prependPendingEvent(eventPayload, dataPayload)
	}
	return codexDataOnlyPayload(dataPayload)
}

func codexDataOnlyPayload(payload []byte) []byte {
	payload = bytes.TrimLeft(payload, " \t\r\n")
	if bytes.HasPrefix(payload, []byte("data:")) {
		return cloneBytes(payload)
	}
	if idx := bytes.Index(payload, []byte("\ndata:")); idx >= 0 {
		return cloneBytes(bytes.TrimLeft(payload[idx+1:], " \t\r\n"))
	}
	return cloneBytes(payload)
}

func shouldEmitRoundEventHeader(payload []byte, round int) bool {
	if round <= 0 {
		return true
	}
	switch codexSSEEventHeaderType(payload) {
	case "response.created", "response.in_progress", "response.queued":
		return false
	default:
		return true
	}
}

func isCodexSSEEventHeader(payload []byte) bool {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 || bytes.Contains(payload, []byte("\ndata:")) || bytes.HasPrefix(payload, []byte("data:")) {
		return false
	}
	return codexSSEEventHeaderType(payload) != ""
}

func codexSSEEventHeaderType(payload []byte) string {
	for _, line := range bytes.Split(bytes.TrimSpace(payload), []byte("\n")) {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("event:")) {
			return strings.TrimSpace(string(line[len("event:"):]))
		}
	}
	return ""
}

func prependPendingEvent(eventPayload, dataPayload []byte) []byte {
	dataPayload = cloneBytes(dataPayload)
	if len(eventPayload) == 0 {
		return dataPayload
	}
	out := make([]byte, 0, len(eventPayload)+1+len(dataPayload))
	out = append(out, bytes.TrimRight(eventPayload, "\r\n")...)
	out = append(out, '\n')
	out = append(out, dataPayload...)
	return out
}

func emitPluginStreamChunk(streamID string, payload []byte) error {
	if strings.TrimSpace(streamID) == "" {
		return fmt.Errorf("plugin stream id is required")
	}
	_, errCall := callHost(pluginabi.MethodHostStreamEmit, rpcStreamEmitRequest{
		StreamID: streamID,
		Payload:  payload,
	})
	return errCall
}

func closePluginStream(streamID, errMsg string) {
	if strings.TrimSpace(streamID) == "" {
		return
	}
	_, _ = callHost(pluginabi.MethodHostStreamClose, rpcStreamCloseRequest{
		StreamID: streamID,
		Error:    strings.TrimSpace(errMsg),
	})
}

func closeHostModelStream(streamID string) error {
	if strings.TrimSpace(streamID) == "" {
		return nil
	}
	_, errCall := callHost(pluginabi.MethodHostModelStreamClose, pluginapi.HostModelStreamCloseRequest{StreamID: streamID})
	return errCall
}

func logContinueDecision(hostCallbackID string, round, continues, maxContinue int, decision continueDecision) {
	stopReason := strings.TrimSpace(decision.StopReason)
	if stopReason == "" && decision.ShouldContinue {
		stopReason = "continue"
	}
	_, _ = callHost(pluginabi.MethodHostLog, map[string]any{
		"host_callback_id": hostCallbackID,
		"level":            "info",
		"message": fmt.Sprintf(
			"codex_continue_decision round=%d continues=%d max_continue=%d should_continue=%t reasoning_tokens=%d n=%d stop_reason=%s",
			round,
			continues,
			maxContinue,
			decision.ShouldContinue,
			decision.ReasoningToken,
			decision.N,
			stopReason,
		),
	})
}

func cloneBytes(src []byte) []byte {
	return bytes.Clone(src)
}

func cloneHeader(src http.Header) http.Header {
	if src == nil {
		return nil
	}
	dst := make(http.Header, len(src))
	for key, values := range src {
		dst[key] = append([]string(nil), values...)
	}
	return dst
}
