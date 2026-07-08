package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);

static const cliproxy_host_api* stored_host;

static void store_host_api(const cliproxy_host_api* host) {
	stored_host = host;
}

static int call_host_api(const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	if (stored_host == NULL || stored_host->call == NULL) {
		return 1;
	}
	return stored_host->call(stored_host->host_ctx, method, request, request_len, response);
}

static void free_host_buffer(void* ptr, size_t len) {
	if (stored_host != NULL && stored_host->free_buffer != NULL && ptr != NULL) {
		stored_host->free_buffer(ptr, len);
	}
}
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"github.com/tidwall/gjson"
	"gopkg.in/yaml.v3"
)

const pluginIdentifier = "codex-continue-thinking"

var currentConfig atomic.Value

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type lifecycleRequest struct {
	ConfigYAML []byte `json:"config_yaml"`
}

type pluginConfig struct {
	Enabled        bool     `yaml:"enabled"`
	FailOpen       bool     `yaml:"fail-open"`
	Models         []string `yaml:"models"`
	ModelPrefixes  []string `yaml:"model-prefixes"`
	SourceFormats  []string `yaml:"source-formats"`
	TruncationStep int      `yaml:"truncation-step"`
	MaxContinue    int      `yaml:"max-continue"`
	MinN           int      `yaml:"min-n"`
	MaxN           int      `yaml:"max-n"`
	MarkerText     string   `yaml:"marker-text"`
	LogDecisions   bool     `yaml:"log-decisions"`
}

type registration struct {
	SchemaVersion uint32                 `json:"schema_version"`
	Metadata      pluginapi.Metadata     `json:"metadata"`
	Capabilities  registrationCapability `json:"capabilities"`
}

type registrationCapability struct {
	ModelRouter           bool     `json:"model_router"`
	Executor              bool     `json:"executor"`
	ExecutorModelScope    string   `json:"executor_model_scope"`
	ExecutorInputFormats  []string `json:"executor_input_formats"`
	ExecutorOutputFormats []string `json:"executor_output_formats"`
	ManagementAPI         bool     `json:"management_api"`
}

type rpcExecutorRequest struct {
	pluginapi.ExecutorRequest
	StreamID       string `json:"stream_id,omitempty"`
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcModelRouteRequest struct {
	pluginapi.ModelRouteRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type hostModelExecutionRequest struct {
	pluginapi.HostModelExecutionRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return 1
	}
	C.store_host_api(host)
	plugin.abi_version = C.uint32_t(pluginabi.ABIVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeResponse(response, errorEnvelope("invalid_method", "method is required"))
		return 1
	}
	var requestBytes []byte
	if request != nil && requestLen > 0 {
		requestBytes = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	raw, errHandle := handleMethod(C.GoString(method), requestBytes)
	if errHandle != nil {
		writeResponse(response, errorEnvelope("plugin_error", errHandle.Error()))
		return 1
	}
	writeResponse(response, raw)
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, _ C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {}

func handleMethod(method string, request []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		if errConfigure := configure(request); errConfigure != nil {
			return nil, errConfigure
		}
		return okEnvelope(pluginRegistration())
	case pluginabi.MethodModelRoute:
		return routeModel(request)
	case pluginabi.MethodExecutorIdentifier:
		return okEnvelope(map[string]string{"identifier": pluginIdentifier})
	case pluginabi.MethodExecutorExecute:
		return errorEnvelope("unsupported", "codex continue-thinking only supports streaming execution"), nil
	case pluginabi.MethodExecutorExecuteStream:
		return executeStream(request)
	case pluginabi.MethodExecutorCountTokens:
		return okEnvelope(pluginapi.ExecutorResponse{Payload: []byte(`{"input_tokens":0}`)})
	case pluginabi.MethodExecutorHTTPRequest:
		return errorEnvelope("unsupported", "executor.http_request is not implemented"), nil
	case pluginabi.MethodManagementRegister:
		return registerMetricsManagement()
	case pluginabi.MethodManagementHandle:
		return handleMetricsManagement(request)
	default:
		return errorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

func configure(raw []byte) error {
	var req lifecycleRequest
	if len(raw) > 0 {
		if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
			return errUnmarshal
		}
	}
	cfg := defaultPluginConfig()
	if len(req.ConfigYAML) > 0 {
		if errUnmarshal := yaml.Unmarshal(req.ConfigYAML, &cfg); errUnmarshal != nil {
			return errUnmarshal
		}
	}
	currentConfig.Store(normalizeConfig(cfg))
	return nil
}

func defaultPluginConfig() pluginConfig {
	return pluginConfig{
		Enabled:        true,
		FailOpen:       true,
		ModelPrefixes:  []string{"gpt-5", "codex"},
		SourceFormats:  []string{"openai", "openai-response", "claude", "gemini", "interactions", "codex"},
		TruncationStep: 518,
		MaxContinue:    3,
		MinN:           1,
		MaxN:           6,
		MarkerText:     "Continue thinking...",
	}
}

func normalizeConfig(cfg pluginConfig) pluginConfig {
	if cfg.TruncationStep <= 0 {
		cfg.TruncationStep = 518
	}
	if cfg.MaxContinue < 0 {
		cfg.MaxContinue = 0
	}
	if cfg.MinN <= 0 {
		cfg.MinN = 1
	}
	if cfg.MaxN <= 0 {
		cfg.MaxN = 6
	}
	if cfg.MaxN < cfg.MinN {
		cfg.MaxN = cfg.MinN
	}
	if strings.TrimSpace(cfg.MarkerText) == "" {
		cfg.MarkerText = "Continue thinking..."
	}
	cfg.Models = normalizeStringList(cfg.Models)
	cfg.ModelPrefixes = normalizeStringList(cfg.ModelPrefixes)
	cfg.SourceFormats = normalizeStringList(cfg.SourceFormats)
	if len(cfg.SourceFormats) == 0 {
		cfg.SourceFormats = defaultPluginConfig().SourceFormats
	}
	return cfg
}

func normalizeStringList(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		trimmed := strings.ToLower(strings.TrimSpace(value))
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		out = append(out, trimmed)
	}
	return out
}

func loadedConfig() pluginConfig {
	raw := currentConfig.Load()
	if cfg, ok := raw.(pluginConfig); ok {
		return cfg
	}
	return defaultPluginConfig()
}

func pluginRegistration() registration {
	return registration{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata: pluginapi.Metadata{
			Name:             pluginIdentifier,
			Version:          "0.1.0",
			Author:           "TwitterIsGood",
			GitHubRepository: "https://github.com/TwitterIsGood/cpa-continue-thinking",
			ConfigFields: []pluginapi.ConfigField{
				{Name: "enabled", Type: pluginapi.ConfigFieldTypeBoolean, Description: "When false, the router declines all requests and CPA uses its native Codex path."},
				{Name: "fail-open", Type: pluginapi.ConfigFieldTypeBoolean, Description: "When true, continue-specific failures emit the current completed event instead of failing the stream."},
				{Name: "models", Type: pluginapi.ConfigFieldTypeArray, Description: "Exact model names to route through this plugin."},
				{Name: "model-prefixes", Type: pluginapi.ConfigFieldTypeArray, Description: "Lowercase model prefixes routed through this plugin when Codex auth is available."},
				{Name: "source-formats", Type: pluginapi.ConfigFieldTypeArray, Description: "Client protocol formats eligible for routing."},
				{Name: "truncation-step", Type: pluginapi.ConfigFieldTypeInteger, Description: "Reasoning token step used for truncation detection."},
				{Name: "max-continue", Type: pluginapi.ConfigFieldTypeInteger, Description: "Maximum additional upstream Codex rounds."},
				{Name: "min-n", Type: pluginapi.ConfigFieldTypeInteger, Description: "Minimum n accepted for reasoning_tokens = truncation_step*n - 2."},
				{Name: "max-n", Type: pluginapi.ConfigFieldTypeInteger, Description: "Maximum n accepted for reasoning_tokens = truncation_step*n - 2."},
				{Name: "marker-text", Type: pluginapi.ConfigFieldTypeString, Description: "Hidden commentary text appended before a continue round."},
				{Name: "log-decisions", Type: pluginapi.ConfigFieldTypeBoolean, Description: "When true, writes per-round continue decisions to host logs."},
			},
		},
		Capabilities: registrationCapability{
			ModelRouter:           true,
			Executor:              true,
			ExecutorModelScope:    string(pluginapi.ExecutorModelScopeBoth),
			ExecutorInputFormats:  []string{"codex"},
			ExecutorOutputFormats: []string{"codex", "openai-response"},
			ManagementAPI:         true,
		},
	}
}

func routeModel(raw []byte) ([]byte, error) {
	var req rpcModelRouteRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	cfg := loadedConfig()
	if !cfg.Enabled || !req.Stream {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false})
	}
	if !containsString(cfg.SourceFormats, req.SourceFormat) {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false})
	}
	if !containsString(req.AvailableProviders, "codex") {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false})
	}
	if !matchesConfiguredModel(req.RequestedModel, req.Body, cfg) {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false})
	}
	return okEnvelope(pluginapi.ModelRouteResponse{
		Handled:    true,
		TargetKind: pluginapi.ModelRouteTargetSelf,
		Reason:     "codex_continue_thinking_enabled",
	})
}

func containsString(values []string, want string) bool {
	want = strings.ToLower(strings.TrimSpace(want))
	for _, value := range values {
		if strings.ToLower(strings.TrimSpace(value)) == want {
			return true
		}
	}
	return false
}

func matchesConfiguredModel(requestedModel string, body []byte, cfg pluginConfig) bool {
	candidates := modelCandidates(requestedModel, body)
	for _, candidate := range candidates {
		if containsString(cfg.Models, candidate) {
			return true
		}
		for _, prefix := range cfg.ModelPrefixes {
			if strings.HasPrefix(candidate, prefix) {
				return true
			}
		}
	}
	return false
}

func modelCandidates(requestedModel string, body []byte) []string {
	values := []string{requestedModel, gjson.GetBytes(body, "model").String()}
	out := make([]string, 0, len(values)*2)
	seen := make(map[string]bool, len(values)*2)
	for _, value := range values {
		model := normalizeModelName(value)
		if model == "" || seen[model] {
			continue
		}
		seen[model] = true
		out = append(out, model)
		if slash := strings.LastIndex(model, "/"); slash >= 0 && slash < len(model)-1 {
			base := model[slash+1:]
			if !seen[base] {
				seen[base] = true
				out = append(out, base)
			}
		}
	}
	return out
}

func normalizeModelName(model string) string {
	model = strings.ToLower(strings.TrimSpace(model))
	if idx := strings.Index(model, "("); idx > 0 {
		model = strings.TrimSpace(model[:idx])
	}
	return model
}

func callHost(method string, payload any) (json.RawMessage, error) {
	rawPayload, errMarshal := json.Marshal(payload)
	if errMarshal != nil {
		return nil, fmt.Errorf("marshal host callback %s: %w", method, errMarshal)
	}
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))

	var response C.cliproxy_buffer
	var requestPtr *C.uint8_t
	if len(rawPayload) > 0 {
		cPayload := C.CBytes(rawPayload)
		if cPayload == nil {
			return nil, fmt.Errorf("allocate host callback %s", method)
		}
		defer C.free(cPayload)
		requestPtr = (*C.uint8_t)(cPayload)
	}
	callCode := C.call_host_api(cMethod, requestPtr, C.size_t(len(rawPayload)), &response)
	var rawResponse []byte
	if response.ptr != nil && response.len > 0 {
		rawResponse = C.GoBytes(response.ptr, C.int(response.len))
	}
	if response.ptr != nil {
		C.free_host_buffer(response.ptr, response.len)
	}
	if len(rawResponse) == 0 {
		return nil, fmt.Errorf("host callback %s returned no response, code=%d", method, int(callCode))
	}

	var env envelope
	if errUnmarshal := json.Unmarshal(rawResponse, &env); errUnmarshal != nil {
		return nil, fmt.Errorf("decode host envelope %s: %w", method, errUnmarshal)
	}
	if !env.OK {
		if env.Error != nil {
			return nil, fmt.Errorf("%s: %s", env.Error.Code, env.Error.Message)
		}
		return nil, fmt.Errorf("host callback %s failed", method)
	}
	if callCode != 0 {
		return nil, fmt.Errorf("host callback %s returned code=%d", method, int(callCode))
	}
	return append(json.RawMessage(nil), env.Result...), nil
}

func okEnvelope(v any) ([]byte, error) {
	raw, errMarshal := json.Marshal(v)
	if errMarshal != nil {
		return nil, errMarshal
	}
	return json.Marshal(envelope{OK: true, Result: raw})
}

func errorEnvelope(code, message string) []byte {
	raw, _ := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Message: message}})
	return raw
}

func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil || len(raw) == 0 {
		return
	}
	ptr := C.CBytes(raw)
	if ptr == nil {
		return
	}
	response.ptr = ptr
	response.len = C.size_t(len(raw))
}

func backgroundContext() context.Context {
	return context.Background()
}
