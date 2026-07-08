package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

const (
	metricsBucketSeconds = int64(300)
	metricsBucketCount   = 288
)

const (
	stopReasonNotTruncated = iota
	stopReasonContinue
	stopReasonNoEncryptedContent
	stopReasonInvalidCompletedEvent
	stopReasonNotCompleted
	stopReasonNoReasoningTokens
	stopReasonTruncationNOutOfRange
	stopReasonAppendFailed
	stopReasonNoCompletedPayload
	stopReasonError
	stopReasonUnknown
	stopReasonCount
)

var stopReasonNames = []string{
	"not_truncated",
	"continue",
	"no_encrypted_content",
	"invalid_completed_event",
	"not_completed",
	"no_reasoning_tokens",
	"truncation_n_out_of_range",
	"append_failed",
	"no_completed_payload",
	"error",
	"unknown",
}

var reasoningTokenBuckets = []int64{516, 1034, 1552, 2070, 2588, 3106, 3624}

var continueMetrics = newContinueMetricsStore()

type continueMetricsStore struct {
	startedAt time.Time

	started           atomic.Uint64
	finished          atomic.Uint64
	failed            atomic.Uint64
	active            atomic.Int64
	requestsContinued atomic.Uint64
	continueRounds    atomic.Uint64
	failOpen          atomic.Uint64
	appendFailed      atomic.Uint64
	noCompleted       atomic.Uint64

	continueRoundDist [5]atomic.Uint64
	finalStopReasons  [stopReasonCount]atomic.Uint64
	decisionReasons   [stopReasonCount]atomic.Uint64
	reasoningHits     [7]atomic.Uint64

	bucketMu sync.Mutex
	buckets  [metricsBucketCount]continueMetricsBucket
}

type continueMetricsBucket struct {
	StartUnix       int64  `json:"start_unix"`
	Start           string `json:"start"`
	Requests        uint64 `json:"requests"`
	Continued       uint64 `json:"continued"`
	ContinueRounds  uint64 `json:"continue_rounds"`
	Failed          uint64 `json:"failed"`
	FailOpen        uint64 `json:"fail_open"`
	AppendFailed    uint64 `json:"append_failed"`
	NoCompleted     uint64 `json:"no_completed"`
	MaxContinueHits uint64 `json:"max_continue_hits"`
}

type continueRequestMetric struct {
	continueRounds int
	maxContinue    int
	finalReason    string
	failOpen       bool
	appendFailed   bool
	noCompleted    bool
}

type continueMetricsSnapshot struct {
	GeneratedAt              string                     `json:"generated_at"`
	StartedAt                string                     `json:"started_at"`
	UptimeSeconds            int64                      `json:"uptime_seconds"`
	BucketSeconds            int64                      `json:"bucket_seconds"`
	RollingWindowBuckets     int                        `json:"rolling_window_buckets"`
	RequestsStarted          uint64                     `json:"requests_started"`
	RequestsFinished         uint64                     `json:"requests_finished"`
	RequestsFailed           uint64                     `json:"requests_failed"`
	ActiveRequests           int64                      `json:"active_requests"`
	RequestsWithContinue     uint64                     `json:"requests_with_continue"`
	ContinueRate             float64                    `json:"continue_rate"`
	TotalContinueRounds      uint64                     `json:"total_continue_rounds"`
	FailOpen                 uint64                     `json:"fail_open"`
	AppendFailed             uint64                     `json:"append_failed"`
	NoCompleted              uint64                     `json:"no_completed"`
	ContinueRoundsPerRequest map[string]uint64          `json:"continue_rounds_per_request"`
	FinalStopReasons         map[string]uint64          `json:"final_stop_reasons"`
	DecisionStopReasons      map[string]uint64          `json:"decision_stop_reasons"`
	ContinuedReasoningTokens map[string]uint64          `json:"continued_reasoning_tokens"`
	Buckets                  []continueMetricsBucket    `json:"buckets"`
	Config                   continueMetricsConfigState `json:"config"`
}

type continueMetricsConfigState struct {
	Enabled      bool `json:"enabled"`
	FailOpen     bool `json:"fail_open"`
	MaxContinue  int  `json:"max_continue"`
	LogDecisions bool `json:"log_decisions"`
}

func newContinueMetricsStore() *continueMetricsStore {
	return &continueMetricsStore{startedAt: time.Now().UTC()}
}

func newContinueRequestMetric(_ pluginapi.ExecutorRequest, cfg pluginConfig) *continueRequestMetric {
	continueMetrics.started.Add(1)
	continueMetrics.active.Add(1)
	return &continueRequestMetric{maxContinue: cfg.MaxContinue}
}

func (m *continueRequestMetric) recordDecision(decision continueDecision) {
	if m == nil {
		return
	}
	reason := continueDecisionStopReason(decision)
	continueMetrics.decisionReasons[stopReasonIndex(reason)].Add(1)
	m.finalReason = reason
	if decision.ShouldContinue {
		m.continueRounds++
		continueMetrics.continueRounds.Add(1)
		if idx := reasoningTokenIndex(decision.ReasoningToken); idx >= 0 {
			continueMetrics.reasoningHits[idx].Add(1)
		}
	}
}

func (m *continueRequestMetric) markFailOpen() {
	if m == nil {
		return
	}
	m.failOpen = true
}

func (m *continueRequestMetric) markAppendFailed() {
	if m == nil {
		return
	}
	m.appendFailed = true
	m.finalReason = "append_failed"
}

func (m *continueRequestMetric) markNoCompleted() {
	if m == nil {
		return
	}
	m.noCompleted = true
	m.finalReason = "no_completed_payload"
}

func (m *continueRequestMetric) finish(errRun error) {
	if m == nil {
		return
	}
	continueMetrics.active.Add(-1)
	continueMetrics.finished.Add(1)
	failed := errRun != nil
	if failed {
		continueMetrics.failed.Add(1)
		if strings.TrimSpace(m.finalReason) == "" {
			m.finalReason = "error"
		}
	}
	if m.failOpen {
		continueMetrics.failOpen.Add(1)
	}
	if m.appendFailed {
		continueMetrics.appendFailed.Add(1)
	}
	if m.noCompleted {
		continueMetrics.noCompleted.Add(1)
	}
	if m.continueRounds > 0 {
		continueMetrics.requestsContinued.Add(1)
	}
	continueMetrics.continueRoundDist[continueRoundDistIndex(m.continueRounds)].Add(1)
	finalReason := strings.TrimSpace(m.finalReason)
	if finalReason == "" {
		finalReason = "unknown"
	}
	continueMetrics.finalStopReasons[stopReasonIndex(finalReason)].Add(1)
	continueMetrics.recordBucket(m, failed)
}

func (s *continueMetricsStore) recordBucket(m *continueRequestMetric, failed bool) {
	if s == nil || m == nil {
		return
	}
	now := time.Now().UTC()
	bucketStart := now.Unix() / metricsBucketSeconds * metricsBucketSeconds
	slot := int((bucketStart / metricsBucketSeconds) % int64(metricsBucketCount))
	s.bucketMu.Lock()
	defer s.bucketMu.Unlock()
	bucket := &s.buckets[slot]
	if bucket.StartUnix != bucketStart {
		*bucket = continueMetricsBucket{
			StartUnix: bucketStart,
			Start:     time.Unix(bucketStart, 0).UTC().Format(time.RFC3339),
		}
	}
	bucket.Requests++
	if m.continueRounds > 0 {
		bucket.Continued++
	}
	bucket.ContinueRounds += uint64(m.continueRounds)
	if failed {
		bucket.Failed++
	}
	if m.failOpen {
		bucket.FailOpen++
	}
	if m.appendFailed {
		bucket.AppendFailed++
	}
	if m.noCompleted {
		bucket.NoCompleted++
	}
	if m.maxContinue > 0 && m.continueRounds >= m.maxContinue {
		bucket.MaxContinueHits++
	}
}

func (s *continueMetricsStore) snapshot(cfg pluginConfig) continueMetricsSnapshot {
	if s == nil {
		s = newContinueMetricsStore()
	}
	started := s.started.Load()
	continued := s.requestsContinued.Load()
	continueRate := 0.0
	if started > 0 {
		continueRate = float64(continued) / float64(started)
	}
	return continueMetricsSnapshot{
		GeneratedAt:          time.Now().UTC().Format(time.RFC3339),
		StartedAt:            s.startedAt.Format(time.RFC3339),
		UptimeSeconds:        int64(time.Since(s.startedAt).Seconds()),
		BucketSeconds:        metricsBucketSeconds,
		RollingWindowBuckets: metricsBucketCount,
		RequestsStarted:      started,
		RequestsFinished:     s.finished.Load(),
		RequestsFailed:       s.failed.Load(),
		ActiveRequests:       s.active.Load(),
		RequestsWithContinue: continued,
		ContinueRate:         continueRate,
		TotalContinueRounds:  s.continueRounds.Load(),
		FailOpen:             s.failOpen.Load(),
		AppendFailed:         s.appendFailed.Load(),
		NoCompleted:          s.noCompleted.Load(),
		ContinueRoundsPerRequest: map[string]uint64{
			"0":  s.continueRoundDist[0].Load(),
			"1":  s.continueRoundDist[1].Load(),
			"2":  s.continueRoundDist[2].Load(),
			"3":  s.continueRoundDist[3].Load(),
			"4+": s.continueRoundDist[4].Load(),
		},
		FinalStopReasons:         s.stopReasonSnapshot(&s.finalStopReasons),
		DecisionStopReasons:      s.stopReasonSnapshot(&s.decisionReasons),
		ContinuedReasoningTokens: s.reasoningHitSnapshot(),
		Buckets:                  s.bucketSnapshot(),
		Config: continueMetricsConfigState{
			Enabled:      cfg.Enabled,
			FailOpen:     cfg.FailOpen,
			MaxContinue:  cfg.MaxContinue,
			LogDecisions: cfg.LogDecisions,
		},
	}
}

func (s *continueMetricsStore) stopReasonSnapshot(values *[stopReasonCount]atomic.Uint64) map[string]uint64 {
	out := make(map[string]uint64, stopReasonCount)
	for idx, name := range stopReasonNames {
		out[name] = values[idx].Load()
	}
	return out
}

func (s *continueMetricsStore) reasoningHitSnapshot() map[string]uint64 {
	out := make(map[string]uint64, len(reasoningTokenBuckets))
	for idx, tokens := range reasoningTokenBuckets {
		out[jsonNumberString(tokens)] = s.reasoningHits[idx].Load()
	}
	return out
}

func (s *continueMetricsStore) bucketSnapshot() []continueMetricsBucket {
	nowBucket := time.Now().UTC().Unix() / metricsBucketSeconds * metricsBucketSeconds
	minBucket := nowBucket - int64(metricsBucketCount-1)*metricsBucketSeconds
	s.bucketMu.Lock()
	defer s.bucketMu.Unlock()
	out := make([]continueMetricsBucket, 0, metricsBucketCount)
	for _, bucket := range s.buckets {
		if bucket.StartUnix < minBucket || bucket.StartUnix == 0 {
			continue
		}
		out = append(out, bucket)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].StartUnix > out[j].StartUnix; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func continueDecisionStopReason(decision continueDecision) string {
	reason := strings.TrimSpace(decision.StopReason)
	if reason == "" && decision.ShouldContinue {
		return "continue"
	}
	if reason == "" {
		return "unknown"
	}
	return reason
}

func continueRoundDistIndex(rounds int) int {
	if rounds <= 0 {
		return 0
	}
	if rounds >= 4 {
		return 4
	}
	return rounds
}

func stopReasonIndex(reason string) int {
	switch strings.TrimSpace(reason) {
	case "not_truncated":
		return stopReasonNotTruncated
	case "continue":
		return stopReasonContinue
	case "no_encrypted_content":
		return stopReasonNoEncryptedContent
	case "invalid_completed_event":
		return stopReasonInvalidCompletedEvent
	case "not_completed":
		return stopReasonNotCompleted
	case "no_reasoning_tokens":
		return stopReasonNoReasoningTokens
	case "truncation_n_out_of_range":
		return stopReasonTruncationNOutOfRange
	case "append_failed":
		return stopReasonAppendFailed
	case "no_completed_payload":
		return stopReasonNoCompletedPayload
	case "error":
		return stopReasonError
	default:
		return stopReasonUnknown
	}
}

func reasoningTokenIndex(tokens int64) int {
	for idx, value := range reasoningTokenBuckets {
		if tokens == value {
			return idx
		}
	}
	return -1
}

func jsonNumberString(value int64) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}

type rpcManagementRequest struct {
	pluginapi.ManagementRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcManagementRegistration struct {
	Routes    []rpcManagementRoute `json:"routes,omitempty"`
	Resources []rpcResourceRoute   `json:"resources,omitempty"`
}

type rpcManagementRoute struct {
	Method      string `json:"Method"`
	Path        string `json:"Path"`
	Menu        string `json:"Menu,omitempty"`
	Description string `json:"Description,omitempty"`
}

type rpcResourceRoute struct {
	Path        string `json:"Path"`
	Menu        string `json:"Menu,omitempty"`
	Description string `json:"Description,omitempty"`
}

func registerMetricsManagement() ([]byte, error) {
	return okEnvelope(rpcManagementRegistration{
		Routes: []rpcManagementRoute{{
			Method:      http.MethodGet,
			Path:        "/codex-continue-thinking/metrics",
			Description: "Codex continue-thinking aggregate metrics.",
		}},
		Resources: []rpcResourceRoute{{
			Path:        "/metrics",
			Menu:        "Codex Continue Metrics",
			Description: "Shows aggregate continue-thinking counters without request content.",
		}},
	})
}

func handleMetricsManagement(raw []byte) ([]byte, error) {
	var req rpcManagementRequest
	if len(raw) > 0 {
		if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
			return nil, errUnmarshal
		}
	}
	path := strings.TrimRight(req.Path, "/")
	switch path {
	case "/v0/management/codex-continue-thinking/metrics":
		return metricsJSONResponse()
	case "/v0/resource/plugins/codex-continue-thinking/metrics":
		if strings.EqualFold(req.Query.Get("format"), "json") {
			return metricsJSONResponse()
		}
		return metricsHTMLResponse()
	default:
		return okEnvelope(pluginapi.ManagementResponse{
			StatusCode: http.StatusNotFound,
			Headers:    http.Header{"Content-Type": []string{"text/plain; charset=utf-8"}},
			Body:       []byte("not found"),
		})
	}
}

func metricsJSONResponse() ([]byte, error) {
	raw, errMarshal := json.MarshalIndent(continueMetrics.snapshot(loadedConfig()), "", "  ")
	if errMarshal != nil {
		return nil, errMarshal
	}
	return okEnvelope(pluginapi.ManagementResponse{
		StatusCode: http.StatusOK,
		Headers: http.Header{
			"Content-Type":  []string{"application/json; charset=utf-8"},
			"Cache-Control": []string{"no-store"},
		},
		Body: raw,
	})
}

func metricsHTMLResponse() ([]byte, error) {
	return okEnvelope(pluginapi.ManagementResponse{
		StatusCode: http.StatusOK,
		Headers: http.Header{
			"Content-Type":  []string{"text/html; charset=utf-8"},
			"Cache-Control": []string{"no-store"},
		},
		Body: []byte(metricsHTML),
	})
}

const metricsHTML = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Codex Continue Metrics</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:24px;background:#10100f;color:#eee}
main{max-width:1100px;margin:auto}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}.card{background:#1b1b19;border:1px solid #333;border-radius:12px;padding:16px}.value{font-size:28px;font-weight:700;margin-top:8px}.muted{color:#aaa;font-size:13px}table{width:100%;border-collapse:collapse;margin-top:12px;background:#1b1b19;border-radius:12px;overflow:hidden}th,td{border-bottom:1px solid #333;padding:10px;text-align:left}th{color:#bbb}code{background:#222;padding:2px 5px;border-radius:5px}button{background:#2d6cdf;color:white;border:0;border-radius:8px;padding:9px 14px;cursor:pointer}.error{color:#ff8f8f}</style>
</head>
<body>
<main>
<h1>Codex Continue Metrics</h1>
<p class="muted">Only aggregate counters are stored in memory. No prompts, responses, API keys, accounts, or auth identifiers are collected.</p>
<p><button onclick="load()">刷新</button> <span id="status" class="muted"></span></p>
<section class="grid" id="cards"></section>
<h2>每请求继续次数分布</h2><table><tbody id="rounds"></tbody></table>
<h2>触发 continue 的 reasoning_tokens</h2><table><tbody id="tokens"></tbody></table>
<h2>最终停止原因</h2><table><tbody id="finalReasons"></tbody></table>
<h2>最近 rolling buckets</h2><table><thead><tr><th>开始时间</th><th>请求</th><th>触发继续</th><th>继续轮次</th><th>失败</th><th>Fail-open</th></tr></thead><tbody id="buckets"></tbody></table>
</main>
<script>
function esc(v){return String(v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function row(k,v){return '<tr><td><code>'+esc(k)+'</code></td><td>'+esc(v)+'</td></tr>'}
function setRows(id,obj){document.getElementById(id).innerHTML=Object.entries(obj||{}).map(([k,v])=>row(k,v)).join('')}
function card(label,value,hint=''){return '<div class="card"><div class="muted">'+esc(label)+'</div><div class="value">'+esc(value)+'</div><div class="muted">'+esc(hint)+'</div></div>'}
async function load(){
  const status=document.getElementById('status'); status.textContent='加载中...';
  try{
    const res=await fetch('/v0/resource/plugins/codex-continue-thinking/metrics?format=json',{credentials:'same-origin',cache:'no-store'});
    if(!res.ok) throw new Error('HTTP '+res.status+'，请从管理后台打开或确认管理权限');
    const m=await res.json();
    const pct=(m.continue_rate*100).toFixed(2)+'%';
    document.getElementById('cards').innerHTML=[
      card('接管请求',m.requests_started,'active '+m.active_requests),
      card('触发继续请求',m.requests_with_continue,pct),
      card('总继续轮次',m.total_continue_rounds,'max_continue '+m.config.max_continue),
      card('失败请求',m.requests_failed,'fail-open '+m.fail_open),
      card('Append 失败',m.append_failed,''),
      card('无 completed payload',m.no_completed,''),
      card('运行时间',m.uptime_seconds+'s','started '+m.started_at),
      card('日志决策',m.config.log_decisions?'开启':'关闭','避免日志膨胀')
    ].join('');
    setRows('rounds',m.continue_rounds_per_request);
    setRows('tokens',m.continued_reasoning_tokens);
    setRows('finalReasons',m.final_stop_reasons);
    document.getElementById('buckets').innerHTML=(m.buckets||[]).slice(-48).map(b=>'<tr><td>'+esc(b.start)+'</td><td>'+esc(b.requests)+'</td><td>'+esc(b.continued)+'</td><td>'+esc(b.continue_rounds)+'</td><td>'+esc(b.failed)+'</td><td>'+esc(b.fail_open)+'</td></tr>').join('');
    status.textContent='更新于 '+m.generated_at;
  }catch(e){status.innerHTML='<span class="error">'+e.message+'</span>'}
}
load(); setInterval(load,30000);
</script>
</body>
</html>`
