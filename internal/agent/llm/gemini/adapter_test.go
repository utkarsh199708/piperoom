package gemini

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/retry"
	"piperoom/internal/config"
)

// recorder answers each request with the next scripted reply (the last one
// repeats) and records requests.
type recorder struct {
	*httptest.Server
	mu      sync.Mutex
	reqs    []recorded
	replies []reply
}

type recorded struct {
	path   string
	header http.Header
	body   []byte
}

type reply struct {
	status int
	header map[string]string
	body   string
}

func newRecorder(t *testing.T, replies ...reply) *recorder {
	t.Helper()
	r := &recorder{replies: replies}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		b, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.reqs = append(r.reqs, recorded{path: req.URL.Path, header: req.Header.Clone(), body: b})
		rep := r.replies[0]
		if len(r.replies) > 1 {
			r.replies = r.replies[1:]
		}
		r.mu.Unlock()
		for k, v := range rep.header {
			w.Header().Set(k, v)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rep.status)
		_, _ = io.WriteString(w, rep.body)
	}))
	t.Cleanup(r.Close)
	return r
}

func (r *recorder) requests() []recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recorded(nil), r.reqs...)
}

func ok(body string) reply { return reply{status: 200, body: body} }

func userText(s string) llm.Message {
	return llm.Message{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartText, Text: s}}}
}

var statusTool = llm.ToolDef{
	Name:        "connect_status",
	Description: "Returns the state of a connector.",
	Schema: map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"connector": map[string]any{"type": "string"}},
		"required":             []string{"connector"},
		"additionalProperties": false,
	},
}

func baseReq(model string) llm.Request {
	return llm.Request{
		Model: model, System: "You are Piperoom.", MaxTokens: 1000,
		Tools:    []llm.ToolDef{statusTool},
		Messages: []llm.Message{userText("Check the connectors.")},
	}
}

// TestParallelCallsWithoutIDs: a model that gives no ids gets made ones; the
// results go back without ids, in the order of the calls (even when the
// agent hands them over in another order), named from the calls.
func TestParallelCallsWithoutIDs(t *testing.T) {
	s := newRecorder(t,
		ok(`{"candidates":[{"content":{"role":"model","parts":[
			{"functionCall":{"name":"connect_status","args":{"connector":"a"}},"thoughtSignature":"c2lnLTE="},
			{"functionCall":{"name":"connect_status","args":{"connector":"b"}}}]},"finishReason":"STOP"}],
			"usageMetadata":{"promptTokenCount":200,"candidatesTokenCount":20}}`),
		ok(`{"candidates":[{"content":{"role":"model","parts":[{"text":"done"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":300,"candidatesTokenCount":2}}`),
	)
	m := newTestModel(t, s.URL, nil)
	req := baseReq("gemini-2.5-pro")
	resp, err := m.Turn(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Stop != llm.StopToolUse {
		t.Fatalf("Stop = %s", resp.Stop)
	}
	var ids []string
	for _, p := range resp.Message.Parts {
		if p.Kind == llm.PartToolCall {
			ids = append(ids, p.ToolCall.ID)
		}
	}
	if strings.Join(ids, ",") != "call_1_1,call_1_2" {
		t.Fatalf("made ids = %v", ids)
	}
	// Reversed order and no names: position and the call map put them back.
	results := llm.Message{Role: llm.RoleUser, Parts: []llm.Part{
		{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{CallID: "call_1_2", Content: "B <&> \"q\""}},
		{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{CallID: "call_1_1", Content: "A"}},
		{Kind: llm.PartText, Text: "steer: look at b"},
	}}
	req.Messages = append(req.Messages, resp.Message, results)
	if _, err := m.Turn(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	body := s.requests()[1].body
	if bytes.Contains(body, []byte("call_1_")) {
		t.Fatalf("a made id went on the wire: %s", body)
	}
	var w struct {
		Contents []struct {
			Role  string            `json:"role"`
			Parts []json.RawMessage `json:"parts"`
		} `json:"contents"`
	}
	if err := json.Unmarshal(body, &w); err != nil {
		t.Fatal(err)
	}
	if len(w.Contents) != 3 || w.Contents[1].Role != "model" || w.Contents[2].Role != "user" {
		t.Fatalf("contents = %s", body)
	}
	// The model turn goes back byte for byte, its signature with it.
	for i, p := range resp.Message.Parts {
		if !bytes.Equal(w.Contents[1].Parts[i], p.Native.Raw) {
			t.Fatalf("model part %d = %s, want %s", i, w.Contents[1].Parts[i], p.Native.Raw)
		}
	}
	var got []string
	for _, raw := range w.Contents[2].Parts[:2] {
		var p struct {
			FunctionResponse struct {
				ID       string `json:"id"`
				Name     string `json:"name"`
				Response struct {
					Content string `json:"content"`
				} `json:"response"`
			} `json:"functionResponse"`
		}
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatal(err)
		}
		if p.FunctionResponse.Name != "connect_status" || p.FunctionResponse.ID != "" {
			t.Fatalf("functionResponse = %s", raw)
		}
		got = append(got, p.FunctionResponse.Response.Content)
	}
	if got[0] != "A" || got[1] != "B <&> \"q\"" {
		t.Fatalf("results by position = %q", got)
	}
	if !bytes.Contains(w.Contents[2].Parts[2], []byte("steer: look at b")) {
		t.Fatalf("text after results: %s", w.Contents[2].Parts[2])
	}
}

// TestThoughtSignatures: a thought part and signatures come back verbatim to
// the same model, and are dropped for another model.
func TestThoughtSignatures(t *testing.T) {
	first := `{"candidates":[{"content":{"role":"model","parts":[
		{"text":"plan","thought":true},
		{"text":"","thoughtSignature":"c2lnLWE="},
		{"text":"Reading.","thoughtSignature":"c2lnLWI="},
		{"functionCall":{"id":"fc-9","name":"connect_status","args":{"connector":"a"}},"thoughtSignature":"c2lnLWM="}]},
		"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":200,"candidatesTokenCount":20,"thoughtsTokenCount":50}}`
	done := `{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":200,"candidatesTokenCount":2}}`
	s := newRecorder(t, ok(first), ok(done))
	m := newTestModel(t, s.URL, nil)
	req := baseReq("gemini-3-pro-preview")
	resp, err := m.Turn(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	kinds := []llm.PartKind{}
	for _, p := range resp.Message.Parts {
		kinds = append(kinds, p.Kind)
	}
	want := []llm.PartKind{llm.PartReasoning, llm.PartReasoning, llm.PartText, llm.PartToolCall}
	if len(kinds) != len(want) {
		t.Fatalf("kinds = %v", kinds)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("kinds = %v, want %v", kinds, want)
		}
	}
	if resp.Usage.Output != 70 || resp.Usage.Reasoning != 50 {
		t.Fatalf("thoughts not counted as output: %+v", resp.Usage)
	}
	results := llm.Message{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{CallID: "fc-9", Name: "connect_status", Content: "RUNNING"}}}}
	req.Messages = append(req.Messages, resp.Message, results)
	if _, err := m.Turn(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	body := s.requests()[1].body
	for _, sig := range []string{"c2lnLWE=", "c2lnLWI=", "c2lnLWM=", `"thought":true`, `"id":"fc-9"`} {
		if !bytes.Contains(body, []byte(sig)) {
			t.Fatalf("second request lost %s: %s", sig, body)
		}
	}
	if c := bytes.Count(body, []byte(`"id":"fc-9"`)); c != 2 {
		t.Fatalf("model-given id should be on the call and its response, got %d: %s", c, body)
	}

	// Another model: signatures are model-bound, so they are not sent.
	req.Model = "gemini-2.5-flash"
	if _, err := m.Turn(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	body = s.requests()[2].body
	if bytes.Contains(body, []byte("thoughtSignature")) || bytes.Contains(body, []byte(`"thought"`)) {
		t.Fatalf("a signature went to another model: %s", body)
	}
	if !bytes.Contains(body, []byte(`"functionCall"`)) || !bytes.Contains(body, []byte("Reading.")) {
		t.Fatalf("neutral parts lost: %s", body)
	}
}

func TestPromptBlockedIsRefusal(t *testing.T) {
	s := newRecorder(t, ok(`{"promptFeedback":{"blockReason":"PROHIBITED_CONTENT","blockReasonMessage":"blocked by policy"},"usageMetadata":{"promptTokenCount":40}}`))
	resp, err := newTestModel(t, s.URL, nil).Turn(context.Background(), baseReq("gemini-2.5-pro"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Stop != llm.StopRefusal || resp.ProviderStop != "PROHIBITED_CONTENT" || !strings.Contains(resp.StopDetail, "blocked by policy") {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestFinishReasons(t *testing.T) {
	for reason, want := range map[string]llm.StopReason{
		"STOP": llm.StopEndTurn, "MAX_TOKENS": llm.StopMaxTokens, "SAFETY": llm.StopRefusal,
		"RECITATION": llm.StopRefusal, "BLOCKLIST": llm.StopRefusal, "SPII": llm.StopRefusal,
		"PROHIBITED_CONTENT": llm.StopRefusal, "OTHER": llm.StopOther, "UNEXPECTED_TOOL_CALL": llm.StopOther,
	} {
		s := newRecorder(t, ok(`{"candidates":[{"content":{"parts":[{"text":"x"}]},"finishReason":"`+reason+`"}],"usageMetadata":{"promptTokenCount":100,"candidatesTokenCount":1}}`))
		resp, err := newTestModel(t, s.URL, nil).Turn(context.Background(), baseReq("gemini-2.5-pro"))
		if err != nil {
			t.Fatal(err)
		}
		if resp.Stop != want {
			t.Errorf("%s: Stop = %s, want %s", reason, resp.Stop, want)
		}
	}
}

// TestMalformedFunctionCall: the stop becomes a tool call the agent answers
// with an error tool_result; both go back as text.
func TestMalformedFunctionCall(t *testing.T) {
	s := newRecorder(t,
		ok(`{"candidates":[{"finishReason":"MALFORMED_FUNCTION_CALL","finishMessage":"Malformed function call: print(default_api.connect_status(connector='a'))"}],"usageMetadata":{"promptTokenCount":200,"candidatesTokenCount":5}}`),
		ok(`{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":200,"candidatesTokenCount":2}}`),
	)
	m := newTestModel(t, s.URL, nil)
	req := baseReq("gemini-2.5-pro")
	resp, err := m.Turn(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Stop != llm.StopToolUse || len(resp.Message.Parts) != 1 {
		t.Fatalf("resp = %+v", resp)
	}
	call := resp.Message.Parts[0].ToolCall
	if call == nil || call.Name != "connect_status" || call.ID == "" || json.Valid(call.Input) {
		t.Fatalf("call = %+v (input must not be JSON, so the agent rejects it as invalid_json)", call)
	}
	res := llm.Message{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{
		CallID: call.ID, Name: call.Name, Content: "error: invalid input: input is not valid JSON", IsError: true}}}}
	req.Messages = append(req.Messages, resp.Message, res)
	if _, err := m.Turn(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	body := s.requests()[1].body
	if bytes.Contains(body, []byte("functionCall")) || bytes.Contains(body, []byte("functionResponse")) || bytes.Contains(body, []byte(malformedMarker)) {
		t.Fatalf("a synthesised call went on the wire as a function part: %s", body)
	}
	if !bytes.Contains(body, []byte("input is not valid JSON")) {
		t.Fatalf("the error result was lost: %s", body)
	}
}

func TestUsageNormalised(t *testing.T) {
	s := newRecorder(t, ok(`{"candidates":[{"content":{"parts":[{"text":"x"}]},"finishReason":"STOP"}],
		"usageMetadata":{"promptTokenCount":1000,"cachedContentTokenCount":600,"candidatesTokenCount":10,"thoughtsTokenCount":90}}`))
	resp, err := newTestModel(t, s.URL, nil).Turn(context.Background(), baseReq("gemini-2.5-pro"))
	if err != nil {
		t.Fatal(err)
	}
	if u := resp.Usage; u.Input != 400 || u.CacheRead != 600 || u.Output != 100 || u.Reasoning != 90 || u.Estimated {
		t.Fatalf("usage = %+v", u)
	}
}

func TestMissingUsageIsEstimated(t *testing.T) {
	s := newRecorder(t, ok(`{"candidates":[{"content":{"parts":[{"text":"x"}]},"finishReason":"STOP"}]}`))
	resp, err := newTestModel(t, s.URL, nil).Turn(context.Background(), baseReq("gemini-2.5-pro"))
	if err != nil {
		t.Fatal(err)
	}
	if u := resp.Usage; !u.Estimated || u.Input == 0 || u.Output == 0 {
		t.Fatalf("usage = %+v", u)
	}
}

func TestTruncatedPrompt(t *testing.T) {
	s := newRecorder(t, ok(`{"candidates":[{"content":{"parts":[{"text":"x"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":50,"candidatesTokenCount":1}}`))
	req := baseReq("gemini-2.5-pro")
	req.Messages = []llm.Message{userText(strings.Repeat("tool output line\n", 2000))}
	resp, err := newTestModel(t, s.URL, nil).Turn(context.Background(), req)
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrTruncated || !llm.Unretryable(err) {
		t.Fatalf("err = %v, want truncated", err)
	}
	if resp.Usage.Input != 50 {
		t.Fatalf("the billed usage must come back with the error: %+v", resp.Usage)
	}
}

func TestRequestEncoding(t *testing.T) {
	out := map[string]any{"type": "object", "properties": map[string]any{"verdict": map[string]any{"type": "string", "enum": []any{"a", "b"}}}, "required": []any{"verdict"}, "additionalProperties": false}
	for _, tc := range []struct {
		name, model, effort string
		choice              llm.ToolChoice
		want, absent        []string
	}{
		{"2.5 budget", "gemini-2.5-pro", "high", "", []string{`"thinkingBudget":500`, `"parametersJsonSchema"`, `"responseJsonSchema"`, `"responseMimeType":"application/json"`, `"systemInstruction"`}, []string{"thinkingLevel", `"parameters"`, `"toolConfig"`}},
		{"3 level", "gemini-3-pro-preview", "low", llm.ToolChoiceRequired, []string{`"thinkingLevel":"low"`, `"mode":"ANY"`}, []string{"thinkingBudget"}},
		{"3 high", "gemini-3-flash", "max", llm.ToolChoiceNone, []string{`"thinkingLevel":"high"`, `"mode":"NONE"`}, nil},
		{"2.0 legacy", "gemini-2.0-flash", "high", "", []string{`"parameters":{`, `"type":"OBJECT"`, `"responseSchema"`, `"enum":["a","b"]`}, []string{"thinkingConfig", "parametersJsonSchema", "responseJsonSchema", "additionalProperties"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := baseReq(tc.model)
			req.Effort, req.Thinking, req.ToolChoice, req.OutputSchema = tc.effort, true, tc.choice, out
			body, _, err := encodeRequest(tc.model, req)
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range tc.want {
				if !bytes.Contains(body, []byte(w)) {
					t.Errorf("missing %s in %s", w, body)
				}
			}
			for _, a := range tc.absent {
				if bytes.Contains(body, []byte(a)) {
					t.Errorf("unexpected %s in %s", a, body)
				}
			}
			if bytes.Contains(body, []byte("You are Piperoom.")) && bytes.Index(body, []byte("You are Piperoom.")) < bytes.Index(body, []byte("systemInstruction")) {
				t.Errorf("system prompt outside systemInstruction")
			}
		})
	}
}

// TestRetryPolicy: on the shared policy (llm/retry), 429 and 503 are retried
// at most twice with the backoff; a quota 429 is billing and sent once;
// Gemini's RetryInfo.retryDelay and a Retry-After are honoured and capped.
func TestRetryPolicy(t *testing.T) {
	okBody := `{"candidates":[{"content":{"parts":[{"text":"x"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100,"candidatesTokenCount":1}}`
	rate := `{"error":{"code":429,"message":"Resource has been exhausted (e.g. check quota).","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"7s"}]}}`
	rateLong := `{"error":{"code":429,"message":"Resource has been exhausted (e.g. check quota).","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"300s"}]}}`
	daily := `{"error":{"code":429,"message":"You exceeded your current quota","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[{"quotaMetric":"generativelanguage.googleapis.com/generate_content_requests","quotaId":"GenerateRequestsPerDayPerProjectPerModel"}]}]}}`
	free := `{"error":{"code":429,"message":"Quota exceeded for metric: x, limit: 0","status":"RESOURCE_EXHAUSTED"}}`
	unavailable := `{"error":{"code":503,"message":"The model is overloaded.","status":"UNAVAILABLE"}}`
	for _, tc := range []struct {
		name    string
		replies []reply
		reqs    int
		kind    llm.ErrorKind
		waits   []time.Duration
	}{
		{"429 then ok", []reply{{status: 429, body: rate}, ok(okBody)}, 2, "", []time.Duration{7 * time.Second}},
		{"retryDelay capped", []reply{{status: 429, body: rateLong}, ok(okBody)}, 2, "", []time.Duration{60 * time.Second}},
		{"503 twice then overloaded", []reply{{status: 503, body: unavailable}}, 3, llm.ErrOverloaded, []time.Duration{time.Second, 4 * time.Second}},
		{"retry-after capped", []reply{{status: 503, header: map[string]string{"Retry-After": "600"}, body: unavailable}, ok(okBody)}, 2, "", []time.Duration{60 * time.Second}},
		{"daily quota not retried", []reply{{status: 429, body: daily}}, 1, llm.ErrBilling, nil},
		{"free tier quota not retried", []reply{{status: 429, body: free}}, 1, llm.ErrBilling, nil},
		{"400 not retried", []reply{{status: 400, body: `{"error":{"code":400,"message":"bad","status":"INVALID_ARGUMENT"}}`}}, 1, llm.ErrBadRequest, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newRecorder(t, tc.replies...)
			m := newTestModel(t, s.URL, nil)
			var waits []time.Duration
			p := retry.Default()
			p.Jitter = 0
			p.Sleep = func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }
			m.policy = p
			_, err := m.Turn(context.Background(), baseReq("gemini-2.5-pro"))
			if n := len(s.requests()); n != tc.reqs {
				t.Fatalf("requests = %d, want %d", n, tc.reqs)
			}
			if tc.kind == "" {
				if err != nil {
					t.Fatalf("err = %v", err)
				}
			} else {
				var ae *llm.APIError
				if !errors.As(err, &ae) || ae.Kind != tc.kind {
					t.Fatalf("err = %v, want kind %s", err, tc.kind)
				}
			}
			if len(waits) != len(tc.waits) {
				t.Fatalf("waits = %v, want %v", waits, tc.waits)
			}
			for i := range waits {
				if waits[i] != tc.waits[i] {
					t.Fatalf("waits = %v, want %v", waits, tc.waits)
				}
			}
		})
	}
}

// TestOptionsRetryUsed: a Policy passed in Options replaces the default.
func TestOptionsRetryUsed(t *testing.T) {
	s := newRecorder(t, reply{status: 503, body: `{"error":{"code":503,"status":"UNAVAILABLE"}}`})
	b, err := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", s.URL, false)
	if err != nil {
		t.Fatal(err)
	}
	var waits int
	p := retry.Policy{MaxRetries: 1, Sleep: func(context.Context, time.Duration) error { waits++; return nil }}
	m, err := New(context.Background(), Options{APIKey: testKey, BaseURL: &b, Retry: &p})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Turn(context.Background(), baseReq("gemini-2.5-pro")); err == nil {
		t.Fatal("want overloaded")
	}
	if n := len(s.requests()); n != 2 || waits != 1 {
		t.Fatalf("requests = %d, waits = %d, want 2 and 1 (MaxRetries 1)", n, waits)
	}
}

func TestRetryStopsAtDeadline(t *testing.T) {
	s := newRecorder(t, reply{status: 503, body: `{"error":{"code":503,"status":"UNAVAILABLE"}}`})
	m := newTestModel(t, s.URL, nil)
	m.policy = retry.Default()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := m.Turn(ctx, baseReq("gemini-2.5-pro"))
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrOverloaded {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 2*time.Second || len(s.requests()) != 1 {
		t.Fatalf("slept into the deadline: %s, %d requests", time.Since(start), len(s.requests()))
	}
}

func TestErrorKinds(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   llm.ErrorKind
	}{
		{400, `{"error":{"code":400,"message":"API key not valid. Please pass a valid API key.","status":"INVALID_ARGUMENT","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"API_KEY_INVALID"}]}}`, llm.ErrAuth},
		{403, `{"error":{"code":403,"message":"denied","status":"PERMISSION_DENIED"}}`, llm.ErrAuth},
		{403, `{"error":{"code":403,"message":"billing","status":"PERMISSION_DENIED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"BILLING_DISABLED"}]}}`, llm.ErrBilling},
		{400, `{"error":{"code":400,"message":"The input token count (1200000) exceeds the maximum number of tokens allowed (1048576).","status":"INVALID_ARGUMENT"}}`, llm.ErrTooLong},
		{404, `{"error":{"code":404,"message":"models/x is not found","status":"NOT_FOUND"}}`, llm.ErrModelNotFound},
		{504, `{}`, llm.ErrOverloaded},
		{422, `not json`, llm.ErrBadRequest},
	} {
		if got := parseError(tc.status, []byte(tc.body)).kind; got != tc.want {
			t.Errorf("%d %s: kind = %s, want %s", tc.status, tc.body, got, tc.want)
		}
	}
}

// TestLimiterWaitPastDeadline: the shared limiter's refusal is rate_limited
// and nothing is sent.
func TestLimiterWaitPastDeadline(t *testing.T) {
	s := newRecorder(t, ok(`{"candidates":[{"content":{"parts":[{"text":"x"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100,"candidatesTokenCount":1}}`))
	lim := retry.NewLimiter("test", retry.Limits{TPM: 60}, nil)
	m := newTestModel(t, s.URL, func(o *Options) { o.Limiter = lim })
	// Drain the bucket, then ask for more than a 1 s deadline can refill.
	release, err := lim.Acquire(context.Background(), 60)
	if err != nil {
		t.Fatal(err)
	}
	release(60)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = m.Turn(ctx, baseReq("gemini-2.5-pro"))
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrRateLimited || !errors.Is(err, llm.ErrNotSent) {
		t.Fatalf("err = %v, want rate_limited, not sent", err)
	}
	if n := len(s.requests()); n != 0 {
		t.Fatalf("requests = %d, want 0", n)
	}
}

// TestLimiterConcurrencyBound: a held slot on the shared limiter keeps the
// turn from sending until the caller's context ends.
func TestLimiterConcurrencyBound(t *testing.T) {
	s := newRecorder(t, ok(`{"candidates":[{"content":{"parts":[{"text":"x"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100,"candidatesTokenCount":1}}`))
	lim := retry.NewLimiter("test", retry.Limits{MaxConcurrency: 1}, nil)
	m := newTestModel(t, s.URL, func(o *Options) { o.Limiter = lim })
	release, err := lim.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := m.Turn(ctx, baseReq("gemini-2.5-pro")); !errors.Is(err, llm.ErrNotSent) {
		t.Fatalf("err = %v, want not sent while the slot is held", err)
	}
	release(0)
	if _, err := m.Turn(context.Background(), baseReq("gemini-2.5-pro")); err != nil {
		t.Fatal(err)
	}
	if n := len(s.requests()); n != 1 {
		t.Fatalf("requests = %d, want 1", n)
	}
}
