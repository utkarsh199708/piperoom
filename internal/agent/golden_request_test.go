package agent

// Golden capture of the Claude requests the agent sends today (V1a-0, KAN-293).
//
// V1a and V1b move the agent onto llm.Model with an Anthropic adapter and must
// not change one byte of what a Claude deployment sends. These tests point the
// real SDK client (newEndpoint, the production constructor) at an httptest
// server, drive each call path with canned streamed answers, and compare what
// arrived on the wire with the files in testdata/golden: method, path, query,
// the headers that change behaviour, and the body (pretty-printed for review
// plus the SHA-256 of the exact bytes, so a whitespace or escaping change also
// fails).
//
// Regenerate (only when a change to the requests is intended and reviewed):
//
//	go test ./internal/agent -run Golden -update
//	go test -tags claude ./internal/agent -run Golden -update   (also the judge)

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go/option"

	"piperoom/internal/agent/fake"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/golden/*.json from the requests today's code sends")

const (
	goldenDir       = "testdata/golden"
	goldenAPIKey    = "sk-ant-golden-not-real"
	goldenWorkspace = "wrkspc_golden"
	goldenMain      = "claude-opus-5-5"
	goldenTriage    = "claude-haiku-4-5"
	goldenFirstLook = "claude-haiku-5-5"
)

// goldenHeaderIgnored reports whether a golden leaves a request header out.
// Every other header is recorded, so a header V1a adds, drops or changes
// fails the comparison. The recorded ones today are anthropic-beta (server-side
// compaction), anthropic-version (the API version the body is read as),
// anthropic-workspace-id (the workspace a user key is scoped to), x-api-key
// (the credential, a test value here), content-type and accept.
//
// Left out: the X-Stainless-* headers (OS, arch, Go version, package version,
// retry count, timeout) and User-Agent describe the machine and the SDK build,
// not the request, and differ between the Windows dev box and CI;
// Content-Length is covered by body_sha256; Accept-Encoding is Go's transport
// default.
func goldenHeaderIgnored(name string) bool {
	switch k := http.CanonicalHeaderKey(name); {
	case strings.HasPrefix(k, "X-Stainless-"):
		return true
	case k == "User-Agent", k == "Content-Length", k == "Accept-Encoding":
		return true
	}
	return false
}

// stepIDRaw matches a step id as newStepID makes it: "s_" and a ULID, whose
// first 10 digits are the current millisecond and the rest crypto/rand.
var stepIDRaw = regexp.MustCompile(`s_[0-9A-HJKMNP-TV-Z]{26}`)

// normaliseGolden is the only rewrite applied to a captured body: each step id
// (time and randomness, see stepIDRaw) becomes s_GOLDEN<n>, numbered in order
// of first appearance, so the same step keeps the same placeholder wherever
// it is cited. Nothing else in the body varies between runs.
func normaliseGolden(body []byte) []byte {
	seen := map[string]string{}
	return stepIDRaw.ReplaceAllFunc(body, func(id []byte) []byte {
		k := string(id)
		if v, ok := seen[k]; ok {
			return []byte(v)
		}
		v := fmt.Sprintf("s_GOLDEN%02d", len(seen)+1)
		seen[k] = v
		return []byte(v)
	})
}

// goldenRequest is one captured request as a golden file holds it.
type goldenRequest struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Query   string            `json:"query"`
	Headers map[string]string `json:"headers"`
	// BodySHA256 is the hash of the exact body bytes after normaliseGolden.
	BodySHA256 string `json:"body_sha256"`
	// Body is the same bytes indented for review; json.Indent changes only
	// whitespace between tokens, never a string or key order.
	Body json.RawMessage `json:"body"`
}

// goldenServer records every request and answers each with the next canned
// stream, in order. The agent's model calls are sequential, so the order is
// the call order.
type goldenServer struct {
	*httptest.Server
	t       *testing.T
	mu      sync.Mutex
	seen    []goldenRequest
	answers []string
}

func newGoldenServer(t *testing.T, answers ...string) *goldenServer {
	t.Helper()
	gs := &goldenServer{t: t, answers: answers}
	gs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		body := normaliseGolden(raw)
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, body, "", "  "); err != nil {
			http.Error(w, "body is not JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		sum := sha256.Sum256(body)
		h := map[string]string{}
		for k, v := range r.Header {
			if !goldenHeaderIgnored(k) {
				h[k] = strings.Join(v, ", ")
			}
		}
		gs.mu.Lock()
		gs.seen = append(gs.seen, goldenRequest{
			Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Headers: h,
			BodySHA256: hex.EncodeToString(sum[:]), Body: pretty.Bytes(),
		})
		n := len(gs.seen)
		gs.mu.Unlock()
		if n > len(gs.answers) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"type":"error","error":{"type":"api_error","message":"golden server: no canned answer left"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, gs.answers[n-1])
	}))
	t.Cleanup(gs.Close)
	return gs
}

func (gs *goldenServer) requests() []goldenRequest {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	return append([]goldenRequest(nil), gs.seen...)
}

// goldenEndpoint is the production endpoint on the anthropic provider with the
// workspace header set, pointed at gs. Retries are off so a failure shows as
// one request, not three; the SDK's retry count only changes a header that is
// not recorded.
func goldenEndpoint(t *testing.T, gs *goldenServer, cfg EndpointConfig) *Endpoint {
	t.Helper()
	ep, err := newEndpoint(context.Background(), cfg, envOf(map[string]string{workspaceEnv: goldenWorkspace}),
		testLoaders(nil), discardLogger(),
		option.WithBaseURL(gs.URL), option.WithAPIKey(goldenAPIKey), option.WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	return ep
}

// sseBlock is one content block of a canned answer, streamed the way the API
// streams it: an empty block_start, then its deltas.
type sseBlock struct {
	start  map[string]any
	deltas []map[string]any
}

func sseText(text string) sseBlock {
	return sseBlock{
		start:  map[string]any{"type": "text", "text": ""},
		deltas: []map[string]any{{"type": "text_delta", "text": text}},
	}
}

func sseThinking(thinking, signature string) sseBlock {
	return sseBlock{
		start: map[string]any{"type": "thinking", "thinking": "", "signature": ""},
		deltas: []map[string]any{
			{"type": "thinking_delta", "thinking": thinking},
			{"type": "signature_delta", "signature": signature},
		},
	}
}

func sseToolUse(id, name, input string) sseBlock {
	return sseBlock{
		start:  map[string]any{"type": "tool_use", "id": id, "name": name, "input": map[string]any{}},
		deltas: []map[string]any{{"type": "input_json_delta", "partial_json": input}},
	}
}

func sseCompaction(content, encrypted string) sseBlock {
	return sseBlock{
		start:  map[string]any{"type": "compaction", "content": nil},
		deltas: []map[string]any{{"type": "compaction_delta", "content": content, "encrypted_content": encrypted}},
	}
}

// sseAnswer renders one streamed assistant turn.
func sseAnswer(t *testing.T, id, model, stop string, blocks ...sseBlock) string {
	t.Helper()
	var b strings.Builder
	ev := func(typ string, v map[string]any) {
		v["type"] = typ
		j, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&b, "event: %s\ndata: %s\n\n", typ, j)
	}
	ev("message_start", map[string]any{"message": map[string]any{
		"id": id, "type": "message", "role": "assistant", "model": model, "content": []any{},
		"stop_reason": nil, "stop_sequence": nil, "usage": map[string]any{"input_tokens": 1200, "output_tokens": 1},
	}})
	for i, bl := range blocks {
		ev("content_block_start", map[string]any{"index": i, "content_block": bl.start})
		for _, d := range bl.deltas {
			ev("content_block_delta", map[string]any{"index": i, "delta": d})
		}
		ev("content_block_stop", map[string]any{"index": i})
	}
	ev("message_delta", map[string]any{"delta": map[string]any{"stop_reason": stop, "stop_sequence": nil},
		"usage": map[string]any{"output_tokens": 80}})
	ev("message_stop", map[string]any{})
	return b.String()
}

// checkGolden compares got with testdata/golden/<name>.json, or writes it
// with -update.
func checkGolden(t *testing.T, name string, got goldenRequest) {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // keep the body's own escaping visible as sent
	enc.SetIndent("", "  ")
	if err := enc.Encode(got); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(goldenDir, name+".json")
	if *updateGolden {
		if err := os.MkdirAll(goldenDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (create it with: go test ./internal/agent -run Golden -update)", path, err)
	}
	// A Windows checkout may have turned the file's LF into CRLF.
	want = bytes.ReplaceAll(want, []byte("\r\n"), []byte("\n"))
	if !bytes.Equal(want, buf.Bytes()) {
		t.Errorf("request %s differs from %s:\n%s\nIf the change is intended and reviewed, rerun with -update.",
			name, path, firstDiff(string(want), buf.String()))
	}
}

// firstDiff shows the first differing line of two texts with a little context.
func firstDiff(want, got string) string {
	wl, gl := strings.Split(want, "\n"), strings.Split(got, "\n")
	i := 0
	for i < len(wl) && i < len(gl) && wl[i] == gl[i] {
		i++
	}
	const ctx = 3
	var b strings.Builder
	fmt.Fprintf(&b, "first difference at line %d (want %d lines, got %d)\n", i+1, len(wl), len(gl))
	for j := max(0, i-ctx); j < i; j++ {
		fmt.Fprintf(&b, "   %s\n", clip(wl[j]))
	}
	for j := i; j < min(len(wl), i+ctx); j++ {
		fmt.Fprintf(&b, " - %s\n", clip(wl[j]))
	}
	for j := i; j < min(len(gl), i+ctx); j++ {
		fmt.Fprintf(&b, " + %s\n", clip(gl[j]))
	}
	return b.String()
}

func clip(s string) string {
	if len(s) > 300 {
		return s[:300] + "…"
	}
	return s
}

// checkGoldens checks that exactly len(names) requests arrived and compares
// each with its golden.
func checkGoldens(t *testing.T, gs *goldenServer, names ...string) {
	t.Helper()
	got := gs.requests()
	if len(got) != len(names) {
		t.Fatalf("server saw %d requests, want %d (%v)", len(got), len(names), names)
	}
	for i, n := range names {
		checkGolden(t, n, got[i])
	}
}

// goldenAlert is the recorded room: the dev stack's schema-change incident.
func goldenAlert() Alert {
	return Alert{
		RoomID:      "r_golden",
		Title:       "analytics.orders is stale",
		PipelineID:  "orders",
		Description: "Warehouse freshness for analytics.orders exceeded 10 minutes.",
		Labels:      map[string]string{"severity": "sev2", "team": "data-platform"},
		PipelineMap: "postgres:orders.public.orders -> connect:pg-orders-connector -> kafka:pg.public.orders -> consumer:order-sync -> warehouse:analytics.orders",
		At:          time.Date(2026, 10, 11, 9, 40, 0, 0, time.UTC),
	}
}

func goldenTools(t *testing.T) *tools.Registry {
	t.Helper()
	reg := tools.NewRegistry()
	if err := fake.Register(reg); err != nil {
		t.Fatal(err)
	}
	return reg
}

// TestGoldenLoopRequests captures the investigation loop: the first turn on
// the triage model, the next on the main model (it carries the triage turn's
// thinking block back, a tool_result and a driver's steer), and the turn after
// the main model's answer opened with a server-side compaction block, which
// goes back verbatim in history.
func TestGoldenLoopRequests(t *testing.T) {
	gs := newGoldenServer(t,
		sseAnswer(t, "msg_golden_1", goldenTriage, "tool_use",
			sseThinking("The connector is the first thing to read.", "sig-golden-thinking-1"),
			sseText("Reading the connector status first."),
			sseToolUse("toolu_golden_1", "connect_status", `{"connector":"pg-orders-connector"}`)),
		sseAnswer(t, "msg_golden_2", goldenMain, "tool_use",
			sseCompaction("Summary: task 0 of pg-orders-connector FAILED on a 409 from Schema Registry.", "enc-golden-compaction-1"),
			sseText("Checking the consumer as the driver asked."),
			sseToolUse("toolu_golden_2", "kafka_consumer_lag", `{"group":"order-sync"}`)),
		sseAnswer(t, "msg_golden_3", goldenMain, "end_turn",
			sseText("The amount column changed from int to decimal(12,2) and the registry rejected the new schema.")),
	)
	ep := goldenEndpoint(t, gs, EndpointConfig{Provider: ProviderAnthropic, TriageModel: goldenTriage, TriageTurns: 1})
	steer := HumanInput{
		Seq:   7,
		At:    time.Date(2026, 10, 11, 9, 41, 0, 0, time.UTC),
		Actor: events.Actor{Kind: events.ActorHuman, ID: "u_asha", Name: "Asha"},
		Kind:  "steer",
		Text:  "Check the lag of order-sync too.",
	}
	inbox := &fakeInbox{drains: [][]HumanInput{nil, {steer}}, waitErr: context.Canceled}
	err := ep.Run(context.Background(), goldenAlert(), Deps{
		Tools: goldenTools(t), Inbox: inbox, Sink: &fakeSink{}, Logger: discardLogger(),
	}, Options{Model: goldenMain})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	checkGoldens(t, gs, "loop_triage_turn", "loop_main_turn", "loop_after_compaction")
}

// TestGoldenFirstLookRequests captures both first-look requests: the read
// pick (read tools only) and the structured verdict over the reads.
func TestGoldenFirstLookRequests(t *testing.T) {
	verdict := `{"headline":"Task 0 of pg-orders-connector failed on a schema registry 409","suspected_layer":"kafka-connect",` +
		`"confidence":"medium","next_check":"Read the subject's compatibility level","based_on_steps":[],"evidence_steps":[]}`
	gs := newGoldenServer(t,
		sseAnswer(t, "msg_golden_fl1", goldenFirstLook, "tool_use",
			sseToolUse("toolu_golden_fl1", "connect_status", `{"connector":"pg-orders-connector"}`)),
		sseAnswer(t, "msg_golden_fl2", goldenFirstLook, "end_turn", sseText(verdict)),
	)
	ep := goldenEndpoint(t, gs, EndpointConfig{Provider: ProviderAnthropic, TriagePass: true})
	res, err := ep.Triage(context.Background(), goldenAlert(), Deps{
		Tools: goldenTools(t), Sink: &fakeSink{}, Logger: discardLogger(),
	}, Options{Model: goldenMain})
	if err != nil {
		t.Fatalf("triage: %v", err)
	}
	if res.Payload.Status != "ok" {
		t.Fatalf("first look status = %q (reason %q), want ok", res.Payload.Status, res.Payload.Reason)
	}
	checkGoldens(t, gs, "first_look_pick", "first_look_verdict")
}

// goldenSummaryJSON renders a hand-off summary as the model's JSON answer.
func goldenSummaryJSON(t *testing.T, s events.HandoffSummary) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestGoldenHandoffRequests captures the hand-off over the recorded
// schema-change room, and its self-correction request: the first answer names
// a connector the log does not have, so the writer replays it as the
// assistant turn and asks once for a corrected summary.
func TestGoldenHandoffRequests(t *testing.T) {
	c := caseByName(t, "schema-change")
	bad := cloneSummary(c.summary)
	bad.Why = "pg-payments-connector rejected the schema"
	gs := newGoldenServer(t,
		sseAnswer(t, "msg_golden_h1", goldenMain, "end_turn", sseText(goldenSummaryJSON(t, bad))),
		sseAnswer(t, "msg_golden_h2", goldenMain, "end_turn", sseText(goldenSummaryJSON(t, c.summary))),
	)
	ep := goldenEndpoint(t, gs, EndpointConfig{Provider: ProviderAnthropic})
	if _, err := ep.WriteHandoff(context.Background(), c.events, Options{Model: goldenMain}); err != nil {
		t.Fatalf("handoff: %v", err)
	}
	checkGoldens(t, gs, "handoff", "handoff_retry")
}

// TestGoldenPostmortemRequest captures the post-mortem draft request.
func TestGoldenPostmortemRequest(t *testing.T) {
	c := caseByName(t, "schema-change")
	gs := newGoldenServer(t,
		sseAnswer(t, "msg_golden_pm", goldenMain, "end_turn", sseText(goldenSummaryJSON(t, c.summary))),
	)
	ep := goldenEndpoint(t, gs, EndpointConfig{Provider: ProviderAnthropic})
	if _, err := ep.WritePostmortem(context.Background(), c.events, Options{Model: goldenMain}); err != nil {
		t.Fatalf("postmortem: %v", err)
	}
	checkGoldens(t, gs, "postmortem")
}

// TestGoldenNormaliser pins the one rewrite the goldens rely on.
func TestGoldenNormaliser(t *testing.T) {
	a, b := newStepID(), newStepID()
	in := fmt.Sprintf(`{"x":"%s %s %s","y":"s_0 s_1"}`, a, b, a)
	got := string(normaliseGolden([]byte(in)))
	if want := `{"x":"s_GOLDEN01 s_GOLDEN02 s_GOLDEN01","y":"s_0 s_1"}`; got != want {
		t.Fatalf("normalised = %s, want %s", got, want)
	}
}
