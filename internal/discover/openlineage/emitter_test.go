package openlineage

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/topology"
)

const (
	testKey      = "ol-SECRET-key-123"
	sourceDSN    = "postgres://piperoom:pg-PASSWORD@pg.internal:5432/orders?sslmode=disable"
	warehouseDSN = "postgres://wh:wh-PASSWORD@wh.internal/warehouse"
)

func testConfig(url string) config.Config {
	return config.Config{OpenLineageURL: url, OpenLineageNamespace: "piperoom-test", OpenLineageAPIKey: testKey,
		SourceDBURL: sourceDSN, KafkaBrokers: "kafka-1:9092,kafka-2:9092", WarehouseDBURL: warehouseDSN}
}

// testGraph is a CDC pipeline with column lineage on both data hops, tags,
// owners, a DLQ, a schema subject and facts that must never be sent.
func testGraph() topology.Graph {
	return topology.Graph{
		PipelineID: "orders",
		Nodes: []topology.Node{
			{ID: "pg:public.orders", Kind: topology.KindSourceTable, Name: "public.orders", Layer: "postgres", Owner: "arjun",
				Health: topology.HealthOK, Facts: map[string]string{"slot_lag": "FACT-VALUE"},
				Tags: map[string]string{topology.TagPII: "customer_email:email", topology.TagRetention: "30d"}},
			{ID: "connect:src", Kind: topology.KindConnector, Name: "src", Layer: "kafka-connect"},
			{ID: "sr:pg.public.orders-value", Kind: topology.KindSchemaSubject, Name: "pg.public.orders-value", Layer: "schema-registry"},
			{ID: "topic:pg.public.orders", Kind: topology.KindTopic, Name: "pg.public.orders", Layer: "kafka", Owner: "mei"},
			{ID: "connect:sink", Kind: topology.KindConnector, Name: "sink", Layer: "kafka-connect"},
			{ID: "wh:public.orders", Kind: topology.KindSinkTable, Name: "public.orders", Layer: "warehouse", Owner: "dana",
				Tags: map[string]string{topology.TagPII: "customer_email:email", topology.TagPIIOmitted: "2"}},
			{ID: "dlq:orders-dlq", Kind: topology.KindDLQTopic, Name: "orders-dlq", Layer: "kafka"},
			{ID: "model:revenue", Kind: topology.KindModel, Name: "revenue", Layer: "transform"},
		},
		Edges: []topology.Edge{
			{From: "pg:public.orders", To: "connect:src"},
			{From: "connect:src", To: "sr:pg.public.orders-value", Kind: topology.EdgeDependsOn},
			{From: "connect:src", To: "topic:pg.public.orders", Columns: []topology.ColumnMap{
				{Input: "pg:public.orders", From: "id", To: "after.id"},
				{Input: "pg:public.orders", From: "customer_email", To: "after.customer_email"},
			}},
			{From: "topic:pg.public.orders", To: "connect:sink"},
			{From: "connect:sink", To: "wh:public.orders", Columns: []topology.ColumnMap{
				{Input: "topic:pg.public.orders", From: "after.id", To: "id", Transform: topology.ColumnUnwrap},
				{Input: "topic:pg.public.orders", From: "after.customer_email", To: "email", Transform: topology.ColumnRename, Mapping: topology.MappingInferred},
				{To: "__op", Transform: topology.ColumnAdded},
				{Input: "topic:pg.public.orders", From: "after.discount", Transform: topology.ColumnDropped},
			}},
			{From: "connect:sink", To: "dlq:orders-dlq"},
		},
	}
}

// receiver is an httptest OpenLineage endpoint that records each POST.
type receiver struct {
	srv    *httptest.Server
	mu     sync.Mutex
	bodies [][]byte
	auth   []string
	got    chan struct{}
	status func(n int) int // status of the n-th request (0-based)
	delay  time.Duration
}

func newReceiver(t *testing.T, status func(n int) int) *receiver {
	r := &receiver{got: make(chan struct{}, 100), status: status}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		b, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		n := len(r.bodies)
		r.bodies = append(r.bodies, b)
		r.auth = append(r.auth, req.Header.Get("Authorization")+"|"+req.Header.Get("Content-Type"))
		r.mu.Unlock()
		if r.delay > 0 {
			time.Sleep(r.delay)
		}
		code := http.StatusOK
		if r.status != nil {
			code = r.status(n)
		}
		w.WriteHeader(code)
		r.got <- struct{}{}
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *receiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bodies)
}

func (r *receiver) wait(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for r.count() < n {
		select {
		case <-r.got:
		case <-deadline:
			t.Fatalf("got %d requests, want %d", r.count(), n)
		}
	}
}

// soloElector leads at once, like store.SoloElector.
type soloElector struct{ calls int }

func (s *soloElector) Run(ctx context.Context, _ string, fn func(context.Context)) {
	s.calls++
	fn(ctx)
	<-ctx.Done()
}

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func newLog() (*slog.Logger, *logBuf) {
	b := &logBuf{}
	return slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug})), b
}

// startEmitter runs Start with a solo elector; stop is registered as cleanup.
func startEmitter(t *testing.T, cfg config.Config, mut func(*Options)) (*Emitter, *logBuf) {
	t.Helper()
	log, buf := newLog()
	o := Options{Logger: log, Backoff: func(n int) time.Duration { return time.Millisecond << n }}
	if mut != nil {
		mut(&o)
	}
	own := &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders"}}}
	e, stop, err := start(context.Background(), cfg, own, &soloElector{}, o)
	if err != nil || e == nil {
		t.Fatalf("start = %v, %v", e, err)
	}
	t.Cleanup(stop)
	waitRunning(t, e)
	return e, buf
}

func waitRunning(t *testing.T, e *Emitter) {
	t.Helper()
	for i := 0; i < 500; i++ {
		e.mu.Lock()
		r := e.running
		e.mu.Unlock()
		if r {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("emitter never started")
}

type fakeBuilder struct {
	topology.Builder
	g topology.Graph
}

func (f fakeBuilder) Build(context.Context, string) (topology.Graph, error) { return f.g, nil }

func outputByName(t *testing.T, ev map[string]any, name string) map[string]any {
	t.Helper()
	for _, o := range ev["outputs"].([]any) {
		if m := o.(map[string]any); m["name"] == name {
			return m
		}
	}
	t.Fatalf("no output %q in %v", name, ev["outputs"])
	return nil
}

// TC1 (E1): one build is one event, valid against the vendored schemas,
// with the documented namespaces, names and facets.
func TestTopologyEventShape(t *testing.T) {
	rcv := newReceiver(t, nil)
	e, _ := startEmitter(t, testConfig(rcv.srv.URL+"/api/v1/lineage"), nil)
	g, err := e.Wrap(fakeBuilder{g: testGraph()}).Build(context.Background(), "orders")
	if err != nil || g.PipelineID != "orders" {
		t.Fatalf("Build = %v, %v", g.PipelineID, err)
	}
	rcv.wait(t, 1)
	body := rcv.bodies[0]
	ev := loadSchemas(t).validateEvent(t, body)

	if ev["eventType"] != "COMPLETE" || ev["schemaURL"] != runEventSchemaURL || !strings.HasPrefix(ev["producer"].(string), "urn:piperoom:") {
		t.Errorf("header = %v %v %v", ev["eventType"], ev["schemaURL"], ev["producer"])
	}
	job := ev["job"].(map[string]any)
	if job["namespace"] != "piperoom-test" || job["name"] != "piperoom.pipeline.orders" {
		t.Errorf("job = %v", job)
	}
	jt := job["facets"].(map[string]any)["jobType"].(map[string]any)
	ep := jt["emissionPattern"].(map[string]any)
	if jt["processingType"] != "STREAMING" || jt["jobType"] != "PIPELINE" || ep["windowDuration"] != float64(900) {
		t.Errorf("jobType = %v", jt)
	}
	inputs := ev["inputs"].([]any)
	if len(inputs) != 1 || inputs[0].(map[string]any)["namespace"] != "postgres://pg.internal:5432" ||
		inputs[0].(map[string]any)["name"] != "orders.public.orders" {
		t.Fatalf("inputs = %v", inputs)
	}
	if n := len(ev["outputs"].([]any)); n != 3 { // topic, sink, DLQ; no model, no connector, no subject
		t.Errorf("outputs = %d, want 3", n)
	}
	topic := outputByName(t, ev, "pg.public.orders")
	if topic["namespace"] != "kafka://kafka-1:9092" {
		t.Errorf("topic namespace = %v", topic["namespace"])
	}
	sink := outputByName(t, ev, "warehouse.public.orders")
	if sink["namespace"] != "postgres://wh.internal:5432" {
		t.Errorf("sink namespace = %v", sink["namespace"])
	}
	f := sink["facets"].(map[string]any)
	cl := f["columnLineage"].(map[string]any)["fields"].(map[string]any)
	id := cl["id"].(map[string]any)["inputFields"].([]any)[0].(map[string]any)
	tr := id["transformations"].([]any)[0].(map[string]any)
	if id["namespace"] != "kafka://kafka-1:9092" || id["name"] != "pg.public.orders" || id["field"] != "after.id" ||
		tr["type"] != "DIRECT" || tr["subtype"] != "IDENTITY" || tr["description"] != "unwrap" || tr["masking"] != false {
		t.Errorf("id lineage = %v", id)
	}
	email := cl["email"].(map[string]any)["inputFields"].([]any)[0].(map[string]any)
	if d := email["transformations"].([]any)[0].(map[string]any)["description"]; d != "rename, inferred (name match)" {
		t.Errorf("email description = %v", d)
	}
	if in := cl["__op"].(map[string]any)["inputFields"].([]any); len(in) != 0 {
		t.Errorf("added column inputs = %v", in)
	}
	if _, ok := cl["discount"]; ok || len(cl) != 3 {
		t.Errorf("columnLineage fields = %v (a dropped column has no slot)", cl)
	}
	if own := f["ownership"].(map[string]any)["owners"].([]any)[0].(map[string]any); own["name"] != "user:dana" || own["type"] != "MAINTAINER" {
		t.Errorf("ownership = %v", own)
	}
	tags := f["tags"].(map[string]any)["tags"].([]any)
	if len(tags) != 2 || tags[0].(map[string]any)["field"] != "customer_email" || tags[0].(map[string]any)["value"] != "email" ||
		tags[1].(map[string]any)["key"] != "pii_omitted" {
		t.Errorf("sink tags = %v", tags)
	}
	src := inputs[0].(map[string]any)["facets"].(map[string]any)
	if fields := src["schema"].(map[string]any)["fields"].([]any); len(fields) != 2 {
		t.Errorf("source schema = %v", fields)
	}
	if _, ok := src["columnLineage"]; ok {
		t.Error("an input carries columnLineage")
	}
	for _, never := range []string{"pg-PASSWORD", "wh-PASSWORD", "piperoom:pg", "sslmode", "FACT-VALUE", "slot_lag", "health", testKey} {
		if bytes.Contains(body, []byte(never)) {
			t.Errorf("body holds %q", never)
		}
	}
	if rcv.auth[0] != "Bearer "+testKey+"|application/json" {
		t.Errorf("headers = %q", rcv.auth[0])
	}
}

// TC2: a receiver answering 500 gets the event 1 + 3 times with backoff,
// then one warning; the build that handed it over returned at once.
func TestRetryThenWarn(t *testing.T) {
	rcv := newReceiver(t, func(int) int { return http.StatusInternalServerError })
	rcv.delay = 50 * time.Millisecond
	e, logs := startEmitter(t, testConfig(rcv.srv.URL), nil)
	b := e.Wrap(fakeBuilder{g: testGraph()})
	start := time.Now()
	for i := 0; i < 5; i++ {
		if _, err := b.Build(context.Background(), "orders"); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d > 40*time.Millisecond {
		t.Errorf("5 builds took %v while the receiver was failing: the emitter blocks the caller", d)
	}
	rcv.wait(t, 1+config.OpenLineageMaxRetries)
	waitLog(t, logs, "event not accepted")
	time.Sleep(30 * time.Millisecond)
	if n := rcv.count(); n != 4 {
		t.Errorf("requests = %d, want 4", n)
	}
	l := logs.String()
	if strings.Count(l, "event not accepted") != 1 || !strings.Contains(l, "attempts=4") || !strings.Contains(l, "status=500") ||
		!strings.Contains(l, "pipeline=orders") || !strings.Contains(l, "level=WARN") {
		t.Errorf("log = %s", l)
	}
}

// A 4xx other than 429 is not retried; a 429 is.
func TestNoRetryOn4xx(t *testing.T) {
	for _, tc := range []struct {
		status, want int
	}{{http.StatusBadRequest, 1}, {http.StatusTooManyRequests, 4}, {http.StatusFound, 1}} {
		rcv := newReceiver(t, func(int) int { return tc.status })
		e, logs := startEmitter(t, testConfig(rcv.srv.URL), nil)
		e.Topology(testGraph())
		waitLog(t, logs, "event not accepted")
		if n := rcv.count(); n != tc.want {
			t.Errorf("status %d: %d requests, want %d", tc.status, n, tc.want)
		}
	}
}

func waitLog(t *testing.T, l *logBuf, s string) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		if strings.Contains(l.String(), s) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("log never said %q: %s", s, l.String())
}

// TC3: no OPENLINEAGE_URL, no emitter, no goroutine, no elector run; every
// method of the nil emitter is a no-op.
func TestOffStartsNothing(t *testing.T) {
	el := &soloElector{}
	log, _ := newLog()
	e, stop, err := Start(context.Background(), config.Config{}, &owners.Owners{}, el, log)
	if err != nil || e != nil || el.calls != 0 {
		t.Fatalf("Start = %v, %v, elector calls %d", e, err, el.calls)
	}
	stop()
	b := &fakeBuilder{g: testGraph()}
	if got := e.Wrap(b); got != topology.Builder(b) {
		t.Error("Wrap on an off emitter changed the builder")
	}
	if e.ActionHook() != nil {
		t.Error("an off emitter has an action hook")
	}
	e.Topology(testGraph())
	e.ActionExecuted(context.Background(), "orders", events.ProposalStep{Action: "x"}, events.ActionExecutedPayload{})
	e.Run(context.Background())
}

// A bad setting refuses start: an API key over plain http to a remote host.
func TestStartRefusesKeyOverHTTP(t *testing.T) {
	log, buf := newLog()
	_, _, err := Start(context.Background(), testConfig("http://marquez.example:5000/api/v1/lineage"), &owners.Owners{}, &soloElector{}, log)
	if err == nil || strings.Contains(err.Error(), testKey) || strings.Contains(buf.String(), testKey) {
		t.Fatalf("err = %v", err)
	}
}

// TC4: the API key reaches the Authorization header only: never a log line
// (start, warnings, network errors) nor a body. A token in the URL's query
// does not reach the log either.
func TestAPIKeyNeverLogged(t *testing.T) {
	rcv := newReceiver(t, func(int) int { return http.StatusServiceUnavailable })
	e, logs := startEmitter(t, testConfig(rcv.srv.URL), nil)
	e.Topology(testGraph())
	e.ActionExecuted(context.Background(), "orders", events.ProposalStep{Action: "connect_restart_task"}, events.ActionExecutedPayload{OK: true})
	waitLog(t, logs, "event not accepted")
	rcv.wait(t, 8)
	for i, b := range rcv.bodies {
		if bytes.Contains(b, []byte(testKey)) {
			t.Errorf("body %d holds the key", i)
		}
	}
	// A dead endpoint: the network error is logged without the URL.
	cfg := testConfig("http://127.0.0.1:1/lineage?token=QUERY-SECRET")
	e2, logs2 := startEmitter(t, cfg, nil)
	e2.Topology(testGraph())
	waitLog(t, logs2, "event not accepted")
	all := logs.String() + logs2.String()
	if !strings.Contains(all, "api_key_set=true") {
		t.Errorf("start log does not say a key is set: %s", all)
	}
	for _, never := range []string{testKey, "QUERY-SECRET", "pg-PASSWORD", "wh-PASSWORD"} {
		if strings.Contains(all, never) {
			t.Errorf("log holds %q: %s", never, all)
		}
	}
}

// TC5: ten builds within a minute are one event; the first build after 15
// minutes is the next.
func TestRateLimitPerPipeline(t *testing.T) {
	rcv := newReceiver(t, nil)
	now := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	e, _ := startEmitter(t, testConfig(rcv.srv.URL), func(o *Options) { o.Now = clock })
	b := e.Wrap(fakeBuilder{g: testGraph()})
	for i := 0; i < 10; i++ {
		if _, err := b.Build(context.Background(), "orders"); err != nil {
			t.Fatal(err)
		}
		advance(6 * time.Second)
	}
	rcv.wait(t, 1)
	time.Sleep(50 * time.Millisecond)
	if n := rcv.count(); n != 1 {
		t.Fatalf("10 builds in a minute sent %d events, want 1", n)
	}
	advance(config.OpenLineageEmitInterval)
	if _, err := b.Build(context.Background(), "orders"); err != nil {
		t.Fatal(err)
	}
	rcv.wait(t, 2)
}

// The action event: job piperoom.<action>, COMPLETE/FAIL/OTHER, the
// piperoom_action run facet, outputs only for the step's topics and tables
// on the pipeline's map; the step output never leaves.
func TestActionEvent(t *testing.T) {
	rcv := newReceiver(t, nil)
	e, _ := startEmitter(t, testConfig(rcv.srv.URL), nil)
	s := loadSchemas(t)
	e.Topology(testGraph())
	rcv.wait(t, 1)
	cases := []struct {
		step     events.ProposalStep
		res      events.ActionExecutedPayload
		typ      string
		outputs  []string
		noOutput bool
	}{
		{events.ProposalStep{Action: "kafka_replay_range", Layer: "kafka", Params: json.RawMessage(`{"topic":"pg.public.orders","to_topic":"other.topic"}`)},
			events.ActionExecutedPayload{ProposalID: "p1", StepIndex: 2, OK: true, Output: "ROW-VALUE 42"}, "COMPLETE", []string{"pg.public.orders"}, false},
		{events.ProposalStep{Action: "warehouse_backfill", Layer: "warehouse", Params: json.RawMessage(`{"table":"public.orders"}`)},
			events.ActionExecutedPayload{ProposalID: "p2"}, "FAIL", []string{"warehouse.public.orders"}, false},
		{events.ProposalStep{Action: "pg_vacuum", Layer: "postgres", Params: json.RawMessage(`{"tables":["public.orders","public.nope"]}`)},
			events.ActionExecutedPayload{ProposalID: "p3", OutcomeUnknown: true}, "OTHER", []string{"orders.public.orders"}, false},
		// A table param of another layer that names both a source and a
		// sink table is not guessed.
		{events.ProposalStep{Action: "row_check", Layer: "consumer", Params: json.RawMessage(`{"table":"public.orders"}`)},
			events.ActionExecutedPayload{ProposalID: "p4", OK: true}, "COMPLETE", nil, true},
		{events.ProposalStep{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"src","task":0}`)},
			events.ActionExecutedPayload{ProposalID: "p5", OK: true}, "COMPLETE", nil, true},
	}
	for i, tc := range cases {
		e.ActionExecuted(context.Background(), "orders", tc.step, tc.res)
		rcv.wait(t, i+2)
		body := rcv.bodies[i+1]
		ev := s.validateEvent(t, body)
		job := ev["job"].(map[string]any)
		jt := job["facets"].(map[string]any)["jobType"].(map[string]any)
		if ev["eventType"] != tc.typ || job["name"] != "piperoom."+tc.step.Action || job["namespace"] != "piperoom-test" ||
			jt["processingType"] != "BATCH" || jt["jobType"] != "ACTION" {
			t.Errorf("%s: event = %v %v %v", tc.step.Action, ev["eventType"], job, jt)
		}
		fa := ev["run"].(map[string]any)["facets"].(map[string]any)["piperoom_action"].(map[string]any)
		if fa["pipeline_id"] != "orders" || fa["proposal_id"] != tc.res.ProposalID || fa["step_index"] != float64(tc.res.StepIndex) {
			t.Errorf("%s: facet = %v", tc.step.Action, fa)
		}
		var names []string
		for _, o := range ev["outputs"].([]any) {
			names = append(names, o.(map[string]any)["name"].(string))
		}
		if strings.Join(names, ",") != strings.Join(tc.outputs, ",") {
			t.Errorf("%s: outputs = %v, want %v", tc.step.Action, names, tc.outputs)
		}
		for _, never := range []string{"ROW-VALUE", "other.topic", "public.nope", `"src"`, "params"} {
			if bytes.Contains(body, []byte(never)) {
				t.Errorf("%s: body holds %q", tc.step.Action, never)
			}
		}
	}
	// A name that is not an action name is never put in a job.
	e.ActionExecuted(context.Background(), "orders", events.ProposalStep{Action: "Bad Name\n"}, events.ActionExecutedPayload{})
	time.Sleep(30 * time.Millisecond)
	if n := rcv.count(); n != len(cases)+1 {
		t.Errorf("requests = %d, want %d", n, len(cases)+1)
	}
}

// An event over 1 MiB goes without its column facets; one still over is
// dropped with a warning.
func TestEventSizeCap(t *testing.T) {
	rcv := newReceiver(t, nil)
	e, logs := startEmitter(t, testConfig(rcv.srv.URL), nil)
	wide := testGraph()
	var cols []topology.ColumnMap
	for i := 0; i < topology.MaxEdgeColumns; i++ {
		c := strings.Repeat("c", 600) + string(rune('a'+i%26)) + strings.Repeat("x", i%40)
		cols = append(cols, topology.ColumnMap{Input: "topic:pg.public.orders", From: "after." + c, To: c})
	}
	wide.Edges[4].Columns = cols
	e.Topology(wide)
	rcv.wait(t, 1)
	if b := rcv.bodies[0]; len(b) > config.OpenLineageMaxEventBytes || bytes.Contains(b, []byte(`"columnLineage"`)) || bytes.Contains(b, []byte(`"schema"`)) {
		t.Errorf("oversized event sent with %d bytes / column facets", len(b))
	}
	loadSchemas(t).validateEvent(t, rcv.bodies[0])

	huge := topology.Graph{PipelineID: "orders"}
	for i := 0; i < 1200; i++ {
		huge.Nodes = append(huge.Nodes, topology.Node{ID: "topic:t" + string(rune(i)), Kind: topology.KindTopic, Name: strings.Repeat("t", 1000) + string(rune(0x4e00+i))})
	}
	e.mu.Lock()
	e.last = map[string]time.Time{}
	e.mu.Unlock()
	e.Topology(huge)
	waitLog(t, logs, "event too large")
	if n := rcv.count(); n != 1 {
		t.Errorf("requests = %d, want 1", n)
	}
}

// The queue drops the oldest event when full, and says how many.
func TestQueueDropsOldest(t *testing.T) {
	release := make(chan struct{})
	got := make(chan []byte, 10)
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- b
		once.Do(func() { <-release }) // hold the first POST
	}))
	t.Cleanup(srv.Close)
	e, logs := startEmitter(t, testConfig(srv.URL), func(o *Options) { o.QueueSize = 2 })
	step := events.ProposalStep{Action: "connect_restart_task"}
	e.ActionExecuted(context.Background(), "orders", step, events.ActionExecutedPayload{ProposalID: "p0"})
	var bodies [][]byte
	bodies = append(bodies, <-got)
	for i := 1; i <= 4; i++ {
		e.ActionExecuted(context.Background(), "orders", step, events.ActionExecutedPayload{ProposalID: "p" + string(rune('0'+i))})
	}
	close(release)
	for len(bodies) < 3 {
		select {
		case b := <-got:
			bodies = append(bodies, b)
		case <-time.After(5 * time.Second):
			t.Fatalf("got %d events, want 3", len(bodies))
		}
	}
	waitLog(t, logs, "dropped=2")
	select {
	case <-got:
		t.Error("a dropped event was sent")
	case <-time.After(30 * time.Millisecond):
	}
	if !bytes.Contains(bodies[1], []byte(`"p3"`)) || !bytes.Contains(bodies[2], []byte(`"p4"`)) {
		t.Errorf("kept %s / %s, want p3 and p4", bodies[1], bodies[2])
	}
}

// Not leading: what is handed over is not queued and does not start the
// rate limit; the goroutine stops with its context, even mid-backoff.
func TestOnlyWhileRunningAndStops(t *testing.T) {
	rcv := newReceiver(t, func(int) int { return http.StatusBadGateway })
	ol, _ := config.ParseOpenLineage(testConfig(rcv.srv.URL))
	log, _ := newLog()
	e := New(ol, NamespacesFromConfig(testConfig("")), Options{Logger: log, Backoff: func(int) time.Duration { return time.Hour }})
	e.Topology(testGraph())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.Run(ctx); close(done) }()
	waitRunning(t, e)
	if rcv.count() != 0 {
		t.Fatal("an event handed over before Run was sent")
	}
	e.Topology(testGraph())
	rcv.wait(t, 1) // then waits an hour to retry
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop with its context")
	}
}

// Section 6.2: namespaces carry host and port only, with default ports, and
// never credentials or query parameters.
func TestNamespacesFromConfig(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cfg           config.Config
		source, srcDB string
		kafka         string
		wh, whDB      string
	}{
		{"postgres url", config.Config{SourceDBURL: sourceDSN, KafkaBrokers: " broker:29092 ,b2:1", WarehouseDBURL: warehouseDSN},
			"postgres://pg.internal:5432", "orders", "kafka://broker:29092", "postgres://wh.internal:5432", "warehouse"},
		{"keyword dsn", config.Config{SourceDBURL: "host=PG.example port=6543 dbname=shop user=u password=pw-SECRET", KafkaBrokers: "k",
			WarehouseKind: "clickhouse", WarehouseDBURL: "clickhouse://r:ch-SECRET@ch.example:9440/analytics?secure=true"},
			"postgres://pg.example:6543", "shop", "kafka://k:9092", "clickhouse://ch.example:9440", "analytics"},
		{"bigquery", config.Config{WarehouseKind: "bigquery", WarehouseDBURL: "bigquery://my-proj/us?billing_project=b"},
			"", "", "", "bigquery", "my-proj"},
		{"databricks", config.Config{WarehouseKind: "databricks", WarehouseDBURL: "token:dapi-SECRET@adb-1.azuredatabricks.net/sql/1.0/warehouses/x?catalog=main"},
			"", "", "", "unitycatalog://adb-1.azuredatabricks.net", "main"},
		{"snowflake", config.Config{WarehouseKind: "SNOWFLAKE", WarehouseDBURL: "svc:sf%40SECRET@MyOrg-Acct/ANALYTICS/PUBLIC?warehouse=WH"},
			"", "", "", "snowflake://myorg-acct", "ANALYTICS"},
		{"unreadable", config.Config{SourceDBURL: "postgres://h1,h2/db", WarehouseKind: "snowflake", WarehouseDBURL: "junk"},
			"", "", "", "", ""},
	} {
		n := NamespacesFromConfig(tc.cfg)
		if n.Source != tc.source || n.SourceDB != tc.srcDB || n.Kafka != tc.kafka || n.Warehouse != tc.wh || n.WarehouseDB != tc.whDB {
			t.Errorf("%s: %+v", tc.name, n)
		}
		if strings.Contains(strings.ToUpper(n.Source+n.Kafka+n.Warehouse+n.WarehouseDB), "SECRET") {
			t.Errorf("%s: a credential reached a namespace: %+v", tc.name, n)
		}
	}
}

// Section 6.2 names per warehouse kind; nodes this process cannot name are
// left out.
func TestDatasetNames(t *testing.T) {
	ch := Namespaces{WarehouseKind: "clickhouse", Warehouse: "clickhouse://ch:9000", WarehouseDB: "analytics"}
	bq := Namespaces{WarehouseKind: "bigquery", Warehouse: "bigquery", WarehouseDB: "proj"}
	pg := Namespaces{Source: "postgres://pg:5432", SourceDB: "orders", WarehouseKind: "postgres", Warehouse: "postgres://wh:5432", WarehouseDB: "wh"}
	for _, tc := range []struct {
		ns   Namespaces
		node topology.Node
		want string // "" = left out
	}{
		{ch, topology.Node{ID: "kafka_engine:analytics.orders_kafka", Kind: topology.KindKafkaEngine, Name: "analytics.orders_kafka"}, "analytics.orders_kafka"},
		{ch, topology.Node{ID: "wh:orders", Kind: topology.KindSinkTable, Name: "orders"}, "analytics.orders"},
		{bq, topology.Node{ID: "wh:sales.orders", Kind: topology.KindSinkTable, Name: "sales.orders"}, "proj.sales.orders"},
		{bq, topology.Node{ID: "wh:orders", Kind: topology.KindSinkTable, Name: "orders"}, ""},
		{bq, topology.Node{ID: "kafka_engine:x.y", Kind: topology.KindKafkaEngine, Name: "x.y"}, ""},
		{pg, topology.Node{ID: "pg:orders", Kind: topology.KindSourceTable, Name: "orders"}, "orders.public.orders"},
		{pg, topology.Node{ID: "mysql:shop.orders", Kind: topology.KindSourceTable, Name: "shop.orders"}, ""},
		{pg, topology.Node{ID: "wh:clickhouse:public.orders", Kind: topology.KindSinkTable, Name: "public.orders"}, ""},
		{pg, topology.Node{ID: "wh:postgres:public.orders", Kind: topology.KindSinkTable, Name: "public.orders"}, "wh.public.orders"},
		{pg, topology.Node{ID: "model:revenue", Kind: topology.KindModel, Name: "revenue"}, ""},
		{pg, topology.Node{ID: "topic:t", Kind: topology.KindTopic, Name: "t"}, ""}, // no Kafka namespace
	} {
		_, name, ok := tc.ns.dataset(tc.node)
		if (tc.want == "") == ok || name != tc.want {
			t.Errorf("%s: %q %v, want %q", tc.node.ID, name, ok, tc.want)
		}
	}
}

func TestRunIDIsUUIDv7(t *testing.T) {
	now := time.UnixMilli(0x0192f3a01234)
	a, b := newRunID(now), newRunID(now)
	if !uuidRE.MatchString(a) || a == b || !strings.HasPrefix(a, "0192f3a0-1234-7") || !strings.ContainsAny(a[19:20], "89ab") {
		t.Errorf("run ids %s %s", a, b)
	}
}
