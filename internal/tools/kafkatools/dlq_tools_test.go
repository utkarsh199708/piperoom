package kafkatools

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakeDLQ adds clients.DLQReader to the plain fakeKafka and counts its calls.
type fakeDLQ struct {
	*fakeKafka
	mu       sync.Mutex
	offsets  map[string][]clients.PartitionOffsets
	recs     []clients.DLQRecord
	offErr   error
	peekErr  error
	offCalls []string
	peeks    []int // the limit of every PeekDLQ call
	deadline bool  // set when a call saw a context with a deadline
}

func (f *fakeDLQ) DLQOffsets(ctx context.Context, topics ...string) (map[string][]clients.PartitionOffsets, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.offCalls = append(f.offCalls, strings.Join(topics, ","))
	if _, ok := ctx.Deadline(); ok {
		f.deadline = true
	}
	return f.offsets, f.offErr
}

func (f *fakeDLQ) PeekDLQ(ctx context.Context, topic string, limit int) ([]clients.DLQRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.peeks = append(f.peeks, limit)
	return f.recs, f.peekErr
}

func (f *fakeDLQ) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.offCalls) + len(f.peeks)
}

var _ clients.DLQReader = (*fakeDLQ)(nil)

const dlqYAML = `
pipelines:
  - id: orders
    title: Orders CDC
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector, pg-orders-sink, pg-plain-sink]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]
    sink_ts_column: updated_at
    watch:
      dlq_topics:
        - {topic: order-sync-dlq, written_by: order-sync}
%s  - id: billing
    title: Billing
    topic_prefix: bill
    source_tables: [public.invoices]
    connectors: [bill-sink]
    consumer_groups: [bill-sync]
    sink_tables: [analytics.invoices]
    sink_ts_column: updated_at
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
`

// dlqOwners loads the fixture with the given dlq_peek.headers mode ("" = unset).
func dlqOwners(t *testing.T, mode string) *owners.Owners {
	t.Helper()
	extra := ""
	if mode != "" {
		extra = "      dlq_peek:\n        headers: " + mode + "\n"
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(strings.Replace(dlqYAML, "%s", extra, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return o
}

const sinkClass = "io.confluent.connect.jdbc.JdbcSinkConnector"

func dlqDeps(t *testing.T, mode string, k *fakeDLQ, cfgs map[string]map[string]string) Deps {
	t.Helper()
	fc := &fakeConnect{configFn: func(_ context.Context, n string) (map[string]string, error) {
		c, ok := cfgs[n]
		if !ok {
			return nil, errors.New("connect: 404")
		}
		return c, nil
	}}
	guardWrites(t, fc)
	return Deps{Connect: fc, Kafka: k, Owners: dlqOwners(t, mode)}
}

func defaultCfgs() map[string]map[string]string {
	return map[string]map[string]string{
		"pg-orders-sink": {
			"connector.class": sinkClass, "errors.tolerance": "all",
			"errors.deadletterqueue.topic.name":             "orders-dlq",
			"errors.deadletterqueue.context.headers.enable": "true",
			"errors.log.enable":                             "true",
			"password":                                      "hunter2",
		},
		"pg-plain-sink":       {"connector.class": sinkClass},
		"pg-orders-connector": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector"},
	}
}

func ts(min int) time.Time { return time.Date(2026, 10, 13, 9, min, 0, 0, time.UTC) }

func hdr(class, msg string, extra ...string) map[string]string {
	h := map[string]string{
		"__connect.errors.exception.class.name": class,
		"__connect.errors.stage":                "VALUE_CONVERTER",
		"__connect.errors.class.name":           "org.apache.kafka.connect.json.JsonConverter",
		"__connect.errors.connector.name":       "pg-orders-sink",
		"__connect.errors.task.id":              "0",
		"__connect.errors.topic":                "pg.public.orders",
		"__connect.errors.partition":            "1",
		"__connect.errors.offset":               "25377154",
		"__connect.errors.exception.message":    msg,
		"__connect.errors.exception.stacktrace": "org.apache.kafka.connect.errors.DataException: " + msg + "\n\tat X",
	}
	for i := 0; i+1 < len(extra); i += 2 {
		h[extra[i]] = extra[i+1]
	}
	return h
}

func okOffsets() map[string][]clients.PartitionOffsets {
	return map[string][]clients.PartitionOffsets{"orders-dlq": {{Partition: 0, Start: 10, End: 60}, {Partition: 1, Start: 0, End: 5}}}
}

func peekTool(t *testing.T, d Deps) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tl, ok := reg.Get("dlq_peek")
	if !ok {
		t.Fatal("dlq_peek is not registered")
	}
	return tl
}

func scoped() context.Context { return tools.WithPipelineID(context.Background(), "orders") }

func TestDLQPeekRegisteredWithEmptyDeps(t *testing.T) {
	tl := peekTool(t, Deps{})
	if tl.Kind() != tools.KindRead {
		t.Fatalf("kind = %s", tl.Kind())
	}
	_, err := tl.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"connector":"x"}`))
	if err == nil || !strings.Contains(err.Error(), "Kafka client cannot read DLQ topics") {
		t.Fatalf("err = %v, want the clear 'cannot read DLQ topics' error", err)
	}
	// A KafkaAdmin without the DLQ methods gets the same error.
	tl = peekTool(t, Deps{Kafka: &fakeKafka{}})
	_, err = tl.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"topic":"x"}`))
	if err == nil || !strings.Contains(err.Error(), "cannot read DLQ topics") {
		t.Fatalf("err = %v", err)
	}
}

func TestDLQPeekInputValidation(t *testing.T) {
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets()}
	tl := peekTool(t, dlqDeps(t, "", k, defaultCfgs()))
	tests := []struct {
		name, in string
	}{
		{"neither", `{}`},
		{"both", `{"connector":"pg-orders-sink","topic":"order-sync-dlq"}`},
		{"limit zero", `{"connector":"pg-orders-sink","limit":0}`},
		{"limit negative", `{"connector":"pg-orders-sink","limit":-3}`},
		{"limit string", `{"connector":"pg-orders-sink","limit":"5"}`},
		{"unknown field", `{"connector":"pg-orders-sink","pipeline_id":"billing"}`},
		{"not json", `{`},
		{"connector wrong type", `{"connector":5}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tl.Run(scoped(), json.RawMessage(tc.in))
			if !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("err = %v, want ErrBadInput", err)
			}
		})
	}
	if k.calls() != 0 {
		t.Fatalf("bad input reached Kafka: %d calls", k.calls())
	}
	schema := tl.InputSchema()
	if schema["additionalProperties"] != false {
		t.Fatalf("schema allows extra properties: %v", schema)
	}
}

func TestDLQPeekLimitCapAndDefault(t *testing.T) {
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets()}
	tl := peekTool(t, dlqDeps(t, "", k, defaultCfgs()))
	for _, tc := range []struct {
		in   string
		want int
	}{
		{`{"connector":"pg-orders-sink"}`, 5},
		{`{"connector":"pg-orders-sink","limit":1}`, 1},
		{`{"connector":"pg-orders-sink","limit":20}`, 20},
		{`{"connector":"pg-orders-sink","limit":500}`, 20},
	} {
		k.peeks = nil
		if _, err := tl.Run(scoped(), json.RawMessage(tc.in)); err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if len(k.peeks) != 1 || k.peeks[0] != tc.want {
			t.Fatalf("%s: PeekDLQ limits = %v, want [%d]", tc.in, k.peeks, tc.want)
		}
	}
	// The reader may return more than asked for; the tool still shows <= limit.
	for i := 0; i < 30; i++ {
		k.recs = append(k.recs, clients.DLQRecord{Topic: "orders-dlq", Offset: int64(i), Timestamp: ts(i), Headers: hdr("org.apache.kafka.connect.errors.DataException", "m")})
	}
	r, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink","limit":3}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Data.(dlqData).Sampled; got != 3 {
		t.Fatalf("sampled = %d, want 3", got)
	}
}

func TestDLQPeekScope(t *testing.T) {
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets()}
	d := dlqDeps(t, "", k, defaultCfgs())
	d.Connect.(*fakeConnect).configFn = func(context.Context, string) (map[string]string, error) {
		t.Error("Connect was called for an out-of-scope name")
		return nil, errors.New("unreachable")
	}
	tl := peekTool(t, d)
	for _, tc := range []struct{ name, in string }{
		{"connector of another pipeline", `{"connector":"bill-sink"}`},
		{"undeclared topic", `{"topic":"orders-dlq"}`},
		{"CDC topic", `{"topic":"pg.public.orders"}`},
		{"other pipeline topic", `{"topic":"bill-dlq"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tl.Run(scoped(), json.RawMessage(tc.in))
			if !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("err = %v, want ErrOutOfScope", err)
			}
		})
	}
	if k.calls() != 0 {
		t.Fatalf("out-of-scope input reached Kafka: %d calls", k.calls())
	}
	if _, err := tl.Run(context.Background(), json.RawMessage(`{"connector":"pg-orders-sink"}`)); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: err = %v, want ErrNoScope", err)
	}
}

func TestDLQPeekConnectorWithoutDLQ(t *testing.T) {
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets()}
	cfgs := defaultCfgs()
	cfgs["pg-plain-sink"] = map[string]string{"connector.class": sinkClass, "errors.tolerance": "all"}
	tl := peekTool(t, dlqDeps(t, "", k, cfgs))
	r, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-plain-sink"}`))
	if err != nil {
		t.Fatalf("a connector without a DLQ must not be an error: %v", err)
	}
	if !strings.Contains(r.Summary, "has no DLQ topic") || !strings.Contains(r.Summary, "silent_drop_sink") {
		t.Fatalf("summary = %q", r.Summary)
	}
	if k.calls() != 0 {
		t.Fatalf("Kafka was called %d times for a connector with no DLQ", k.calls())
	}
	// The debezium source: no DLQ either.
	r, err = tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err != nil || !strings.Contains(r.Summary, "has no DLQ topic") {
		t.Fatalf("source: %v %q", err, r.Summary)
	}
	// An invalid topic name in the config is said, not read.
	cfgs["pg-plain-sink"]["errors.deadletterqueue.topic.name"] = "bad name/x"
	r, err = tl.Run(scoped(), json.RawMessage(`{"connector":"pg-plain-sink"}`))
	if err != nil || !strings.Contains(r.Summary, "not a valid Kafka topic name") || strings.Contains(r.Summary, "bad name") {
		t.Fatalf("invalid topic: %v %q", err, r.Summary)
	}
	if k.calls() != 0 {
		t.Fatalf("Kafka was called for an invalid topic name")
	}
}

func TestDLQPeekConnectorResolvesTopic(t *testing.T) {
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(),
		recs: []clients.DLQRecord{{Topic: "orders-dlq", Partition: 1, Offset: 412, Timestamp: ts(58), Headers: hdr("org.apache.kafka.connect.errors.DataException", "bad")}}}
	tl := peekTool(t, dlqDeps(t, "", k, defaultCfgs()))
	r, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(k.offCalls) != 1 || k.offCalls[0] != "orders-dlq" {
		t.Fatalf("DLQOffsets topics = %v, want [orders-dlq]", k.offCalls)
	}
	d := r.Data.(dlqData)
	if d.Topic != "orders-dlq" || d.Writer != "pg-orders-sink" || d.Declared || d.Records != 55 || d.Partitions != 2 {
		t.Fatalf("data = %+v", d)
	}
	if d.ContextHeaders == nil || !*d.ContextHeaders {
		t.Fatalf("context_headers = %v", d.ContextHeaders)
	}
	if !k.deadline {
		t.Fatal("Kafka call had no deadline")
	}
	for _, want := range []string{"orders-dlq (DLQ of pg-orders-sink): 55 records in 2 partitions", "last hour: unknown (no watcher in this process)", "DataException x1", "p1@412 2026-10-13T09:58:00Z org.apache.kafka.connect.errors.DataException stage=VALUE_CONVERTER"} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, r.Summary)
		}
	}
	// Config errors are errors (the connector is in scope but Connect fails).
	cfgs := defaultCfgs()
	delete(cfgs, "pg-orders-sink")
	tl = peekTool(t, dlqDeps(t, "", k, cfgs))
	if _, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`)); err == nil {
		t.Fatal("want the Connect error")
	}
}

func TestDLQPeekDeclaredTopic(t *testing.T) {
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: map[string][]clients.PartitionOffsets{"order-sync-dlq": {{Partition: 0, Start: 0, End: 7}}},
		recs: []clients.DLQRecord{{Topic: "order-sync-dlq", Offset: 6, Timestamp: ts(1)}}}
	d := dlqDeps(t, "", k, nil)
	d.Connect = nil // topic mode needs no Connect
	tl := peekTool(t, d)
	r, err := tl.Run(scoped(), json.RawMessage(`{"topic":"order-sync-dlq"}`))
	if err != nil {
		t.Fatal(err)
	}
	dd := r.Data.(dlqData)
	if !dd.Declared || dd.Writer != "order-sync" || dd.Records != 7 {
		t.Fatalf("data = %+v", dd)
	}
	if !strings.Contains(r.Summary, "no Connect error headers on these records") || !strings.Contains(r.Summary, "p0@6") {
		t.Fatalf("summary:\n%s", r.Summary)
	}
}

func TestDLQPeekMissingAndEmptyTopic(t *testing.T) {
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: map[string][]clients.PartitionOffsets{}}
	tl := peekTool(t, dlqDeps(t, "", k, defaultCfgs()))
	r, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
	if err != nil {
		t.Fatalf("a missing topic is not an error: %v", err)
	}
	if !strings.Contains(r.Summary, "does not exist") || !strings.Contains(r.Summary, "dlq_topic_missing") {
		t.Fatalf("summary:\n%s", r.Summary)
	}
	if len(k.peeks) != 0 {
		t.Fatal("PeekDLQ called for a missing topic")
	}
	// Declared: healthy absence, no risk.
	d := dlqDeps(t, "", k, nil)
	d.Connect = nil
	r, err = peekTool(t, d).Run(scoped(), json.RawMessage(`{"topic":"order-sync-dlq"}`))
	if err != nil || !strings.Contains(r.Summary, "does not exist") || strings.Contains(r.Summary, "dlq_topic_missing") {
		t.Fatalf("declared: %v\n%s", err, r.Summary)
	}
	// Existing but empty topic: no peek.
	k.offsets = map[string][]clients.PartitionOffsets{"orders-dlq": {{Partition: 0, Start: 4, End: 4}}}
	r, err = peekTool(t, dlqDeps(t, "", k, defaultCfgs())).Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
	if err != nil || len(k.peeks) != 0 || !strings.Contains(r.Summary, "0 records") {
		t.Fatalf("empty: %v peeks=%v\n%s", err, k.peeks, r.Summary)
	}
}

func TestDLQPeekClientErrors(t *testing.T) {
	boom := errors.New("kafka: down")
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), offErr: boom}
	tl := peekTool(t, dlqDeps(t, "", k, defaultCfgs()))
	if _, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`)); !errors.Is(err, boom) {
		t.Fatalf("offsets err = %v", err)
	}
	k.offErr, k.peekErr = nil, boom
	if _, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`)); !errors.Is(err, boom) {
		t.Fatalf("peek err = %v", err)
	}
}

func TestDLQPeekHonoursCancelledContext(t *testing.T) {
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets()}
	k.offErr = context.Canceled
	tl := peekTool(t, dlqDeps(t, "", k, defaultCfgs()))
	ctx, cancel := context.WithCancel(scoped())
	cancel()
	if _, err := tl.Run(ctx, json.RawMessage(`{"connector":"pg-orders-sink"}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func modeRecs() []clients.DLQRecord {
	return []clients.DLQRecord{
		{Topic: "orders-dlq", Partition: 1, Offset: 412, Timestamp: ts(58), Headers: hdr("org.apache.kafka.connect.errors.DataException", "Unexpected character ('x')")},
		{Topic: "orders-dlq", Partition: 0, Offset: 389, Timestamp: ts(57), Headers: hdr("org.apache.kafka.connect.errors.DataException", "m2")},
		{Topic: "orders-dlq", Partition: 0, Offset: 388, Timestamp: ts(56), Headers: hdr("org.apache.kafka.connect.errors.SerializationException", "m3")},
	}
}

func TestDLQPeekHeaderModes(t *testing.T) {
	tests := []struct {
		mode    string
		want    []string
		wantNot []string
	}{
		{"counts_only", []string{"headers: counts_only", "p1@412 2026-10-13T09:58:00Z", "DataException x2"},
			[]string{"stage=", "VALUE_CONVERTER", "JsonConverter", "pg.public.orders", "Unexpected character", "task="}},
		{"classes", []string{"headers: classes", "p1@412 2026-10-13T09:58:00Z org.apache.kafka.connect.errors.DataException stage=VALUE_CONVERTER class=org.apache.kafka.connect.json.JsonConverter connector=pg-orders-sink task=0 from pg.public.orders/1@25377154", "SerializationException x1"},
			[]string{"Unexpected character", "msg=", "trace="}},
		{"messages", []string{"headers: messages", "stage=VALUE_CONVERTER", `msg="Unexpected character ('x')"`, `trace="org.apache.kafka.connect.errors.DataException: Unexpected character ('x')"`},
			nil},
		{"", []string{"headers: classes"}, []string{"msg="}}, // unset = default
	}
	for _, tc := range tests {
		t.Run("mode_"+tc.mode, func(t *testing.T) {
			k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), recs: modeRecs()}
			tl := peekTool(t, dlqDeps(t, tc.mode, k, defaultCfgs()))
			r, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range tc.want {
				if !strings.Contains(r.Summary, w) {
					t.Errorf("summary lacks %q:\n%s", w, r.Summary)
				}
			}
			for _, w := range tc.wantNot {
				if strings.Contains(r.Summary, w) {
					t.Errorf("summary has %q:\n%s", w, r.Summary)
				}
			}
			// Data is the same in every mode: counts, positions, timestamps.
			d := r.Data.(dlqData)
			if d.ExceptionClassCount["org.apache.kafka.connect.errors.DataException"] != 2 || d.ExceptionClassCount["org.apache.kafka.connect.errors.SerializationException"] != 1 {
				t.Fatalf("class counts = %v", d.ExceptionClassCount)
			}
			wantMode := tc.mode
			if wantMode == "" {
				wantMode = "classes"
			}
			if d.HeaderMode != wantMode {
				t.Fatalf("header_mode = %q", d.HeaderMode)
			}
			if len(r.Summary) > tools.MaxSummaryBytes {
				t.Fatalf("summary %d bytes", len(r.Summary))
			}
		})
	}
}

func TestDLQPeekDataShape(t *testing.T) {
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), recs: modeRecs()}
	tl := peekTool(t, dlqDeps(t, "messages", k, defaultCfgs()))
	r, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(r.Data)
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"topic": true, "writer": true, "declared": true, "exists": true, "partitions": true, "records": true,
		"start_offsets": true, "end_offsets": true, "newest_timestamp": true, "sampled": true, "exception_class_counts": true,
		"other_writers": true, "context_headers": true, "header_mode": true, "new_in_window": true, "risks": true}
	for key := range m {
		if !allowed[key] {
			t.Errorf("unexpected Data key %q", key)
		}
	}
	for _, banned := range []string{"Unexpected character", "m2", "m3", "stage", "VALUE_CONVERTER", "25377154"} {
		if strings.Contains(string(b), banned) {
			t.Errorf("Data carries record text %q: %s", banned, b)
		}
	}
	if m["records"].(float64) != 55 {
		t.Fatalf("records = %v", m["records"])
	}
}

func TestDLQPeekCrossPipelineFilter(t *testing.T) {
	recs := modeRecs()
	recs = append(recs, clients.DLQRecord{Topic: "orders-dlq", Offset: 1, Timestamp: ts(59),
		Headers: hdr("org.acme.OtherException", "x", "__connect.errors.connector.name", "bill-sink")})
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), recs: recs}
	tl := peekTool(t, dlqDeps(t, "", k, defaultCfgs()))
	r, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
	if err != nil {
		t.Fatal(err)
	}
	d := r.Data.(dlqData)
	if d.OtherWriters != 1 || d.Sampled != 3 || d.ExceptionClassCount["org.acme.OtherException"] != 0 {
		t.Fatalf("data = %+v", d)
	}
	if strings.Contains(r.Summary, "org.acme.OtherException") || strings.Contains(r.Summary, "bill-sink") ||
		!strings.Contains(r.Summary, "1 sampled records written by other connectors were left out") {
		t.Fatalf("summary:\n%s", r.Summary)
	}
}

func TestDLQPeekNoHeadersLine(t *testing.T) {
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(),
		recs: []clients.DLQRecord{{Topic: "orders-dlq", Partition: 0, Offset: 9, Timestamp: ts(5)}}}
	cfgs := defaultCfgs()
	cfgs["pg-orders-sink"]["errors.deadletterqueue.context.headers.enable"] = "false"
	tl := peekTool(t, dlqDeps(t, "", k, cfgs))
	r, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{"no Connect error headers on these records", "errors.deadletterqueue.context.headers.enable=true", "dlq_no_headers", "p0@9"} {
		if !strings.Contains(r.Summary, w) {
			t.Errorf("summary lacks %q:\n%s", w, r.Summary)
		}
	}
}

func TestDLQPeekStatsFromWatcher(t *testing.T) {
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), recs: modeRecs()}
	d := dlqDeps(t, "", k, defaultCfgs())
	d.DLQStats = func(topic string) (int64, time.Duration, bool) { return 37, time.Hour, topic == "orders-dlq" }
	r, err := peekTool(t, d).Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
	if err != nil || !strings.Contains(r.Summary, "37 new in the last 1h") || *r.Data.(dlqData).NewInWindow != 37 {
		t.Fatalf("%v\n%s", err, r.Summary)
	}
}

func TestDLQPeekSummaryIsCapped(t *testing.T) {
	long := strings.Repeat("a", 290)
	var recs []clients.DLQRecord
	for i := 0; i < 20; i++ {
		recs = append(recs, clients.DLQRecord{Topic: "orders-dlq", Offset: int64(i), Timestamp: ts(i),
			Headers: hdr("org.apache.kafka.connect.errors.DataException", long)})
	}
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), recs: recs}
	tl := peekTool(t, dlqDeps(t, "messages", k, defaultCfgs()))
	r, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink","limit":20}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Summary) > tools.MaxSummaryBytes || !r.Truncated {
		t.Fatalf("len=%d truncated=%v", len(r.Summary), r.Truncated)
	}
	// The counts and risks come before the records, so a cut loses records only.
	if !strings.HasPrefix(r.Summary, "orders-dlq (DLQ of pg-orders-sink): 55 records") {
		t.Fatalf("summary starts %q", r.Summary[:80])
	}
}

// The leak test: a fake reader that misbehaves (returns application headers,
// free text in fixed-vocabulary headers, unredacted messages) must not get its
// markers into the summary or Data, in plain, base64 or hex form.
func TestDLQPeekLeakMarkers(t *testing.T) {
	const (
		msgMarker  = "MSGMARKER-7f3a" // inside a quoted string / [Source:] tail: redaction removes it
		keyMarker  = "KEYMARKER-91bc" // an application header (stands for a key/value): must never get through
		valMarker  = "VALMARKER-c40d" // free text in a class/stage/task header: must not pass the vocabulary check
		sqlMarker  = "SQLMARKER-5e22"
		pathMarker = "PATHMARKER-0a9f" // a bogus original topic
	)
	jackson := `Unexpected character ('x' (code 120)): was expecting double-quote at [Source: (byte[])"{"email":"` + msgMarker + `@b.com"}"; line: 1, column: 2]`
	jdbc := `Batch entry failed. Detail: Key (id)=(` + sqlMarker + `) already exists.`
	recs := []clients.DLQRecord{
		{Topic: "orders-dlq", Partition: 0, Offset: 3, Timestamp: ts(3), Headers: hdr("org.apache.kafka.connect.errors.DataException", jackson,
			"x-trace-id", keyMarker, "key", keyMarker, "value", keyMarker, "__connect.other."+keyMarker, keyMarker,
			"__connect.errors.stage", "bad stage "+valMarker,
			"__connect.errors.task.id", valMarker,
			"__connect.errors.topic", pathMarker+" x/y",
			"__connect.errors.class.name", "has space "+valMarker)},
		{Topic: "orders-dlq", Partition: 0, Offset: 2, Timestamp: ts(2), Headers: hdr("Foo bar "+valMarker, jdbc,
			"__connect.errors.exception.stacktrace", "java.sql.BatchUpdateException: "+jdbc+"\n\tat Z "+keyMarker)},
		// Tolerated reader bug: the header map carries only application headers.
		{Topic: "orders-dlq", Partition: 0, Offset: 1, Timestamp: ts(1), Headers: map[string]string{"authorization": keyMarker, "k": keyMarker}},
	}
	markers := []string{msgMarker, keyMarker, valMarker, sqlMarker, pathMarker}
	for _, mode := range []string{"counts_only", "classes", "messages"} {
		t.Run(mode, func(t *testing.T) {
			k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), recs: recs}
			tl := peekTool(t, dlqDeps(t, mode, k, defaultCfgs()))
			r, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink","limit":20}`))
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(r.Data)
			if err != nil {
				t.Fatal(err)
			}
			for _, where := range []struct{ name, text string }{{"summary", r.Summary}, {"data", string(data)}} {
				for _, m := range markers {
					for form, needle := range map[string]string{
						"plain":  m,
						"base64": base64.StdEncoding.EncodeToString([]byte(m)),
						"b64url": base64.RawURLEncoding.EncodeToString([]byte(m)),
						"hex":    hex.EncodeToString([]byte(m)),
					} {
						if strings.Contains(where.text, needle) {
							t.Errorf("%s mode: %s carries marker %s in %s form:\n%s", mode, where.name, m, form, where.text)
						}
					}
				}
			}
			if mode == "messages" && !strings.Contains(r.Summary, "<redacted>") {
				t.Errorf("messages mode shows no redaction:\n%s", r.Summary)
			}
		})
	}
}

func TestDLQPeekNilKafkaDoesNotPanic(t *testing.T) {
	tl := peekTool(t, Deps{Owners: dlqOwners(t, "")})
	_, err := tl.Run(scoped(), json.RawMessage(`{"topic":"order-sync-dlq"}`))
	if err == nil || !strings.Contains(err.Error(), "cannot read DLQ topics") {
		t.Fatalf("err = %v", err)
	}
}

func TestConnectConfigShowsErrorsKeys(t *testing.T) {
	cfgs := defaultCfgs()
	cfgs["pg-orders-sink"]["errors.retry.timeout"] = "60000"
	cfgs["pg-orders-sink"]["errors.log.include.messages"] = "false"
	cfgs["pg-orders-sink"]["event.processing.failure.handling.mode"] = "skip"
	cfgs["pg-orders-sink"]["not.listed"] = "hidden-value"
	d := dlqDeps(t, "", &fakeDLQ{fakeKafka: &fakeKafka{}}, cfgs)
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tl, _ := reg.Get("connect_config")
	r, err := tl.Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{
		"errors.tolerance: all", "errors.deadletterqueue.topic.name: orders-dlq", "errors.log.enable: true",
		"errors.log.include.messages: false", "errors.deadletterqueue.context.headers.enable: true",
		"errors.retry.timeout: 60000", "event.processing.failure.handling.mode: skip", "password: ***",
	} {
		if !strings.Contains(r.Summary, w) {
			t.Errorf("connect_config lacks %q:\n%s", w, r.Summary)
		}
	}
	if strings.Contains(r.Summary, "hunter2") || strings.Contains(r.Summary, "hidden-value") {
		t.Fatalf("connect_config leaks:\n%s", r.Summary)
	}
}
