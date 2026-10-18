package actions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ---- fakes ----

// sigConnect serves one connector config; block makes Config wait for ctx.
type sigConnect struct {
	mu    sync.Mutex
	cfg   map[string]string
	err   error
	block bool
	calls int
}

func (f *sigConnect) ListConnectors(context.Context) ([]string, error) {
	return []string{"pg-orders-connector"}, nil
}
func (f *sigConnect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{State: "RUNNING"}, nil
}
func (f *sigConnect) Config(ctx context.Context, _ string) (map[string]string, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[string]string, len(f.cfg))
	for k, v := range f.cfg {
		out[k] = v
	}
	return out, nil
}
func (f *sigConnect) n() int { f.mu.Lock(); defer f.mu.Unlock(); return f.calls }

// topicConfig is a Debezium Postgres connector set up for Kafka signals.
func topicConfig() map[string]string {
	return map[string]string{
		"connector.class":                "io.debezium.connector.postgresql.PostgresConnector",
		"topic.prefix":                   "pg",
		"signal.enabled.channels":        "source,kafka",
		"signal.kafka.topic":             "pg-signal",
		"signal.kafka.bootstrap.servers": "kafka:9092",
		"signal.data.collection":         "public.debezium_signal",
		"table.include.list":             "public.orders,public.debezium_signal",
	}
}

// recKafka is a clients.KafkaWriter without the narrow signal method: the
// action falls back to Produce.
type recKafka struct {
	topic string
	recs  []clients.Record
	acked int
	err   error
	block bool
}

func (k *recKafka) ResetGroupOffsets(context.Context, string, clients.OffsetTarget) (int, error) {
	return 0, errors.New("fake: unexpected ResetGroupOffsets")
}
func (k *recKafka) Produce(ctx context.Context, topic string, recs []clients.Record) (int, error) {
	if k.block {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	k.topic, k.recs = topic, recs
	if k.err != nil {
		return 0, k.err
	}
	if k.acked >= 0 {
		return len(recs), nil
	}
	return 0, nil
}

// signalKafka has the narrow method, as the real kafka.Writer.
type signalKafka struct {
	recKafka
	args []string
	tbls []string
	err  error
}

func (k *signalKafka) SendSnapshotSignal(_ context.Context, topic, connector, key string, tables []string) (kafka.SnapshotSignal, error) {
	k.args, k.tbls = []string{topic, connector, key}, tables
	if k.err != nil {
		return kafka.SnapshotSignal{}, k.err
	}
	return kafka.SnapshotSignal{ID: "42", Topic: topic, Offset: 42}, nil
}

type sigSourceWriter struct {
	args []string
	tbls []string
	id   string
	err  error
}

func (w *sigSourceWriter) AddPublicationTables(context.Context, string, []string) error {
	return errors.New("fake: unexpected AddPublicationTables")
}
func (w *sigSourceWriter) DropReplicationSlot(context.Context, string) error {
	return errors.New("fake: unexpected DropReplicationSlot")
}
func (w *sigSourceWriter) InsertSnapshotSignal(_ context.Context, table, connector string, tables []string) (string, error) {
	w.args, w.tbls = []string{table, connector}, tables
	return w.id, w.err
}

// estSource is a SourceReader with the optional row estimate.
type estSource struct {
	clients.SourceReader
	rows map[string]int64
}

func (s estSource) EstimatedRows(_ context.Context, table string) (int64, error) {
	n, ok := s.rows[table]
	if !ok {
		return 0, errors.New("no stats")
	}
	return n, nil
}

var snapPipeline = owners.Pipeline{
	ID:           "orders",
	TopicPrefix:  "pg",
	SourceTables: []string{"public.orders", "public.refunds"},
	Connectors:   []string{"pg-orders-connector"},
}

func roomCtx() context.Context { return tools.WithPipeline(context.Background(), snapPipeline) }

func snapParams(via string, tables ...string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{"connector": "pg-orders-connector", "tables": tables, "via": via})
	return b
}

// ---- params and scope ----

func TestSnapshotParamsRefused(t *testing.T) {
	a := &snapshotTable{d: SnapshotDeps{Connect: &sigConnect{cfg: topicConfig()}}}
	many := make([]string, maxSnapshotTables+1)
	for i := range many {
		many[i] = "public.t" + strings.Repeat("x", i)
	}
	for _, tc := range []struct {
		name, raw, want string
	}{
		{"not json", `{`, "bad params"},
		{"unknown field", `{"connector":"c","tables":["public.orders"],"via":"signal_topic","force":true}`, "bad params"},
		{"no connector", `{"tables":["public.orders"],"via":"signal_topic"}`, "connector is required"},
		{"no via", `{"connector":"c","tables":["public.orders"]}`, "via must be"},
		{"bad via", `{"connector":"c","tables":["public.orders"],"via":"restart"}`, "via must be"},
		{"no tables", `{"connector":"c","tables":[],"via":"signal_topic"}`, "at least one"},
		{"bare table", `{"connector":"c","tables":["orders"],"via":"signal_topic"}`, "not schema.table"},
		{"regex table", `{"connector":"c","tables":["public.*"],"via":"signal_topic"}`, "not schema.table"},
		{"too many", string(snapParams(ViaSignalTopic, many...)), "at most"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := a.Validate(context.Background(), json.RawMessage(tc.raw)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate = %v, want %q", err, tc.want)
			}
			if err := a.CheckScope(snapPipeline, json.RawMessage(tc.raw)); err == nil {
				t.Fatal("CheckScope accepted bad params")
			}
		})
	}
}

func TestSnapshotCheckScope(t *testing.T) {
	a := &snapshotTable{}
	for _, tc := range []struct {
		name string
		raw  json.RawMessage
		ok   bool
	}{
		{"in scope", snapParams(ViaSignalTopic, "public.orders", "public.refunds"), true},
		{"table outside source_tables", snapParams(ViaSignalTopic, "public.orders", "public.users"), false},
		{"other connector", json.RawMessage(`{"connector":"billing-connector","tables":["public.orders"],"via":"signal_topic"}`), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := a.CheckScope(snapPipeline, tc.raw)
			if tc.ok != (err == nil) || (!tc.ok && !errors.Is(err, tools.ErrOutOfScope)) {
				t.Fatalf("CheckScope = %v, ok want %v (out of scope wraps tools.ErrOutOfScope)", err, tc.ok)
			}
		})
	}
}

func TestSnapshotLayerFor(t *testing.T) {
	a := &snapshotTable{}
	if l, _ := a.LayerFor(snapParams(ViaSignalTopic, "public.orders")); l != a.Layer() || l != "kafka-connect" {
		t.Fatalf("signal_topic layer %q", l)
	}
	if l, _ := a.LayerFor(snapParams(ViaSignalTable, "public.orders")); l != "postgres" {
		t.Fatalf("signal_table layer %q", l)
	}
}

// ---- Validate ----

func TestSnapshotValidateConfig(t *testing.T) {
	with := func(edit func(map[string]string)) map[string]string {
		c := topicConfig()
		edit(c)
		return c
	}
	for _, tc := range []struct {
		name string
		via  string
		cfg  map[string]string
		want string // "" = accepted
		sent error  // wrapped error, when want != ""
	}{
		{"configured", ViaSignalTopic, topicConfig(), "", nil},
		{"read-only needs no signal table", ViaSignalTopic, with(func(c map[string]string) {
			delete(c, "signal.data.collection")
			c["read.only"] = "true"
		}), "", nil},
		{"not debezium", ViaSignalTopic, with(func(c map[string]string) { c["connector.class"] = "io.confluent.connect.jdbc.JdbcSinkConnector" }), "not a Debezium source connector", nil},
		{"kafka channel off (default source)", ViaSignalTopic, with(func(c map[string]string) { delete(c, "signal.enabled.channels") }), "signal.enabled.channels to include kafka", ErrSignalNotConfigured},
		{"no signal topic", ViaSignalTopic, with(func(c map[string]string) { delete(c, "signal.kafka.topic") }), "signal.kafka.topic", ErrSignalNotConfigured},
		{"no signal brokers", ViaSignalTopic, with(func(c map[string]string) { delete(c, "signal.kafka.bootstrap.servers") }), "signal.kafka.bootstrap.servers", ErrSignalNotConfigured},
		{"no topic.prefix", ViaSignalTopic, with(func(c map[string]string) { delete(c, "topic.prefix") }), "topic.prefix", ErrSignalNotConfigured},
		{"no watermark table", ViaSignalTopic, with(func(c map[string]string) { delete(c, "signal.data.collection") }), "watermarks", ErrSignalNotConfigured},
		{"table: no collection", ViaSignalTable, with(func(c map[string]string) { delete(c, "signal.data.collection") }), "signal.data.collection to the signal table", ErrSignalNotConfigured},
		{"table: source channel off", ViaSignalTable, with(func(c map[string]string) { c["signal.enabled.channels"] = "kafka" }), "include source", ErrSignalNotConfigured},
		{"table: not postgres", ViaSignalTable, with(func(c map[string]string) { c["connector.class"] = "io.debezium.connector.mysql.MySqlConnector" }), "Postgres connector", ErrSignalNotConfigured},
		{"table: bad collection", ViaSignalTable, with(func(c map[string]string) { c["signal.data.collection"] = "orders.public.sig" }), "not schema.table", ErrSignalNotConfigured},
		{"table: configured, layer not checkable yet", ViaSignalTable, topicConfig(), "postgres layer", errSignalTableLayer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &snapshotTable{d: SnapshotDeps{Connect: &sigConnect{cfg: tc.cfg}}}
			err := a.Validate(context.Background(), snapParams(tc.via, "public.orders"))
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Validate = %v, want accepted", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate = %v, want %q", err, tc.want)
			}
			if tc.sent != nil && !errors.Is(err, tc.sent) {
				t.Fatalf("Validate = %v, want it to wrap %v", err, tc.sent)
			}
		})
	}
}

func TestSnapshotValidateReadErrors(t *testing.T) {
	boom := clients.ErrNotFound
	a := &snapshotTable{d: SnapshotDeps{Connect: &sigConnect{err: boom}}}
	if err := a.Validate(context.Background(), snapParams(ViaSignalTopic, "public.orders")); !errors.Is(err, boom) {
		t.Fatalf("connector missing: %v", err)
	}
	a = &snapshotTable{d: SnapshotDeps{Connect: &sigConnect{block: true}, Timeout: 20 * time.Millisecond}}
	start := time.Now()
	if err := a.Validate(context.Background(), snapParams(ViaSignalTopic, "public.orders")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hung Connect: %v, want deadline exceeded", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("Validate did not honour its timeout")
	}
	a = &snapshotTable{}
	if err := a.Validate(context.Background(), snapParams(ViaSignalTopic, "public.orders")); err == nil || !strings.Contains(err.Error(), "no Connect client") {
		t.Fatalf("no Connect: %v", err)
	}
}

// Write mode off: wiring passes no writers, and Validate and Preview still
// work.
func TestSnapshotReadOnlyValidateAndPreview(t *testing.T) {
	a := &snapshotTable{d: SnapshotDeps{Connect: &sigConnect{cfg: topicConfig()}}}
	if err := a.Validate(context.Background(), snapParams(ViaSignalTopic, "public.orders")); err != nil {
		t.Fatalf("Validate with no writer: %v", err)
	}
	if _, err := a.Preview(roomCtx(), snapParams(ViaSignalTopic, "public.orders")); err != nil {
		t.Fatalf("Preview with no writer: %v", err)
	}
	if _, err := a.Execute(roomCtx(), snapParams(ViaSignalTopic, "public.orders")); err == nil || !strings.Contains(err.Error(), "PIPEROOM_WRITE_MODE=on") {
		t.Fatalf("Execute with no writer: %v", err)
	}
	if _, err := a.Execute(roomCtx(), snapParams(ViaSignalTable, "public.orders")); err == nil || !strings.Contains(err.Error(), "no write credentials") {
		t.Fatalf("Execute signal_table with no writer: %v", err)
	}
}

// ---- Preview ----

func TestSnapshotPreview(t *testing.T) {
	fc := &sigConnect{cfg: topicConfig()}
	k := &signalKafka{}
	sw := &sigSourceWriter{}
	a := &snapshotTable{d: SnapshotDeps{Connect: fc, Kafka: k, SourceWriter: sw,
		Source: estSource{rows: map[string]int64{"public.orders": 120000}}}}
	res, err := a.Preview(roomCtx(), snapParams(ViaSignalTopic, "public.orders", "public.refunds"))
	if err != nil {
		t.Fatal(err)
	}
	all := res.Target + "|" + res.Current + "|" + res.Expected + "|" + strings.Join(res.Downstream, "|")
	for _, want := range []string{
		"public.orders, public.refunds", "pg-signal", "~120000 rows to pg.public.orders",
		"public.refunds: estimate unavailable", "duplicates", "stop-snapshot", `key "pg"`,
		"table.include.list does not match public.refunds",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("preview lacks %q:\n%s", want, all)
		}
	}
	if res.Reversible || res.Rollback != "" {
		t.Errorf("a snapshot is not reversible: %+v", res)
	}
	if k.args != nil || k.recs != nil || sw.args != nil {
		t.Fatal("Preview called a writer")
	}

	// No estimator on the source client.
	a.d.Source = nil
	res, err = a.Preview(roomCtx(), snapParams(ViaSignalTopic, "public.orders"))
	if err != nil || !strings.Contains(res.Current, "row estimate unavailable") {
		t.Fatalf("preview without estimator: %v %q", err, res.Current)
	}
	// The signal_table variant previews too (its Validate is gated, not its preview).
	res, err = a.Preview(roomCtx(), snapParams(ViaSignalTable, "public.orders"))
	if err != nil || !strings.Contains(res.Target, "public.debezium_signal") {
		t.Fatalf("signal_table preview: %v %+v", err, res)
	}
}

func TestSnapshotScopeGateBeforeIO(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  context.Context
		raw  json.RawMessage
		want error
	}{
		{"no scope", context.Background(), snapParams(ViaSignalTopic, "public.orders"), tools.ErrNoScope},
		{"pipeline id only", tools.WithPipelineID(context.Background(), "orders"), snapParams(ViaSignalTopic, "public.orders"), tools.ErrNoScope},
		{"table out of scope", roomCtx(), snapParams(ViaSignalTopic, "public.users"), tools.ErrOutOfScope},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := &sigConnect{cfg: topicConfig()}
			k := &signalKafka{}
			a := &snapshotTable{d: SnapshotDeps{Connect: fc, Kafka: k, SourceWriter: &sigSourceWriter{}}}
			if _, err := a.Preview(tc.ctx, tc.raw); !errors.Is(err, tc.want) {
				t.Fatalf("Preview = %v, want %v", err, tc.want)
			}
			if _, err := a.Execute(tc.ctx, tc.raw); !errors.Is(err, tc.want) {
				t.Fatalf("Execute = %v, want %v", err, tc.want)
			}
			if fc.n() != 0 || k.args != nil {
				t.Fatalf("I/O before the scope gate: %d config reads, signal %v", fc.n(), k.args)
			}
		})
	}
}

// ---- Execute ----

func TestSnapshotExecuteSignalTopic(t *testing.T) {
	k := &signalKafka{}
	a := &snapshotTable{d: SnapshotDeps{Connect: &sigConnect{cfg: topicConfig()}, Kafka: k}}
	out, err := a.Execute(roomCtx(), snapParams(ViaSignalTopic, "public.orders", "public.orders"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(k.args, ",") != "pg-signal,pg-orders-connector,pg" || strings.Join(k.tbls, ",") != "public.orders" {
		t.Fatalf("signal args %v tables %v", k.args, k.tbls)
	}
	if !strings.Contains(out, "signal 42") || !strings.Contains(out, "pg-signal") {
		t.Fatalf("result %q does not report the signal id and topic", out)
	}
}

func TestSnapshotExecuteProduceFallback(t *testing.T) {
	k := &recKafka{}
	a := &snapshotTable{d: SnapshotDeps{Connect: &sigConnect{cfg: topicConfig()}, Kafka: k}}
	if _, err := a.Execute(roomCtx(), snapParams(ViaSignalTopic, "public.orders")); err != nil {
		t.Fatal(err)
	}
	if k.topic != "pg-signal" || len(k.recs) != 1 || string(k.recs[0].Key) != "pg" {
		t.Fatalf("produced %q %+v", k.topic, k.recs)
	}
	want := `{"type":"execute-snapshot","data":{"data-collections":["public.orders"],"type":"incremental"}}`
	if string(k.recs[0].Value) != want {
		t.Fatalf("value %s, want %s", k.recs[0].Value, want)
	}
}

func TestSnapshotExecuteErrors(t *testing.T) {
	boom := errors.New("broker said no")
	for _, tc := range []struct {
		name  string
		deps  SnapshotDeps
		via   string
		want  error
		wantS string
	}{
		{"signal error", SnapshotDeps{Kafka: &signalKafka{err: boom}}, ViaSignalTopic, boom, ""},
		{"produce error", SnapshotDeps{Kafka: &recKafka{err: boom}}, ViaSignalTopic, boom, ""},
		{"not acked", SnapshotDeps{Kafka: &recKafka{acked: -1}}, ViaSignalTopic, nil, "acked 0 of 1"},
		{"produce timeout", SnapshotDeps{Kafka: &recKafka{block: true}, Timeout: 20 * time.Millisecond}, ViaSignalTopic, context.DeadlineExceeded, ""},
		{"config gone since approval", SnapshotDeps{Kafka: &signalKafka{}}, ViaSignalTopic, ErrSignalNotConfigured, ""},
		{"source writer not ready (P3b-4)", SnapshotDeps{SourceWriter: &sigSourceWriter{err: clients.ErrNotSupported}}, ViaSignalTable, clients.ErrNotSupported, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := topicConfig()
			if tc.name == "config gone since approval" {
				delete(cfg, "signal.kafka.topic")
			}
			tc.deps.Connect = &sigConnect{cfg: cfg}
			a := &snapshotTable{d: tc.deps}
			out, err := a.Execute(roomCtx(), snapParams(tc.via, "public.orders"))
			if err == nil || out != "" {
				t.Fatalf("Execute = %q, %v; want an error", out, err)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("Execute = %v, want it to wrap %v", err, tc.want)
			}
			if tc.wantS != "" && !strings.Contains(err.Error(), tc.wantS) {
				t.Fatalf("Execute = %v, want %q", err, tc.wantS)
			}
		})
	}
}

func TestSnapshotExecuteSignalTableDelegates(t *testing.T) {
	sw := &sigSourceWriter{id: "6f1c"}
	a := &snapshotTable{d: SnapshotDeps{Connect: &sigConnect{cfg: topicConfig()}, SourceWriter: sw}}
	out, err := a.Execute(roomCtx(), snapParams(ViaSignalTable, "public.orders"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(sw.args, ",") != "public.debezium_signal,pg-orders-connector" || strings.Join(sw.tbls, ",") != "public.orders" {
		t.Fatalf("InsertSnapshotSignal args %v %v", sw.args, sw.tbls)
	}
	if !strings.Contains(out, "6f1c") {
		t.Fatalf("result %q lacks the signal id", out)
	}
}

func TestSnapshotRegister(t *testing.T) {
	reg := NewRegistry()
	if err := RegisterSnapshot(reg, SnapshotDeps{}); err != nil {
		t.Fatal(err)
	}
	a, ok := reg.Get(SnapshotTableName)
	if !ok {
		t.Fatal("not registered")
	}
	if _, ok := a.(Preview); !ok {
		t.Fatal("no Preview")
	}
	if err := RegisterSnapshot(reg, SnapshotDeps{}); err == nil {
		t.Fatal("registered twice")
	}
}

func TestNotIncluded(t *testing.T) {
	for _, tc := range []struct {
		list string
		want string
	}{
		{"", ""},
		{"public.orders", "public.refunds"},
		{"public\\..*", ""},
		{"public.orders, public.refunds", ""},
		{"(", ""}, // not judged
	} {
		if got := strings.Join(notIncluded(tc.list, []string{"public.orders", "public.refunds"}), ","); got != tc.want {
			t.Errorf("notIncluded(%q) = %q, want %q", tc.list, got, tc.want)
		}
	}
}
