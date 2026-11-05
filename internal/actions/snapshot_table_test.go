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
// action refuses it (ErrNoSignalWriter) and never calls its Produce.
type recKafka struct {
	topic string
	recs  []clients.Record
}

func (k *recKafka) ResetGroupOffsets(context.Context, string, clients.OffsetTarget) (int, error) {
	return 0, errors.New("fake: unexpected ResetGroupOffsets")
}
func (k *recKafka) Produce(_ context.Context, topic string, recs []clients.Record) (int, error) {
	k.topic, k.recs = topic, recs
	return len(recs), nil
}

// signalKafka has the narrow method, as the real kafka.Writer.
type signalKafka struct {
	recKafka
	args []string
	tbls []string
	err  error
}

func (k *signalKafka) SendSnapshotSignal(_ context.Context, topic, connector, key string, tables []string) (clients.SnapshotSignal, error) {
	k.args, k.tbls = []string{topic, connector, key}, tables
	if k.err != nil {
		return clients.SnapshotSignal{}, k.err
	}
	return clients.SnapshotSignal{ID: "42", Topic: topic, Offset: 42}, nil
}

var _ clients.SnapshotSignaler = (*signalKafka)(nil)

// blockingSignalKafka is a signal writer whose broker never answers: it
// returns only when ctx ends, with ctx's error.
type blockingSignalKafka struct {
	recKafka
	hadDeadline bool
}

func (k *blockingSignalKafka) SendSnapshotSignal(ctx context.Context, _, _, _ string, _ []string) (clients.SnapshotSignal, error) {
	_, k.hadDeadline = ctx.Deadline()
	<-ctx.Done()
	return clients.SnapshotSignal{}, ctx.Err()
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
		{"signal topic is a CDC topic", ViaSignalTopic, with(func(c map[string]string) { c["signal.kafka.topic"] = "pg.public.orders" }), "CDC topic of public.orders", ErrSignalNotConfigured},
		{"table: configured, belongs to pg_snapshot_signal", ViaSignalTable, topicConfig(), "postgres-layer action pg_snapshot_signal", errSignalTableLayer},
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
	if _, err := a.Execute(roomCtx(), snapParams(ViaSignalTable, "public.orders")); !errors.Is(err, errSignalTableLayer) {
		t.Fatalf("Execute signal_table with no writer: %v, want errSignalTableLayer", err)
	}
}

// ---- Preview ----

func TestSnapshotPreview(t *testing.T) {
	fc := &sigConnect{cfg: topicConfig()}
	k := &signalKafka{}
	a := &snapshotTable{d: SnapshotDeps{Connect: fc, Kafka: k,
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
	if k.args != nil || k.recs != nil {
		t.Fatal("Preview called a writer")
	}

	// No estimator on the source client.
	a.d.Source = nil
	res, err = a.Preview(roomCtx(), snapParams(ViaSignalTopic, "public.orders"))
	if err != nil || !strings.Contains(res.Current, "row estimate unavailable") {
		t.Fatalf("preview without estimator: %v %q", err, res.Current)
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
			a := &snapshotTable{d: SnapshotDeps{Connect: fc, Kafka: k}}
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

// A signal writer that never answers does not hold Execute past a deadline:
// the caller's, or the action's own bound when the caller set none. The test
// fails after 5 s instead of hanging when Execute does not return.
func TestSnapshotExecuteSignalBlockedEndsAtDeadline(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration // the action's own bound (SnapshotDeps.Timeout)
		caller  time.Duration // the caller's deadline; 0 = none
	}{
		{"caller deadline", time.Minute, 100 * time.Millisecond},
		{"action bound", 100 * time.Millisecond, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := &blockingSignalKafka{}
			a := &snapshotTable{d: SnapshotDeps{Connect: &sigConnect{cfg: topicConfig()}, Kafka: k, Timeout: tc.timeout}}
			ctx := roomCtx()
			if tc.caller > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.caller)
				defer cancel()
			}
			type result struct {
				out string
				err error
			}
			done := make(chan result, 1)
			go func() {
				out, err := a.Execute(ctx, snapParams(ViaSignalTopic, "public.orders"))
				done <- result{out, err}
			}()
			select {
			case r := <-done:
				if !errors.Is(r.err, context.DeadlineExceeded) || r.out != "" {
					t.Fatalf("Execute = %q, %v; want the deadline", r.out, r.err)
				}
				if !k.hadDeadline {
					t.Fatal("SendSnapshotSignal got a ctx without a deadline")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Execute still blocked 5 s after a 100 ms deadline")
			}
		})
	}
}

// A writer without SendSnapshotSignal is refused before any I/O by Validate
// and Execute: no connector config read, nothing produced. Its Produce would
// skip the check against every source table's CDC topic.
func TestSnapshotRefusesWriterWithoutSignal(t *testing.T) {
	for _, ctx := range []context.Context{roomCtx(), tools.WithUnscoped(context.Background())} {
		fc := &sigConnect{cfg: topicConfig()}
		k := &recKafka{}
		a := &snapshotTable{d: SnapshotDeps{Connect: fc, Kafka: k}}
		raw := snapParams(ViaSignalTopic, "public.orders")
		if err := a.Validate(ctx, raw); !errors.Is(err, ErrNoSignalWriter) || !strings.Contains(err.Error(), "cannot send snapshot signals") {
			t.Fatalf("Validate = %v, want ErrNoSignalWriter", err)
		}
		out, err := a.Execute(ctx, raw)
		if !errors.Is(err, ErrNoSignalWriter) || out != "" {
			t.Fatalf("Execute = %q, %v; want ErrNoSignalWriter", out, err)
		}
		if fc.n() != 0 {
			t.Fatalf("read the connector config %d times before refusing", fc.n())
		}
		if k.topic != "" || k.recs != nil {
			t.Fatalf("produced %q %+v", k.topic, k.recs)
		}
	}
}

// With write mode off there is no writer: Validate still passes, so a step
// can be proposed and approved before write mode is on.
func TestSnapshotValidateWithoutWriter(t *testing.T) {
	a := &snapshotTable{d: SnapshotDeps{Connect: &sigConnect{cfg: topicConfig()}}}
	if err := a.Validate(roomCtx(), snapParams(ViaSignalTopic, "public.orders")); err != nil {
		t.Fatalf("Validate = %v", err)
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
		{"writer without signal method", SnapshotDeps{Kafka: &recKafka{}}, ViaSignalTopic, ErrNoSignalWriter, "cannot send snapshot signals"},
		{"config gone since approval", SnapshotDeps{Kafka: &signalKafka{}}, ViaSignalTopic, ErrSignalNotConfigured, ""},
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

// via signal_table is pg_snapshot_signal's (P3b-4): every stage refuses it
// with errSignalTableLayer before any client call, so neither Connect nor a
// writer of any kind (the signal method, Produce, offset reset) is touched,
// whatever the scope in ctx.
func TestSnapshotSignalTableRefusedEveryStage(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  context.Context
	}{
		{"room", roomCtx()},
		{"unscoped", tools.WithUnscoped(context.Background())},
		{"no scope", context.Background()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := &sigConnect{cfg: topicConfig()}
			sk := &signalKafka{}
			pk := &recKafka{}
			raw := snapParams(ViaSignalTable, "public.orders")
			check := func(stage string, err error) {
				t.Helper()
				if !errors.Is(err, errSignalTableLayer) || !strings.Contains(err.Error(), "pg_snapshot_signal") {
					t.Fatalf("%s = %v, want errSignalTableLayer naming pg_snapshot_signal", stage, err)
				}
			}
			for _, k := range []clients.KafkaWriter{sk, pk} {
				a := &snapshotTable{d: SnapshotDeps{Connect: fc, Kafka: k}}
				_, err := a.Execute(tc.ctx, raw)
				check("Execute", err)
				_, err = a.Preview(tc.ctx, raw)
				check("Preview", err)
				check("Validate", a.Validate(tc.ctx, raw))
				check("CheckScope", a.CheckScope(snapPipeline, raw))
			}
			if fc.n() != 0 {
				t.Fatalf("read the connector config %d times before refusing", fc.n())
			}
			if sk.args != nil || sk.recs != nil || pk.recs != nil || pk.topic != "" {
				t.Fatalf("a writer was called: signal %v, produce %q %v / %v", sk.args, pk.topic, pk.recs, sk.recs)
			}
		})
	}
}

func TestSnapshotParamsSchemaOffersSignalTopicOnly(t *testing.T) {
	b, err := json.Marshal((&snapshotTable{}).ParamsSchema())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"`+ViaSignalTable+`"`) {
		t.Fatalf("schema still offers via %s: %s", ViaSignalTable, b)
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
