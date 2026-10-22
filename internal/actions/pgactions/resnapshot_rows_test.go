package pgactions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/clients/warehouse"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// resnapDB is a fake read-only source or sink: the X2a range reads, the X1
// Reconciler window count and the signal-table state. With noIO set any
// call fails the test, which proves a refusal came first.
type resnapDB struct {
	clients.SourceDB
	t        *testing.T
	noIO     bool
	cols     map[string]postgres.RangeColumn // "table.column"
	counts   map[string]int64                // table
	countErr error
	window   map[string]int64 // table: CountBetween
	st       postgres.SnapshotSignalState
	calls    int
	ranges   []string // "table column from..to"
	windows  []string
	deadline bool
}

func (d *resnapDB) io(ctx context.Context) {
	if d.noIO {
		d.t.Fatal("I/O on a call that must be refused first")
	}
	d.calls++
	_, d.deadline = ctx.Deadline()
}

func (d *resnapDB) RangeColumn(ctx context.Context, table, column string) (postgres.RangeColumn, error) {
	d.io(ctx)
	c, ok := d.cols[table+"."+column]
	if !ok {
		return postgres.RangeColumn{}, clients.ErrNotFound
	}
	return c, nil
}

func (d *resnapDB) CountRange(ctx context.Context, table, column, from, to string) (int64, error) {
	d.io(ctx)
	d.ranges = append(d.ranges, table+" "+column+" "+from+".."+to)
	if d.countErr != nil {
		return 0, d.countErr
	}
	return d.counts[table], nil
}

func (d *resnapDB) CountBetween(ctx context.Context, table, column string, from, to time.Time) (int64, error) {
	d.io(ctx)
	d.windows = append(d.windows, table+" "+column+" "+from.Format(time.RFC3339)+".."+to.Format(time.RFC3339))
	return d.window[table], nil
}

func (d *resnapDB) SampleKeys(context.Context, string, string, string, time.Time, int) ([]string, error) {
	panic("not used")
}

func (d *resnapDB) RowTimes(context.Context, string, string, []string, []string) (map[string]map[string]time.Time, error) {
	panic("not used")
}

func (d *resnapDB) SnapshotSignalState(ctx context.Context, _, _ string, _ []string) (postgres.SnapshotSignalState, error) {
	d.io(ctx)
	return d.st, nil
}

// resnapWriter records InsertSnapshotSignalWhere; any other write panics.
type resnapWriter struct {
	clients.SourceWriter
	calls                     int
	signal, connector, filter string
	tables                    []string
}

func (w *resnapWriter) InsertSnapshotSignalWhere(_ context.Context, signalTable, connectorID string, tables []string, cond string) (string, error) {
	w.calls++
	w.signal, w.connector, w.tables, w.filter = signalTable, connectorID, tables, cond
	if err := clients.ValidateSnapshotCondition(cond); err != nil {
		return "", err
	}
	return "6f1c2a7e-0d3b-4c55-9a1e-2b7f0c9d8e11", nil
}

// resnapKafka records SendSnapshotSignalWhere; any other write panics.
type resnapKafka struct {
	clients.KafkaWriter
	calls                       int
	topic, connector, key, cond string
}

func (k *resnapKafka) SendSnapshotSignalWhere(_ context.Context, topic, connector, key string, _ []string, cond string) (clients.SnapshotSignal, error) {
	k.calls++
	k.topic, k.connector, k.key, k.cond = topic, connector, key, cond
	return clients.SnapshotSignal{ID: "7", Topic: topic, Offset: 7}, nil
}

func resnapPipeline() owners.Pipeline {
	return owners.Pipeline{
		ID:           "orders",
		TopicPrefix:  "pg",
		Connectors:   []string{"pg-orders"},
		SourceTables: []string{"public.orders", "public.customers"},
		SinkTables:   []string{"analytics.orders", "analytics.customers"},
	}
}

func resnapCtx() context.Context { return tools.WithPipeline(context.Background(), resnapPipeline()) }

func resnapTableCfg() map[string]string {
	return map[string]string{
		"connector.class":         "io.debezium.connector.postgresql.PostgresConnector",
		"publication.name":        "dbz_publication",
		"signal.enabled.channels": "source",
		"signal.data.collection":  "debezium.signal",
		"topic.prefix":            "pg",
	}
}

func resnapTopicCfg() map[string]string {
	return map[string]string{
		"connector.class":                "io.debezium.connector.postgresql.PostgresConnector",
		"signal.enabled.channels":        "kafka",
		"signal.kafka.topic":             "pg-signal",
		"signal.kafka.bootstrap.servers": "kafka:9092",
		"signal.data.collection":         "debezium.signal",
		"topic.prefix":                   "pg",
	}
}

type resnapFixture struct {
	conn *addPubConnect
	src  *resnapDB
	sink *resnapDB
	w    *resnapWriter
	k    *resnapKafka
	a    actions.Action
}

func newResnapFixture(t *testing.T, cfg map[string]string) *resnapFixture {
	if cfg == nil {
		cfg = resnapTableCfg()
	}
	f := &resnapFixture{
		conn: &addPubConnect{cfg: map[string]map[string]string{"pg-orders": cfg}},
		src: &resnapDB{t: t,
			cols: map[string]postgres.RangeColumn{
				"public.orders.id":         {Type: "int8", Kind: pgconv.RangeKindInteger, PrimaryKey: true},
				"public.orders.updated_at": {Type: "timestamptz", Kind: pgconv.RangeKindTime},
				"public.orders.status":     {Type: "text", Kind: pgconv.RangeKindText},
				"public.customers.code":    {Type: "varchar", Kind: pgconv.RangeKindText, PrimaryKey: true},
				"public.customers.ref":     {Type: "uuid", Kind: pgconv.RangeKindUUID},
			},
			counts: map[string]int64{"public.orders": 412, "public.customers": 3},
			window: map[string]int64{"public.orders": 300},
			st:     postgres.SnapshotSignalState{PublicationExists: true, SignalExists: true, SignalColumns: true, SignalPublished: true},
		},
		sink: &resnapDB{t: t,
			cols: map[string]postgres.RangeColumn{
				"analytics.orders.id":         {Type: "int8", Kind: pgconv.RangeKindInteger},
				"analytics.orders.updated_at": {Type: "timestamptz", Kind: pgconv.RangeKindTime},
				"analytics.customers.code":    {Type: "text", Kind: pgconv.RangeKindText},
			},
			counts: map[string]int64{"analytics.orders": 400, "analytics.customers": 3},
			window: map[string]int64{"analytics.orders": 290},
		},
		w: &resnapWriter{},
		k: &resnapKafka{},
	}
	f.a = NewResnapshotRows(ResnapshotRowsDeps{Connect: f.conn, Source: f.src, Sink: f.sink, Writer: f.w, Kafka: f.k})
	return f
}

func resnapRaw(v map[string]any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func resnapKeys(from, to string) json.RawMessage {
	return resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.orders", "key_range": map[string]any{"column": "id", "from": from, "to": to}})
}

func resnapSinceAt(at string) json.RawMessage {
	return resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.orders", "since": map[string]any{"column": "updated_at", "at": at}})
}

// TC5: the action is a recovery action with Verify, and the registry takes it.
func TestResnapshotRowsMeta(t *testing.T) {
	a := newResnapFixture(t, nil).a
	if a.Name() != "pg_resnapshot_rows" || a.Layer() != "postgres" || a.Describe() == "" {
		t.Fatalf("meta: %s %s", a.Name(), a.Layer())
	}
	if actions.ClassOf(a) != actions.ClassRecovery {
		t.Fatalf("class = %q, want recovery", actions.ClassOf(a))
	}
	for name, ok := range map[string]bool{
		"Verifier": func() bool { _, ok := a.(actions.Verifier); return ok }(),
		"Preview":  func() bool { _, ok := a.(actions.Preview); return ok }(),
		"Scoped":   func() bool { _, ok := a.(actions.Scoped); return ok }(),
	} {
		if !ok {
			t.Errorf("must implement %s", name)
		}
	}
	if err := actions.NewRegistry().Register(a); err != nil {
		t.Fatalf("Register: %v", err)
	}
	s := a.ParamsSchema()
	if s["type"] != "object" || s["additionalProperties"] != false {
		t.Fatalf("schema: %v", s)
	}
}

// TC1: the condition is built from typed params only; a SQL-ish value is a
// quoted literal and passes the writers' grammar check.
func TestResnapshotFilter(t *testing.T) {
	for _, tc := range []struct{ col, from, to, want string }{
		{"id", "100", "511", `"id" >= '100' AND "id" <= '511'`},
		{"code", "1; drop", "z", `"code" >= '1; drop' AND "code" <= 'z'`},
		{"code", "o'brien", "", `"code" >= 'o''brien'`},
		{"code", "a -- b /* c", "", `"code" >= 'a -- b /* c'`},
		{"updated_at", "2026-10-22T09:00:00Z", "", `"updated_at" >= '2026-10-22T09:00:00Z'`},
	} {
		got := resnapFilter(tc.col, tc.from, tc.to)
		if got != tc.want {
			t.Errorf("filter(%q, %q, %q) = %s, want %s", tc.col, tc.from, tc.to, got, tc.want)
		}
		if err := clients.ValidateSnapshotCondition(got); err != nil {
			t.Errorf("filter %s fails the writers' check: %v", got, err)
		}
	}
}

// TC1: Validate, table-driven.
func TestResnapshotValidate(t *testing.T) {
	customers := func(kr map[string]any) json.RawMessage {
		return resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.customers", "key_range": kr})
	}
	for _, tc := range []struct {
		name string
		raw  json.RawMessage
		want error // nil = valid
		noIO bool
	}{
		{"key range on the pk", resnapKeys("100", "511"), nil, false},
		{"since on a timestamp", resnapSinceAt("2026-10-22T09:00:00Z"), nil, false},
		{"since as a date", resnapSinceAt("2026-10-22"), nil, false},
		{"text pk with SQL-ish value", customers(map[string]any{"column": "code", "from": "1; drop", "to": "z"}), nil, false},
		{"both key_range and since", resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.orders",
			"key_range": map[string]any{"column": "id", "from": "1", "to": "2"}, "since": map[string]any{"column": "updated_at", "at": "2026-10-22T09:00:00Z"}}), tools.ErrBadInput, true},
		{"neither", resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.orders"}), tools.ErrBadInput, true},
		{"free SQL field", resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.orders", "condition": "1=1"}), tools.ErrBadInput, true},
		{"injected column", resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.orders", "key_range": map[string]any{"column": `id" OR 1=1 --`, "from": "1", "to": "2"}}), tools.ErrBadInput, true},
		{"bad table", resnapRaw(map[string]any{"connector": "pg-orders", "table": "orders", "key_range": map[string]any{"column": "id", "from": "1", "to": "2"}}), tools.ErrBadInput, true},
		{"empty bound", resnapKeys("", "2"), tools.ErrBadInput, true},
		{"backslash in bound", resnapKeys(`1\`, "2"), tools.ErrBadInput, true},
		{"newline in bound", resnapKeys("1\n", "2"), tools.ErrBadInput, true},
		{"column neither pk nor time", resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.orders", "since": map[string]any{"column": "status", "at": "x"}}), tools.ErrBadInput, false},
		{"uuid column not pk", customers(map[string]any{"column": "ref", "from": "a", "to": "b"}), tools.ErrBadInput, false},
		{"missing column", resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.orders", "since": map[string]any{"column": "nope", "at": "1"}}), clients.ErrNotFound, false},
		{"from after to", resnapKeys("511", "100"), tools.ErrBadInput, false},
		{"time from after to", resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.orders", "key_range": map[string]any{"column": "updated_at", "from": "2026-10-22T10:00:00Z", "to": "2026-10-22T09:00:00Z"}}), tools.ErrBadInput, false},
		{"text from after to", customers(map[string]any{"column": "code", "from": "b", "to": "a"}), tools.ErrBadInput, false},
		{"not an integer", resnapKeys("1; drop", "2"), tools.ErrBadInput, false},
		{"not a time", resnapSinceAt("yesterday"), tools.ErrBadInput, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newResnapFixture(t, nil)
			f.src.noIO = tc.noIO
			err := f.a.Validate(resnapCtx(), tc.raw)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate = %v, want %v", err, tc.want)
			}
			if strings.Contains(err.Error(), "1; drop") || strings.Contains(err.Error(), "yesterday") {
				t.Fatalf("error echoes a bound: %v", err)
			}
			if f.w.calls+f.k.calls != 0 {
				t.Fatal("Validate wrote")
			}
		})
	}
}

// TC2: above max_rows the preview refuses, naming the count; owners.yaml
// recovery.max_rows moves the cap.
func TestResnapshotPreviewMaxRows(t *testing.T) {
	f := newResnapFixture(t, nil)
	f.src.counts["public.orders"] = 100_001
	_, err := f.a.(actions.Preview).Preview(resnapCtx(), resnapKeys("1", "200000"))
	if !errors.Is(err, ErrResnapshotTooLarge) || !strings.Contains(err.Error(), "100001 rows") || !strings.Contains(err.Error(), "max_rows 100000") {
		t.Fatalf("Preview = %v, want the max_rows refusal with the count", err)
	}
	if err := f.a.Validate(resnapCtx(), resnapKeys("1", "200000")); !errors.Is(err, ErrResnapshotTooLarge) {
		t.Fatalf("Validate = %v, want the same refusal", err)
	}

	pl := resnapPipeline()
	limit := owners.RowCount(50)
	pl.RecoverySpec = &owners.RecoverySpec{MaxRows: &limit}
	f = newResnapFixture(t, nil)
	f.src.counts["public.orders"] = 60
	if _, err := f.a.(actions.Preview).Preview(tools.WithPipeline(context.Background(), pl), resnapKeys("1", "60")); !errors.Is(err, ErrResnapshotTooLarge) || !strings.Contains(err.Error(), "max_rows 50") {
		t.Fatalf("override 50: %v", err)
	}
	f.src.counts["public.orders"] = 50
	if _, err := f.a.(actions.Preview).Preview(tools.WithPipeline(context.Background(), pl), resnapKeys("1", "50")); err != nil {
		t.Fatalf("at the cap: %v", err)
	}

	f.src.counts["public.orders"] = 0
	if _, err := f.a.(actions.Preview).Preview(resnapCtx(), resnapKeys("1", "2")); !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "matches no rows") {
		t.Fatalf("no rows: %v", err)
	}
}

// E1: the preview counts on the read-only source ("will re-emit N rows")
// and never shows the condition's values.
func TestResnapshotPreview(t *testing.T) {
	f := newResnapFixture(t, nil)
	raw := resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.customers", "key_range": map[string]any{"column": "code", "from": "1; drop", "to": "zz-secret"}})
	res, err := f.a.(actions.Preview).Preview(resnapCtx(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if res.Count == nil || *res.Count != 3 || res.Unit != "rows" || !strings.Contains(res.Expected, "will re-emit 3 rows") || res.Reversible {
		t.Fatalf("preview = %+v", res)
	}
	if !strings.Contains(res.Target, "signal table debezium.signal") || !strings.Contains(res.Downstream[0], "topic pg.public.customers") {
		t.Fatalf("preview target/downstream = %+v", res)
	}
	b, _ := json.Marshal(res)
	if strings.Contains(string(b), "drop") || strings.Contains(string(b), "zz-secret") {
		t.Fatalf("preview shows a bound: %s", b)
	}
	if f.src.ranges[0] != "public.customers code 1; drop..zz-secret" || !f.src.deadline {
		t.Fatalf("count = %v (deadline %v)", f.src.ranges, f.src.deadline)
	}
	if f.w.calls+f.k.calls != 0 {
		t.Fatal("Preview wrote")
	}

	f = newResnapFixture(t, resnapTopicCfg())
	res, err = f.a.(actions.Preview).Preview(resnapCtx(), resnapKeys("100", "511"))
	if err != nil || !strings.Contains(res.Target, "signal topic pg-signal") || *res.Count != 412 {
		t.Fatalf("topic preview = %+v, %v", res, err)
	}
}

// TC3 / E2: a target outside the room's pipeline, or no scope, is refused
// at every stage before any I/O.
func TestResnapshotScopeBeforeIO(t *testing.T) {
	sink := func(st string) json.RawMessage {
		return resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.orders", "sink_table": st, "key_range": map[string]any{"column": "id", "from": "1", "to": "2"}})
	}
	for _, tc := range []struct {
		name string
		ctx  context.Context
		raw  json.RawMessage
		want error
	}{
		{"other table", resnapCtx(), resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.payroll", "since": map[string]any{"column": "id", "at": "1"}}), tools.ErrOutOfScope},
		{"other connector", resnapCtx(), resnapRaw(map[string]any{"connector": "pg-payroll", "table": "public.orders", "since": map[string]any{"column": "id", "at": "1"}}), tools.ErrOutOfScope},
		{"other sink table", resnapCtx(), sink("analytics.payroll"), tools.ErrOutOfScope},
		{"batch pipeline", tools.WithPipeline(context.Background(), owners.Pipeline{ID: "b", Kind: "batch"}), resnapKeys("1", "2"), tools.ErrOutOfScope},
		{"no scope", context.Background(), resnapKeys("1", "2"), tools.ErrNoScope},
		{"pipeline id only", tools.WithPipelineID(context.Background(), "orders"), resnapKeys("1", "2"), tools.ErrNoScope},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newResnapFixture(t, nil)
			f.src.noIO, f.sink.noIO = true, true
			f.conn.err = errors.New("Connect must not be read")
			if err := f.a.Validate(tc.ctx, tc.raw); !errors.Is(err, tc.want) {
				t.Errorf("Validate = %v", err)
			}
			if _, err := f.a.(actions.Preview).Preview(tc.ctx, tc.raw); !errors.Is(err, tc.want) {
				t.Errorf("Preview = %v", err)
			}
			if _, err := f.a.Execute(tc.ctx, tc.raw); !errors.Is(err, tc.want) {
				t.Errorf("Execute = %v", err)
			}
			if _, err := f.a.(actions.Verifier).Verify(tc.ctx, tc.raw, time.Now()); !errors.Is(err, tc.want) {
				t.Errorf("Verify = %v", err)
			}
			if f.conn.calls != 0 || f.w.calls+f.k.calls != 0 {
				t.Fatalf("I/O before the scope refusal: connect %d, writes %d", f.conn.calls, f.w.calls+f.k.calls)
			}
		})
	}
	f := newResnapFixture(t, nil)
	if err := f.a.(actions.Scoped).CheckScope(resnapPipeline(), resnapKeys("1", "2")); err != nil {
		t.Fatalf("CheckScope in scope: %v", err)
	}
	if err := f.a.(actions.Scoped).CheckScope(owners.Pipeline{ID: "x", Connectors: []string{"pg-orders"}}, resnapKeys("1", "2")); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("CheckScope other pipeline: %v", err)
	}
}

// TC4: Execute takes the signal-table path or the Kafka-signal path the
// connector config names, with the condition built from the params.
func TestResnapshotExecuteRoutes(t *testing.T) {
	f := newResnapFixture(t, nil)
	raw := resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.customers", "key_range": map[string]any{"column": "code", "from": "1; drop", "to": "z"}})
	out, err := f.a.Execute(resnapCtx(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if f.w.calls != 1 || f.k.calls != 0 || f.w.signal != "debezium.signal" || f.w.connector != "pg-orders" ||
		len(f.w.tables) != 1 || f.w.tables[0] != "public.customers" || f.w.filter != `"code" >= '1; drop' AND "code" <= 'z'` {
		t.Fatalf("writer = %+v, kafka = %+v", f.w, f.k)
	}
	if !strings.Contains(out, "6f1c2a7e") || !strings.Contains(out, "re-emits 3 rows") || strings.Contains(out, "drop") {
		t.Fatalf("result = %q", out)
	}

	f = newResnapFixture(t, resnapTopicCfg())
	if _, err := f.a.Execute(resnapCtx(), resnapSinceAt("2026-10-22T11:00:00+02:00")); err != nil {
		t.Fatal(err)
	}
	if f.k.calls != 1 || f.w.calls != 0 || f.k.topic != "pg-signal" || f.k.key != "pg" || f.k.connector != "pg-orders" ||
		f.k.cond != `"updated_at" >= '2026-10-22T09:00:00Z'` {
		t.Fatalf("kafka = %+v, writer calls %d", f.k, f.w.calls)
	}
}

func TestResnapshotExecuteRefusals(t *testing.T) {
	// Write mode off: no writer at all, refused before any I/O.
	f := newResnapFixture(t, nil)
	f.src.noIO = true
	f.a = NewResnapshotRows(ResnapshotRowsDeps{Connect: f.conn, Source: f.src})
	if _, err := f.a.Execute(resnapCtx(), resnapKeys("1", "2")); err == nil || !strings.Contains(err.Error(), "no write credentials") || f.conn.calls != 0 {
		t.Fatalf("no writers = %v (connect calls %d)", err, f.conn.calls)
	}

	for _, tc := range []struct {
		name string
		cfg  map[string]string
		deps func(f *resnapFixture) ResnapshotRowsDeps
		mod  func(f *resnapFixture)
		want string
	}{
		{"table route without source writer", nil, func(f *resnapFixture) ResnapshotRowsDeps {
			return ResnapshotRowsDeps{Connect: f.conn, Source: f.src, Kafka: f.k}
		}, nil, "no write credentials"},
		{"source writer without the conditional insert", nil, func(f *resnapFixture) ResnapshotRowsDeps {
			return ResnapshotRowsDeps{Connect: f.conn, Source: f.src, Writer: &snapSigWriter{}}
		}, nil, "no InsertSnapshotSignalWhere"},
		{"topic route without Kafka writer", resnapTopicCfg(), func(f *resnapFixture) ResnapshotRowsDeps {
			return ResnapshotRowsDeps{Connect: f.conn, Source: f.src, Writer: f.w}
		}, nil, "no Kafka write client"},
		{"signal table not published", nil, nil, func(f *resnapFixture) { f.src.st.SignalPublished = false }, "not in publication"},
		{"signal table is a source table", map[string]string{
			"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "signal.data.collection": "public.customers",
		}, nil, nil, "captured data table"},
		{"no signal channel", map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}, nil, nil, "takes no snapshot signal"},
		{"over the cap at execute", nil, nil, func(f *resnapFixture) { f.src.counts["public.orders"] = 1_000_000 }, "over max_rows"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newResnapFixture(t, tc.cfg)
			if tc.deps != nil {
				f.a = NewResnapshotRows(tc.deps(f))
			}
			if tc.mod != nil {
				tc.mod(f)
			}
			_, err := f.a.Execute(resnapCtx(), resnapKeys("1", "2"))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Execute = %v, want %q", err, tc.want)
			}
			if f.w.calls+f.k.calls != 0 {
				t.Fatal("a refused step wrote")
			}
		})
	}
}

// E1 / Verify: counts on both sides over the step's range; never a writer.
func TestResnapshotVerifyKeyRange(t *testing.T) {
	f := newResnapFixture(t, nil)
	v := f.a.(actions.Verifier)
	res, err := v.Verify(resnapCtx(), resnapKeys("100", "511"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.Before != 412 || res.After != 12 || res.Unit != "rows" || len(res.Detail) > 200 || !strings.Contains(res.Detail, "analytics.orders") {
		t.Fatalf("verify = %+v", res)
	}
	if f.sink.ranges[0] != "analytics.orders id 100..511" || !f.sink.deadline {
		t.Fatalf("sink count = %v", f.sink.ranges)
	}
	f.sink.counts["analytics.orders"] = 500 // more at the sink: nothing missing
	if res, _ := v.Verify(resnapCtx(), resnapKeys("100", "511"), time.Now()); res.After != 0 || res.Before != 412 {
		t.Fatalf("sink ahead = %+v", res)
	}
	if f.w.calls+f.k.calls != 0 {
		t.Fatal("Verify wrote")
	}
	b, _ := json.Marshal(res)
	if strings.Contains(string(b), "511") {
		t.Fatalf("verify result shows a bound: %s", b)
	}
}

func TestResnapshotVerifySinceWindow(t *testing.T) {
	f := newResnapFixture(t, nil)
	executed := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	res, err := f.a.(actions.Verifier).Verify(resnapCtx(), resnapSinceAt("2026-10-22T09:00:00Z"), executed)
	if err != nil {
		t.Fatal(err)
	}
	if res.Before != 300 || res.After != 10 {
		t.Fatalf("verify = %+v", res)
	}
	want := "updated_at 2026-10-22T09:00:00Z..2026-10-22T10:00:00Z"
	if f.src.windows[0] != "public.orders "+want || f.sink.windows[0] != "analytics.orders "+want || len(f.src.ranges) != 0 {
		t.Fatalf("windows = %v / %v, ranges %v", f.src.windows, f.sink.windows, f.src.ranges)
	}
	// Executed before the window starts: nothing to count.
	if res, err := f.a.(actions.Verifier).Verify(resnapCtx(), resnapSinceAt("2026-10-22T11:00:00Z"), executed); err != nil || res.Before != 0 || res.After != 0 {
		t.Fatalf("empty window = %+v, %v", res, err)
	}
}

// TC4 of X2c's input: a sink that cannot answer is unverifiable.
func TestResnapshotVerifyUnsupported(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  json.RawMessage
		mod  func(f *resnapFixture)
	}{
		{"no sink client", resnapKeys("1", "2"), func(f *resnapFixture) {
			f.a = NewResnapshotRows(ResnapshotRowsDeps{Connect: f.conn, Source: f.src})
		}},
		{"sink column missing", resnapKeys("1", "2"), func(f *resnapFixture) { delete(f.sink.cols, "analytics.orders.id") }},
		{"sink column of another kind", resnapKeys("1", "2"), func(f *resnapFixture) {
			f.sink.cols["analytics.orders.id"] = postgres.RangeColumn{Type: "text", Kind: pgconv.RangeKindText}
		}},
		{"sink table missing", resnapKeys("1", "2"), func(f *resnapFixture) { f.sink.countErr = clients.ErrNotFound }},
		{"no sink table named like the source", resnapRaw(map[string]any{"connector": "pg-orders", "table": "public.orders", "key_range": map[string]any{"column": "id", "from": "1", "to": "2"}}),
			func(f *resnapFixture) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newResnapFixture(t, nil)
			tc.mod(f)
			ctx := resnapCtx()
			if tc.name == "no sink table named like the source" {
				pl := resnapPipeline()
				pl.SinkTables = []string{"analytics.a", "analytics.b"}
				ctx = tools.WithPipeline(context.Background(), pl)
			}
			if _, err := f.a.(actions.Verifier).Verify(ctx, tc.raw, time.Now()); !errors.Is(err, clients.ErrNotSupported) {
				t.Fatalf("Verify = %v, want ErrNotSupported", err)
			}
		})
	}
	// Any other read failure is a failed poll, not unverifiable.
	f := newResnapFixture(t, nil)
	f.src.countErr = context.DeadlineExceeded
	if _, err := f.a.(actions.Verifier).Verify(resnapCtx(), resnapKeys("1", "2"), time.Now()); err == nil || errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("source timeout = %v", err)
	}
}

func TestResnapshotSinkTable(t *testing.T) {
	pl := resnapPipeline()
	for _, tc := range []struct {
		table, sink string
		sinks       []string
		want        string
	}{
		{"public.orders", "", nil, "analytics.orders"},
		{"public.orders", "analytics.customers", nil, "analytics.customers"},
		{"public.orders", "", []string{"public.orders", "analytics.orders"}, "public.orders"},
		{"public.orders", "", []string{"dw.fact"}, "dw.fact"},
		{"public.orders", "", []string{"a.orders", "b.orders"}, ""},
	} {
		p := pl
		if tc.sinks != nil {
			p.SinkTables = tc.sinks
		}
		got, err := resnapSinkTable(resnapParams{Table: tc.table, SinkTable: tc.sink}, p, true)
		if got != tc.want || (tc.want == "") != (err != nil) {
			t.Errorf("sink of %s (%s, %v) = %q, %v; want %q", tc.table, tc.sink, tc.sinks, got, err, tc.want)
		}
	}
}

// E7: params_hash input is the raw params, and the parse is deterministic:
// the same step decodes the same way twice and builds the same condition.
func TestResnapshotStableCondition(t *testing.T) {
	f := newResnapFixture(t, nil)
	raw := resnapSinceAt("2026-10-22T11:00:00+02:00")
	p1, err1 := parseResnapshot(raw)
	p2, err2 := parseResnapshot(raw)
	if err1 != nil || err2 != nil || *p1.Since != *p2.Since {
		t.Fatalf("parse not stable: %v %v", err1, err2)
	}
	c1, err1 := f.a.(*resnapshotRows).condition(resnapCtx(), f.src, p1)
	c2, err2 := f.a.(*resnapshotRows).condition(resnapCtx(), f.src, p2)
	if err1 != nil || err2 != nil || c1.filter != c2.filter || c1.filter != `"updated_at" >= '2026-10-22T09:00:00Z'` {
		t.Fatalf("conditions %q %q (%v %v)", c1.filter, c2.filter, err1, err2)
	}
}

// The dev Postgres warehouse is a whole sink for Verify.
var (
	_ RowRangeCounter = (*warehouse.DB)(nil)
	_ SinkCounter     = (*warehouse.DB)(nil)
)
