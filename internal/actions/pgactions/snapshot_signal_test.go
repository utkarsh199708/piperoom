package pgactions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// snapSigSource is a fake read-only source client. Its SourceDB methods are
// never called (nil embedded interface).
type snapSigSource struct {
	clients.SourceDB
	st       postgres.SnapshotSignalState
	err      error
	calls    int
	pub, sig string
	tables   []string
	deadline bool
}

func (s *snapSigSource) SnapshotSignalState(ctx context.Context, publication, signalTable string, tables []string) (postgres.SnapshotSignalState, error) {
	s.calls++
	s.pub, s.sig, s.tables = publication, signalTable, tables
	_, s.deadline = ctx.Deadline()
	if s.err != nil {
		return postgres.SnapshotSignalState{}, s.err
	}
	st := s.st
	if st.Tables == nil {
		for _, t := range tables {
			st.Tables = append(st.Tables, postgres.SnapshotTableState{Table: t, Exists: true, EstimatedRows: 1000})
		}
	}
	return st, nil
}

// snapSigWriter is a fake SourceWriter that records InsertSnapshotSignal;
// any other write panics on the nil embedded interface.
type snapSigWriter struct {
	clients.SourceWriter
	err       error
	calls     int
	signal    string
	connector string
	tables    []string
	deadline  bool
}

func (w *snapSigWriter) InsertSnapshotSignal(ctx context.Context, signalTable, connectorID string, tables []string) (string, error) {
	w.calls++
	w.signal, w.connector, w.tables = signalTable, connectorID, tables
	_, w.deadline = ctx.Deadline()
	if w.err != nil {
		return "", w.err
	}
	return "6f1c2a7e-0d3b-4c55-9a1e-2b7f0c9d8e11", nil
}

func snapSigPipeline() owners.Pipeline {
	return owners.Pipeline{
		ID:           "orders",
		TopicPrefix:  "pg",
		Connectors:   []string{"pg-orders"},
		SourceTables: []string{"public.orders", "public.customers", "public.refunds"},
	}
}

func snapSigConfig() map[string]string {
	return map[string]string{
		"connector.class":         "io.debezium.connector.postgresql.PostgresConnector",
		"publication.name":        "dbz_publication",
		"signal.enabled.channels": "source",
		"signal.data.collection":  "debezium.signal",
	}
}

func snapSigOKState() postgres.SnapshotSignalState {
	return postgres.SnapshotSignalState{PublicationExists: true, SignalExists: true, SignalColumns: true, SignalPublished: true}
}

type snapSigFixture struct {
	conn *addPubConnect
	src  *snapSigSource
	w    *snapSigWriter
	a    actions.Action
}

func newSnapSigFixture(cfg map[string]string) *snapSigFixture {
	if cfg == nil {
		cfg = snapSigConfig()
	}
	f := &snapSigFixture{
		conn: &addPubConnect{cfg: map[string]map[string]string{"pg-orders": cfg, "jdbc-sink": {"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector"}}},
		src:  &snapSigSource{st: snapSigOKState()},
		w:    &snapSigWriter{},
	}
	f.a = NewSnapshotSignal(SnapshotSignalDeps{Connect: f.conn, Source: f.src, Writer: f.w})
	return f
}

func snapSigParams(connector string, tables ...string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{"connector": connector, "tables": tables})
	return b
}

var snapSigGood = snapSigParams("pg-orders", "public.orders", "public.refunds")

func snapSigScopedCtx() context.Context {
	return tools.WithPipeline(context.Background(), snapSigPipeline())
}

func TestSnapshotSignalMeta(t *testing.T) {
	a := newSnapSigFixture(nil).a
	if a.Name() != "pg_snapshot_signal" || a.Layer() != "postgres" || a.Describe() == "" {
		t.Fatalf("meta: %s %s", a.Name(), a.Layer())
	}
	s := a.ParamsSchema()
	if s["type"] != "object" || s["additionalProperties"] != false {
		t.Fatalf("schema: %v", s)
	}
	if _, ok := a.(actions.Scoped); !ok {
		t.Fatal("must be Scoped")
	}
	if _, ok := a.(actions.Preview); !ok {
		t.Fatal("must be Preview")
	}
	if err := actions.NewRegistry().Register(a); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotSignalRefusesBadParamsBeforeIO(t *testing.T) {
	many := make([]string, snapSigMaxTables+1)
	for i := range many {
		many[i] = fmt.Sprintf("public.t%d", i)
	}
	long := make([]string, 40)
	for i := range long {
		long[i] = "public." + strings.Repeat("x", 55) + fmt.Sprint(i)
	}
	cases := []struct {
		name string
		raw  json.RawMessage
	}{
		{"not json", json.RawMessage(`{`)},
		{"unknown field", json.RawMessage(`{"connector":"pg-orders","tables":["public.orders"],"sql":"DROP TABLE x"}`)},
		{"signal_table injection", json.RawMessage(`{"connector":"pg-orders","tables":["public.orders"],"signal_table":"debezium.signal; DROP TABLE x"}`)},
		{"signal_table no schema", json.RawMessage(`{"connector":"pg-orders","tables":["public.orders"],"signal_table":"signal"}`)},
		{"no connector", snapSigParams("", "public.orders")},
		{"no tables", snapSigParams("pg-orders")},
		{"too many tables", snapSigParams("pg-orders", many...)},
		{"data over 2048 bytes", snapSigParams("pg-orders", long...)},
		{"table injection", snapSigParams("pg-orders", "public.orders'); DELETE FROM x; --")},
		{"table quoted", snapSigParams("pg-orders", `public."orders"`)},
		{"table no schema", snapSigParams("pg-orders", "orders")},
		{"table three parts", snapSigParams("pg-orders", "db.public.orders")},
		{"table regex", snapSigParams("pg-orders", "public.*")},
		{"table regex dot", snapSigParams("pg-orders", "public.ord.rs")},
		{"table json breakout", snapSigParams("pg-orders", `public.orders"],"type":"blocking`)},
		{"table digit", snapSigParams("pg-orders", "public.1orders")},
		{"table too long", snapSigParams("pg-orders", "public."+strings.Repeat("o", 64))},
		{"one bad of two", snapSigParams("pg-orders", "public.orders", "public.orders;")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSnapSigFixture(nil)
			if err := f.a.Validate(context.Background(), tc.raw); err == nil {
				t.Fatal("Validate: expected a refusal")
			}
			if err := f.a.(actions.Scoped).CheckScope(snapSigPipeline(), tc.raw); err == nil {
				t.Fatal("CheckScope: expected a refusal")
			}
			if _, err := f.a.(actions.Preview).Preview(snapSigScopedCtx(), tc.raw); err == nil {
				t.Fatal("Preview: expected a refusal")
			}
			if _, err := f.a.Execute(snapSigScopedCtx(), tc.raw); err == nil {
				t.Fatal("Execute: expected a refusal")
			}
			if f.conn.calls+f.src.calls+f.w.calls != 0 {
				t.Fatalf("a refused call did I/O: connect %d source %d writer %d", f.conn.calls, f.src.calls, f.w.calls)
			}
		})
	}
}

func TestSnapshotSignalValidateOK(t *testing.T) {
	f := newSnapSigFixture(nil)
	raw := snapSigParams("pg-orders", "public.orders", "public.refunds", "public.orders")
	if err := f.a.Validate(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if f.src.pub != "dbz_publication" || f.src.sig != "debezium.signal" || !reflect.DeepEqual(f.src.tables, []string{"public.orders", "public.refunds"}) {
		t.Fatalf("read %q %q %v", f.src.pub, f.src.sig, f.src.tables)
	}
	if !f.src.deadline {
		t.Fatal("Validate read without a deadline")
	}
	if f.w.calls != 0 {
		t.Fatal("Validate touched the writer")
	}
}

func TestSnapshotSignalValidateRefusals(t *testing.T) {
	with := func(mod func(map[string]string)) map[string]string {
		c := snapSigConfig()
		mod(c)
		return c
	}
	state := func(mod func(*postgres.SnapshotSignalState)) postgres.SnapshotSignalState {
		s := snapSigOKState()
		mod(&s)
		return s
	}
	cases := []struct {
		name    string
		cfg     map[string]string
		st      postgres.SnapshotSignalState
		conn    string
		wantSub string
		wantIs  error
	}{
		{"not a postgres source", nil, snapSigOKState(), "jdbc-sink", "not a Debezium Postgres", nil},
		{"unknown connector", nil, snapSigOKState(), "pg-nope", "read connector", clients.ErrNotFound},
		{"no signal collection", with(func(c map[string]string) { delete(c, "signal.data.collection") }), snapSigOKState(), "pg-orders", "signal.data.collection to the signal table", ErrSignalTableNotConfigured},
		{"source channel off", with(func(c map[string]string) { c["signal.enabled.channels"] = "kafka" }), snapSigOKState(), "pg-orders", "include source", ErrSignalTableNotConfigured},
		{"collection not schema.table", with(func(c map[string]string) { c["signal.data.collection"] = "orders.debezium.signal" }), snapSigOKState(), "pg-orders", "not schema.table", ErrSignalTableNotConfigured},
		{"collection injection", with(func(c map[string]string) { c["signal.data.collection"] = `public.sig"; DROP TABLE x` }), snapSigOKState(), "pg-orders", "not schema.table", ErrSignalTableNotConfigured},
		{"bad publication.name", with(func(c map[string]string) { c["publication.name"] = `p"; x` }), snapSigOKState(), "pg-orders", "not a plain identifier", nil},
		{"signal table missing", nil, state(func(s *postgres.SnapshotSignalState) { s.SignalExists = false }), "pg-orders", "does not exist", clients.ErrNotFound},
		{"signal table columns", nil, state(func(s *postgres.SnapshotSignalState) { s.SignalColumns = false }), "pg-orders", "id, type and data", ErrSignalTableNotConfigured},
		{"publication missing", nil, state(func(s *postgres.SnapshotSignalState) { s.PublicationExists = false }), "pg-orders", "publication dbz_publication", clients.ErrNotFound},
		{"signal not published", nil, state(func(s *postgres.SnapshotSignalState) { s.SignalPublished = false }), "pg-orders", "never sees the signal", ErrSignalTableNotConfigured},
		{"table missing", nil, state(func(s *postgres.SnapshotSignalState) {
			s.Tables = []postgres.SnapshotTableState{{Table: "public.orders", Exists: true}, {Table: "public.refunds", EstimatedRows: -1}}
		}), "pg-orders", "public.refunds does not exist", clients.ErrNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSnapSigFixture(tc.cfg)
			f.src.st = tc.st
			err := f.a.Validate(context.Background(), snapSigParams(tc.conn, "public.orders", "public.refunds"))
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("err = %v, want %q", err, tc.wantSub)
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.wantIs)
			}
			if f.w.calls != 0 {
				t.Fatal("Validate touched the writer")
			}
		})
	}
}

func TestSnapshotSignalDefaultsChannelAndPublication(t *testing.T) {
	cfg := snapSigConfig()
	delete(cfg, "signal.enabled.channels") // Debezium's default is source
	delete(cfg, "publication.name")
	f := newSnapSigFixture(cfg)
	if err := f.a.Validate(context.Background(), snapSigGood); err != nil {
		t.Fatal(err)
	}
	if f.src.pub != "dbz_publication" {
		t.Fatalf("publication = %q", f.src.pub)
	}
}

func TestSnapshotSignalValidateReadErrorWrapped(t *testing.T) {
	f := newSnapSigFixture(nil)
	boom := errors.New("connection refused")
	f.src.err = boom
	if err := f.a.Validate(context.Background(), snapSigGood); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	f = newSnapSigFixture(nil)
	f.conn.err = context.DeadlineExceeded
	if err := f.a.Validate(context.Background(), snapSigGood); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestSnapshotSignalNoReaderRefuses(t *testing.T) {
	a := NewSnapshotSignal(SnapshotSignalDeps{Connect: &addPubConnect{}, Source: &addPubSource{}})
	if err := a.Validate(context.Background(), snapSigGood); err == nil || !strings.Contains(err.Error(), "SnapshotSignalState") {
		t.Fatalf("err = %v", err)
	}
}

func TestSnapshotSignalCheckScope(t *testing.T) {
	cdcOff := snapSigPipeline()
	cdcOff.Kind = events.PipelineKindBatch
	cases := []struct {
		name string
		pl   owners.Pipeline
		raw  json.RawMessage
		ok   bool
	}{
		{"in scope", snapSigPipeline(), snapSigGood, true},
		{"other connector", snapSigPipeline(), snapSigParams("pg-billing", "public.orders"), false},
		{"table outside source_tables", snapSigPipeline(), snapSigParams("pg-orders", "public.orders", "public.invoices"), false},
		{"not a CDC pipeline", cdcOff, snapSigGood, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSnapSigFixture(nil)
			err := f.a.(actions.Scoped).CheckScope(tc.pl, tc.raw)
			if tc.ok != (err == nil) {
				t.Fatalf("err = %v", err)
			}
			if !tc.ok && !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("err = %v, want ErrOutOfScope", err)
			}
			if f.conn.calls+f.src.calls+f.w.calls != 0 {
				t.Fatal("CheckScope did I/O")
			}
		})
	}
}

func TestSnapshotSignalExecute(t *testing.T) {
	f := newSnapSigFixture(nil)
	out, err := f.a.Execute(snapSigScopedCtx(), snapSigGood)
	if err != nil {
		t.Fatal(err)
	}
	if f.w.calls != 1 || f.w.signal != "debezium.signal" || f.w.connector != "pg-orders" || !reflect.DeepEqual(f.w.tables, []string{"public.orders", "public.refunds"}) {
		t.Fatalf("writer got %d %q %q %v", f.w.calls, f.w.signal, f.w.connector, f.w.tables)
	}
	if !f.w.deadline {
		t.Fatal("Execute wrote without a deadline")
	}
	if !strings.Contains(out, "6f1c2a7e-0d3b-4c55-9a1e-2b7f0c9d8e11") || !strings.Contains(out, "debezium.signal") {
		t.Fatalf("result %q must name the signal id and table", out)
	}
	if f.src.calls != 1 || f.src.sig != "debezium.signal" {
		t.Fatalf("Execute must re-read the signal state before the write: %d reads of %q", f.src.calls, f.src.sig)
	}
}

func snapSigWithSignal(signal string, tables ...string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{"connector": "pg-orders", "tables": tables, "signal_table": signal})
	return b
}

func TestSnapshotSignalExecuteWithApprovedSignalTable(t *testing.T) {
	f := newSnapSigFixture(nil)
	raw := snapSigWithSignal("debezium.signal", "public.orders")
	if err := f.a.Validate(snapSigScopedCtx(), raw); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if _, err := f.a.Execute(snapSigScopedCtx(), raw); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if f.w.calls != 1 || f.w.signal != "debezium.signal" {
		t.Fatalf("writer got %d %q", f.w.calls, f.w.signal)
	}
}

// The connector config moves between approval and Execute: the row must not
// land in a table the approvers never saw.
func TestSnapshotSignalExecuteConfigChangedAfterApproval(t *testing.T) {
	cases := []struct {
		name    string
		raw     json.RawMessage
		newSig  string
		state   func(*postgres.SnapshotSignalState)
		wantIs  error
		wantSub string
	}{
		{"recorded signal table differs", snapSigWithSignal("debezium.signal", "public.orders"), "debezium.other_signal", nil, ErrSignalTableChanged, "config changed"},
		{"now a source table, recorded", snapSigWithSignal("debezium.signal", "public.orders"), "public.customers", nil, ErrSignalTableChanged, "config changed"},
		{"now a source table, not recorded", snapSigParams("pg-orders", "public.orders"), "public.customers", nil, ErrSignalIsSourceTable, "captured data table"},
		{"now a table being snapshotted", snapSigParams("pg-orders", "public.orders"), "public.orders", nil, ErrSignalIsSourceTable, "captured data table"},
		{"not recorded, new table missing", snapSigParams("pg-orders", "public.orders"), "debezium.other_signal",
			func(s *postgres.SnapshotSignalState) { s.SignalExists = false }, clients.ErrNotFound, "refused at execute"},
		{"not recorded, new table lacks columns", snapSigParams("pg-orders", "public.orders"), "debezium.other_signal",
			func(s *postgres.SnapshotSignalState) { s.SignalColumns = false }, ErrSignalTableNotConfigured, "id, type and data"},
		{"not recorded, new table not published", snapSigParams("pg-orders", "public.orders"), "debezium.other_signal",
			func(s *postgres.SnapshotSignalState) { s.SignalPublished = false }, ErrSignalTableNotConfigured, "not in publication"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSnapSigFixture(nil)
			// Approval: Validate passes against the config as it was.
			if err := f.a.Validate(snapSigScopedCtx(), tc.raw); err != nil {
				t.Fatalf("Validate at approval: %v", err)
			}
			// The config and the database move.
			f.conn.cfg["pg-orders"]["signal.data.collection"] = tc.newSig
			if tc.state != nil {
				tc.state(&f.src.st)
			}
			_, err := f.a.Execute(snapSigScopedCtx(), tc.raw)
			if !errors.Is(err, tc.wantIs) || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("err = %v, want %v containing %q", err, tc.wantIs, tc.wantSub)
			}
			if f.w.calls != 0 {
				t.Fatalf("wrote into %q after the config changed", f.w.signal)
			}
		})
	}
}

func TestSnapshotSignalRefusesSourceTableAsSignalTable(t *testing.T) {
	cfg := snapSigConfig()
	cfg["signal.data.collection"] = "public.customers" // one of the pipeline's source_tables
	f := newSnapSigFixture(cfg)
	if err := f.a.Validate(snapSigScopedCtx(), snapSigGood); !errors.Is(err, ErrSignalIsSourceTable) {
		t.Fatalf("Validate: %v, want ErrSignalIsSourceTable", err)
	}
	if _, err := f.a.Execute(snapSigScopedCtx(), snapSigGood); !errors.Is(err, ErrSignalIsSourceTable) {
		t.Fatalf("Execute: %v, want ErrSignalIsSourceTable", err)
	}
	if _, err := f.a.Execute(tools.WithUnscoped(context.Background()), snapSigParams("pg-orders", "public.customers")); !errors.Is(err, ErrSignalIsSourceTable) {
		t.Fatalf("unscoped Execute snapshotting the signal table: %v, want ErrSignalIsSourceTable", err)
	}
	if f.w.calls != 0 {
		t.Fatal("wrote a signal row into a source table")
	}
	res, err := f.a.(actions.Preview).Preview(snapSigScopedCtx(), snapSigGood)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(res.Expected, "no change: this step is refused") || !strings.Contains(res.Expected, "captured data table") || res.Rollback != "" {
		t.Fatalf("preview = %+v", res)
	}
	// Without the pipeline in ctx Validate still refuses a signal table that
	// is one of the tables being snapshotted.
	if err := f.a.Validate(context.Background(), snapSigParams("pg-orders", "public.customers")); !errors.Is(err, ErrSignalIsSourceTable) {
		t.Fatalf("Validate unscoped: %v, want ErrSignalIsSourceTable", err)
	}
}

func TestSnapshotSignalValidateRefusesOtherRecordedSignalTable(t *testing.T) {
	f := newSnapSigFixture(nil)
	if err := f.a.Validate(context.Background(), snapSigWithSignal("debezium.elsewhere", "public.orders")); !errors.Is(err, ErrSignalTableChanged) {
		t.Fatalf("err = %v, want ErrSignalTableChanged", err)
	}
	if f.src.calls != 0 {
		t.Fatal("read the source for a step that is refused on its config")
	}
}

func TestSnapshotSignalExecuteRefusals(t *testing.T) {
	t.Run("write mode off", func(t *testing.T) {
		f := newSnapSigFixture(nil)
		a := NewSnapshotSignal(SnapshotSignalDeps{Connect: f.conn, Source: f.src}) // Writer: nil
		// Validate and Preview still work, so the step can be proposed.
		if err := a.Validate(context.Background(), snapSigGood); err != nil {
			t.Fatalf("Validate with write mode off: %v", err)
		}
		if _, err := a.(actions.Preview).Preview(snapSigScopedCtx(), snapSigGood); err != nil {
			t.Fatalf("Preview with write mode off: %v", err)
		}
		_, err := a.Execute(snapSigScopedCtx(), snapSigGood)
		if err == nil || !strings.Contains(err.Error(), "no write credentials") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("no scope", func(t *testing.T) {
		f := newSnapSigFixture(nil)
		if _, err := f.a.Execute(context.Background(), snapSigGood); !errors.Is(err, tools.ErrNoScope) {
			t.Fatalf("err = %v, want ErrNoScope", err)
		}
		if _, err := f.a.Execute(tools.WithPipelineID(context.Background(), "orders"), snapSigGood); !errors.Is(err, tools.ErrNoScope) {
			t.Fatalf("id-only scope: err = %v, want ErrNoScope", err)
		}
		if f.conn.calls+f.w.calls != 0 {
			t.Fatal("refused Execute did I/O")
		}
	})
	t.Run("out of scope", func(t *testing.T) {
		f := newSnapSigFixture(nil)
		other := owners.Pipeline{ID: "billing", Connectors: []string{"pg-orders"}, SourceTables: []string{"public.invoices"}}
		if _, err := f.a.Execute(tools.WithPipeline(context.Background(), other), snapSigGood); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("err = %v, want ErrOutOfScope", err)
		}
		if f.conn.calls+f.w.calls != 0 {
			t.Fatal("refused Execute did I/O")
		}
	})
	t.Run("config changed since approval", func(t *testing.T) {
		f := newSnapSigFixture(nil)
		delete(f.conn.cfg["pg-orders"], "signal.data.collection")
		if _, err := f.a.Execute(snapSigScopedCtx(), snapSigGood); !errors.Is(err, ErrSignalTableNotConfigured) {
			t.Fatalf("err = %v", err)
		}
		if f.w.calls != 0 {
			t.Fatal("wrote with no signal table configured")
		}
	})
	t.Run("writer error wrapped", func(t *testing.T) {
		f := newSnapSigFixture(nil)
		f.w.err = fmt.Errorf("postgres: insert: %w", clients.ErrNotFound)
		if _, err := f.a.Execute(snapSigScopedCtx(), snapSigGood); !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestSnapshotSignalExecuteUnscopedOptOut(t *testing.T) {
	f := newSnapSigFixture(nil)
	if _, err := f.a.Execute(tools.WithUnscoped(context.Background()), snapSigGood); err != nil {
		t.Fatal(err)
	}
	if f.w.calls != 1 {
		t.Fatal("unscoped Execute did not write")
	}
}

func TestSnapshotSignalPreview(t *testing.T) {
	f := newSnapSigFixture(nil)
	f.src.st.Tables = []postgres.SnapshotTableState{
		{Table: "public.orders", Exists: true, EstimatedRows: 12_000_000},
		{Table: "public.refunds", Exists: true, EstimatedRows: -1},
	}
	res, err := f.a.(actions.Preview).Preview(snapSigScopedCtx(), snapSigGood)
	if err != nil {
		t.Fatal(err)
	}
	if f.w.calls != 0 {
		t.Fatal("Preview touched the writer")
	}
	if !f.src.deadline {
		t.Fatal("Preview read without a deadline")
	}
	for _, want := range []string{
		`'execute-snapshot'`,
		`{"data-collections":["public.orders","public.refunds"],"type":"INCREMENTAL"}`,
		`"debezium"."signal"`,
		"~12000000 rows",
		"never analyzed",
		"Warning: a large snapshot",
		"chunks of 1024",
	} {
		if !strings.Contains(res.Expected, want) {
			t.Errorf("Expected lacks %q:\n%s", want, res.Expected)
		}
	}
	if !strings.Contains(res.Current, "is in publication dbz_publication") || !strings.Contains(res.Current, "public.orders ~12000000 rows") {
		t.Errorf("Current = %q", res.Current)
	}
	if res.Reversible || !strings.Contains(res.Rollback, "stop-snapshot") {
		t.Errorf("rollback = %v %q", res.Reversible, res.Rollback)
	}
	if len(res.Downstream) != 2 || !strings.Contains(res.Downstream[0], "topic pg.public.orders") {
		t.Errorf("downstream = %v", res.Downstream)
	}
}

func TestSnapshotSignalPreviewSmallHasNoWarningAndCustomChunk(t *testing.T) {
	cfg := snapSigConfig()
	cfg["incremental.snapshot.chunk.size"] = "5000"
	f := newSnapSigFixture(cfg)
	res, err := f.a.(actions.Preview).Preview(snapSigScopedCtx(), snapSigGood)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(res.Expected, "Warning") || !strings.Contains(res.Expected, "chunks of 5000") || !strings.Contains(res.Expected, "~2000 rows") {
		t.Fatalf("Expected = %s", res.Expected)
	}
}

func TestSnapshotSignalPreviewSignalTableCaveat(t *testing.T) {
	cfg := snapSigConfig()
	cfg["table.include.list"] = "public.orders,public.refunds"
	f := newSnapSigFixture(cfg)
	res, err := f.a.(actions.Preview).Preview(snapSigScopedCtx(), snapSigGood)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Expected, "Signal table: Follow-up needed: the connector's table.include.list does not list debezium.signal") {
		t.Fatalf("Expected = %s", res.Expected)
	}
}

func TestSnapshotSignalPreviewRefusedState(t *testing.T) {
	f := newSnapSigFixture(nil)
	f.src.st.SignalPublished = false
	res, err := f.a.(actions.Preview).Preview(snapSigScopedCtx(), snapSigGood)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(res.Expected, "no change: this step is refused") || !strings.Contains(res.Current, "NOT in publication") || res.Rollback != "" {
		t.Fatalf("preview = %+v", res)
	}
}

func TestSnapshotSignalPreviewScope(t *testing.T) {
	f := newSnapSigFixture(nil)
	p := f.a.(actions.Preview)
	if _, err := p.Preview(context.Background(), snapSigGood); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: %v", err)
	}
	if _, err := p.Preview(snapSigScopedCtx(), snapSigParams("pg-orders", "public.invoices")); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("out of scope: %v", err)
	}
	if f.conn.calls+f.src.calls != 0 {
		t.Fatal("refused Preview did I/O")
	}
	res, err := p.Preview(tools.WithUnscoped(context.Background()), snapSigGood)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Downstream) != 2 || !strings.Contains(res.Downstream[0], "the topic of public.orders") {
		t.Fatalf("unscoped downstream = %v", res.Downstream)
	}
}
