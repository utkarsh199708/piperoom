package pgactions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// addPubConnect is a fake Connect config reader.
type addPubConnect struct {
	cfg   map[string]map[string]string
	err   error
	calls int
}

func (c *addPubConnect) Config(_ context.Context, name string) (map[string]string, error) {
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	cfg, ok := c.cfg[name]
	if !ok {
		return nil, fmt.Errorf("connector %q: %w", name, clients.ErrNotFound)
	}
	return cfg, nil
}

// addPubSource is a fake read-only source client. Its SourceDB methods are
// never called (nil embedded interface).
type addPubSource struct {
	clients.SourceDB
	st       postgres.PublicationAddState
	err      error
	calls    int
	visible  []string
	deadline bool
}

func (s *addPubSource) PublicationAddState(ctx context.Context, publication, table string, visible []string) (postgres.PublicationAddState, error) {
	s.calls++
	s.visible = visible
	_, s.deadline = ctx.Deadline()
	return s.st, s.err
}

// addPubWriter is a fake SourceWriter that records AddPublicationTables.
type addPubWriter struct {
	clients.SourceWriter
	err   error
	calls int
	pub   string
	tabs  []string
}

func (w *addPubWriter) AddPublicationTables(ctx context.Context, publication string, tables []string) error {
	w.calls++
	w.pub, w.tabs = publication, tables
	return w.err
}

func addPubPipeline() owners.Pipeline {
	return owners.Pipeline{
		ID:           "orders",
		TopicPrefix:  "pg",
		Connectors:   []string{"pg-orders"},
		SourceTables: []string{"public.orders", "public.customers", "public.refunds"},
	}
}

func addPubOKState() postgres.PublicationAddState {
	return postgres.PublicationAddState{
		Members: []string{"public.customers", "public.orders"}, OtherMembers: 1,
		TableExists: true, RelKind: "r", ReplicaIdentity: "d", HasPrimaryKey: true, EstimatedRows: 1200,
	}
}

type addPubFixture struct {
	conn *addPubConnect
	src  *addPubSource
	w    *addPubWriter
	a    actions.Action
}

func newAddPubFixture(cfg map[string]string) *addPubFixture {
	if cfg == nil {
		cfg = map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "publication.name": "dbz_publication"}
	}
	f := &addPubFixture{
		conn: &addPubConnect{cfg: map[string]map[string]string{"pg-orders": cfg, "jdbc-sink": {"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector"}}},
		src:  &addPubSource{st: addPubOKState()},
		w:    &addPubWriter{},
	}
	f.a = NewAddPublicationTable(AddPublicationTableDeps{Connect: f.conn, Source: f.src, Writer: f.w})
	return f
}

func addPubParams(connector, pub, table string) json.RawMessage {
	b, _ := json.Marshal(map[string]string{"connector": connector, "publication": pub, "table": table})
	return b
}

var addPubGood = addPubParams("pg-orders", "dbz_publication", "public.refunds")

func addPubScopedCtx() context.Context {
	return tools.WithPipeline(context.Background(), addPubPipeline())
}

func TestAddPublicationTableMeta(t *testing.T) {
	a := newAddPubFixture(nil).a
	if a.Name() != "pg_add_publication_table" || a.Layer() != "postgres" || a.Describe() == "" {
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
	reg := actions.NewRegistry()
	if err := reg.Register(a); err != nil {
		t.Fatal(err)
	}
}

func TestAddPublicationTableRefusesBadIdentifiersBeforeIO(t *testing.T) {
	cases := []struct {
		name string
		raw  json.RawMessage
	}{
		{"not json", json.RawMessage(`{`)},
		{"unknown field", json.RawMessage(`{"connector":"pg-orders","publication":"dbz_publication","table":"public.refunds","sql":"DROP TABLE x"}`)},
		{"no connector", addPubParams("", "dbz_publication", "public.refunds")},
		{"publication injection", addPubParams("pg-orders", `dbz_publication"; DROP TABLE x; --`, "public.refunds")},
		{"publication space", addPubParams("pg-orders", "dbz publication", "public.refunds")},
		{"publication digit", addPubParams("pg-orders", "1pub", "public.refunds")},
		{"publication too long", addPubParams("pg-orders", strings.Repeat("p", 64), "public.refunds")},
		{"table injection", addPubParams("pg-orders", "dbz_publication", "public.refunds; DROP TABLE x")},
		{"table quoted", addPubParams("pg-orders", "dbz_publication", `public."refunds"`)},
		{"table no schema", addPubParams("pg-orders", "dbz_publication", "refunds")},
		{"table three parts", addPubParams("pg-orders", "dbz_publication", "db.public.refunds")},
		{"table regex", addPubParams("pg-orders", "dbz_publication", "public.*")},
		{"table comment", addPubParams("pg-orders", "dbz_publication", "public.refunds--")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAddPubFixture(nil)
			if err := f.a.Validate(context.Background(), tc.raw); err == nil {
				t.Fatal("Validate: expected a refusal")
			}
			if err := f.a.(actions.Scoped).CheckScope(addPubPipeline(), tc.raw); err == nil {
				t.Fatal("CheckScope: expected a refusal")
			}
			if _, err := f.a.(actions.Preview).Preview(addPubScopedCtx(), tc.raw); err == nil {
				t.Fatal("Preview: expected a refusal")
			}
			if _, err := f.a.Execute(addPubScopedCtx(), tc.raw); err == nil {
				t.Fatal("Execute: expected a refusal")
			}
			if f.conn.calls+f.src.calls+f.w.calls != 0 {
				t.Fatalf("a refused call did I/O: connect %d source %d writer %d", f.conn.calls, f.src.calls, f.w.calls)
			}
		})
	}
}

func TestAddPublicationTableValidateOK(t *testing.T) {
	f := newAddPubFixture(nil)
	// No scope in ctx and no writer: Validate depends on neither.
	f.a = NewAddPublicationTable(AddPublicationTableDeps{Connect: f.conn, Source: f.src})
	if err := f.a.Validate(context.Background(), addPubGood); err != nil {
		t.Fatal(err)
	}
	if !f.src.deadline {
		t.Fatal("Validate must read with a deadline")
	}
	if len(f.src.visible) != 1 || f.src.visible[0] != "public.refunds" {
		t.Fatalf("Validate must name only the target table, got %v", f.src.visible)
	}
}

func TestAddPublicationTableDefaultPublication(t *testing.T) {
	f := newAddPubFixture(map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector"})
	if err := f.a.Validate(context.Background(), addPubGood); err != nil {
		t.Fatalf("dbz_publication is the default: %v", err)
	}
	if err := f.a.Validate(context.Background(), addPubParams("pg-orders", "other_pub", "public.refunds")); err == nil {
		t.Fatal("expected wrong publication")
	}
}

func TestAddPublicationTableValidateRefusals(t *testing.T) {
	cases := []struct {
		name   string
		cfg    map[string]string
		raw    json.RawMessage
		mutate func(*postgres.PublicationAddState)
		srcErr error
		conErr error
		want   error
		text   string
	}{
		{name: "wrong publication", raw: addPubParams("pg-orders", "other_pub", "public.refunds"), text: "wrong publication"},
		{name: "not a postgres source", raw: addPubParams("jdbc-sink", "dbz_publication", "public.refunds"), text: "not a Debezium Postgres source"},
		{name: "unknown connector", raw: addPubParams("nope", "dbz_publication", "public.refunds"), want: clients.ErrNotFound},
		{name: "connect down", conErr: errors.New("connection refused"), text: "connection refused"},
		{name: "missing publication", srcErr: fmt.Errorf("postgres: publication: %w", clients.ErrNotFound), want: clients.ErrNotFound},
		{name: "missing table", mutate: func(s *postgres.PublicationAddState) { *s = postgres.PublicationAddState{EstimatedRows: -1} }, want: clients.ErrNotFound, text: "does not exist"},
		{name: "already published", mutate: func(s *postgres.PublicationAddState) { s.InPublication = true }, want: clients.ErrAlreadyPublished},
		{name: "for all tables", mutate: func(s *postgres.PublicationAddState) { s.AllTables = true }, want: clients.ErrAlreadyPublished},
		{name: "no primary key", mutate: func(s *postgres.PublicationAddState) { s.HasPrimaryKey = false }, text: "no primary key"},
		{name: "replica identity nothing", mutate: func(s *postgres.PublicationAddState) { s.ReplicaIdentity = "n" }, text: "NOTHING"},
		{name: "partitioned", mutate: func(s *postgres.PublicationAddState) { s.RelKind = "p" }, text: "partitioned"},
		{name: "view", mutate: func(s *postgres.PublicationAddState) { s.RelKind = "v" }, text: "not an ordinary table"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAddPubFixture(tc.cfg)
			f.conn.err = tc.conErr
			f.src.err = tc.srcErr
			if tc.mutate != nil {
				tc.mutate(&f.src.st)
			}
			raw := tc.raw
			if raw == nil {
				raw = addPubGood
			}
			err := f.a.Validate(context.Background(), raw)
			if err == nil {
				t.Fatal("expected a refusal")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err %v does not wrap %v", err, tc.want)
			}
			if !strings.Contains(err.Error(), tc.text) || !strings.HasPrefix(err.Error(), "pgactions: pg_add_publication_table: ") {
				t.Fatalf("err %q lacks %q", err, tc.text)
			}
		})
	}
}

func TestAddPublicationTableValidateAcceptsFullAndIndexIdentity(t *testing.T) {
	for _, ri := range []string{"f", "i"} {
		f := newAddPubFixture(nil)
		f.src.st.HasPrimaryKey, f.src.st.ReplicaIdentity = false, ri
		if err := f.a.Validate(context.Background(), addPubGood); err != nil {
			t.Fatalf("replica identity %s: %v", ri, err)
		}
	}
}

func TestAddPublicationTableNoReaderRefuses(t *testing.T) {
	a := NewAddPublicationTable(AddPublicationTableDeps{Connect: &addPubConnect{}})
	if err := a.Validate(context.Background(), addPubGood); err == nil || !strings.Contains(err.Error(), "cannot read publications") {
		t.Fatalf("err = %v", err)
	}
}

func TestAddPublicationTableCheckScope(t *testing.T) {
	a := newAddPubFixture(nil).a.(actions.Scoped)
	pl := addPubPipeline()
	if err := a.CheckScope(pl, addPubGood); err != nil {
		t.Fatal(err)
	}
	batch := pl
	batch.Kind = "batch"
	for name, tc := range map[string]struct {
		pl  owners.Pipeline
		raw json.RawMessage
	}{
		"table outside":     {pl, addPubParams("pg-orders", "dbz_publication", "public.payroll")},
		"connector outside": {pl, addPubParams("pg-billing", "dbz_publication", "public.refunds")},
		"batch pipeline":    {batch, addPubGood},
	} {
		if err := a.CheckScope(tc.pl, tc.raw); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("%s: err = %v, want ErrOutOfScope", name, err)
		}
	}
}

func TestAddPublicationTableExecute(t *testing.T) {
	f := newAddPubFixture(nil)
	out, err := f.a.Execute(addPubScopedCtx(), addPubGood)
	if err != nil {
		t.Fatal(err)
	}
	if f.w.calls != 1 || f.w.pub != "dbz_publication" || len(f.w.tabs) != 1 || f.w.tabs[0] != "public.refunds" {
		t.Fatalf("writer got %d calls: %s %v", f.w.calls, f.w.pub, f.w.tabs)
	}
	for _, want := range []string{"added public.refunds to publication dbz_publication", "connect_snapshot_table", `DROP TABLE "public"."refunds"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("result %q lacks %q", out, want)
		}
	}
	if f.src.calls != 0 || f.conn.calls != 0 {
		t.Fatal("Execute must only write (the executor runs Validate first)")
	}
}

func TestAddPublicationTableExecuteAlreadyPublishedIsNoOp(t *testing.T) {
	f := newAddPubFixture(nil)
	f.w.err = fmt.Errorf("postgres: add public.refunds: %w: public.refunds", clients.ErrAlreadyPublished)
	out, err := f.a.Execute(addPubScopedCtx(), addPubGood)
	if err != nil {
		t.Fatalf("already published must be a no-op, got %v", err)
	}
	if !strings.Contains(out, "already in publication dbz_publication; no change made") {
		t.Fatalf("result %q", out)
	}
}

func TestAddPublicationTableExecuteRefusals(t *testing.T) {
	other := addPubPipeline()
	other.SourceTables = []string{"public.orders"}
	cases := []struct {
		name   string
		ctx    context.Context
		writer bool
		wErr   error
		want   error
		text   string
		writes int
	}{
		{name: "no scope", ctx: context.Background(), writer: true, want: tools.ErrNoScope},
		{name: "pipeline id only", ctx: tools.WithPipelineID(context.Background(), "orders"), writer: true, want: tools.ErrNoScope},
		{name: "out of scope", ctx: tools.WithPipeline(context.Background(), other), writer: true, want: tools.ErrOutOfScope},
		{name: "no writer", ctx: addPubScopedCtx(), text: "no write credentials"},
		{name: "write fails", ctx: addPubScopedCtx(), writer: true, wErr: errors.New("permission denied"), text: "permission denied", writes: 1},
		{name: "table gone", ctx: addPubScopedCtx(), writer: true, wErr: fmt.Errorf("x: %w", clients.ErrNotFound), want: clients.ErrNotFound, writes: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAddPubFixture(nil)
			f.w.err = tc.wErr
			deps := AddPublicationTableDeps{Connect: f.conn, Source: f.src}
			if tc.writer {
				deps.Writer = f.w
			}
			_, err := NewAddPublicationTable(deps).Execute(tc.ctx, addPubGood)
			if err == nil {
				t.Fatal("expected an error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err %v does not wrap %v", err, tc.want)
			}
			if !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("err %q lacks %q", err, tc.text)
			}
			if f.w.calls != tc.writes {
				t.Fatalf("writer calls = %d, want %d", f.w.calls, tc.writes)
			}
		})
	}
}

func TestAddPublicationTableExecuteUnscopedOptOut(t *testing.T) {
	f := newAddPubFixture(nil)
	if _, err := f.a.Execute(tools.WithUnscoped(context.Background()), addPubGood); err != nil {
		t.Fatal(err)
	}
}

func TestAddPublicationTablePreview(t *testing.T) {
	f := newAddPubFixture(nil)
	f.a = NewAddPublicationTable(AddPublicationTableDeps{Connect: f.conn, Source: f.src}) // no writer: Preview never needs one
	res, err := f.a.(actions.Preview).Preview(addPubScopedCtx(), addPubGood)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(f.src.visible, ","); got != "public.orders,public.customers,public.refunds" {
		t.Fatalf("visible = %s", got)
	}
	for _, want := range []string{"publication dbz_publication has 3 tables: public.customers, public.orders", "+1 not of this pipeline, not shown", "public.refunds is not in it (primary key, REPLICA IDENTITY DEFAULT)"} {
		if !strings.Contains(res.Current, want) {
			t.Fatalf("Current %q lacks %q", res.Current, want)
		}
	}
	for _, want := range []string{`ALTER PUBLICATION "dbz_publication" ADD TABLE "public"."refunds"`, "4 tables after", "Debezium picks the table up on the next change; use connect_snapshot_table for history", "~1200 existing rows"} {
		if !strings.Contains(res.Expected, want) {
			t.Fatalf("Expected %q lacks %q", res.Expected, want)
		}
	}
	if !res.Reversible || res.Rollback != `ALTER PUBLICATION "dbz_publication" DROP TABLE "public"."refunds" (by a DBA; no registered action). Change events already sent stay in Kafka.` {
		t.Fatalf("rollback = %v %q", res.Reversible, res.Rollback)
	}
	if len(res.Downstream) != 1 || !strings.Contains(res.Downstream[0], "pg.public.refunds") {
		t.Fatalf("downstream = %v", res.Downstream)
	}
	if f.w.calls != 0 {
		t.Fatal("Preview must never write")
	}
}

func TestAddPublicationTablePreviewDebeziumCaveats(t *testing.T) {
	cfg := map[string]string{
		"connector.class":             "io.debezium.connector.postgresql.PostgresConnector",
		"publication.name":            "dbz_publication",
		"table.include.list":          "public.orders,public.customers",
		"publication.autocreate.mode": "filtered",
	}
	f := newAddPubFixture(cfg)
	res, err := f.a.(actions.Preview).Preview(addPubScopedCtx(), addPubGood)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"table.include.list does not list public.refunds", "connect_update_connector_config", "publication.autocreate.mode is filtered"} {
		if !strings.Contains(res.Expected, want) {
			t.Fatalf("Expected %q lacks %q", res.Expected, want)
		}
	}
	cfg["table.include.list"] = "public\\..*"
	delete(cfg, "publication.autocreate.mode")
	res, _ = f.a.(actions.Preview).Preview(addPubScopedCtx(), addPubGood)
	if strings.Contains(res.Expected, "Follow-up") {
		t.Fatalf("a matching include list needs no follow-up: %q", res.Expected)
	}
	cfg["table.exclude.list"] = "public.refunds"
	res, _ = f.a.(actions.Preview).Preview(addPubScopedCtx(), addPubGood)
	if !strings.Contains(res.Expected, "table.exclude.list matches public.refunds") {
		t.Fatalf("Expected %q", res.Expected)
	}
}

func TestAddPublicationTablePreviewAlreadyPublished(t *testing.T) {
	f := newAddPubFixture(nil)
	f.src.st.InPublication = true
	res, err := f.a.(actions.Preview).Preview(addPubScopedCtx(), addPubGood)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(res.Expected, "no change: this step is refused") || !strings.Contains(res.Expected, "already in publication") || res.Reversible {
		t.Fatalf("preview = %+v", res)
	}
}

func TestAddPublicationTablePreviewScope(t *testing.T) {
	other := addPubPipeline()
	other.SourceTables = []string{"public.orders"}
	for name, tc := range map[string]struct {
		ctx  context.Context
		want error
	}{
		"no scope":     {context.Background(), tools.ErrNoScope},
		"out of scope": {tools.WithPipeline(context.Background(), other), tools.ErrOutOfScope},
	} {
		f := newAddPubFixture(nil)
		if _, err := f.a.(actions.Preview).Preview(tc.ctx, addPubGood); !errors.Is(err, tc.want) {
			t.Fatalf("%s: err = %v, want %v", name, err, tc.want)
		}
		if f.src.calls+f.conn.calls != 0 {
			t.Fatalf("%s: refused preview did I/O", name)
		}
	}
}

func TestAddPublicationTablePreviewWrongPublication(t *testing.T) {
	f := newAddPubFixture(nil)
	if _, err := f.a.(actions.Preview).Preview(addPubScopedCtx(), addPubParams("pg-orders", "other_pub", "public.refunds")); err == nil || !strings.Contains(err.Error(), "wrong publication") {
		t.Fatalf("err = %v", err)
	}
	if f.src.calls != 0 {
		t.Fatal("no publication read for a wrong publication")
	}
}

func TestAddPublicationTablePreviewManyMembersCapped(t *testing.T) {
	f := newAddPubFixture(nil)
	f.src.st.Members = nil
	for i := 0; i < 25; i++ {
		f.src.st.Members = append(f.src.st.Members, fmt.Sprintf("public.t%02d", i))
	}
	res, err := f.a.(actions.Preview).Preview(addPubScopedCtx(), addPubGood)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Current, "and 5 more of this pipeline") || strings.Contains(res.Current, "public.t24") {
		t.Fatalf("Current %q", res.Current)
	}
}
