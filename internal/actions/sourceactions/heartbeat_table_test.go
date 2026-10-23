package sourceactions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/actions/pgactions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

const hbTable = "public.debezium_heartbeat"

func hbParams(table string) json.RawMessage {
	b, _ := json.Marshal(map[string]string{"connector": testConnector, "table": table})
	return b
}

func hbParamsConn(connector, table string) json.RawMessage {
	b, _ := json.Marshal(map[string]string{"connector": connector, "table": table})
	return b
}

var hbGood = hbParams(hbTable)

func TestHeartbeatTableMeta(t *testing.T) {
	_, _, _, a := newHeartbeatFixture(nil, postgres.PublicationAddState{})
	if a.Name() != "source_create_heartbeat_table" || a.Layer() != "postgres" || a.Describe() == "" {
		t.Fatalf("meta: %s %s", a.Name(), a.Layer())
	}
	schema := a.ParamsSchema()
	if schema["type"] != "object" || len(schema["required"].([]string)) != 2 {
		t.Fatalf("schema = %v", schema)
	}
}

func TestHeartbeatTableCheckScope(t *testing.T) {
	pl := testPipeline()
	cases := map[string]json.RawMessage{
		"foreign connector": hbParamsConn("pg-other", hbTable),
		"source table":      hbParams("public.orders"),
		"foreign schema":    hbParams("other.heartbeat"),
	}
	_, _, _, a := newHeartbeatFixture(nil, postgres.PublicationAddState{})
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if err := a.CheckScope(pl, raw); !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("CheckScope = %v", err)
			}
		})
	}
	foreign := owners.Pipeline{ID: "other", Kind: "cdc", Connectors: []string{"pg-other"}, SourceTables: []string{"public.other"}}
	if err := a.CheckScope(foreign, hbGood); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("foreign pipeline = %v", err)
	}
	batch := owners.Pipeline{ID: "warehouse", Kind: "batch"}
	if err := a.CheckScope(batch, hbGood); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("batch = %v", err)
	}
	if err := a.CheckScope(pl, hbGood); err != nil {
		t.Fatalf("good = %v", err)
	}
}

func TestHeartbeatTableValidate(t *testing.T) {
	c, s, w, a := newHeartbeatFixture(nil, postgres.PublicationAddState{})
	if err := a.Validate(scopedCtx(), hbGood); err != nil {
		t.Fatal(err)
	}
	if c.calls != 1 || s.pubCalls != 1 || !s.deadline {
		t.Fatalf("reads: connect %d, publication %d, deadline %v", c.calls, s.pubCalls, s.deadline)
	}
	if s.pub != "dbz_publication" || s.table != hbTable {
		t.Fatalf("read %q %q", s.pub, s.table)
	}
	if len(w.calls) != 0 {
		t.Fatalf("Validate wrote: %v", w.calls)
	}
}

func TestHeartbeatTableRefusesWithoutWriter(t *testing.T) {
	_, _, _, a := newHeartbeatFixture(nil, postgres.PublicationAddState{})
	a.d.Writer = nil
	if err := a.Validate(scopedCtx(), hbGood); err == nil || !strings.Contains(err.Error(), "SOURCE_DB_WRITE_URL") {
		t.Fatalf("Validate = %v", err)
	}
	if _, err := a.Execute(scopedCtx(), hbGood); err == nil || !strings.Contains(err.Error(), "SOURCE_DB_WRITE_URL") {
		t.Fatalf("Execute = %v", err)
	}
}

func TestHeartbeatTableValidateRefusesOutOfScopeBeforeIO(t *testing.T) {
	other := owners.Pipeline{ID: "other", Kind: "cdc", Connectors: []string{testConnector}, SourceTables: []string{"other.orders"}}
	ctx := tools.WithPipeline(context.Background(), other)
	c, s, _, a := newHeartbeatFixture(nil, postgres.PublicationAddState{})
	if err := a.Validate(ctx, hbGood); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("Validate = %v", err)
	}
	if c.calls != 0 || s.pubCalls != 0 {
		t.Fatalf("out-of-scope Validate did I/O")
	}
}

func TestHeartbeatTableValidateRefusesExistingNonTable(t *testing.T) {
	_, _, _, a := newHeartbeatFixture(nil, postgres.PublicationAddState{TableExists: true, RelKind: "v"})
	err := a.Validate(scopedCtx(), hbGood)
	if err == nil || !strings.Contains(err.Error(), "not an ordinary table") {
		t.Fatalf("view = %v", err)
	}
}

func TestHeartbeatTableValidateToleratesMissingPublication(t *testing.T) {
	_, s, _, a := newHeartbeatFixture(nil, postgres.PublicationAddState{})
	s.stErr = clients.ErrNotFound
	if err := a.Validate(scopedCtx(), hbGood); err != nil {
		t.Fatalf("missing publication = %v", err)
	}
}

func TestHeartbeatTableValidateBadParamsWithoutIO(t *testing.T) {
	cases := map[string]json.RawMessage{
		"unknown field": json.RawMessage(`{"connector":"pg-orders","table":"public.hb","x":1}`),
		"no connector":  json.RawMessage(`{"table":"public.hb"}`),
		"bad table":     hbParams("hb"),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			c, s, _, a := newHeartbeatFixture(nil, postgres.PublicationAddState{})
			if err := a.Validate(scopedCtx(), raw); err == nil {
				t.Fatal("no error")
			}
			if c.calls != 0 || s.pubCalls != 0 {
				t.Fatalf("bad params did I/O")
			}
		})
	}
}

func TestHeartbeatTablePreviewFiltered(t *testing.T) {
	_, _, w, a := newHeartbeatFixture(nil, postgres.PublicationAddState{})
	res, err := a.Preview(scopedCtx(), hbGood)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Current, "does not exist") || !strings.Contains(res.Current, "dbz_publication") {
		t.Fatalf("current = %q", res.Current)
	}
	if !strings.Contains(res.Expected, "CREATE TABLE IF NOT EXISTS") || !strings.Contains(res.Expected, "no lock on the captured tables") {
		t.Fatalf("expected = %q", res.Expected)
	}
	if !res.Reversible || !strings.Contains(res.Rollback, `DROP TABLE "public"."debezium_heartbeat"`) {
		t.Fatalf("rollback = %q", res.Rollback)
	}
	down := strings.Join(res.Downstream, "\n")
	for _, want := range []string{
		"same proposal",
		"connect_update_connector_config",
		`"heartbeat.action.query"`,
		`INSERT INTO \"public\".\"debezium_heartbeat\"`,
		"heartbeat.interval.ms",
		pgactions.AddPublicationTableName,
		"filtered",
	} {
		if !strings.Contains(down, want) {
			t.Fatalf("downstream lacks %q:\n%s", want, down)
		}
	}
	if len(w.calls) != 0 {
		t.Fatalf("Preview wrote: %v", w.calls)
	}
}

func TestHeartbeatTablePreviewNotFilteredHasNoPublicationStep(t *testing.T) {
	cfg := pgConnectorConfig(map[string]string{"publication.autocreate.mode": "all_tables"})
	_, _, _, a := newHeartbeatFixture(cfg, postgres.PublicationAddState{AllTables: true})
	res, err := a.Preview(scopedCtx(), hbGood)
	if err != nil {
		t.Fatal(err)
	}
	down := strings.Join(res.Downstream, "\n")
	if strings.Contains(down, pgactions.AddPublicationTableName) {
		t.Fatalf("unfiltered publication needs no add step:\n%s", down)
	}
	if !strings.Contains(down, "connect_update_connector_config") {
		t.Fatalf("the query step is always needed:\n%s", down)
	}
}

func TestHeartbeatTablePreviewRequiresScope(t *testing.T) {
	c, s, _, a := newHeartbeatFixture(nil, postgres.PublicationAddState{})
	if _, err := a.Preview(context.Background(), hbGood); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("Preview = %v", err)
	}
	if c.calls != 0 || s.pubCalls != 0 {
		t.Fatalf("unscoped Preview did I/O")
	}
}

func TestHeartbeatFollowUps(t *testing.T) {
	cfg := pgConnectorConfig(nil)
	fus := HeartbeatFollowUps(cfg, testConnector, "dbz_publication", hbTable)
	if len(fus) != 2 {
		t.Fatalf("follow-ups = %v", fus)
	}
	if fus[0].Action != updateConnectorConfigAction || fus[0].Layer != "kafka-connect" {
		t.Fatalf("config step = %+v", fus[0])
	}
	var cp struct {
		Connector string            `json:"connector"`
		Changes   map[string]string `json:"changes"`
	}
	if err := json.Unmarshal(fus[0].Params, &cp); err != nil {
		t.Fatal(err)
	}
	q := cp.Changes["heartbeat.action.query"]
	if cp.Connector != testConnector || q != `INSERT INTO "public"."debezium_heartbeat" (id, ts) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET ts = now()` {
		t.Fatalf("config step params = %+v", cp)
	}
	if fus[1].Action != pgactions.AddPublicationTableName || fus[1].Layer != "postgres" {
		t.Fatalf("publication step = %+v", fus[1])
	}
	var pp struct{ Connector, Publication, Table string }
	if err := json.Unmarshal(fus[1].Params, &pp); err != nil {
		t.Fatal(err)
	}
	if pp.Connector != testConnector || pp.Publication != "dbz_publication" || pp.Table != hbTable {
		t.Fatalf("publication step params = %+v", pp)
	}

	cfg["publication.autocreate.mode"] = "all_tables"
	if got := HeartbeatFollowUps(cfg, testConnector, "dbz_publication", hbTable); len(got) != 1 {
		t.Fatalf("unfiltered follow-ups = %v", got)
	}
	if heartbeatQuery("public.x; drop") != "" {
		t.Fatal("an invalid identifier reached the heartbeat query")
	}
}

func TestHeartbeatTableExecute(t *testing.T) {
	_, _, w, a := newHeartbeatFixture(nil, postgres.PublicationAddState{})
	out, err := a.Execute(scopedCtx(), hbGood)
	if err != nil {
		t.Fatal(err)
	}
	if len(w.calls) != 1 || w.calls[0] != "CreateHeartbeatTable" || w.heartbeat != hbTable {
		t.Fatalf("writer = %v %q", w.calls, w.heartbeat)
	}
	for _, want := range []string{"created heartbeat table public.debezium_heartbeat", pgactions.AddPublicationTableName, `DROP TABLE "public"."debezium_heartbeat"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("result %q lacks %q", out, want)
		}
	}
}

func TestHeartbeatTableExecuteOutOfScope(t *testing.T) {
	other := owners.Pipeline{ID: "other", Kind: "cdc", Connectors: []string{testConnector}, SourceTables: []string{"other.orders"}}
	ctx := tools.WithPipeline(context.Background(), other)
	_, _, w, a := newHeartbeatFixture(nil, postgres.PublicationAddState{})
	if _, err := a.Execute(ctx, hbGood); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("Execute = %v", err)
	}
	if len(w.calls) != 0 {
		t.Fatalf("out-of-scope Execute wrote: %v", w.calls)
	}
}

func TestHeartbeatTableWriterError(t *testing.T) {
	_, _, w, a := newHeartbeatFixture(nil, postgres.PublicationAddState{})
	w.err = errors.New("boom")
	if _, err := a.Execute(scopedCtx(), hbGood); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Execute = %v", err)
	}
}
