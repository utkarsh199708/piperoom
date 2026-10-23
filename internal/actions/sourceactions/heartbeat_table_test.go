package sourceactions

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
)

var hbParams = json.RawMessage(`{"connector":"pg-orders","table":"public.debezium_heartbeat"}`)

func hbFixture() (*fakeConnect, *fakeSource, *fakeWriter, *createHeartbeatTable) {
	conn, src, w := newConnect(), &fakeSource{pub: postgres.PublicationAddState{Members: []string{"public.orders"}, OtherMembers: 2}}, &fakeWriter{}
	return conn, src, w, &createHeartbeatTable{d: HeartbeatDeps{Connect: conn, Source: src, Writer: w}}
}

func TestHeartbeatCheckScope(t *testing.T) {
	_, _, _, a := hbFixture()
	pl := testPipeline()
	tests := []struct {
		name, raw, want string
		outOfScope      bool
	}{
		{name: "ok", raw: string(hbParams)},
		{name: "other schema", raw: `{"connector":"pg-orders","table":"audit.heartbeat"}`, outOfScope: true},
		{name: "a source table", raw: `{"connector":"pg-orders","table":"public.orders"}`, want: "not a heartbeat table"},
		{name: "other connector", raw: `{"connector":"pg-billing","table":"public.hb"}`, outOfScope: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := a.CheckScope(pl, json.RawMessage(tt.raw))
			switch {
			case tt.outOfScope && !errors.Is(err, tools.ErrOutOfScope):
				t.Fatalf("CheckScope = %v, want ErrOutOfScope", err)
			case tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)):
				t.Fatalf("CheckScope = %v, want %q", err, tt.want)
			case !tt.outOfScope && tt.want == "" && err != nil:
				t.Fatalf("CheckScope: %v", err)
			}
		})
	}
}

func TestHeartbeatValidate(t *testing.T) {
	conn, _, _, a := hbFixture()
	if err := a.Validate(scopedCtx(), hbParams); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := a.Validate(scopedCtx(), json.RawMessage(`{"connector":"jdbc-sink","table":"public.hb"}`)); err == nil {
		t.Fatal("Validate accepted a connector outside the pipeline")
	}
	pl := testPipeline()
	pl.Connectors = append(pl.Connectors, "jdbc-sink")
	ctx := tools.WithPipeline(scopedCtx(), pl)
	if err := a.Validate(ctx, json.RawMessage(`{"connector":"jdbc-sink","table":"public.hb"}`)); err == nil || !strings.Contains(err.Error(), "not a Debezium source connector") {
		t.Fatalf("sink connector = %v", err)
	}
	conn.err = clients.ErrNotFound
	if err := a.Validate(scopedCtx(), hbParams); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Connect down = %v", err)
	}
	a.d.Connect = nil
	if err := a.Validate(scopedCtx(), hbParams); err == nil {
		t.Fatal("Validate without Connect accepted")
	}
}

func TestHeartbeatPreviewFilteredPublication(t *testing.T) {
	conn, _, w, a := hbFixture()
	conn.cfg["pg-orders"]["publication.autocreate.mode"] = "filtered"
	res, err := a.Preview(scopedCtx(), hbParams)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	for _, want := range []string{
		`CREATE TABLE IF NOT EXISTS "public"."debezium_heartbeat" (id integer PRIMARY KEY`,
		`(1) pg_add_publication_table {"connector":"pg-orders","publication":"dbz_publication","table":"public.debezium_heartbeat"}`,
		"source_tables in owners.yaml first",
		"table.include.list",
		`(2) connect_update_connector_config {"changes":{"heartbeat.action.query":"INSERT INTO \"public\".\"debezium_heartbeat\" (id, ts) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts","heartbeat.interval.ms":"10000"},"connector":"pg-orders"}`,
	} {
		if !strings.Contains(res.Expected, want) {
			t.Errorf("Expected lacks %q:\n%s", want, res.Expected)
		}
	}
	for _, want := range []string{"does not exist", "sends no heartbeats", "publication dbz_publication is filtered (3 tables) and does not have the table"} {
		if !strings.Contains(res.Current, want) {
			t.Errorf("Current lacks %q: %s", want, res.Current)
		}
	}
	if !res.Reversible || !strings.Contains(res.Rollback, `DROP TABLE "public"."debezium_heartbeat"`) {
		t.Fatalf("rollback: %v %q", res.Reversible, res.Rollback)
	}
	if len(w.calls) != 0 {
		t.Fatal("Preview wrote")
	}
}

func TestHeartbeatPreviewAllTablesAndExisting(t *testing.T) {
	conn, src, _, a := hbFixture()
	conn.cfg["pg-orders"]["heartbeat.interval.ms"] = "5000"
	conn.cfg["pg-orders"]["heartbeat.action.query"] = "INSERT INTO x\nVALUES (1)"
	src.pub = postgres.PublicationAddState{AllTables: true, TableExists: true}
	res, err := a.Preview(scopedCtx(), hbParams)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if strings.Contains(res.Expected, "pg_add_publication_table") || strings.Contains(res.Expected, "heartbeat.interval.ms") {
		t.Errorf("no publication or interval follow-up expected:\n%s", res.Expected)
	}
	if !strings.Contains(res.Expected, "(1) connect_update_connector_config") || !strings.Contains(res.Expected, "changes nothing") || res.Reversible {
		t.Errorf("existing table preview: %+v", res)
	}
	if !strings.Contains(res.Current, "FOR ALL TABLES") || !strings.Contains(res.Current, `"INSERT INTO x VALUES (1)"`) || strings.Contains(res.Current, "\n") {
		t.Errorf("Current: %q", res.Current)
	}
}

func TestHeartbeatPreviewPublicationUnread(t *testing.T) {
	_, src, _, a := hbFixture()
	src.pubErr = clients.ErrNotFound
	res, err := a.Preview(scopedCtx(), hbParams)
	if err != nil || !strings.Contains(res.Current, "does not exist yet") || !strings.Contains(res.Expected, "if the publication is filtered (it was not read)") {
		t.Fatalf("missing publication: %v %+v", err, res)
	}
	src.pubErr = errors.New("boom")
	res, err = a.Preview(scopedCtx(), hbParams)
	if err != nil || !strings.Contains(res.Current, "publication not read (boom)") {
		t.Fatalf("read error: %v %+v", err, res)
	}
}

func TestHeartbeatPreviewNonPostgresSource(t *testing.T) {
	conn, src, _, a := hbFixture()
	conn.cfg["pg-orders"]["connector.class"] = "io.debezium.connector.mysql.MySqlConnector"
	res, err := a.Preview(scopedCtx(), hbParams)
	if err != nil || strings.Contains(res.Expected, "pg_add_publication_table") || src.calls != 0 {
		t.Fatalf("mysql source: %v calls %d %+v", err, src.calls, res)
	}
}

func TestHeartbeatExecute(t *testing.T) {
	_, _, w, a := hbFixture()
	out, err := a.Execute(scopedCtx(), hbParams)
	if err != nil || !strings.Contains(out, "heartbeat.action.query") {
		t.Fatalf("Execute: %q %v", out, err)
	}
	if len(w.calls) != 1 || w.calls[0].method != "CreateHeartbeatTable" || w.calls[0].args[0] != "public.debezium_heartbeat" {
		t.Fatalf("writes: %+v", w.calls)
	}
	w.err = tools.ErrOutOfScope
	if _, err := a.Execute(scopedCtx(), hbParams); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("writer error = %v", err)
	}
	// A pipeline with no source table in the schema is refused before the writer.
	pl := testPipeline()
	pl.SourceTables = []string{"sales.orders"}
	w.calls = nil
	if _, err := a.Execute(tools.WithPipeline(scopedCtx(), pl), hbParams); !errors.Is(err, tools.ErrOutOfScope) || len(w.calls) != 0 {
		t.Fatalf("schema outside pipeline = %v, writes %d", err, len(w.calls))
	}
}
