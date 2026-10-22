package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// scopeGapTool builds pg_connector_scope_gap with a fake source whose publication
// holds the given members.
func scopeGapTool(t *testing.T, members []postgres.PublicationTable, cfg map[string]string) tools.Tool {
	t.Helper()
	d := testDeps()
	d.Src = &fakeSource{pubTables: members}
	d.Connect = &fakeConnect{configs: map[string]map[string]string{"pg-orders-connector": cfg}}
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tt, ok := reg.Get("pg_connector_scope_gap")
	if !ok {
		t.Fatal("pg_connector_scope_gap not registered")
	}
	return tt
}

func scopeGapConfig(include string) map[string]string {
	return map[string]string{
		"connector.class":        "io.debezium.connector.postgresql.PostgresConnector",
		"database.user":          "piperoom",
		"database.server.name":   "pg",
		"table.include.list":     include,
		"signal.data.collection": "public.devctl_signal",
	}
}

func dataMap(t *testing.T, res tools.Result) map[string]any {
	t.Helper()
	data, ok := res.Data.(map[string]any)
	if !ok {
		t.Fatalf("data type %T, want map[string]any", res.Data)
	}
	return data
}

func rowsByTable(t *testing.T, data map[string]any) map[string]scopeTable {
	t.Helper()
	rows, ok := data["tables"].([]scopeTable)
	if !ok {
		t.Fatalf("tables type %T, want []scopeTable", data["tables"])
	}
	out := map[string]scopeTable{}
	for _, r := range rows {
		out[r.Table] = r
	}
	return out
}

// TC1: the connector captures a table owners.yaml does not declare, and it is
// missing from the publication. The tool must name it as outside declared
// scope and report it as not published.
func TestConnectorScopeGapListsOutsideDeclaredTable(t *testing.T) {
	members := []postgres.PublicationTable{
		{Schema: "public", Table: "orders", InPublication: true, Selectable: true},
		{Schema: "public", Table: "devctl_signal", InPublication: true, Selectable: true},
	}
	tt := scopeGapTool(t, members, scopeGapConfig("public.orders,public.devctl_refunds,public.devctl_signal"))
	res, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("pg_connector_scope_gap: %v", err)
	}

	rows := rowsByTable(t, dataMap(t, res))
	refunds, ok := rows["public.devctl_refunds"]
	if !ok {
		t.Fatalf("devctl_refunds missing from rows: %+v", rows)
	}
	if refunds.Declared || !refunds.InConnector || refunds.InPublication {
		t.Fatalf("devctl_refunds = %+v, want declared=no in_connector=yes in_publication=no", refunds)
	}
	if !strings.Contains(res.Summary, "public.devctl_refunds: declared=no, in_connector=yes, in_publication=no") {
		t.Fatalf("summary missing the gap line: %q", res.Summary)
	}
	if !strings.Contains(res.Summary, "outside declared scope") {
		t.Fatalf("summary must flag the outside-scope table: %q", res.Summary)
	}
	if !strings.Contains(res.Summary, "add it to source_tables") {
		t.Fatalf("summary must tell the agent who can declare it: %q", res.Summary)
	}

	data := dataMap(t, res)
	gaps := data["outside_declared_scope"].([]string)
	if len(gaps) != 1 || gaps[0] != "public.devctl_refunds" {
		t.Fatalf("outside_declared_scope = %v, want [public.devctl_refunds]", gaps)
	}
	if listed := data["declared_not_in_publication"].([]string); len(listed) != 0 {
		t.Fatalf("declared_not_in_publication = %v, want none", listed)
	}
}

// TC2: a table that is in neither the connector nor owners.yaml is never
// disclosed. The tool's table set is the union of the two, nothing else.
func TestConnectorScopeGapNeverWidensScope(t *testing.T) {
	members := []postgres.PublicationTable{
		{Schema: "public", Table: "orders", InPublication: true, Selectable: true},
		{Schema: "public", Table: "secret_other_team", InPublication: true, Selectable: true},
	}
	tt := scopeGapTool(t, members, scopeGapConfig("public.orders,public.devctl_refunds"))
	res, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("pg_connector_scope_gap: %v", err)
	}
	if strings.Contains(res.Summary, "secret_other_team") {
		t.Fatalf("summary disclosed an unrelated table: %q", res.Summary)
	}
	if _, ok := rowsByTable(t, dataMap(t, res))["public.secret_other_team"]; ok {
		t.Fatal("data disclosed an unrelated table")
	}
}

// TC3: scoped to the room's pipeline, an input naming another pipeline is
// refused before any read.
func TestConnectorScopeGapRefusesAnotherPipelineWhenScoped(t *testing.T) {
	tt := scopeGapTool(t, nil, scopeGapConfig("public.orders"))
	p := owners.Pipeline{ID: "orders", SourceTables: []string{"public.orders"}, Connectors: []string{"pg-orders-connector"}}
	_, err := tt.Run(tools.WithPipeline(context.Background(), p), json.RawMessage(`{"pipeline_id":"payments"}`))
	if err == nil {
		t.Fatal("expected an error for another pipeline")
	}
	if !errors.Is(err, tools.ErrOutOfScope) && !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("error %v must refuse the pipeline", err)
	}
}

// An exclude-list entry removes a table from the connector's scope, so it is
// not reported as captured.
func TestConnectorScopeGapHonoursExcludeList(t *testing.T) {
	cfg := scopeGapConfig("public.orders,public.devctl_refunds")
	cfg["table.exclude.list"] = "public.devctl_refunds"
	tt := scopeGapTool(t, nil, cfg)
	res, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("pg_connector_scope_gap: %v", err)
	}
	if strings.Contains(res.Summary, "public.devctl_refunds") {
		t.Fatalf("excluded table was reported: %q", res.Summary)
	}
}

// No include list means Debezium captures every table: the tool says the
// scope cannot be compared instead of inventing gaps.
func TestConnectorScopeGapUnboundedIncludeList(t *testing.T) {
	cfg := scopeGapConfig("")
	tt := scopeGapTool(t, nil, cfg)
	res, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("pg_connector_scope_gap: %v", err)
	}
	if !strings.Contains(res.Summary, "table.include.list is not set") {
		t.Fatalf("summary must explain the unbounded filter: %q", res.Summary)
	}
	if !dataMap(t, res)["include_unbounded"].(bool) {
		t.Fatal("include_unbounded = false, want true")
	}
}

// A regex include entry is reported as a pattern; it cannot be expanded into
// table names.
func TestConnectorScopeGapReportsPatterns(t *testing.T) {
	tt := scopeGapTool(t, nil, scopeGapConfig(`public\.devctl_.*`))
	res, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("pg_connector_scope_gap: %v", err)
	}
	if !strings.Contains(res.Summary, "patterns that cannot be expanded") {
		t.Fatalf("summary must report the pattern: %q", res.Summary)
	}
	patterns := dataMap(t, res)["include_patterns"].([]string)
	if len(patterns) != 1 || patterns[0] != `public\.devctl_.*` {
		t.Fatalf("include_patterns = %v", patterns)
	}
}

// TC8: the tool is read-only.
func TestConnectorScopeGapIsReadOnly(t *testing.T) {
	tt := scopeGapTool(t, nil, scopeGapConfig("public.orders"))
	if tt.Kind() != tools.KindRead {
		t.Fatalf("kind = %v, want read", tt.Kind())
	}
}
