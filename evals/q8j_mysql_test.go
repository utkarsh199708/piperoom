package evals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// Q8j: MySQL rooms in the replay. A scenario whose pipeline says
// source_kind: mysql gets the tools and actions wiring gives a MySQL client
// set (Q8g): dbtools' MySQL list, mysql_binlog_status, kafka_schema_history,
// no pg_* tool or action, and a binlog reader behind the Connect actions, so
// connect_update_connector_config offers database.server.id and
// snapshot.mode. A Postgres scenario's room does not change.

// q8jFixes are the labelled fixes of the MySQL scenarios that need a MySQL
// room's actions, for TestEveryScenarioCanPass (used once the file sets
// expect.fix).
var q8jFixes = map[string][]events.ProposalStep{
	"mysql-history-topic-deleted": {ps("connect_update_connector_config", `{"connector":"orders-mysql","changes":{"snapshot.mode":"recovery"}}`)},
	"mysql-server-id-clash":       {ps("connect_update_connector_config", `{"connector":"orders-mysql","changes":{"database.server.id":"5402"}}`)},
}

// mysqlScenarios are the four Q8i files (design §10.4).
var mysqlScenarios = []string{"mysql-binlog-purged", "mysql-history-topic-deleted", "mysql-missing-replication-grant", "mysql-server-id-clash"}

// mysqlOnly are the read tools only a MySQL room offers (agent.ToolServesSource).
var mysqlOnly = []string{"kafka_schema_history", "source_long_transactions", "source_row", "mysql_binlog_status"}

func toolNames(defs []tools.Tool) []string {
	out := make([]string, 0, len(defs))
	for _, d := range defs {
		out = append(out, d.Name())
	}
	return out
}

// toolSig is each tool's name, description and input schema, in order: what
// the model is shown.
func toolSig(t *testing.T, defs []tools.Tool) []string {
	t.Helper()
	out := make([]string, 0, len(defs))
	for _, d := range defs {
		b, err := json.Marshal(d.InputSchema())
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, d.Name()+"\n"+d.Description()+"\n"+string(b))
	}
	return out
}

func registryOf(t *testing.T, defs []tools.Tool) *tools.Registry {
	t.Helper()
	reg := tools.NewRegistry()
	for _, d := range defs {
		if err := reg.Register(d); err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

// The Postgres list is LiveTools' and holds no MySQL-only tool; "" reads as
// postgres; an unknown kind is refused.
func TestLiveToolsForPostgresIsLiveTools(t *testing.T) {
	pg, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"", owners.SourceKindPostgres} {
		got, err := LiveToolsFor(k)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(toolSig(t, got), toolSig(t, pg)) {
			t.Errorf("LiveToolsFor(%q) differs from LiveTools()", k)
		}
	}
	for _, n := range mysqlOnly {
		if slices.Contains(toolNames(pg), n) {
			t.Errorf("Postgres room offers %s", n)
		}
	}
	if k, err := agent.RoomSourceKind(registryOf(t, pg)); err != nil || k != owners.SourceKindPostgres {
		t.Errorf("RoomSourceKind = %q, %v; want postgres", k, err)
	}
	if _, err := LiveToolsFor("oracle"); err == nil {
		t.Error("LiveToolsFor(oracle): want an error")
	}
}

// A MySQL room's list is the Postgres list without its pg_* tools plus the
// four MySQL-only tools, and the agent reads it as a MySQL room.
func TestLiveToolsForMySQL(t *testing.T) {
	pg, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	my, err := LiveToolsFor(owners.SourceKindMySQL)
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, n := range toolNames(pg) {
		if !strings.HasPrefix(n, "pg_") {
			want = append(want, n)
		}
	}
	want = append(want, mysqlOnly...)
	got := toolNames(my)
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("MySQL tools = %v\nwant %v", got, want)
	}
	if k, err := agent.RoomSourceKind(registryOf(t, my)); err != nil || k != owners.SourceKindMySQL {
		t.Errorf("RoomSourceKind = %q, %v; want mysql", k, err)
	}
}

// mysqlRecs is a MySQL connector's config and the source's binlog status
// (server_id 223344), as the scenarios record them.
var mysqlRecs = []Recording{
	{Tool: "connect_config", Input: map[string]any{"connector": "orders-mysql"}, Data: map[string]any{
		"connector.class": "io.debezium.connector.mysql.MySqlConnector", "database.server.id": "5401",
	}},
	{Tool: "mysql_binlog_status", Input: map[string]any{"connector": "orders-mysql"}, Data: map[string]any{
		"server": map[string]any{"file": "binlog.000007", "pos": 15530, "gtid_mode": "ON", "server_id": 223344,
			"binlogs": []any{map[string]any{"name": "binlog.000006", "size": 1}}},
	}},
}

// A MySQL room has no pg_* action, and connect_update_connector_config
// offers the two MySQL keys there only.
func TestLiveActionsMySQLRoom(t *testing.T) {
	pg, err := LiveActions(Resources{Connectors: []string{"orders-mysql"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	my, err := LiveActions(Resources{Connectors: []string{"orders-mysql"}, SourceKind: owners.SourceKindMySQL}, mysqlRecs)
	if err != nil {
		t.Fatal(err)
	}
	var pgNames, myNames []string
	for _, a := range pg.All() {
		pgNames = append(pgNames, a.Name())
	}
	for _, a := range my.All() {
		myNames = append(myNames, a.Name())
		if strings.HasPrefix(a.Name(), "pg_") {
			t.Errorf("MySQL room offers %s", a.Name())
		}
	}
	var want []string
	for _, n := range pgNames {
		if !strings.HasPrefix(n, "pg_") {
			want = append(want, n)
		}
	}
	if !slices.Equal(myNames, want) {
		t.Errorf("MySQL actions = %v\nwant %v", myNames, want)
	}
	changeKeys := func(t *testing.T, a interface{ ParamsSchema() map[string]any }) map[string]any {
		t.Helper()
		props, _ := a.ParamsSchema()["properties"].(map[string]any)
		ch, _ := props["changes"].(map[string]any)
		cp, _ := ch["properties"].(map[string]any)
		return cp
	}
	pu, _ := pg.Get("connect_update_connector_config")
	mu, _ := my.Get("connect_update_connector_config")
	for _, k := range []string{"database.server.id", "snapshot.mode"} {
		if _, ok := changeKeys(t, pu)[k]; ok {
			t.Errorf("Postgres room offers %s", k)
		}
		if _, ok := changeKeys(t, mu)[k]; !ok {
			t.Errorf("MySQL room does not offer %s", k)
		}
	}
}

// The MySQL keys validate against the recorded config and binlog status: a
// recovery snapshot passes, an id equal to the server's own is refused, a
// new id passes, and without a binlog recording the id cannot be checked.
func TestMySQLConfigKeysValidateOnReplay(t *testing.T) {
	sc := Scenario{Pipeline: owners.Pipeline{ID: "orders-mysql", SourceKind: owners.SourceKindMySQL, Connectors: []string{"orders-mysql"}},
		Resources: Resources{Connectors: []string{"orders-mysql"}}}
	ctx := tools.WithPipeline(context.Background(), sc.Pipeline)
	validate := func(recs []Recording, params string) error {
		reg, err := LiveActions(sc.resources(), recs)
		if err != nil {
			t.Fatal(err)
		}
		a, ok := reg.Get("connect_update_connector_config")
		if !ok {
			t.Fatal("no connect_update_connector_config")
		}
		return a.Validate(ctx, json.RawMessage(params))
	}
	for _, c := range []struct {
		name, params string
		recs         []Recording
		wantErr      string
	}{
		{"recovery", `{"connector":"orders-mysql","changes":{"snapshot.mode":"recovery"}}`, mysqlRecs, ""},
		{"new id", `{"connector":"orders-mysql","changes":{"database.server.id":"5402"}}`, mysqlRecs, ""},
		{"server's own id", `{"connector":"orders-mysql","changes":{"database.server.id":"223344"}}`, mysqlRecs, "own @@server_id"},
		{"no binlog recording", `{"connector":"orders-mysql","changes":{"database.server.id":"5402"}}`, mysqlRecs[:1], "not available in replay"},
		{"no config recording", `{"connector":"orders-mysql","changes":{"snapshot.mode":"recovery"}}`, nil, "not available in replay"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := validate(c.recs, c.params)
			switch {
			case c.wantErr == "" && err != nil:
				t.Fatalf("Validate: %v", err)
			case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
				t.Fatalf("Validate = %v, want an error with %q", err, c.wantErr)
			}
		})
	}
}

func TestReplayBinlogReadsTheRecordedServer(t *testing.T) {
	st, err := replayBinlog{recs: mysqlRecs}.BinlogStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.ServerID != 223344 || st.File != "binlog.000007" || st.Pos != 15530 || len(st.Binlogs) != 1 || st.Binlogs[0].Name != "binlog.000006" {
		t.Errorf("status = %+v", st)
	}
	if _, err := (replayBinlog{}).BinlogStatus(context.Background()); !errors.Is(err, errNotReplayed) {
		t.Errorf("no recording: err = %v, want errNotReplayed", err)
	}
}

func TestLiveActionsRefusesUnknownSourceKind(t *testing.T) {
	if _, err := LiveActions(Resources{SourceKind: "oracle"}, nil); err == nil {
		t.Error("want an error")
	}
}

func TestParseScenarioCarriesSourceKind(t *testing.T) {
	for _, name := range mysqlScenarios {
		sc := loadScenario(t, "scenarios/"+name+".yaml")
		if sc.Resources.SourceKind != owners.SourceKindMySQL || sc.sourceKind() != owners.SourceKindMySQL {
			t.Errorf("%s: resources source kind %q, scenario %q", name, sc.Resources.SourceKind, sc.sourceKind())
		}
	}
	sc := loadScenario(t, "scenarios/schema-change.yaml")
	if sc.Resources.SourceKind != "" || sc.sourceKind() != owners.SourceKindPostgres || sc.resources().SourceKind != owners.SourceKindPostgres {
		t.Errorf("schema-change: %q %q", sc.Resources.SourceKind, sc.sourceKind())
	}
}

// q8jFixFor is the fix a focused run of sc proposes: its labelled fix once
// the file grades one (expect.fix), none otherwise.
func q8jFixFor(sc Scenario) []events.ProposalStep {
	if len(sc.Expect.Fix) == 0 {
		return nil
	}
	if fix, ok := q8jFixes[sc.Name]; ok {
		return fix
	}
	return q6Fixes[sc.Name]
}

// The four MySQL scenarios run in a MySQL room (the tools the scripted
// agent is handed) and score 100 on a focused run; with a Postgres scenario
// beside them the report gives the pass rate per source kind.
func TestMySQLScenariosScoreInAMySQLRoom(t *testing.T) {
	var scs []Scenario
	var results []RunResult
	for _, name := range mysqlScenarios {
		sc := loadScenario(t, "scenarios/"+name+".yaml")
		scs = append(scs, sc)
		t.Run(name, func(t *testing.T) {
			inner := scriptedRunner(canPassScript(t, sc, q8jFixFor(sc)))
			var offered []string
			runner := func(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
				for _, tl := range d.Tools.All() {
					offered = append(offered, tl.Name())
				}
				return inner(ctx, a, d, o)
			}
			rep, err := Evaluate(context.Background(), []Scenario{sc}, Config{Runner: runner, Logger: quietLogger()})
			if err != nil {
				t.Fatal(err)
			}
			r := rep.Results[0]
			results = append(results, r)
			if !r.Pass || r.Score != 100 || len(r.Misses) != 0 {
				t.Errorf("pass=%v score=%v misses=%v failures=%v facts missing=%v", r.Pass, r.Score, r.Misses, r.Failures, r.FactsMissing)
			}
			if r.FixExpected && (r.RightAction == nil || !r.RightAction.OK) {
				t.Errorf("right action = %+v", r.RightAction)
			}
			for _, n := range offered {
				if strings.HasPrefix(n, "pg_") {
					t.Errorf("MySQL room offered %s", n)
				}
			}
			for _, n := range mysqlOnly {
				if !slices.Contains(offered, n) {
					t.Errorf("MySQL room did not offer %s", n)
				}
			}
			for _, rec := range sc.Recordings {
				if !slices.Contains(offered, rec.Tool) {
					t.Errorf("recording of %s, which the room does not offer", rec.Tool)
				}
			}
		})
	}

	pg := loadScenario(t, "scenarios/schema-change.yaml")
	r, _ := evalOnce(t, pg, canPassScript(t, pg, []events.ProposalStep{
		ps("schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"NONE"}`),
		ps("connect_restart_task", `{"connector":"pg-orders-connector","task":0}`)}))
	scs, results = append(scs, pg), append(results, r)

	rep := buildReport(Config{}.withDefaults(), scs, results, time.Now())
	want := []Breakdown{
		{Kind: owners.SourceKindMySQL, Scenarios: len(mysqlScenarios), Runs: len(mysqlScenarios), Passes: len(mysqlScenarios), PassRate: 1},
		{Kind: owners.SourceKindPostgres, Scenarios: 1, Runs: 1, Passes: 1, PassRate: 1},
	}
	if !reflect.DeepEqual(rep.BySource, want) {
		t.Fatalf("by source = %+v, want %+v", rep.BySource, want)
	}
	for _, s := range rep.Scenarios {
		wantKind := owners.SourceKindMySQL
		if s.Name == pg.Name {
			wantKind = owners.SourceKindPostgres
		}
		if s.SourceKind != wantKind {
			t.Errorf("scenario %s source kind = %q, want %q", s.Name, s.SourceKind, wantKind)
		}
	}
	var md strings.Builder
	if err := rep.WriteMarkdown(&md); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(md.String(), fmt.Sprintf("by source: mysql %d/%d (100%%) · postgres 1/1 (100%%)", len(mysqlScenarios), len(mysqlScenarios))) {
		t.Errorf("markdown has no per-source-kind line:\n%s", md.String())
	}
}

// A scenario row with no source kind (a report written before Q8j) reads as
// postgres in BySource, and Restrict recomputes it.
func TestBySourceReadsEmptyAsPostgres(t *testing.T) {
	ss := []ScenarioSummary{
		{Name: "a", Summary: Summary{Runs: 1, Passes: 1, PassRate: 1, MeanScore: 100}},
		{Name: "b", SourceKind: owners.SourceKindPostgres, Summary: Summary{Runs: 1, PassRate: 0, MeanScore: 40}},
		{Name: "c", SourceKind: owners.SourceKindMySQL, Summary: Summary{Runs: 1, Passes: 1, PassRate: 1, MeanScore: 90}},
		{Name: "d", SourceKind: owners.SourceKindMySQL},
	}
	r := Report{Scenarios: ss}
	got := r.Restrict([]string{"a", "b", "c", "d"}).BySource
	want := []Breakdown{
		{Kind: "mysql", Scenarios: 1, Runs: 1, Passes: 1, PassRate: 1},
		{Kind: "postgres", Scenarios: 2, Runs: 2, Passes: 1, PassRate: 0.5},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("by source = %+v, want %+v", got, want)
	}
	if got := r.Restrict([]string{"a", "b"}).BySource; len(got) != 1 || got[0].Kind != "postgres" || got[0].Scenarios != 2 {
		t.Errorf("restricted to postgres: %+v", got)
	}
}

// The mysql layer is named by the MySQL source's words (Q0 item 7 added the
// layer); none of them names a Postgres layer, and the Postgres words still
// name postgres only.
func TestMySQLLayerTerms(t *testing.T) {
	p := owners.Pipeline{ID: "orders-mysql", SourceKind: owners.SourceKindMySQL, Connectors: []string{"orders-mysql"}}
	for _, c := range []struct {
		layer, text string
		want        bool
	}{
		{"mysql", "The MySQL source purged the binlogs the connector needs.", true},
		{"mysql", "gtid_purged holds transactions the stored GTID set lacks.", true},
		{"mysql", "REPLICATION SLAVE was revoked on mysql:3306.", true},
		{"mysql", "The binary logs were purged at 09:05.", true},
		{"mysql", "It is not the MySQL server: the history topic was deleted.", false},
		{"mysql", "PostgreSQL dropped the column.", false},
		{"postgres", "The MySQL source purged the binlogs.", false},
		{"postgres", "PostgreSQL dropped the column.", true},
	} {
		if got := namesLayer(layerTermsFor(p, c.layer), c.text); got != c.want {
			t.Errorf("namesLayer(%s, %q) = %v, want %v", c.layer, c.text, got, c.want)
		}
	}
}
