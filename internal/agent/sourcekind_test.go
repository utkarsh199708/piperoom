package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// sourceRegistry is a read-only registry of recordingTools named names.
func sourceRegistry(t *testing.T, names ...string) *tools.Registry {
	t.Helper()
	reg := tools.NewReadOnlyRegistry()
	for _, n := range names {
		if err := reg.Register(&recordingTool{name: n, schema: testSchema(), result: tools.Result{Summary: "ok"}}); err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

// sourceLayeredAction is a source admin action: Layer() is postgres, the
// approving layer is the pipeline's source layer (actions.SourceLayered).
type sourceLayeredAction struct{ fakeAction }

func (*sourceLayeredAction) SourceLayered() {}

// DescribeFor is sourceactions.SourceDescribed's.
func (a *sourceLayeredAction) DescribeFor(kind string) string {
	return a.name + " on " + kind
}

var _ actions.SourceLayered = (*sourceLayeredAction)(nil)

func sourceTestActions(t *testing.T) *actions.Registry {
	t.Helper()
	return kindTestActions(t,
		&fakeAction{name: "pg_drop_replication_slot", layer: "postgres", params: map[string]any{"type": "object"}},
		&sourceLayeredAction{fakeAction{name: "source_grant_select", layer: "postgres", params: map[string]any{"type": "object"}}},
	)
}

// The postgres and mysql sets of names (design §3).
var (
	pgRoomTools    = []string{"pg_replication_slots", "pg_recent_ddl", "source_settings", "connect_status", "kafka_consumer_lag", "warehouse_freshness", "what_changed"}
	mysqlRoomTools = []string{"source_settings", "source_long_transactions", "source_row", "mysql_binlog_status", "connect_status", "kafka_consumer_lag", "kafka_schema_history", "warehouse_freshness", "what_changed"}
)

func TestToolServesSource(t *testing.T) {
	for _, tt := range []struct {
		name      string
		pg, mysql bool
	}{
		{"pg_replication_slots", true, false},
		{"pg_row", true, false},
		{"mysql_binlog_status", false, true},
		{"source_row", false, true},
		{"source_long_transactions", false, true},
		{"kafka_schema_history", false, true},
		{"source_settings", true, true},
		{"source_blocking_sessions", true, true},
		{"connect_status", true, true},
		{"kafka_consumer_lag", true, true},
		{"what_changed", true, true},
	} {
		if got := ToolServesSource(tt.name, events.SourceKindPostgres); got != tt.pg {
			t.Errorf("ToolServesSource(%s, postgres) = %v, want %v", tt.name, got, tt.pg)
		}
		if got := ToolServesSource(tt.name, events.SourceKindMySQL); got != tt.mysql {
			t.Errorf("ToolServesSource(%s, mysql) = %v, want %v", tt.name, got, tt.mysql)
		}
	}
	for _, tt := range []struct {
		name      string
		pg, mysql bool
	}{
		{"pg_set_replica_identity", true, false},
		{"pg_drop_replication_slot", true, false},
		{"pg_resnapshot_rows", true, false},
		{"mysql_anything", false, true},
		{"source_grant_select", true, true},
		{"connect_reset_source_offsets", true, true},
	} {
		if got := ActionServesSource(tt.name, events.SourceKindPostgres); got != tt.pg {
			t.Errorf("ActionServesSource(%s, postgres) = %v, want %v", tt.name, got, tt.pg)
		}
		if got := ActionServesSource(tt.name, events.SourceKindMySQL); got != tt.mysql {
			t.Errorf("ActionServesSource(%s, mysql) = %v, want %v", tt.name, got, tt.mysql)
		}
	}
}

func TestSourceKindNormalises(t *testing.T) {
	for in, want := range map[string]string{"": "postgres", "postgres": "postgres", "mysql": "mysql"} {
		if got, err := SourceKind(in); err != nil || got != want {
			t.Errorf("SourceKind(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"mariadb", "MySQL", "sqlserver"} {
		if _, err := SourceKind(bad); err == nil {
			t.Errorf("SourceKind(%q) must fail", bad)
		}
	}
	if _, err := ToolsForSource(nil, "oracle"); err == nil {
		t.Error("ToolsForSource(oracle) must fail")
	}
	if _, err := PromptHashForSource("", "oracle"); err == nil {
		t.Error("PromptHashForSource(oracle) must fail")
	}
}

func TestRoomSourceKind(t *testing.T) {
	for _, tt := range []struct {
		name  string
		tools []string
		want  string
	}{
		{"nil", nil, "postgres"},
		{"postgres", pgRoomTools, "postgres"},
		{"mysql", mysqlRoomTools, "mysql"},
		{"mysql by binlog tool alone", []string{"mysql_binlog_status", "connect_status"}, "mysql"},
		{"mysql by neutral names alone", []string{"source_row", "connect_status"}, "mysql"},
		// kafka_schema_history and the Q2a source_* tools say nothing.
		{"neutral only", []string{"kafka_schema_history", "source_settings", "connect_status"}, "postgres"},
		{"batch", []string{"dag_runs", "warehouse_freshness"}, "postgres"},
	} {
		var reg *tools.Registry
		if tt.tools != nil {
			reg = sourceRegistry(t, tt.tools...)
		}
		got, err := RoomSourceKind(reg)
		if err != nil || got != tt.want {
			t.Errorf("%s: RoomSourceKind = %q, %v; want %q", tt.name, got, err, tt.want)
		}
	}
	_, err := RoomSourceKind(sourceRegistry(t, "pg_row", "mysql_binlog_status"))
	if err == nil || !strings.Contains(err.Error(), "pg_row") || !strings.Contains(err.Error(), "mysql_binlog_status") {
		t.Fatalf("mixed registry: err = %v, want both names", err)
	}
}

// A MySQL room offers no pg_* tool or action and gets the MySQL prompt; a
// Postgres room no mysql_* tool or kafka_schema_history and the CDC prompt
// byte for byte; a source admin step is the mysql owner's in a MySQL room.
func TestRunnerToolsActionsPromptBySource(t *testing.T) {
	for _, tt := range []struct {
		source      string
		tools       []string
		wantTools   []string
		wantActions []string
		wantPrompt  string
		wantLayer   string
	}{
		{
			source:      "postgres",
			tools:       append(slices.Clone(pgRoomTools), "kafka_schema_history"),
			wantTools:   pgRoomTools,
			wantActions: []string{"connect_restart_task", "schema_set_compatibility", "pg_drop_replication_slot", "source_grant_select"},
			wantPrompt:  systemPrompt(),
			wantLayer:   "postgres",
		},
		{
			source:      "mysql",
			tools:       mysqlRoomTools,
			wantTools:   mysqlRoomTools,
			wantActions: []string{"connect_restart_task", "schema_set_compatibility", "source_grant_select"},
			wantPrompt:  systemPromptForSource(events.PipelineKindCDC, events.SourceKindMySQL, RoomMode{Write: true}),
			wantLayer:   "mysql",
		},
	} {
		acts := sourceTestActions(t)
		m := &promptCapture{}
		props := &fakeProposals{id: "p_1"}
		r, err := newRunnerWith(Alert{Title: "t", PipelineID: "p"}, Deps{
			Tools: sourceRegistry(t, tt.tools...), Actions: acts, Proposals: props,
			Inbox: &fakeInbox{waitErr: context.Canceled}, Sink: &fakeSink{}, Logger: discardLogger(),
		}, Options{MaxTurns: 3}, viaSDK(m))
		if err != nil {
			t.Fatalf("%s: %v", tt.source, err)
		}
		if r.source != tt.source {
			t.Fatalf("%s: runner source = %q", tt.source, r.source)
		}
		if got := names(r.deps.Tools); !slices.Equal(got, tt.wantTools) {
			t.Fatalf("%s: tools %v, want %v", tt.source, got, tt.wantTools)
		}
		if got := actionNames(r.deps.Actions); !slices.Equal(got, tt.wantActions) {
			t.Fatalf("%s: actions %v, want %v", tt.source, got, tt.wantActions)
		}
		_ = r.run(context.Background())
		if len(m.system) == 0 || m.system[0] != tt.wantPrompt {
			t.Fatalf("%s: wrong system prompt", tt.source)
		}
		if len(m.propose) == 0 {
			t.Fatalf("%s: propose_fix not offered", tt.source)
		}
		desc := m.propose[0]
		if want := "- source_grant_select() [" + tt.wantLayer + "]"; !strings.Contains(desc, want) {
			t.Errorf("%s: propose_fix lists the source action without %q:\n%s", tt.source, want, desc)
		}
		if tt.source == "mysql" {
			if strings.Contains(desc, "pg_") {
				t.Errorf("mysql: propose_fix description names a pg_ action:\n%s", desc)
			}
			if !strings.Contains(desc, "source_grant_select on mysql") {
				t.Errorf("mysql: propose_fix does not use DescribeFor(mysql):\n%s", desc)
			}
		} else if !strings.Contains(desc, "source_grant_select description") {
			t.Errorf("postgres: propose_fix must keep Describe():\n%s", desc)
		}

		// The step's layer, as the room checks it (Q8o), is LayerFor's.
		var tool *proposeFixTool
		for _, e := range r.toolList.byName {
			if p, ok := e.tool.(*proposeFixTool); ok {
				tool = p
			}
		}
		if tool == nil {
			t.Fatalf("%s: no propose_fix in the tool list", tt.source)
		}
		raw := json.RawMessage(`{"title":"grant","steps":[{"action":"source_grant_select","params":"{}","description":"grant"}],"risk":"low","rollback":"revoke the grant"}`)
		if _, err := tool.Run(context.Background(), raw); err != nil {
			t.Fatalf("%s: propose: %v", tt.source, err)
		}
		if got := props.created.Steps[0].Layer; got != tt.wantLayer {
			t.Errorf("%s: step layer %q, want %q", tt.source, got, tt.wantLayer)
		}
		if !slices.Equal(props.created.RequiredLayers, []string{tt.wantLayer}) {
			t.Errorf("%s: required layers %v", tt.source, props.created.RequiredLayers)
		}
	}
}

func TestRunnerRefusesMixedSourceRegistry(t *testing.T) {
	_, err := newRunnerWith(Alert{Title: "t", PipelineID: "p"}, Deps{
		Tools: sourceRegistry(t, "pg_row", "source_row"),
		Inbox: &fakeInbox{}, Sink: &fakeSink{}, Logger: discardLogger(),
	}, Options{}, viaSDK(&promptCapture{}))
	if err == nil {
		t.Fatal("a registry with pg_row and source_row must be refused")
	}
}

// mysqlPromptSHA256 and mysqlPromptWriteOffSHA256 pin the MySQL CDC prompt
// beside its snapshots (Q8g), so a regenerated snapshot cannot move it
// silently. #994 moved them when the replacer followed W4h's restart bullet.
// W4g moved them again: the shared method and fixes carry W4g's wording, the
// resume pattern follows W4g's resume check, and MySQL-only trims (the server
// id rule folded into the config-setting bullet, a shorter intro, purged
// binlog and resume lines) bring the prompt back under the CDC ceiling; its
// fix round (rule 4's "correctly refusing" and <layer> <component> form,
// rule 5's steer and tool clause) is word-neutral. With KAN-515's hold rule
// the MySQL prompt is the CDC prompt's size: 1353 words with write mode off,
// 1352 with it on. W4i moved them with the shared method and fixes (rule 1's
// negative claim, rule 3, the restart bullet, the offsets note); the replacer
// follows the restart bullet and the resume check's dropped "that", so the
// MySQL prompt stays word-neutral too.
const (
	mysqlPromptSHA256         = "22fef92a1f561c88a3fb735664a9eddfaee38a66b6a6b340cff8281e7446b9f9"
	mysqlPromptWriteOffSHA256 = "ab3b23dfab5bba51e5585d6c05aae8ccbf5b4e4c136a914fcbbc522493369fa1"
)

func TestSystemPromptSnapshotMySQL(t *testing.T) {
	for _, tt := range []struct {
		file, sum string
		mode      RoomMode
	}{
		{"system_prompt_cdc_mysql.txt", mysqlPromptSHA256, RoomMode{Write: true}},
		{"system_prompt_cdc_mysql_write_off.txt", mysqlPromptWriteOffSHA256, RoomMode{Write: false}},
	} {
		got := systemPromptForSource(events.PipelineKindCDC, events.SourceKindMySQL, tt.mode)
		checkPromptSnapshot(t, tt.file, got)
		sum := sha256.Sum256([]byte(got))
		if h := hex.EncodeToString(sum[:]); h != tt.sum {
			t.Fatalf("%s: sha256 = %s, want %s", tt.file, h, tt.sum)
		}
		// The CDC word ceiling holds for the MySQL prompt too. #994 raised
		// it by 20 for the merge of #934 and #988 (W4h's shared fixes made
		// the MySQL bullets replace a clause, not a bullet), and KAN-515 by
		// 20 more for the hold rule; W4g's MySQL-only trims bring the prompt
		// back to the CDC prompt's size, so the ceiling is the CDC one
		// again (1355 since KAN-515).
		if wc := len(strings.Fields(got)); wc > 1355 {
			t.Fatalf("%s: %d words, over the 1355 limit", tt.file, wc)
		}
	}
}

// The Postgres prompt is systemPromptForMode itself in every mode and kind,
// so its snapshots, hash and the CDC evals do not move.
func TestPostgresPromptUnchangedBySource(t *testing.T) {
	for _, kind := range []string{events.PipelineKindCDC, events.PipelineKindBatch} {
		for _, m := range []RoomMode{{Write: true}, {Write: false}} {
			for _, sk := range []string{"", events.SourceKindPostgres} {
				if systemPromptForSource(kind, sk, m) != systemPromptForMode(kind, m) {
					t.Fatalf("kind %s source %q mode %+v: prompt differs from systemPromptForMode", kind, sk, m)
				}
			}
		}
		// A batch prompt has no source wording.
		if systemPromptForSource(events.PipelineKindBatch, events.SourceKindMySQL, RoomMode{Write: true}) != systemPromptFor(events.PipelineKindBatch) {
			t.Fatal("batch prompt must not depend on the source kind")
		}
	}
	h, err := PromptHashForSource("", "")
	if err != nil || h != PromptHash() {
		t.Fatalf(`PromptHashForSource("", "") = %q, %v; want PromptHash()`, h, err)
	}
	hm, err := PromptHashForSource("", events.SourceKindMySQL)
	if err != nil || hm == h {
		t.Fatalf("PromptHashForSource(cdc, mysql) = %q, %v", hm, err)
	}
	names, err := PromptToolNamesForSource("", "")
	if err != nil || !slices.Equal(names, PromptToolNames()) {
		t.Fatalf(`PromptToolNamesForSource("", "") = %v, %v`, names, err)
	}
}

// TestMySQLReplacerPatternsMatchOnce: each old string of the MySQL replacers
// occurs exactly once in the CDC text it rewrites, its replacement reaches
// the MySQL prompt, and the old string does not. #934's publication pattern
// was written against the pre-W4h fixes section, matched nothing once W4h
// landed, and showed up only as pg_* names in MySQL rooms; this names the
// pattern instead (LEAD-REVIEW on #993).
func TestMySQLReplacerPatternsMatchOnce(t *testing.T) {
	for _, set := range []struct {
		name  string
		base  string
		pairs []string
	}{
		{"promptCDCMethod", promptCDCMethod, mysqlMethodReplacements()},
		{"promptCDCFixes", promptCDCFixes, mysqlFixesReplacements()},
	} {
		if len(set.pairs)%2 != 0 {
			t.Fatalf("%s: odd number of replacer strings", set.name)
		}
		for i := 0; i < len(set.pairs); i += 2 {
			old, repl := set.pairs[i], set.pairs[i+1]
			if n := strings.Count(set.base, old); n != 1 {
				t.Errorf("%s: pattern %d occurs %d times, want 1: %q", set.name, i/2, n, old)
			}
			for _, m := range []RoomMode{{Write: true}, {Write: false}} {
				p := systemPromptForSource(events.PipelineKindCDC, events.SourceKindMySQL, m)
				if !strings.Contains(p, repl) {
					t.Errorf("%s: replacement %d missing from the MySQL prompt (mode %+v): %q", set.name, i/2, m, repl)
				}
				if strings.Contains(p, old) {
					t.Errorf("%s: pattern %d still in the MySQL prompt (mode %+v): %q", set.name, i/2, m, old)
				}
			}
		}
	}
}

// The MySQL prompt names no pg_* callable (a MySQL room offers none) and
// names the MySQL reads: the binlog, GTIDs, the schema history topic. Each
// name it does use is offered in a MySQL room.
func TestMySQLPromptNamesNoPostgresTool(t *testing.T) {
	for _, m := range []RoomMode{{Write: true}, {Write: false}} {
		p := systemPromptForSource(events.PipelineKindCDC, events.SourceKindMySQL, m)
		for _, banned := range []string{"pg_", "Postgres", "WAL", "slot", "publication", "wal_status"} {
			if strings.Contains(p, banned) {
				t.Errorf("MySQL prompt (mode %+v) contains %q", m, banned)
			}
		}
		for _, want := range []string{"MySQL (binlog, GTID)", "mysql_binlog_status", "kafka_schema_history", "database.server.id", "GTIDs", "snapshot.mode recovery",
			promptRulesFor(events.PipelineKindCDC), promptPastIncidents, promptAlertIsData, promptCDCOffsetsNote} {
			if !strings.Contains(p, want) {
				t.Errorf("MySQL prompt (mode %+v) lacks %q", m, want)
			}
		}
	}
	got, err := PromptToolNamesForSource(events.PipelineKindCDC, events.SourceKindMySQL)
	if err != nil {
		t.Fatal(err)
	}
	room := RoomToolNames()
	for _, n := range got {
		if slices.Contains(room, n) {
			continue
		}
		if !ToolServesSource(n, events.SourceKindMySQL) || !ActionServesSource(n, events.SourceKindMySQL) {
			t.Errorf("MySQL prompt names %s, which a MySQL room does not offer", n)
		}
	}
}

// The first-look prompts keep their Postgres bytes; a MySQL room's name the
// MySQL source and the mysql layer of mysql_* reads.
func TestTriagePromptsBySource(t *testing.T) {
	for _, p := range []string{triagePrompt(), triageVerdictPrompt()} {
		if triageForSource(p, events.SourceKindPostgres) != p || triageForSource(p, "") != p {
			t.Fatal("the Postgres first-look prompt changed")
		}
		my := triageForSource(p, events.SourceKindMySQL)
		if strings.Contains(my, "Postgres") || strings.Contains(my, "pg_") {
			t.Errorf("MySQL first-look prompt names Postgres:\n%s", my)
		}
		if !strings.Contains(my, "(MySQL -> Debezium") {
			t.Errorf("MySQL first-look prompt lacks the MySQL source:\n%s", my)
		}
	}
	if !strings.Contains(triageForSource(triageVerdictPrompt(), events.SourceKindMySQL), "mysql_* are mysql") {
		t.Error("MySQL verdict prompt does not give mysql_* reads their layer")
	}
	if triageToolLayer("mysql_binlog_status") != events.LayerMySQL || componentForTool("mysql_binlog_status") != events.LayerMySQL {
		t.Error("mysql_* reads must be the mysql layer's")
	}
}
