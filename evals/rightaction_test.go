package evals

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// missedTableP3 is missed-table with the fix grading and resources of the C7
// patch (P3g-2), so these tests run whether or not the patch has landed.
func missedTableP3(t *testing.T) Scenario {
	t.Helper()
	sc := loadScenario(t, "scenarios/missed-table.yaml")
	rows := int64(25)
	sc.Resources.ConnectorConfigs = map[string]map[string]string{
		"pg-orders-connector": {"publication.autocreate.mode": "disabled", "signal.data.collection": "public.devctl_signal"},
	}
	sc.Resources.Tables = map[string]TableState{
		"public.orders":         {PrimaryKey: true},
		"public.devctl_refunds": {PrimaryKey: true, EstimatedRows: &rows},
		"public.devctl_signal":  {PrimaryKey: true, SignalColumns: true},
	}
	sc.Resources.Publications = map[string][]string{"dbz_publication": {"public.orders", "public.devctl_signal"}}
	sc.Expect.Fix = [][]string{{"pg_add_publication_table"}, {"pg_snapshot_signal"}}
	sc.Expect.FixTargets = map[string]map[string]any{
		"pg_add_publication_table": {"connector": "pg-orders-connector", "publication": "dbz_publication", "table": "public.devctl_refunds"},
		"pg_snapshot_signal":       {"connector": "pg-orders-connector", "tables": []any{"public.devctl_refunds"}, "signal_table": "public.devctl_signal"},
	}
	return sc
}

var (
	addRefunds    = ps("pg_add_publication_table", `{"connector":"pg-orders-connector","publication":"dbz_publication","table":"public.devctl_refunds"}`)
	snapRefunds   = ps("pg_snapshot_signal", `{"connector":"pg-orders-connector","tables":["public.devctl_refunds"],"signal_table":"public.devctl_signal"}`)
	addWrongTable = ps("pg_add_publication_table", `{"connector":"pg-orders-connector","publication":"dbz_publication","table":"public.orders"}`)
)

func proposalEvents(t *testing.T, props ...[]events.ProposalStep) []RecordedEvent {
	t.Helper()
	var evs []RecordedEvent
	for i, steps := range props {
		b, err := json.Marshal(events.ProposalCreatedPayload{ProposalID: "p" + string(rune('1'+i)), Steps: steps})
		if err != nil {
			t.Fatal(err)
		}
		evs = append(evs, RecordedEvent{Type: events.ProposalCreated, Payload: b})
	}
	return evs
}

func TestJudgeRightAction(t *testing.T) {
	sc := missedTableP3(t)
	tests := []struct {
		name    string
		props   [][]events.ProposalStep
		ok      bool
		missing string
	}{
		{name: "both in order, one proposal", props: [][]events.ProposalStep{{addRefunds, snapRefunds}}, ok: true},
		{name: "both in order, two proposals", props: [][]events.ProposalStep{{addRefunds}, {snapRefunds}}, ok: true},
		{name: "snapshot first", props: [][]events.ProposalStep{{snapRefunds, addRefunds}}, missing: "pg_snapshot_signal"},
		{name: "snapshot in an earlier proposal", props: [][]events.ProposalStep{{snapRefunds}, {addRefunds}}, missing: "pg_snapshot_signal"},
		{name: "wrong target", props: [][]events.ProposalStep{{addWrongTable, snapRefunds}}, missing: "pg_add_publication_table"},
		{name: "a step in between", props: [][]events.ProposalStep{{addRefunds, addWrongTable, snapRefunds}}, ok: true},
		{name: "nothing proposed", missing: "pg_add_publication_table"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ra := judgeRightAction(sc, proposalEvents(t, tt.props...))
			if ra == nil || ra.OK != tt.ok || ra.Missing != tt.missing {
				t.Errorf("right action = %+v, want ok=%v missing=%q", ra, tt.ok, tt.missing)
			}
		})
	}

	// Without fix_targets only the actions and their order count.
	loose := sc
	loose.Expect.FixTargets = nil
	if ra := judgeRightAction(loose, proposalEvents(t, []events.ProposalStep{addWrongTable, snapRefunds})); !ra.OK {
		t.Errorf("no fix_targets: %+v, want ok", ra)
	}
	// A proposal the room made is not the agent's.
	evs := proposalEvents(t, []events.ProposalStep{addRefunds, snapRefunds})
	evs[0].Actor.Kind = events.ActorSystem
	if ra := judgeRightAction(sc, evs); ra.OK {
		t.Errorf("system proposal counted: %+v", ra)
	}
	// No fix expected: no score.
	none := sc
	none.Expect.Fix = nil
	if ra := judgeRightAction(none, evs); ra != nil {
		t.Errorf("no fix expected: %+v, want nil", ra)
	}
}

// TestRightActionDoesNotGrade runs missed-table on the room path with the
// fix proposed in the wrong order: fix_ok and pass stay as they were (every
// group appears), only right_action says the order is wrong.
func TestRightActionDoesNotGrade(t *testing.T) {
	sc := missedTableP3(t)
	script := []step{
		{tool: "pg_publication_tables", input: map[string]any{"pipeline_id": "orders"}},
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "connect_config", input: map[string]any{"connector": "pg-orders-connector"}},
		{hypothesis: hypotheses["missed-table"].good},
	}
	right, _ := evalOnce(t, sc, append(append([]step(nil), script...), proposal(addRefunds, snapRefunds), step{idle: true}))
	if !right.Pass || !right.FixOK || right.RightAction == nil || !right.RightAction.OK {
		t.Fatalf("right order: pass=%v fix_ok=%v right=%+v failures=%v bad=%v", right.Pass, right.FixOK, right.RightAction, right.Failures, right.BadSteps)
	}
	wrong, _ := evalOnce(t, sc, append(append([]step(nil), script...), proposal(snapRefunds, addRefunds), step{idle: true}))
	if wrong.Pass != right.Pass || wrong.Score != right.Score || !wrong.FixOK {
		t.Errorf("wrong order changed the grade: pass=%v score=%v fix_ok=%v failures=%v", wrong.Pass, wrong.Score, wrong.FixOK, wrong.Failures)
	}
	if wrong.RightAction == nil || wrong.RightAction.OK || wrong.RightAction.Missing != "pg_snapshot_signal" {
		t.Errorf("wrong order: right action = %+v", wrong.RightAction)
	}
	var md strings.Builder
	rep := Report{Scenarios: []ScenarioSummary{{Name: sc.Name, Summary: summarize([]RunResult{right, wrong})}}, Results: []RunResult{wrong}}
	rep.Results[0].Pass = false // force the failed-run details
	if err := rep.WriteMarkdown(&md); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(md.String(), "| right action |") || !strings.Contains(md.String(), "| 50% |") ||
		!strings.Contains(md.String(), "right action not proposed: no step for pg_snapshot_signal") {
		t.Errorf("markdown lacks the right-action column or detail:\n%s", md.String())
	}
}

func TestRightActionSummary(t *testing.T) {
	ok, bad := &RightAction{OK: true}, &RightAction{}
	s := summarize([]RunResult{{RightAction: ok}, {RightAction: bad}, {}})
	if s.FixRuns != 2 || s.RightActions != 1 || s.RightActionRate != 0.5 {
		t.Errorf("summary = %+v", s)
	}
	if s := summarize([]RunResult{{}}); s.RightActionRate != -1 || rightActionCell(s) != "-" {
		t.Errorf("no fix runs: rate %v cell %q", s.RightActionRate, rightActionCell(s))
	}
	all := combine([]Summary{
		summarize([]RunResult{{RightAction: ok}}),
		summarize([]RunResult{{RightAction: bad}, {RightAction: bad}}),
		summarize([]RunResult{{}}),
	})
	if all.FixRuns != 3 || all.RightActions != 1 || all.RightActionRate != 0.5 {
		t.Errorf("combined = %+v, want 3 runs, 1 right, rate 0.5 (per scenario)", all)
	}
}

// TestP3ActionsOnReplay: each P3 action validates against the recorded
// stack of its scenario, refuses what the stack refuses, and its Execute
// writes nothing: the replay client records the call.
func TestP3ActionsOnReplay(t *testing.T) {
	mt := missedTableP3(t)
	slot := loadScenario(t, "scenarios/dead-slot-wal.yaml")
	bad := loadScenario(t, "scenarios/consumer-bad-deploy.yaml")
	dlq := loadScenario(t, "scenarios/dlq-backlog.yaml")
	stable := dlq // order-sync is Stable with 1 member in dlq-backlog
	tests := []struct {
		name      string
		sc        Scenario
		action    string
		params    string
		wantErr   string // substring; empty = Validate passes
		wantWrite string // the recorded write of Execute
	}{
		{name: "add table", sc: mt, action: "pg_add_publication_table", params: string(addRefunds.Params), wantWrite: "AddPublicationTables"},
		{name: "add table already published", sc: mt, action: "pg_add_publication_table", params: string(addWrongTable.Params), wantErr: "already in publication"},
		{name: "add to another publication", sc: mt, action: "pg_add_publication_table", params: `{"connector":"pg-orders-connector","publication":"other_pub","table":"public.devctl_refunds"}`, wantErr: "wrong publication"},
		{name: "snapshot signal", sc: mt, action: "pg_snapshot_signal", params: string(snapRefunds.Params), wantWrite: "InsertSnapshotSignal"},
		{name: "snapshot signal, wrong signal table", sc: mt, action: "pg_snapshot_signal", params: `{"connector":"pg-orders-connector","tables":["public.devctl_refunds"],"signal_table":"public.other"}`, wantErr: "signal.data.collection is now public.devctl_signal"},
		{name: "kafka snapshot without a signal topic", sc: mt, action: "connect_snapshot_table", params: `{"connector":"pg-orders-connector","tables":["public.devctl_refunds"],"via":"signal_topic"}`, wantErr: "not configured"},
		{name: "drop slot", sc: slot, action: "pg_drop_replication_slot", params: `{"connector":"pg-orders-connector","slot":"debezium_orders"}`, wantWrite: "DropReplicationSlot"},
		{name: "drop active slot", sc: mt, action: "pg_drop_replication_slot", params: `{"connector":"pg-orders-connector","slot":"debezium_orders"}`, wantErr: "in use by a consumer"},
		{name: "reset offsets", sc: bad, action: "kafka_reset_consumer_offsets", params: `{"group":"order-sync","topics":["pg.public.orders"],"to":"earliest"}`, wantWrite: "ResetGroupOffsets"},
		{name: "reset offsets, wrong topics", sc: bad, action: "kafka_reset_consumer_offsets", params: `{"group":"order-sync","topics":["pg.public.devctl_refunds"],"to":"earliest"}`, wantErr: "also has committed offsets on pg.public.orders"},
		{name: "reset an active group", sc: stable, action: "kafka_reset_consumer_offsets", params: `{"group":"order-sync","topics":["pg.public.orders"],"to":"earliest"}`, wantErr: "active members"},
		{name: "dlq replay", sc: dlq, action: "dlq_replay", params: `{"dlq_topic":"devctl.order-sync.dlq","to_topic":"pg.public.orders","count":20}`},
		{name: "dlq replay past the end", sc: dlq, action: "dlq_replay", params: `{"dlq_topic":"devctl.order-sync.dlq","to_topic":"pg.public.orders","count":5,"from_offset":20}`, wantErr: "at or past the end offset 20"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg, wl, err := liveActions(tt.sc.Resources, tt.sc.Recordings)
			if err != nil {
				t.Fatal(err)
			}
			a, ok := reg.Get(tt.action)
			if !ok {
				t.Fatalf("%s not registered", tt.action)
			}
			ctx := tools.WithPipeline(t.Context(), tt.sc.Pipeline)
			err = a.Validate(ctx, json.RawMessage(tt.params))
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("Validate: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("Validate err = %v, want %q", err, tt.wantErr)
			case tt.wantErr != "":
				return
			}
			if err := a.(actions.Scoped).CheckScope(tt.sc.Pipeline, json.RawMessage(tt.params)); err != nil {
				t.Errorf("CheckScope: %v", err)
			}
			if tt.wantWrite == "" {
				return
			}
			ectx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if _, err := a.Execute(ectx, json.RawMessage(tt.params)); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if got := wl.snapshot(); len(got) != 1 || got[0].Method != tt.wantWrite {
				t.Errorf("writes = %+v, want one %s", got, tt.wantWrite)
			}
		})
	}
}

// TestP3ReplayReadsNeedState: a scenario that declares nothing for the pg
// actions gets the replay refusal, not a "does not exist" that would blame
// the agent.
func TestP3ReplayReadsNeedState(t *testing.T) {
	sc := loadScenario(t, "scenarios/missed-table.yaml")
	sc.Resources.Tables, sc.Resources.Publications = nil, nil
	reg, err := LiveActions(sc.Resources, sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("pg_add_publication_table")
	if err := a.Validate(t.Context(), addRefunds.Params); !errors.Is(err, errNotReplayed) {
		t.Errorf("err = %v, want errNotReplayed", err)
	}
	if _, err := (replayConfig{res: Resources{Connectors: []string{"x"}}}).Config(t.Context(), "nope"); !errors.Is(err, clients.ErrNotFound) {
		t.Errorf("unknown connector: %v", err)
	}
}

func TestResourcesValidate(t *testing.T) {
	r := Resources{
		Connectors:       []string{"c"},
		ConnectorConfigs: map[string]map[string]string{"other": {"k": "v"}},
		Tables:           map[string]TableState{"nodot": {}, "public.t": {RelKind: "x", ReplicaIdentity: "z"}},
		Publications:     map[string][]string{"pub": {"public.missing"}},
	}
	var msgs []string
	for _, e := range r.validate() {
		msgs = append(msgs, e.Error())
	}
	got := strings.Join(msgs, "\n")
	for _, want := range []string{`connector "other" is not in resources.connectors`, `"nodot" is not "schema.table"`, `relkind "x"`, `replica_identity "z"`, `table "public.missing" is not in resources.tables`} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if errs := missedTableP3(t).Resources.validate(); len(errs) != 0 {
		t.Errorf("missed-table resources: %v", errs)
	}
}
