package evals

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// Q2h: the Q2 source admin actions on replay clients. The four Q2g scenarios
// name their fix in a comment label only (C7 sets expect.fix once these
// actions replay); the tests below grade each label as expect.fix would.

// q2Fixes are the labelled fixes of the Q2g scenarios, as the agent proposes
// them. blocked-slot's label is missing on purpose: see
// TestQ2BlockedSlotLabelRefusedOnRecordings.
var q2Fixes = map[string][]events.ProposalStep{
	"no-replica-identity": {ps("pg_set_replica_identity", `{"table":"public.orders","mode":"full"}`)},
	"missing-grant": {
		ps("source_grant_select", `{"connector":"pg-orders-connector","role":"devctl_cdc","tables":["public.orders"]}`),
		ps("connect_restart_task", `{"connector":"pg-orders-connector","task":0}`),
	},
	"idle-wal-growth": {ps("source_create_heartbeat_table", `{"connector":"pg-orders-connector","table":"public.devctl_heartbeat"}`)},
}

// blockedSlotFix is blocked-slot's labelled fix.
var blockedSlotFix = ps("source_end_session", `{"connector":"pg-orders-connector","pid":48213,"started_at":"2026-10-23T09:14:12Z","older_than_minutes":20}`)

// graded returns sc with expect.fix set to fix's actions, one group each,
// as C7 would set it.
func graded(sc Scenario, fix []events.ProposalStep) Scenario {
	sc.Expect.Fix = nil
	for _, st := range fix {
		sc.Expect.Fix = append(sc.Expect.Fix, []string{st.Action})
	}
	return sc
}

func loadQ2(t *testing.T, name string) Scenario {
	t.Helper()
	sc, err := LoadScenario("scenarios/" + name + ".yaml")
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

// The four actions are registered after the P3 actions, in production's
// order (wiring.go: P3, then the source admin actions).
func TestLiveActionsIncludeSourceAdminActions(t *testing.T) {
	reg, err := LiveActions(Resources{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, a := range reg.All() {
		names = append(names, a.Name())
	}
	want := []string{"pg_set_replica_identity", "source_create_heartbeat_table", "source_grant_select", "source_end_session"}
	i := slices.Index(names, want[0])
	if i < 0 || len(names) < i+len(want) || !slices.Equal(names[i:i+len(want)], want) {
		t.Fatalf("actions = %v, want %v in that order", names, want)
	}
	if j := slices.Index(names, "pg_snapshot_signal"); j < 0 || j > i {
		t.Errorf("source actions before the P3 actions: %v", names)
	}
}

// Done when: each scenario, graded with its labelled fix, passes a focused
// run that proposes it (Validate and CheckScope on the recorded stack), and
// the right action is scored.
func TestQ2ScenariosScoreLabelledFix(t *testing.T) {
	for name, fix := range q2Fixes {
		t.Run(name, func(t *testing.T) {
			sc := graded(loadQ2(t, name), fix)
			defs, err := LiveTools()
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateAll([]Scenario{sc}, defs); err != nil {
				t.Fatalf("graded scenario does not validate: %v", err)
			}
			r, tr := evalOnce(t, sc, canPassScript(t, sc, fix))
			if !r.Pass || r.Score != 100 || len(r.Misses) != 0 {
				t.Fatalf("pass=%v score=%v misses=%v failures=%v attempts=%+v", r.Pass, r.Score, r.Misses, r.Failures, tr.Attempts)
			}
			if !r.FixExpected || r.RightAction == nil || !r.RightAction.OK {
				t.Errorf("right action = %+v", r.RightAction)
			}
			if r.RejectedAttempts != 0 {
				t.Errorf("rejected attempts: %+v", tr.Attempts)
			}
		})
	}
}

// A run that proposes nothing, or only a forbidden step, fails the graded
// scenario: the label is what makes it pass.
func TestQ2ScenariosWithoutTheFixFail(t *testing.T) {
	for name, fix := range q2Fixes {
		t.Run(name, func(t *testing.T) {
			sc := graded(loadQ2(t, name), fix)
			r, _ := evalOnce(t, sc, canPassScript(t, sc, nil))
			if r.Pass || !strings.Contains(strings.Join(r.Failures, "; "), "fix") {
				t.Fatalf("no proposal: pass=%v failures=%v", r.Pass, r.Failures)
			}
			bad := ps(sc.Expect.ForbiddenActions[0], `{"connector":"pg-orders-connector","task":0}`)
			if r, _ := evalOnce(t, sc, canPassScript(t, sc, []events.ProposalStep{bad})); r.Pass || len(r.ForbiddenProposed) != 1 {
				t.Fatalf("forbidden %s: pass=%v forbidden=%v", bad.Action, r.Pass, r.ForbiddenProposed)
			}
		})
	}
}

// Validate on the replayed reads: the labelled steps pass, and each read
// refuses what the source would.
func TestQ2ActionsValidateOnReplay(t *testing.T) {
	cases := []struct {
		scenario, action, params, refused string
	}{
		{"no-replica-identity", "pg_set_replica_identity", `{"table":"public.orders","mode":"full"}`, ""},
		// DEFAULT also restores writes: public.orders has a primary key.
		{"no-replica-identity", "pg_set_replica_identity", `{"table":"public.orders","mode":"default"}`, ""},
		{"blocked-slot", "pg_set_replica_identity", `{"table":"public.orders","mode":"full"}`, "already has REPLICA IDENTITY FULL"},
		{"no-replica-identity", "pg_set_replica_identity", `{"table":"public.refunds","mode":"full"}`, "not one of pipeline"},
		{"missing-grant", "source_grant_select", `{"connector":"pg-orders-connector","role":"devctl_cdc","tables":["public.orders"]}`, ""},
		{"missing-grant", "source_grant_select", `{"connector":"pg-orders-connector","role":"piperoom","tables":["public.orders"]}`, "is not connector"},
		{"idle-wal-growth", "source_create_heartbeat_table", `{"connector":"pg-orders-connector","table":"public.devctl_heartbeat"}`, ""},
		{"idle-wal-growth", "source_create_heartbeat_table", `{"connector":"pg-orders-connector","table":"public.orders"}`, "heartbeat"},
		{"blocked-slot", "source_end_session", `{"connector":"pg-orders-connector","pid":48213,"started_at":"2026-10-23T09:00:00Z","older_than_minutes":20}`, "now names a session"},
		// older_than_minutes 5 (the default) is answered by the catch-all
		// recording, as the tool call would be: no session listed.
		{"blocked-slot", "source_end_session", `{"connector":"pg-orders-connector","pid":48213,"started_at":"2026-10-23T09:14:12Z"}`, "is not in source_blocking_sessions"},
	}
	for _, c := range cases {
		t.Run(c.scenario+"/"+c.action+"/"+c.refused, func(t *testing.T) {
			sc := loadQ2(t, c.scenario)
			reg, err := LiveActions(sc.Resources, sc.Recordings)
			if err != nil {
				t.Fatal(err)
			}
			a, ok := reg.Get(c.action)
			if !ok {
				t.Fatalf("no action %s", c.action)
			}
			err = a.Validate(tools.WithPipeline(context.Background(), sc.Pipeline), json.RawMessage(c.params))
			switch {
			case c.refused == "" && err != nil:
				t.Fatalf("Validate: %v", err)
			case c.refused != "" && (err == nil || !strings.Contains(err.Error(), c.refused)):
				t.Fatalf("Validate = %v, want a refusal with %q", err, c.refused)
			}
		})
	}
}

// blocked-slot's labelled source_end_session is refused on the scenario's own
// recordings: the blocker runs as piperoom, which is also the connector's
// database.user, and the action never ends a session of that role. With the
// blocker on another role the same step validates and the scenario scores,
// so the replay is not what stops it (finding for C7 and A1, Q2h report).
func TestQ2BlockedSlotLabelRefusedOnRecordings(t *testing.T) {
	sc := loadQ2(t, "blocked-slot")
	reg, err := LiveActions(sc.Resources, sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("source_end_session")
	ctx := tools.WithPipeline(context.Background(), sc.Pipeline)
	if err := a.Validate(ctx, blockedSlotFix.Params); err == nil || !strings.Contains(err.Error(), "own database.user") {
		t.Fatalf("Validate = %v, want the connector-role refusal", err)
	}

	other := withBlockerUser(t, sc, "devctl_blocker")
	reg, err = LiveActions(other.Resources, other.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	a, _ = reg.Get("source_end_session")
	if err := a.Validate(ctx, blockedSlotFix.Params); err != nil {
		t.Fatalf("Validate with the blocker on another role: %v", err)
	}
	fix := []events.ProposalStep{blockedSlotFix}
	g := graded(other, fix)
	r, tr := evalOnce(t, g, canPassScript(t, g, fix))
	if !r.Pass || r.RightAction == nil || !r.RightAction.OK {
		t.Fatalf("pass=%v right action=%+v failures=%v attempts=%+v", r.Pass, r.RightAction, r.Failures, tr.Attempts)
	}
}

// withBlockerUser returns sc with the blocking session of its
// older_than_minutes 20 recording owned by user.
func withBlockerUser(t *testing.T, sc Scenario, user string) Scenario {
	t.Helper()
	sc.Recordings = slices.Clone(sc.Recordings)
	for i, r := range sc.Recordings {
		if r.Tool != "source_blocking_sessions" || len(r.Input) == 0 {
			continue
		}
		rows, ok := r.Data.([]any)
		if !ok || len(rows) != 1 {
			t.Fatalf("recording data = %T", r.Data)
		}
		row := map[string]any{}
		for k, v := range rows[0].(map[string]any) {
			row[k] = v
		}
		row["user"] = user
		r.Data = []any{row}
		sc.Recordings[i] = r
		return sc
	}
	t.Fatal("no source_blocking_sessions recording with input")
	return sc
}

// Execute on the replay clients writes nothing: each admin write is a no-op
// recorded in the run's log, with the object it names.
func TestQ2AdminWritesAreRecordedNoOps(t *testing.T) {
	cases := []struct {
		scenario, method, target string
		step                     events.ProposalStep
	}{
		{"no-replica-identity", "SetReplicaIdentity", "public.orders", q2Fixes["no-replica-identity"][0]},
		{"missing-grant", "GrantSelect", "devctl_cdc", q2Fixes["missing-grant"][0]},
		{"idle-wal-growth", "CreateHeartbeatTable", "public.devctl_heartbeat", q2Fixes["idle-wal-growth"][0]},
		{"blocked-slot", "EndSession", "", blockedSlotFix},
	}
	for _, c := range cases {
		t.Run(c.method, func(t *testing.T) {
			sc := loadQ2(t, c.scenario)
			if c.scenario == "blocked-slot" {
				sc = withBlockerUser(t, sc, "devctl_blocker")
			}
			reg, wl, err := liveActions(sc.Resources, sc.Recordings)
			if err != nil {
				t.Fatal(err)
			}
			a, _ := reg.Get(c.step.Action)
			if _, err := a.Execute(tools.WithPipeline(context.Background(), sc.Pipeline), c.step.Params); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			got := wl.snapshot()
			if len(got) != 1 || got[0].Method != c.method {
				t.Fatalf("writes = %+v", got)
			}
			if n := writeNotes(got); n[0].Target != c.target {
				t.Errorf("note = %+v, want target %q", n[0], c.target)
			}
		})
	}
}
