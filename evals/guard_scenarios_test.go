package evals

// H2: the harness side of the hypothesis guard (docs/design/hypothesis-guard.md):
// the scenario field watch_status, the guard mode of an eval run, the counts
// expect.guard_refusals and expect.max_unbacked_rule_outs grade, and the four
// guard scenarios replayed with a scripted model.

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/events"
	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
)

// ownerChangedSource is the source Postgres of publication-owner-ruled-out-trap:
// public.orders was handed to orders_owner, the publication dbz_publication is
// still owned by the connector role piperoom and already lists the table.
type ownerChangedSource struct{ clients.SourceDB }

func (ownerChangedSource) ObjectOwners(_ context.Context, role, publication string, tables []string) (postgres.ObjectOwners, error) {
	out := postgres.ObjectOwners{
		Role: role, RoleExists: true, Replication: true,
		Publication: postgres.PublicationOwner{Name: publication, Exists: true, Owner: role, RoleIsOwner: true, RoleHasOwnerPrivs: true},
	}
	for _, t := range tables {
		out.Tables = append(out.Tables, postgres.TableOwner{Table: t, Exists: true, Owner: "orders_owner", InPublication: true})
	}
	return out, nil
}

// ownerChangedRegistry registers the real pg_object_owners over that source.
func ownerChangedRegistry() (*tools.Registry, error) {
	reg := tools.NewRegistry()
	err := dbtools.RegisterObjectOwners(reg, dbtools.Deps{Src: ownerChangedSource{}, Connect: w4kConnect{w: w4kHorizonHeld()}, Owners: w4kOwners()})
	return reg, err
}

// TestGuardOwnerReadIsRealToolOutput: the pg_object_owners recording of
// publication-owner-ruled-out-trap equals what the real tool returns over the
// source above. The rest of that scenario is its parent's, hand-composed.
func TestGuardOwnerReadIsRealToolOutput(t *testing.T) {
	sc := loadScenario(t, "scenarios/publication-owner-ruled-out-trap.yaml")
	reg, err := ownerChangedRegistry()
	if err != nil {
		t.Fatal(err)
	}
	w4kCheck(t, sc, reg, "", "pg_object_owners", `{"pipeline_id":"orders"}`)
}

// TestGuardScenarioSet: the four scenarios exist with the shape the task asks
// for, and the one that was already there carries a clean watch row.
func TestGuardScenarioSet(t *testing.T) {
	long := loadScenario(t, "scenarios/long-tx-slot-stuck-watcher-firing.yaml")
	if long.WatchStatus == nil || len(long.WatchStatus.Signals) == 0 || long.WatchStatus.Signals[0].Signal != "long_tx" ||
		long.WatchStatus.Signals[0].State != clients.WatchFiring || long.WatchStatus.Signals[0].ClearingSince != "" {
		t.Errorf("long-tx watch_status = %+v", long.WatchStatus)
	}
	if long.Expect.RootCauseLayer != "postgres" || !matchForbidden(long.Expect.RootCauseForbidden, "Nothing to fix here.") ||
		!matchForbidden(long.Expect.RootCauseForbidden, "There are no long transactions.") {
		t.Errorf("long-tx expectations: layer %q forbidden %v", long.Expect.RootCauseLayer, long.Expect.RootCauseForbidden)
	}

	trap := loadScenario(t, "scenarios/publication-owner-ruled-out-trap.yaml")
	if n := trap.Expect.MaxUnbackedRuleOuts; n == nil || *n != 0 || trap.Expect.RootCauseLayer != "postgres" {
		t.Errorf("trap expectations: max_unbacked_rule_outs %v layer %q", n, trap.Expect.RootCauseLayer)
	}

	clearing := loadScenario(t, "scenarios/healthy-self-recovered-watcher-clearing.yaml")
	if clearing.Expect.Outcome != OutcomeHealthy || clearing.Expect.GuardRefusals == nil || !clearing.Expect.GuardRefusals.has(0) || clearing.Expect.GuardRefusals.has(1) {
		t.Errorf("clearing expectations: %+v", clearing.Expect)
	}
	if s := clearing.WatchStatus.Signals; len(s) != 1 || s[0].ClearingSince == "" {
		t.Errorf("clearing watch_status = %+v", s)
	}

	lag := loadScenario(t, "scenarios/healthy-self-recovered-watcher-lagging.yaml")
	if g := lag.Expect.GuardRefusals; g == nil || g.has(0) || !g.has(1) || !g.has(2) || g.has(3) {
		t.Errorf("lagging guard_refusals = %v, want 1 to 2", g)
	}
	if s := lag.WatchStatus.Signals; len(s) != 1 || s[0].ClearingSince != "" || s[0].State != clients.WatchFiring {
		t.Errorf("lagging watch_status = %+v", s)
	}

	recov := loadScenario(t, "scenarios/healthy-after-recovery.yaml")
	if recov.WatchStatus == nil || len(recov.WatchStatus.Signals) != 0 {
		t.Errorf("healthy-after-recovery watch_status = %+v, want a fresh empty row", recov.WatchStatus)
	}
}

func TestWatchStatusValidation(t *testing.T) {
	toolIn := map[string][]string{"connect_status": {"connector"}}
	base := func() Scenario {
		return Scenario{
			Name: "x", Pipeline: pipelineOrders(), Alert: Alert{Title: "t"},
			Recordings: []Recording{{Tool: "connect_status", Summary: "ok"}},
			Expect:     Expect{RootCause: [][]string{{"a"}}},
		}
	}
	row := func(mut func(*WatchSignalSpec)) func(*Scenario) {
		return func(s *Scenario) {
			g := WatchSignalSpec{Signal: "long_tx", State: "firing", Severity: "sev2", Since: "-4m"}
			if mut != nil {
				mut(&g)
			}
			s.WatchStatus = &WatchStatusSpec{CheckedAt: "-20s", Signals: []WatchSignalSpec{g}}
		}
	}
	n := func(v int) *int { return &v }
	tests := []struct {
		name   string
		mutate func(*Scenario)
		want   string
	}{
		{"absent", func(*Scenario) {}, ""},
		{"valid row", row(nil), ""},
		{"empty row", func(s *Scenario) { s.WatchStatus = &WatchStatusSpec{} }, ""},
		{"clearing", row(func(g *WatchSignalSpec) { g.ClearingSince = "-1m" }), ""},
		{"unknown signal", row(func(g *WatchSignalSpec) { g.Signal = "slot_walz" }), `signal "slot_walz" is not a watcher signal`},
		{"state ok is not listed", row(func(g *WatchSignalSpec) { g.State = "ok" }), `state "ok"`},
		{"state missing", row(func(g *WatchSignalSpec) { g.State = "" }), "watch_status.signals[0].state"},
		{"bad severity", row(func(g *WatchSignalSpec) { g.Severity = "high" }), `severity "high"`},
		{"since missing", row(func(g *WatchSignalSpec) { g.Since = "" }), "watch_status.signals[0].since"},
		{"since not a duration", row(func(g *WatchSignalSpec) { g.Since = "yesterday" }), "want a duration"},
		{"since in the future", row(func(g *WatchSignalSpec) { g.Since = "5m" }), "in the future"},
		{"clearing before since", row(func(g *WatchSignalSpec) { g.ClearingSince = "-10m" }), "is before since"},
		{"clearing in the future", row(func(g *WatchSignalSpec) { g.ClearingSince = "1m" }), "in the future"},
		{"bad checked_at", func(s *Scenario) { s.WatchStatus = &WatchStatusSpec{CheckedAt: "now"} }, "watch_status.checked_at"},
		{"duplicate signal", func(s *Scenario) {
			g := WatchSignalSpec{Signal: "long_tx", State: "firing", Severity: "sev2", Since: "-4m"}
			s.WatchStatus = &WatchStatusSpec{Signals: []WatchSignalSpec{g, g}}
		}, "listed twice"},
		{"too many signals", func(s *Scenario) {
			s.WatchStatus = &WatchStatusSpec{Signals: make([]WatchSignalSpec, clients.MaxWatchSignals+1)}
		}, "at most"},
		{"with a watcher start", func(s *Scenario) {
			row(nil)(s)
			s.Start = Start{By: StartWatcher, Watcher: &WatcherStart{}}
		}, "alert-started"},
		{"guard_refusals range", func(s *Scenario) { s.Expect.GuardRefusals = &IntRange{Min: n(1), Max: n(2)} }, ""},
		{"guard_refusals reversed", func(s *Scenario) { s.Expect.GuardRefusals = &IntRange{Min: n(3), Max: n(2)} }, "above max"},
		{"guard_refusals negative", func(s *Scenario) { s.Expect.GuardRefusals = &IntRange{Max: n(-1)} }, "negative"},
		{"max_unbacked_rule_outs 0", func(s *Scenario) { s.Expect.MaxUnbackedRuleOuts = n(0) }, ""},
		{"max_unbacked_rule_outs negative", func(s *Scenario) { s.Expect.MaxUnbackedRuleOuts = n(-1) }, "max_unbacked_rule_outs is negative"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sc := base()
			tc.mutate(&sc)
			err := sc.Validate(toolIn, nil)
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("valid scenario refused: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Errorf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestParseWatchStatusAndRanges(t *testing.T) {
	good := `
name: x
pipeline: {id: orders, title: t, topic_prefix: pg, source_tables: [public.orders], connectors: [c], consumer_groups: [g], sink_tables: [analytics.orders], sink_ts_column: updated_at}
alert: {title: t}
watch_status:
  checked_at: -30s
  signals:
    - {signal: connector_down, state: firing, severity: sev2, node: 'connect:c', since: -9m, clearing_since: -4m}
expect:
  outcome: healthy
  guard_refusals: %s
  max_unbacked_rule_outs: 0
`
	for _, tc := range []struct {
		in       string
		lo, hi   int // -1: unset
		inRange  []int
		outRange []int
	}{
		{"0", 0, 0, []int{0}, []int{1}},
		{"{min: 1, max: 2}", 1, 2, []int{1, 2}, []int{0, 3}},
		{"{max: 2}", -1, 2, []int{0, 2}, []int{3}},
		{"{min: 1}", 1, -1, []int{1, 9}, []int{0}},
	} {
		sc, err := ParseScenario([]byte(strings.Replace(good, "%s", tc.in, 1)))
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		r := sc.Expect.GuardRefusals
		if r == nil || (tc.lo >= 0) != (r.Min != nil) || (tc.hi >= 0) != (r.Max != nil) {
			t.Fatalf("%s: parsed %+v", tc.in, r)
		}
		for _, n := range tc.inRange {
			if !r.has(n) {
				t.Errorf("%s: %d is outside %s", tc.in, n, r)
			}
		}
		for _, n := range tc.outRange {
			if r.has(n) {
				t.Errorf("%s: %d is inside %s", tc.in, n, r)
			}
		}
		if sc.Expect.MaxUnbackedRuleOuts == nil || *sc.Expect.MaxUnbackedRuleOuts != 0 {
			t.Errorf("max_unbacked_rule_outs = %v, want a set 0", sc.Expect.MaxUnbackedRuleOuts)
		}
		if w := sc.WatchStatus; w == nil || len(w.Signals) != 1 || w.Signals[0].ClearingSince != "-4m" || w.CheckedAt != "-30s" {
			t.Errorf("watch_status = %+v", w)
		}
	}
	for _, bad := range []string{"{}", "{minimum: 1}", "[1, 2]", "many"} {
		if _, err := ParseScenario([]byte(strings.Replace(good, "%s", bad, 1))); err == nil {
			t.Errorf("guard_refusals %s was accepted", bad)
		}
	}
	// An unknown field of the row is an error, like everywhere in a scenario.
	typo := strings.Replace(good, "clearing_since", "clearing_sinse", 1)
	if _, err := ParseScenario([]byte(strings.Replace(typo, "%s", "0", 1))); err == nil {
		t.Error("a typo in a watch_status signal was accepted")
	}
}

// TestWatchStatusRow: the offsets are taken from the start of the run, so the
// row is fresh whenever the run happens.
func TestWatchStatusRow(t *testing.T) {
	spec := &WatchStatusSpec{CheckedAt: "-20s", Signals: []WatchSignalSpec{
		{Signal: "connector_down", State: "firing", Severity: "sev2", Node: "connect:c", Since: "-9m", ClearingSince: "-4m"},
	}}
	start := time.Date(2026, 10, 25, 9, 40, 0, 0, time.UTC)
	st, err := spec.status("orders", start)
	if err != nil {
		t.Fatal(err)
	}
	g := st.Signals[0]
	if st.PipelineID != "orders" || !st.CheckedAt.Equal(start.Add(-20*time.Second)) || !g.Since.Equal(start.Add(-9*time.Minute)) ||
		g.ClearingSince == nil || !g.ClearingSince.Equal(start.Add(-4*time.Minute)) || g.Node != "connect:c" {
		t.Errorf("row = %+v", st)
	}
	empty, err := (&WatchStatusSpec{}).status("orders", start)
	if err != nil || empty.Signals == nil || len(empty.Signals) != 0 || !empty.CheckedAt.Equal(start) {
		t.Errorf("empty row = %+v, %v", empty, err)
	}
}

// capture is a Runner that records what the room hands the agent and ends.
type capture struct {
	mu    sync.Mutex
	opts  agent.Options
	rows  []clients.WatchStatus
	watch bool
}

func (c *capture) run(ctx context.Context, alert agent.Alert, d agent.Deps, o agent.Options) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opts, c.watch = o, d.Watch != nil
	if d.Watch != nil {
		rows, err := d.Watch.WatchStatuses(ctx, []string{alert.PipelineID})
		if err != nil {
			return err
		}
		c.rows = rows
	}
	return nil
}

// TestGuardModeAndWatchReachTheAgent: the room gets the scenario's status row
// as Deps.Watch (and no reader for a scenario without the field), and the
// guard mode of the run's Options reaches the agent.
func TestGuardModeAndWatchReachTheAgent(t *testing.T) {
	for _, tc := range []struct {
		scenario string
		mode     string
		wantRow  bool
	}{
		{"scenarios/long-tx-slot-stuck-watcher-firing.yaml", agent.GuardEnforce, true},
		{"scenarios/schema-change.yaml", agent.GuardOff, false},
	} {
		c := &capture{}
		var tr Transcript
		_, err := Evaluate(context.Background(), []Scenario{loadScenario(t, tc.scenario)}, Config{
			Runner: c.run, Logger: quietLogger(), Options: agent.Options{HypothesisGuard: tc.mode},
			OnRun: func(_ RunResult, got Transcript) { tr = got },
		})
		if err != nil {
			t.Fatal(err)
		}
		if c.opts.HypothesisGuard != tc.mode || tr.GuardMode != tc.mode {
			t.Errorf("%s: guard mode in options %q, in the transcript %q, want %q", tc.scenario, c.opts.HypothesisGuard, tr.GuardMode, tc.mode)
		}
		if c.watch != tc.wantRow {
			t.Errorf("%s: Deps.Watch set = %v, want %v", tc.scenario, c.watch, tc.wantRow)
		}
		if tc.wantRow {
			if len(c.rows) != 1 || c.rows[0].PipelineID != "orders" || len(c.rows[0].Signals) != 2 ||
				c.rows[0].Signals[0].Signal != "long_tx" || c.rows[0].Signals[0].ClearingSince != nil {
				t.Errorf("rows = %+v", c.rows)
			}
			if age := time.Since(c.rows[0].CheckedAt); age < 0 || age > 2*time.Minute {
				t.Errorf("the row is %s old, want about 20 s", age)
			}
		}
	}
}

// guardResult is a hand-built run result for the grading tests.
func guardResult(mode string, refusals, unbacked int) RunResult {
	return RunResult{GuardMode: mode, GuardRefusals: refusals, UnbackedRuleOuts: unbacked, UnbackedRuledOut: make([]string, unbacked)}
}

// TestGuardFailures: the refusal bound is graded in enforce mode only; the
// rule-out bound in every mode.
func TestGuardFailures(t *testing.T) {
	lo, hi, zero := 1, 2, 0
	sc := Scenario{Expect: Expect{GuardRefusals: &IntRange{Min: &lo, Max: &hi}, MaxUnbackedRuleOuts: &zero}}
	for _, tc := range []struct {
		name string
		res  RunResult
		want string // "" = none
	}{
		{"in range", guardResult(agent.GuardEnforce, 1, 0), ""},
		{"top of range", guardResult(agent.GuardEnforce, 2, 0), ""},
		{"below the minimum", guardResult(agent.GuardEnforce, 0, 0), "refused 0 call(s), want 1 to 2"},
		{"above the maximum", guardResult(agent.GuardEnforce, 3, 0), "refused 3 call(s), want 1 to 2"},
		{"log mode skips the refusal bound", guardResult(agent.GuardLog, 0, 0), ""},
		{"unset mode is log", guardResult("", 0, 0), ""},
		{"off mode skips it", guardResult(agent.GuardOff, 0, 0), ""},
		{"unbacked rule-out", guardResult(agent.GuardEnforce, 1, 1), "1 rule-out claim(s) no cited read backs (max 0)"},
		{"unbacked rule-out in log mode", guardResult(agent.GuardLog, 0, 2), "2 rule-out claim(s)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := guardFailures(sc, tc.res)
			switch {
			case tc.want == "" && len(got) != 0:
				t.Errorf("failures = %v, want none", got)
			case tc.want != "" && !strings.Contains(strings.Join(got, "|"), tc.want):
				t.Errorf("failures = %v, want %q", got, tc.want)
			}
		})
	}
	if got := guardFailures(Scenario{}, guardResult(agent.GuardEnforce, 5, 5)); len(got) != 0 {
		t.Errorf("a scenario without bounds fails: %v", got)
	}
	if got := (&IntRange{Max: &hi}).String(); got != "at most 2" {
		t.Errorf("String = %q", got)
	}
}

// TestJudgeCountsGuardRefusalsAndUnbackedRuleOuts: the counts come from the
// room log, so a refusal is any error result that carries the guard's marker
// and a rule-out is backed only by a cited successful read.
func TestJudgeCountsGuardRefusalsAndUnbackedRuleOuts(t *testing.T) {
	sc := loadScenario(t, "scenarios/publication-owner-ruled-out-trap.yaml")
	cause := "Task 0 FAILED: Debezium cannot ALTER PUBLICATION dbz_publication because orders_owner, not piperoom, owns public.orders (permission denied; pg_recent_ddl shows the OWNER TO). "
	read := []step{
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "pg_recent_ddl", input: map[string]any{}},
	}
	// "Ruled out: publication" with only the status and DDL reads cited.
	r, _ := evalOnce(t, sc, append(slices.Clone(read), step{hypothesis: cause + "Ruled out: publication."}))
	if r.UnbackedRuleOuts != 1 || len(r.UnbackedRuledOut) != 1 || r.UnbackedRuledOut[0] != "publication" {
		t.Fatalf("unbacked = %d %v", r.UnbackedRuleOuts, r.UnbackedRuledOut)
	}
	if r.Pass || !hasFailure(r, "rule-out claim(s) no cited read backs (max 0)") {
		t.Errorf("pass=%v failures=%v", r.Pass, r.Failures)
	}
	// The same claim after a cited pg_publication_tables read stands.
	withRead := append(slices.Clone(read), step{tool: "pg_publication_tables", input: map[string]any{"pipeline_id": "orders"}},
		step{hypothesis: cause + "Ruled out: publication."})
	r, _ = evalOnce(t, sc, withRead)
	if r.UnbackedRuleOuts != 0 || hasFailure(r, "rule-out") {
		t.Errorf("a backed claim counts: unbacked=%d failures=%v", r.UnbackedRuleOuts, r.Failures)
	}
	// Read but not cited: it does not back the claim.
	notCited := append(slices.Clone(read), step{tool: "pg_publication_tables", input: map[string]any{"pipeline_id": "orders"}},
		step{hypothesis: cause + "Ruled out: publication.", cite: []string{"s_0", "s_1"}})
	r, _ = evalOnce(t, sc, notCited)
	if r.UnbackedRuleOuts != 1 {
		t.Errorf("an uncited read backed the claim: unbacked=%d", r.UnbackedRuleOuts)
	}

	// The refusals of a run come from the transcript (the guard's log record,
	// see TestGuardRefusalsAreCountedFromTheLog).
	res := Judge(sc, Transcript{GuardMode: agent.GuardEnforce, GuardRefusals: 2}, nil, false)
	if res.GuardRefusals != 2 || res.GuardMode != agent.GuardEnforce {
		t.Errorf("guard refusals = %d mode %q, want 2 in enforce", res.GuardRefusals, res.GuardMode)
	}
}

// TestGuardRefusalsAreCountedFromTheLog: the counting handler counts the
// guard's refusal record whatever level the run's logger shows, passes every
// record the logger wants and nothing else, and does not count "would refuse".
func TestGuardRefusalsAreCountedFromTheLog(t *testing.T) {
	var out strings.Builder
	var c guardCounter
	base := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelError}))
	log := slog.New(countingHandler{next: base.Handler(), c: &c}).With("scenario", "x").WithGroup("g")
	log.Warn(guardRefusedMessage, "rule", "symptom_persists")
	log.Warn(guardRefusedMessage, "rule", "unbacked_rule_out")
	log.Warn("agent: hypothesis guard would refuse", "rule", "symptom_persists")
	log.Warn("agent: hypothesis guard accepted at low confidence after refusals")
	log.Error("something else")
	if got := c.n.Load(); got != 2 {
		t.Errorf("counted %d refusals, want 2", got)
	}
	if text := out.String(); strings.Contains(text, "guard") || !strings.Contains(text, "something else") || !strings.Contains(text, "scenario=x") {
		t.Errorf("the underlying logger saw %q: warnings must stay below its level, errors pass with their attributes", text)
	}
}

// guardModel runs sc once on a fake model in guard mode and returns the result
// and the room log.
func guardModel(t *testing.T, sc Scenario, mode string, turns []fakeTurn) (RunResult, Transcript) {
	r, tr, _ := guardModelBodies(t, sc, mode, turns)
	return r, tr
}

// guardModelBodies is guardModel that also returns the requests the model got.
func guardModelBodies(t *testing.T, sc Scenario, mode string, turns []fakeTurn) (RunResult, Transcript, []string) {
	t.Helper()
	fake := newFakeModel(t, turns)
	cfg := endpointConfig(t, fake)
	cfg.Options.HypothesisGuard = mode
	var tr Transcript
	cfg.OnRun = func(_ RunResult, got Transcript) { tr = got }
	rep, err := Evaluate(context.Background(), []Scenario{sc}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Results) != 1 {
		t.Fatalf("results = %d", len(rep.Results))
	}
	return rep.Results[0], tr, fake.bodies()
}

// hypothesesOf returns the agent's hypotheses and guard notes of a room log.
func hypothesesOf(tr Transcript) (hyps []events.AgentHypothesisPayload, notes []string) {
	for _, e := range tr.Events {
		switch e.Type {
		case events.AgentHypothesis:
			var p events.AgentHypothesisPayload
			if json.Unmarshal(e.Payload, &p) == nil {
				hyps = append(hyps, p)
			}
		case events.AgentMessage:
			var p events.AgentMessagePayload
			if json.Unmarshal(e.Payload, &p) == nil && strings.Contains(p.Text, "[guard]") {
				notes = append(notes, p.Text)
			}
		}
	}
	return hyps, notes
}

const flatHealthy = "Healthy now, nothing to fix."

func healthyTurn(text, confidence string) fakeTurn {
	return fakeTurn{tool: "update_hypothesis", input: map[string]any{"text": text, "confidence": confidence}, cite: true}
}

// The deterministic replay of the live room: a scripted model says "Healthy
// now, nothing to fix" while the watcher row fires long_tx and slot_wal.
// Enforce refuses it twice and accepts the third try at low confidence with a
// [guard] note; log and off let it through at the confidence the model gave.
func TestGuardRefusesFlatHealthyWhileWatcherFires(t *testing.T) {
	sc := loadScenario(t, "scenarios/long-tx-slot-stuck-watcher-firing.yaml")
	turns := []fakeTurn{
		{tool: "pg_replication_slots", input: map[string]any{"why": "the slot the watcher names"}},
		healthyTurn(flatHealthy, "medium"),
		healthyTurn(flatHealthy, "medium"),
		healthyTurn(flatHealthy, "medium"),
		{text: "Nothing more to do."},
	}

	t.Run("enforce", func(t *testing.T) {
		r, tr, bodies := guardModelBodies(t, sc, agent.GuardEnforce, turns)
		if r.GuardRefusals != 2 || r.GuardMode != agent.GuardEnforce {
			t.Fatalf("guard refusals = %d mode %q, want 2 in enforce (failures %v)", r.GuardRefusals, r.GuardMode, r.Failures)
		}
		hyps, notes := hypothesesOf(tr)
		if len(hyps) != 1 || hyps[0].Text != flatHealthy || hyps[0].Confidence != "low" {
			t.Errorf("hypotheses = %+v, want one accepted at low confidence", hyps)
		}
		if len(notes) != 1 || !strings.Contains(notes[0], "long_tx") {
			t.Errorf("guard notes = %q, want one naming the signal", notes)
		}
		// The accepted text is still the wrong story: the scenario fails it.
		if r.Pass || r.RootCause {
			t.Errorf("a flat healthy verdict passes: %v", r.Failures)
		}
		// The refusal reaches the model as the result of its call.
		var seen int
		for _, b := range bodies {
			if strings.Contains(b, "hypothesis guard (symptom_persists)") && strings.Contains(b, "long_tx") {
				seen++
			}
		}
		if seen < 2 {
			t.Errorf("the refusal text is in %d of the model's requests, want it in the ones after a refusal", seen)
		}
	})

	for _, mode := range []string{agent.GuardLog, agent.GuardOff, ""} {
		t.Run("mode "+mode, func(t *testing.T) {
			r, tr := guardModel(t, sc, mode, turns[:2])
			if r.GuardRefusals != 0 {
				t.Errorf("guard refusals = %d, want 0", r.GuardRefusals)
			}
			hyps, notes := hypothesesOf(tr)
			if len(hyps) != 1 || hyps[0].Text != flatHealthy || hyps[0].Confidence != "medium" || len(notes) != 0 {
				t.Errorf("hypotheses = %+v notes %q, want the text accepted as given and no note", hyps, notes)
			}
			if hasFailure(r, "hypothesis guard refused") {
				t.Errorf("a guard bound was graded outside enforce: %v", r.Failures)
			}
		})
	}
}

// A healthy verdict passes when the watcher has nothing firing, and when the
// signal is already clearing; the lagging row refuses the first flat verdict
// and accepts the "recovering" wording the refusal asks for.
func TestGuardPassPathsAndTheRecoveringWording(t *testing.T) {
	healthy := "The pipeline is healthy; nothing to fix."
	recovering := "The connector is recovering: task 0 is RUNNING again and the slot is active, but the watcher has not cleared connector_down yet, so keep the room open until it does."

	t.Run("clean row", func(t *testing.T) {
		sc := loadScenario(t, "scenarios/healthy-after-recovery.yaml")
		r, tr := guardModel(t, sc, agent.GuardEnforce, []fakeTurn{
			{tool: "kafka_consumer_lag", input: map[string]any{"group": "order-sync"}},
			healthyTurn(healthy, "medium"),
			{text: "The pipeline is healthy; nothing to fix."},
		})
		hyps, notes := hypothesesOf(tr)
		if !r.Pass || r.GuardRefusals != 0 || len(hyps) != 1 || hyps[0].Confidence != "medium" || len(notes) != 0 {
			t.Errorf("pass=%v refusals=%d hyps=%+v notes=%q failures=%v", r.Pass, r.GuardRefusals, hyps, notes, r.Failures)
		}
	})

	t.Run("clearing row", func(t *testing.T) {
		sc := loadScenario(t, "scenarios/healthy-self-recovered-watcher-clearing.yaml")
		r, _ := guardModel(t, sc, agent.GuardEnforce, []fakeTurn{
			{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
			healthyTurn(healthy, "medium"),
			{text: "The pipeline is healthy; nothing to fix."},
		})
		if !r.Pass || r.GuardRefusals != 0 {
			t.Errorf("pass=%v refusals=%d failures=%v", r.Pass, r.GuardRefusals, r.Failures)
		}
	})

	t.Run("lagging row, refused once then recovering", func(t *testing.T) {
		sc := loadScenario(t, "scenarios/healthy-self-recovered-watcher-lagging.yaml")
		r, tr := guardModel(t, sc, agent.GuardEnforce, []fakeTurn{
			{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
			healthyTurn(healthy, "medium"),
			healthyTurn(recovering, "low"),
			{text: "Recovering; the watcher has not cleared yet."},
		})
		hyps, _ := hypothesesOf(tr)
		if !r.Pass || r.GuardRefusals != 1 || len(hyps) != 1 || hyps[0].Text != recovering {
			t.Errorf("pass=%v refusals=%d hyps=%+v failures=%v", r.Pass, r.GuardRefusals, hyps, r.Failures)
		}
	})

	t.Run("lagging row, refused twice then accepted at low", func(t *testing.T) {
		sc := loadScenario(t, "scenarios/healthy-self-recovered-watcher-lagging.yaml")
		r, tr := guardModel(t, sc, agent.GuardEnforce, []fakeTurn{
			{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
			healthyTurn(healthy, "medium"), healthyTurn(healthy, "medium"), healthyTurn(healthy, "medium"),
			{text: "Done."},
		})
		hyps, notes := hypothesesOf(tr)
		if !r.Pass || r.GuardRefusals != 2 || len(hyps) != 1 || hyps[0].Confidence != "low" || len(notes) != 1 {
			t.Errorf("pass=%v refusals=%d hyps=%+v notes=%q failures=%v", r.Pass, r.GuardRefusals, hyps, notes, r.Failures)
		}
	})

	t.Run("lagging row, never claims healthy", func(t *testing.T) {
		// Zero refusals is outside 1 to 2: the scenario is about the verdict
		// after a refusal, and says so.
		sc := loadScenario(t, "scenarios/healthy-self-recovered-watcher-lagging.yaml")
		r, _ := guardModel(t, sc, agent.GuardEnforce, []fakeTurn{
			{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
			healthyTurn(recovering, "low"),
			{text: "Recovering; the watcher has not cleared yet."},
		})
		if r.Pass || !hasFailure(r, "hypothesis guard refused 0 call(s), want 1 to 2") {
			t.Errorf("pass=%v failures=%v", r.Pass, r.Failures)
		}
	})
}
