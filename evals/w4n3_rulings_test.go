package evals

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

// W4n-3 (KAN-497) applies the leads' rulings R14-R18
// (tasks/W4-evals-to-100.md) to the scenarios. These tests pin each applied
// ruling, so a later edit that undoes one fails here and not only in a paid
// live run. R14's label row is in TestW4nLayerLabels, R18's budget in
// TestW4nBudgets.

// TestW4n3OffsetsFirstFailingConsumer (R14): order-sync is the first failing
// stage, Kafka the root cause, and nothing in the ground truth the judge
// reads still says otherwise.
func TestW4n3OffsetsFirstFailingConsumer(t *testing.T) {
	sc := w4nScenario(t, "consumer-offsets-out-of-range-retention")
	if len(sc.Expect.RootCauseLayerAlt) != 0 {
		t.Errorf("root_cause_layer_alt = %v, want none", sc.Expect.RootCauseLayerAlt)
	}
	if strings.Contains(sc.Description, "the topic are healthy") || !strings.Contains(sc.Description, "the topic answers every read") ||
		!strings.Contains(sc.Description, "The first component that fails is order-sync") {
		t.Errorf("description does not state the R14 reading:\n%s", sc.Description)
	}
	p := judgePrompt(sc, "")
	for _, want := range []string{"First failing layer: consumer (", "Root-cause layer: kafka\n"} {
		if !strings.Contains(p, want) {
			t.Errorf("judge prompt lacks %q:\n%s", want, p)
		}
	}
	if strings.Contains(p, "alternatively") {
		t.Errorf("judge prompt still offers an alternative root-cause layer:\n%s", p)
	}
	// The run the audit read: the agent named order-sync's crash loop as
	// the failure and retention as the cause. That now passes both layers.
	h := "order-sync is crash-looping on OffsetOutOfRangeException: its committed offsets on pg.public.orders are below the log start. " +
		"Root cause: the topic's 48 h retention deleted the change records while order-sync was scaled to zero for 87 hours, so they were lost before it read them."
	g := GradeKeywords(sc, h)
	if !g.Ambiguity.KeywordRootCause || !g.Ambiguity.KeywordLayer || !g.Ambiguity.KeywordRootCauseLayer {
		t.Errorf("crash-loop hypothesis: %+v, want root cause, layer and root-cause layer", g.Ambiguity)
	}
	// "The consumer was down" alone still misses the root cause.
	if rootCauseMatches(sc.Expect, "order-sync was down for 87 hours and crash-loops on OffsetOutOfRangeException.") {
		t.Error("a consumer-only story passes the root cause")
	}
}

// TestW4n3DuplicatesNoFirstFailing (R15): the healthy-now verdict with a past
// finding grades no current first failing layer; the root-cause layer and
// its Connect alternative stay, and the redelivery wording passes.
func TestW4n3DuplicatesNoFirstFailing(t *testing.T) {
	sc := w4nScenario(t, "consumer-duplicates")
	e := sc.Expect
	if e.FirstFailingLayer != "" || e.RootCauseLayer != "kafka" || !slices.Equal(e.RootCauseLayerAlt, []string{"kafka-connect"}) {
		t.Fatalf("first %q root %q alt %v, want none, kafka, [kafka-connect]", e.FirstFailingLayer, e.RootCauseLayer, e.RootCauseLayerAlt)
	}
	if p := judgePrompt(sc, ""); strings.Contains(p, "First failing layer:") {
		t.Errorf("judge prompt still names a first failing layer:\n%s", p)
	}
	if label := syntheticLabels[sc.Name]; label.Layer != e.RootCauseLayer {
		t.Errorf("syntheticLabels layer %q, want the root-cause layer %q", label.Layer, e.RootCauseLayer)
	}
	tests := []struct {
		id, text string
		want     bool
	}{
		{"re-sent at-least-once", "Healthy now. pg-orders-connector re-sent 10 change events of pg.public.orders at-least-once; the upsert sink absorbed them.", true},
		{"resent same key", "The 10 events were resent with the same key; order-sync upserts by id, so analytics.orders is right.", true},
		{"sent again", "Ten change events were sent again to pg.public.orders and the sink absorbed the copies.", true},
		{"at least once", "An at least once redelivery put 10 events on pg.public.orders twice; the sink is right.", true},
		{"same lsn group 1", "The topic holds 10 events with the same lsn as earlier records; nothing to fix.", true},
		{"deduplicated", "The sink deduplicated 10 events that arrived again with the same key.", true},
		{"inserted twice", "The application inserted twice: the 10 orders were written again with the same key.", false},
		{"wrote twice", "The application wrote 10 orders twice.", false},
	}
	for _, tt := range tests {
		if got := rootCauseMatches(e, tt.text); got != tt.want {
			t.Errorf("%s: rootCauseMatches = %v, want %v: %q", tt.id, got, tt.want, tt.text)
		}
	}
	// The alert and the steer, alone or together, must not pass.
	handed := append([]string{sc.Alert.Title}, inputTexts(sc)...)
	for _, in := range append(handed, strings.Join(handed, " ")) {
		if rootCauseMatches(e, in) {
			t.Errorf("handed text passes the root cause: %q", in)
		}
	}
}

// r17Case is one scenario whose fix rewrites data (ruling R17).
type r17Case struct {
	file, start, action string
	inserts             string // warehouse_load_errors after the fix
	extraTools          []string
}

var r17Cases = []r17Case{
	{"consumer-skipped-range", "skipped", "kafka_replay_range", "1341 rows inserted", nil},
	{"rows-lost-after-retention", "lost", "pg_resnapshot_rows", "1351 rows inserted", []string{"kafka_find_by_key", "kafka_topic_offsets"}},
}

// TestW4n3ReplayedPhaseShape (R17): the fix moves the run to phase replayed
// through a transition without ok: true, so there is no recovered phase, and
// the replayed recordings show the restored rows.
func TestW4n3ReplayedPhaseShape(t *testing.T) {
	for _, c := range r17Cases {
		t.Run(c.file, func(t *testing.T) {
			sc := w4nScenario(t, c.file)
			if !slices.Equal(sc.Phases, []string{c.start, "replayed"}) {
				t.Fatalf("phases = %v", sc.Phases)
			}
			if len(sc.Transitions) != 1 {
				t.Fatalf("transitions = %+v, want one", sc.Transitions)
			}
			tr := sc.Transitions[0]
			if tr.On.Action != c.action || tr.On.OK != nil || tr.To != "replayed" || tr.From != "" {
				t.Errorf("transition = %+v, want on %s with no ok, to replayed", tr, c.action)
			}
			if rp := sc.recoveredPhases(); len(rp) != 0 {
				t.Errorf("recovered phases = %v, want none (a recovered phase would make verify_score judge X2c's verdict)", rp)
			}
			for _, row := range sc.Expect.RootCauseForbidden {
				if len(row) == 1 && (row[0] == "pipeline is healthy" || row[0] == "nothing is wrong") {
					t.Errorf("root_cause_forbidden still holds %q", row[0])
				}
			}
			for _, keep := range []string{"false alarm", "false positive"} {
				if !slices.ContainsFunc(sc.Expect.RootCauseForbidden, func(r []string) bool { return len(r) == 1 && r[0] == keep }) {
					t.Errorf("root_cause_forbidden lost %q", keep)
				}
			}
			byTool := map[string][]Recording{}
			for _, r := range sc.Recordings {
				if r.Phase == "replayed" {
					byTool[r.Tool] = append(byTool[r.Tool], r)
				} else if r.Phase != "" {
					t.Errorf("recording %s in phase %q", r.Tool, r.Phase)
				}
			}
			rec := byTool["reconcile"]
			if len(rec) != 1 || len(rec[0].Sequence) != 2 {
				t.Fatalf("replayed reconcile = %+v, want one recording with both windows", rec)
			}
			for i, o := range rec[0].Sequence {
				for _, want := range []string{"verdict: the sink matches the source in every bucket", "; missing 0;", "(no drift)"} {
					if !strings.Contains(o.Summary, want) {
						t.Errorf("reconcile[%d] lacks %q:\n%s", i, want, o.Summary)
					}
				}
				if strings.Contains(o.Summary, "| -") {
					t.Errorf("reconcile[%d] still has a negative bucket:\n%s", i, o.Summary)
				}
			}
			seen := map[string]bool{}
			for _, r := range byTool["warehouse_row"] {
				if !strings.HasPrefix(r.Summary, "found: yes") {
					t.Errorf("warehouse_row %v: %q", r.Input["key"], r.Summary)
				}
				seen["wh"+fmt.Sprint(r.Input["key"])] = true
			}
			for _, r := range byTool["trace_record"] {
				if !strings.Contains(r.Summary, "warehouse analytics.orders: seen") || strings.Contains(r.Summary, "stopped at") {
					t.Errorf("trace_record %v:\n%s", r.Input["key"], r.Summary)
				}
				seen["tr"+fmt.Sprint(r.Input["key"])] = true
			}
			for id := 1301; id <= 1340; id++ {
				if !seen[fmt.Sprintf("wh%d", id)] || !seen[fmt.Sprintf("tr%d", id)] {
					t.Errorf("id %d: replayed warehouse_row or trace_record missing", id)
				}
			}
			if lag := byTool["kafka_consumer_lag"]; len(lag) != 1 || !strings.Contains(lag[0].Summary, "order-sync: Stable, 1 members, total lag 0") {
				t.Errorf("replayed kafka_consumer_lag = %+v", lag)
			}
			if le := byTool["warehouse_load_errors"]; len(le) != 1 || !strings.Contains(le[0].Summary, c.inserts) {
				t.Errorf("replayed warehouse_load_errors = %+v, want %q", le, c.inserts)
			}
			for _, tool := range c.extraTools {
				if len(byTool[tool]) == 0 {
					t.Errorf("no replayed %s", tool)
				}
			}
		})
	}
}

// resultTap records the tool results a scripted run sees.
type resultTap struct {
	agent.Sink
	mu  *sync.Mutex
	got *[]events.AgentToolResultPayload
}

func (s resultTap) Emit(ctx context.Context, typ events.Type, payload any) error {
	if p, ok := payload.(events.AgentToolResultPayload); ok && typ == events.AgentToolResult {
		s.mu.Lock()
		*s.got = append(*s.got, p)
		s.mu.Unlock()
	}
	return s.Sink.Emit(ctx, typ, payload)
}

// TestW4n3ReplayedReadsAfterFix (R17, end to end): the same reads answer the
// broken state before the executed fix and the restored state after it, and
// a final hypothesis that keeps the cause and calls the pipeline healthy
// after the recovery passes.
func TestW4n3ReplayedReadsAfterFix(t *testing.T) {
	for _, c := range recoveryCases(t) {
		t.Run(c.file, func(t *testing.T) {
			sc := w4nScenario(t, c.file)
			reconcile := map[string]any{"table": "public.orders", "since": "2026-10-22T08:40:00Z", "until": "2026-10-22T09:38:00Z"}
			post := []step{
				{tool: "reconcile", input: reconcile},
				{tool: "warehouse_row", input: map[string]any{"pipeline_id": "orders", "table": "analytics.orders", "pk_column": "id", "key": "1301"}},
				{tool: "trace_record", input: map[string]any{"pipeline_id": "orders", "table": "public.orders", "key": "1301"}},
				{tool: "kafka_consumer_lag", input: map[string]any{"group": "order-sync"}},
			}
			script := append(slices.Clone(c.reads), step{hypothesis: hypotheses[c.file].good}, step{proposeFix: c.fix})
			for range sc.Inputs {
				script = append(script, step{idle: true})
			}
			script = append(script, post...)
			script = append(script, step{hypothesis: hypotheses[c.file].good + " After the fix the pipeline is healthy: reconcile shows the sink matches the source and the 40 rows are in analytics.orders."}, step{idle: true})

			var mu sync.Mutex
			var got []events.AgentToolResultPayload
			inner := scriptedRunner(script)
			runner := func(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
				d.Sink = resultTap{Sink: d.Sink, mu: &mu, got: &got}
				return inner(ctx, a, d, o)
			}
			rep, err := Evaluate(context.Background(), []Scenario{sc}, Config{Runs: 1, Runner: runner, Logger: quietLogger()})
			if err != nil {
				t.Fatal(err)
			}
			r := rep.Results[0]
			if !r.Pass || r.Score != 100 {
				t.Fatalf("pass=%v score=%v failures=%v misses=%v", r.Pass, r.Score, r.Failures, r.Misses)
			}
			if !slices.Equal(r.PhasesVisited, sc.Phases) {
				t.Errorf("phases visited = %v, want %v", r.PhasesVisited, sc.Phases)
			}
			if r.ClaimedRecoveryWithoutCheck {
				t.Error("claimed recovery without check: the replayed phase must not be a recovered one")
			}
			summaries := func(tool string) []string {
				var out []string
				for _, p := range got {
					if p.Tool == tool {
						out = append(out, p.Summary)
					}
				}
				return out
			}
			rec := summaries("reconcile")
			if len(rec) != 2 || !strings.Contains(rec[0], "sink is missing 40 rows") || !strings.Contains(rec[1], "the sink matches the source in every bucket") {
				t.Errorf("reconcile before/after the fix:\n%s", strings.Join(rec, "\n---\n"))
			}
			if wh := summaries("warehouse_row"); len(wh) != 1 || !strings.HasPrefix(wh[0], "found: yes") {
				t.Errorf("warehouse_row after the fix = %q", wh)
			}
			tr := summaries("trace_record")
			if last := tr[len(tr)-1]; !strings.Contains(last, "warehouse analytics.orders: seen") {
				t.Errorf("trace_record after the fix:\n%s", last)
			}
		})
	}
}
