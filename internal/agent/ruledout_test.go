package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

func claimItems(text string) []string {
	var out []string
	for _, c := range RuledOutClaims(text) {
		out = append(out, c.Item)
	}
	return out
}

// Every claim form of the design (§B) is read, and every non-claim is left
// alone. The word lists are pinned here: keep them in sync with the eval
// grader's denial list by review (evals/textmatch.go).
func TestRuledOutClaimForms(t *testing.T) {
	cases := []struct {
		name, text string
		want       []string
	}{
		{"label list", "Ruled out: role/grants, publication, Connect config validation, long transactions",
			[]string{"role/grants", "publication", "connect config validation", "long transactions"}},
		{"label excluded with semicolons", "Excluded: the slot; the DLQ.", []string{"slot", "dlq"}},
		{"label list ends at a statement", "Ruled out: the slot; the cause is consumer lag.", []string{"slot"}},
		{"label not the cause", "Not the cause: schema registry", []string{"schema registry"}},
		{"label with words before colon", "Ruled out so far: broker", []string{"broker"}},
		{"label after a sentence", "Leading suspect: the consumer. Ruled out: TOAST", []string{"toast"}},
		{"verb with by", "We ruled out the publication by reading its tables.", []string{"publication"}},
		{"verb excluding", "Excluding the replication slot, the cause is downstream.", []string{"replication slot"}},
		{"verb object ends at and", "Eliminated broker health and the registry", []string{"broker health"}},
		{"predicate not the cause", "The slot is not the cause.", []string{"slot"}},
		{"predicate ruled out", "The publication was ruled out (s_01JB).", []string{"publication"}},
		{"predicate has been", "The slot has been ruled out.", []string{"slot"}},
		{"predicate looks fine", "Schema registry looks fine.", []string{"schema registry"}},
		{"predicate after but", "Lag grows, but the slot is healthy.", []string{"slot"}},
		{"predicate after an earlier verb", "The consumer is stuck and the slot is fine.", []string{"slot"}},
		{"predicate list", "Source, slot and connector are healthy.", []string{"slot", "connector"}},
		{"no-form", "No long transactions.", []string{"no long transactions"}},
		{"no-form with adjective", "There is no recent schema change", []string{"no recent schema change"}},
		{"no-form dlq", "No DLQ records", []string{"no dlq"}},
		{"no-form sessions", "no blocking sessions on the source", []string{"no blocking sessions"}},

		{"cannot rule out", "Cannot rule out the slot.", nil},
		{"plan", "To rule out the registry, read schema_registry_diff.", nil},
		{"not yet ruled out", "The publication is not yet ruled out.", nil},
		{"has not been ruled out", "The slot has not been ruled out.", nil},
		{"negated label", "Not yet ruled out: publication", nil},
		{"would", "This would rule out the slot.", nil},
		{"if", "If the slot is fine, the connector is the cause.", nil},
		{"still to check", "Still to check: slot, publication", nil},
		{"not checked", "Not checked: publication", nil},
		{"past room", "A past room ruled out the publication.", nil},
		{"similar incident", "In a similar incident the slot was fine.", nil},
		{"no evidence of", "No evidence of long transactions.", nil},
		{"no ... yet", "No DDL yet.", nil},
		{"question", "Is the slot healthy?", nil},
		{"evidence 3 sentence", "Leading suspect is the 21:52 DDL — no consumer-side evidence yet.", nil},
		{"no-form on a topic without it", "No consumer lag.", nil},
		{"unknown topic", "Ruled out: the moon, the pipeline", nil},
		{"pipeline healthy", "The pipeline is healthy.", nil},
		{"hedge", "The slot looks unlikely to matter.", nil},
		{"state not list", "The connector is running fine.", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := claimItems(tc.text); !slices.Equal(got, tc.want) {
				t.Fatalf("claims of %q = %q, want %q", tc.text, got, tc.want)
			}
		})
	}
}

// An item naming several specific topics is backed by a read of any; a
// generic topic (connector state) counts only when nothing specific matched.
func TestRuledOutTopics(t *testing.T) {
	c := RuledOutClaims("Ruled out: the connector role's grants")[0]
	if !slices.Equal(c.Topics, []string{"role/grants"}) {
		t.Fatalf("topics = %v, want role/grants only (connector is generic)", c.Topics)
	}
	c = RuledOutClaims("Ruled out: slot/publication")[0]
	if !c.BackedBy("pg_replication_slots") || !c.BackedBy("pg_publication_tables") {
		t.Fatalf("multi-topic item %+v not backed by either topic's read", c)
	}
	c = RuledOutClaims("Ruled out: the connector")[0]
	if !c.BackedBy("connect_status") {
		t.Fatalf("generic-only item %+v not backed by connect_status", c)
	}
	c = RuledOutClaims("Ruled out: WAL retention")[0]
	if c.BackedBy("kafka_topic_config") || !c.BackedBy("pg_replication_slots") {
		t.Fatalf("WAL retention %+v must be a slot claim, not a topic-retention one", c)
	}
}

// Every tool the table names is a read tool registered under internal/tools
// (design: "every tool name verified"). A rename there fails here.
func TestRuleOutToolsExist(t *testing.T) {
	var src strings.Builder
	err := filepath.WalkDir(filepath.Join("..", "tools"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		src.Write(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range ruleOutTopics {
		for _, name := range topic.tools {
			if !strings.Contains(src.String(), `"`+name+`"`) {
				t.Errorf("topic %s names %s, which no file under internal/tools registers", topic.name, name)
			}
		}
	}
}

// evidence2 is the connector-auth hypothesis of the design: it rules out four
// things and cites a connector status and a DDL read only.
const evidence2 = "Connector task 0 fails; leading suspect the connector user. Ruled out: role/grants, publication, Connect config validation, long transactions."

// evidence3 is the consumer-stuck verdict of run 3: a hedge on the consumer
// and a predicate on source, slot and connector.
const evidence3 = "First failing: consumer order-sync (Empty, 0 members, committed offsets frozen while lag grew 358→474). Root cause: not established; leading suspect is the 21:52 amount int→numeric(12,2) DDL that produced value schema v2 (amount int→bytes decimal), which the order-sync consumer may fail to decode — no consumer-side evidence yet. Source, slot and connector are healthy."

func steps(facts ...stepFact) []ledgerStep {
	out := make([]ledgerStep, len(facts))
	for i, f := range facts {
		out[i] = ledgerStep{id: "s_" + strings.Repeat("0", 25) + string(rune('A'+i)), fact: f}
	}
	return out
}

func TestCheckRuledOutEvidence2(t *testing.T) {
	cited := steps(stepFact{tool: "connect_status", summary: "task 0 FAILED"}, stepFact{tool: "pg_recent_ddl", summary: "no DDL"})
	f := checkRuledOut(evidence2, cited, cited, nil)
	if f == nil {
		t.Fatal("evidence 2 passed with no matching reads")
	}
	if !errors.Is(f.refusal, tools.ErrBadInput) {
		t.Fatalf("refusal %v is not ErrBadInput", f.refusal)
	}
	want := []string{"role/grants", "publication", "connect config validation", "long transactions"}
	if !slices.Equal(f.unbacked, want) {
		t.Fatalf("unbacked = %q, want %q (ownership is not claimed)", f.unbacked, want)
	}
	msg := f.refusal.Error()
	for _, w := range []string{"bad tool input: hypothesis guard (unbacked_rule_out): ", "`role/grants` (source_role_grants or pg_publication_tables)",
		"`connect config validation` (connect_config or connect_validate_config)", `write "not checked: <item>"`} {
		if !strings.Contains(msg, w) {
			t.Fatalf("refusal lacks %q:\n%s", w, msg)
		}
	}

	// The exported form the evals score with (C6 max_unbacked_rule_outs).
	if got := UnbackedRuledOut(evidence2, []string{"connect_status", "pg_recent_ddl"}); len(got) != len(want) {
		t.Fatalf("UnbackedRuledOut = %+v, want %d claims", got, len(want))
	}
	if got := UnbackedRuledOut(evidence2, []string{"source_role_grants", "pg_publication_tables", "connect_config", "source_blocking_sessions"}); len(got) != 0 {
		t.Fatalf("UnbackedRuledOut with every topic read = %+v", got)
	}

	backed := steps(stepFact{tool: "source_role_grants"}, stepFact{tool: "pg_publication_tables"},
		stepFact{tool: "connect_validate_config"}, stepFact{tool: "pg_long_transactions"})
	if f := checkRuledOut(evidence2, backed, backed, nil); f != nil {
		t.Fatalf("evidence 2 with every read cited refused: %v", f.refusal)
	}
}

func TestCheckRuledOutEvidence3(t *testing.T) {
	if got := claimItems("which the order-sync consumer may fail to decode — no consumer-side evidence yet."); got != nil {
		t.Fatalf("the consumer hedge is a claim: %q", got)
	}
	cited := steps(stepFact{tool: "pg_replication_slots"}, stepFact{tool: "connect_status"}, stepFact{tool: "kafka_consumer_lag"})
	if f := checkRuledOut(evidence3, cited, cited, nil); f != nil {
		t.Fatalf("evidence 3 with its status reads cited refused: %v", f.refusal)
	}
}

// A read that errored does not back a claim; a not-found answer does.
func TestCheckRuledOutErroredAndNotFound(t *testing.T) {
	text := "Ruled out: the publication"
	unreachable := steps(stepFact{tool: "pg_publication_tables", isError: true, summary: "error: unreachable: postgres (connection refused): dial"})
	if checkRuledOut(text, unreachable, unreachable, nil) == nil {
		t.Fatal("a claim backed only by an errored read passed")
	}
	notFound := steps(stepFact{tool: "pg_publication_tables", isError: true, summary: "error: not-found: postgres (not found): publication dbz"})
	if f := checkRuledOut(text, notFound, notFound, nil); f != nil {
		t.Fatalf("a not-found answer did not back the claim: %v", f.refusal)
	}
	other := steps(stepFact{tool: "pg_publication_tables", isError: true, summary: "error: bad input"})
	if checkRuledOut(text, other, other, nil) == nil {
		t.Fatal("an errored read with no fact backed the claim")
	}
	noLongTx := "No long transactions on the source."
	if checkRuledOut(noLongTx, nil, nil, nil) == nil {
		t.Fatal(`"no long transactions" with no read passed`)
	}
	if f := checkRuledOut(noLongTx, steps(stepFact{tool: "pg_long_transactions"}), nil, nil); f != nil {
		t.Fatalf(`"no long transactions" with a cited read refused: %v`, f.refusal)
	}
}

// The refusal names an uncited step that would back the claim, lists at most
// five items cut to 80 bytes, and names only offered tools.
func TestCheckRuledOutRefusalText(t *testing.T) {
	all := steps(stepFact{tool: "pg_long_transactions"})
	f := checkRuledOut("Ruled out: long transactions", nil, all, nil)
	if f == nil || !strings.Contains(f.refusal.Error(), "step "+all[0].id+" of this run read it: cite it") {
		t.Fatalf("refusal does not name the uncited step: %v", f)
	}
	offered := func(n string) bool { return n == "source_long_transactions" }
	f = checkRuledOut("Ruled out: long transactions", nil, nil, offered)
	if msg := f.refusal.Error(); !strings.Contains(msg, "(source_long_transactions)") || strings.Contains(msg, "pg_long_transactions") {
		t.Fatalf("refusal names tools the room does not offer: %s", msg)
	}
	long := "Ruled out: " + strings.Repeat("slot", 30) + " slot, publication, broker, toast, dlq, warehouse, registry"
	f = checkRuledOut(long, nil, nil, nil)
	if len(f.unbacked) != 7 || len(f.unbacked[0]) > maxUnbackedTermBytes {
		t.Fatalf("unbacked = %q", f.unbacked)
	}
	if msg := f.refusal.Error(); strings.Count(msg, "` (") != maxRuleOutItemsListed || !strings.Contains(msg, "and 2 more") {
		t.Fatalf("refusal lists more than %d items: %s", maxRuleOutItemsListed, msg)
	}
	// post_message has no citations: the hint says read, not cite.
	f = checkRuledOut("Ruled out: the slot", nil, nil, nil)
	if !strings.Contains(f.refusal.Error(), "no step of this run read") {
		t.Fatalf("post_message refusal: %v", f.refusal)
	}
}

func TestResolveGuardMode(t *testing.T) {
	for in, want := range map[string]string{"": GuardLog, GuardOff: GuardOff, GuardLog: GuardLog, GuardEnforce: GuardEnforce} {
		if got, err := resolveGuardMode(in); err != nil || got != want {
			t.Fatalf("resolveGuardMode(%q) = %q, %v", in, got, err)
		}
	}
	if _, err := resolveGuardMode("strict"); err == nil || !strings.Contains(err.Error(), "HypothesisGuard") {
		t.Fatalf("unknown mode: %v", err)
	}
	_, err := newRunner(Alert{}, Deps{Tools: tools.NewRegistry(), Inbox: &fakeInbox{}, Sink: &fakeSink{}}, Options{HypothesisGuard: "Enforce"})
	if err == nil {
		t.Fatal("newRunner accepted an unknown guard mode")
	}
}

// guardTools builds the two guarded room tools over one ledger with a
// connector status read, in mode, logging to logs.
func guardTools(mode string, logs *bytes.Buffer) (*updateHypothesisTool, *postMessageTool, *fakeSink, string) {
	l := newStepLedger()
	id := "s_" + strings.Repeat("0", 25) + "A"
	l.add(id, "connect_status", "task 0 RUNNING", false)
	g := newHypothesisGuard(mode, slog.New(slog.NewJSONHandler(logs, nil)), Alert{RoomID: "r_1", PipelineID: "orders"}, nil)
	sink := &fakeSink{}
	return &updateHypothesisTool{sink: sink, steps: l, guard: g}, &postMessageTool{sink: sink, steps: l, guard: g}, sink, id
}

func hypInput(text, step, conf string) json.RawMessage {
	b, _ := json.Marshal(hypothesisInput{Text: text, BasedOnSteps: []string{step}, Confidence: conf})
	return b
}

// Each claim form is refused in enforce, logged in log, ignored in off, in
// update_hypothesis and post_message alike; a backed claim passes everywhere.
func TestGuardModes(t *testing.T) {
	forms := []string{
		"Ruled out: the publication.",
		"We ruled out the publication.",
		"The publication is not the cause.",
		"No long transactions.",
	}
	ctx := context.Background()
	for _, mode := range []string{GuardOff, GuardLog, GuardEnforce} {
		for _, text := range forms {
			var logs bytes.Buffer
			hyp, msg, sink, id := guardTools(mode, &logs)
			_, herr := hyp.Run(ctx, hypInput(text, id, "medium"))
			_, merr := msg.Run(ctx, json.RawMessage(`{"text":`+strconvQuote(text)+`}`))
			switch mode {
			case GuardEnforce:
				if !errors.Is(herr, tools.ErrBadInput) || !errors.Is(merr, tools.ErrBadInput) {
					t.Fatalf("%s %q: errors %v / %v, want refusals", mode, text, herr, merr)
				}
				if n := len(sink.ofType(events.AgentHypothesis)) + len(sink.ofType(events.AgentMessage)); n != 0 {
					t.Fatalf("%s %q: %d events emitted for refused calls", mode, text, n)
				}
			default:
				if herr != nil || merr != nil {
					t.Fatalf("%s %q: errors %v / %v, want none", mode, text, herr, merr)
				}
			}
			wouldLog := strings.Count(logs.String(), "hypothesis guard would refuse")
			if want := map[string]int{GuardOff: 0, GuardLog: 2, GuardEnforce: 0}[mode]; wouldLog != want {
				t.Fatalf("%s %q: %d would-refuse log lines, want %d:\n%s", mode, text, wouldLog, want, logs.String())
			}
			if strings.Contains(strings.ToLower(logs.String()), "publication is") || strings.Contains(logs.String(), text) {
				t.Fatalf("%s: the log carries the model's text:\n%s", mode, logs.String())
			}
			if mode == GuardLog && !strings.Contains(logs.String(), `"rule":"unbacked_rule_out"`) {
				t.Fatalf("log line lacks the rule: %s", logs.String())
			}
		}
	}
	// A backed claim passes in enforce.
	var logs bytes.Buffer
	hyp, msg, _, id := guardTools(GuardEnforce, &logs)
	if _, err := hyp.Run(ctx, hypInput("Ruled out: the connector.", id, "medium")); err != nil {
		t.Fatalf("backed hypothesis refused: %v", err)
	}
	if _, err := msg.Run(ctx, json.RawMessage(`{"text":"The connector is fine."}`)); err != nil {
		t.Fatalf("backed message refused: %v", err)
	}
}

func strconvQuote(s string) string { b, _ := json.Marshal(s); return string(b) }

// Two refusals per rule per episode; the third attempt is recorded at low
// confidence with the leftover items in UnbackedTerms. An accepted call and a
// non-quiet input start the count again; a quiet alert update does not.
func TestGuardCap(t *testing.T) {
	ctx := context.Background()
	var logs bytes.Buffer
	hyp, _, sink, id := guardTools(GuardEnforce, &logs)
	in := hypInput(evidence2, id, "high")
	for i := 1; i <= guardMaxRefusals; i++ {
		if _, err := hyp.Run(ctx, in); !errors.Is(err, tools.ErrBadInput) {
			t.Fatalf("attempt %d: %v, want a refusal", i, err)
		}
	}
	res, err := hyp.Run(ctx, in)
	if err != nil {
		t.Fatalf("third attempt refused: %v", err)
	}
	if !strings.Contains(res.Summary, "low confidence after 2 refusals") {
		t.Fatalf("summary does not say it was capped: %q", res.Summary)
	}
	hs := sink.ofType(events.AgentHypothesis)
	if len(hs) != 1 {
		t.Fatalf("hypotheses = %d, want 1", len(hs))
	}
	var p events.AgentHypothesisPayload
	if err := json.Unmarshal(hs[0].payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.Confidence != "low" {
		t.Fatalf("confidence = %q, want low", p.Confidence)
	}
	for _, w := range []string{"role/grants", "publication", "connect config validation", "long transactions"} {
		if !slices.Contains(p.UnbackedTerms, w) {
			t.Fatalf("UnbackedTerms %q lacks %q", p.UnbackedTerms, w)
		}
	}
	// The accepted call reset the streak: the next one is refused again.
	if _, err := hyp.Run(ctx, in); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("after an accepted call: %v, want a refusal", err)
	}
	// A quiet input keeps the streak; a steer resets it.
	hyp.guard.observeInputs([]HumanInput{{Kind: "alert_update"}})
	if _, err := hyp.Run(ctx, in); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("second refusal: %v", err)
	}
	hyp.guard.observeInputs([]HumanInput{{Kind: "alert_update"}})
	if _, err := hyp.Run(ctx, in); err != nil {
		t.Fatalf("a quiet input reset the streak: %v", err)
	}
	hyp.guard.observeInputs([]HumanInput{steerInput()})
	if _, err := hyp.Run(ctx, in); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("after a steer: %v, want a refusal", err)
	}
}

// A capped finding's note is posted as one agent.message (rule 1 uses it);
// rules are checked in order and a refusal stops the later ones.
func TestGuardCheckOrderAndNotes(t *testing.T) {
	var logs bytes.Buffer
	g := newHypothesisGuard(GuardEnforce, slog.New(slog.NewJSONHandler(&logs, nil)), Alert{}, nil)
	ran := 0
	refuse := func() *guardFinding {
		return &guardFinding{rule: "a", refusal: tools.ErrBadInput, note: "[guard] a"}
	}
	later := func() *guardFinding { ran++; return nil }
	for i := 0; i < guardMaxRefusals; i++ {
		if _, err := g.check("t", refuse, later); err == nil {
			t.Fatal("not refused")
		}
	}
	if ran != 0 {
		t.Fatalf("a later rule ran after a refusal %d times", ran)
	}
	res, err := g.check("t", refuse, later)
	if err != nil || !res.capped || ran != 1 || !slices.Equal(res.notes, []string{"[guard] a"}) {
		t.Fatalf("capped check = %+v, %v, later ran %d", res, err, ran)
	}
	sink := &fakeSink{}
	g.post(context.Background(), sink, res.notes)
	if n := len(sink.ofType(events.AgentMessage)); n != 1 {
		t.Fatalf("notes posted = %d, want 1", n)
	}
	var off *hypothesisGuard
	if res, err := off.check("t", refuse); err != nil || res.capped {
		t.Fatalf("nil guard checked: %+v, %v", res, err)
	}
}

// Through the runner: in enforce mode the model sees two refusals with the
// rule's text and recovers; the third attempt is recorded at low confidence.
// The default mode (log) records the first attempt unchanged.
func TestRunGuardRefusesThenCaps(t *testing.T) {
	hyp := `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu%d","name":"update_hypothesis","input":{"text":"The connector user lost access. Ruled out: the publication, long transactions.","based_on_steps":[],"confidence":"low"}}]}`
	script := func() *scriptedModel {
		return &scriptedModel{turns: []anthropic.BetaMessage{
			mustMessage(t, fmt.Sprintf(hyp, 1)),
			mustMessage(t, fmt.Sprintf(hyp, 2)),
			mustMessage(t, fmt.Sprintf(hyp, 3)),
		}}
	}
	tool := &outcomeTool{name: "connect_status"}

	model := script()
	sink := &fakeSink{}
	r := newTestRunner(t, model, &fakeInbox{waitErr: context.Canceled}, sink, tool, Options{MaxTurns: 6, HypothesisGuard: GuardEnforce})
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	hs := sink.ofType(events.AgentHypothesis)
	if len(hs) != 1 {
		t.Fatalf("hypotheses = %d, want only the capped third", len(hs))
	}
	var p events.AgentHypothesisPayload
	if err := json.Unmarshal(hs[0].payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.Confidence != "low" || !slices.Contains(p.UnbackedTerms, "publication") || !slices.Contains(p.UnbackedTerms, "long transactions") {
		t.Fatalf("capped hypothesis = %+v", p)
	}
	js := messagesJSON(model.calls[len(model.calls)-1])
	if n := strings.Count(js, "hypothesis guard (unbacked_rule_out)"); n != 2 {
		t.Fatalf("the model saw %d refusals, want 2", n)
	}

	model = script()
	sink = &fakeSink{}
	r = newTestRunner(t, model, &fakeInbox{waitErr: context.Canceled}, sink, tool, Options{MaxTurns: 6})
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if n := len(sink.ofType(events.AgentHypothesis)); n != 3 {
		t.Fatalf("log mode recorded %d hypotheses, want all 3", n)
	}
}

// A person's steer between attempts starts a new episode in the runner: the
// count starts again, so the attempt after it is refused, not capped.
func TestRunGuardSteerResetsCap(t *testing.T) {
	hyp := `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu%d","name":"update_hypothesis","input":{"text":"Ruled out: the publication.","based_on_steps":[],"confidence":"low"}}]}`
	end := `{"stop_reason":"end_turn","content":[{"type":"text","text":"waiting"}]}`
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, fmt.Sprintf(hyp, 1)),
		mustMessage(t, fmt.Sprintf(hyp, 2)),
		mustMessage(t, end),
		mustMessage(t, fmt.Sprintf(hyp, 4)),
		mustMessage(t, end),
	}}
	// Drains: the opening one, one after each tool turn, then the idle wait.
	inbox := &fakeInbox{waitErr: context.Canceled, drains: [][]HumanInput{nil, nil, nil, {steerInput()}}}
	sink := &fakeSink{}
	r := newTestRunner(t, model, inbox, sink, &outcomeTool{name: "connect_status"}, Options{MaxTurns: 10, HypothesisGuard: GuardEnforce})
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if n := len(sink.ofType(events.AgentHypothesis)); n != 0 {
		t.Fatalf("hypotheses = %d, want 0: the attempt after the steer was capped instead of refused", n)
	}
	if len(model.calls) < 5 {
		t.Fatalf("model calls = %d, want the post-steer turns", len(model.calls))
	}
	if js := messagesJSON(model.calls[4]); strings.Count(js, "hypothesis guard (unbacked_rule_out)") != 3 {
		t.Fatalf("the model did not see three refusals:\n%s", js)
	}
}
