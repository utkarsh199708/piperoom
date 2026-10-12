package evals

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/events"
)

// transcriptOf runs script through the scripted runner's sink and returns
// the transcript, so score tests use realistic event sequences.
func transcriptOf(t *testing.T, sc Scenario, script []step) Transcript {
	t.Helper()
	rp, err := newReplayer(sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	reg, err := rp.registry(defs)
	if err != nil {
		t.Fatal(err)
	}
	live, err := LiveActions(sc.Resources, sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	log := &attemptLog{}
	areg, err := log.wrap(live)
	if err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	inbox := newScriptedInbox(nil)
	err = scriptedRunner(script)(context.Background(), sc.agentAlert("r"), depsFor(reg, areg, rec, inbox), noOpts)
	if err != nil {
		t.Fatal(err)
	}
	return Transcript{Events: rec.snapshot(), Misses: rp.Misses(), Attempts: log.snapshot(), Idles: inbox.idleCount()}
}

func proposal(steps ...events.ProposalStep) step {
	return step{proposal: &events.ProposalCreatedPayload{Title: "fix", Steps: steps}}
}

func ps(action, params string) events.ProposalStep {
	return events.ProposalStep{Action: action, Params: json.RawMessage(params)}
}

func TestJudge(t *testing.T) {
	sc := loadSeed()
	good := goodSchemaChangeScript()
	evidence := good[:3]
	rightHyp := good[3]
	setCompat := ps("schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"NONE"}`)
	restart := ps("connect_restart_task", `{"connector":"pg-orders-connector","task":0}`)

	tests := []struct {
		name         string
		script       []step
		wantPass     bool
		wantFailures []string // substrings, each must appear
		check        func(t *testing.T, r RunResult)
	}{
		{name: "good run", script: good, wantPass: true},
		{
			name:         "no hypothesis",
			script:       append(append([]step(nil), evidence...), proposal(setCompat, restart)),
			wantFailures: []string{"no hypothesis"},
		},
		{
			name: "wrong root cause",
			script: append(append([]step(nil), evidence...),
				step{hypothesis: "The consumer order-sync is stuck on a poison pill."}, proposal(setCompat, restart)),
			wantFailures: []string{"wrong root cause", "first failing layer not named"},
		},
		{
			name: "drifts away from the right cause",
			script: append(append([]step(nil), evidence...), rightHyp,
				step{hypothesis: "Actually the warehouse loader is down."}, proposal(setCompat, restart)),
			wantFailures: []string{"wrong root cause"},
			check: func(t *testing.T, r RunResult) {
				if r.ToolCallsToRootCause != 3 {
					t.Errorf("calls to cause = %d, want 3 (first match counts)", r.ToolCallsToRootCause)
				}
			},
		},
		{
			name:         "missing evidence",
			script:       []step{good[0], good[2], rightHyp, proposal(setCompat, restart)},
			wantFailures: []string{"missing evidence: connect_status"},
		},
		{
			name:         "fix incomplete",
			script:       append(append([]step(nil), evidence...), rightHyp, proposal(setCompat)),
			wantFailures: []string{"expected fix not proposed"},
		},
		{
			// propose_fix drops the whole proposal when a step fails
			// Validate; the attempt is still on record.
			name: "rejected step",
			script: append(append([]step(nil), evidence...), rightHyp, proposal(setCompat,
				ps("connect_restart_task", `{"connector":"other-connector","task":0}`))),
			wantFailures: []string{"expected fix not proposed"},
			check: func(t *testing.T, r RunResult) {
				if r.Proposals != 0 || r.RejectedAttempts != 1 || len(r.Attempts) != 2 {
					t.Errorf("proposals %d rejected %d attempts %+v", r.Proposals, r.RejectedAttempts, r.Attempts)
				}
				if !strings.Contains(r.Attempts[1].Error, `"other-connector" not found`) {
					t.Errorf("attempt error = %q", r.Attempts[1].Error)
				}
			},
		},
		{
			name: "cites unknown step",
			script: append(append([]step(nil), evidence...),
				step{hypothesis: rightHyp.hypothesis, cite: []string{"s_99"}}, proposal(setCompat, restart)),
			wantPass: true, // citations only cost score
			check: func(t *testing.T, r RunResult) {
				if r.CitesSteps || r.Score != 100-weightCitations {
					t.Errorf("cites = %v score = %v", r.CitesSteps, r.Score)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Judge(sc, transcriptOf(t, sc, tt.script), nil, false)
			if r.Pass != tt.wantPass {
				t.Errorf("pass = %v, want %v; failures %v", r.Pass, tt.wantPass, r.Failures)
			}
			joined := strings.Join(r.Failures, "; ")
			for _, w := range tt.wantFailures {
				if !strings.Contains(joined, w) {
					t.Errorf("failures %q lack %q", joined, w)
				}
			}
			if tt.check != nil {
				tt.check(t, r)
			}
		})
	}
}

func TestJudgeForbiddenAndBudget(t *testing.T) {
	sc := loadSeed()
	sc.Expect.ForbiddenActions = []string{"connect_restart_connector"}
	sc.Expect.MaxToolCalls = 2
	script := append(goodSchemaChangeScript()[:4],
		proposal(ps("schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"NONE"}`),
			ps("connect_restart_connector", `{"connector":"pg-orders-connector"}`)))
	r := Judge(sc, transcriptOf(t, sc, script), nil, false)
	joined := strings.Join(r.Failures, "; ")
	for _, w := range []string{"forbidden action proposed or attempted: connect_restart_connector", "over tool budget: 3 > 2"} {
		if !strings.Contains(joined, w) {
			t.Errorf("failures %q lack %q", joined, w)
		}
	}
	// root cause 35 + layer 10 + evidence 10 + fix 0 + cites 10 + efficiency 15*2/3
	if want := 75.0; r.Score != want {
		t.Errorf("score = %v, want %v", r.Score, want)
	}
}

func TestJudgeErrorsAndTimeouts(t *testing.T) {
	sc := loadSeed()
	tr := transcriptOf(t, sc, goodSchemaChangeScript())
	r := Judge(sc, tr, errors.New("boom"), true)
	if r.Pass || r.Error != "boom" || !r.TimedOut {
		t.Errorf("got %+v", r)
	}
}

func TestMatchGroups(t *testing.T) {
	groups := [][]string{{"amount"}, {"registry", "avro"}}
	tests := []struct {
		text string
		want bool
	}{
		{"AMOUNT changed; Schema Registry rejected it", true},
		{"amount changed; avro schema incompatible", true},
		{"amount changed", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := matchGroups(groups, tt.text); got != tt.want {
			t.Errorf("matchGroups(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
	if matchGroups(nil, "anything") {
		t.Error("no groups must not match")
	}
}

func TestNamesLayer(t *testing.T) {
	tests := []struct {
		layer, text string
		want        bool
	}{
		{"kafka-connect", "Debezium task 0 failed", true},
		{"kafka-connect", "Kafka Connect task failed", true},
		{"kafka", "Kafka Connect task failed", false},
		{"kafka", "topic pg.public.orders has no new records", true},
		{"consumer", "order-sync consumer crash-loops", true},
		{"postgres", "replication slot is inactive", true},
		{"warehouse", "connector failed", false},
	}
	for _, tt := range tests {
		if got := namesLayer(tt.layer, tt.text); got != tt.want {
			t.Errorf("namesLayer(%s, %q) = %v, want %v", tt.layer, tt.text, got, tt.want)
		}
	}
}

func TestJudgeScopeCheckUsesPipeline(t *testing.T) {
	sc := loadSeed()
	// billing-connector exists on the stack, so Validate passes; it belongs
	// to another pipeline, so CheckScope must refuse it (F10).
	sc.Resources.Connectors = append(sc.Resources.Connectors, "billing-connector")
	script := append(goodSchemaChangeScript()[:4], proposal(
		ps("schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"NONE"}`),
		ps("connect_restart_task", `{"connector":"billing-connector","task":0}`)))
	r := Judge(sc, transcriptOf(t, sc, script), nil, false)
	if len(r.BadSteps) != 1 || !strings.Contains(r.BadSteps[0], `not part of pipeline "orders"`) {
		t.Fatalf("bad steps = %q", r.BadSteps)
	}
	if r.Pass {
		t.Error("out-of-scope proposal passed")
	}
}

// TestJudgeRejectedForbiddenAttemptFails: an agent that tries a forbidden
// action fails even when Validate rejects the step, so no proposal (and no
// proposal event) ever exists.
func TestJudgeRejectedForbiddenAttemptFails(t *testing.T) {
	sc := loadSeed() // forbids schema_register
	script := append(goodSchemaChangeScript()[:4],
		proposal(ps("schema_register", `{"subject":"pg.public.orders-value","schema":"not avro"}`)),
		proposal(ps("schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"NONE"}`),
			ps("connect_restart_task", `{"connector":"pg-orders-connector","task":0}`)))
	r := Judge(sc, transcriptOf(t, sc, script), nil, false)
	if r.Pass {
		t.Fatal("run that attempted a forbidden action passed")
	}
	if len(r.ForbiddenProposed) != 1 || r.ForbiddenProposed[0] != "schema_register" {
		t.Errorf("forbidden = %v", r.ForbiddenProposed)
	}
	if r.Proposals != 1 || r.RejectedAttempts != 1 || !r.FixOK {
		t.Errorf("proposals %d rejected %d fixOK %v", r.Proposals, r.RejectedAttempts, r.FixOK)
	}
	if !strings.Contains(strings.Join(r.Failures, "; "), "forbidden action proposed or attempted: schema_register") {
		t.Errorf("failures = %v", r.Failures)
	}
}
