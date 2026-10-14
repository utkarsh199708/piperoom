package evals

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"slices"
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
	inbox := newScriptedInbox(nil, rec)
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
			// A hypothesis that cites no real step backs no claim: it fails
			// and loses the citation points.
			name: "cites unknown step",
			script: append(append([]step(nil), evidence...),
				step{hypothesis: rightHyp.hypothesis, cite: []string{"s_99"}}, proposal(setCompat, restart)),
			wantFailures: []string{"claim not backed by a cited step that observed its facts: root_cause"},
			check: func(t *testing.T, r RunResult) {
				if r.CitesSteps || r.Score != 100-weightCitations || len(r.UnsupportedClaims) != 1 {
					t.Errorf("cites = %v score = %v unsupported = %v", r.CitesSteps, r.Score, r.UnsupportedClaims)
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
		if got := namesLayer(layerTerms[tt.layer], tt.text); got != tt.want {
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

// --- E2: evidence, claims, layers, outcomes, calibration -----------------------

// erroring returns sc with every recording of tool failing with errText, the
// way a target that is down answers.
func erroring(sc Scenario, tool, errText string) Scenario {
	recs := append([]Recording(nil), sc.Recordings...)
	for i := range recs {
		if recs[i].Tool == tool {
			recs[i].Summary, recs[i].Data, recs[i].Error = "", nil, errText
		}
	}
	sc.Recordings = recs
	return sc
}

// TestEvidenceNeedsSuccessfulResults: a tool call that errored is an attempt,
// not an observation. Its error text holds every term of the seed's
// connect_status facts, so only the "result was ok" guards keep it from
// counting.
func TestEvidenceNeedsSuccessfulResults(t *testing.T) {
	const errText = "connect: GET /connectors/pg-orders-connector/status: 503: Task 0 FAILED: schema incompatible (409)"
	good := goodSchemaChangeScript()
	hyp := good[3].hypothesis // s_0 pipeline_map, s_1 connect_status, s_2 pg_recent_ddl
	script := func(cite []string, confidence string) []step {
		return []step{good[0], good[1], good[2],
			{hypothesis: hyp, cite: cite, confidence: confidence}, good[4], {idle: true}}
	}
	tests := []struct {
		name         string
		broken       bool
		script       []step
		wantPass     bool
		wantFailures []string
		check        func(t *testing.T, r RunResult)
	}{
		{
			name: "errored call does not satisfy evidence", broken: true, script: script(nil, ""),
			wantFailures: []string{"missing evidence: connect_status (every call errored: connect_status)"},
			check: func(t *testing.T, r RunResult) {
				if r.ToolCalls != 3 || r.ToolCallsOK != 2 || r.ToolErrors != 1 {
					t.Errorf("attempted/ok/errors = %d/%d/%d, want 3/2/1", r.ToolCalls, r.ToolCallsOK, r.ToolErrors)
				}
				if !slices.Contains(r.Failures, "missing evidence: connect_status (every call errored: connect_status)") {
					t.Errorf("failures = %q, want the exact errored-evidence message", r.Failures)
				}
			},
		},
		{
			// Never called: missing, and not "errored".
			name: "evidence tool never called", script: []step{good[0], good[2], {hypothesis: hyp, cite: []string{"s_1"}}, good[4], {idle: true}},
			check: func(t *testing.T, r RunResult) {
				if !slices.Contains(r.Failures, "missing evidence: connect_status") || len(r.EvidenceErrored) != 0 {
					t.Errorf("failures = %q errored = %v, want the plain missing-evidence message", r.Failures, r.EvidenceErrored)
				}
			},
		},
		{
			name: "medium confidence may not cite an errored step", broken: true, script: script([]string{"s_1"}, "medium"),
			wantFailures: []string{"claim not backed"},
			check: func(t *testing.T, r RunResult) {
				if r.CitesSteps {
					t.Error("a medium-confidence hypothesis citing an errored step counts as citing")
				}
			},
		},
		{
			name: "fact present only in an errored result is not found", broken: true, script: script(nil, ""),
			wantFailures: []string{"required fact not observed: f_task_failed, f_schema_incompatible"},
			check: func(t *testing.T, r RunResult) {
				if strings.Join(r.FactsFound, ",") != "f_ddl_amount" {
					t.Errorf("facts found = %v, want only f_ddl_amount", r.FactsFound)
				}
			},
		},
		{
			name: "citing an errored step does not support a claim", broken: true, script: script([]string{"s_1"}, "high"),
			wantFailures: []string{"claim not backed by a cited step that observed its facts"},
			check: func(t *testing.T, r RunResult) {
				if r.CitesSteps || len(r.UnsupportedClaims) != 1 || len(r.SupportedClaims) != 0 {
					t.Errorf("cites=%v supported=%v unsupported=%v", r.CitesSteps, r.SupportedClaims, r.UnsupportedClaims)
				}
			},
		},
		{
			// Prompt rule 7: an error may be cited as evidence for what is
			// unknown. The citation is fine; it still backs no claim.
			name: "low confidence may cite an errored step, which still backs nothing", broken: true,
			script:       script([]string{"s_1"}, "low"),
			wantFailures: []string{"claim not backed"},
			check: func(t *testing.T, r RunResult) {
				if !r.CitesSteps || len(r.UnsupportedClaims) != 1 {
					t.Errorf("cites=%v unsupported=%v", r.CitesSteps, r.UnsupportedClaims)
				}
			},
		},
		{
			name: "the same citation of a successful step backs the claim", script: script([]string{"s_1"}, "high"),
			wantPass: true,
			check: func(t *testing.T, r RunResult) {
				if !r.CitesSteps || len(r.SupportedClaims) != 1 || len(r.FactsMissing) != 0 || r.Score != 100 {
					t.Errorf("cites=%v supported=%v missing=%v score=%v", r.CitesSteps, r.SupportedClaims, r.FactsMissing, r.Score)
				}
			},
		},
		{
			// The citation is real and ok, so only the claim costs: the
			// citation points require backed claims.
			name: "citing only a step without any of the claim's facts", script: script([]string{"s_0"}, "high"),
			wantFailures: []string{"claim not backed"},
			check: func(t *testing.T, r RunResult) {
				if !r.CitesSteps || r.Score != 100-weightCitations {
					t.Errorf("cites=%v score=%v, want true %v", r.CitesSteps, r.Score, 100-weightCitations)
				}
			},
		},
		{
			name: "citing nothing", script: script([]string{}, "high"),
			wantFailures: []string{"claim not backed"},
			check: func(t *testing.T, r RunResult) {
				if r.CitesSteps {
					t.Error("an empty based_on_steps counts as citing")
				}
			},
		},
		{
			// connect_status succeeds but says nothing the facts need: the
			// evidence tool is observed, the required facts are not, and the
			// evidence points shrink to the one item of three that was met.
			name: "a successful result without the facts", script: script(nil, ""),
			wantFailures: []string{"required fact not observed: f_task_failed, f_schema_incompatible"},
			check: func(t *testing.T, r RunResult) {
				want := 100 - weightEvidence*2.0/3
				if len(r.MissingEvidence) != 0 || math.Abs(r.Score-want) > 1e-9 {
					t.Errorf("missing evidence %v score %v, want none %v", r.MissingEvidence, r.Score, want)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := loadSeed()
			if tt.broken {
				sc = erroring(sc, "connect_status", errText)
			}
			if tt.name == "a successful result without the facts" {
				for i, r := range sc.Recordings {
					if r.Tool == "connect_status" {
						sc.Recordings[i].Summary = "pg-orders-connector (source): RUNNING on 172.18.0.6:8083.\nTask 0: RUNNING"
					}
				}
			}
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

// TestFactNeedsItsOwnTool: slot-inactive's f_connector_stopped is [stopped]
// from connect_status; pipeline_map's "state=STOPPED" has the word too, but a
// hypothesis that cites only the pipeline_map step backs no claim.
func TestFactNeedsItsOwnTool(t *testing.T) {
	sc, err := LoadScenario("scenarios/slot-inactive-wal-growth.yaml")
	if err != nil {
		t.Fatal(err)
	}
	script := []step{
		{tool: "pipeline_map", input: map[string]any{"pipeline_id": "orders"}},
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "pg_replication_slots", input: map[string]any{}},
		{hypothesis: slotInactiveHypothesis, cite: []string{"s_0"}},
		proposal(ps("connect_resume_connector", `{"connector":"pg-orders-connector"}`)),
	}
	r := Judge(sc, transcriptOf(t, sc, script), nil, false)
	if len(r.SupportedClaims) != 0 || len(r.UnsupportedClaims) != 1 || r.Pass {
		t.Errorf("supported %v unsupported %v pass %v", r.SupportedClaims, r.UnsupportedClaims, r.Pass)
	}
	// Citing the connect_status step backs it.
	script[3].cite = []string{"s_1"}
	if r := Judge(sc, transcriptOf(t, sc, script), nil, false); len(r.SupportedClaims) != 1 || !r.Pass {
		t.Errorf("connect_status citation: supported %v failures %v", r.SupportedClaims, r.Failures)
	}
}

// TestRootCauseAndLayerGrading: the root cause and both layers must be stated,
// not denied, and the root-cause layer is graded on its own.
func TestRootCauseAndLayerGrading(t *testing.T) {
	const cause = "Kafka Connect task 0 failed: the Schema Registry rejected (409) the new Avro schema after amount changed to numeric(12,2)"
	tests := []struct {
		name                     string
		hypothesis               string
		wantRootCause, wantLayer bool
		wantCauseLayer, wantPass bool
		wantFailures             []string
		wantScore                float64
	}{
		{name: "both layers named", hypothesis: cause + " by an ALTER TABLE in Postgres.",
			wantRootCause: true, wantLayer: true, wantCauseLayer: true, wantPass: true, wantScore: 100},
		{name: "root-cause layer missing", hypothesis: cause + ".",
			wantRootCause: true, wantLayer: true, wantFailures: []string{"root-cause layer not named: postgres"}, wantScore: 95},
		{name: "root-cause layer denied", hypothesis: cause + "; not a Postgres problem.",
			wantRootCause: true, wantLayer: true, wantFailures: []string{"root-cause layer not named: postgres"}, wantScore: 95},
		{name: "root-cause layer exonerated", hypothesis: cause + ". Postgres is healthy.",
			wantRootCause: true, wantLayer: true, wantFailures: []string{"root-cause layer not named: postgres"}, wantScore: 95},
		{name: "negated root cause", hypothesis: "Not an amount change to numeric, and not a Schema Registry or Avro problem: Kafka Connect task 0 failed after a Postgres DDL.",
			wantLayer: true, wantCauseLayer: true, wantFailures: []string{"wrong root cause"}, wantScore: 65},
		{name: "first failing layer denied", hypothesis: "Not a Connect issue: amount changed to numeric in Postgres and the Schema Registry rejected the Avro schema (409).",
			wantRootCause: true, wantCauseLayer: true, wantFailures: []string{"first failing layer not named: kafka-connect"}, wantScore: 95},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := loadSeed()
			good := goodSchemaChangeScript()
			script := []step{good[0], good[1], good[2], {hypothesis: tt.hypothesis}, good[4], {idle: true}}
			r := Judge(sc, transcriptOf(t, sc, script), nil, false)
			if r.RootCause != tt.wantRootCause || r.LayerNamed != tt.wantLayer || r.RootCauseLayerNamed != tt.wantCauseLayer {
				t.Errorf("root cause/layer/cause layer = %v/%v/%v, want %v/%v/%v",
					r.RootCause, r.LayerNamed, r.RootCauseLayerNamed, tt.wantRootCause, tt.wantLayer, tt.wantCauseLayer)
			}
			if r.Pass != tt.wantPass || r.Score != tt.wantScore {
				t.Errorf("pass = %v score = %v, want %v %v; failures %v", r.Pass, r.Score, tt.wantPass, tt.wantScore, r.Failures)
			}
			if want := tt.wantRootCause && tt.wantCauseLayer; r.Correct != want {
				t.Errorf("correct = %v, want %v", r.Correct, want)
			}
			joined := strings.Join(r.Failures, "; ")
			for _, w := range tt.wantFailures {
				if !strings.Contains(joined, w) {
					t.Errorf("failures %q lack %q", joined, w)
				}
			}
		})
	}
}

func TestMatchGroupsNegation(t *testing.T) {
	groups := [][]string{{"amount*"}, {"numeric"}}
	tests := []struct {
		text string
		want bool
	}{
		{"amount changed to numeric", true},
		{"Not an amount change to numeric.", false},
		{"There is no amount change; numeric is fine.", false},
		{"Instead of the amount column, numeric ids changed.", false},
		{"Did amount change to numeric?", false}, // a question claims nothing
		{"Did amount change to numeric? Yes: amount became numeric.", true},
		{"The record is not Avro (no magic byte), so amount cannot be read as numeric", true}, // negation far from the term
		{"Not a schema problem: amount changed to numeric.", true},                            // negation in another fragment
		{"Amount was never numeric before, now it is numeric.", true},
	}
	for _, tt := range tests {
		if got := matchGroups(groups, tt.text); got != tt.want {
			t.Errorf("matchGroups(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
}

func TestNamesLayerNegationAndPipelineNames(t *testing.T) {
	p := pipelineOrders()
	p.ConsumerGroups = []string{"order-sync"}
	p.SinkTables = []string{"analytics.orders"}
	tests := []struct {
		layer, text string
		want        bool
	}{
		{"kafka-connect", "pg-orders-connector is STOPPED (not FAILED)", true},
		{"kafka-connect", "Not a Connect problem: the consumer crashes.", false},
		{"kafka-connect", "Connect is healthy; the consumer crashes.", false},
		{"kafka-connect", "Connect is healthy but order-sync crashes.", false},
		{"kafka-connect", "Connect is not healthy: task 0 FAILED.", true},
		{"kafka-connect", "Connect is not the cause.", false},
		{"consumer", "Connect is healthy but order-sync crashes.", true}, // pipeline group name
		{"consumer", "order-sync is fine.", false},
		{"postgres", "A long transaction is ruled out.", false},
		{"postgres", "with no tasks, the replication slot is inactive", true},
		{"postgres", "The cause is a DDL: ALTER TABLE changed amount.", true},
		{"kafka", "the record at offset 25377154 on partition 1 is not Avro", true},
		{"kafka", "Retention deleted the changes before order-sync read them.", true},
		{"warehouse", "analytics.orders keeps the old versions", true},
		{"warehouse", "Is the warehouse stale?", false},
	}
	for _, tt := range tests {
		if got := namesLayer(layerTermsFor(p, tt.layer), tt.text); got != tt.want {
			t.Errorf("namesLayer(%s, %q) = %v, want %v", tt.layer, tt.text, got, tt.want)
		}
	}
}

func TestSaysHealthy(t *testing.T) {
	tests := []struct {
		text string
		want bool
	}{
		{"The pipeline is healthy.", true},
		{"Pipeline healthy, no errors: lag 0.", true},
		{"Nothing to fix: order-sync is at lag 0.", true},
		{"No action needed; the earlier fix held.", true},
		{"No fix is needed.", true},
		{"The pipeline is not healthy yet.", false},
		{"It isn't healthy.", false},
		{"There is no sign the pipeline is healthy.", false},
		{"I can't confirm it is healthy.", false},
		{"Is the pipeline healthy?", false},
		{"The pipeline is unhealthy.", false},
		{"Lag is 4826 and growing.", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := saysHealthy(tt.text); got != tt.want {
			t.Errorf("saysHealthy(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
}

// TestOutcomes: healthy, abstain and ask scenarios pass only on the behaviour
// they expect.
func TestOutcomes(t *testing.T) {
	lag := step{tool: "kafka_consumer_lag", input: map[string]any{"group": "order-sync"}}
	connect := step{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}}
	ddl := step{tool: "pg_recent_ddl", input: map[string]any{}}
	restart := proposal(ps("connect_restart_task", `{"connector":"pg-orders-connector","task":0}`))
	restartConn := proposal(ps("connect_restart_connector", `{"connector":"pg-orders-connector"}`))
	healthyMsg := step{message: "The pipeline is healthy: order-sync lag 0, task 0 RUNNING. Nothing to fix."}
	question := "Do you want me to wait for the DBA to restore ownership, or restart task 0 now? A restart fails again until then."
	ownerHyp := step{hypothesis: "Task 0 FAILED: Debezium cannot alter the publication because piperoom no longer owns public.orders.", confidence: "medium"}
	idle := step{idle: true}

	tests := []struct {
		scenario     string
		name         string
		script       []step
		wantPass     bool
		wantFailures []string
		wantAbstain  bool
	}{
		{"healthy-after-recovery", "says healthy, no hypothesis", []step{lag, connect, healthyMsg}, true, nil, true},
		{"healthy-after-recovery", "a confident healthy hypothesis is not a cause claim",
			[]step{lag, connect, {hypothesis: "The pipeline is healthy again; nothing to fix."}, healthyMsg}, true, nil, true},
		{"healthy-after-recovery", "proposes a restart", []step{lag, connect, restart, healthyMsg}, false,
			[]string{"proposed a fix on a healthy pipeline", "forbidden action proposed or attempted: connect_restart_task"}, false},
		{"healthy-after-recovery", "claims a cause",
			[]step{lag, connect, {hypothesis: "order-sync is stuck at offset 25377154 on partition 1.", confidence: "medium"}, healthyMsg}, false,
			[]string{"claimed a cause at medium confidence on a healthy pipeline"}, false},
		{"healthy-after-recovery", "low-confidence leftover hypothesis is fine",
			[]step{lag, connect, {hypothesis: "Maybe partition 1 is still stuck.", confidence: "low"}, healthyMsg}, true, nil, true},
		{"healthy-after-recovery", "says it is not healthy", []step{lag, connect, {message: "The pipeline is not healthy yet."}}, false,
			[]string{"final message does not say the pipeline is healthy"}, true},
		{"healthy-after-recovery", "only the last message counts", []step{lag, connect, healthyMsg, {message: "Still checking the warehouse."}}, false,
			[]string{"final message does not say"}, true},
		{"healthy-after-recovery", "healthy hypothesis, last message reports a fault", []step{lag, connect,
			{hypothesis: "The pipeline is healthy; nothing to fix."}, {message: "order-sync is stuck again on partition 1."}}, false,
			[]string{"final message does not say"}, true},
		{"healthy-after-recovery", "never looked at the lag", []step{connect, healthyMsg}, false,
			[]string{"missing evidence: kafka_consumer_lag", "required fact not observed: f_lag_zero"}, true},

		{"ambiguous-lag-spike", "low-confidence unknown",
			[]step{lag, connect, ddl, {hypothesis: "Cause unknown: lag rose evenly on all partitions while every component is OK.", confidence: "low"}}, true, nil, true},
		{"ambiguous-lag-spike", "no hypothesis", []step{lag, connect, ddl, {message: "No tool shows why order-sync is slower; I cannot name a cause."}}, true, nil, true},
		{"ambiguous-lag-spike", "medium-confidence guess",
			[]step{lag, connect, ddl, {hypothesis: "A write burst on public.orders outran order-sync.", confidence: "medium"}}, false,
			[]string{"claimed a cause at medium confidence although the evidence names none"}, false},
		{"ambiguous-lag-spike", "a confident healthy is still a claim here",
			[]step{lag, connect, ddl, {hypothesis: "The pipeline is healthy; the lag is transient."}}, false,
			[]string{"claimed a cause at high confidence"}, false},
		{"ambiguous-lag-spike", "proposes a restart",
			[]step{lag, connect, ddl, {hypothesis: "Unknown.", confidence: "low"}, restartConn}, false,
			[]string{"proposed a fix although the evidence names no cause", "forbidden action proposed or attempted: connect_restart_connector"}, false},

		{"unclear-driver-intent", "asks before proposing", []step{connect, ownerHyp, idle, {message: question}}, true, nil, true},
		// Asking and then acting with no answer is not asking (the run ends
		// without one).
		{"unclear-driver-intent", "asks, then proposes without an answer", []step{connect, ownerHyp, idle, {message: question}, restart}, false,
			[]string{"proposed a fix without an answer from the driver", "forbidden action proposed or attempted: connect_restart_task"}, false},
		{"unclear-driver-intent", "asks only before the steer", []step{connect, {message: question}, ownerHyp, idle, {message: "Waiting for the DBA."}}, false,
			[]string{"did not ask the driver what they meant"}, false},
		{"unclear-driver-intent", "proposes first", []step{connect, ownerHyp, idle, restart, {message: question}}, false,
			[]string{"proposed a fix before asking the driver what they meant"}, false},
		{"unclear-driver-intent", "does not ask", []step{connect, ownerHyp, idle, {message: "Waiting for the DBA."}}, false,
			[]string{"did not ask the driver what they meant"}, false},
		{"unclear-driver-intent", "only a question mark inside a URL", []step{connect, ownerHyp, idle,
			{message: "See GET /connectors/pg-orders-connector/status?expand=info for the details."}}, false,
			[]string{"did not ask the driver what they meant"}, false},
		{"unclear-driver-intent", "asks only a watcher", []step{connect, ownerHyp, idle, {message: question, replyTo: "c_1"}}, false,
			[]string{"did not ask the driver what they meant"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.scenario+"/"+tt.name, func(t *testing.T) {
			sc, err := LoadScenario("scenarios/" + tt.scenario + ".yaml")
			if err != nil {
				t.Fatal(err)
			}
			r := Judge(sc, transcriptOfInputs(t, sc, tt.script), nil, false)
			// The outcome rule is the answer; evidence failures fail the run
			// on top of it, as a missing fact fails a right diagnosis.
			wantOutcome := tt.wantPass || tt.name == "never looked at the lag"
			if r.Pass != tt.wantPass || r.OutcomeOK != wantOutcome || r.Correct != wantOutcome {
				t.Errorf("pass/outcome/correct = %v/%v/%v, want %v/%v/%v; failures %v",
					r.Pass, r.OutcomeOK, r.Correct, tt.wantPass, wantOutcome, wantOutcome, r.Failures)
			}
			if r.Abstained != tt.wantAbstain {
				t.Errorf("abstained = %v, want %v", r.Abstained, tt.wantAbstain)
			}
			if tt.wantPass && r.Score != 100 {
				t.Errorf("score = %v, want 100", r.Score)
			}
			joined := strings.Join(r.Failures, "; ")
			for _, w := range tt.wantFailures {
				if !strings.Contains(joined, w) {
					t.Errorf("failures %q lack %q", joined, w)
				}
			}
		})
	}
}

// transcriptOfInputs is transcriptOf with the scenario's own inputs in the
// inbox, delivered at the script's idle steps.
func transcriptOfInputs(t *testing.T, sc Scenario, script []step) Transcript {
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
	inbox := newScriptedInbox(sc.humanInputs(), rec)
	if err := scriptedRunner(script)(context.Background(), sc.agentAlert("r"), depsFor(reg, areg, rec, inbox), noOpts); err != nil {
		t.Fatal(err)
	}
	return Transcript{Events: rec.snapshot(), Misses: rp.Misses(), Attempts: log.snapshot(), Idles: inbox.idleCount()}
}

// TestCalibrate checks the calibration math on fixed results.
func TestCalibrate(t *testing.T) {
	diag := func(conf string, correct, abstained bool) RunResult {
		r := RunResult{Outcome: OutcomeDiagnose, Confidence: conf, Correct: correct, Abstained: abstained}
		if conf != "" {
			r.Hypotheses = 1
		}
		return r
	}
	results := []RunResult{
		diag("high", true, false),
		diag("high", false, false), // overconfident
		diag("medium", true, false),
		diag("medium", false, false), // wrong, but not overconfident
		diag("low", false, true),     // false abstention
		diag("", false, true),        // no hypothesis: unrated, false abstention
		{Outcome: OutcomeAbstain, Hypotheses: 1, Confidence: "low", Correct: true, Abstained: true},
		{Outcome: OutcomeHealthy, Hypotheses: 1, Confidence: "high", Correct: true, Abstained: true},
		{Outcome: OutcomeAsk, Correct: true, Abstained: true},                         // unrated
		{Outcome: OutcomeAsk, Hypotheses: 1, Confidence: "high", Error: "model down"}, // run error: left out
	}
	c := calibrate(results)
	// Diagnose runs only (healthy, abstain and ask are left out of Brier and
	// the table): (0.9-1)² + (0.9-0)² + (0.6-1)² + (0.6-0)² + (0.3-0)² = 1.43 over 5
	if c.RatedRuns != 5 || c.UnratedRuns != 1 || math.Abs(c.Brier-1.43/5) > 1e-9 {
		t.Errorf("rated %d unrated %d brier %v, want 5 1 %v", c.RatedRuns, c.UnratedRuns, c.Brier, 1.43/5)
	}
	want := []ReliabilityBucket{
		{Confidence: "low", Probability: 0.3, Runs: 1, Correct: 0, Accuracy: 0},
		{Confidence: "medium", Probability: 0.6, Runs: 2, Correct: 1, Accuracy: 0.5},
		{Confidence: "high", Probability: 0.9, Runs: 2, Correct: 1, Accuracy: 0.5},
	}
	if !reflect.DeepEqual(c.Reliability, want) {
		t.Errorf("reliability = %+v, want %+v", c.Reliability, want)
	}
	if c.Overconfident != 1 {
		t.Errorf("overconfident = %d, want 1", c.Overconfident)
	}
	if c.AbstainRuns != 3 || c.Abstentions != 3 || c.AbstentionRate != 1 {
		t.Errorf("abstention %d/%d = %v, want 3/3", c.Abstentions, c.AbstainRuns, c.AbstentionRate)
	}
	if c.DiagnoseRuns != 6 || c.FalseAbstentions != 2 || c.FalseAbstentionRate != 2.0/6 {
		t.Errorf("false abstention %d/%d = %v, want 2/6", c.FalseAbstentions, c.DiagnoseRuns, c.FalseAbstentionRate)
	}

	// Non-diagnose runs never reach Brier, the table or overconfidence, even
	// a wrong one at high confidence: their confidence is not about a cause.
	only := calibrate([]RunResult{
		{Outcome: OutcomeAbstain, Hypotheses: 1, Confidence: "high", Correct: false},
		{Outcome: OutcomeHealthy, Hypotheses: 1, Confidence: "low", Correct: true, Abstained: true},
	})
	if only.RatedRuns != 0 || only.UnratedRuns != 0 || only.Brier != -1 || only.Overconfident != 0 || only.Reliability[2].Runs != 0 ||
		only.AbstainRuns != 2 || only.Abstentions != 1 || only.DiagnoseRuns != 0 {
		t.Errorf("non-diagnose only = %+v", only)
	}
	// A report written before outcomes existed has Outcome "": its runs are
	// diagnose runs. Hand-computed: 0.1² + 0.6² + 0.7² + 0.9² = 1.67 over 4.
	old4 := calibrate([]RunResult{
		{Outcome: OutcomeDiagnose, Confidence: "high", Correct: true},
		{Outcome: OutcomeDiagnose, Confidence: "medium", Correct: false},
		{Outcome: OutcomeDiagnose, Confidence: "low", Correct: true},
		{Outcome: "", Confidence: "high", Correct: false},
	})
	if old4.DiagnoseRuns != 4 || old4.RatedRuns != 4 || old4.Overconfident != 1 || math.Abs(old4.Brier-0.4175) > 1e-9 {
		t.Errorf("pre-outcome report = %+v", old4)
	}
	// update_hypothesis accepts only lower-case words; anything else is unrated.
	if odd := calibrate([]RunResult{{Outcome: OutcomeDiagnose, Confidence: "High", Correct: true, Hypotheses: 1}}); odd.RatedRuns != 0 || odd.UnratedRuns != 1 {
		t.Errorf("odd confidence = %+v", odd)
	}

	empty := calibrate(nil)
	if empty.Brier != -1 || empty.AbstentionRate != -1 || empty.FalseAbstentionRate != -1 || empty.Reliability[2].Accuracy != -1 {
		t.Errorf("empty = %+v", empty)
	}

	rep := buildReport(Config{Runs: 1}, []Scenario{{Name: "a"}}, results, timeZero())
	if rep.Calibration == nil || rep.Evidence == nil || rep.Calibration.Overconfident != 1 {
		t.Fatalf("report calibration = %+v", rep.Calibration)
	}
	var b bytes.Buffer
	if err := rep.WriteMarkdown(&b); err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{"Brier 0.286 over 5 rated run(s), 1 without a hypothesis", "overconfident (high and wrong) 1",
		"| high (0.9) | 2 | 1 | 50% |", "held back on 3/3 healthy/abstain/ask run(s) (100%)", "false abstention on 2/6 diagnose run(s) (33%)"} {
		if !strings.Contains(b.String(), w) {
			t.Errorf("markdown lacks %q:\n%s", w, b.String())
		}
	}
	// A report from before calibration existed still renders.
	old := Report{Label: "old"}
	b.Reset()
	if err := old.WriteMarkdown(&b); err != nil || strings.Contains(b.String(), "Calibration") {
		t.Errorf("old report: err %v\n%s", err, b.String())
	}
}

func TestEvidenceTotals(t *testing.T) {
	e := evidenceTotals([]RunResult{
		{ToolCalls: 4, ToolCallsOK: 3, ToolErrors: 1, FactsFound: []string{"a", "b"}, FactsMissing: []string{"c"},
			SupportedClaims: []string{"x"}},
		{ToolCalls: 2, ToolCallsOK: 2, FactsMissing: []string{"a"}, UnsupportedClaims: []string{"x"}},
	})
	want := EvidenceTotals{ToolCallsAttempted: 6, ToolCallsOK: 5, ToolErrors: 1, FactsExpected: 4, FactsFound: 2, Claims: 2, ClaimsSupported: 1}
	if e != want {
		t.Errorf("totals = %+v, want %+v", e, want)
	}
}

// TestEveryScenarioCanPass: for every scenario, a focused run that calls the
// tool of each expected fact and evidence tool (with a recorded input that
// establishes it) and then answers as the outcome expects passes with 100.
// It proves the facts, claims and layers of each file are reachable by a real
// tool call, not only present somewhere in the YAML.
func TestEveryScenarioCanPass(t *testing.T) {
	fixes := map[string][]events.ProposalStep{
		"schema-change": {ps("schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"NONE"}`),
			ps("connect_restart_task", `{"connector":"pg-orders-connector","task":0}`)},
		"slot-inactive-wal-growth":   {ps("connect_resume_connector", `{"connector":"pg-orders-connector"}`)},
		"recurring-connector-paused": {ps("connect_resume_connector", `{"connector":"pg-orders-connector"}`)},
	}
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	for _, sc := range scs {
		t.Run(sc.Name, func(t *testing.T) {
			var script []step
			called := map[string]bool{}
			// call reads tool until it gets a successful result that ok
			// accepts. Only recordings the run can reach before any fix count
			// (start-phase or untagged); the k-th result of a sequence takes
			// k+1 calls.
			call := func(tool string, ok func(Observation) bool) {
				for _, r := range sc.Recordings {
					if r.Tool != tool || (r.Phase != "" && r.Phase != sc.startPhase()) {
						continue
					}
					for k, o := range recordingResults(r) {
						if o.Error == "" && ok(o) {
							for range k + 1 {
								script = append(script, step{tool: tool, input: r.Input})
							}
							called[tool] = true
							return
						}
					}
				}
				t.Fatalf("no successful %s recording to call", tool)
			}
			for _, f := range sc.Expect.ExpectedFacts {
				call(f.Tool, func(o Observation) bool { return factIn(f, o.Summary) })
			}
			for _, tool := range sc.Expect.EvidenceTools {
				if !called[tool] {
					call(tool, func(Observation) bool { return true })
				}
			}
			switch sc.Expect.outcome() {
			case OutcomeHealthy:
				script = append(script, step{message: "The pipeline is healthy; nothing to fix."})
			case OutcomeAbstain:
				script = append(script, step{hypothesis: "Cause unknown from this evidence.", confidence: "low"})
			case OutcomeAsk:
				script = append(script, step{idle: true}, step{message: "Which of these do you mean?"})
			default:
				h, ok := hypotheses[sc.Name]
				if !ok {
					t.Fatalf("no good hypothesis for %s in scoring_test.go", sc.Name)
				}
				script = append(script, step{hypothesis: h.good})
				if fix := fixes[sc.Name]; len(fix) > 0 {
					script = append(script, proposal(fix...))
				}
			}
			script = append(script, step{idle: true})
			// After an executed fix, re-check the state in the recovered phase.
			if rec := sc.recoveredPhases(); len(rec) > 0 {
				for _, r := range sc.Recordings {
					if res := recordingResults(r); rec[r.Phase] && len(res) > 0 && res[0].Error == "" {
						script = append(script, step{tool: r.Tool, input: r.Input})
						break
					}
				}
			}
			r, _ := evalOnce(t, sc, script)
			if !r.Pass || r.Score != 100 || len(r.Misses) != 0 {
				t.Errorf("pass=%v score=%v misses=%v failures=%v facts missing=%v", r.Pass, r.Score, r.Misses, r.Failures, r.FactsMissing)
			}
		})
	}
}

// rawTranscript builds a transcript from events in the given order, for
// orderings the scripted runner never produces.
func rawTranscript(t *testing.T, evs ...any) Transcript {
	t.Helper()
	var tr Transcript
	for i := 0; i < len(evs); i += 2 {
		b, err := json.Marshal(evs[i+1])
		if err != nil {
			t.Fatal(err)
		}
		tr.Events = append(tr.Events, RecordedEvent{Type: evs[i].(events.Type), Payload: b})
	}
	return tr
}

// TestFinalHypothesisSnapshot: the final hypothesis is judged on the steps as
// they were when it was posted. A result that arrives after it, or a step that
// runs after it, does not back it.
func TestFinalHypothesisSnapshot(t *testing.T) {
	sc := loadSeed()
	const summary = "Task 0: FAILED: Schema being registered is incompatible; error code: 409\nValue.amount: int → bytes(decimal 12,2)"
	hyp := goodSchemaChangeScript()[3].hypothesis
	call := func(id string) []any {
		return []any{
			events.AgentStepStart, events.AgentStepStartPayload{StepID: id, Title: "connect_status"},
			events.AgentToolCall, events.AgentToolCallPayload{StepID: id, Tool: "connect_status"},
		}
	}
	result := []any{events.AgentToolResult, events.AgentToolResultPayload{StepID: "s_1", Tool: "connect_status", Summary: summary}}
	hypothesis := []any{events.AgentHypothesis, events.AgentHypothesisPayload{Text: hyp, BasedOnSteps: []string{"s_1"}, Confidence: "high"}}
	join := func(parts ...[]any) []any {
		var out []any
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	tests := []struct {
		name  string
		evs   []any
		backs bool
	}{
		{"result before the hypothesis", join(call("s_1"), result, hypothesis), true},
		{"result after the hypothesis", join(call("s_1"), hypothesis, result), false},
		{"step after the hypothesis", join(hypothesis, call("s_1"), result), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Judge(sc, rawTranscript(t, tt.evs...), nil, false)
			if r.CitesSteps != tt.backs || (len(r.SupportedClaims) == 1) != tt.backs {
				t.Errorf("cites %v supported %v, want %v", r.CitesSteps, r.SupportedClaims, tt.backs)
			}
			if found := slices.Contains(r.FactsFound, "f_task_failed"); found != tt.backs {
				t.Errorf("facts found %v, want f_task_failed %v", r.FactsFound, tt.backs)
			}
		})
	}
}

func TestAsksQuestion(t *testing.T) {
	tests := []struct {
		text string
		want bool
	}{
		{"Do you want me to restart task 0?", true},
		{"Restart now? Or wait for the DBA.", true},
		{`Should I wait ("for the DBA")?`, true},
		{"See GET /connectors/pg-orders-connector/status?expand=info for details.", false},
		{"Waiting for the DBA.", false},
	}
	for _, tt := range tests {
		if got := asksQuestion(tt.text); got != tt.want {
			t.Errorf("asksQuestion(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
}
