package evals

import (
	"strings"
	"testing"
)

// altScenario is a two-sided cause: a consumer down for longer than the
// topic's retention. The label puts the cause in kafka (retention deleted
// the records) and allows consumer (it was down too long) as an alternative;
// the first failing layer stays consumer.
func altScenario() Scenario {
	sc := loadSeed()
	sc.Expect.FirstFailingLayer = "consumer"
	sc.Expect.RootCauseLayer = "kafka"
	sc.Expect.RootCauseLayerAlt = []string{"postgres"}
	return sc
}

// TestRootCauseLayerAlt: the root-cause-layer grade passes on root_cause_layer
// or on any alternative, and fails when the hypothesis names neither; the
// first-failing-layer grade is untouched by the alternatives.
func TestRootCauseLayerAlt(t *testing.T) {
	const cause = "The Schema Registry rejected (409) the new Avro schema after amount changed to numeric(12,2)"
	tests := []struct {
		name                     string
		hypothesis               string
		wantCauseLayer, wantPass bool
		wantFailure              string
	}{
		{name: "names root_cause_layer", hypothesis: cause + "; the Kafka topic orders.public.orders carries the bad records. The order-sync consumer stalled first.",
			wantCauseLayer: true, wantPass: true},
		{name: "names the alternative", hypothesis: cause + " by an ALTER TABLE in Postgres. The order-sync consumer stalled first.",
			wantCauseLayer: true, wantPass: true},
		{name: "names neither", hypothesis: cause + ". The order-sync consumer stalled first.",
			wantFailure: "root-cause layer not named: kafka"},
		{name: "alternative denied", hypothesis: cause + ". The order-sync consumer stalled first; not a Postgres problem.",
			wantFailure: "root-cause layer not named: kafka"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := altScenario()
			good := goodSchemaChangeScript()
			script := []step{good[0], good[1], good[2], {hypothesis: tt.hypothesis}, good[4], {idle: true}}
			r := Judge(sc, transcriptOf(t, sc, script), nil, false)
			if r.RootCauseLayerNamed != tt.wantCauseLayer || !r.LayerNamed {
				t.Errorf("cause layer/layer = %v/%v, want %v/true; failures %v", r.RootCauseLayerNamed, r.LayerNamed, tt.wantCauseLayer, r.Failures)
			}
			if r.Pass != tt.wantPass {
				t.Errorf("pass = %v, want %v; failures %v", r.Pass, tt.wantPass, r.Failures)
			}
			if joined := strings.Join(r.Failures, "; "); tt.wantFailure != "" && !strings.Contains(joined, tt.wantFailure) {
				t.Errorf("failures %q lack %q", joined, tt.wantFailure)
			}
			// The tri-state pass agrees with Judge.
			if g := GradeKeywords(sc, tt.hypothesis); (g.RootCauseLayer == GradePass) != tt.wantCauseLayer {
				t.Errorf("GradeKeywords root-cause layer = %v, want pass=%v", g.RootCauseLayer, tt.wantCauseLayer)
			}
		})
	}
}

// TestRootCauseLayerAltUnchangedWithoutField: a scenario without the field
// grades exactly as before, so the report column and the corpus keep their
// meaning; the tri-state pass and the judge prompt see no alternative.
func TestRootCauseLayerAltUnchangedWithoutField(t *testing.T) {
	sc := loadSeed()
	if sc.Expect.RootCauseLayerAlt != nil {
		t.Fatalf("schema-change sets root_cause_layer_alt %v", sc.Expect.RootCauseLayerAlt)
	}
	if got := sc.Expect.rootCauseLayers(); len(got) != 1 || got[0] != sc.Expect.RootCauseLayer {
		t.Errorf("rootCauseLayers = %v, want [%s]", got, sc.Expect.RootCauseLayer)
	}
	for _, text := range []string{"Connect failed after an ALTER TABLE in Postgres.", "Connect failed."} {
		if keywordRootCauseLayer(sc, text) != keywordLayer(sc, sc.Expect.RootCauseLayer, text) {
			t.Errorf("%q: alt-aware grade differs from the single-layer grade", text)
		}
	}
	p := judgePrompt(sc, "h")
	if !strings.Contains(p, "Root-cause layer: postgres\n") || strings.Contains(p, "alternatively") {
		t.Errorf("prompt changed for a scenario without alternatives:\n%s", p)
	}
	if e := (Expect{}); e.rootCauseLayers() != nil {
		t.Errorf("rootCauseLayers of an empty Expect = %v", e.rootCauseLayers())
	}
}

// TestRootCauseLayerAltJudgePrompt: the judge's layer question names every
// defensible layer, only for a scenario with alternatives.
func TestRootCauseLayerAltJudgePrompt(t *testing.T) {
	sc := altScenario()
	sc.Expect.RootCauseLayerAlt = []string{"postgres", "warehouse"}
	p := judgePrompt(sc, "h")
	want := "Root-cause layer: kafka, or alternatively postgres or warehouse ("
	if !strings.Contains(p, want) || !strings.Contains(p, "First failing layer: consumer") {
		t.Errorf("prompt lacks %q:\n%s", want, p)
	}
}

// TestRootCauseLayerAltValidation: -validate refuses an unknown layer, a
// duplicate of root_cause_layer, an alternative equal to
// first_failing_layer (#684), a repeated alternative, an alternative without
// root_cause_layer and the field on a non-diagnose outcome.
func TestRootCauseLayerAltValidation(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Scenario)
		want string
	}{
		{"valid alternative", func(s *Scenario) { s.Expect.RootCauseLayerAlt = []string{"kafka"} }, ""},
		{"unknown layer", func(s *Scenario) { s.Expect.RootCauseLayerAlt = []string{"network"} },
			`expect.root_cause_layer_alt[0]: unknown layer "network"`},
		{"environment is not a layer", func(s *Scenario) { s.Expect.RootCauseLayerAlt = []string{"environment"} },
			`expect.root_cause_layer_alt[0]: unknown layer "environment"`},
		{"duplicate of root_cause_layer", func(s *Scenario) { s.Expect.RootCauseLayerAlt = []string{"postgres"} },
			`expect.root_cause_layer_alt[0]: "postgres" duplicates expect.root_cause_layer`},
		{"equal to first_failing_layer", func(s *Scenario) { s.Expect.RootCauseLayerAlt = []string{"kafka-connect"} },
			`expect.root_cause_layer_alt[0]: "kafka-connect" equals expect.first_failing_layer`},
		{"repeated alternative", func(s *Scenario) { s.Expect.RootCauseLayerAlt = []string{"kafka", "kafka"} },
			`expect.root_cause_layer_alt[1]: "kafka" repeated`},
		{"alternative without root_cause_layer", func(s *Scenario) {
			s.Expect.RootCauseLayer, s.Expect.RootCauseLayerAlt = "", []string{"kafka"}
		}, "expect.root_cause_layer_alt: needs expect.root_cause_layer"},
		{"alternative on a healthy scenario", func(s *Scenario) {
			s.Expect.Outcome, s.Expect.RootCause, s.Expect.RootCauseLayer, s.Expect.RootCauseLayerAlt = OutcomeHealthy, nil, "", []string{"kafka"}
		}, "expect.root_cause_layer_alt: only graded when outcome is diagnose"},
	}
	tools := map[string][]string{"connect_status": {"connector"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := Scenario{
				Name:       "x",
				Pipeline:   pipelineOrders(),
				Alert:      Alert{Title: "t"},
				Recordings: []Recording{{Tool: "connect_status", Summary: "ok"}},
				Expect:     Expect{RootCause: [][]string{{"a"}}, FirstFailingLayer: "kafka-connect", RootCauseLayer: "postgres"},
			}
			tt.edit(&sc)
			err := sc.Validate(tools, nil)
			switch {
			case tt.want == "" && err != nil:
				t.Errorf("unexpected error: %v", err)
			case tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)):
				t.Errorf("error %v lacks %q", err, tt.want)
			}
		})
	}
}
