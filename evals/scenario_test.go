package evals

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSeedScenarioIsValid(t *testing.T) {
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	// Every scenario in the folder (C7 adds more) must stay valid against the
	// live tool and action definitions; a renamed tool breaks this test.
	if err := ValidateAll(scs, defs); err != nil {
		t.Fatal(err)
	}
	seed := loadSeed()
	if seed.Name != "schema-change" || seed.Pipeline.ID != "orders" || len(seed.Recordings) == 0 {
		t.Errorf("seed = %+v", seed)
	}
}

func TestParseScenarioRejectsUnknownFields(t *testing.T) {
	_, err := ParseScenario([]byte("name: x\nexpect:\n  root_casue: [[a]]\n"))
	if err == nil || !strings.Contains(err.Error(), "root_casue") {
		t.Fatalf("err = %v, want unknown field error", err)
	}
}

func TestLoadScenariosErrors(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadScenarios(dir); err == nil {
		t.Error("empty dir accepted")
	}
	body := "name: dup\npipeline: {id: p, topic_prefix: t}\nalert: {title: t}\nexpect: {root_cause: [[a]]}\n"
	for _, f := range []string{"a.yaml", "b.yaml"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := LoadScenarios(dir); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("err = %v, want duplicate name", err)
	}
	if _, err := LoadScenario(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Error("missing file accepted")
	}
}

func TestLoadScenarioDefaultsNameToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lag-spike.yaml")
	if err := os.WriteFile(path, []byte("alert: {title: t}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := LoadScenario(path)
	if err != nil || s.Name != "lag-spike" {
		t.Fatalf("s.Name = %q, err %v", s.Name, err)
	}
}

func TestValidate(t *testing.T) {
	tools := map[string][]string{"connect_status": {"connector"}, "connect_list_connectors": {}}
	acts := []string{"connect_restart_task"}
	base := func() Scenario {
		return Scenario{
			Name:     "x",
			Pipeline: pipelineOrders(),
			Alert:    Alert{Title: "t"},
			Recordings: []Recording{
				{Tool: "connect_status", Summary: "ok"},
			},
			Expect: Expect{RootCause: [][]string{{"a"}}},
		}
	}
	tests := []struct {
		name   string
		mutate func(*Scenario)
		want   string // "" = valid
	}{
		{"valid", func(*Scenario) {}, ""},
		{"no pipeline", func(s *Scenario) { s.Pipeline.ID = "" }, "pipeline.id"},
		{"no topic prefix", func(s *Scenario) { s.Pipeline.TopicPrefix = "" }, "pipeline.topic_prefix"},
		{"no title", func(s *Scenario) { s.Alert.Title = " " }, "alert.title"},
		{"no root cause", func(s *Scenario) { s.Expect.RootCause = nil }, "root_cause is empty"},
		{"empty group", func(s *Scenario) { s.Expect.RootCause = [][]string{{}} }, "empty group"},
		{"blank term", func(s *Scenario) { s.Expect.RootCause = [][]string{{" "}} }, "blank term"},
		{"unknown tool", func(s *Scenario) { s.Recordings[0].Tool = "nope" }, `unknown tool "nope"`},
		{"known input field", func(s *Scenario) { s.Recordings[0].Input = map[string]any{"connector": "c"} }, ""},
		{"typo in input field", func(s *Scenario) { s.Recordings[0].Input = map[string]any{"connector_name": "c"} }, `recordings[0] (connect_status): unknown input field "connector_name"`},
		{"input on tool without inputs", func(s *Scenario) {
			s.Recordings = append(s.Recordings, Recording{Tool: "connect_list_connectors", Input: map[string]any{"prefix": "pg"}, Summary: "x"})
		}, `recordings[1] (connect_list_connectors): unknown input field "prefix"`},
		{"no result", func(s *Scenario) { s.Recordings[0].Summary = "" }, "needs summary or error"},
		{"bad data", func(s *Scenario) { s.Recordings[0].Data = map[string]any{"f": func() {}} }, "data does not convert"},
		{"unknown evidence", func(s *Scenario) { s.Expect.EvidenceTools = []string{"nope"} }, "evidence_tools"},
		{"unknown fix action", func(s *Scenario) { s.Expect.Fix = [][]string{{"drop_slot"}} }, `unknown action "drop_slot"`},
		{"unknown forbidden", func(s *Scenario) { s.Expect.ForbiddenActions = []string{"x"} }, "forbidden_actions"},
		{"unknown layer", func(s *Scenario) { s.Expect.FirstFailingLayer = "network" }, "unknown layer"},
		{"bad input kind", func(s *Scenario) { s.Inputs = []Input{{Kind: "shout", Text: "hi"}} }, "unknown kind"},
		{"empty input", func(s *Scenario) { s.Inputs = []Input{{Kind: "steer"}} }, "text is empty"},
		{"negative budget", func(s *Scenario) { s.Expect.MaxToolCalls = -1 }, "negative"},

		// E2: outcomes, layers, facts and claims.
		{"unknown root cause layer", func(s *Scenario) { s.Expect.RootCauseLayer = "network" }, "expect.root_cause_layer: unknown layer"},
		{"unknown outcome", func(s *Scenario) { s.Expect.Outcome = "shrug" }, `unknown outcome "shrug"`},
		{"healthy needs no root cause", func(s *Scenario) { s.Expect.Outcome, s.Expect.RootCause = OutcomeHealthy, nil }, ""},
		{"root cause on a healthy scenario", func(s *Scenario) { s.Expect.Outcome = OutcomeHealthy }, "expect.root_cause: only graded when outcome is diagnose"},
		{"fix on an abstain scenario", func(s *Scenario) {
			s.Expect.Outcome, s.Expect.RootCause, s.Expect.Fix = OutcomeAbstain, nil, [][]string{{"connect_restart_task"}}
		}, "expect.fix: only graded"},
		{"claude judge on an ask scenario", func(s *Scenario) { s.Expect.Outcome, s.Expect.RootCause, s.Judge = OutcomeAsk, nil, JudgeClaude }, "outcome ask has none"},
		{"valid fact and claim", func(s *Scenario) {
			s.Expect.ExpectedFacts = []Fact{{ID: "f_ok", Tool: "connect_status", Match: []string{"ok"}, Required: true}}
			s.Expect.Claims = []Claim{{About: ClaimRootCause, Facts: []string{"f_ok"}}}
		}, ""},
		{"fact term in no recording", func(s *Scenario) {
			s.Expect.ExpectedFacts = []Fact{{ID: "f_x", Tool: "connect_status", Match: []string{"okay"}}}
		}, `expect.expected_facts[0] (f_x): no successful connect_status recording contains all of ["okay"]`},
		{"fact terms split over two recordings", func(s *Scenario) {
			s.Recordings = append(s.Recordings, Recording{Tool: "connect_status", Input: map[string]any{"connector": "c"}, Summary: "running"})
			s.Expect.ExpectedFacts = []Fact{{ID: "f_x", Tool: "connect_status", Match: []string{"ok", "running"}}}
		}, "no successful connect_status recording contains all of"},
		{"fact only in an error recording", func(s *Scenario) {
			s.Recordings = append(s.Recordings, Recording{Tool: "connect_status", Input: map[string]any{"connector": "c"}, Summary: "task FAILED", Error: "connect: 503"})
			s.Expect.ExpectedFacts = []Fact{{ID: "f_x", Tool: "connect_status", Match: []string{"failed"}}}
		}, "no successful connect_status recording contains"},
		{"fact on another tool's recording", func(s *Scenario) {
			s.Expect.ExpectedFacts = []Fact{{ID: "f_x", Tool: "connect_list_connectors", Match: []string{"ok"}}}
		}, "no successful connect_list_connectors recording"},
		{"fact with unknown tool", func(s *Scenario) {
			s.Expect.ExpectedFacts = []Fact{{ID: "f_x", Tool: "nope", Match: []string{"ok"}}}
		}, `(f_x): unknown tool "nope"`},
		{"fact without id", func(s *Scenario) {
			s.Expect.ExpectedFacts = []Fact{{Tool: "connect_status", Match: []string{"ok"}}}
		}, "expected_facts[0]: id is empty"},
		{"duplicate fact id", func(s *Scenario) {
			f := Fact{ID: "f_x", Tool: "connect_status", Match: []string{"ok"}}
			s.Expect.ExpectedFacts = []Fact{f, f}
		}, `duplicate id "f_x"`},
		{"fact without terms", func(s *Scenario) {
			s.Expect.ExpectedFacts = []Fact{{ID: "f_x", Tool: "connect_status"}}
		}, "match is empty"},
		{"fact term in camelCase", func(s *Scenario) {
			s.Expect.ExpectedFacts = []Fact{{ID: "f_x", Tool: "connect_status", Match: []string{"taskFailed"}}}
		}, "write it lower-case"},
		{"claim on an unknown fact", func(s *Scenario) {
			s.Expect.Claims = []Claim{{About: ClaimRootCause, Facts: []string{"f_nope"}}}
		}, `unknown fact "f_nope"`},
		{"claim about something else", func(s *Scenario) {
			s.Expect.ExpectedFacts = []Fact{{ID: "f_ok", Tool: "connect_status", Match: []string{"ok"}}}
			s.Expect.Claims = []Claim{{About: "fix", Facts: []string{"f_ok"}}}
		}, `unknown about "fix"`},
		{"claim without facts", func(s *Scenario) { s.Expect.Claims = []Claim{{About: ClaimRootCause}} }, "facts is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := base()
			tt.mutate(&s)
			err := s.Validate(tools, acts)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestHumanInputs(t *testing.T) {
	s := Scenario{Inputs: []Input{
		{Kind: "steer", Text: "a"},
		{Kind: "comment", From: "Kabir", CommentID: "c_1", Text: "b"},
		{Kind: "approval_result", Text: "c"},
	}}
	got := s.humanInputs()
	if got[0].Actor.Name != "driver" || got[0].Seq != 1 {
		t.Errorf("steer = %+v", got[0])
	}
	if got[1].Actor.Name != "Kabir" || got[1].CommentID != "c_1" {
		t.Errorf("comment = %+v", got[1])
	}
	if got[2].Actor.Kind != "system" {
		t.Errorf("approval = %+v", got[2])
	}
}

// TestReadmeExampleIsValid keeps the documented format honest: C7 writes
// scenarios from README.md, so its example must parse and validate.
func TestReadmeExampleIsValid(t *testing.T) {
	b, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	_, rest, ok := strings.Cut(string(b), "### Example\n\n```yaml\n")
	if !ok {
		t.Fatal("README has no yaml example")
	}
	example, _, ok := strings.Cut(rest, "```")
	if !ok {
		t.Fatal("unterminated yaml example")
	}
	s, err := ParseScenario([]byte(example))
	if err != nil {
		t.Fatal(err)
	}
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateAll([]Scenario{s}, defs); err != nil {
		t.Fatal(err)
	}
}

func TestToolInputsUsesLiveSchemas(t *testing.T) {
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	in := ToolInputs(defs)
	if got := in["connect_status"]; len(got) != 1 || got[0] != "connector" {
		t.Errorf("connect_status fields = %v", got)
	}
	if got, ok := in["connect_list_connectors"]; !ok || len(got) != 0 {
		t.Errorf("connect_list_connectors fields = %v (present %v)", got, ok)
	}
}

// TestSeedMatchesRealFormats pins the format rules the seed must follow so
// replayed summaries stay byte-identical to the real tools'.
func TestSeedMatchesRealFormats(t *testing.T) {
	seed := loadSeed()
	// dbtools and the topology renderer end every line with "\n"; kafkatools
	// join lines and end without one.
	endsWithNewline := map[string]bool{
		"pipeline_map": true, "trace_record": true,
		"pg_replication_slots": true, "pg_recent_ddl": true, "pg_long_transactions": true,
	}
	for i, r := range seed.Recordings {
		if r.Error != "" {
			continue
		}
		if got, want := strings.HasSuffix(r.Summary, "\n"), endsWithNewline[r.Tool]; got != want {
			t.Errorf("recordings[%d] (%s): trailing newline = %v, want %v", i, r.Tool, got, want)
		}
		if strings.HasSuffix(r.Summary, "\n\n") {
			t.Errorf("recordings[%d] (%s): more than one trailing newline", i, r.Tool)
		}
	}
	var mapSummary string
	for _, r := range seed.Recordings {
		if r.Tool == "pipeline_map" && r.Input["pipeline_id"] == seed.Pipeline.ID {
			mapSummary = r.Summary
		}
	}
	if mapSummary == "" || seed.Alert.PipelineMap != mapSummary {
		t.Errorf("alert.pipeline_map differs from the pipeline_map recording:\n%q\n%q", seed.Alert.PipelineMap, mapSummary)
	}
}
