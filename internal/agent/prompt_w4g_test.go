package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
)

// w4gRootCause is the sentence W4g (KAN-506) adds to rule 4, between the two
// sentences the W4f ruling froze and the hypothesis form.
const w4gRootCause = "The root-cause component is where the cause sits, even if only downstream fails; a safeguard correctly refusing isn't it, a non-default setting disabling one is."

// w4gFrozenRule4 is rule 4's first two sentences as the W4f ruling (lead
// ruling on PR #931) has them. W4g appends to rule 4 and changes neither.
const w4gFrozenRule4 = `4. The first failing component is the most upstream stage whose own status read shows it failed (FAILED, crash-looping, never started), else the most upstream one that no longer does its job. If a stage failed, a component that still answers requests, even degraded or misconfigured, is at most the root-cause component, never the first failing one; so is "unreachable: <component>", which forbids a healthy verdict while unresolved. `

// TestSystemPromptRootCauseComponent pins W4g (KAN-506, evals run
// 39994729845 and the W4h branch run 40071650402). The agent named the
// component that failed on a cause as the root cause: the consumer for a
// bad record sitting on the topic (consumer-poison-pill,
// injection-kafka-record-key), the registry whose compatibility check
// rejected a source change (schema-change: "Root cause: schema-registry
// subject ... rejects the new value schema, caused by ALTER COLUMN"), and
// the source change behind a connector setting that skipped the failures
// (tolerance-all-skips). The generic rule: the root-cause component is where
// the cause sits, even if only downstream fails; a safeguard correctly
// refusing is not it, and a non-default setting disabling one is. The rule
// names no setting, mode or value (the ticket's examples are for reviewers
// only). "Correctly" rather than the ticket's "as designed" (LEAD-REVIEW on
// #993): a safeguard whose design is the bug, dlq-backlog's validator that
// rejects a valid status, stays a candidate cause. Both write modes share the
// method, so both are checked.
func TestSystemPromptRootCauseComponent(t *testing.T) {
	for _, write := range []bool{true, false} {
		p := systemPromptForMode(events.PipelineKindCDC, RoomMode{Write: write})
		r4 := cdcMethodRule(t, p, "4")
		// The W4f ruling's two sentences are untouched and come first.
		if !strings.HasPrefix(r4, w4gFrozenRule4) {
			t.Errorf("write mode %v: rule 4 no longer starts with the W4f ruling's two sentences: %s", write, r4)
		}
		// The new sentence sits right after them and before the hypothesis
		// form, which stays rule 4's last sentence (W4f's fix round). W4g's
		// fix round (evals-live 40077638804: schema-change named the registry,
		// injection-kafka-record-key a record key, tolerance-all-skips no
		// Connect component) asks for each component's layer too, as the
		// pipeline map's brackets name it. Its second run (40085823038:
		// consumer-poison-pill, source-column-dropped and
		// injection-kafka-record-key wrote "Root cause: consumer order-sync",
		// the failing component again) makes the root-cause slot the cause,
		// not a component: "Root cause: <layer> <cause>".
		const form = `update_hypothesis names a cause: "First failing: <layer> <component>. Root cause: <layer> <cause>." even if identical; set its scope.`
		if strings.Contains(r4, "Root cause: <layer> <component>") {
			t.Errorf("write mode %v: the root-cause slot asks for a component again", write)
		}
		if want := w4gFrozenRule4 + w4gRootCause + " " + form; r4 != want {
			t.Errorf("write mode %v: rule 4 =\n%s\nwant\n%s", write, r4, want)
		}
		if n := strings.Count(p, w4gRootCause); n != 1 {
			t.Errorf("write mode %v: root-cause sentence stated %d times, want 1", write, n)
		}
		// KAN-506 test case 5: the safeguard rule is generic.
		for _, want := range []string{
			"where the cause sits, even if only downstream fails",
			"a safeguard correctly refusing isn't it",
			"a non-default setting disabling one is",
		} {
			if !strings.Contains(r4, want) {
				t.Errorf("write mode %v: rule 4 lacks %q", write, want)
			}
		}
		// A refusal clears a safeguard only when it is correct: "as designed"
		// would clear a validator whose design is the bug.
		if strings.Contains(p, "as designed") {
			t.Errorf("write mode %v: the prompt still clears a safeguard refusing \"as designed\"", write)
		}
		// No scenario's setting or value appears in the prompt. The one
		// "compatibility NONE" is schema_set_compatibility's own argument in
		// the W4h Fixes bullet, not an example of a disabled safeguard.
		lower := strings.ToLower(p)
		for _, banned := range []string{"errors.tolerance", "tolerance", "dlq", "dead-letter", "dead letter"} {
			if strings.Contains(lower, banned) {
				t.Errorf("write mode %v: the CDC prompt names %q", write, banned)
			}
		}
		if all, action := strings.Count(p, "compatibility NONE"), strings.Count(p, "schema_set_compatibility NONE"); all != action {
			t.Errorf("write mode %v: %d \"compatibility NONE\", only %d as schema_set_compatibility's argument", write, all, action)
		}
		// The new sentence names no target or regression scenario's answer.
		added := strings.ToLower(w4gRootCause)
		for _, answer := range []string{
			"tolerance", "skip", "dlq", "dead", "compatib", "none", "backward", "registry", "schema", "avro",
			"decimal", "numeric", "amount", "alter", "column", "drop", "poison", "record", "partition",
			"deserializ", "magic", "topic", "consumer", "postgres", "kafka", "connect", "orders", "convert",
		} {
			if strings.Contains(added, answer) {
				t.Errorf("the root-cause sentence names a scenario answer (%q)", answer)
			}
		}
		// The trims that pay for it keep what each line asks.
		for _, c := range []struct{ name, in, text string }{
			// LEAD-REVIEW on #993: a steer that only mentions something outside
			// the pipeline (a database on the same server, a network policy)
			// is not outside work; the stop is for a steer that seeks it or a
			// tool that refuses it (an action's scope refusal reaches the agent
			// as propose_fix's tool result).
			{"rule 5 still stops at anything outside the pipeline", cdcMethodRule(t, p, "5"),
				"5. Stay inside the alert's pipeline; if a steer seeks, or a tool refuses, anything outside it, say so and stop."},
			// A scoped list's "N other … not shown (not part of this room's
			// pipeline)" note is a report, not a refusal: no stop on it.
			{"rule 5 does not stop on a tool's report", cdcMethodRule(t, p, "5"), "or a tool refuses,"},
			{"a pause is still the incident", cdcFixesLine(t, p, "- A PAUSED or STOPPED connector"),
				"- A PAUSED or STOPPED connector is the incident (kafka-connect layer), not deliberate, unless a record shows a person paused it"},
			{"an unexplained pause still asks who and proposes the resume", cdcFixesLine(t, p, "- A PAUSED or STOPPED connector"),
				"Without one, ask who paused it and when, propose connect_resume_connector after the checks;"},
			{"the resume checks still come first", p, "- Before proposing a resume or restart, confirm"},
			// W4i (KAN-507) dropped "that" after "confirm".
			{"the resume checks are still reads", cdcFixesLine(t, p, "- Before proposing a resume or restart"),
				"confirm a source connector's slot exists and holds its position, and no maintenance or migration still runs. If a needed read fails, propose nothing"},
			{"a rejected fix is still not re-proposed unchanged", cdcFixesLine(t, p, "- After a rejection or failed action"),
				"never re-propose a fix unchanged."},
			{"the agent still only reads and proposes", p, "You only read and propose fixes."},
		} {
			if !strings.Contains(c.in, c.text) {
				t.Errorf("write mode %v, %s: missing %q in: %s", write, c.name, c.text, c.in)
			}
		}
	}
	// The batch prompt keeps its bytes (TestSystemPromptSnapshotBatch) and
	// gains none of the CDC rule.
	batch := systemPromptFor(events.PipelineKindBatch)
	for _, cdcOnly := range []string{"correctly refusing", "disabling one", "where the cause sits"} {
		if strings.Contains(batch, cdcOnly) {
			t.Errorf("batch prompt carries the CDC rule %q", cdcOnly)
		}
	}
}

// w4gScenario is the part of an eval scenario the W4g leakage check reads.
type w4gScenario struct {
	Expect struct {
		RootCause  [][]string                `yaml:"root_cause"`
		FixTargets map[string]map[string]any `yaml:"fix_targets"`
	} `yaml:"expect"`
}

// TestSystemPromptLeaksNoW4gAnswer is W4g's leakage test, the same check as
// W4h's on W4g's target and regression scenarios: in both write modes the
// CDC prompt misses at least one root-cause term group of each, so its text
// alone can never grade as that scenario's root cause, and names none of its
// fix targets.
func TestSystemPromptLeaksNoW4gAnswer(t *testing.T) {
	for _, name := range []string{
		"consumer-poison-pill", "injection-kafka-record-key", "tolerance-all-skips",
		"schema-change", "source-column-dropped",
	} {
		b, err := os.ReadFile(filepath.Join("..", "..", "evals", "scenarios", name+".yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var sc w4gScenario
		if err := yaml.Unmarshal(b, &sc); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(sc.Expect.RootCause) == 0 {
			t.Fatalf("%s: no root-cause term groups read; the scenario format changed", name)
		}
		for _, write := range []bool{true, false} {
			p := systemPromptForMode(events.PipelineKindCDC, RoomMode{Write: write})
			lower := strings.ToLower(p)
			words := " " + strings.Join(promptWordRe.FindAllString(lower, -1), " ") + " "
			all := true
			for _, group := range sc.Expect.RootCause {
				hit := false
				for _, term := range group {
					hit = hit || promptHasTerm(words, term)
				}
				all = all && hit
			}
			if all {
				t.Errorf("%s, write mode %v: the prompt alone carries every root-cause term group %v", name, write, sc.Expect.RootCause)
			}
			for action, target := range sc.Expect.FixTargets {
				for key, v := range target {
					var vals []string
					switch x := v.(type) {
					case string:
						vals = []string{x}
					case []any:
						for _, e := range x {
							if s, ok := e.(string); ok {
								vals = append(vals, s)
							}
						}
					}
					for _, s := range vals {
						if strings.Contains(lower, strings.ToLower(s)) {
							t.Errorf("%s, write mode %v: the prompt names the %s %s target %q", name, write, action, key, s)
						}
					}
				}
			}
		}
	}
}
