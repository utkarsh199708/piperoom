package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
)

// w4iNegativeClaim is the clause W4i (KAN-507) puts in rule 1, right before
// the competing-explanation sentence: a negative claim needs its read.
// Clearing a component (calling it absent, healthy or unchanged) needs its own
// read as blaming it does.
const w4iNegativeClaim = "confirm a component with its own read before blaming or clearing it."

// w4iRule3 is rule 3 with its general clause kept (LEAD-REVIEW on #1004): a
// fact no tool returned, such as a trace_record result never read, stays
// forbidden whatever component it is about.
const w4iRule3 = `3. Cite evidence by step id; assert nothing tools didn't return. Re-reading an unchanged component, or re-running an answered read with other versions or windows, is waste; "not found" is the finding.`

// w4iRestart and w4iOffsets are the LEAD-ARCH scope addition on KAN-507 (the
// audit of main run 40076876491): blocked-slot restarted an UNASSIGNED task
// before the cause was removed, and rows-lost-after-retention proposed its fix
// before reading the log start. The restart stays in the proposal removing the
// cause (W4h's meaning: missing-grant proposed the grant alone), and the note
// counts records, not rows, above the log start (LEAD-REVIEW on #1004).
const (
	w4iRestart = "- Restart only a FAILED task, never an UNASSIGNED one, in the proposal removing its cause; after pg_add_publication_table, that table's pg_snapshot_signal."
	w4iOffsets = "- Reset source offsets only for lost positions; replay missing records above the log start (kafka_topic_offsets), else snapshot them."
	// w4iRestartMySQL is the restart bullet in a MySQL room: the replacer
	// swaps the publication clause for the schema history bullet.
	w4iRestartMySQL = "- Restart only a FAILED task, never an UNASSIGNED one, in the proposal removing its cause."
)

// w4iAdded is the text W4i adds, without the words its lines kept from the
// base: the leakage checks read it, tool names taken out (they are not
// answers).
var w4iAdded = []string{
	"before blaming or clearing it",
	"assert nothing tools didn't return",
	"Restart only a FAILED task, never an UNASSIGNED one, in the proposal",
	"replay missing records above the log start (kafka_topic_offsets), else snapshot them",
}

// w4iAddedText is w4iAdded in lower case without tool names.
func w4iAddedText() string {
	return promptTokenRe.ReplaceAllString(strings.ToLower(strings.Join(w4iAdded, "\n")), "")
}

// TestSystemPromptNegativeClaimNeedsItsRead pins W4i (KAN-507, evals run
// 39994729845: sasl-credential-rotated told the room the ACLs were intact
// without the kafka_acls read that shows it). The generic rule: the agent
// does not clear a component (call it absent, healthy or unchanged) without
// that component's own read, as it does not blame one without it. The clause
// sits in rule 1, beside the competing-explanation clause it sharpens ("rule
// out the nearest competing explanation with one read": the read is the
// competing cause's own). Rule 3 keeps its general clause, "assert nothing
// tools didn't return" (LEAD-REVIEW on #1004): a fact no tool returned stays
// forbidden whatever it is about (consumer-poison-pill's unbacked
// trace_record fact). The scope addition's two Fixes rewrites ride along, and
// the replacer carries all of it into the MySQL prompt. Word-neutral: rule
// 3's "a tool" became "tools" and the resume check lost "that".
func TestSystemPromptNegativeClaimNeedsItsRead(t *testing.T) {
	prompts := map[string]string{}
	for _, write := range []bool{true, false} {
		prompts["cdc, write "+boolWord(write)] = systemPromptForMode(events.PipelineKindCDC, RoomMode{Write: write})
		prompts["mysql, write "+boolWord(write)] = systemPromptForSource(events.PipelineKindCDC, events.SourceKindMySQL, RoomMode{Write: write})
	}
	for name, p := range prompts {
		// The leakage checks read text the prompt really has.
		for _, frag := range w4iAdded {
			if !strings.Contains(p, frag) {
				t.Errorf("%s: the prompt lacks W4i's %q", name, frag)
			}
		}
		line := cdcMethodLine(t, p, "   From the most downstream symptom")
		// The clause sits right before the competing-explanation sentence,
		// in the sentence that already asks for a component's own read.
		if want := "verifying (unconfirmed) ones; " + w4iNegativeClaim + " Before concluding, rule out the nearest competing explanation with one read;"; !strings.Contains(line, want) {
			t.Errorf("%s: rule 1 lacks %q next to the competing-explanation clause: %s", name, want, line)
		}
		if n := strings.Count(p, "before blaming or clearing it"); n != 1 {
			t.Errorf("%s: the negative-claim clause is stated %d times, want 1", name, n)
		}
		// Rule 3 keeps its general clause, stated once.
		if got := cdcMethodRule(t, p, "3"); got != w4iRule3 {
			t.Errorf("%s: rule 3 =\n%s\nwant\n%s", name, got, w4iRule3)
		}
		if n := strings.Count(p, "didn't return"); n != 1 {
			t.Errorf("%s: \"didn't return\" is stated %d times, want 1 (rule 3)", name, n)
		}
		for _, gone := range []string{"assert nothing a tool didn't return", "A FAILED task's restart", "after the step removing", "replay a missing range still on the topic", "missing rows above", "confirm that"} {
			if strings.Contains(p, gone) {
				t.Errorf("%s: the prompt still says %q", name, gone)
			}
		}
		// The scope addition's bullets, as the lead worded them.
		restart := w4iRestart
		if strings.HasPrefix(name, "mysql") {
			restart = w4iRestartMySQL
		}
		if got := cdcFixesLine(t, p, "- Restart only a FAILED task"); got != restart {
			t.Errorf("%s: restart bullet =\n%s\nwant\n%s", name, got, restart)
		}
		if got := cdcFixesLine(t, p, "- Reset source offsets"); got != w4iOffsets {
			t.Errorf("%s: offsets note =\n%s\nwant\n%s", name, got, w4iOffsets)
		}
		// The trims keep what the resume check asks.
		if !strings.Contains(cdcFixesLine(t, p, "- Before proposing a resume or restart"), "and no maintenance or migration still runs. If a needed read fails, propose nothing and say what's missing;") {
			t.Errorf("%s: the resume check lost its reads", name)
		}
		// The rules W4i must keep: rule 4's frozen sentences and W4g's
		// safeguard and form, and rule 5's steer and tool clause.
		if r4 := cdcMethodRule(t, p, "4"); !strings.HasPrefix(r4, w4gFrozenRule4+w4gRootCause+" update_hypothesis names a cause:") {
			t.Errorf("%s: rule 4 changed: %s", name, r4)
		}
		if r5 := cdcMethodRule(t, p, "5"); r5 != "5. Stay inside the alert's pipeline; if a steer seeks, or a tool refuses, anything outside it, say so and stop." {
			t.Errorf("%s: rule 5 changed: %s", name, r5)
		}
	}
	// The rule is generic: W4i's own words name no target or regression
	// scenario's answer (the cause, the trap story, the fix target).
	added := w4iAddedText()
	for _, answer := range []string{
		"sasl", "authenticat", "credential", "password", "secret", "rotat", "acl", "grant", "permission",
		"transaction", "blocker", "session", "xid", "48213", "slot",
		"retention", "purg", "expir", "delet", "446", "1301", "reprocess", "skip", "latest", "deploy",
		"publication", "refund", "order", "schema", "dlq", "toleran", "binlog", "server id",
	} {
		if strings.Contains(added, answer) {
			t.Errorf("a W4i line names a scenario answer (%q)", answer)
		}
	}
	// The batch prompt keeps its bytes (TestSystemPromptSnapshotBatch): its
	// own rule 3 still says what a tool did not return, and none of the CDC
	// wording reaches it.
	batch := systemPromptFor(events.PipelineKindBatch)
	if !strings.Contains(batch, "do not assert what a tool did not return") {
		t.Error("the batch prompt lost its rule 3")
	}
	for _, cdcOnly := range []string{"blaming or clearing", "nothing tools", "UNASSIGNED", "log start", "Restart only a FAILED task"} {
		if strings.Contains(batch, cdcOnly) {
			t.Errorf("batch prompt carries the CDC rule %q", cdcOnly)
		}
	}
}

func boolWord(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// w4iScenario is the part of an eval scenario the W4i leakage check reads.
type w4iScenario struct {
	Expect struct {
		RootCause          [][]string                `yaml:"root_cause"`
		RootCauseForbidden [][]string                `yaml:"root_cause_forbidden"`
		FixTargets         map[string]map[string]any `yaml:"fix_targets"`
	} `yaml:"expect"`
}

// TestSystemPromptLeaksNoW4iAnswer is W4i's leakage test, in the style of
// W4g's and W4h's, over its target scenarios (sasl-credential-rotated, the
// scope addition's blocked-slot and rows-lost-after-retention, the live-run
// additions consumer-skipped-range and missed-table) and the regression set
// named for its live run. In both write modes and both sources, W4i's own
// words carry no root-cause term of any of them and the prompt names none of
// their fix targets. For each target the CDC prompt also misses at least one
// root-cause term group, so its text alone can never grade as that
// scenario's root cause. sasl-credential-rotated is the exception, on the
// base already: the shared injection rule's "secrets" and rule 1's "numbers
// may have changed" meet both of its groups as bare words, so for it the
// check is that W4i's words carry none of its terms and the prompt never
// names the ACLs its trap story blames. The group check is not run on the
// regression set or the MySQL prompt: words W4i does not touch already meet
// some of their groups (dlq-backlog's "validat*" and "reject*",
// stale-source-offsets' "old" and "offset*", the MySQL schema history
// bullet's "retention").
func TestSystemPromptLeaksNoW4iAnswer(t *testing.T) {
	prompts := map[string]string{}
	for _, write := range []bool{true, false} {
		prompts["cdc, write "+boolWord(write)] = systemPromptForMode(events.PipelineKindCDC, RoomMode{Write: write})
		prompts["mysql, write "+boolWord(write)] = systemPromptForSource(events.PipelineKindCDC, events.SourceKindMySQL, RoomMode{Write: write})
	}
	targets := map[string]bool{
		"sasl-credential-rotated": true, "blocked-slot": true, "rows-lost-after-retention": true,
		"consumer-skipped-range": true, "missed-table": true,
	}
	addedWords := " " + strings.Join(promptWordRe.FindAllString(w4iAddedText(), -1), " ") + " "
	for _, name := range []string{
		"sasl-credential-rotated", "blocked-slot", "rows-lost-after-retention", "consumer-skipped-range", "missed-table",
		"schema-change", "dlq-backlog", "stale-source-offsets", "tolerance-all-skips", "healthy-after-recovery",
		"ambiguous-lag-spike", "mysql-binlog-purged", "mysql-server-id-clash",
	} {
		b, err := os.ReadFile(filepath.Join("..", "..", "evals", "scenarios", name+".yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var sc w4iScenario
		if err := yaml.Unmarshal(b, &sc); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		// healthy-after-recovery and ambiguous-lag-spike name no cause.
		if targets[name] && len(sc.Expect.RootCause) == 0 {
			t.Fatalf("%s: no root-cause term groups read; the scenario format changed", name)
		}
		for _, group := range sc.Expect.RootCause {
			for _, term := range group {
				if promptHasTerm(addedWords, term) {
					t.Errorf("%s: W4i's words carry its root-cause term %q", name, term)
				}
			}
		}
		for pname, p := range prompts {
			lower := strings.ToLower(p)
			words := " " + strings.Join(promptWordRe.FindAllString(lower, -1), " ") + " "
			switch {
			case name == "sasl-credential-rotated":
				if promptHasTerm(words, "acl*") || promptHasTerm(words, "ac ls") {
					t.Errorf("%s, %s: the prompt names the ACLs of the trap story", name, pname)
				}
			case targets[name] && strings.HasPrefix(pname, "cdc"):
				all := true
				for _, group := range sc.Expect.RootCause {
					hit := false
					for _, term := range group {
						hit = hit || promptHasTerm(words, term)
					}
					all = all && hit
				}
				if all {
					t.Errorf("%s, %s: the prompt alone carries every root-cause term group %v", name, pname, sc.Expect.RootCause)
				}
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
							t.Errorf("%s, %s: the prompt names the %s %s target %q", name, pname, action, key, s)
						}
					}
				}
			}
		}
	}
}
