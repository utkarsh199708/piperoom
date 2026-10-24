package agent

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
)

// cdcSectionLine returns the line of the CDC prompt section between the
// headings from and to that starts with prefix.
func cdcSectionLine(t *testing.T, prompt, from, to, prefix string) string {
	t.Helper()
	start, end := strings.Index(prompt, from+"\n"), strings.Index(prompt, to+"\n")
	if start < 0 || end < start {
		t.Fatalf("CDC prompt has no %s section", from)
	}
	for _, line := range strings.Split(prompt[start:end], "\n") {
		if strings.HasPrefix(line, prefix) {
			return line
		}
	}
	t.Fatalf("CDC %s section has no line starting %q", from, prefix)
	return ""
}

// cdcFixesLine returns the line of the CDC Fixes section that starts with
// prefix.
func cdcFixesLine(t *testing.T, prompt, prefix string) string {
	t.Helper()
	return cdcSectionLine(t, prompt, "Fixes", "Tools", prefix)
}

// cdcMethodLine returns the line of the CDC method section that starts with
// prefix (a rule's continuation lines start with three spaces).
func cdcMethodLine(t *testing.T, prompt, prefix string) string {
	t.Helper()
	return cdcSectionLine(t, prompt, "Method", "Past incidents", prefix)
}

// TestSystemPromptSmallestListedFix pins W4h (KAN-505, evals run 39994729845).
// The generic rule: once cited evidence establishes a cause a listed action
// fixes, the agent proposes the smallest such action, instead of a question
// nobody prompted, and says what it gives up (schema-change and both
// injection-ddl-comment variants laid out "revert upstream or relax
// compatibility" and asked instead of proposing). The compatibility clause
// gets its pre-#786/#883 meaning back, sharpened: a NONE is high-risk and
// proposed when no narrower listed action fixes a rejected registration,
// saying what the old contract can no longer read, and is undone once
// recovered, by a later fix: the executor runs a proposal's steps back to
// back, so a restore step in the same proposal would land before Debezium
// registers the new schema and fail the task again (verify.go's
// compatFollowUp asks for that later fix). schema_register stays "last
// resort only" because the injected comments ask for it. A FAILED task's
// restart is the next step of the proposal removing its cause (missing-grant
// proposed the grant alone),
// and the publication add's snapshot signal joins that sentence. The offsets
// note replays a missing range still on the topic and snapshots the rows only
// otherwise (consumer-skipped-range re-read rows still on the topic). The
// steer-scoped ask rule is unchanged. Both write modes are checked.
func TestSystemPromptSmallestListedFix(t *testing.T) {
	for _, write := range []bool{true, false} {
		p := systemPromptForMode(events.PipelineKindCDC, RoomMode{Write: write})
		rule7 := cdcMethodRule(t, p, "7")
		restart := cdcFixesLine(t, p, "- A FAILED task's restart")
		compat := cdcFixesLine(t, p, "- schema_set_compatibility NONE is high-risk")
		offsets := cdcFixesLine(t, p, "- Reset source offsets")
		cases := []struct {
			name string
			in   string
			text string
			want bool
		}{
			{"rule 7 proposes the smallest listed fix instead of an unprompted question", rule7,
				"7. Once cited evidence establishes the cause, stop reading; instead of asking unprompted, call propose_fix with the smallest listed action that fixes it, saying what it gives up, or say nothing needs fixing.", true},
			{"the old smallest-change bullet is folded into rule 7", p, "Fix the cause you established with the smallest change", false},
			{"NONE is high-risk, proposed when nothing narrower listed fixes a rejected registration", compat,
				"- schema_set_compatibility NONE is high-risk: if no narrower listed action fixes a rejected registration, propose it, saying what the old contract can no longer read;", true},
			{"NONE is undone once recovered, not by a step of the same proposal", compat, "; undo it once recovered;", true},
			{"no restore step in the NONE proposal", p, "restore step", false},
			{"schema_register stays last resort only", compat, "schema_register: last resort only.", true},
			{"NONE is no longer a last resort", p, "schema_set_compatibility set to NONE and schema_register are high-risk: last resort only.", false},
			{"restart of a FAILED task is the next step of the same proposal", restart,
				"- A FAILED task's restart is the next step of the proposal removing its cause;", true},
			{"the publication add's snapshot signal joins the restart sentence", restart,
				"after pg_add_publication_table, that table's pg_snapshot_signal.", true},
			{"the separate publication bullet is gone", p, "pg_add_publication_table misses rows written before it", false},
			{"offsets note replays a range still on the topic, else snapshots", offsets,
				"replay a missing range still on the topic; otherwise snapshot the rows", true},
			{"offsets note keeps the reset guard", offsets, "- Reset source offsets only for lost positions;", true},
			{"no more snapshot-first wording", p, "snapshot a table missing rows instead", false},
		}
		for _, c := range cases {
			if got := strings.Contains(c.in, c.text); got != c.want {
				t.Errorf("write mode %v, %s: contains %q = %v, want %v\nin: %s", write, c.name, c.text, got, c.want, c.in)
			}
		}
		// "last resort" names schema_register only, once.
		if n := strings.Count(p, "last resort"); n != 1 {
			t.Errorf("write mode %v: %d \"last resort\" mentions, want 1 (schema_register's)", write, n)
		}
		// The steer-scoped ask rule is left as it is.
		if !strings.Contains(p, "\n"+promptAmbiguousSteerCDC+"\n") || promptAmbiguousSteerCDC != "- If what a driver steer asks for needs a fix no listed action can make, or could mean more than one action, ask the driver one short question with post_message and propose nothing until they answer." {
			t.Errorf("write mode %v: the steer-scoped ask rule changed", write)
		}
		// Each new or reworded line is generic: no wording of a target or
		// regression scenario's answer (schema-change, injection-ddl-comment
		// and -subtle, consumer-skipped-range, missing-grant,
		// rows-lost-after-retention, missed-table).
		changed := strings.ToLower(strings.Join([]string{
			rule7, restart, compat, offsets,
			cdcMethodRule(t, p, "1"), cdcMethodRule(t, p, "2"), cdcMethodRule(t, p, "3"), cdcMethodRule(t, p, "5"),
			cdcMethodLine(t, p, "   From the most downstream symptom"),
			cdcMethodLine(t, p, "   Before abstaining"),
			cdcMethodLine(t, p, "   If no evidence names a cause"),
			cdcMethodLine(t, p, "   If every stage reads healthy"),
			cdcFixesLine(t, p, "- Use connect_update_connector_config"),
			cdcFixesLine(t, p, "- Before proposing a resume or restart"),
		}, "\n"))
		changed = promptTokenRe.ReplaceAllString(changed, "") // tool names are not answers
		for _, answer := range []string{
			"amount", "numeric", "decimal", "bytes", "avro", "409", "backward", "type change", "alter",
			"comment", "devctl", "grant", "permission", "privilege", "role",
			"retention", "purge", "expir", "latest", "deploy", "1301", "reprocess", "refund",
			"orders", "order-sync", "revert",
		} {
			if strings.Contains(changed, answer) {
				t.Errorf("write mode %v: a W4h line names a scenario answer (%q)", write, answer)
			}
		}
	}
	batch := systemPromptFor(events.PipelineKindBatch)
	for _, cdcOnly := range []string{"smallest listed action", "A FAILED task's restart", "schema_register", "replay a missing range"} {
		if strings.Contains(batch, cdcOnly) {
			t.Errorf("batch prompt carries the CDC rule %q", cdcOnly)
		}
	}
}

// w4hScenario is the part of an eval scenario the leakage check reads.
type w4hScenario struct {
	Expect struct {
		RootCause  [][]string                `yaml:"root_cause"`
		FixTargets map[string]map[string]any `yaml:"fix_targets"`
	} `yaml:"expect"`
}

// promptWordRe splits prompt text into lower-case words for the term check.
var promptWordRe = regexp.MustCompile(`[a-z0-9]+`)

// promptHasTerm reports whether a scenario term occurs in the prompt as
// whole words, a trailing "*" matching any word that starts with the stem,
// as the eval's keyword grade reads terms.
func promptHasTerm(words, term string) bool {
	term = strings.ToLower(term)
	prefix := strings.HasSuffix(term, "*")
	t := strings.Join(promptWordRe.FindAllString(strings.TrimSuffix(term, "*"), -1), " ")
	if t == "" {
		return false
	}
	if prefix {
		return strings.Contains(words, " "+t)
	}
	return strings.Contains(words, " "+t+" ")
}

// TestSystemPromptLeaksNoW4hAnswer is W4h's leakage test: the CDC prompt, in
// both write modes, never states a target or regression scenario's answer on
// its own. For each scenario it must miss at least one root-cause term group
// (so the prompt text alone could never grade as that scenario's root cause),
// and no fix target the scenario names (subject, connector, role, table,
// topic, publication) may appear in it.
func TestSystemPromptLeaksNoW4hAnswer(t *testing.T) {
	for _, name := range []string{
		"schema-change", "injection-ddl-comment", "injection-ddl-comment-subtle", "consumer-skipped-range",
		"missing-grant", "rows-lost-after-retention", "missed-table",
	} {
		b, err := os.ReadFile(filepath.Join("..", "..", "evals", "scenarios", name+".yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var sc w4hScenario
		if err := yaml.Unmarshal(b, &sc); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(sc.Expect.FixTargets) == 0 {
			t.Fatalf("%s: no fix targets read; the scenario format changed", name)
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
