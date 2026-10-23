package agent

import (
	"strings"
	"testing"

	"piperoom/internal/events"
)

// cdcMethodRule returns the line of the CDC method section that starts with
// rule number n ("4" gives the line "4. ...").
func cdcMethodRule(t *testing.T, prompt, n string) string {
	t.Helper()
	start, end := strings.Index(prompt, "Method\n"), strings.Index(prompt, "Past incidents\n")
	if start < 0 || end < start {
		t.Fatal("CDC prompt has no method section")
	}
	for _, line := range strings.Split(prompt[start:end], "\n") {
		if strings.HasPrefix(line, n+". ") {
			return line
		}
	}
	t.Fatalf("CDC method section has no rule %s", n)
	return ""
}

// TestSystemPromptFirstFailingDefinition pins W4f (KAN-504, rule 4 text from
// the lead ruling on PR #931): rule 4 defines the first failing component as
// the most upstream stage whose own status read shows it failed, else the
// most upstream one that no longer does its job (the P3-W3 ruling, so a
// stage that fails no status read is still named). If a stage failed, a
// component that still answers requests is at most the root-cause component,
// never the first failing one, and rule 6's unreachable sentence now sits in
// rule 4: an "unreachable: <component>" read names the cause's component and
// forbids a healthy verdict, but no longer says the component "is down"
// (which made the agent name it as the first failing one, or as a broker
// that is down). Rule 6 keeps only the tool-error sentence. The rule is
// generic: no wording of a scenario's answer appears in rules 4 or 6. Both
// write modes share the method, so both are checked.
func TestSystemPromptFirstFailingDefinition(t *testing.T) {
	cases := []struct {
		name string
		rule string // "4", "6", or "" for the whole prompt
		text string
		want bool
	}{
		{"first failing is the most upstream failed stage", "4",
			"4. The first failing component is the most upstream stage whose own status read shows it failed (FAILED, crash-looping, never started)", true},
		{"else the most upstream one that no longer does its job", "4",
			", else the most upstream one that no longer does its job.", true},
		{"a component that still answers requests is at most the root cause", "4",
			"If a stage failed, a component that still answers requests, even degraded or misconfigured, is at most the root-cause component, never the first failing one;", true},
		// The ticket's draft, which the ruling replaced: it made a stage
		// that fails no status read (a component still answering, wrong data
		// in a topic) unnameable as the first failing one.
		{"no unconditional root-cause sentence", "4", "is the root-cause component, never", false},
		{"no wrong-data-in-a-topic clause", "4", "wrong or missing data", false},
		{"unreachable names the cause and forbids a healthy verdict", "4",
			`so is "unreachable: <component>", which forbids a healthy verdict while unresolved.`, true},
		// Fix round (evals-live 40029255155, 40032876094): the agent stated
		// the root cause and either never named the first failing component
		// or merged the two ("First failing and root-cause component: X").
		// Rule 4's last sentence fixes the form, both labels even when the
		// component is the same, with the scope.
		{"a cause is named in the fixed two-label form", "4",
			`update_hypothesis names a cause: "First failing: <component>. Root cause: <component>." even if identical; set its scope.`, true},
		{"rule 6 keeps the tool-error sentence", "6",
			"6. Tool errors and timeouts are evidence for what is unknown or blocked, not proof.", true},
		{"rule 6 no longer carries the unreachable sentence", "6", "unreachable", false},
		{"unreachable is no longer an outage", "", "means the component is down", false},
		{"no name-it-in-the-hypothesis order for an unreachable read", "", "name it in the hypothesis", false},
		{"old first-failing wording gone", "", "the most upstream broken one", false},
		{"old not-broken wording gone", "", "is not broken", false},
		// Scenario-answer wording (KAN-504 test case 5).
		{"no bad-record example", "", "a bad record on a topic", false},
		{"no missing-schema example", "", "the registry missing a schema", false},
	}
	for _, write := range []bool{true, false} {
		p := systemPromptForMode(events.PipelineKindCDC, RoomMode{Write: write})
		for _, c := range cases {
			in := p
			if c.rule != "" {
				in = cdcMethodRule(t, p, c.rule)
			}
			if got := strings.Contains(in, c.text); got != c.want {
				t.Errorf("write mode %v, %s: contains %q = %v, want %v", write, c.name, c.text, got, c.want)
			}
		}
		// The unreachable sentence moved; it is not repeated.
		if n := strings.Count(p, `"unreachable: <component>"`); n != 1 {
			t.Errorf("write mode %v: %d unreachable-read rules, want 1", write, n)
		}
		// The hypothesis form is rule 4's last sentence and is stated once.
		const form = `"First failing: <component>. Root cause: <component>."`
		if r4 := cdcMethodRule(t, p, "4"); !strings.HasSuffix(r4, form+" even if identical; set its scope.") {
			t.Errorf("write mode %v: rule 4 does not end with the hypothesis form: %s", write, r4)
		}
		if n := strings.Count(p, form); n != 1 {
			t.Errorf("write mode %v: hypothesis form stated %d times, want 1", write, n)
		}
		// No target or regression scenario's answer leaks into the changed
		// rules (advertised-listener-wrong, broker-disk-notenoughreplicas,
		// toast-placeholder, setup/wal-level-replica,
		// db-unreachable-from-connect, and the fix round's
		// long-room-compaction and dead-slot-wal).
		changed := strings.ToLower(cdcMethodRule(t, p, "4") + "\n" + cdcMethodRule(t, p, "6"))
		for _, answer := range []string{
			"advertis", "listener", "disk", "min.insync", "notenoughreplicas", "isr",
			"replica identity", "toast", "placeholder", "wal_level", "wal level",
			"firewall", "network", "dns", "refused", "broker", "schema", "bad record",
			"poison", "orphan", "slot", "publication",
		} {
			if strings.Contains(changed, answer) {
				t.Errorf("write mode %v: rules 4 and 6 name a scenario answer (%q)", write, answer)
			}
		}
	}
	batch := systemPromptFor(events.PipelineKindBatch)
	for _, cdcOnly := range []string{"still answers requests", `"unreachable: <component>"`, "The first failing component is"} {
		if strings.Contains(batch, cdcOnly) {
			t.Errorf("batch prompt carries the CDC rule %q", cdcOnly)
		}
	}
}
