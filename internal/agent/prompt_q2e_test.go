package agent

import (
	"strings"
	"testing"

	"piperoom/internal/events"
)

// TestSystemPromptReadsSettingsBeforeConfigChange pins Q2e: the CDC agent
// reads the source database's settings (the Q2a source_settings and
// source_table_replication tools) before it proposes a configuration change,
// so a wal_level or replica identity fix rests on the server's real values.
// It sits in method rule 2, with the other reads that come before a
// proposal. The batch prompt has no source database, so it must not carry it.
func TestSystemPromptReadsSettingsBeforeConfigChange(t *testing.T) {
	const want = "Read database settings before proposing a config change."
	prompt := systemPrompt()
	start := strings.Index(prompt, "\n2. ")
	if start < 0 {
		t.Fatal("system prompt has no method rule 2")
	}
	rule := prompt[start+1:]
	rule = rule[:strings.Index(rule, "\n")]
	if !strings.HasSuffix(rule, " "+want) {
		t.Errorf("method rule 2 is\n%q, want it to end with %q", rule, want)
	}
	if strings.Contains(systemPromptFor(events.PipelineKindBatch), want) {
		t.Error("the batch prompt carries the CDC database-settings rule")
	}
}
