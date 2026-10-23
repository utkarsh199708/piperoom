package agent

import (
	"strings"
	"testing"

	"piperoom/internal/events"
)

// TestSystemPromptOffsetsRule pins Q5d: with write mode on the CDC Fixes
// section says when to clear a source connector's stored offsets (only for a
// position the source lost) and when to snapshot one table instead (P3a-5).
// Wiring registers connect_reset_source_offsets only with write mode on, so
// the write-off and batch prompts do not carry the rule.
func TestSystemPromptOffsetsRule(t *testing.T) {
	const want = "- Reset source offsets only if the source lost their position; snapshot a table missing rows instead."
	on := systemPromptForMode(events.PipelineKindCDC, RoomMode{Write: true})
	fixes := on[strings.Index(on, "Fixes\n"):strings.Index(on, "Tools\n")]
	if !strings.Contains(fixes, want+"\n") {
		t.Errorf("CDC write-on Fixes section lacks %q:\n%s", want, fixes)
	}
	if strings.Contains(systemPromptForMode(events.PipelineKindCDC, RoomMode{Write: false}), "Reset source offsets") {
		t.Error("the write-off prompt carries the offsets rule, but no offsets action is registered there")
	}
	if strings.Contains(systemPromptFor(events.PipelineKindBatch), "Reset source offsets") {
		t.Error("the batch prompt carries the CDC offsets rule")
	}
}
