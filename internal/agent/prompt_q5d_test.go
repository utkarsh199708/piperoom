package agent

import (
	"strings"
	"testing"

	"piperoom/internal/events"
)

// TestSystemPromptOffsetsRule pins Q5d: in both write modes the CDC Fixes
// section says when to clear a source connector's stored offsets (only for a
// position the source no longer has) and when to snapshot one table instead
// (P3a-5). Wiring registers connect_reset_source_offsets in both modes (P0
// ruling #606), so the write-off prompt carries the rule too; the batch
// prompt does not.
func TestSystemPromptOffsetsRule(t *testing.T) {
	const want = "- Reset source offsets only if their position is gone; snapshot a table missing rows instead."
	for _, write := range []bool{true, false} {
		p := systemPromptForMode(events.PipelineKindCDC, RoomMode{Write: write})
		fixes := p[strings.Index(p, "Fixes\n"):strings.Index(p, "Tools\n")]
		if !strings.Contains(fixes, want+"\n") {
			t.Errorf("CDC Fixes section (write mode %v) lacks %q:\n%s", write, want, fixes)
		}
	}
	if strings.Contains(systemPromptFor(events.PipelineKindBatch), "Reset source offsets") {
		t.Error("the batch prompt carries the CDC offsets rule")
	}
}
