package agent

import (
	"strings"
	"testing"

	"piperoom/internal/events"
)

// TestSystemPromptOffsetsRule pins Q5d: in both write modes the CDC Fixes
// section says when to clear a source connector's stored offsets (only for a
// position the source no longer has) and, since W4h (KAN-505), how missing
// rows come back: replayed from the topic while their records are still on
// it, snapshotted from the source otherwise (the old "snapshot a table
// missing rows instead" sent consumer-skipped-range to pg_resnapshot_rows).
// W4i (KAN-507, LEAD-ARCH scope addition) keys "still on the topic" on the
// read that shows it: records above the log start that kafka_topic_offsets
// reads (rows-lost-after-retention proposed a fix before reading it).
// Wiring registers connect_reset_source_offsets in both modes (P0 ruling
// #606), so the write-off prompt carries the rule too; the batch prompt does
// not.
func TestSystemPromptOffsetsRule(t *testing.T) {
	const want = "- Reset source offsets only for lost positions; replay missing records above the log start (kafka_topic_offsets), else snapshot them."
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
