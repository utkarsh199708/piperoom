package topology

import (
	"testing"
	"time"
)

// A newest sink timestamp ahead of now (clock skew, a TIMESTAMP without time
// zone written in another zone, one bad row) used to clamp to "stale for 0s"
// and show a stale sink as OK (QA sweep H5). A minute of skew still reads as
// fresh; beyond that the age is unknown and the sink warns.
func TestSinkHealthFutureTimestamp(t *testing.T) {
	th := DefaultThresholds()
	now := time.Now()
	if got, facts := sinkHealth(now.Add(30*time.Second), now, th); got != HealthOK || facts["stale_for"] != "0s" {
		t.Fatalf("within skew: %s %v, want ok and stale_for 0s", got, facts)
	}
	got, facts := sinkHealth(now.Add(3*time.Hour), now, th)
	if got != HealthWarn || facts["future_timestamp"] != "3.0h ahead" || facts["stale_for"] != "unknown" {
		t.Fatalf("3h ahead: %s %v, want warn, future_timestamp 3.0h ahead, stale_for unknown", got, facts)
	}
}
