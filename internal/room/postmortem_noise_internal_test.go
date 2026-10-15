package room

import "testing"

// Which summary lines are hand-off bookkeeping and which are about the
// incident. The match is narrow on purpose: a line must name the summary or the
// hand-off together with a failure or a cut.
func TestIsHandoffNoise(t *testing.T) {
	cases := []struct {
		line string
		want bool
	}{
		{"Agent summary lines were truncated", true},
		{"One handoff failed", true},
		{"The hand-off was truncated, so some tool output is missing", true},
		{"Hand-off request timed out", true},
		{"hand off failed twice", true},
		{"Summary was cut off", true},
		{"The handoff to Priya failed because she is not a member", true},
		{"Restart the consumer", false},
		{"lag keeps growing", false},
		{"Priya has not accepted the hand-off yet", false},
		{"Connector task failed with a decimal error", false},
		{"The log line was truncated by the broker", false},
		{"Tool output was truncated", false},
		{"", false},
		// QA F12: a table or object named "summary" is the incident, not
		// the hand-off.
		{"orders_summary table was truncated by the failed reload; rows missing", false},
		{"daily-summary was truncated by the reload", false},
		{"summary.orders was truncated", false},
		{"The summary table was truncated by the failed reload", false},
		{"order summary rows were cut off by the failed load", false},
		{"handoff_log table truncated", false},
		{"Summaries were truncated", true},
		{"The closing summary was truncated", true},
	}
	for _, c := range cases {
		if got := isHandoffNoise(c.line); got != c.want {
			t.Errorf("isHandoffNoise(%q) = %v, want %v", c.line, got, c.want)
		}
	}
}
