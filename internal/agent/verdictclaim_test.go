package agent

import "testing"

// Rule 1's matcher: true only for a verdict on the incident or the whole
// pipeline. The component rows are the live false positives of 2026-10-25.
func TestPipelineVerdictHealthy(t *testing.T) {
	tests := []struct {
		text string
		want bool
	}{
		// Whole-pipeline verdicts (task table).
		{"Healthy now, nothing to fix.", true},
		{"Nothing to fix.", true},
		{"No action needed.", true},
		{"The pipeline is healthy.", true},
		{"The pipeline has recovered.", true},
		{"Everything is healthy.", true},
		{"All layers are healthy.", true},
		{"The incident has resolved itself.", true},
		{"Recovered; no further action.", true},
		{"No fault found: the pipeline is fine.", true},
		{"Healthy now: the slot is active and lag is 0, nothing to fix.", true},
		{"The connector recovered and the pipeline is healthy again.", true},
		{"This is a false alarm, nothing is wrong.", true},
		// Whole-pipeline verdicts (more).
		{"Healthy now. Nothing to fix.", true},
		{"No fault: recovered.", true},
		{"HEALTHY NOW, NOTHING TO FIX.", true},
		{"The pipeline is healthy (s_3, s_4).", true},
		{"**Healthy now.** Nothing to fix.", true},
		{"- slot active\n- lag 0\n- nothing to fix", true},
		{"Verdict: healthy.", true},
		{"The pipeline restarted on its own and it has recovered.", true},
		{"All clear.", true},
		{"The system is back to normal.", true},
		{"No action is required (s_7).", true},
		{"Nothing is wrong with the pipeline.", true},
		{"End to end is healthy, nothing to fix.", true},
		{"The pipeline is still healthy.", true},
		{"The alert is resolved.", true},
		{"Recovered.", true},
		{"Root cause: none. The pipeline is `healthy`.", true},
		// Component-level or caveated (task table).
		{"Source, slot and connector are healthy.", false},
		{"First failing: consumer group order-sync. The connector is healthy, so the problem is downstream.", false},
		{"The connector is healthy, so the problem is downstream.", false},
		{"The consumer is healthy.", false},
		{"Kafka and Connect look healthy.", false},
		{"Root cause: consumer, not established. Source and topic are healthy.", false},
		{"Source and topic are healthy.", false},
		{"Nothing is wrong with the connector.", false},
		{"Upstream is fine: slot active, lag 0.", false},
		{"Upstream ruled out: the source connector is RUNNING.", false},
		{"Not healthy: the consumer group is Empty.", false},
		{"I cannot say the pipeline is healthy.", false},
		{"Is the pipeline healthy?", false},
		{"It is not yet healthy; recovery is not confirmed.", false},
		{"The connector is RUNNING but the sink has not caught up.", false},
		{"Recovering; the watcher has not cleared yet.", false},
		{"Healthy now, but the consumer is still down.", false},
		{"Root cause not established: the consumer group order-sync is Empty with 0 members.", false},
		// Component-level or caveated (more).
		{"First failing: consumer group order-sync is Empty with 0 members and lag is 4,210. Source and topic are healthy.", false},
		{"**The connector is healthy** (s_2); the consumer group is Empty.", false},
		{"- Source: healthy\n- Connector: healthy\n- Consumer: Empty, 0 members", false},
		{"THE CONSUMER IS HEALTHY.", false},
		{"Nothing to fix upstream; the consumer group is down.", false},
		{"No issues with the slot or the connector.", false},
		{"The slot has recovered.", false},
		{"Everything upstream is healthy.", false},
		{"Everything else is healthy; the consumer is stuck.", false},
		{"The connector restarted and it has recovered; the consumer has not.", false},
		{"The pipeline is not healthy.", false},
		{"The pipeline is healthy except the registry.", false},
		{"It is recovering and the watcher has not cleared yet.", false},
		{"The connector is recovering: task 0 is RUNNING again and the slot is active, but the watcher has not cleared connector_down yet, so keep the room open until it does.", false},
		{"Kafka brokers are fine (s_1, s_2).", false},
		{"The warehouse has caught up.", false},
		{"This is not a false alarm.", false},
		{"Would the pipeline be healthy if the consumer restarted?", false},
		{"Healthy now, the consumer is still down.", false},
		{"No data loss yet, the risk is rising.", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := pipelineVerdictHealthy(tt.text); got != tt.want {
			t.Errorf("pipelineVerdictHealthy(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
}

// The old matcher's live false positives stay healthy claims for KAN-408:
// this change does not touch healthyClaim.
func TestHealthyClaimUnchangedForComponentFindings(t *testing.T) {
	for _, text := range []string{
		"Source, slot and connector are healthy.",
		"The consumer is healthy.",
		"Nothing is wrong with the connector.",
	} {
		if !healthyClaim(text) {
			t.Errorf("healthyClaim(%q) = false; KAN-408 must keep its wider reading", text)
		}
		if pipelineVerdictHealthy(text) {
			t.Errorf("pipelineVerdictHealthy(%q) = true", text)
		}
	}
}
