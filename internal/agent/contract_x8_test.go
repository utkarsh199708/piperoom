package agent

import (
	"encoding/json"
	"strings"
	"testing"
)

// X8-0: Alert.Staging is omitted when false, so an alert without staging
// marshals as before, and survives a round trip when true; a
// "proposal_staged" input carries the verdict in OK.
func TestX8AlertStagingField(t *testing.T) {
	raw, _ := json.Marshal(Alert{RoomID: "r_1", PipelineID: "orders"})
	if strings.Contains(string(raw), "staging") {
		t.Fatalf("staging false must be omitted: %s", raw)
	}
	raw, _ = json.Marshal(Alert{RoomID: "r_1", PipelineID: "orders", Staging: true})
	var a Alert
	if err := json.Unmarshal(raw, &a); err != nil || !a.Staging || !strings.Contains(string(raw), `"staging":true`) {
		t.Fatalf("round trip = %+v, %v (%s)", a, err, raw)
	}

	ok := true
	in := HumanInput{Seq: 9, Kind: "proposal_staged", ProposalID: "p_1", Text: "step 0: healthy", OK: &ok}
	raw, _ = json.Marshal(in)
	var out HumanInput
	if err := json.Unmarshal(raw, &out); err != nil || out.Kind != "proposal_staged" || out.ProposalID != "p_1" || out.OK == nil || !*out.OK {
		t.Fatalf("proposal_staged input = %+v, %v", out, err)
	}
}
