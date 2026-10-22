package agent

import (
	"encoding/json"
	"slices"
	"testing"
)

// The suspected_layer enum the model sees is byte-identical to main before
// U0: the empty string and the eight incident layers, never the setup layer
// "environment" (events.IncidentLayers). The whole schema is pinned as JSON
// so any other drift shows too. KAN-402 added evidence_steps, the read ids a
// suspected layer must cite to be kept.
func TestTriageSchemaUnchangedByEnvironmentLayer(t *testing.T) {
	enum, ok := triageSchema()["properties"].(map[string]any)["suspected_layer"].(map[string]any)["enum"].([]string)
	if !ok {
		t.Fatal("suspected_layer enum is not a []string")
	}
	want := []string{"", "postgres", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse", "orchestrator", "transform"}
	if !slices.Equal(enum, want) {
		t.Fatalf("suspected_layer enum = %v, want %v", enum, want)
	}
	raw, err := json.Marshal(triageSchema())
	if err != nil {
		t.Fatal(err)
	}
	const pinned = `{"additionalProperties":false,"properties":{"based_on_steps":{"items":{"type":"string"},"type":"array"},"confidence":{"enum":["low","medium","high"],"type":"string"},"evidence_steps":{"description":"step ids of the reads of suspected_layer that show it","items":{"type":"string"},"type":"array"},"headline":{"description":"one plain sentence, at most 200 characters","type":"string"},"next_check":{"description":"the first read of the full run, at most 120 characters","type":"string"},"suspected_layer":{"enum":["","postgres","kafka-connect","schema-registry","kafka","consumer","warehouse","orchestrator","transform"],"type":"string"}},"required":["headline","suspected_layer","confidence","next_check","based_on_steps","evidence_steps"],"type":"object"}`
	if string(raw) != pinned {
		t.Fatalf("triage schema drifted:\n got %s\nwant %s", raw, pinned)
	}
}
