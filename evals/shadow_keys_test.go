package evals

import "testing"

// Map keys in tool data are untrusted. Unknown fields are removed by the
// connect_status projection and never retained in the scenario.
func TestScenarioFromShadowHostileKeys(t *testing.T) {
	for _, k := range []string{"<<", "---", "? x", "- a", "&a", "*a", "!!str", "=", "null", "a: b", "# c", "\n", "", " lead", "y", "~", "0o7", "1e3"} {
		m := sampleDoc(t)
		for _, e := range evList(m) {
			p := sub(e.(map[string]any), "payload")
			if p["tool_use_id"] == "toolu_01" && p["summary"] != nil {
				p["data"] = map[string]any{k: 1, "z": []any{map[string]any{k: "v"}}}
			}
		}
		res, err := ScenarioFromShadow(encode(t, m), lookupOrders, "")
		if err != nil {
			t.Errorf("key %q: %v", k, err)
			continue
		}
		for _, r := range res.Scenario.Recordings {
			if r.Tool == "connect_status" && r.Data != nil {
				t.Errorf("unknown key %q remained in data: %#v", k, r.Data)
			}
		}
	}
}
