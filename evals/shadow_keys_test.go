package evals

import "testing"

// Map keys are untrusted too. Whatever a key says, the data either comes back
// equal or is dropped; it is never altered.
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
			if r.Tool != "connect_status" || r.Data == nil {
				continue
			}
			want := map[string]any{k: 1, "z": []any{map[string]any{k: "v"}}}
			if !sameJSON(r.Data, want) {
				t.Errorf("key %q came back as %#v", k, r.Data)
			}
		}
	}
}
