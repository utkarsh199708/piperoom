package llm

import (
	"strings"
	"testing"
)

func TestSupportedRatesParse(t *testing.T) {
	list, err := ParseSupported([]byte(`{"models":[
		{"provider":"gemini","model":"a","status":"supported","baseline":"b","gated_at":"2026-10-01","pass_rate":0.92,"root_cause_rate":0.8,"cost_per_run_usd":0.41},
		{"provider":"gemini","model":"z","status":"supported","baseline":"b","gated_at":"2026-10-01","pass_rate":0,"root_cause_rate":1,"cost_per_run_usd":0},
		{"provider":"gemini","model":"n","status":"experimental","baseline":"b","gated_at":"2026-10-01"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	a := list[0]
	if a.PassRate == nil || *a.PassRate != 0.92 || a.RootCauseRate == nil || *a.RootCauseRate != 0.8 || a.CostPerRunUSD == nil || *a.CostPerRunUSD != 0.41 {
		t.Fatalf("row a = %+v", a)
	}
	// A recorded zero is kept apart from "not recorded".
	if z := list[1]; z.PassRate == nil || *z.PassRate != 0 || z.CostPerRunUSD == nil {
		t.Fatalf("row z = %+v", z)
	}
	if n := list[2]; n.PassRate != nil || n.RootCauseRate != nil || n.CostPerRunUSD != nil {
		t.Fatalf("row n = %+v, want no rates", n)
	}

	for _, bad := range []string{
		`"pass_rate":1.2`, `"pass_rate":-0.1`, `"root_cause_rate":2`, `"cost_per_run_usd":-1`, `"pass_rate":"high"`,
	} {
		doc := `{"models":[{"provider":"gemini","model":"a","status":"supported","baseline":"b","gated_at":"2026-10-01",` + bad + `}]}`
		if _, err := ParseSupported([]byte(doc)); err == nil {
			t.Errorf("accepted %s", bad)
		} else if !strings.Contains(err.Error(), "supported.json") {
			t.Errorf("%s: error %v does not name supported.json", bad, err)
		}
	}

	// The embedded list still parses (models: []).
	if _, err := SupportedModels(); err != nil {
		t.Fatal(err)
	}
}
