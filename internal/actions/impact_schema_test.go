package actions

import (
	"encoding/json"
	"slices"
	"testing"

	"piperoom/internal/events"
)

// A schema registry write reaches the readers of the topic: the consumer and,
// through it, the warehouse. Compatibility NONE lets any later schema in, so it
// is high risk (Q2 F10).
func TestSchemaActionsImpact(t *testing.T) {
	reg := resumeRegistry(t, testDeps())
	down := []string{"consumer", "warehouse"}
	cases := []struct {
		action string
		params string
		layers []string
		risk   string
	}{
		{"schema_register", `{"subject":"pg.public.orders-value","schema":"{}"}`, down, events.RiskMedium},
		{"schema_register", `{"subject":"pg.public.orders-key","schema":"{}"}`, down, events.RiskMedium},
		{"schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"NONE"}`, down, events.RiskHigh},
		{"schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"BACKWARD"}`, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.action+" "+tc.params, func(t *testing.T) {
			imp := mustGet(t, reg, tc.action).(Impacting).Impact(json.RawMessage(tc.params))
			if imp.RiskLevel != tc.risk || !slices.Equal(imp.Layers, tc.layers) {
				t.Fatalf("Impact = %+v, want layers %v risk %q", imp, tc.layers, tc.risk)
			}
			if tc.risk != "" && imp.Reason == "" {
				t.Error("no reason for approvers")
			}
		})
	}
	// Params that do not parse give no opinion; Validate refuses them.
	for _, name := range []string{"schema_register", "schema_set_compatibility"} {
		if imp := mustGet(t, reg, name).(Impacting).Impact(json.RawMessage(`not json`)); imp.RiskLevel != "" || imp.Layers != nil {
			t.Errorf("%s: Impact of bad params = %+v", name, imp)
		}
	}
	// A returned slice is the caller's.
	a := mustGet(t, reg, "schema_register").(Impacting)
	first := a.Impact(json.RawMessage(cases[0].params))
	first.Layers[0] = "tampered"
	if again := a.Impact(json.RawMessage(cases[0].params)); again.Layers[0] != "consumer" {
		t.Fatalf("Layers shared between calls: %v", again.Layers)
	}
}
