package actions

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/events"
)

func impactOf(t *testing.T, key, value string) Impact {
	t.Helper()
	// Zero Deps: Impact must decide from the params alone, so any client call
	// would panic on the nil interface.
	a := &updateConnectorConfig{}
	return a.Impact(paramsJSON(t, key, value))
}

func TestConnectorConfigImpactTable(t *testing.T) {
	both := []string{"consumer", "warehouse"}
	cases := []struct {
		key, value string
		layers     []string
		risk       string
		reason     []string // substrings
	}{
		// The QA case, money as double, is refused by the action now (#216): see
		// TestConnectorConfigImpactNoOpinion and TestConfigImpactDoubleIsHigh.
		{"decimal.handling.mode", "string", both, events.RiskMedium, []string{"NUMERIC/DECIMAL", "string", "new type"}},
		{"decimal.handling.mode", "precise", both, events.RiskMedium, []string{"NUMERIC/DECIMAL", "exact decimals"}},
		{"time.precision.mode", "adaptive", both, events.RiskMedium, []string{"time.precision.mode=adaptive", "date and time columns"}},
		{"time.precision.mode", "adaptive_time_microseconds", both, events.RiskMedium, []string{"adaptive_time_microseconds"}},
		{"time.precision.mode", "connect", both, events.RiskMedium, []string{"connect"}},
		{"tombstones.on.delete", "false", both, events.RiskMedium, []string{"tombstone", "miss deletes"}},
		{"tombstones.on.delete", "true", []string{"consumer"}, events.RiskLow, []string{"tombstone", "null"}},
		{"errors.log.enable", "true", nil, events.RiskLow, []string{"only adds visibility"}},
		{"errors.deadletterqueue.context.headers.enable", "true", nil, events.RiskLow, []string{"only adds visibility"}},
		{"heartbeat.interval.ms", "10000", nil, events.RiskLow, []string{"every 10000 ms", "writes nothing to the source database"}},
		{"heartbeat.interval.ms", "0", nil, events.RiskLow, []string{"turns heartbeats off"}},
	}
	for _, tc := range cases {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			got := impactOf(t, tc.key, tc.value)
			if !slices.Equal(got.Layers, tc.layers) {
				t.Errorf("Layers = %v, want %v", got.Layers, tc.layers)
			}
			if got.RiskLevel != tc.risk {
				t.Errorf("RiskLevel = %q, want %q", got.RiskLevel, tc.risk)
			}
			for _, w := range tc.reason {
				if !strings.Contains(got.Reason, w) {
					t.Errorf("Reason lacks %q: %s", w, got.Reason)
				}
			}
		})
	}
}

// Every value the action accepts has an opinion, so adding a key or a value
// to the allow-list without an Impact fails here, not in a room.
func TestConnectorConfigImpactCoversTheWholeAllowList(t *testing.T) {
	a := &updateConnectorConfig{}
	check := func(key, value string) {
		t.Helper()
		got := a.Impact(paramsJSON(t, key, value))
		if got.RiskLevel != events.RiskLow && got.RiskLevel != events.RiskMedium && got.RiskLevel != events.RiskHigh {
			t.Errorf("%s=%s: RiskLevel %q is not low, medium or high", key, value, got.RiskLevel)
		}
		if got.Reason == "" || len(got.Reason) > 300 {
			t.Errorf("%s=%s: Reason is %d bytes, want 1..300 (events.ProposalCreatedPayload.ImpactReasons)", key, value, len(got.Reason))
		}
		for _, l := range got.Layers {
			if !slices.Contains(events.Layers, l) {
				t.Errorf("%s=%s: layer %q is not in events.Layers", key, value, l)
			}
			if l == (&updateConnectorConfig{}).Layer() {
				t.Errorf("%s=%s: Layers repeats the action's own layer %q", key, value, l)
			}
		}
		if !slices.IsSorted(got.Layers) {
			t.Errorf("%s=%s: Layers %v not sorted", key, value, got.Layers)
		}
	}
	for key, values := range defaultConnectorConfigAllowList() {
		for value := range values {
			check(key, value)
		}
	}
	for key := range defaultConnectorConfigBounds() {
		for _, value := range []string{"0", "1000", "3600000"} {
			check(key, value)
		}
	}
}

// Only the layers of the table above can be named, whatever the value-schema
// key: the keys that are not on the allow-list yet are covered when added.
func TestConnectorConfigImpactValueSchemaKeys(t *testing.T) {
	for key := range valueSchemaKeys {
		got := configImpact(key, "x")
		if !slices.Equal(got.Layers, []string{"consumer", "warehouse"}) || got.RiskLevel != events.RiskMedium || !strings.Contains(got.Reason, key+"=x") {
			t.Errorf("%s: %+v", key, got)
		}
	}
}

// The action refuses double, so no step reaches this branch; the function still
// says high for it, in case the allow-list ever offers double again.
func TestConfigImpactDoubleIsHigh(t *testing.T) {
	got := configImpact("decimal.handling.mode", "double")
	if got.RiskLevel != events.RiskHigh || !slices.Equal(got.Layers, []string{"consumer", "warehouse"}) {
		t.Fatalf("configImpact(double) = %+v", got)
	}
}

func TestConnectorConfigImpactNoOpinion(t *testing.T) {
	a := &updateConnectorConfig{}
	for name, raw := range map[string]json.RawMessage{
		"not json":          json.RawMessage(`{`),
		"empty":             json.RawMessage(`{}`),
		"key not allowed":   paramsJSON(t, "tasks.max", "4"),
		"bad value":         paramsJSON(t, "decimal.handling.mode", "float"),
		"double is refused": paramsJSON(t, "decimal.handling.mode", "double"),
		"secret key":        paramsJSON(t, "database.password", "x"),
		"human-only key":    paramsJSON(t, "heartbeat.action.query", "SELECT 1"),
		"heartbeat range":   paramsJSON(t, "heartbeat.interval.ms", "5"),
		"a lookalike key":   paramsJSON(t, "Decimal.Handling.Mode", "double"),
		"a lookalike val":   paramsJSON(t, "decimal.handling.mode", "double "),
		"wrong json types":  json.RawMessage(`{"connector":1,"key":2,"value":3}`),
	} {
		t.Run(name, func(t *testing.T) {
			if got := a.Impact(raw); got.RiskLevel != "" || got.Reason != "" || len(got.Layers) != 0 {
				t.Fatalf("Impact = %+v, want none", got)
			}
		})
	}
}

// A returned slice is the caller's: changing it does not change the next answer.
func TestConnectorConfigImpactReturnsFreshLayers(t *testing.T) {
	first := impactOf(t, "decimal.handling.mode", "string")
	first.Layers[0] = "tampered"
	if again := impactOf(t, "decimal.handling.mode", "string"); again.Layers[0] != "consumer" {
		t.Fatalf("Layers shared between calls: %v", again.Layers)
	}
}

// Only the action that can reach past its layer says so; the restarts and the
// schema actions have no opinion.
func TestOnlyConnectorConfigIsImpacting(t *testing.T) {
	reg := resumeRegistry(t, testDeps())
	var impacting []string
	for _, a := range reg.All() {
		if _, ok := a.(Impacting); ok {
			impacting = append(impacting, a.Name())
		}
	}
	if len(impacting) != 1 || impacting[0] != "connect_update_connector_config" {
		t.Fatalf("Impacting actions = %v", impacting)
	}
}

// Through the registry, as the room service calls it: Validate first, then
// Impact, on the same params.
func TestConnectorConfigImpactThroughTheRegistry(t *testing.T) {
	a := mustGet(t, resumeRegistry(t, testDeps()), "connect_update_connector_config")
	p := paramsJSON(t, "decimal.handling.mode", "string")
	if err := a.Validate(p); err != nil {
		t.Fatal(err)
	}
	imp := a.(Impacting).Impact(p)
	if imp.RiskLevel != events.RiskMedium || !slices.Equal(imp.Layers, []string{"consumer", "warehouse"}) {
		t.Fatalf("Impact = %+v", imp)
	}
}
