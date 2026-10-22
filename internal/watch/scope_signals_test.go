package watch

import (
	"strings"
	"testing"
	"time"

	"piperoom/internal/owners"
)

// TestConfigRiskFiresForConnectorTableOutsideScope pins KAN-404's watcher half:
// a table the pipeline's connector streams but owners.yaml does not declare is
// a config risk, one finding per table, and it clears when the two agree again.
func TestConfigRiskFiresForConnectorTableOutsideScope(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{
		id:     "orders",
		tables: "[public.t]",
		watch:  "    watch:\n      signals:\n        config_risk: {action: room}\n",
	}}})
	h.conn.cfg["pg-orders"]["table.include.list"] = "public.t,public.devctl_refunds"
	h.steps(4, time.Minute)

	if h.sigPhase(owners.SignalConfigRisk) != phaseFiring {
		t.Fatalf("an undeclared connector table is not a config risk\n%s", h.logs)
	}
	if !strings.Contains(h.logs.String(), "connector_table_outside_scope") {
		t.Fatalf("log line lacks the risk code\n%s", h.logs)
	}
	ev := h.openedEv(t, owners.SignalConfigRisk)
	risks, _ := ev["risks"].([]any)
	found := false
	for _, r := range risks {
		m, ok := r.(map[string]any)
		if ok && m["code"] == "connector_table_outside_scope" {
			if m["table"] != "public.devctl_refunds" {
				t.Fatalf("risk %v does not name the table", m)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("evidence risks %v lack the scope-gap finding", risks)
	}

	// The signal table is infrastructure: listing it in the include list is not
	// a scope gap and must not add a second finding.
	h.conn.cfg["pg-orders"]["table.include.list"] = "public.t,public.devctl_refunds,public.devctl_signal"
	h.conn.cfg["pg-orders"]["signal.data.collection"] = "public.devctl_signal"
	h.step(time.Minute)
	if got := strings.Count(h.logs.String(), "public.devctl_signal"); got != 0 {
		t.Fatalf("the signal table was reported as a scope gap\n%s", h.logs)
	}

	// Owners declares the table (or the connector drops it): the finding clears.
	h.conn.cfg["pg-orders"]["table.include.list"] = "public.t"
	h.steps(4, time.Minute)
	if h.sigPhase(owners.SignalConfigRisk) != phaseOK {
		t.Fatalf("the risk stayed after the connector stopped streaming the table\n%s", h.logs)
	}
}
