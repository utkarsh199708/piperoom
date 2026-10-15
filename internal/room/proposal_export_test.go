package room_test

import (
	"context"
	"fmt"
	"testing"

	"piperoom/internal/events"
)

// The audit export carries the impact fields and the withdrawal (QA G5, G9);
// both are omitted when empty, so a room from before Q0 exports as it did.
func TestExportCarriesImpactAndWithdrawal(t *testing.T) {
	ctx := context.Background()
	e := newLifecycleEnv(t, connectConfig(), &fakeAction{name: "pg_fix", layer: "postgres"})
	roomID := createRoom(t, e.svc, "arjun").ID
	impacted := e.propose(t, roomID, events.AgentActor, events.ProposalCreatedPayload{
		Steps: []events.ProposalStep{step("connect_config", "kafka-connect", "double")},
	})
	plain := e.propose(t, roomID, events.AgentActor, events.ProposalCreatedPayload{Steps: []events.ProposalStep{step("pg_fix", "postgres", "")}})
	if _, err := e.svc.WithdrawProposal(ctx, e.user(t, "arjun"), roomID, plain, "not needed"); err != nil {
		t.Fatal(err)
	}
	sum, body := exportRoomFor(t, e.svc, roomID)
	doc := verifyExport(t, sum, body)
	props, _ := doc["proposals"].([]any)
	byID := map[string]map[string]any{}
	for _, p := range props {
		m, _ := p.(map[string]any)
		id, _ := m["proposal_id"].(string)
		byID[id] = m
	}
	im, pl := byID[impacted], byID[plain]
	if im == nil || pl == nil {
		t.Fatalf("export proposals = %v", props)
	}
	if fmt.Sprint(im["impacted_layers"]) != "[consumer warehouse]" || im["risk_level"] != events.RiskHigh ||
		fmt.Sprint(im["impact_reasons"]) != "["+decimalReason+"]" || im["status"] != "open" || im["withdrawn"] != nil {
		t.Fatalf("impacted proposal in the export = %v", im)
	}
	if fmt.Sprint(im["required_layers"]) != "[kafka-connect consumer warehouse]" {
		t.Fatalf("required_layers = %v", im["required_layers"])
	}
	w, _ := pl["withdrawn"].(map[string]any)
	by, _ := w["by"].(map[string]any)
	if pl["status"] != "withdrawn" || w["reason"] != "not needed" || by["id"] != "arjun" {
		t.Fatalf("withdrawn proposal in the export = %v", pl)
	}
	if _, ok := pl["impacted_layers"]; ok {
		t.Fatalf("empty impacted_layers must be omitted: %v", pl)
	}
	if _, ok := pl["impact_reasons"]; ok {
		t.Fatalf("empty impact_reasons must be omitted: %v", pl)
	}
}
