package evals

import (
	"testing"
)

// The two "recurring" scenarios need a past closed room. A scripted run that
// uses similar_incidents and names the past fix passes; the same run without
// the tool, or a hypothesis without the past fix, does not.

const schemaDeletedHypothesis = "Schema Registry no longer has schema id 2 (version 1 of pg.public.orders-value): it was " +
	"deleted, so order-sync cannot decode records written with it (error code 40403). Connect and the slot are healthy. " +
	"A past room had the same hard delete; the fix is to restore the schema under its original id 2 via IMPORT mode and restart order-sync."

const schemaDeletedNoHistory = "Schema Registry no longer has schema id 2 (version 1 of pg.public.orders-value): it was " +
	"deleted, so order-sync cannot decode records written with it (error code 40403). Connect and the slot are healthy."

func schemaDeletedScript(withTool bool, hypothesis string) []step {
	s := []step{
		{tool: "kafka_consumer_lag", input: map[string]any{"group": "order-sync"}},
		{tool: "schema_registry_versions", input: map[string]any{"subject": "pg.public.orders-value"}},
	}
	if withTool {
		s = append(s, step{tool: "similar_incidents", input: map[string]any{"query": "order-sync Schema 2 not found 40403"}})
	}
	return append(s, step{hypothesis: hypothesis}, step{idle: true})
}

func TestRecurringSchemaDeleted(t *testing.T) {
	sc, err := LoadScenario("scenarios/recurring-schema-deleted.yaml")
	if err != nil {
		t.Fatal(err)
	}
	good := Judge(sc, transcriptOf(t, sc, schemaDeletedScript(true, schemaDeletedHypothesis)), nil, false)
	if !good.Pass || good.Score != 100 {
		t.Fatalf("pass=%v score=%v failures=%v", good.Pass, good.Score, good.Failures)
	}
	// The right words without ever asking the room store: the evidence tool is missing.
	noTool := Judge(sc, transcriptOf(t, sc, schemaDeletedScript(false, schemaDeletedHypothesis)), nil, false)
	if noTool.Pass {
		t.Error("a run that never called similar_incidents passed")
	}
	// Called the tool but did not name the past fix: root cause incomplete.
	noFix := Judge(sc, transcriptOf(t, sc, schemaDeletedScript(true, schemaDeletedNoHistory)), nil, false)
	if noFix.Pass || noFix.RootCause {
		t.Errorf("hypothesis without the past fix passed: pass=%v rootCause=%v", noFix.Pass, noFix.RootCause)
	}
}

const connectorPausedHypothesis = "pg-orders-connector is PAUSED, not failed: the slot is active but nothing flows, so the " +
	"warehouse is 7.6 h stale. A past room shows the nightly maintenance job pauses it at 02:00 and its resume call timed out. " +
	"Resuming is safe."

func connectorPausedScript(withTool bool, proposed string) []step {
	s := []step{
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "pg_replication_slots", input: map[string]any{}},
	}
	if withTool {
		s = append(s, step{tool: "similar_incidents", input: map[string]any{"query": "orders stale connector paused"}})
	}
	s = append(s, step{hypothesis: connectorPausedHypothesis})
	if proposed != "" {
		s = append(s, proposal(ps(proposed, `{"connector":"pg-orders-connector"}`)))
	}
	return append(s, step{idle: true})
}

func TestRecurringConnectorPaused(t *testing.T) {
	sc, err := LoadScenario("scenarios/recurring-connector-paused.yaml")
	if err != nil {
		t.Fatal(err)
	}
	good := Judge(sc, transcriptOf(t, sc, connectorPausedScript(true, "connect_resume_connector")), nil, false)
	if !good.Pass || good.Score != 100 {
		t.Fatalf("pass=%v score=%v failures=%v bad=%v", good.Pass, good.Score, good.Failures, good.BadSteps)
	}
	if r := Judge(sc, transcriptOf(t, sc, connectorPausedScript(false, "connect_resume_connector")), nil, false); r.Pass {
		t.Error("a run that never called similar_incidents passed")
	}
	if r := Judge(sc, transcriptOf(t, sc, connectorPausedScript(true, "connect_restart_connector")), nil, false); r.Pass {
		t.Error("restarting a paused connector passed")
	}
}
