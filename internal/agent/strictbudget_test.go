package agent

import (
	"encoding/json"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/tools"
	"piperoom/internal/tools/runbooks"
)

// productionToolSet mirrors what internal/wiring sends: every real read tool
// (db, kafka, runbook) plus the room tools with the real actions registry, so
// propose_fix carries the nested params of every registered action.
// Registering actions needs no live clients.
func productionToolSet(t *testing.T) (*tools.Registry, []tools.Tool) {
	t.Helper()
	reg := fullRegistry(t)
	if err := runbooks.Register(reg); err != nil {
		t.Fatal(err)
	}
	areg := actions.NewRegistry()
	if err := actions.Register(areg, actions.Deps{}); err != nil {
		t.Fatal(err)
	}
	return reg, newRoomTools(Deps{Actions: areg})
}

// strictSchemaBytes sums the marshalled input_schema size of every tool the
// request sends with strict: true, read back from the wire JSON.
func strictSchemaBytes(t *testing.T, tl toolList) (total, count int) {
	t.Helper()
	for _, p := range tl.params {
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatalf("marshal tool param: %v", err)
		}
		var def struct {
			Name        string          `json:"name"`
			Strict      *bool           `json:"strict"`
			InputSchema json.RawMessage `json:"input_schema"`
		}
		if err := json.Unmarshal(b, &def); err != nil {
			t.Fatalf("unmarshal tool param: %v", err)
		}
		if def.Strict == nil || !*def.Strict {
			continue
		}
		total += len(def.InputSchema)
		count++
	}
	return total, count
}

// The API rejects a request whose strict schemas compile to a grammar that is
// too large. A new or grown strict schema must not push the shipped strict set
// past the byte budget derived from the live bisect (see maxStrictSchemaBytes).
func TestStrictSchemaBytesWithinBudget(t *testing.T) {
	reg, room := productionToolSet(t)

	got, n := strictSchemaBytes(t, buildToolList(reg, room))
	if n != maxStrictTools {
		t.Fatalf("%d strict tools, want %d", n, maxStrictTools)
	}
	t.Logf("cap %d: %d strict tools, %d schema bytes (budget %d)", maxStrictTools, n, got, maxStrictSchemaBytes)
	if got > maxStrictSchemaBytes {
		t.Errorf("strict tool schemas total %d bytes, budget is %d: shrink a strict schema or lower maxStrictTools, and re-run TestLiveStrictToolCap (-tags claude)", got, maxStrictSchemaBytes)
	}

	// The budget must stay below the largest sum proven accepted live, or a
	// set inside the budget could still be rejected.
	if maxStrictSchemaBytes >= liveAcceptedSchemaBytes {
		t.Errorf("budget %d bytes is not below the largest live-accepted sum (%d bytes)", maxStrictSchemaBytes, liveAcceptedSchemaBytes)
	}
}
