package events_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/events"
)

// The setup layer is last in the CDC list (code that indexes the six by
// position is unchanged), known to Layers, and absent from batch.
func TestEnvironmentLayer(t *testing.T) {
	cdc := events.LayersOf(events.PipelineKindCDC)
	if cdc[len(cdc)-1] != events.LayerEnvironment {
		t.Fatalf("LayersOf(cdc) = %v, want %q last", cdc, events.LayerEnvironment)
	}
	if !slices.Contains(events.Layers, events.LayerEnvironment) {
		t.Fatalf("Layers = %v lacks %q", events.Layers, events.LayerEnvironment)
	}
	if slices.Contains(events.LayersOf(events.PipelineKindBatch), events.LayerEnvironment) {
		t.Fatalf("batch pipelines have no setup layer: %v", events.LayersOf(events.PipelineKindBatch))
	}
	// environment was appended last by U0; Q8-0 appended mysql after it, so
	// environment keeps its index and every older list keeps its order.
	if events.Layers[8] != events.LayerEnvironment || events.Layers[len(events.Layers)-1] != events.LayerMySQL {
		t.Fatalf("Layers = %v, want %q at index 8 and %q last so older lists keep their order", events.Layers, events.LayerEnvironment, events.LayerMySQL)
	}
}

// IncidentLayers is the pre-U Layers list (what the triage schema and the
// label validators saw before U0), in order, plus the Q8 source layer mysql
// at the end, and a fresh slice.
func TestIncidentLayers(t *testing.T) {
	want := []string{"postgres", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse", "orchestrator", "transform", "mysql"}
	got := events.IncidentLayers()
	if !slices.Equal(got, want) {
		t.Fatalf("IncidentLayers() = %v, want %v", got, want)
	}
	got[0] = "changed"
	if events.IncidentLayers()[0] != "postgres" || events.Layers[0] != "postgres" {
		t.Fatal("IncidentLayers shares its slice")
	}
}

// The U-wave fields on payloads that already exist are omitempty, so an event
// written before U0 marshals exactly as before (export hashes).
func TestUWaveFieldsAreOmittedWhenEmpty(t *testing.T) {
	tests := []struct {
		name   string
		v      any
		absent []string
	}{
		{"room mode", events.RoomCreatedPayload{Title: "t", Severity: "sev2", PipelineID: "p"}, []string{"mode"}},
		{"action receipt", events.ActionExecutedPayload{ProposalID: "p", OK: true, Output: "ok"}, []string{"receipt"}},
	}
	for _, tt := range tests {
		raw, err := json.Marshal(tt.v)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		for _, k := range tt.absent {
			if strings.Contains(string(raw), `"`+k+`"`) {
				t.Errorf("%s: %s has %q when empty", tt.name, raw, k)
			}
		}
	}
}

func TestUWavePayloadShapes(t *testing.T) {
	raw, err := json.Marshal(events.RoomCreatedPayload{Title: "Set up orders", Severity: "sev4", PipelineID: "orders", Mode: events.RoomModeSetup})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"mode":"setup"`) {
		t.Errorf("room.created = %s", raw)
	}

	var req events.SetupRequest
	if err := json.Unmarshal([]byte(`{"pipeline_id":"orders","tenant":"data","title":"Set up orders","source_kind":"postgres","target":"compose","topic_prefix":"orders","tables":["public.orders"],"targets":[{"kind":"postgres","address":"db.acme:5432","class":"production"}]}`), &req); err != nil {
		t.Fatal(err)
	}
	if req.Tenant != "data" || req.Target != "compose" || len(req.Targets) != 1 || req.Targets[0].Class != "production" || req.Targets[0].Address != "db.acme:5432" {
		t.Errorf("SetupRequest decoded as %+v", req)
	}

	raw, err = json.Marshal(events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 2, OK: true, Output: "up", Receipt: &events.SetupReceipt{
		Kind: "exec", Argv: []string{"docker", "compose", "-f", "compose.yaml", "up", "-d"}, Target: "/home/u/orders", OutMAC: "ab",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"receipt":{"kind":"exec","argv":["docker","compose","-f","compose.yaml","up","-d"],"target":"/home/u/orders","exit_code":0,"at":"0001-01-01T00:00:00Z","took_ms":0,"out_mac":"ab"}`) {
		t.Errorf("action.executed with receipt = %s", raw)
	}

	var verified events.SetupVerifiedPayload
	if err := json.Unmarshal([]byte(`{"plan_id":"pl_1","nonce_hash":"ff","rungs":[{"name":"connector_running","ok":true,"step_id":"s_9"},{"name":"canary_event","ok":false,"note":"not seen in 60 s"}]}`), &verified); err != nil {
		t.Fatal(err)
	}
	if verified.PlanID != "pl_1" || len(verified.Rungs) != 2 || verified.Rungs[1].OK || verified.FirstEventLatencyMS != nil {
		t.Errorf("setup.verified decoded as %+v", verified)
	}

	raw, err = json.Marshal(events.SetupRunnerRefusedPayload{Op: "compose_up", Reason: "params_hash", ProposalID: "p_1"})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"op":"compose_up","reason":"params_hash","proposal_id":"p_1"}`; string(raw) != want {
		t.Errorf("setup.runner_refused = %s, want %s", raw, want)
	}

	raw, err = json.Marshal(events.ProposalScheduledPayload{ProposalID: "p_1", Policy: events.PolicySetupLocal})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"policy":"setup_local"`) {
		t.Errorf("proposal.scheduled = %s", raw)
	}
	if events.GrantAdmin != "admin" || events.RoomModeSetup != "setup" || events.WatchedByLocal != "local" {
		t.Error("U-wave constants changed")
	}
}

// Every setup event type is a distinct stable string under the setup. prefix
// (pipeline.registered aside), so the UI and the export can rely on them.
func TestSetupEventTypesAreDistinct(t *testing.T) {
	types := []events.Type{
		events.SetupPairing, events.SetupRunnerAttached, events.SetupRunnerLost, events.SetupRunnerRefused,
		events.SetupPlan, events.SetupTargetRegistered, events.SetupTargetClassed, events.SetupPipelineExtended,
		events.SetupSecretSet, events.SetupCredentialsRegistered, events.SetupMaintenanceWindow, events.SetupVerified,
		events.SetupAwaitingChange, events.SetupBlocked, events.SetupVerificationRequested,
	}
	seen := map[events.Type]bool{}
	for _, ty := range types {
		if !strings.HasPrefix(string(ty), "setup.") {
			t.Errorf("%q is not under setup.", ty)
		}
		if seen[ty] {
			t.Errorf("%q twice", ty)
		}
		seen[ty] = true
	}
	if events.SetupVerificationRequested != "setup.verification_requested" {
		t.Errorf("SetupVerificationRequested = %q", events.SetupVerificationRequested)
	}
	if events.PipelineRegistered != "pipeline.registered" {
		t.Errorf("PipelineRegistered = %q", events.PipelineRegistered)
	}
}
