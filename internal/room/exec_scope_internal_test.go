package room

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
)

// pipelineCtx gives an action call the room's whole pipeline when the owners
// config resolves it, and otherwise nothing a write client accepts: never the
// tools.WithUnscoped opt-out, never a pipeline the parent ctx carried.
func TestPipelineCtx(t *testing.T) {
	orders := owners.Pipeline{ID: "orders", SourceTables: []string{"public.orders"}}
	payments := owners.Pipeline{ID: "payments", SourceTables: []string{"public.payments"}}
	known := &owners.Owners{Pipelines: []owners.Pipeline{orders, payments}}
	bg := context.Background()
	for _, tc := range []struct {
		name   string
		owners *owners.Owners
		parent context.Context
		id     string
		want   string // id of the whole pipeline in ctx; "" means none
	}{
		{"known pipeline", known, bg, "orders", "orders"},
		{"room pipeline wins over a carried one", known, tools.WithPipeline(bg, payments), "orders", "orders"},
		{"no pipeline", known, bg, "", ""},
		{"no pipeline clears a carried one", known, tools.WithPipeline(bg, payments), "", ""},
		{"pipeline not in the owners config", known, bg, "ghost", ""},
		{"no owners config", nil, bg, "orders", ""},
		{"empty id never matches a pipeline", &owners.Owners{Pipelines: []owners.Pipeline{{ID: "", SourceTables: []string{"public.orders"}}}}, bg, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Service{owners: tc.owners}
			ctx := s.pipelineCtx(tc.parent, tc.id)
			p, whole := tools.PipelineFrom(ctx)
			// The write clients resolve with no owners config of their own.
			sc, err := tools.ScopeFor(ctx, nil)
			if tc.want != "" {
				if !whole || p.ID != tc.want {
					t.Fatalf("ctx pipeline %q (whole=%v), want %q", p.ID, whole, tc.want)
				}
				if err != nil || sc.Unscoped() || sc.Pipeline().ID != tc.want {
					t.Fatalf("ScopeFor = %q unscoped=%v err=%v, want %q", sc.Pipeline().ID, sc.Unscoped(), err, tc.want)
				}
				return
			}
			if whole {
				t.Fatalf("ctx carries pipeline %q, want none", p.ID)
			}
			if err == nil {
				t.Fatalf("ScopeFor = unscoped %v, nil error; want a refusal", sc.Unscoped())
			}
			if tc.id == "" && !errors.Is(err, tools.ErrNoScope) {
				t.Fatalf("ScopeFor err = %v, want ErrNoScope", err)
			}
		})
	}
}

// scopeRecAction is a postgres-layer action that records whether each
// Validate saw the room's whole pipeline and counts its Executes. With strict
// set its Validate fails closed like a write client's check.
type scopeRecAction struct {
	strict    bool
	mu        sync.Mutex
	validates []bool
	executes  int
}

func (*scopeRecAction) Name() string                                      { return "scope_rec" }
func (*scopeRecAction) Layer() string                                     { return "postgres" }
func (*scopeRecAction) Describe() string                                  { return "records its scope" }
func (*scopeRecAction) ParamsSchema() map[string]any                      { return map[string]any{"type": "object"} }
func (*scopeRecAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }

func (a *scopeRecAction) Validate(ctx context.Context, _ json.RawMessage) error {
	_, whole := tools.PipelineFrom(ctx)
	sc, err := tools.ScopeFor(ctx, nil)
	a.mu.Lock()
	a.validates = append(a.validates, whole)
	a.mu.Unlock()
	if err == nil && sc.Unscoped() {
		return errors.New("validate ran unscoped")
	}
	if a.strict && !whole {
		return tools.ErrNoScope
	}
	return nil
}

func (a *scopeRecAction) Execute(context.Context, json.RawMessage) (string, error) {
	a.mu.Lock()
	a.executes++
	a.mu.Unlock()
	return "written", nil
}

// An approved proposal in a room whose pipeline the owners config does not
// resolve never writes: its Validate gets no pipeline and no opt-out, so a
// write client's check refuses there, and the scope check refuses before
// Execute when Validate does not look. The step is recorded as failed.
func TestExecutorWithoutTheRoomPipelineWritesNothing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		owners *owners.Owners
		strict bool
		want   string
	}{
		{"no owners config, validate checks the scope", nil, true, "validation failed: " + tools.ErrNoScope.Error()},
		{"no owners config, validate does not look", nil, false, "scope check failed"},
		{"pipeline not in the owners config, validate checks the scope", &owners.Owners{Pipelines: []owners.Pipeline{{ID: "payments"}}}, true, "validation failed: " + tools.ErrNoScope.Error()},
		{"pipeline not in the owners config, validate does not look", &owners.Owners{Pipelines: []owners.Pipeline{{ID: "payments"}}}, false, "scope check failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mem := memstore.New()
			act := &scopeRecAction{strict: tc.strict}
			reg := actions.NewRegistry()
			if err := reg.Register(act); err != nil {
				t.Fatal(err)
			}
			svc := New(Deps{Store: mem, Actions: reg, Users: DefaultUsers(), Owners: tc.owners, Run: idleRun})
			t.Cleanup(svc.Close)
			// An alert room opens on a pipeline the owners config does not
			// know (with proposals disabled), so the row below stands in for
			// one that reached the executor anyway.
			snap, _, err := svc.OpenAlertRoom(ctx, alertin.Alert{
				Source: alertin.Generic, Status: alertin.StatusFiring, Title: "lag", Severity: "sev2", PipelineID: "orders",
			})
			if err != nil {
				t.Fatalf("open room: %v", err)
			}

			steps := []events.ProposalStep{{Action: "scope_rec", Layer: "postgres", Params: json.RawMessage(`{}`)}}
			layers := []string{"postgres"}
			const pid = "p_scope"
			hash, err := ComputeParamsHash(snap.ID, pid, "fix", "low", "undo", layers, steps)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(steps)
			if err != nil {
				t.Fatal(err)
			}
			row := store.ProposalRow{
				ProposalID: pid, RoomID: snap.ID, Title: "fix", Status: "open", RequiredLayers: layers,
				Steps: raw, Risk: "low", Rollback: "undo", ParamsHash: hash,
			}
			if err := mem.CreateProposal(ctx, row); err != nil {
				t.Fatal(err)
			}

			if err := svc.executeProposal(row, false); err != nil {
				t.Fatalf("executeProposal: %v", err)
			}

			act.mu.Lock()
			validates, executes := append([]bool(nil), act.validates...), act.executes
			act.mu.Unlock()
			if len(validates) != 1 || validates[0] {
				t.Fatalf("validate saw the whole pipeline %v, want one call without it", validates)
			}
			if executes != 0 {
				t.Fatalf("Execute ran %d times without the room's pipeline", executes)
			}
			if p, err := mem.GetProposal(ctx, pid); err != nil || p.Status != "failed" {
				t.Fatalf("proposal = %+v, %v; want failed", p, err)
			}
			evs, err := mem.Since(ctx, snap.ID, 0, 1000)
			if err != nil {
				t.Fatal(err)
			}
			var outs []string
			for _, e := range evs {
				if e.Type != events.ActionExecuted {
					continue
				}
				p, err := events.Decode[events.ActionExecutedPayload](e)
				if err != nil {
					t.Fatal(err)
				}
				if p.OK {
					t.Fatalf("step %d recorded OK", p.StepIndex)
				}
				outs = append(outs, p.Output)
			}
			if len(outs) != 1 || !strings.HasPrefix(outs[0], tc.want) {
				t.Fatalf("action.executed outputs %q, want one starting %q", outs, tc.want)
			}
		})
	}
}
