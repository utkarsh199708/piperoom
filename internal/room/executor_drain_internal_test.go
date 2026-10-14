package room

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store/memstore"
)

// countAction counts its runs.
type countAction struct{ calls atomic.Int32 }

func (a *countAction) Name() string                                      { return "count_act" }
func (a *countAction) Layer() string                                     { return "postgres" }
func (a *countAction) Describe() string                                  { return "counts its runs" }
func (a *countAction) ParamsSchema() map[string]any                      { return map[string]any{"type": "object"} }
func (a *countAction) Validate(json.RawMessage) error                    { return nil }
func (a *countAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (a *countAction) Execute(context.Context, json.RawMessage) (string, error) {
	a.calls.Add(1)
	return "done", nil
}

// An approved proposal still queued when the executor's context ends is
// recorded as failed, never dropped: the worker is parked in its select and
// sees only the cancel (no wake), the case where it used to return with the
// proposal still queued.
func TestExecutorRecordsQueuedProposalsWhenItStops(t *testing.T) {
	for _, tc := range []struct {
		name      string
		closeRoom bool
		want      string
	}{
		{"room closed", true, "room closed before this step ran"},
		{"server stopping", false, "the server stopped before this proposal ran"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mem := memstore.New()
			act := &countAction{}
			reg := actions.NewRegistry()
			if err := reg.Register(act); err != nil {
				t.Fatalf("register: %v", err)
			}
			svc := New(Deps{
				Store:   mem,
				Actions: reg,
				Users:   DefaultUsers(),
				Owners:  &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}},
				Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
					<-ctx.Done()
					return nil
				},
			})
			t.Cleanup(svc.Close)
			arjun, _ := svc.LookupUser("arjun")
			snap, err := svc.CreateRoom(ctx, arjun, "t", "sev2", "orders")
			if err != nil {
				t.Fatalf("create room: %v", err)
			}
			pid, err := svc.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
				Title: "fix", Risk: "low", Rollback: "revert the change",
				Steps: []events.ProposalStep{{Action: "count_act", Layer: "postgres", Params: json.RawMessage(`{}`)}},
			}, events.AgentActor)
			if err != nil {
				t.Fatalf("create proposal: %v", err)
			}
			row, err := mem.GetProposal(ctx, pid)
			if err != nil {
				t.Fatalf("get proposal: %v", err)
			}

			q := newExecQueue()
			execCtx, cancel := context.WithCancel(context.Background())
			svc.wg.Add(1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				svc.runExecutor(execCtx, snap.ID, q)
			}()
			// Let the worker reach its select; the fix holds either way.
			time.Sleep(20 * time.Millisecond)
			if tc.closeRoom {
				if _, err := svc.CloseRoom(ctx, arjun, snap.ID, "fixed by hand"); err != nil {
					t.Fatalf("close: %v", err)
				}
			}
			q.mu.Lock()
			q.items = append(q.items, row) // queued without a wake
			q.mu.Unlock()
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("executor did not stop after its context ended")
			}

			if q.busy() {
				t.Fatal("executor stopped with work still queued")
			}
			if n := act.calls.Load(); n != 0 {
				t.Fatalf("action ran %d times after the executor stopped", n)
			}
			if p, err := mem.GetProposal(ctx, pid); err != nil || p.Status != "failed" {
				t.Fatalf("proposal = %+v, %v; want failed", p, err)
			}
			evs, err := mem.Since(ctx, snap.ID, 0, 1000)
			if err != nil {
				t.Fatalf("events: %v", err)
			}
			found := false
			for _, e := range evs {
				if e.Type != events.ActionExecuted {
					continue
				}
				p, ok := decode[events.ActionExecutedPayload](e)
				if ok && p.ProposalID == pid && !p.OK && !p.OutcomeUnknown && strings.Contains(p.Output, tc.want) {
					found = true
				}
			}
			if !found {
				t.Fatalf("no failing action.executed saying %q for %s", tc.want, pid)
			}
		})
	}
}
