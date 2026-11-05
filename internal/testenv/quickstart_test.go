//go:build integration

package testenv

import (
	"bytes"
	"context"
	"net/http"
	"os/exec"
	"slices"
	"testing"
	"time"

	"piperoom/internal/events"
)

// TestQuickstart is the P1f check of the read-only quickstart
// (tasks/P1-quickstart-readonly.md item 6): discover -write-owners, server
// --quickstart, the schema-change alert, a room, and no action.executed.
//
// It never skips once the stack is up, so CI's fail-on-skips needs no entry:
//
//   - Without ANTHROPIC_API_KEY (CI) the model endpoint is a closed loopback
//     port, and the documented no-key behaviour is asserted: the room opens,
//     the agent reports agent.status "error", nothing is proposed or run.
//   - With ANTHROPIC_API_KEY the test breaks the pipeline the way the demo
//     does (`devctl break schema-change`, so the stack lock must be held by
//     PIPEROOM_STACK_OWNER and orders.amount must still be integer: run
//     `devctl reset` first if an earlier run or demo changed it), waits for
//     the agent's tool calls and its proposal, approves every layer as the
//     quickstart's one user, and asserts the proposal is blocked by write
//     mode and no action.executed is written. Cleanup runs `devctl fix
//     schema-change` (the connector runs again; amount stays numeric).
func TestQuickstart(t *testing.T) {
	qs := Quickstart(t)

	t.Run("healthz says write mode off", func(t *testing.T) {
		var h struct {
			Status    string `json:"status"`
			WriteMode string `json:"write_mode"`
		}
		if code := qs.Do(t, http.MethodGet, "/healthz", nil, &h); code != http.StatusOK {
			t.Fatalf("GET /healthz: status %d", code)
		}
		if h.Status != "ok" || h.WriteMode != "off" {
			t.Fatalf("GET /healthz = %+v, want status ok, write_mode off", h)
		}
	})

	t.Run("the one user owns the discovered layers", func(t *testing.T) {
		// LEAD-ARCH's risk on #600: --quickstart gives its user only the
		// owners file's top-level layers: block. Quickstart already failed if
		// discover wrote per-pipeline owners: only; this checks the server
		// side of the same pair.
		t.Logf("owners file shape: %s", qs.OwnersShape)
		var me struct {
			ID     string   `json:"id"`
			Layers []string `json:"layers"`
		}
		if code := qs.Do(t, http.MethodGet, "/api/me", nil, &me); code != http.StatusOK {
			t.Fatalf("GET /api/me: status %d", code)
		}
		want := topLayers(qs.Owners)
		if me.ID != qs.User || len(me.Layers) == 0 || !sameLayers(me.Layers, want) {
			t.Fatalf("GET /api/me = %+v, want id %s with layers %v (the owners file's layers: block)", me, qs.User, want)
		}
	})

	t.Run("the discovered pipeline is served", func(t *testing.T) {
		var ps []struct {
			ID string `json:"id"`
		}
		if code := qs.Do(t, http.MethodGet, "/api/pipelines", nil, &ps); code != http.StatusOK {
			t.Fatalf("GET /api/pipelines: status %d", code)
		}
		if !slices.ContainsFunc(ps, func(p struct {
			ID string `json:"id"`
		}) bool {
			return p.ID == qs.PipelineID
		}) {
			t.Fatalf("GET /api/pipelines = %+v, want %s", ps, qs.PipelineID)
		}
	})

	if qs.WithKey {
		breakSchemaChange(t, qs)
	}

	// The alert the watcher would raise for the schema-change scenario,
	// through the dev webhook (docs/pilot/setup.md section 6).
	alert := map[string]any{
		"title":       "pg-orders-connector task 0 FAILED",
		"severity":    "sev2",
		"pipeline_id": qs.PipelineID,
		"dedup_key":   Name("schema_change"),
		"scenario":    "schema-change",
	}
	var snap struct {
		ID         string `json:"id"`
		PipelineID string `json:"pipeline_id"`
		Status     string `json:"status"`
	}
	if code := qs.Do(t, http.MethodPost, "/api/alerts/webhook", alert, &snap); code != http.StatusCreated {
		t.Fatalf("POST /api/alerts/webhook: status %d, want 201", code)
	}
	if snap.ID == "" || snap.PipelineID != qs.PipelineID || snap.Status != "open" {
		t.Fatalf("webhook snapshot = %+v, want an open room on %s", snap, qs.PipelineID)
	}
	roomID := snap.ID

	t.Run("room opened", func(t *testing.T) {
		evs := qs.Events(t, roomID)
		if len(evs) == 0 || evs[0].Type != events.RoomCreated {
			t.Fatalf("room %s log starts with %s, want room.created", roomID, typeCounts(evs))
		}
		var s struct {
			Mode struct {
				Write bool `json:"write"`
			} `json:"mode"`
		}
		if code := qs.Do(t, http.MethodGet, "/api/rooms/"+roomID, nil, &s); code != http.StatusOK {
			t.Fatalf("GET /api/rooms/%s: status %d", roomID, code)
		}
		if s.Mode.Write {
			t.Fatal("snapshot mode.write = true under --quickstart")
		}
	})

	if qs.WithKey {
		t.Run("agent investigates and proposes, nothing runs", func(t *testing.T) {
			agentProposesNothingRuns(t, qs, roomID)
		})
	} else {
		t.Run("agent without a key reports an error, nothing runs", func(t *testing.T) {
			var evs []events.Event
			WaitFor(t, 3*time.Minute, func() (bool, error) {
				evs = qs.Events(t, roomID)
				_, ok := agentErrored(evs)
				return ok, nil
			})
			detail, _ := agentErrored(evs)
			t.Logf("agent.status error: %s", detail)
			for _, typ := range []events.Type{events.ProposalCreated, events.ActionExecuted} {
				if _, ok := firstOfType(evs, typ); ok {
					t.Fatalf("%s without a model: %s", typ, typeCounts(evs))
				}
			}
		})
	}
}

// breakSchemaChange runs `devctl break schema-change` and registers `devctl
// fix schema-change` for cleanup. devctl itself refuses without the stack
// lock or on an already changed schema, and says what to do.
func breakSchemaChange(t *testing.T, qs *QuickstartServer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	devctl := BuildBinary(ctx, t, qs.BinDir, "devctl")
	root, err := moduleRoot(ctx)
	if err != nil {
		t.Fatalf("%v", err)
	}
	run := func(ctx context.Context, verb string) error {
		cmd := exec.CommandContext(ctx, devctl, verb, "schema-change")
		cmd.Dir = root // devctl finds dev/docker-compose.yml from the checkout
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		err := cmd.Run()
		if err != nil {
			t.Logf("devctl %s schema-change:\n%s", verb, out.String())
		}
		return err
	}
	if err := run(ctx, "break"); err != nil {
		t.Fatalf("devctl break schema-change: %v (hold the stack lock as PIPEROOM_STACK_OWNER; run `devctl reset` if orders.amount is no longer integer)", err)
	}
	t.Cleanup(func() {
		fctx, fcancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer fcancel()
		if err := run(fctx, "fix"); err != nil {
			t.Errorf("devctl fix schema-change: %v (the dev pipeline stays broken: run `devctl fix schema-change` or `devctl reset`)", err)
		}
	})
}

// agentProposesNothingRuns waits for the real agent's tool calls and its
// proposal, approves every required layer as the quickstart's one user, and
// checks that write mode off keeps the steps from running.
func agentProposesNothingRuns(t *testing.T, qs *QuickstartServer, roomID string) {
	t.Helper()
	var evs []events.Event
	WaitFor(t, 10*time.Minute, func() (bool, error) {
		evs = qs.Events(t, roomID)
		_, ok := firstOfType(evs, events.ProposalCreated)
		return ok, nil
	})
	if !agentRan(evs) {
		t.Fatalf("a proposal with no agent.tool_call before it: %s", typeCounts(evs))
	}

	var snap struct {
		OpenProposals []struct {
			ProposalID     string            `json:"proposal_id"`
			ParamsHash     string            `json:"params_hash"`
			RequiredLayers []string          `json:"required_layers"`
			Approvals      map[string]string `json:"approvals"`
			Blocked        string            `json:"blocked"`
		} `json:"open_proposals"`
	}
	if code := qs.Do(t, http.MethodGet, "/api/rooms/"+roomID, nil, &snap); code != http.StatusOK {
		t.Fatalf("GET /api/rooms/%s: status %d", roomID, code)
	}
	if len(snap.OpenProposals) == 0 {
		t.Fatalf("proposal.created in the log but no open proposal in the snapshot: %s", typeCounts(evs))
	}
	p := snap.OpenProposals[0]
	t.Logf("proposal %s, required layers %v", p.ProposalID, p.RequiredLayers)
	body := map[string]any{"params_hash": p.ParamsHash}
	if code := qs.Do(t, http.MethodPost, "/api/proposals/"+p.ProposalID+"/approve-all", body, nil); code != http.StatusOK && code != http.StatusCreated {
		t.Fatalf("POST /api/proposals/%s/approve-all as %s: status %d", p.ProposalID, qs.User, code)
	}

	// The executor would have run by now with write mode on; give it the
	// same time it would get, then check it did not.
	WaitFor(t, time.Minute, func() (bool, error) {
		if code := qs.Do(t, http.MethodGet, "/api/rooms/"+roomID, nil, &snap); code != http.StatusOK {
			return false, nil
		}
		for _, op := range snap.OpenProposals {
			if op.ProposalID == p.ProposalID {
				return op.Blocked == "write_mode_off", nil
			}
		}
		return false, nil
	})
	time.Sleep(5 * time.Second)
	evs = qs.Events(t, roomID)
	if e, ok := firstOfType(evs, events.ActionExecuted); ok {
		t.Fatalf("action.executed (seq %d) with write mode off: %s", e.Seq, typeCounts(evs))
	}
}
