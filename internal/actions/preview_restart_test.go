package actions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// The QA run showed "Preview unavailable" for connect_restart_task. The preview
// reads the task's state, says what to expect within ~60 s, and writes nothing.
func TestPreviewRestartTaskStateAndExpectation(t *testing.T) {
	cases := []struct {
		name        string
		status      clients.ConnectorStatus
		task        int
		wantCurrent string
	}{
		{
			"connector-auth: RUNNING for Connect while it crash-loops",
			clients.ConnectorStatus{State: "RUNNING", WorkerID: "w:8083", Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING", WorkerID: "172.18.0.6:8083"}}},
			0, "RUNNING on 172.18.0.6:8083 (GET /connectors/pg-orders-connector/status)",
		},
		{
			"failed task falls back to the connector's worker",
			clients.ConnectorStatus{State: "RUNNING", WorkerID: "w:8083", Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING"}, {ID: 1, State: "FAILED"}}},
			1, "FAILED on w:8083 (GET /connectors/pg-orders-connector/status)",
		},
		{
			"task not listed",
			clients.ConnectorStatus{State: "RUNNING", Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING"}}},
			3, "task 3 is not listed in the status (GET /connectors/pg-orders-connector/status)",
		},
		{
			"no state at all",
			clients.ConnectorStatus{Tasks: []clients.TaskStatus{{ID: 0}}},
			0, "UNKNOWN on UNKNOWN (GET /connectors/pg-orders-connector/status)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := &pvConnect{status: tc.status}
			res, err := (&restartTask{d: Deps{Connect: fc}}).Preview(context.Background(),
				raw(t, map[string]any{"connector": "pg-orders-connector", "task": tc.task}))
			if err != nil {
				t.Fatal(err)
			}
			if res.Target != fmt.Sprintf("connector pg-orders-connector task %d", tc.task) {
				t.Errorf("Target = %q", res.Target)
			}
			if res.Current != tc.wantCurrent {
				t.Errorf("Current = %q, want %q", res.Current, tc.wantCurrent)
			}
			if want := fmt.Sprintf("task %d restarts and should be RUNNING within ~60 s", tc.task); !strings.HasPrefix(res.Expected, want) {
				t.Errorf("Expected = %q, want it to start with %q", res.Expected, want)
			}
			// A restart cannot be undone, and it has no dry run.
			if res.Reversible || res.Rollback != "" || res.DryRun != nil || res.Downstream != nil {
				t.Errorf("unexpected fields: %+v", res)
			}
			// It read the status and nothing else.
			if len(fc.calls) != 1 || fc.calls[0] != "Status" {
				t.Errorf("client calls = %v, want [Status]", fc.calls)
			}
		})
	}
}

func TestPreviewRestartConnectorStateAndExpectation(t *testing.T) {
	fc := &pvConnect{status: clients.ConnectorStatus{State: "RUNNING", Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED"}, {ID: 1, State: "RUNNING"}}}}
	res, err := (&restartConnector{d: Deps{Connect: fc}}).Preview(context.Background(),
		raw(t, map[string]any{"connector": "pg-orders-connector"}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Target != "connector pg-orders-connector" || !strings.Contains(res.Current, "task 0 FAILED") ||
		res.Expected != "connector and its failed tasks RUNNING" || res.Reversible || res.DryRun != nil {
		t.Fatalf("unexpected: %+v", res)
	}
	if len(fc.calls) != 1 || fc.calls[0] != "Status" {
		t.Errorf("client calls = %v, want [Status]", fc.calls)
	}
}

// A status read that fails is an error the room stores on the step, with the
// action named; it never panics and never writes.
func TestPreviewRestartFailuresAreWrapped(t *testing.T) {
	boom := errors.New("connect: GET /connectors/pg-orders-connector/status: connection refused")
	for name, a := range map[string]Preview{
		"task":      &restartTask{d: Deps{Connect: &pvConnect{statErr: boom}}},
		"connector": &restartConnector{d: Deps{Connect: &pvConnect{statErr: boom}}},
	} {
		t.Run(name, func(t *testing.T) {
			params := map[string]any{"connector": "pg-orders-connector", "task": 0}
			_, err := a.Preview(context.Background(), raw(t, params))
			if !errors.Is(err, boom) || !strings.HasPrefix(err.Error(), "actions: preview connect_restart_") {
				t.Fatalf("err = %v", err)
			}
		})
	}
	// Bad params: no client call at all.
	for _, bad := range []map[string]any{{"connector": "", "task": 0}, {"connector": "c", "task": -1}, {}} {
		fc := &pvConnect{}
		_, err := (&restartTask{d: Deps{Connect: fc}}).Preview(context.Background(), raw(t, bad))
		if err == nil || len(fc.calls) != 0 {
			t.Errorf("params %v: err = %v, calls %v", bad, err, fc.calls)
		}
	}
}

// The room gives a preview 10 s; the action keeps its own bound too, so a hung
// Connect cannot hold the proposal's preview open past it.
func TestPreviewRestartTaskStopsWithItsContext(t *testing.T) {
	fc := &pvConnect{block: true}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := (&restartTask{d: Deps{Connect: fc}}).Preview(ctx, raw(t, map[string]any{"connector": "c", "task": 0}))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Preview ignored the cancelled context")
	}
}

// Through the registry, the way the room finds a preview: every restart and
// resume action offers one.
func TestRestartActionsOfferAPreview(t *testing.T) {
	reg := resumeRegistry(t, testDeps())
	for _, name := range []string{"connect_restart_task", "connect_restart_connector", "connect_resume_connector"} {
		a := mustGet(t, reg, name)
		if _, ok := a.(Preview); !ok {
			t.Errorf("%s has no Preview", name)
		}
	}
}
