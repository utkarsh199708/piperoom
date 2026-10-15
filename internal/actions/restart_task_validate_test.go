package actions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

// Q6 U6: a task id the connector does not list is refused at proposal creation,
// not approved and then failed with a 404.
func TestValidateRestartTaskRefusesUnlistedTask(t *testing.T) {
	reg := NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("connect_restart_task")
	if err := a.Validate(t.Context(), json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)); err != nil {
		t.Fatalf("listed task refused: %v", err)
	}
	err := a.Validate(t.Context(), json.RawMessage(`{"connector":"pg-orders-connector","task":7}`))
	if err == nil || !strings.Contains(err.Error(), "task 7") {
		t.Fatalf("want an error naming task 7, got %v", err)
	}
}

// statusConnect wraps the test Connect fake with a chosen Status answer.
type statusConnect struct {
	*fakeConnect
	st  clients.ConnectorStatus
	err error
}

func (s statusConnect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return s.st, s.err
}

// The U6 check refuses only what the status proves: an unreadable status
// (Connect flaky, replayed evals) or a connector that lists no tasks says
// nothing about the id, so Validate lets the proposal through and Execute
// reports a real 404 if there is one.
func TestValidateRestartTaskFailsOpenOnUnknowns(t *testing.T) {
	for name, c := range map[string]statusConnect{
		"status unreadable": {fakeConnect: &fakeConnect{names: []string{"pg-orders-connector"}}, err: errors.New("connect: 503")},
		"no tasks listed":   {fakeConnect: &fakeConnect{names: []string{"pg-orders-connector"}}, st: clients.ConnectorStatus{Name: "pg-orders-connector", State: "FAILED"}},
	} {
		t.Run(name, func(t *testing.T) {
			reg := NewRegistry()
			d := testDeps()
			d.Connect = c
			if err := Register(reg, d); err != nil {
				t.Fatal(err)
			}
			a, _ := reg.Get("connect_restart_task")
			if err := a.Validate(t.Context(), json.RawMessage(`{"connector":"pg-orders-connector","task":3}`)); err != nil {
				t.Fatalf("Validate = %v, want nil when the status proves nothing", err)
			}
		})
	}
}
