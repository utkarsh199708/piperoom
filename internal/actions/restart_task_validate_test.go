package actions

import (
	"encoding/json"
	"strings"
	"testing"
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
