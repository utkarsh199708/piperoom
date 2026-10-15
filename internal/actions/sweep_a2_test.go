package actions

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// TestRestartTaskValidateRefusesUnknownTask (QA U6): a task id the connector
// does not list passed Validate, so the proposal went to approvers and only
// failed at Execute with Connect's 404.
func TestRestartTaskValidateRefusesUnknownTask(t *testing.T) {
	a := &restartTask{d: Deps{Connect: &fakeConnect{names: []string{"pg-orders-connector"}}}}
	err := a.Validate(t.Context(), json.RawMessage(`{"connector":"pg-orders-connector","task":7}`))
	if err == nil || !strings.Contains(err.Error(), "task 7") {
		t.Fatalf("Validate(task 7) = %v; want refused: the connector lists only task 0", err)
	}
	if err := a.Validate(t.Context(), json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)); err != nil {
		t.Fatalf("Validate(task 0) = %v", err)
	}
}

// TestRegisterSchemaValidateIgnoresOtherParses: Validate parsed with avro's
// process-wide cache, so a schema naming a type it never defines passed once
// any earlier Validate (another room) had defined that name.
func TestRegisterSchemaValidateIgnoresOtherParses(t *testing.T) {
	a := &registerSchema{d: Deps{Registry: &fakeRegistry{}}}
	params := func(schema string) json.RawMessage {
		b, _ := json.Marshal(registerSchemaParams{Subject: "pg.public.orders-value", Schema: schema})
		return b
	}
	orphan := `{"type":"record","name":"Outer","namespace":"zz.sweep","fields":[{"name":"i","type":"zz.sweep.Inner"}]}`
	defines := `{"type":"record","name":"Inner","namespace":"zz.sweep","fields":[{"name":"x","type":"int"}]}`
	if err := a.Validate(context.Background(), params(orphan)); err == nil || !strings.Contains(err.Error(), "not valid Avro") {
		t.Fatalf("orphan reference first: %v; want refused as invalid Avro", err)
	}
	_ = a.Validate(context.Background(), params(defines))
	if err := a.Validate(context.Background(), params(orphan)); err == nil || !strings.Contains(err.Error(), "not valid Avro") {
		t.Fatalf("orphan reference after another schema defined zz.sweep.Inner: %v; want refused", err)
	}
}

// TestPreviewUpdateConfigAlreadySet: Execute writes nothing when the stored
// value already equals the proposed one; the preview must not show approvers
// a change, a task restart and a rollback.
func TestPreviewUpdateConfigAlreadySet(t *testing.T) {
	fc := &pvConnect{cfg: map[string]string{"connector.class": "io.debezium.X", "decimal.handling.mode": "precise"}}
	a := &updateConnectorConfig{d: Deps{Connect: fc}}
	res, err := a.Preview(context.Background(), raw(t, map[string]any{"connector": "c", "key": "decimal.handling.mode", "value": "precise"}))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(res.Expected, "restarts") || res.Rollback != "" || res.DryRun != nil || !strings.Contains(res.Expected, "no change") {
		t.Fatalf("preview of a value already set = %+v; want no change, no restart, no rollback", res)
	}
}
