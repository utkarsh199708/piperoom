package actions

import (
	"encoding/json"
	"testing"

	"piperoom/internal/owners"
)

func testPipeline() owners.Pipeline {
	return owners.Pipeline{
		ID:           "pg-orders",
		TopicPrefix:  "pg",
		Connectors:   []string{"pg-orders-connector"},
		SourceTables: []string{"public.orders"},
	}
}

func TestCheckScope(t *testing.T) {
	p := testPipeline()
	reg := NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		action   string
		pipeline owners.Pipeline
		params   string
		wantErr  bool
	}{
		{name: "restart task in scope", action: "connect_restart_task", pipeline: p, params: `{"connector":"pg-orders-connector","task":0}`},
		{name: "restart task other connector", action: "connect_restart_task", pipeline: p, params: `{"connector":"pg-inventory-connector","task":0}`, wantErr: true},
		{name: "restart connector in scope", action: "connect_restart_connector", pipeline: p, params: `{"connector":"pg-orders-connector"}`},
		{name: "restart connector other pipeline", action: "connect_restart_connector", pipeline: p, params: `{"connector":"pg-inventory-connector"}`, wantErr: true},
		{name: "set compatibility value subject in scope", action: "schema_set_compatibility", pipeline: p, params: `{"subject":"pg.public.orders-value","level":"FULL"}`},
		{name: "set compatibility key subject in scope", action: "schema_set_compatibility", pipeline: p, params: `{"subject":"pg.public.orders-key","level":"NONE"}`},
		{name: "set compatibility other subject", action: "schema_set_compatibility", pipeline: p, params: `{"subject":"pg.public.customers-value","level":"NONE"}`, wantErr: true},
		{name: "set compatibility foreign prefix", action: "schema_set_compatibility", pipeline: p, params: `{"subject":"billing.public.orders-value","level":"NONE"}`, wantErr: true},
		{name: "set compatibility missing prefix", action: "schema_set_compatibility", pipeline: p, params: `{"subject":"public.orders-value","level":"NONE"}`, wantErr: true},
		{name: "set compatibility empty subject", action: "schema_set_compatibility", pipeline: p, params: `{"subject":"","level":"NONE"}`, wantErr: true},
		{name: "register schema in scope", action: "schema_register", pipeline: p, params: `{"subject":"pg.public.orders-value","schema":"{\"type\":\"string\"}"}`},
		{name: "register schema other subject", action: "schema_register", pipeline: p, params: `{"subject":"pg.public.customers-value","schema":"{\"type\":\"string\"}"}`, wantErr: true},
		{name: "subject without key or value suffix", action: "schema_set_compatibility", pipeline: p, params: `{"subject":"pg.public.orders","level":"NONE"}`, wantErr: true},
		{name: "malformed params", action: "connect_restart_task", pipeline: p, params: `{`, wantErr: true},
		{name: "malformed params subject action", action: "schema_set_compatibility", pipeline: p, params: `{`, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, ok := reg.Get(tc.action)
			if !ok {
				t.Fatalf("action %q not registered", tc.action)
			}
			scoped, ok := a.(Scoped)
			if !ok {
				t.Fatalf("action %q does not implement Scoped", tc.action)
			}
			err := scoped.CheckScope(tc.pipeline, json.RawMessage(tc.params))
			if tc.wantErr && err == nil {
				t.Fatal("want error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("want nil, got %v", err)
			}
		})
	}
}

func TestRegisterSchemaValidateRejectsMissingSubject(t *testing.T) {
	reg := NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("schema_register")
	// Valid Avro, but the subject does not exist in the fake registry.
	if err := a.Validate(json.RawMessage(`{"subject":"pg.public.nope-value","schema":"{\"type\":\"string\"}"}`)); err == nil {
		t.Fatal("want error for non-existent subject")
	}
}

func TestRegisterSchemaValidateAcceptsExistingSubject(t *testing.T) {
	reg := NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("schema_register")
	if err := a.Validate(json.RawMessage(`{"subject":"pg.public.orders-value","schema":"{\"type\":\"string\"}"}`)); err != nil {
		t.Fatalf("want nil, got %v", err)
	}
}
