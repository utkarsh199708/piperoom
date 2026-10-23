package setupactions

import (
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/setup/catalog"
	"piperoom/internal/setup/plan"
	"piperoom/internal/tools"
)

// provisional is the pipeline a setup room is scoped to before registration.
func provisional(t *testing.T) owners.Pipeline {
	t.Helper()
	p, err := owners.Provisional(events.SetupRequest{
		PipelineID: "orders", Title: "orders cdc", SourceKind: "postgres", Target: "compose", TopicPrefix: "orders",
		Tables: []string{"public.orders", "public.piperoom_canary"},
		Targets: []events.SetupTarget{
			{Kind: "postgres", Address: "db.acme:5432", Class: "shared"},
			{Kind: "connect", Address: "http://localhost:8083", Class: "local"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCheckScope(t *testing.T) {
	p := provisional(t)
	scope := func(op, params string) error {
		return adapterFor(t, Config{}, op).(actions.Scoped).CheckScope(p, json.RawMessage(params))
	}
	for _, tc := range []struct {
		op, params string
		ok         bool
	}{
		{"source_create_publication", `{"publication":"dbz","tables":["public.orders"]}`, true},
		{"source_create_publication", `{"publication":"dbz","tables":["public.orders","public.salaries"]}`, false},
		{"source_create_canary_table", `{"table":"public.piperoom_canary"}`, true},
		{"source_create_canary_table", `{"table":"public.other_canary"}`, false},
		{"source_insert_canary", `{"table":"public.piperoom_canary","nonce":"00112233445566778899aabbccddeeff"}`, true},
		{"source_create_read_role", `{"role":"piperoom_read","tables":["billing.invoices"]}`, false},
		{"source_create_publication", `{"publication":"dbz","tables":"public.salaries"}`, false},
		{"source_create_publication", `{"publication":"dbz","tables":[7]}`, false},
		{"source_create_publication", `{"publication":"dbz","tables":["public.orders"],"Tables":["public.salaries"]}`, false},
		{"source_create_publication", `{`, false},
		// Ops that name nothing of the pipeline: the runner confines them.
		{"file_write", `{"path":"notes.txt","content":"x"}`, true},
		{"compose_up", `{"services":["connect"]}`, true},
	} {
		err := scope(tc.op, tc.params)
		if tc.ok && err != nil {
			t.Errorf("%s %s: %v", tc.op, tc.params, err)
		}
		if !tc.ok && !errors.Is(err, tools.ErrOutOfScope) {
			t.Errorf("%s %s: err = %v, want ErrOutOfScope", tc.op, tc.params, err)
		}
	}

	// No pipeline at all, and a source step on a pipeline with no source.
	a := adapterFor(t, Config{}, "file_write").(actions.Scoped)
	if err := a.CheckScope(owners.Pipeline{}, json.RawMessage(`{"path":"a","content":"x"}`)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Errorf("no pipeline: %v", err)
	}
	batch := p
	batch.Kind = events.PipelineKindBatch
	src := adapterFor(t, Config{}, "source_create_canary_table").(actions.Scoped)
	if err := src.CheckScope(batch, json.RawMessage(`{"table":"public.piperoom_canary"}`)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Errorf("batch pipeline: %v", err)
	}
}

// A spec that names a host is scoped to the pipeline's hosts: the provisional
// pipeline's connections are the targets the person named.
func TestCheckScopeHosts(t *testing.T) {
	spec := catalog.Spec{Name: "fake_probe_write", Tier: plan.TierT1, Target: "connect", Layer: "kafka-connect",
		Via: catalog.ViaSetup, HostKeys: []string{"target"},
		Schema: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"target":{"type":"string"}},"required":["target"]}`)}
	p := provisional(t)
	for _, tc := range []struct {
		target string
		ok     bool
	}{
		{"db.acme:5432", true},
		{"DB.ACME", true},
		{"http://localhost:8083/connectors", true},
		{"localhost:9999", true}, // same host, another port: the runner checks the port against its targets
		{"db.other:5432", false},
		{"169.254.169.254", false},
		{"http://user@evil.example/", false},
		{"", false},
	} {
		raw, _ := json.Marshal(map[string]string{"target": tc.target})
		err := checkScope(spec, p, raw)
		if tc.ok && err != nil {
			t.Errorf("%q: %v", tc.target, err)
		}
		if !tc.ok && !errors.Is(err, tools.ErrOutOfScope) {
			t.Errorf("%q: err = %v, want ErrOutOfScope", tc.target, err)
		}
	}
	// A connection given by reference names no host CheckScope can read.
	ref := p
	ref.Connections = owners.Connections{"source_db_url": {Env: "SRC_URL"}}
	if err := checkScope(spec, ref, json.RawMessage(`{"target":"db.acme:5432"}`)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Errorf("env reference: %v", err)
	}
}
