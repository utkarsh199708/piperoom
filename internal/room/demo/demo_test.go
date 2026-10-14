package demo

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/topology"
)

func TestBuilderBuildIsConsistentWithLayerOwner(t *testing.T) {
	// The fixture graph and the layer-to-owner map both claim to follow
	// dev/owners.yaml. If they disagree the demo shows two owners for one layer.
	var b Builder
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if g.PipelineID != "orders" {
		t.Fatalf("PipelineID = %q, want orders", g.PipelineID)
	}
	ids := map[string]bool{}
	for _, n := range g.Nodes {
		ids[n.ID] = true
		owner, ok := b.LayerOwner("orders", n.Layer)
		if !ok {
			t.Errorf("node %s: layer %q has no owner", n.ID, n.Layer)
			continue
		}
		if owner != n.Owner {
			t.Errorf("node %s: owner %q, LayerOwner says %q", n.ID, n.Owner, owner)
		}
	}
	for _, e := range g.Edges {
		if !ids[e.From] || !ids[e.To] {
			t.Errorf("edge %s -> %s points at a node that does not exist", e.From, e.To)
		}
	}
}

func TestBuilderBuildEchoesPipelineID(t *testing.T) {
	// Build is a fixture: whatever pipeline is asked for, it returns the orders
	// graph labelled with the requested id.
	var b Builder
	g, err := b.Build(context.Background(), "payments")
	if err != nil {
		t.Fatal(err)
	}
	if g.PipelineID != "payments" || len(g.Nodes) == 0 {
		t.Fatalf("got pipeline %q with %d nodes", g.PipelineID, len(g.Nodes))
	}
}

func TestBuilderRender(t *testing.T) {
	var b Builder
	tests := []struct {
		name string
		g    topology.Graph
		want string
	}{
		{
			name: "empty graph is only the header",
			g:    topology.Graph{PipelineID: "orders"},
			want: "pipeline orders\n",
		},
		{
			name: "one line per node in order",
			g: topology.Graph{PipelineID: "orders", Nodes: []topology.Node{
				{ID: "pg:public.orders", Kind: topology.KindSourceTable, Layer: "postgres", Health: topology.HealthWarn},
				{ID: "topic:pg.public.orders", Kind: topology.KindTopic, Layer: "kafka", Health: topology.HealthOK},
			}},
			want: "pipeline orders\n" +
				"- pg:public.orders (" + string(topology.KindSourceTable) + ", postgres, " + string(topology.HealthWarn) + ")\n" +
				"- topic:pg.public.orders (" + string(topology.KindTopic) + ", kafka, " + string(topology.HealthOK) + ")\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := b.Render(tt.g); got != tt.want {
				t.Fatalf("Render:\n got %q\nwant %q", got, tt.want)
			}
		})
	}

	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(b.Render(g), "\n"), "\n")
	if len(lines) != len(g.Nodes)+1 {
		t.Fatalf("fixture render has %d lines, want %d", len(lines), len(g.Nodes)+1)
	}
}

func TestBuilderTrace(t *testing.T) {
	var b Builder
	tests := []struct {
		name     string
		pipeline string
		table    string
		wantErr  bool
	}{
		{name: "orders source table", pipeline: "orders", table: "public.orders"},
		{name: "table outside the pipeline", pipeline: "orders", table: "public.payments", wantErr: true},
		{name: "unknown pipeline", pipeline: "nope", table: "public.orders", wantErr: true},
		{name: "empty table", pipeline: "orders", table: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages, err := b.Trace(context.Background(), tt.pipeline, tt.table, "10042")
			if tt.wantErr {
				if !errors.Is(err, topology.ErrBadInput) {
					t.Fatalf("err = %v, want topology.ErrBadInput", err)
				}
				if stages != nil {
					t.Fatalf("stages returned with an error: %+v", stages)
				}
				return
			}
			if err != nil {
				t.Fatalf("Trace: %v", err)
			}
			if len(stages) == 0 {
				t.Fatal("no stages")
			}
			last := stages[len(stages)-1]
			if last.Layer != "warehouse" || last.Status != "stopped_here" {
				t.Fatalf("last stage = %+v, want warehouse stopped_here (the demo story ends there)", last)
			}
		})
	}
}

func TestBuilderTraceFixtureExact(t *testing.T) {
	// Every fact is pinned: a changed offset or row id in the fixture is a
	// change to the demo story and must be a deliberate edit of this test.
	var b Builder
	got, err := b.Trace(context.Background(), "orders", "public.orders", "10042")
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	want := []events.TraceStage{
		{Layer: "postgres", Name: "public.orders", Status: "seen", Facts: map[string]string{"row": "id=10042"}},
		{Layer: "kafka-connect", Name: "pg-orders-connector", Status: "seen", Facts: map[string]string{"task": "0"}},
		{Layer: "schema-registry", Name: "pg.public.orders-value", Status: "seen"},
		{Layer: "kafka", Name: "pg.public.orders", Status: "seen", Facts: map[string]string{"partition": "0", "offset": "48213"}},
		{Layer: "consumer", Name: "order-sync", Status: "not_seen"},
		{Layer: "warehouse", Name: "analytics.orders", Status: "stopped_here", Facts: map[string]string{"reason": "no row for id=10042"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Trace fixture changed:\n got %+v\nwant %+v", got, want)
	}
}

func TestBuilderLayerOwner(t *testing.T) {
	var b Builder
	tests := []struct {
		layer     string
		wantOwner string
		wantOK    bool
	}{
		{layer: "postgres", wantOwner: "priya", wantOK: true},
		{layer: "schema-registry", wantOwner: "priya", wantOK: true},
		{layer: "kafka-connect", wantOwner: "arjun", wantOK: true},
		{layer: "kafka", wantOwner: "arjun", wantOK: true},
		{layer: "consumer", wantOwner: "kabir", wantOK: true},
		{layer: "warehouse", wantOwner: "meera", wantOK: true},
		{layer: "unknown-layer"},
		{layer: ""},
		{layer: "Postgres"}, // layers are case sensitive
	}
	for _, tt := range tests {
		owner, ok := b.LayerOwner("orders", tt.layer)
		if owner != tt.wantOwner || ok != tt.wantOK {
			t.Errorf("LayerOwner(%q) = (%q, %v), want (%q, %v)", tt.layer, owner, ok, tt.wantOwner, tt.wantOK)
		}
	}
}

func TestWriteHandoffReturnsCompleteFixture(t *testing.T) {
	s, err := WriteHandoff(context.Background(), nil, agent.Options{})
	if err != nil {
		t.Fatalf("WriteHandoff: %v", err)
	}
	if s.WhatBroke == "" || s.Why == "" {
		t.Fatalf("hand-off lacks what-broke or why: %+v", s)
	}
	if len(s.Done) == 0 || len(s.RuledOut) == 0 || len(s.Open) == 0 || len(s.Risks) == 0 {
		t.Fatalf("hand-off has an empty section: %+v", s)
	}
}

func TestMustJSON(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{name: "map", in: map[string]any{"task": 0}, want: `{"task":0}`},
		{name: "nil", in: nil, want: `null`},
		// A channel cannot be marshalled. The fallback must be valid JSON so
		// the event still carries a payload instead of failing the run.
		{name: "unmarshallable falls back to empty object", in: make(chan int), want: `{}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mustJSON(tt.in)
			if string(got) != tt.want {
				t.Fatalf("mustJSON = %s, want %s", got, tt.want)
			}
			if !json.Valid(got) {
				t.Fatalf("mustJSON produced invalid JSON: %s", got)
			}
		})
	}
}

func TestFakeConnectStatusAndConfig(t *testing.T) {
	ctx := context.Background()
	f := NewFakeConnect()

	names, err := f.ListConnectors(ctx)
	if err != nil || len(names) != 1 || names[0] != connectorName {
		t.Fatalf("ListConnectors = %v, %v", names, err)
	}

	st, err := f.Status(ctx, connectorName)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.State != "RUNNING" || len(st.Tasks) != 1 || st.Tasks[0].State != "FAILED" {
		t.Fatalf("demo story must start with task 0 FAILED on a RUNNING connector, got %+v", st)
	}

	cfg, err := f.Config(ctx, connectorName)
	if err != nil || cfg["topic.prefix"] != "pg" {
		t.Fatalf("Config = %v, %v", cfg, err)
	}

	for _, call := range []struct {
		name string
		fn   func() error
	}{
		{"Status", func() error { _, err := f.Status(ctx, "other"); return err }},
		{"Config", func() error { _, err := f.Config(ctx, "other"); return err }},
		{"RestartConnector", func() error { return f.RestartConnector(ctx, "other") }},
		{"RestartTask", func() error { return f.RestartTask(ctx, "other", 0) }},
		{"Resume", func() error { return f.Resume(ctx, "other") }},
	} {
		if err := call.fn(); !errors.Is(err, clients.ErrNotFound) {
			t.Errorf("%s on unknown connector: err = %v, want clients.ErrNotFound", call.name, err)
		}
	}
}

func TestFakeConnectRestartPaths(t *testing.T) {
	ctx := context.Background()

	f := NewFakeConnect()
	if err := f.RestartTask(ctx, connectorName, 1); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("RestartTask(task 1): err = %v, want ErrNotFound", err)
	}
	st, _ := f.Status(ctx, connectorName)
	if st.Tasks[0].State != "FAILED" {
		t.Fatal("a refused restart must not change task 0")
	}

	f = NewFakeConnect()
	if err := f.RestartConnector(ctx, connectorName); err != nil {
		t.Fatalf("RestartConnector: %v", err)
	}
	st, _ = f.Status(ctx, connectorName)
	if st.Tasks[0].State != "RUNNING" || st.State != "RUNNING" {
		t.Fatalf("after RestartConnector: connector %s, task %s; want both RUNNING", st.State, st.Tasks[0].State)
	}
}

func TestFakeRegistryReads(t *testing.T) {
	ctx := context.Background()
	f := NewFakeRegistry()

	subjects, err := f.Subjects(ctx)
	if err != nil || len(subjects) != 1 || subjects[0] != subjectName {
		t.Fatalf("Subjects = %v, %v", subjects, err)
	}

	versions, err := f.Versions(ctx, subjectName)
	if err != nil || len(versions) != 1 || versions[0] != 1 {
		t.Fatalf("Versions = %v, %v", versions, err)
	}
	if _, err := f.Versions(ctx, "unknown-value"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Versions(unknown): err = %v, want ErrNotFound", err)
	}

	tests := []struct {
		name    string
		subject string
		version int
		wantErr bool
	}{
		{name: "version 1", subject: subjectName, version: 1},
		{name: "latest alias -1", subject: subjectName, version: -1},
		{name: "version 2 does not exist", subject: subjectName, version: 2, wantErr: true},
		{name: "version 0 does not exist", subject: subjectName, version: 0, wantErr: true},
		{name: "unknown subject", subject: "unknown-value", version: 1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sv, err := f.Schema(ctx, tt.subject, tt.version)
			if tt.wantErr {
				if !errors.Is(err, clients.ErrNotFound) {
					t.Fatalf("err = %v, want ErrNotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Schema: %v", err)
			}
			if sv.Subject != tt.subject || sv.Version != 1 || sv.ID != 1 || !json.Valid([]byte(sv.Schema)) {
				t.Fatalf("Schema = %+v", sv)
			}
		})
	}
}

func TestFakeRegistryCompatibilityWrites(t *testing.T) {
	ctx := context.Background()
	f := NewFakeRegistry()

	level, ok, err := f.Compatibility(ctx, subjectName)
	if err != nil || !ok || level != "BACKWARD" {
		t.Fatalf("Compatibility = %q, %v, %v; want BACKWARD", level, ok, err)
	}

	if _, _, err := f.Compatibility(ctx, "unknown-value"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Compatibility(unknown): err = %v, want ErrNotFound", err)
	}
	if err := f.SetCompatibility(ctx, "unknown-value", "NONE"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("SetCompatibility(unknown): err = %v, want ErrNotFound", err)
	}
	if _, ok := f.level("unknown-value"); ok {
		t.Fatal("SetCompatibility created a subject it should have refused")
	}

	if err := f.SetCompatibility(ctx, subjectName, "NONE"); err != nil {
		t.Fatalf("SetCompatibility: %v", err)
	}
	level, _, _ = f.Compatibility(ctx, subjectName)
	if level != "NONE" {
		t.Fatalf("after SetCompatibility, level = %q, want NONE", level)
	}
}

func TestFakeRegistryRegister(t *testing.T) {
	ctx := context.Background()
	f := NewFakeRegistry()
	id, err := f.Register(ctx, subjectName, `{"type":"record","name":"orders","fields":[]}`)
	if err != nil || id != 2 {
		t.Fatalf("Register known subject = %d, %v; want 2, nil", id, err)
	}
	id, err = f.Register(ctx, "unknown-value", `{}`)
	if !errors.Is(err, clients.ErrNotFound) || id != 0 {
		t.Fatalf("Register unknown subject = %d, %v; want 0, ErrNotFound", id, err)
	}
}

func TestFakeActionDepsAreWired(t *testing.T) {
	d := FakeActionDeps()
	if d.Connect == nil || d.Registry == nil {
		t.Fatalf("FakeActionDeps left a client nil: %+v", d)
	}
}

// recordingSink keeps every emitted event; it is safe for concurrent use.
type recordingSink struct {
	mu    sync.Mutex
	types []events.Type
	pays  []any
}

func (s *recordingSink) Emit(_ context.Context, typ events.Type, payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.types = append(s.types, typ)
	s.pays = append(s.pays, payload)
	return nil
}

func TestRunStopsWhenContextIsCancelled(t *testing.T) {
	// A cancelled room must end the scripted agent at its first pause without
	// creating a proposal, and it must return nil (a cancel is not a failure).
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sink := &recordingSink{}

	err := Run(ctx, agent.Alert{}, agent.Deps{Sink: sink}, agent.Options{})
	if err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
	if len(sink.types) != 1 || sink.types[0] != events.AgentStatus {
		t.Fatalf("emitted %v, want exactly one agent.status before the first pause", sink.types)
	}
	p, ok := sink.pays[0].(events.AgentStatusPayload)
	if !ok || p.State != "working" {
		t.Fatalf("first status payload = %#v, want state working", sink.pays[0])
	}
}

// failingProposals answers every Create with the error the room service gives
// for a pipeline that is not configured.
type failingProposals struct{}

func (failingProposals) Create(context.Context, events.ProposalCreatedPayload) (string, error) {
	return "", &room.Error{Status: 400, Code: "validation", Msg: `pipeline "payments" is unknown; proposals are disabled for this room`}
}

func TestDemoStopsWhenProposalsDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &recordingSink{}
	inbox := &closedInbox{}
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, agent.Alert{}, agent.Deps{Sink: sink, Proposals: failingProposals{}, Inbox: inbox}, agent.Options{})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not end")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	hyps, idle := 0, false
	for i, typ := range sink.types {
		switch typ {
		case events.AgentHypothesis:
			hyps++
		case events.AgentStatus:
			if p, ok := sink.pays[i].(events.AgentStatusPayload); ok && p.State == "idle" && p.Detail == "proposals disabled" {
				idle = true
			}
			if p, ok := sink.pays[i].(events.AgentStatusPayload); ok && p.State == "error" {
				t.Fatalf("unexpected error status: %+v", p)
			}
		}
	}
	if hyps != 1 || !idle {
		t.Fatalf("hypotheses = %d, idle = %v; want 1, true", hyps, idle)
	}
}

// The demo's fake clients must let the real actions preview, so the scripted
// room shows a full preview card.
func TestDemoActionsPreview(t *testing.T) {
	reg := actions.NewRegistry()
	if err := actions.Register(reg, FakeActionDeps()); err != nil {
		t.Fatal(err)
	}
	cases := []struct{ action, params string }{
		{ActionConnectRestartTask, `{"connector":"pg-orders-connector","task":0}`},
		{ActionSchemaSetCompatibility, `{"subject":"pg.public.orders-value","level":"NONE"}`},
	}
	for _, c := range cases {
		a, ok := reg.Get(c.action)
		if !ok {
			t.Fatalf("action %s not registered", c.action)
		}
		pv, ok := a.(actions.Preview)
		if !ok {
			t.Fatalf("action %s has no Preview", c.action)
		}
		res, err := pv.Preview(context.Background(), json.RawMessage(c.params))
		if err != nil {
			t.Fatalf("%s: %v", c.action, err)
		}
		if res.Current == "" || res.Expected == "" {
			t.Fatalf("%s: preview is empty: %+v", c.action, res)
		}
	}
}
