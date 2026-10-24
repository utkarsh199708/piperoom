package setuptools_test

// Sink registration through Q5c over the remote clients (row U3b): the config
// SinkConfig builds is registered by the unchanged connect_create_connector
// action, constructed over the setup runner's remote Connect client, and
// reaches the runner as one T2 op carrying the frozen step's params. This test
// is external so it may import internal/room (for the step context) without
// the package itself ever importing it.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/setup/knowledge"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/remoteclients"
	setuptools "piperoom/internal/setup/tools"
	"piperoom/internal/tools"
)

const sinkClass = "io.debezium.connector.jdbc.JdbcSinkConnector"

// connectRunner answers the Connect ops the create action sends.
type connectRunner struct {
	mu    sync.Mutex
	calls []protocol.OpRequest
}

func (r *connectRunner) Execute(ctx context.Context, _ string, req protocol.OpRequest) (protocol.OpResult, error) {
	r.mu.Lock()
	r.calls = append(r.calls, req)
	r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return protocol.OpResult{}, err
	}
	answers := map[string]string{
		remoteclients.OpConnectListConnectors: `{"connectors":["pg-orders-connector"]}`,
		remoteclients.OpConnectConfig: `{"config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector",` +
			`"database.hostname":"postgres","database.user":"debezium","topic.prefix":"pg"}}`,
		remoteclients.OpConnectStatus: `{"status":{"name":"pg-orders-connector","type":"source","state":"RUNNING",` +
			`"worker_id":"connect:8083","tasks":[]}}`,
		remoteclients.OpConnectValidateConfig:  `{"validate":{"error_count":0,"errors":{}}}`,
		remoteclients.OpConnectCreateConnector: `{}`,
	}
	data, ok := answers[req.Op]
	if !ok {
		return protocol.OpResult{}, fmt.Errorf("unexpected op %s", req.Op)
	}
	return protocol.OpResult{ID: req.ID, OK: true, Summary: "ok", Data: json.RawMessage(data)}, nil
}

func (r *connectRunner) sent(op string) []protocol.OpRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []protocol.OpRequest
	for _, c := range r.calls {
		if c.Op == op {
			out = append(out, c)
		}
	}
	return out
}

func TestSinkRegistrationThroughQ5c(t *testing.T) {
	ix, err := knowledge.LoadIndex()
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	b, err := setuptools.NewConfigBuilder(ix, setuptools.Versions{"debezium": "3.2.0.Final"})
	if err != nil {
		t.Fatalf("NewConfigBuilder: %v", err)
	}
	cfg, err := b.SinkConfig(setuptools.SinkOptions{
		Sink:   "debezium-jdbc-sink",
		Topics: []string{"pg.public.orders"},
		Config: map[string]string{
			"connection.url":      "jdbc:postgresql://postgres:5432/orders",
			"connection.username": "sink_writer",
			"connection.password": "${secret:sink_password}",
			"schema.evolution":    "basic",
		},
		Transforms: []setuptools.Transform{{Alias: "route", Class: "jdbc-collection-name-transformation"}},
	})
	if err != nil {
		t.Fatalf("SinkConfig: %v", err)
	}
	params, err := setuptools.SinkRegistrationParams("pg-orders-sink", cfg, false)
	if err != nil {
		t.Fatalf("SinkRegistrationParams: %v", err)
	}

	r := &connectRunner{}
	s := remoteclients.New(r, "r_setup")
	reg := actions.NewRegistry()
	if err := actions.RegisterLifecycle(reg, actions.LifecycleDeps{Connect: s.Connect()}); err != nil {
		t.Fatalf("RegisterLifecycle: %v", err)
	}
	act, ok := reg.Get(setuptools.SinkRegistrationAction)
	if !ok {
		t.Fatal("connect_create_connector did not register")
	}

	p, err := owners.Provisional(events.SetupRequest{
		PipelineID:  "orders",
		Title:       "Orders CDC",
		TopicPrefix: "pg",
		Tables:      []string{"public.orders"},
		Connectors:  []string{"pg-orders-connector", "pg-orders-sink"},
		Targets: []events.SetupTarget{
			{Kind: "postgres", Address: "postgres:5432"},
			{Kind: "connect", Address: "connect:8083"},
		},
	})
	if err != nil {
		t.Fatalf("owners.Provisional: %v", err)
	}
	if err := act.(actions.Scoped).CheckScope(p, params); err != nil {
		t.Fatalf("CheckScope: %v", err)
	}
	ctx := tools.WithPipeline(context.Background(), p)
	ctx = room.WithRoomID(ctx, "r_setup")
	ctx = room.WithSetupStep(ctx, room.SetupStep{
		ProposalID: "p_1",
		StepIndex:  0,
		ParamsHash: "b1946ac92492d2347c6235b4d2611184",
		Tier:       protocol.TierT2,
		Class:      protocol.ClassLocal,
		Action:     setuptools.SinkRegistrationAction,
		Params:     params,
	})
	ctx = remoteclients.WithProposal(ctx, json.RawMessage(`{"room_id":"r_setup","proposal_id":"p_1","title":"sink","steps":[]}`))

	if err := act.Validate(ctx, params); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	prev, err := act.(actions.Preview).Preview(ctx, params)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(prev.Current, "not registered") {
		t.Fatalf("preview current = %q", prev.Current)
	}
	if _, err := act.Execute(ctx, params); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	creates := r.sent(remoteclients.OpConnectCreateConnector)
	if len(creates) != 1 {
		t.Fatalf("create sent %d times, want 1", len(creates))
	}
	req := creates[0]
	if req.Tier != protocol.TierT2 || req.ProposalID != "p_1" || req.StepIndex == nil || len(req.Proposal) == 0 {
		t.Fatalf("create is not the proposal step's T2 write: %+v", req)
	}
	if string(req.Params) != string(params) {
		t.Fatalf("create params = %s, want the frozen step's %s", req.Params, params)
	}
	validates := r.sent(remoteclients.OpConnectValidateConfig)
	if len(validates) == 0 {
		t.Fatal("the create never asked Connect to validate the sink config")
	}
	var vp struct {
		Class string `json:"class"`
	}
	if err := json.Unmarshal(validates[0].Params, &vp); err != nil || vp.Class != sinkClass {
		t.Fatalf("validate params = %s (%v)", validates[0].Params, err)
	}
}
