package wiring_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/setup/protocol"
	setuptools "piperoom/internal/setup/tools"
	"piperoom/internal/tools"
	"piperoom/internal/wiring"
	"piperoom/internal/wiring/setupwire"
)

// TestSetupStackWriteModes extends the P1a-3 test (wiring's
// TestForgeWiringWriteModes): with write mode off no setup stack is built,
// and no setup action and no remote writer is constructed — the client
// constructor and the action hook are never called, so nothing that can
// reach the runner exists (design §3.8).
func TestSetupStackWriteModes(t *testing.T) {
	req := setupRequest(t)
	ex := &countingExecutor{}

	for name, tc := range map[string]struct {
		cfg         config.Config
		wantErr     string
		wantClients bool
	}{
		"write mode off": {
			cfg:     config.Config{SetupEnabled: "on", WriteMode: "off", SecretsDir: t.TempDir()},
			wantErr: "PIPEROOM_WRITE_MODE is off",
		},
		"setup off": {
			cfg:     config.Config{SetupEnabled: "off", WriteMode: "on"},
			wantErr: "PIPEROOM_SETUP_ENABLED is off",
		},
		"setup on, write on": {
			cfg:         config.Config{SetupEnabled: "on", WriteMode: "on", SecretsDir: t.TempDir()},
			wantClients: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var clientCalls, actionCalls int
			d := setupDeps(t, req, ex,
				func(ex protocol.RunnerExecutor, roomID string) (wiring.SetupClients, error) {
					clientCalls++
					return setupwire.Clients(ex, roomID)
				},
				func(areg *actions.Registry, ex protocol.RunnerExecutor, roomID string) error {
					actionCalls++
					return setupwire.RegisterActions(areg, ex, roomID)
				})
			st, err := wiring.OpenSetupStack(context.Background(), tc.cfg, d)
			if tc.wantErr != "" {
				if err == nil || !contains(tc.wantErr, err.Error()) {
					t.Fatalf("err = %v, want one naming %q", err, tc.wantErr)
				}
				if clientCalls != 0 || actionCalls != 0 {
					t.Fatalf("write-mode-off built clients %d times and setup actions %d times: no remote writer and no setup action may exist", clientCalls, actionCalls)
				}
				if ex.calls() != 0 {
					t.Fatalf("the runner was reached %d times during a refused construction", ex.calls())
				}
				return
			}
			if err != nil {
				t.Fatalf("OpenSetupStack: %v", err)
			}
			if clientCalls != 1 || actionCalls != 1 {
				t.Fatalf("clients built %d times, setup actions registered %d times; want once each", clientCalls, actionCalls)
			}
			if ex.calls() != 0 {
				t.Fatalf("the runner was reached %d times during construction; want none", ex.calls())
			}
			if !st.Tools.ReadOnly() {
				t.Fatal("the setup stack's tools are not from tools.NewReadOnlyRegistry")
			}
			// The Q read tools over the remote clients (design §3.2: Q5c,
			// Q2c, Q2d, Q4a, Q6a keep their names) and the setup read tools.
			for _, name := range []string{"connect_validate_config", "pg_replication_slots", "source_table_replication"} {
				if _, ok := st.Tools.Get(name); !ok {
					t.Errorf("%s did not register in the setup stack's tools", name)
				}
			}
			for _, name := range setuptools.ReadToolNames() {
				if _, ok := st.Tools.Get(name); !ok {
					t.Errorf("setup read tool %s did not register", name)
				}
			}
			// The Q actions over the remote clients and the setup-only
			// actions.
			for _, name := range []string{"connect_create_connector", "source_grant_select", "pg_set_replica_identity", "compose_write", "compose_up"} {
				if _, ok := st.Actions.Get(name); !ok {
					t.Errorf("%s did not register in the setup stack's actions", name)
				}
			}
		})
	}
}

// TestSetupStackRefusesBadDeps pins the construction-time refusals: each one
// fails before Clients or SetupActions run, so a miswired server refuses at
// the first room instead of building a stack that half works.
func TestSetupStackRefusesBadDeps(t *testing.T) {
	cfg := config.Config{SetupEnabled: "on", WriteMode: "on", SecretsDir: t.TempDir()}
	good := setupDeps(t, setupRequest(t), &countingExecutor{}, nil, nil)
	good.Clients = func(ex protocol.RunnerExecutor, roomID string) (wiring.SetupClients, error) {
		return setupwire.Clients(ex, roomID)
	}
	good.SetupActions = func(areg *actions.Registry, ex protocol.RunnerExecutor, roomID string) error {
		return setupwire.RegisterActions(areg, ex, roomID)
	}

	cases := []struct {
		name string
		mod  func(*wiring.SetupStackDeps)
		want string
	}{
		{"nil executor", func(d *wiring.SetupStackDeps) { d.Executor = nil }, "executor is required"},
		{"empty room", func(d *wiring.SetupStackDeps) { d.RoomID = " " }, "room id is required"},
		{"nil CallRoom", func(d *wiring.SetupStackDeps) { d.CallRoom = nil }, "CallRoom is required"},
		{"nil Clients", func(d *wiring.SetupStackDeps) { d.Clients = nil }, "Clients is required"},
		{"bad class", func(d *wiring.SetupStackDeps) { d.Class = "prod" }, "not local, shared or production"},
		{"request without pipeline id", func(d *wiring.SetupStackDeps) { d.Request.PipelineID = "" }, "pipeline_id is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := good
			tc.mod(&d)
			_, err := wiring.OpenSetupStack(context.Background(), cfg, d)
			if err == nil || !contains(tc.want, err.Error()) {
				t.Fatalf("err = %v, want one naming %q", err, tc.want)
			}
		})
	}
}

// setupDeps are OpenSetupStack's collaborators with the hooks a test may spy
// on; a nil hook is left nil so the refusal tests exercise the required-hook
// path.
func setupDeps(t *testing.T, req events.SetupRequest, ex protocol.RunnerExecutor,
	clients func(protocol.RunnerExecutor, string) (wiring.SetupClients, error),
	setupActions func(*actions.Registry, protocol.RunnerExecutor, string) error) wiring.SetupStackDeps {
	t.Helper()
	return wiring.SetupStackDeps{
		Executor:     ex,
		RoomID:       "r_setup",
		Request:      req,
		CallRoom:     func(context.Context) string { return "r_setup" },
		Clients:      clients,
		SetupActions: setupActions,
	}
}

func setupRequest(t *testing.T) events.SetupRequest {
	t.Helper()
	return events.SetupRequest{
		PipelineID:  "orders-setup",
		Tenant:      "",
		Title:       "orders pipeline",
		SourceKind:  events.SourceKindPostgres,
		Target:      "db.acme:5432",
		TopicPrefix: "pg",
		Tables:      []string{"public.orders"},
		Connectors:  []string{"pg-orders-connector"},
		Targets: []events.SetupTarget{
			{Kind: "postgres", Address: "db.acme:5432", Class: protocol.ClassLocal},
			{Kind: "connect", Address: "connect.acme:8083", Class: protocol.ClassLocal},
		},
	}
}

// countingExecutor records every op the stack construction (or a test)
// reaches; construction must reach none.
type countingExecutor struct {
	n int
}

func (c *countingExecutor) Execute(context.Context, string, protocol.OpRequest) (protocol.OpResult, error) {
	c.n++
	return protocol.OpResult{}, errors.New("countingExecutor: no answers scripted")
}

func (c *countingExecutor) calls() int { return c.n }

func contains(sub, s string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestSetupStackPipelineOption (row U1-32): the caller may pass the room's
// provisional pipeline as it stands, and the stack's owners scope is built
// from it — a consumer group a driver added after the request is in scope for
// the stack's tools only after a rebuild with that pipeline. Without the
// option the stack derives the pipeline from the request alone, which cannot
// carry consumer groups, so the same call is refused as out of scope.
func TestSetupStackPipelineOption(t *testing.T) {
	cfg := config.Config{SetupEnabled: "on", WriteMode: "on", SecretsDir: t.TempDir()}
	req := setupRequest(t)
	// The call resolves the pipeline by id against the stack's owners, the
	// path a caller without a full pipeline in its context takes.
	callCtx := tools.WithPipelineID(context.Background(), req.PipelineID)
	input := json.RawMessage(`{"group":"orders-cg"}`)

	build := func(p *owners.Pipeline, ex protocol.RunnerExecutor) *wiring.SetupStack {
		t.Helper()
		d := setupDeps(t, req, ex,
			func(ex protocol.RunnerExecutor, roomID string) (wiring.SetupClients, error) {
				return setupwire.Clients(ex, roomID)
			},
			func(areg *actions.Registry, ex protocol.RunnerExecutor, roomID string) error {
				return setupwire.RegisterActions(areg, ex, roomID)
			})
		d.Pipeline = p
		st, err := wiring.OpenSetupStack(context.Background(), cfg, d)
		if err != nil {
			t.Fatalf("OpenSetupStack: %v", err)
		}
		return st
	}

	// Before: the request-derived pipeline has no consumer groups, so the
	// group is out of scope and nothing reaches the runner.
	exBefore := &countingExecutor{}
	st := build(nil, exBefore)
	tool, ok := st.Tools.Get("kafka_consumer_lag")
	if !ok {
		t.Fatal("kafka_consumer_lag is not in the stack's tools")
	}
	if _, err := tool.Run(callCtx, input); err == nil || !contains("consumer group", err.Error()) {
		t.Fatalf("err = %v, want a consumer-group scope refusal", err)
	}
	if exBefore.calls() != 0 {
		t.Fatalf("an out-of-scope read reached the runner %d times", exBefore.calls())
	}

	// After a rebuild with the room's pipeline: the group is in scope, so the
	// read passes the scope check and reaches the remote client (whose fake
	// runner answers nothing, which is fine — the refusal now comes from the
	// client, not the scope).
	prov, err := owners.Provisional(req)
	if err != nil {
		t.Fatalf("Provisional: %v", err)
	}
	prov.ConsumerGroups = []string{"orders-cg"}
	exAfter := &countingExecutor{}
	st = build(&prov, exAfter)
	tool, _ = st.Tools.Get("kafka_consumer_lag")
	if _, err := tool.Run(callCtx, input); err == nil || contains("consumer group", err.Error()) {
		t.Fatalf("err = %v, want a client error, not a scope refusal", err)
	}
	if exAfter.calls() == 0 {
		t.Fatal("an in-scope read never reached the runner")
	}
}
