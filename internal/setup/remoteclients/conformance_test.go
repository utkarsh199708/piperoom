package remoteclients

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/actions/sourceactions"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
	"piperoom/internal/tools/kafkatools"
)

// The tests in this file construct the four Q tools and Q actions the row's
// "done when" names over the remote clients, unchanged, and run the vectors
// their own packages test: connect_validate_config, connect_create_connector,
// pg_set_replica_identity and source_grant_select. They prove the remote shape
// satisfies the real interfaces (compile time by construction) and that each
// step reaches the runner as one op with the proposal fields.

const postgresConnectorClass = "io.debezium.connector.postgresql.PostgresConnector"

// The sourceactions readers are the Q2a methods with the postgres DTOs (the
// package aliases them), so the one Source client serves the Q2 actions and the
// Q2 tools directly, with no adapter.
var (
	_ sourceactions.TableReplicationReader = (*Source)(nil)
	_ sourceactions.RoleInfoReader         = (*Source)(nil)
	_ sourceactions.BlockerReader          = (*Source)(nil)
	_ sourceactions.ScopedBlockerReader    = (*Source)(nil)

	// Connect implements the tool's ConfigValidator as well as the client
	// contracts its own file asserts.
	_ kafkatools.ConfigValidator = (*Connect)(nil)
)

// TestConnectValidateConfigTool runs connect_validate_config over the remote
// Connect client: the plugin definitions, the pipeline connector's status and
// config (for the host guard) and the validation itself all reach the runner.
func TestConnectValidateConfigTool(t *testing.T) {
	f := newFakeRunner()
	s := New(f, "r_setup")
	ctx := roomCtx(t)

	reg := tools.NewReadOnlyRegistry()
	if err := kafkatools.Register(reg, kafkatools.Deps{Connect: s.Connect(), Kafka: s.Kafka()}); err != nil {
		t.Fatalf("kafkatools.Register: %v", err)
	}
	tool, ok := reg.Get("connect_validate_config")
	if !ok {
		t.Fatal("connect_validate_config did not register over the remote Connect client (no ConfigValidator?)")
	}

	input := json.RawMessage(`{
		"class": "` + postgresConnectorClass + `",
		"config": {
			"connector.class": "` + postgresConnectorClass + `",
			"database.hostname": "postgres",
			"database.password": "${secret:source_password}",
			"topic.prefix": "pg"
		}
	}`)
	res, err := tool.Run(ctx, input)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(res.Summary, "0 errors") {
		t.Fatalf("summary = %q, want the validate verdict", res.Summary)
	}

	calls := f.callsFor(OpConnectValidateConfig)
	if len(calls) != 1 {
		t.Fatalf("connect_validate_config op sent %d times, want 1", len(calls))
	}
	req := calls[0].req
	if req.Tier != protocol.TierT0 || req.ProposalID != "" || req.StepIndex != nil {
		t.Fatalf("the validate op is not a plain T0 read: %+v", req)
	}
	var params struct {
		Class  string            `json:"class"`
		Config map[string]string `json:"config"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		t.Fatalf("params: %v", err)
	}
	if params.Class != postgresConnectorClass {
		t.Fatalf("class = %q", params.Class)
	}
	if params.Config["database.password"] != "${secret:source_password}" {
		t.Fatalf("the config was not sent as the tool built it: %v", params.Config)
	}
	for _, op := range []string{OpConnectPluginConfig, OpConnectStatus, OpConnectConfig} {
		if got := len(f.callsFor(op)); got == 0 {
			t.Fatalf("the tool never reached the runner for %s", op)
		}
	}
	for _, c := range f.allCalls() {
		if c.req.Tier != protocol.TierT0 {
			t.Fatalf("%s is a read tool but sent tier %q", c.req.Op, c.req.Tier)
		}
	}
}

// TestConnectCreateConnectorAction runs connect_create_connector over the
// remote Connect client: the list, the config read-back for the host guard,
// the validate round trip and the create itself are one op each, and the
// create is a T2 request with the proposal fields.
func TestConnectCreateConnectorAction(t *testing.T) {
	f := newFakeRunner()
	f.overrides[OpConnectListConnectors] = json.RawMessage(`{"connectors":[]}`) // not registered yet
	s := New(f, "r_setup")

	reg := actions.NewRegistry()
	if err := actions.RegisterLifecycle(reg, actions.LifecycleDeps{Connect: s.Connect()}); err != nil {
		t.Fatalf("RegisterLifecycle: %v", err)
	}
	act, ok := reg.Get(actions.CreateConnectorName)
	if !ok {
		t.Fatal("connect_create_connector did not register")
	}
	ctx := stepCtxLocal(t)
	input := json.RawMessage(`{
		"connector": "pg-orders-connector",
		"config": {
			"connector.class": "` + postgresConnectorClass + `",
			"database.hostname": "postgres",
			"database.password": "${secret:source_password}",
			"topic.prefix": "pg"
		}
	}`)
	if err := act.Validate(ctx, input); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	prev, err := act.(actions.Preview).Preview(ctx, input)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(prev.Current, "not registered") {
		t.Fatalf("preview current = %q", prev.Current)
	}
	if _, err := act.Execute(ctx, input); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	calls := f.callsFor(OpConnectCreateConnector)
	if len(calls) != 1 {
		t.Fatalf("connect_create_connector op sent %d times, want 1", len(calls))
	}
	req := calls[0].req
	if req.Tier != protocol.TierT2 || req.Class != protocol.ClassLocal {
		t.Fatalf("create = tier %q class %q, want T2 local", req.Tier, req.Class)
	}
	if req.ProposalID != "p_1" || req.StepIndex == nil || *req.StepIndex != 0 || req.ParamsHash == "" {
		t.Fatalf("create is missing the proposal fields: %+v", req)
	}
	if len(req.Proposal) == 0 {
		t.Fatal("create did not carry the frozen proposal")
	}
	var params struct {
		Connector string            `json:"connector"`
		Config    map[string]string `json:"config"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		t.Fatalf("params: %v", err)
	}
	if params.Connector != "pg-orders-connector" || params.Config["topic.prefix"] != "pg" {
		t.Fatalf("create params = %+v", params)
	}
	if got := len(f.callsFor(OpConnectValidateConfig)); got == 0 {
		t.Fatal("the create never asked Connect to validate the config")
	}
}

// TestSetReplicaIdentityAction runs pg_set_replica_identity over the remote
// source clients (the action view for its Q2a DTOs, Source for the write).
func TestSetReplicaIdentityAction(t *testing.T) {
	f := newFakeRunner()
	s := New(f, "r_setup")
	src := s.Source()

	act := sourceactions.NewSetReplicaIdentity(sourceactions.ReplicaIdentityDeps{
		Source: src,
		Writer: src,
	})
	ctx := stepCtxLocal(t)
	input := json.RawMessage(`{"table":"public.orders","mode":"full"}`)
	if err := act.Validate(ctx, input); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	prev, err := act.(actions.Preview).Preview(ctx, input)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(prev.Expected, "REPLICA IDENTITY FULL") {
		t.Fatalf("preview expected = %q", prev.Expected)
	}
	out, err := act.Execute(ctx, input)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(out, "public.orders") {
		t.Fatalf("Execute output = %q", out)
	}

	calls := f.callsFor(OpSourceSetReplicaIdentity)
	if len(calls) != 1 {
		t.Fatalf("pg_set_replica_identity op sent %d times, want 1", len(calls))
	}
	req := calls[0].req
	if req.Tier != protocol.TierT2 || req.ProposalID != "p_1" || req.StepIndex == nil || req.ParamsHash == "" {
		t.Fatalf("the alter is not a T2 proposal step: %+v", req)
	}
	var params map[string]any
	if err := json.Unmarshal(req.Params, &params); err != nil {
		t.Fatalf("params: %v", err)
	}
	if params["table"] != "public.orders" || params["mode"] != "full" {
		t.Fatalf("alter params = %v", params)
	}
	if got := len(f.callsFor(OpSourceTableReplication)); got != 2 {
		// Validate and Preview each read the table once; Execute re-runs
		// neither (params are the approved ones).
		t.Fatalf("table replication read %d times, want 2 (Validate, Preview)", got)
	}
}

// TestGrantSelectAction runs source_grant_select over the remote clients: the
// connector config read, the role's privileges and the grant itself.
func TestGrantSelectAction(t *testing.T) {
	f := newFakeRunner()
	s := New(f, "r_setup")
	src := s.Source()

	act := sourceactions.NewGrantSelect(sourceactions.GrantSelectDeps{
		Connect: s.Connect(),
		Source:  src,
		Writer:  src,
	})
	ctx := stepCtxLocal(t)
	input := json.RawMessage(`{"connector":"pg-orders-connector","role":"debezium","tables":["public.orders"]}`)
	if err := act.Validate(ctx, input); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	prev, err := act.(actions.Preview).Preview(ctx, input)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(prev.Current, "cannot SELECT public.orders") {
		t.Fatalf("preview current = %q", prev.Current)
	}
	if _, err := act.Execute(ctx, input); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	calls := f.callsFor(OpSourceGrantSelect)
	if len(calls) != 1 {
		t.Fatalf("source_grant_select op sent %d times, want 1", len(calls))
	}
	req := calls[0].req
	if req.Tier != protocol.TierT2 || req.ProposalID != "p_1" || req.StepIndex == nil || req.ParamsHash == "" {
		t.Fatalf("the grant is not a T2 proposal step: %+v", req)
	}
	var params struct {
		Role   string   `json:"role"`
		Tables []string `json:"tables"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		t.Fatalf("params: %v", err)
	}
	if params.Role != "debezium" || len(params.Tables) != 1 || params.Tables[0] != "public.orders" {
		t.Fatalf("grant params = %+v", params)
	}
	if got := len(f.callsFor(OpConnectConfig)); got == 0 {
		t.Fatal("the grant never read the connector's database.user over the runner")
	}
	if got := len(f.callsFor(OpSourceRoleInfo)); got != 2 {
		t.Fatalf("role info read %d times, want 2 (Validate, Preview)", got)
	}
}

// TestNoRunnerThroughTheAction proves the ErrNoRunner contract from the
// action's side: a setup stack built with no runner refuses before I/O.
func TestNoRunnerThroughTheAction(t *testing.T) {
	act := sourceactions.NewSetReplicaIdentity(sourceactions.ReplicaIdentityDeps{
		Source: New(nil, "").Source(),
		Writer: New(nil, "").Source(),
	})
	err := act.Validate(stepCtxLocal(t), json.RawMessage(`{"table":"public.orders","mode":"full"}`))
	if !errors.Is(err, ErrNoRunner) {
		t.Fatalf("Validate without a runner = %v, want ErrNoRunner", err)
	}
}
