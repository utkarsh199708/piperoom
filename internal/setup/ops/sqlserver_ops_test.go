package ops

import (
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/clients"
	mssqlclient "piperoom/internal/clients/sqlserver"
	"piperoom/internal/clients/sqlserver/sqlservertypes"
	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// RegisterSQLServerSource adds exactly the SQL Server source ops at their
// floors (reads T0, the setup writes T2: U1-2b), and New alone registers none
// of them, so a runner of another source kind cannot reach them.
func TestRegisterSQLServerSource(t *testing.T) {
	want := map[string]string{
		OpSourceServerInfo: protocol.TierT0, OpSourceAgentState: protocol.TierT0,
		OpSourceDatabaseCDC: protocol.TierT0, OpSourceTableCDC: protocol.TierT0, OpSourceRoleInfo: protocol.TierT0,
		OpSourceCreatePublication: protocol.TierT2, OpSourceCreateCanaryTable: protocol.TierT2,
		OpSourceInsertCanary: protocol.TierT2, OpSourceCreateReadRole: protocol.TierT2,
	}
	plain, err := New(Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plain.Close() })
	for op := range want {
		if _, ok := plain.Floor(op); ok {
			t.Errorf("New alone registers %s", op)
		}
	}

	r, err := New(Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if err := RegisterSQLServerSource(r, SQLServerSource{}); err != nil {
		t.Fatalf("RegisterSQLServerSource: %v", err)
	}
	for op, tier := range want {
		got, ok := r.Floor(op)
		if !ok || got != tier {
			t.Errorf("%s floor = %q (registered %v), want %q", op, got, ok, tier)
		}
		if layer, _ := r.LayerOf(op); layer != sqlserverLayer {
			t.Errorf("%s layer = %q, want %q", op, layer, sqlserverLayer)
		}
	}
	// A second registration is a duplicate, never a silent overwrite.
	if err := RegisterSQLServerSource(r, SQLServerSource{}); err == nil {
		t.Fatal("registering the SQL Server source twice succeeded")
	}
	if err := RegisterSQLServerSource(nil, SQLServerSource{}); err == nil {
		t.Fatal("a nil registry was accepted")
	}
}

func TestSQLServerOpsValidate(t *testing.T) {
	r, err := New(Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if err := RegisterSQLServerSource(r, SQLServerSource{}); err != nil {
		t.Fatal(err)
	}
	long := make([]byte, maxSysnameBytes+1)
	for i := range long {
		long[i] = 'a'
	}
	cases := []struct {
		op     string
		params string
		ok     bool
	}{
		{OpSourceServerInfo, `{}`, true},
		{OpSourceServerInfo, `{"extra":1}`, false},
		{OpSourceDatabaseCDC, `{"database":"orders"}`, true},
		{OpSourceDatabaseCDC, `{"database":"orders; DROP DATABASE orders"}`, false},
		{OpSourceDatabaseCDC, `{"database":"` + string(long) + `"}`, false},
		{OpSourceDatabaseCDC, `{}`, false},
		{OpSourceTableCDC, `{"tables":["dbo.orders"]}`, true},
		{OpSourceTableCDC, `{"tables":["orders"]}`, false},
		{OpSourceRoleInfo, `{"role":"piperoom_cdc","tables":[]}`, true},
		{OpSourceRoleInfo, `{"role":"x]; DROP LOGIN sa; --"}`, false},
		{OpSourceCreatePublication, `{"publication":"orders_mssql","tables":["dbo.orders"]}`, true},
		{OpSourceCreatePublication, `{"publication":"orders_mssql","tables":[]}`, false},
		{OpSourceCreateCanaryTable, `{"table":"dbo.piperoom_canary"}`, true},
		{OpSourceCreateCanaryTable, `{"table":"piperoom_canary"}`, false},
		{OpSourceInsertCanary, `{"table":"dbo.piperoom_canary","nonce":"n0nce_42"}`, true},
		{OpSourceInsertCanary, `{"table":"dbo.piperoom_canary","nonce":"'); DROP TABLE x; --"}`, false},
		{OpSourceCreateReadRole, `{"role":"piperoom_cdc","tables":["dbo.orders"]}`, true},
		{OpSourceCreateReadRole, `{"role":"piperoom_cdc","tables":[]}`, false},
	}
	for _, tc := range cases {
		op, ok := r.Get(tc.op)
		if !ok {
			t.Fatalf("%s is not registered", tc.op)
		}
		err := op.Validate(json.RawMessage(tc.params), Env{})
		if (err == nil) != tc.ok {
			t.Errorf("%s %s: Validate = %v, want ok %v", tc.op, tc.params, err, tc.ok)
		}
		if err != nil && !errors.Is(err, ErrParams) {
			t.Errorf("%s %s: refusal %v is not ErrParams", tc.op, tc.params, err)
		}
	}
	// CheckTarget: bad params are a schema refusal; a room with no source
	// database target is a target refusal.
	if got := r.CheckTarget(protocol.OpRequest{Op: OpSourceDatabaseCDC, Params: json.RawMessage(`{"database":"x;"}`)}); got != protocol.RefuseSchema {
		t.Errorf("CheckTarget(bad params) = %q, want %q", got, protocol.RefuseSchema)
	}
	if got := r.CheckTarget(protocol.OpRequest{Op: OpSourceDatabaseCDC, Params: json.RawMessage(`{"database":"orders"}`)}); got != protocol.RefuseTarget {
		t.Errorf("CheckTarget(no target) = %q, want %q", got, protocol.RefuseTarget)
	}
	withTarget, err := New(Config{ProjectDir: t.TempDir(), Targets: []events.SetupTarget{{Kind: "postgres", Address: "mssql.acme:1433"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = withTarget.Close() })
	if err := RegisterSQLServerSource(withTarget, SQLServerSource{}); err != nil {
		t.Fatal(err)
	}
	if got := withTarget.CheckTarget(protocol.OpRequest{Op: OpSourceDatabaseCDC, Params: json.RawMessage(`{"database":"orders"}`)}); got != "" {
		t.Errorf("CheckTarget(with target) = %q, want none", got)
	}
}

// The real SQL Server client and setup writer satisfy the interfaces the ops
// take (asserted here, not in sqlserver_ops.go, so the catalogue does not
// link go-mssqldb into the server).
var (
	_ sqlservertypes.SettingsReader = (*mssqlclient.Client)(nil)
	_ clients.SetupSourceWriter     = (*mssqlclient.SetupWriter)(nil)
)
