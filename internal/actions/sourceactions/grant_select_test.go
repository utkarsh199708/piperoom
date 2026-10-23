package sourceactions

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

func grantFixture() (*fakeConnect, *fakeSource, *fakeWriter, *grantSelect) {
	conn, w := newConnect(), &fakeWriter{}
	src := &fakeSource{role: RoleInfo{Role: "debezium", Exists: true, CanLogin: true, Replication: true,
		Selectable: map[string]bool{"public.orders": true, "public.refunds": false}}}
	return conn, src, w, &grantSelect{d: GrantSelectDeps{Connect: conn, Source: src, Writer: w}}
}

func grantParamsJSON(role string, tables ...string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{"connector": "pg-orders", "role": role, "tables": tables})
	return b
}

func TestGrantSelectCheckScope(t *testing.T) {
	_, _, _, a := grantFixture()
	if err := a.CheckScope(testPipeline(), grantParamsJSON("debezium", "public.orders", "public.refunds")); err != nil {
		t.Fatalf("CheckScope: %v", err)
	}
	if err := a.CheckScope(testPipeline(), grantParamsJSON("debezium", "public.refunds", "public.salaries")); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("table outside source_tables = %v, want ErrOutOfScope", err)
	}
}

func TestGrantSelectValidate(t *testing.T) {
	tests := []struct {
		name    string
		user    string
		role    string
		tables  []string
		info    *RoleInfo
		wantErr string
		is      error
	}{
		{name: "ok", user: "debezium", role: "debezium", tables: []string{"public.orders", "public.refunds"}},
		{name: "not the connector user", user: "debezium", role: "app", tables: []string{"public.refunds"}, wantErr: "is not connector"},
		{name: "case differs", user: "Debezium", role: "debezium", tables: []string{"public.refunds"}, wantErr: "is not connector"},
		{name: "config provider user", user: "${file:/secrets/db.properties:user}", role: "debezium", tables: []string{"public.refunds"}, wantErr: "config provider reference"},
		{name: "no user", user: "", role: "debezium", tables: []string{"public.refunds"}, wantErr: "sets no database.user"},
		{name: "already selectable", user: "debezium", role: "debezium", tables: []string{"public.orders"}, wantErr: "nothing to do"},
		{name: "missing role", user: "debezium", role: "debezium", tables: []string{"public.refunds"}, info: &RoleInfo{Role: "debezium"}, is: clients.ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, src, w, a := grantFixture()
			conn.cfg["pg-orders"]["database.user"] = tt.user
			if tt.info != nil {
				src.role = *tt.info
			}
			err := a.Validate(scopedCtx(), grantParamsJSON(tt.role, tt.tables...))
			switch {
			case tt.is != nil:
				if !errors.Is(err, tt.is) {
					t.Fatalf("Validate = %v, want %v", err, tt.is)
				}
			case tt.wantErr == "" && err != nil:
				t.Fatalf("Validate: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("Validate = %v, want %q", err, tt.wantErr)
			}
			if len(w.calls) != 0 {
				t.Fatal("Validate wrote")
			}
		})
	}
}

func TestGrantSelectValidateReadErrors(t *testing.T) {
	conn, src, _, a := grantFixture()
	src.err = errors.New("db down")
	if err := a.Validate(scopedCtx(), grantParamsJSON("debezium", "public.refunds")); err == nil || !strings.Contains(err.Error(), "db down") {
		t.Fatalf("reader error = %v", err)
	}
	conn.cfg["pg-orders"]["connector.class"] = "io.confluent.connect.jdbc.JdbcSinkConnector"
	if err := a.Validate(scopedCtx(), grantParamsJSON("debezium", "public.refunds")); err == nil || !strings.Contains(err.Error(), "not a Debezium source") {
		t.Fatalf("sink = %v", err)
	}
	a.d.Source = nil
	conn.cfg["pg-orders"]["connector.class"] = pgClass
	if err := a.Validate(scopedCtx(), grantParamsJSON("debezium", "public.refunds")); err == nil || !strings.Contains(err.Error(), "cannot read roles") {
		t.Fatalf("no reader = %v", err)
	}
}

func TestGrantSelectPreview(t *testing.T) {
	_, src, w, a := grantFixture()
	res, err := a.Preview(scopedCtx(), grantParamsJSON("debezium", "public.orders", "public.refunds"))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(res.Expected, `GRANT SELECT ON TABLE "public"."orders", "public"."refunds" TO "debezium": SELECT only`) || !strings.Contains(res.Expected, "1 of 2 tables") {
		t.Errorf("Expected: %s", res.Expected)
	}
	if !strings.Contains(res.Current, "can SELECT public.orders") || !strings.Contains(res.Current, "cannot SELECT public.refunds") {
		t.Errorf("Current: %s", res.Current)
	}
	if !res.Reversible || res.Rollback != `REVOKE SELECT ON TABLE "public"."refunds" FROM "debezium" (by a DBA; no registered action; only the tables that lacked it)` {
		t.Errorf("rollback: %v %q", res.Reversible, res.Rollback)
	}
	if !slices.Equal(src.lastTables, []string{"public.orders", "public.refunds"}) || len(w.calls) != 0 {
		t.Fatalf("reads %v, writes %d", src.lastTables, len(w.calls))
	}

	src.role.CanLogin = false
	res, _ = a.Preview(scopedCtx(), grantParamsJSON("debezium", "public.refunds"))
	if !strings.Contains(res.Expected, "cannot log in") {
		t.Errorf("no-login note missing: %s", res.Expected)
	}
	src.role.Selectable["public.refunds"] = true
	res, _ = a.Preview(scopedCtx(), grantParamsJSON("debezium", "public.refunds"))
	if !strings.HasPrefix(res.Expected, "no change: this step is refused") || res.Reversible {
		t.Errorf("refused preview: %+v", res)
	}
	if _, err := a.Preview(scopedCtx(), grantParamsJSON("app", "public.refunds")); err == nil {
		t.Error("Preview accepted a role that is not database.user")
	}
}

func TestGrantSelectExecute(t *testing.T) {
	_, _, w, a := grantFixture()
	if _, err := a.Execute(scopedCtx(), grantParamsJSON("debezium", "public.orders", "public.refunds")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(w.calls) != 1 || w.calls[0].method != "GrantSelect" || w.calls[0].args[0] != "debezium" ||
		!slices.Equal(w.calls[0].args[1].([]string), []string{"public.orders", "public.refunds"}) {
		t.Fatalf("writes: %+v", w.calls)
	}
	w.err = clients.ErrNotFound
	if _, err := a.Execute(scopedCtx(), grantParamsJSON("debezium", "public.refunds")); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("writer error = %v", err)
	}
}

// The connector's database.user changed after approval: Execute grants nothing.
func TestGrantSelectExecuteRechecksUser(t *testing.T) {
	conn, _, w, a := grantFixture()
	conn.cfg["pg-orders"]["database.user"] = "app"
	if _, err := a.Execute(scopedCtx(), grantParamsJSON("debezium", "public.refunds")); err == nil || !strings.Contains(err.Error(), "is not connector") {
		t.Fatalf("Execute = %v, want a database.user refusal", err)
	}
	if len(w.calls) != 0 {
		t.Fatalf("writes: %+v", w.calls)
	}
}
