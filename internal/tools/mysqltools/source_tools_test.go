package mysqltools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/mysql"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
)

// Board Q8c: "a test shows the Q2a source_* tools work on MySQL through the
// interface". The four tools (dbtools, A3) assert the item 2 reader on the
// source client. Until the "[Q8-0] contracts: item 7" PR moves item 2 into
// clients.go, dbtools asserts it with the postgres package's DTOs and the
// MySQL client returns its own identical local copies (mysql/types.go), so
// the test bridges them with pgShaped: a field-for-field copy and nothing
// else. Once both packages alias the clients types, pgShaped is deleted and
// *mysql.DB is the Src as it is (Q8n). What this proves today: a reader with
// exactly the MySQL client's method set and DTOs drives all four tools in a
// MySQL room, scoped to the pipeline's db.table names.

// fakeMySQLReader implements the MySQL client's reader interfaces with its
// DTOs, as *mysql.DB does (mysql.go asserts the same two).
type fakeMySQLReader struct {
	clients.SourceDB
	scopedTables []string
	role         string
}

var (
	_ mysql.SourceSettingsReader = (*fakeMySQLReader)(nil)
	_ mysql.ScopedBlockerReader  = (*fakeMySQLReader)(nil)
)

func (f *fakeMySQLReader) Settings(context.Context) ([]mysql.DBSetting, error) {
	return []mysql.DBSetting{
		{Name: "version", Value: "8.4.3"},
		{Name: "log_bin", Value: "ON"},
		{Name: "binlog_format", Value: "ROW"},
		{Name: "binlog_row_image", Value: "MINIMAL"},
		{Name: "binlog_expire_logs_seconds", Value: "86400", Unit: "s", PendingRestart: true},
	}, nil
}

func (f *fakeMySQLReader) TableReplication(_ context.Context, tables []string) ([]mysql.TableReplication, error) {
	out := make([]mysql.TableReplication, 0, len(tables))
	for _, t := range tables {
		out = append(out, mysql.TableReplication{Table: t, HasPrimaryKey: t != "shop.order_items", Engine: "InnoDB"})
	}
	return out, nil
}

func (f *fakeMySQLReader) RoleInfo(_ context.Context, role string, tables []string) (mysql.RoleInfo, error) {
	f.role = role
	sel := map[string]bool{}
	for _, t := range tables {
		sel[t] = t == "shop.orders"
	}
	return mysql.RoleInfo{Role: role, Exists: true, CanLogin: true, Replication: true, Selectable: sel, Accounts: []string{role + "@%"}}, nil
}

func (f *fakeMySQLReader) Blockers(context.Context, time.Duration) ([]mysql.DBSession, error) {
	return nil, nil
}

func (f *fakeMySQLReader) BlockersOnTables(_ context.Context, _ time.Duration, tables []string) ([]mysql.DBSession, error) {
	f.scopedTables = tables
	start := time.Now().Add(-time.Hour)
	return []mysql.DBSession{
		{PID: 42, StartedAt: start, XactStart: start, User: "app", App: "orders-svc", State: "Sleep", Query: "UPDATE shop.orders SET ...", BlocksSlot: true, InScope: true},
		{PID: 43, StartedAt: start, XactStart: start, State: "Query", InScope: false},
	}, nil
}

// pgShaped presents a MySQL reader with the postgres package's DTOs, which
// dbtools asserts until item 7 lands. Field-for-field; the MySQL-only fields
// (Engine, Accounts, Privileges) have no postgres twin yet and are dropped.
type pgShaped struct {
	clients.SourceDB
	m interface {
		mysql.SourceSettingsReader
		mysql.ScopedBlockerReader
	}
}

func (p pgShaped) Settings(ctx context.Context) ([]postgres.DBSetting, error) {
	in, err := p.m.Settings(ctx)
	out := make([]postgres.DBSetting, len(in))
	for i, s := range in {
		out[i] = postgres.DBSetting(s)
	}
	return out, err
}

func (p pgShaped) TableReplication(ctx context.Context, tables []string) ([]postgres.TableReplication, error) {
	in, err := p.m.TableReplication(ctx, tables)
	out := make([]postgres.TableReplication, len(in))
	for i, t := range in {
		out[i] = postgres.TableReplication{Table: t.Table, HasPrimaryKey: t.HasPrimaryKey, ReplicaIdentity: t.ReplicaIdentity}
	}
	return out, err
}

func (p pgShaped) RoleInfo(ctx context.Context, role string, tables []string) (postgres.RoleInfo, error) {
	r, err := p.m.RoleInfo(ctx, role, tables)
	return postgres.RoleInfo{Role: r.Role, Exists: r.Exists, CanLogin: r.CanLogin, Replication: r.Replication, Selectable: r.Selectable}, err
}

func sessionsPG(in []mysql.DBSession) []postgres.DBSession {
	out := make([]postgres.DBSession, len(in))
	for i, s := range in {
		out[i] = postgres.DBSession(s)
	}
	return out
}

func (p pgShaped) Blockers(ctx context.Context, d time.Duration) ([]postgres.DBSession, error) {
	in, err := p.m.Blockers(ctx, d)
	return sessionsPG(in), err
}

func (p pgShaped) BlockersOnTables(ctx context.Context, d time.Duration, tables []string) ([]postgres.DBSession, error) {
	in, err := p.m.BlockersOnTables(ctx, d, tables)
	return sessionsPG(in), err
}

func TestSourceToolsOnMySQL(t *testing.T) {
	o := loadOwners(t)
	m := &fakeMySQLReader{}
	conn := &fakeConnect{configs: map[string]map[string]string{"orders-mysql": mysqlCfg("184054"), "orders-mysql-2": mysqlCfg("184055")}}
	reg := tools.NewRegistry()
	if err := dbtools.Register(reg, dbtools.Deps{Src: pgShaped{m: m}, Connect: conn, Owners: o}); err != nil {
		t.Fatal(err)
	}
	ctx := tools.WithPipeline(context.Background(), pipeline(t, o, "orders-mysql"))
	run := func(name, in string) tools.Result {
		t.Helper()
		tool, ok := reg.Get(name)
		if !ok {
			t.Fatalf("%s not registered", name)
		}
		res, err := tool.Run(ctx, json.RawMessage(in))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return res
	}

	res := run("source_settings", `{}`)
	for _, want := range []string{"log_bin=ON", "binlog_format=ROW", "binlog_row_image=MINIMAL", "binlog_expire_logs_seconds=86400 s (pending_restart"} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("source_settings misses %q:\n%s", want, res.Summary)
		}
	}

	res = run("source_table_replication", `{"pipeline_id":"orders-mysql"}`)
	if !strings.Contains(res.Summary, "shop.order_items") || !strings.Contains(res.Summary, "shop.orders") {
		t.Errorf("source_table_replication:\n%s", res.Summary)
	}

	res = run("source_role_grants", `{"pipeline_id":"orders-mysql"}`)
	if m.role != "debezium" || !strings.Contains(res.Summary, "role debezium: exists=yes login=yes replication=yes") || !strings.Contains(res.Summary, "selectable tables: 1 of 2") {
		t.Errorf("source_role_grants (role %q):\n%s", m.role, res.Summary)
	}

	res = run("source_blocking_sessions", `{}`)
	if strings.Join(m.scopedTables, ",") != "shop.orders,shop.order_items" && strings.Join(m.scopedTables, ",") != "shop.order_items,shop.orders" {
		t.Errorf("scoped read got tables %v", m.scopedTables)
	}
	if !strings.Contains(res.Summary, "pid=42 user=app app=orders-svc") || !strings.Contains(res.Summary, "pid=43") || !strings.Contains(res.Summary, "identity hidden") {
		t.Errorf("source_blocking_sessions:\n%s", res.Summary)
	}
}
