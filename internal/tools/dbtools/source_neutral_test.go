package dbtools

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/mysql"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakeMySQLSource is a MySQL source client as *mysql.DB is: clients.SourceDB
// (ReplicationSlots and RecentDDL answer ErrNotSupported), the item 2 reads
// typed with the mysql package's DTOs, and no table-filtered
// LongTransactions. PrimaryKeyColumn is optional (mysqlPKSource below).
type fakeMySQLSource struct {
	settings []mysql.DBSetting
	tables   map[string]mysql.TableReplication
	role     mysql.RoleInfo
	sessions []mysql.DBSession
	txns     []clients.Transaction
	rows     map[string]map[string]any
	rowCalls int
}

func (f *fakeMySQLSource) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	return nil, clients.ErrNotSupported
}
func (f *fakeMySQLSource) RecentDDL(context.Context, time.Time, int) ([]clients.DDLChange, error) {
	return nil, clients.ErrNotSupported
}
func (f *fakeMySQLSource) LongTransactions(context.Context, time.Duration) ([]clients.Transaction, error) {
	return f.txns, nil
}
func (f *fakeMySQLSource) Row(_ context.Context, table, _, key string) (map[string]any, bool, error) {
	f.rowCalls++
	r, ok := f.rows[table+"|"+key]
	return r, ok, nil
}
func (f *fakeMySQLSource) Settings(context.Context) ([]mysql.DBSetting, error) {
	return f.settings, nil
}
func (f *fakeMySQLSource) TableReplication(_ context.Context, tables []string) ([]mysql.TableReplication, error) {
	var out []mysql.TableReplication
	for _, t := range tables {
		tr, ok := f.tables[t]
		if !ok {
			return nil, fmt.Errorf("mysql: table %s: %w", t, clients.ErrNotFound)
		}
		out = append(out, tr)
	}
	return out, nil
}
func (f *fakeMySQLSource) RoleInfo(context.Context, string, []string) (mysql.RoleInfo, error) {
	return f.role, nil
}
func (f *fakeMySQLSource) Blockers(context.Context, time.Duration) ([]mysql.DBSession, error) {
	return f.sessions, nil
}
func (f *fakeMySQLSource) BlockersOnTables(context.Context, time.Duration, []string) ([]mysql.DBSession, error) {
	out := slices.Clone(f.sessions)
	for i := range out {
		if !out[i].InScope {
			out[i].User, out[i].App, out[i].Query = "", "", ""
		}
	}
	return out, nil
}

var (
	_ clients.SourceDB           = (*fakeMySQLSource)(nil)
	_ mysql.SourceSettingsReader = (*fakeMySQLSource)(nil)
	_ mysql.ScopedBlockerReader  = (*fakeMySQLSource)(nil)
)

// mysqlPKSource adds PrimaryKeyColumn.
type mysqlPKSource struct {
	*fakeMySQLSource
	pk string
}

func (p mysqlPKSource) PrimaryKeyColumn(context.Context, string) (string, error) { return p.pk, nil }

func mysqlOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{{
		ID: "orders-mysql", SourceKind: owners.SourceKindMySQL, TopicPrefix: "shop",
		SourceTables: []string{"shop.orders", "shop.order_items"}, Connectors: []string{"orders-mysql"},
	}}}
}

func mysqlDeps(src clients.SourceDB) Deps {
	return Deps{
		Src: src, SourceKind: owners.SourceKindMySQL, Owners: mysqlOwners(),
		Builder: &fakeBuilder{},
		Connect: &fakeConnect{configs: map[string]map[string]string{
			"orders-mysql": {"connector.class": "io.debezium.connector.mysql.MySqlConnector", "database.user": "debezium"},
		}},
	}
}

func mysqlRoom() context.Context { return tools.WithPipelineID(context.Background(), "orders-mysql") }

func runMySQL(t *testing.T, d Deps, name, in string) tools.Result {
	t.Helper()
	r, err := scopedTool(t, d, name).Run(mysqlRoom(), json.RawMessage(in))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return r
}

func registered(t *testing.T, d Deps) []string {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tl := range reg.All() {
		names = append(names, tl.Name())
	}
	return names
}

// The two neutral names are registered for a MySQL source only, the pg_*
// tools for a Postgres source only, and a Postgres list is the one it was.
func TestRegisterBySourceKind(t *testing.T) {
	pg := registered(t, testDeps())
	want := []string{"pg_replication_slots", "pg_streaming_liveness", "pg_recent_ddl", "pg_long_transactions", "pg_row",
		"pg_publication_tables", "pg_connector_scope_gap", "pg_toast_risk", "source_settings", "source_table_replication",
		"source_role_grants", "source_blocking_sessions", "warehouse_freshness", "warehouse_row", "warehouse_load_errors",
		"reconcile", "pipeline_map", "trace_record"}
	if !slices.Equal(pg, want) {
		t.Fatalf("Postgres tool list changed:\n got %v\nwant %v", pg, want)
	}
	pgExplicit := testDeps()
	pgExplicit.SourceKind = owners.SourceKindPostgres
	if got := registered(t, pgExplicit); !slices.Equal(got, want) {
		t.Fatalf("SourceKind postgres list = %v", got)
	}

	my := registered(t, mysqlDeps(&fakeMySQLSource{}))
	for _, n := range PostgresOnlyTools() {
		if slices.Contains(my, n) {
			t.Errorf("MySQL source registers Postgres-only %s", n)
		}
	}
	for _, n := range MySQLOnlyTools() {
		if !slices.Contains(my, n) {
			t.Errorf("MySQL source lacks %s", n)
		}
		if slices.Contains(pg, n) {
			t.Errorf("Postgres source registers MySQL-only %s", n)
		}
	}
	for _, n := range my {
		if strings.HasPrefix(n, "pg_") {
			t.Errorf("MySQL source registers %s", n)
		}
	}
	for _, n := range []string{"source_settings", "source_table_replication", "source_role_grants", "source_blocking_sessions"} {
		if !slices.Contains(my, n) {
			t.Errorf("MySQL source lacks %s", n)
		}
	}

	bad := testDeps()
	bad.SourceKind = "oracle"
	if err := Register(tools.NewRegistry(), bad); err == nil {
		t.Fatal("unknown SourceKind registered")
	}
}

func TestMySQLDescriptionsNamePostgresNothing(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, mysqlDeps(&fakeMySQLSource{})); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"source_settings", "source_table_replication", "source_role_grants", "source_blocking_sessions", "source_long_transactions", "source_row"} {
		tl, _ := reg.Get(n)
		d := tl.Description()
		for _, pgWord := range []string{"wal_level", "replica identity", "pg_read_all_stats", "source Postgres", "xid horizon"} {
			if strings.Contains(d, pgWord) {
				t.Errorf("%s description names %q: %s", n, pgWord, d)
			}
		}
	}
}

// The four Q2a tools on a MySQL client, through the interface.
func TestSourceToolsOnMySQL(t *testing.T) {
	retention := "0"
	src := &fakeMySQLSource{
		settings: []mysql.DBSetting{
			{Name: "version", Value: "8.4.3"},
			{Name: "log_bin", Value: "ON"},
			{Name: "binlog_format", Value: "MIXED", PendingRestart: true},
			{Name: "binlog_row_image", Value: "FULL"},
			{Name: "gtid_mode", Value: "OFF"},
			{Name: "enforce_gtid_consistency", Value: "OFF"},
			{Name: "server_id", Value: "1"},
			{Name: "binlog_expire_logs_seconds", Value: "86400", Unit: "s"},
			{Name: "lower_case_table_names", Value: "0"},
			{Name: "rds.binlog_retention_hours", Value: retention, Unit: "h"},
		},
		tables: map[string]mysql.TableReplication{
			"shop.orders":      {Table: "shop.orders", HasPrimaryKey: true, Engine: "InnoDB"},
			"shop.order_items": {Table: "shop.order_items", HasPrimaryKey: false, Engine: "MyISAM"},
		},
		role: mysql.RoleInfo{
			Role: "debezium", Exists: true, CanLogin: true, Replication: false,
			Selectable: map[string]bool{"shop.orders": true, "shop.order_items": true},
			Accounts:   []string{"debezium@%", "debezium@10.0.%"},
			Privileges: map[string]bool{"REPLICATION SLAVE": false, "REPLICATION CLIENT": true, "RELOAD": true, "LOCK TABLES": true, "SHOW DATABASES": true},
		},
		sessions: []mysql.DBSession{
			{PID: 31, StartedAt: time.Now().Add(-time.Hour), XactStart: time.Now().Add(-time.Hour), User: "app", App: "orders-api", State: "Sleep", Query: "UPDATE shop.orders SET status = 'x'", InScope: true, OldXact: true},
			{PID: 32, User: "ops", State: "Query\nWaiting for table metadata lock", BlocksSlot: true, InScope: true},
			{PID: 33, StartedAt: time.Now().Add(-2 * time.Hour), XactStart: time.Now().Add(-2 * time.Hour), User: "other", State: "Sleep", InScope: false, OldXact: true},
		},
	}
	d := mysqlDeps(src)

	r := runMySQL(t, d, "source_settings", `{}`)
	for _, want := range []string{
		"version=8.4.3\n",
		"binlog_format=MIXED (pending_restart",
		"(Debezium needs ROW)",
		"(ON is recommended: with GTIDs the stored position survives a failover)",
		"binlog_expire_logs_seconds=86400 s",
		"rds.binlog_retention_hours=0 h",
		"RDS purges binlogs as soon as it can",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("source_settings summary lacks %q:\n%s", want, r.Summary)
		}
	}
	if strings.Contains(r.Summary, "pg_settings") || strings.Contains(r.Summary, "wal_level") || strings.Contains(r.Summary, "binlog_expire_logs_auto_purge") {
		t.Errorf("MySQL settings summary names Postgres settings or the optional 8.0.29 one: %s", r.Summary)
	}

	r = runMySQL(t, d, "source_table_replication", `{"pipeline_id":"orders-mysql"}`)
	for _, want := range []string{"tables=2 without_primary_key=1 engine_not_innodb=1", "message.key.columns", "not transactional", "shop.orders: has_primary_key=yes engine=InnoDB", "shop.order_items: has_primary_key=no engine=MyISAM"} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("source_table_replication lacks %q:\n%s", want, r.Summary)
		}
	}
	if strings.Contains(r.Summary, "replica_identity") {
		t.Errorf("MySQL summary names replica identity: %s", r.Summary)
	}

	r = runMySQL(t, d, "source_role_grants", `{"pipeline_id":"orders-mysql"}`)
	for _, want := range []string{"replication=no", "accounts: debezium@%, debezium@10.0.%", "several accounts", "REPLICATION SLAVE=no", "LOCK TABLES=yes", "selectable tables: 2 of 2"} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("source_role_grants lacks %q:\n%s", want, r.Summary)
		}
	}
	if got := r.Data.(roleGrantsData).Role; len(got.Accounts) != 2 || got.Privileges["REPLICATION SLAVE"] {
		t.Errorf("role Data lost the MySQL fields: %+v", got)
	}

	r = runMySQL(t, d, "source_blocking_sessions", `{}`)
	for _, want := range []string{
		"pid=31 user=app app=orders-api state=Sleep", "holds_waited_lock=no",
		"pid=32 user=ops app=- state=Query Waiting for table metadata lock age=- holds_waited_lock=yes",
		"pid=33 state=Sleep", "identity hidden", "processlist id",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("source_blocking_sessions lacks %q:\n%s", want, r.Summary)
		}
	}
	if strings.Contains(r.Summary, "blocks_slot") || strings.Contains(r.Summary, "state=?") {
		t.Errorf("MySQL blocking summary uses the Postgres vocabulary: %s", r.Summary)
	}
}

func TestSourceTableReplicationMySQLMissingTable(t *testing.T) {
	d := mysqlDeps(&fakeMySQLSource{tables: map[string]mysql.TableReplication{}})
	_, err := scopedTool(t, d, "source_table_replication").Run(mysqlRoom(), json.RawMessage(`{"pipeline_id":"orders-mysql"}`))
	if err == nil || !strings.Contains(err.Error(), "not visible to the read user") {
		t.Fatalf("err = %v", err)
	}
}

func TestSourceLongTransactionsMySQL(t *testing.T) {
	now := time.Now()
	src := &fakeMySQLSource{
		sessions: []mysql.DBSession{
			{PID: 31, StartedAt: now.Add(-time.Hour), XactStart: now.Add(-time.Hour), User: "app", App: "orders-api", State: "Sleep", Query: "update shop.orders set x = 1", InScope: true, OldXact: true},
			{PID: 32, User: "ops", State: "Query", BlocksSlot: true, InScope: true},
			{PID: 33, StartedAt: now.Add(-2 * time.Hour), XactStart: now.Add(-2 * time.Hour), User: "secret-user", State: "Sleep", OldXact: true},
		},
		txns: []clients.Transaction{{PID: 40, Username: "batch", State: "RUNNING", Age: 3 * time.Hour, Query: "DELETE FROM shop.orders"}},
	}
	d := mysqlDeps(src)
	r := runMySQL(t, d, "source_long_transactions", `{}`)
	for _, want := range []string{"pid=31 user=app app=orders-api state=Sleep", "verb=UPDATE", "1 other long transaction"} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("scoped summary lacks %q:\n%s", want, r.Summary)
		}
	}
	for _, leak := range []string{"secret-user", "pid=33", "pid=32", "writer="} {
		if strings.Contains(r.Summary, leak) {
			t.Errorf("scoped summary shows %q:\n%s", leak, r.Summary)
		}
	}
	data := r.Data.(scopedTxns)
	if len(data.Transactions) != 1 || data.OtherNotShown != 1 {
		t.Errorf("data = %+v", data)
	}

	// Unscoped: the plain LongTransactions read.
	tl := scopedTool(t, d, "source_long_transactions")
	r, err := tl.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{}`))
	if err != nil || !strings.Contains(r.Summary, "pid=40 user=batch") || !strings.Contains(r.Summary, "verb=DELETE") {
		t.Fatalf("unscoped = %q, %v", r.Summary, err)
	}
}

// pg_long_transactions keeps refusing a client without the table filter in
// a room: the blocking-session fallback is source_long_transactions' only.
func TestPgLongTransactionsKeepsItsRefusal(t *testing.T) {
	d := testDeps()
	d.Src = &fakeMySQLSource{}
	_, err := scopedTool(t, d, "pg_long_transactions").Run(roomCtx(), json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "pg_long_transactions: the source client cannot filter transactions by table") {
		t.Fatalf("err = %v", err)
	}
}

func TestSourceRowMySQL(t *testing.T) {
	base := &fakeMySQLSource{rows: map[string]map[string]any{
		"shop.orders|7": {"id": "7", "email": "a@example.com", "updated_at": "2026-10-11T10:00:00Z"},
	}}
	in := `{"pipeline_id":"orders-mysql","table":"shop.orders","pk_column":"id","key":"7"}`

	// Without PrimaryKeyColumn the tool refuses rather than read by an
	// arbitrary column.
	_, err := scopedTool(t, mysqlDeps(base), "source_row").Run(mysqlRoom(), json.RawMessage(in))
	if err == nil || !strings.Contains(err.Error(), "cannot name a table's primary key column") || base.rowCalls != 0 {
		t.Fatalf("no-pk client: err = %v, rows read %d", err, base.rowCalls)
	}

	d := mysqlDeps(mysqlPKSource{fakeMySQLSource: base, pk: "id"})
	r := runMySQL(t, d, "source_row", in)
	if !strings.Contains(r.Summary, "found: yes") || strings.Contains(r.Summary, "a@example.com") {
		t.Fatalf("summary = %q", r.Summary)
	}
	if got := r.Data.(map[string]any); got["email"] != nil || got["id"] != "7" {
		t.Fatalf("data = %v", got)
	}
	_, err = scopedTool(t, d, "source_row").Run(mysqlRoom(), json.RawMessage(`{"pipeline_id":"orders-mysql","table":"shop.orders","pk_column":"email","key":"a@example.com"}`))
	if err == nil || !strings.Contains(err.Error(), "primary key column") {
		t.Fatalf("non-pk column: %v", err)
	}
	_, err = scopedTool(t, d, "source_row").Run(mysqlRoom(), json.RawMessage(`{"pipeline_id":"orders-mysql","table":"shop.customers","pk_column":"id","key":"7"}`))
	if err == nil {
		t.Fatal("table outside the pipeline read")
	}
}
