package sourceactions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/mysql"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakeMySQL is a MySQL source client as *mysql.DB is: the item 2 reads typed
// with the mysql package's DTOs, which srcneutral.Wrap re-types.
type fakeMySQL struct {
	clients.SourceDB

	tables   map[string]mysql.TableReplication
	role     mysql.RoleInfo
	sessions []mysql.DBSession
	scoped   int
}

func (f *fakeMySQL) Settings(context.Context) ([]mysql.DBSetting, error) { return nil, nil }

func (f *fakeMySQL) TableReplication(_ context.Context, tables []string) ([]mysql.TableReplication, error) {
	var out []mysql.TableReplication
	for _, t := range tables {
		tr, ok := f.tables[t]
		if !ok {
			return nil, fmt.Errorf("table %s: %w", t, clients.ErrNotFound)
		}
		out = append(out, tr)
	}
	return out, nil
}

func (f *fakeMySQL) RoleInfo(context.Context, string, []string) (mysql.RoleInfo, error) {
	return f.role, nil
}

func (f *fakeMySQL) Blockers(context.Context, time.Duration) ([]mysql.DBSession, error) {
	return f.sessions, nil
}

func (f *fakeMySQL) BlockersOnTables(context.Context, time.Duration, []string) ([]mysql.DBSession, error) {
	f.scoped++
	return f.sessions, nil
}

var (
	_ mysql.SourceSettingsReader = (*fakeMySQL)(nil)
	_ mysql.ScopedBlockerReader  = (*fakeMySQL)(nil)
)

const mysqlClass = "io.debezium.connector.mysql.MySqlConnector"

func mysqlPipeline() owners.Pipeline {
	return owners.Pipeline{
		ID:           "orders-mysql",
		SourceKind:   owners.SourceKindMySQL,
		TopicPrefix:  "shop",
		Connectors:   []string{"orders-mysql"},
		SourceTables: []string{"shop.orders", "shop.order_items"},
	}
}

func mysqlCtx() context.Context { return tools.WithPipeline(context.Background(), mysqlPipeline()) }

func mysqlConnect() *fakeConnect {
	return &fakeConnect{cfg: map[string]map[string]string{
		"orders-mysql": {"connector.class": mysqlClass, "database.user": "debezium"},
	}}
}

func TestLayerFor(t *testing.T) {
	neutral := []actions.Action{
		NewGrantSelect(GrantSelectDeps{}),
		NewCreateHeartbeatTable(HeartbeatDeps{}),
		NewEndSession(EndSessionDeps{}),
	}
	for _, a := range neutral {
		if _, ok := a.(SourceLayered); !ok {
			t.Fatalf("%s is not SourceLayered", a.Name())
		}
		if a.Layer() != "postgres" {
			t.Errorf("%s Layer() = %q, want postgres (the value a caller without a pipeline shows)", a.Name(), a.Layer())
		}
		for kind, want := range map[string]string{"": "postgres", "postgres": "postgres", "mysql": "mysql", "oracle": ""} {
			if got := LayerFor(a, kind); got != want {
				t.Errorf("LayerFor(%s, %q) = %q, want %q", a.Name(), kind, got, want)
			}
		}
	}
	ri := NewSetReplicaIdentity(ReplicaIdentityDeps{})
	if _, ok := ri.(SourceLayered); ok {
		t.Fatal("pg_set_replica_identity must not be SourceLayered: it is Postgres-only")
	}
	if got := LayerFor(ri, "mysql"); got != "postgres" {
		t.Errorf("LayerFor(pg_set_replica_identity, mysql) = %q, want postgres", got)
	}
}

func TestDescribeFor(t *testing.T) {
	for _, a := range []actions.Action{NewCreateHeartbeatTable(HeartbeatDeps{}), NewEndSession(EndSessionDeps{})} {
		d := a.(SourceDescribed)
		if d.DescribeFor("postgres") != a.Describe() || d.DescribeFor("") != a.Describe() {
			t.Errorf("%s: the Postgres description must be Describe() unchanged", a.Name())
		}
		my := d.DescribeFor("mysql")
		if my == a.Describe() || strings.Contains(my, "pg_terminate_backend") || strings.Contains(my, "timestamptz") {
			t.Errorf("%s: MySQL description names Postgres things: %s", a.Name(), my)
		}
	}
}

func TestReplicaIdentityRefusesMySQL(t *testing.T) {
	raw := mustJSON(t, map[string]any{"table": "shop.orders", "mode": "full"})
	src := &fakeMySQL{tables: map[string]mysql.TableReplication{"shop.orders": {Table: "shop.orders", HasPrimaryKey: true, Engine: "InnoDB"}}}
	w := &fakeWriter{}
	a := NewSetReplicaIdentity(ReplicaIdentityDeps{Source: src, Writer: w})
	err := a.(actions.Scoped).CheckScope(mysqlPipeline(), raw)
	if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "Postgres-only") {
		t.Fatalf("CheckScope on a MySQL pipeline = %v, want ErrOutOfScope naming Postgres-only", err)
	}
	if err := a.Validate(mysqlCtx(), raw); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("Validate = %v, want ErrOutOfScope", err)
	}
	// The unscoped opt-out has no pipeline: the client's kind refuses.
	unscoped := tools.WithUnscoped(context.Background())
	if err := a.Validate(unscoped, raw); err == nil || !strings.Contains(err.Error(), "source is MySQL") {
		t.Fatalf("unscoped Validate on a MySQL client = %v", err)
	}
	if _, err := a.Execute(unscoped, raw); err == nil || !strings.Contains(err.Error(), "source is MySQL") {
		t.Fatalf("unscoped Execute on a MySQL client = %v", err)
	}
	if len(w.calls) != 0 {
		t.Fatalf("writer called: %v", w.calls)
	}
}

func grantMySQLFixture(accounts ...string) (*fakeMySQL, *fakeWriter, actions.Action) {
	src := &fakeMySQL{role: mysql.RoleInfo{
		Role: "debezium", Exists: true, CanLogin: true, Replication: true,
		Selectable: map[string]bool{"shop.orders": true, "shop.order_items": false},
		Accounts:   accounts,
		Privileges: map[string]bool{"RELOAD": true, "LOCK TABLES": false, "SHOW DATABASES": true, "REPLICATION SLAVE": true, "REPLICATION CLIENT": true},
	}}
	w := &fakeWriter{}
	return src, w, NewGrantSelect(GrantSelectDeps{Connect: mysqlConnect(), Source: src, Writer: w})
}

func grantMySQLParams(t *testing.T, role string) json.RawMessage {
	return mustJSON(t, map[string]any{"connector": "orders-mysql", "role": role, "tables": []string{"shop.orders", "shop.order_items"}})
}

func TestGrantSelectMySQL(t *testing.T) {
	_, w, a := grantMySQLFixture("debezium@%")
	raw := grantMySQLParams(t, "debezium")
	if err := a.Validate(mysqlCtx(), raw); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	pr, err := a.(actions.Preview).Preview(mysqlCtx(), raw)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	for _, want := range []string{"'debezium'@'%'", "GRANT SELECT ON `shop`.`orders`, `shop`.`order_items` TO 'debezium'@'%'", "1 of 2 tables"} {
		if !strings.Contains(pr.Target+pr.Expected, want) {
			t.Errorf("preview lacks %q:\ntarget %s\nexpected %s", want, pr.Target, pr.Expected)
		}
	}
	if !strings.Contains(pr.Current, "lacks LOCK TABLES") {
		t.Errorf("current does not name the missing privilege: %s", pr.Current)
	}
	if !strings.Contains(pr.Rollback, "REVOKE SELECT ON `shop`.`order_items` FROM 'debezium'@'%'") {
		t.Errorf("rollback = %s", pr.Rollback)
	}
	if _, err := a.Execute(mysqlCtx(), raw); err != nil || len(w.calls) != 1 {
		t.Fatalf("Execute: %v, calls %v", err, w.calls)
	}
}

func TestGrantSelectMySQLAmbiguousAccountRefusedAtValidate(t *testing.T) {
	_, w, a := grantMySQLFixture("debezium@%", "debezium@10.0.%")
	raw := grantMySQLParams(t, "debezium")
	err := a.Validate(mysqlCtx(), raw)
	if err == nil || !strings.Contains(err.Error(), "'debezium'@'%', 'debezium'@'10.0.%'") || !strings.Contains(err.Error(), "grant by hand") {
		t.Fatalf("Validate = %v, want the ambiguous-account refusal with both accounts", err)
	}
	pr, err := a.(actions.Preview).Preview(mysqlCtx(), raw)
	if err != nil || !strings.HasPrefix(pr.Expected, "no change: this step is refused") {
		t.Fatalf("Preview = %+v, %v", pr, err)
	}
	if len(w.calls) != 0 {
		t.Fatalf("writer called: %v", w.calls)
	}
}

func TestGrantSelectMySQLRefusesRoot(t *testing.T) {
	_, _, a := grantMySQLFixture("root@localhost")
	conn := mysqlConnect()
	conn.cfg["orders-mysql"]["database.user"] = "root"
	a.(*grantSelect).d.Connect = conn
	if err := a.Validate(mysqlCtx(), grantMySQLParams(t, "root")); err == nil || !strings.Contains(err.Error(), "administrator") {
		t.Fatalf("Validate root = %v", err)
	}
}

func TestHeartbeatMySQLPreview(t *testing.T) {
	src := &fakeMySQL{tables: map[string]mysql.TableReplication{}}
	a := NewCreateHeartbeatTable(HeartbeatDeps{Connect: mysqlConnect(), Source: src, Writer: &fakeWriter{}})
	raw := mustJSON(t, map[string]any{"connector": "orders-mysql", "table": "shop.debezium_heartbeat"})
	if err := a.Validate(mysqlCtx(), raw); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	pr, err := a.(actions.Preview).Preview(mysqlCtx(), raw)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if strings.Contains(pr.Current+pr.Expected, "publication") && !strings.Contains(pr.Expected, "no publication step") {
		t.Errorf("MySQL preview mentions a publication: %s / %s", pr.Current, pr.Expected)
	}
	if strings.Contains(pr.Expected, "pg_add_publication_table") {
		t.Errorf("MySQL preview proposes pg_add_publication_table: %s", pr.Expected)
	}
	for _, want := range []string{
		"shop.debezium_heartbeat does not exist",
		"ENGINE=InnoDB",
		"heartbeat.interval.ms alone keeps the stored position moving",
		`connect_update_connector_config {"changes":{"heartbeat.interval.ms":"10000"},"connector":"orders-mysql"}`,
		"ON DUPLICATE KEY UPDATE",
	} {
		if !strings.Contains(pr.Current+" "+pr.Expected, want) {
			t.Errorf("preview lacks %q:\ncurrent %s\nexpected %s", want, pr.Current, pr.Expected)
		}
	}
	// The follow-up step never carries the query: it is optional on MySQL.
	if strings.Contains(pr.Expected, `"heartbeat.action.query"`) {
		t.Errorf("follow-up step sets heartbeat.action.query: %s", pr.Expected)
	}

	src.tables["shop.debezium_heartbeat"] = mysql.TableReplication{Table: "shop.debezium_heartbeat", HasPrimaryKey: true, Engine: "InnoDB"}
	pr, err = a.(actions.Preview).Preview(mysqlCtx(), raw)
	if err != nil || !strings.Contains(pr.Current, "already exists") || pr.Reversible {
		t.Fatalf("existing table preview = %+v, %v", pr, err)
	}
}

func esMySQLFixture(sessions ...mysql.DBSession) (*fakeMySQL, *fakeWriter, actions.Action) {
	src := &fakeMySQL{sessions: sessions}
	w := &fakeWriter{}
	now := func() time.Time { return testStart.Add(2 * time.Hour) }
	return src, w, &endSession{d: EndSessionDeps{Connect: mysqlConnect(), Source: src, Writer: w, now: now}}
}

func esMySQLParams(t *testing.T, pid int64, started time.Time) json.RawMessage {
	m := map[string]any{"connector": "orders-mysql", "pid": pid}
	if !started.IsZero() {
		m["started_at"] = started.Format(time.RFC3339Nano)
	}
	return mustJSON(t, m)
}

func TestEndSessionMySQL(t *testing.T) {
	src, w, a := esMySQLFixture(
		mysql.DBSession{PID: 31, StartedAt: testStart, XactStart: testStart, User: "app", App: "orders-api", State: "Sleep", Query: "UPDATE shop.orders SET x = 1", InScope: true, OldXact: true, SameDB: true, ClientBackend: true},
		mysql.DBSession{PID: 32, User: "ops", State: "Query Waiting for table metadata lock", BlocksSlot: true, InScope: true, ClientBackend: true},
	)
	raw := esMySQLParams(t, 31, testStart)
	if err := a.Validate(mysqlCtx(), raw); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if src.scoped == 0 {
		t.Fatal("Validate did not use the scoped read")
	}
	pr, err := a.(actions.Preview).Preview(mysqlCtx(), raw)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	for _, want := range []string{"KILL CONNECTION 31", "rolls back the open transaction", "as long to roll back as it ran", `program_name "orders-api"`, `user "app"`, "transaction age 2h0m0s"} {
		if !strings.Contains(pr.Target+pr.Current+pr.Expected, want) {
			t.Errorf("preview lacks %q:\ncurrent %s\nexpected %s", want, pr.Current, pr.Expected)
		}
	}
	if strings.Contains(pr.Expected, "pg_terminate_backend") {
		t.Errorf("MySQL preview names pg_terminate_backend: %s", pr.Expected)
	}
	msg, err := a.Execute(mysqlCtx(), raw)
	if err != nil || !strings.Contains(msg, "killed connection 31") || len(w.calls) != 1 {
		t.Fatalf("Execute = %q, %v, calls %v", msg, err, w.calls)
	}
}

// A session without an open transaction (zero StartedAt) is refused: at
// Validate when the step names no started_at, and when the listed session
// has none (design D3).
func TestEndSessionMySQLZeroStartedAtRefused(t *testing.T) {
	_, w, a := esMySQLFixture(mysql.DBSession{PID: 32, User: "ops", State: "Query", BlocksSlot: true, InScope: true, ClientBackend: true})
	if err := a.Validate(mysqlCtx(), esMySQLParams(t, 32, time.Time{})); err == nil || !strings.Contains(err.Error(), "started_at is required") {
		t.Fatalf("Validate without started_at = %v", err)
	}
	if err := a.Validate(mysqlCtx(), mustJSON(t, map[string]any{"connector": "orders-mysql", "pid": 32, "started_at": time.Time{}})); err == nil || !strings.Contains(err.Error(), "started_at is required") {
		t.Fatalf("Validate with a zero started_at = %v", err)
	}
	err := a.Validate(mysqlCtx(), esMySQLParams(t, 32, testStart))
	if err == nil || !strings.Contains(err.Error(), "no open transaction") {
		t.Fatalf("Validate on a session without a transaction = %v", err)
	}
	if _, err := a.Execute(mysqlCtx(), esMySQLParams(t, 32, testStart)); err == nil {
		t.Fatal("Execute ended a session without a transaction")
	}
	if len(w.calls) != 0 {
		t.Fatalf("writer called: %v", w.calls)
	}
}

// The Postgres previews do not change on a Postgres pipeline: the source
// kind comes from the room pipeline.
func TestPostgresPreviewsUnchangedByKind(t *testing.T) {
	_, _, _, a := esFixture()
	pr, err := a.Preview(scopedCtx(), esParams(4242, testStart))
	if err != nil || !strings.Contains(pr.Expected, "pg_terminate_backend(4242)") {
		t.Fatalf("Postgres end-session preview = %+v, %v", pr, err)
	}
}
