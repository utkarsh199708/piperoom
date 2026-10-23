package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"piperoom/internal/clients"
)

func TestSettings(t *testing.T) {
	srv := (&fakeServer{}).
		on("performance_schema.global_variables", []string{"n", "v", "p"},
			// out of order, upper case, one persisted-only change, one equal persisted value
			row([]byte("BINLOG_FORMAT"), []byte("ROW"), nil),
			row([]byte("version"), []byte("8.4.3"), nil),
			row([]byte("binlog_expire_logs_seconds"), []byte("86400"), []byte("604800")),
			row([]byte("gtid_mode"), []byte("ON"), []byte("on")),
			row([]byte("log_bin"), []byte("ON"), nil),
		).
		onErr("rds_show_configuration", &gomysql.MySQLError{Number: 1305, Message: "PROCEDURE mysql.rds_show_configuration does not exist"})
	d := newFake(t, srv)
	got, err := d.Settings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []DBSetting{
		{Name: "version", Value: "8.4.3"},
		{Name: "log_bin", Value: "ON"},
		{Name: "binlog_format", Value: "ROW"},
		{Name: "gtid_mode", Value: "ON"},
		{Name: "binlog_expire_logs_seconds", Value: "86400", Unit: "s", PendingRestart: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}
	var q call
	for _, c := range srv.queries() {
		if strings.Contains(c.query, "global_variables") {
			q = c
		}
	}
	if len(q.args) != len(settingNames) || q.args[0] != "version" {
		t.Fatalf("names not bound: %v", q.args)
	}
}

func TestSettingsRDS(t *testing.T) {
	for _, tt := range []struct {
		value driver.Value
		want  *DBSetting
	}{
		{[]byte("24"), &DBSetting{Name: rdsRetentionSetting, Value: "24", Unit: "h"}},
		{nil, &DBSetting{Name: rdsRetentionSetting, Value: "", Unit: "h"}},
		{[]byte("NULL"), &DBSetting{Name: rdsRetentionSetting, Value: "", Unit: "h"}},
		{[]byte("soon"), nil},
	} {
		srv := (&fakeServer{}).
			on("global_variables", []string{"n", "v", "p"}).
			on("rds_show_configuration", []string{"name", "value", "description"},
				row([]byte("target delay"), []byte("0"), []byte("")),
				row([]byte("binlog retention hours"), tt.value, []byte("binlog retention hours specifies the duration in hours")))
		d := newFake(t, srv)
		got, err := d.Settings(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if tt.want == nil {
			if len(got) != 0 {
				t.Errorf("value %v: got %+v, want nothing", tt.value, got)
			}
			continue
		}
		if len(got) != 1 || got[0] != *tt.want {
			t.Errorf("value %v: got %+v", tt.value, got)
		}
	}
}

func TestSettingsPrivilege(t *testing.T) {
	srv := (&fakeServer{}).onErr("global_variables", &gomysql.MySQLError{Number: 1142, Message: "SELECT command denied to user"})
	d := newFake(t, srv)
	_, err := d.Settings(context.Background())
	if !errors.Is(err, ErrPrivilege) || !strings.Contains(err.Error(), "performance_schema") {
		t.Fatalf("err = %v", err)
	}
}

func TestTableReplication(t *testing.T) {
	srv := (&fakeServer{}).on("information_schema.TABLES", []string{"s", "n", "e", "pk"},
		row([]byte("shop"), []byte("order_items"), []byte("MyISAM"), int64(0)),
		row([]byte("Shop"), []byte("Orders"), []byte("InnoDB"), int64(1)),
	)
	d := newFake(t, srv)
	got, err := d.TableReplication(context.Background(), []string{"shop.orders", "shop.order_items"})
	if err != nil {
		t.Fatal(err)
	}
	want := []TableReplication{
		{Table: "shop.orders", HasPrimaryKey: true, Engine: "InnoDB"},
		{Table: "shop.order_items", Engine: "MyISAM"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
	q := srv.queries()[len(srv.queries())-1]
	if !reflect.DeepEqual(q.args, []any{"shop", "orders", "shop", "order_items"}) || !strings.Contains(q.query, "IN ((?, ?), (?, ?))") {
		t.Fatalf("not bound: %v %s", q.args, q.query)
	}
	if strings.Contains(q.query, "orders") {
		t.Fatal("a table name reached the statement text")
	}

	_, err = d.TableReplication(context.Background(), []string{"shop.orders", "shop.gone"})
	if !errors.Is(err, clients.ErrNotFound) || !strings.Contains(err.Error(), "shop.gone") {
		t.Fatalf("missing table: %v", err)
	}
	if got, err := d.TableReplication(context.Background(), nil); got != nil || err != nil {
		t.Fatalf("no tables: %v %v", got, err)
	}
}

// grantsServer answers the RoleInfo reads for a user with the given
// mysql.user rows (user, host, locked, expired, slave, client, select,
// reload, lock, showdb).
func grantsServer(users [][]driver.Value, roleEdges [][]driver.Value, allRoles string, dbRows, tableRows [][]driver.Value) *fakeServer {
	srv := &fakeServer{}
	userCols := []string{"User", "Host", "l", "e", "rs", "rc", "s", "r", "lt", "sd"}
	srv.onFunc("FROM mysql.user", func(args []driver.NamedValue) result {
		var out [][]driver.Value
		for _, u := range users {
			user, host := string(u[0].([]byte)), string(u[1].([]byte))
			if len(args) == 1 { // WHERE User = ?
				if user == args[0].Value.(string) {
					out = append(out, u)
				}
				continue
			}
			for i := 0; i+1 < len(args); i += 2 { // WHERE (User, Host) IN (...)
				if user == args[i].Value.(string) && host == args[i+1].Value.(string) {
					out = append(out, u)
				}
			}
		}
		return result{cols: userCols, rows: out}
	})
	srv.on("activate_all_roles_on_login", []string{"a"}, row([]byte(allRoles)))
	srv.on("FROM mysql.role_edges", []string{"th", "fu", "fh", "def"}, roleEdges...)
	srv.on("FROM mysql.db", []string{"u", "h", "db"}, dbRows...)
	srv.on("FROM mysql.tables_priv", []string{"u", "h", "db", "t"}, tableRows...)
	return srv
}

func u(user, host string, flags ...int64) []driver.Value {
	v := []driver.Value{[]byte(user), []byte(host)}
	for _, f := range flags {
		v = append(v, f)
	}
	return v
}

func b(s string) []byte { return []byte(s) }

func TestRoleInfoMissingUser(t *testing.T) {
	srv := grantsServer(nil, nil, "OFF", nil, nil)
	d := newFake(t, srv)
	got, err := d.RoleInfo(context.Background(), "debezium", []string{"shop.orders"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Exists || got.CanLogin || got.Replication || got.Selectable["shop.orders"] || got.Accounts != nil || got.Privileges != nil {
		t.Fatalf("got %+v", got)
	}
}

func TestRoleInfoOneAccount(t *testing.T) {
	srv := grantsServer(
		[][]driver.Value{u("debezium", "%", 0, 0, 1, 1, 0, 1, 0, 1)},
		nil, "OFF",
		[][]driver.Value{{b("debezium"), b("%"), b("sh_p")}}, // db-level SELECT on shop, by pattern
		[][]driver.Value{{b("debezium"), b("%"), b("audit"), b("events")}},
	)
	d := newFake(t, srv)
	got, err := d.RoleInfo(context.Background(), "debezium", []string{"shop.orders", "audit.events", "audit.secrets"})
	if err != nil {
		t.Fatal(err)
	}
	want := RoleInfo{
		Role: "debezium", Exists: true, CanLogin: true, Replication: true,
		Selectable: map[string]bool{"shop.orders": true, "audit.events": true, "audit.secrets": false},
		Accounts:   []string{"debezium@%"},
		Privileges: map[string]bool{PrivReplicationSlave: true, PrivReplicationClient: true, PrivReload: true, PrivLockTables: false, PrivShowDatabases: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}
}

// Several accounts combine with AND (design §5.2); a locked one cannot log
// in, and a privilege missing on one is missing.
func TestRoleInfoAccountsCombineWithAnd(t *testing.T) {
	srv := grantsServer(
		[][]driver.Value{
			u("debezium", "%", 0, 0, 1, 1, 1, 1, 1, 1),
			u("debezium", "10.0.%", 1, 0, 0, 1, 0, 1, 1, 1),
		},
		nil, "OFF", nil, nil,
	)
	d := newFake(t, srv)
	got, err := d.RoleInfo(context.Background(), "debezium", []string{"shop.orders"})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Exists || got.CanLogin || got.Replication || got.Selectable["shop.orders"] ||
		got.Privileges[PrivReplicationSlave] || !got.Privileges[PrivReplicationClient] ||
		!reflect.DeepEqual(got.Accounts, []string{"debezium@%", "debezium@10.0.%"}) {
		t.Fatalf("got %+v", got)
	}
}

// A role's grants count only when the role is active at login.
func TestRoleInfoRoles(t *testing.T) {
	users := [][]driver.Value{
		u("debezium", "%", 0, 0, 0, 0, 0, 0, 0, 0),
		u("cdc_role", "%", 1, 1, 1, 1, 0, 1, 1, 1), // a role is a locked account
	}
	edges := [][]driver.Value{{b("%"), b("cdc_role"), b("%"), int64(0)}}
	tables := [][]driver.Value{{b("cdc_role"), b("%"), b("shop"), b("orders")}}
	for _, tt := range []struct {
		name    string
		allOn   string
		def     int64
		granted bool
	}{
		{"not default, activate_all off", "OFF", 0, false},
		{"default role", "OFF", 1, true},
		{"activate_all on", "ON", 0, true},
		{"activate_all 1", "1", 0, true},
	} {
		e := [][]driver.Value{{edges[0][0], edges[0][1], edges[0][2], tt.def}}
		d := newFake(t, grantsServer(users, e, tt.allOn, nil, tables))
		got, err := d.RoleInfo(context.Background(), "debezium", []string{"shop.orders"})
		if err != nil {
			t.Fatal(err)
		}
		if got.Replication != tt.granted || got.Selectable["shop.orders"] != tt.granted || got.Privileges[PrivReload] != tt.granted {
			t.Errorf("%s: got %+v", tt.name, got)
		}
		if !got.CanLogin {
			t.Errorf("%s: the role's lock must not lock the user", tt.name)
		}
	}
}

// The client never selects a password hash and never runs SHOW GRANTS.
func TestRoleInfoNeverReadsPasswordHashes(t *testing.T) {
	srv := grantsServer([][]driver.Value{u("debezium", "%", 0, 0, 1, 1, 1, 1, 1, 1)},
		[][]driver.Value{{b("%"), b("r"), b("%"), int64(1)}}, "OFF", nil, nil)
	d := newFake(t, srv)
	if _, err := d.RoleInfo(context.Background(), "debezium", []string{"shop.orders"}); err != nil {
		t.Fatal(err)
	}
	for _, c := range srv.queries() {
		lq := strings.ToLower(c.query)
		if strings.Contains(lq, "authentication_string") || strings.Contains(lq, "show grants") ||
			(strings.Contains(lq, "mysql.user") && strings.Contains(lq, "*")) {
			t.Errorf("forbidden read: %s", c.query)
		}
		if strings.Contains(c.query, "debezium") {
			t.Errorf("the user name reached the statement text: %s", c.query)
		}
	}
	for _, bad := range []string{"", strings.Repeat("u", 33)} {
		if _, err := d.RoleInfo(context.Background(), bad, nil); err == nil {
			t.Errorf("user %q accepted", bad)
		}
	}
}

func TestRoleInfoPrivilegeError(t *testing.T) {
	srv := (&fakeServer{}).onErr("FROM mysql.user", &gomysql.MySQLError{Number: 1143, Message: "SELECT command denied for column 'Host'"})
	d := newFake(t, srv)
	_, err := d.RoleInfo(context.Background(), "debezium", nil)
	if !errors.Is(err, ErrPrivilege) || !strings.Contains(err.Error(), "mysql.user") {
		t.Fatalf("err = %v", err)
	}
}

func TestLikeMatch(t *testing.T) {
	tests := []struct {
		p, s string
		want bool
	}{
		{"shop", "shop", true},
		{"shop", "Shop", false},
		{"sho%", "shop", true},
		{"%", "anything", true},
		{"%", "", true},
		{"s_op", "shop", true},
		{"s_op", "sop", false},
		{`sho\_p`, "sho_p", true},
		{`sho\_p`, "shoXp", false},
		{`a\%`, "a%", true},
		{`a\%`, "ab", false},
		{"a%b%c", "aXXbYYc", true},
		{"a%b%c", "aXXbYY", false},
		{"", "", true},
		{"", "a", false},
	}
	for _, tt := range tests {
		if got := likeMatch(tt.p, tt.s); got != tt.want {
			t.Errorf("likeMatch(%q, %q) = %v", tt.p, tt.s, got)
		}
	}
}
