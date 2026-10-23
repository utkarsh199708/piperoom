package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

func newFakeWriter(t *testing.T, srv *fakeServer, protected ...string) *Writer {
	t.Helper()
	srv.routes = append([]route{{substr: "SELECT VERSION()", res: result{
		cols: []string{"VERSION()", "@@version_comment", "@@session.transaction_read_only"},
		rows: [][]driver.Value{{[]byte("8.4.3"), []byte("MySQL Community Server - GPL"), int64(0)}},
	}}}, srv.routes...)
	sdb := sql.OpenDB(fakeConnector{srv: srv})
	t.Cleanup(func() { _ = sdb.Close() })
	w, err := openWriter(context.Background(), sdb, "mysql shop@db:3306", append([]string{"piperoom_write"}, protected...))
	if err != nil {
		t.Fatalf("openWriter: %v", err)
	}
	return w
}

var mysqlPipeline = owners.Pipeline{
	ID:           "orders-mysql",
	SourceKind:   owners.SourceKindMySQL,
	SourceTables: []string{"shop.orders", "shop.order_items"},
	Connectors:   []string{"orders-mysql"},
}

func scopedCtx() context.Context { return tools.WithPipeline(context.Background(), mysqlPipeline) }

func myErr(n uint16) error { return &gomysql.MySQLError{Number: n, Message: "server says no"} }

// writes returns the statements the writer ran after the open probe.
func writes(srv *fakeServer) []call {
	var out []call
	for _, c := range srv.queries() {
		if !strings.Contains(c.query, "SELECT VERSION()") {
			out = append(out, c)
		}
	}
	return out
}

func TestNewWriterRefusals(t *testing.T) {
	if _, err := NewWriter(context.Background(), ""); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("empty URL: %v", err)
	}
	if _, err := NewWriter(context.Background(), "mysql://u:p@db.example/shop?allowAllFiles=true"); !errors.Is(err, ErrBadURL) {
		t.Fatalf("refused parameter: %v", err)
	}
	var w *Writer
	ctx := scopedCtx()
	for name, err := range map[string]error{
		"grant":     w.GrantSelect(ctx, "debezium", []string{"shop.orders"}),
		"heartbeat": w.CreateHeartbeatTable(ctx, "shop.debezium_heartbeat"),
		"end":       w.EndSession(ctx, 1, time.Now()),
	} {
		if !errors.Is(err, ErrNoWriter) {
			t.Errorf("nil writer %s: %v", name, err)
		}
	}

	// A read-only write session (a replica, a proxy) is refused at start.
	srv := (&fakeServer{}).on("SELECT VERSION()", []string{"v", "c", "ro"}, row([]byte("8.4.3"), []byte("x"), int64(1)))
	sdb := sql.OpenDB(fakeConnector{srv: srv})
	defer sdb.Close()
	if _, err := openWriter(context.Background(), sdb, "t", nil); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("read-only session: %v", err)
	}
}

func TestSetReplicaIdentityNotSupported(t *testing.T) {
	srv := &fakeServer{}
	w := newFakeWriter(t, srv)
	if err := w.SetReplicaIdentity(scopedCtx(), "shop.orders", "full", ""); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("err %v", err)
	}
	if n := len(writes(srv)); n != 0 {
		t.Fatalf("%d statements", n)
	}
}

func TestWriterScopeBeforeIO(t *testing.T) {
	pg := owners.Pipeline{ID: "orders", SourceTables: []string{"shop.orders"}}
	cases := []struct {
		name string
		ctx  context.Context
		call func(w *Writer, ctx context.Context) error
		want error
	}{
		{"grant without scope", context.Background(), func(w *Writer, ctx context.Context) error {
			return w.GrantSelect(ctx, "debezium", []string{"shop.orders"})
		}, tools.ErrNoScope},
		{"grant on a postgres pipeline", tools.WithPipeline(context.Background(), pg), func(w *Writer, ctx context.Context) error {
			return w.GrantSelect(ctx, "debezium", []string{"shop.orders"})
		}, tools.ErrOutOfScope},
		{"grant outside the pipeline", scopedCtx(), func(w *Writer, ctx context.Context) error {
			return w.GrantSelect(ctx, "debezium", []string{"shop.orders", "shop.payments"})
		}, tools.ErrOutOfScope},
		{"end session without scope", context.Background(), func(w *Writer, ctx context.Context) error {
			return w.EndSession(ctx, 42, time.Unix(1000, 0))
		}, tools.ErrNoScope},
		{"end session on a postgres pipeline", tools.WithPipeline(context.Background(), pg), func(w *Writer, ctx context.Context) error {
			return w.EndSession(ctx, 42, time.Unix(1000, 0))
		}, tools.ErrOutOfScope},
		{"heartbeat in another database", scopedCtx(), func(w *Writer, ctx context.Context) error {
			return w.CreateHeartbeatTable(ctx, "other.debezium_heartbeat")
		}, tools.ErrOutOfScope},
		{"heartbeat over a source table", tools.WithPipeline(context.Background(), owners.Pipeline{ID: "p", SourceKind: "mysql", SourceTables: []string{"shop.heartbeat"}}), func(w *Writer, ctx context.Context) error {
			return w.CreateHeartbeatTable(ctx, "shop.heartbeat")
		}, tools.ErrOutOfScope},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := &fakeServer{}
			w := newFakeWriter(t, srv)
			if err := tc.call(w, tc.ctx); !errors.Is(err, tc.want) {
				t.Fatalf("err %v, want %v", err, tc.want)
			}
			if n := len(writes(srv)); n != 0 {
				t.Fatalf("%d statements before the refusal", n)
			}
		})
	}
}

func accounts(hosts ...string) *fakeServer {
	srv := &fakeServer{}
	rows := make([][]driver.Value, len(hosts))
	for i, h := range hosts {
		rows[i] = row([]byte(h))
	}
	return srv.on("FROM mysql.user", []string{"Host"}, rows...)
}

func TestGrantSelect(t *testing.T) {
	srv := accounts("10.0.%").on("GRANT SELECT", nil)
	w := newFakeWriter(t, srv)
	if err := w.GrantSelect(scopedCtx(), "debezium", []string{"shop.orders", "shop.order_items", "shop.orders"}); err != nil {
		t.Fatal(err)
	}
	got := writes(srv)
	if len(got) != 3 {
		t.Fatalf("statements %+v", got)
	}
	if got[0].query != accountSQL || len(got[0].args) != 1 || got[0].args[0] != "debezium" {
		t.Fatalf("account lookup %+v: the user must be bound", got[0])
	}
	want := []string{
		"GRANT SELECT ON `shop`.`orders` TO 'debezium'@'10.0.%'",
		"GRANT SELECT ON `shop`.`order_items` TO 'debezium'@'10.0.%'",
	}
	for i, w := range want {
		c := got[i+1]
		if c.query != w || !c.exec || len(c.args) != 0 {
			t.Fatalf("statement %d = %+v, want %q", i+1, c, w)
		}
		if strings.Contains(strings.ToUpper(c.query), "GRANT OPTION") || strings.Contains(c.query, ",") {
			t.Fatalf("grant widened: %q", c.query)
		}
	}
	if got[0].conn != got[1].conn || got[1].conn != got[2].conn {
		t.Fatal("the account was resolved on another connection than the grants")
	}
}

func TestGrantSelectRefusals(t *testing.T) {
	cases := []struct {
		name    string
		srv     *fakeServer
		role    string
		tables  []string
		want    error
		text    string
		noGrant bool
	}{
		{name: "two accounts", srv: accounts("%", "10.0.%"), role: "debezium", tables: []string{"shop.orders"}, want: ErrAmbiguousAccount, text: "debezium@% and debezium@10.0.%", noGrant: true},
		{name: "no account", srv: accounts(), role: "debezium", tables: []string{"shop.orders"}, want: clients.ErrNotFound, noGrant: true},
		{name: "host with a quote", srv: accounts("x'y"), role: "debezium", tables: []string{"shop.orders"}, text: "not a plain host name", noGrant: true},
		{name: "no grant option", srv: accounts("%").onErr("GRANT SELECT", myErr(1142)), role: "debezium", tables: []string{"shop.orders"}, text: "needs SELECT WITH GRANT OPTION on shop.orders"},
		{name: "missing table", srv: accounts("%").onErr("GRANT SELECT", myErr(1146)), role: "debezium", tables: []string{"shop.orders"}, want: clients.ErrNotFound},
		{name: "account read denied", srv: (&fakeServer{}).onErr("FROM mysql.user", myErr(1142)), role: "debezium", tables: []string{"shop.orders"}, text: "SELECT on mysql.user", noGrant: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newFakeWriter(t, tc.srv)
			err := w.GrantSelect(scopedCtx(), tc.role, tc.tables)
			if tc.want == nil && tc.text == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err %v, want %v", err, tc.want)
			}
			if tc.text != "" && (err == nil || !strings.Contains(err.Error(), tc.text)) {
				t.Fatalf("err %v, want %q", err, tc.text)
			}
			if tc.noGrant {
				for _, c := range writes(tc.srv) {
					if strings.HasPrefix(c.query, "GRANT") {
						t.Fatalf("granted: %q", c.query)
					}
				}
			}
		})
	}

	// The second table fails: the error names the first, which stays.
	n := 0
	srv := accounts("%").onFunc("GRANT SELECT", func([]driver.NamedValue) result {
		n++
		if n == 2 {
			return result{err: myErr(1146)}
		}
		return result{}
	})
	w := newFakeWriter(t, srv)
	err := w.GrantSelect(scopedCtx(), "debezium", []string{"shop.orders", "shop.order_items"})
	if err == nil || !strings.Contains(err.Error(), "already granted on shop.orders") {
		t.Fatalf("partial: %v", err)
	}

	// Invalid names are refused before any I/O.
	for _, role := range []string{"", "deb'ezium", "debezium@%", strings.Repeat("a", 33), "x y"} {
		srv := &fakeServer{}
		w := newFakeWriter(t, srv)
		if err := w.GrantSelect(scopedCtx(), role, []string{"shop.orders"}); err == nil || len(writes(srv)) != 0 {
			t.Errorf("role %q: err %v, %d statements", role, err, len(writes(srv)))
		}
	}
	for _, tables := range [][]string{nil, {"orders"}, {"shop.or`ders"}, {"shop.orders; DROP"}} {
		srv := &fakeServer{}
		w := newFakeWriter(t, srv)
		if err := w.GrantSelect(scopedCtx(), "debezium", tables); err == nil || len(writes(srv)) != 0 {
			t.Errorf("tables %q: err %v", tables, err)
		}
	}
}

func TestCreateHeartbeatTable(t *testing.T) {
	srv := (&fakeServer{}).on("CREATE TABLE", nil)
	w := newFakeWriter(t, srv)
	if err := w.CreateHeartbeatTable(scopedCtx(), "shop.debezium_heartbeat"); err != nil {
		t.Fatal(err)
	}
	got := writes(srv)
	want := "CREATE TABLE IF NOT EXISTS `shop`.`debezium_heartbeat` (id INT NOT NULL PRIMARY KEY, ts TIMESTAMP(6) NOT NULL) ENGINE=InnoDB"
	if len(got) != 1 || got[0].query != want || !got[0].exec {
		t.Fatalf("statements %+v", got)
	}

	for _, table := range []string{"shop.orders_copy", "shop", "shop.heart`beat"} {
		srv := &fakeServer{}
		w := newFakeWriter(t, srv)
		if err := w.CreateHeartbeatTable(scopedCtx(), table); err == nil || len(writes(srv)) != 0 {
			t.Errorf("%q: err %v", table, err)
		}
	}
	srv = (&fakeServer{}).onErr("CREATE TABLE", myErr(1049))
	w = newFakeWriter(t, srv)
	if err := w.CreateHeartbeatTable(tools.WithUnscoped(context.Background()), "nodb.heartbeat"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing database: %v", err)
	}
	srv = (&fakeServer{}).onErr("CREATE TABLE", myErr(1142))
	w = newFakeWriter(t, srv)
	if err := w.CreateHeartbeatTable(scopedCtx(), "shop.heartbeat"); err == nil || !strings.Contains(err.Error(), "needs CREATE on database shop") {
		t.Fatalf("denied: %v", err)
	}
}

// sess is a sessionSQL row.
type sess struct {
	self                                   bool
	user, host, command, typ, program, cur string
	trx                                    int64
}

func (s sess) values() []driver.Value {
	self := int64(0)
	if s.self {
		self = 1
	}
	return row(self, []byte(s.user), []byte(s.host), []byte(s.command), []byte(s.typ), []byte(s.program), s.trx, []byte(s.cur))
}

var sessionCols = []string{"self", "user", "host", "command", "type", "program", "trx", "current_user"}

func appSession() sess {
	return sess{user: "app", host: "10.0.0.7", command: "Sleep", typ: "FOREGROUND", program: "orders-svc", cur: "piperoom_write", trx: 1_700_000_000}
}

func onSession(srv *fakeServer, s ...sess) *fakeServer {
	i := 0
	return srv.onFunc("SUBSTRING_INDEX(CURRENT_USER()", func([]driver.NamedValue) result {
		cur := s[min(i, len(s)-1)]
		i++
		return result{cols: sessionCols, rows: [][]driver.Value{cur.values()}}
	})
}

func TestEndSession(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	srv := onSession(&fakeServer{}, appSession()).on("KILL CONNECTION", nil)
	w := newFakeWriter(t, srv)
	if err := w.EndSession(scopedCtx(), 42, started); err != nil {
		t.Fatal(err)
	}
	got := writes(srv)
	if len(got) != 2 || got[1].query != "KILL CONNECTION 42" || !got[1].exec {
		t.Fatalf("statements %+v", got)
	}
	if len(got[0].args) != 1 || got[0].args[0] != int64(42) {
		t.Fatalf("the pid must be bound in the re-read: %+v", got[0])
	}
	if got[0].conn != got[1].conn {
		t.Fatal("re-read and KILL ran on different connections")
	}
}

func TestEndSessionRefusals(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	with := func(f func(*sess)) sess {
		s := appSession()
		f(&s)
		return s
	}
	cases := []struct {
		name string
		srv  *fakeServer
		want error
		text string
	}{
		{"gone", (&fakeServer{}).on("SUBSTRING_INDEX(CURRENT_USER()", sessionCols), clients.ErrSessionChanged, "no session"},
		{"no open transaction", onSession(&fakeServer{}, with(func(s *sess) { s.trx = 0 })), clients.ErrSessionChanged, "no open transaction"},
		{"another transaction", onSession(&fakeServer{}, with(func(s *sess) { s.trx++ })), clients.ErrSessionChanged, "started"},
		{"this connection", onSession(&fakeServer{}, with(func(s *sess) { s.self = true })), clients.ErrSessionProtected, "this write connection"},
		{"background thread", onSession(&fakeServer{}, with(func(s *sess) { s.typ = "BACKGROUND" })), clients.ErrSessionProtected, "background"},
		{"binlog dump", onSession(&fakeServer{}, with(func(s *sess) { s.command = "Binlog Dump" })), clients.ErrSessionProtected, "binlog dump"},
		{"binlog dump gtid", onSession(&fakeServer{}, with(func(s *sess) { s.command = "Binlog Dump GTID" })), clients.ErrSessionProtected, "binlog dump"},
		{"system user", onSession(&fakeServer{}, with(func(s *sess) { s.user = "system user" })), clients.ErrSessionProtected, "server thread"},
		{"event scheduler", onSession(&fakeServer{}, with(func(s *sess) { s.user, s.command = "event_scheduler", "Daemon" })), clients.ErrSessionProtected, "server thread"},
		{"write role", onSession(&fakeServer{}, with(func(s *sess) { s.user = "piperoom_write" })), clients.ErrSessionProtected, "write role"},
		{"reader user", onSession(&fakeServer{}, with(func(s *sess) { s.user = "piperoom_read" })), ErrSessionProtected, "Piperoom's user"},
		{"piperoom program", onSession(&fakeServer{}, with(func(s *sess) { s.program = "Piperoom-Reader" })), ErrSessionProtected, "program_name"},
		{"killed in between", onSession(&fakeServer{}, appSession()).onErr("KILL CONNECTION", myErr(1094)), ErrSessionChanged, "gone"},
		{"kill denied off RDS", onSession(&fakeServer{}, appSession()).onErr("KILL CONNECTION", myErr(1095)).on("@@basedir", []string{"b"}, row([]byte("/usr/"))), nil, "lacks CONNECTION_ADMIN"},
		{"rds identity changed", onSession(&fakeServer{}, appSession(), with(func(s *sess) { s.host = "10.0.0.8" })).onErr("KILL CONNECTION", myErr(1095)).on("@@basedir", []string{"b"}, row([]byte("/rdsdbbin/mysql-8.0.35/"))), ErrSessionChanged, "between the reads"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newFakeWriter(t, tc.srv, "piperoom_read")
			err := w.EndSession(scopedCtx(), 42, started)
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err %v, want %v", err, tc.want)
			}
			if err == nil || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("err %v, want %q", err, tc.text)
			}
			for _, c := range writes(tc.srv) {
				if strings.Contains(c.query, "rds_kill") || (strings.HasPrefix(c.query, "KILL") && tc.want == ErrSessionProtected) {
					t.Fatalf("killed: %q", c.query)
				}
			}
		})
	}

	// Refused before any I/O.
	for name, call := range map[string]func(w *Writer) error{
		"zero startedAt": func(w *Writer) error { return w.EndSession(scopedCtx(), 42, time.Time{}) },
		"zero pid":       func(w *Writer) error { return w.EndSession(scopedCtx(), 0, started) },
		"negative pid":   func(w *Writer) error { return w.EndSession(scopedCtx(), -1, started) },
	} {
		srv := &fakeServer{}
		w := newFakeWriter(t, srv)
		if err := call(w); err == nil || len(writes(srv)) != 0 {
			t.Errorf("%s: err %v, %d statements", name, err, len(writes(srv)))
		}
	}
}

func TestEndSessionRDSFallback(t *testing.T) {
	srv := onSession(&fakeServer{}, appSession()).
		onErr("KILL CONNECTION", myErr(1095)).
		on("@@basedir", []string{"b"}, row([]byte("/rdsdbbin/mysql-8.0.35/"))).
		on("mysql.rds_kill", nil)
	w := newFakeWriter(t, srv)
	if err := w.EndSession(scopedCtx(), 42, time.Unix(1_700_000_000, 0)); err != nil {
		t.Fatal(err)
	}
	got := writes(srv)
	last := got[len(got)-1]
	if last.query != rdsKillSQL || len(last.args) != 1 || last.args[0] != int64(42) {
		t.Fatalf("rds_kill %+v: the id must be bound", last)
	}
	reads := 0
	for _, c := range got {
		if c.conn != got[0].conn {
			t.Fatal("the fallback left the pinned connection")
		}
		if strings.Contains(c.query, "SUBSTRING_INDEX(CURRENT_USER()") {
			reads++
		}
	}
	if reads != 2 {
		t.Fatalf("identity read %d times, want 2 (before KILL and before rds_kill)", reads)
	}
}

func TestEndSessionDeadline(t *testing.T) {
	srv := &fakeServer{}
	w := newFakeWriter(t, srv)
	srv.mu.Lock()
	srv.block = true
	srv.mu.Unlock()
	ctx, cancel := context.WithTimeout(scopedCtx(), 50*time.Millisecond)
	defer cancel()
	if err := w.EndSession(ctx, 42, time.Unix(1, 0)); err == nil {
		t.Fatal("no error after the deadline")
	}
}
