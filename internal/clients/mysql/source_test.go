package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

func TestOpenVersionProbe(t *testing.T) {
	tests := []struct {
		version, comment string
		ok               bool
		major, minor     int
	}{
		{"8.0.36", "MySQL Community Server - GPL", true, 8, 0},
		{"8.4.3-log", "Source distribution", true, 8, 4},
		{"9.1.0", "MySQL Community Server - GPL", true, 9, 1},
		{"8.0.mysql_aurora.3.05.2", "Source distribution", true, 8, 0},
		{"5.7.44", "MySQL Community Server (GPL)", false, 0, 0},
		{"10.11.6-MariaDB", "mariadb.org binary distribution", false, 0, 0},
		{"8.0.36", "MariaDB Server", false, 0, 0},
		{"garbage", "", false, 0, 0},
	}
	for _, tt := range tests {
		srv := (&fakeServer{}).on("SELECT VERSION()", []string{"v", "c", "ro"}, row([]byte(tt.version), []byte(tt.comment), int64(1)))
		sdb := sql.OpenDB(fakeConnector{srv: srv})
		d, err := open(context.Background(), sdb, "t")
		_ = sdb.Close()
		if tt.ok != (err == nil) {
			t.Errorf("version %q %q: err = %v, want ok=%v", tt.version, tt.comment, err, tt.ok)
			continue
		}
		if !tt.ok {
			if tt.version != "garbage" && !errors.Is(err, clients.ErrNotSupported) {
				t.Errorf("version %q: err = %v, want ErrNotSupported", tt.version, err)
			}
			continue
		}
		if d.major != tt.major || d.minor != tt.minor || d.Version() == "" {
			t.Errorf("version %q: %d.%d", tt.version, d.major, d.minor)
		}
	}
}

func TestOpenRefusesWritableSession(t *testing.T) {
	srv := (&fakeServer{}).on("SELECT VERSION()", []string{"v", "c", "ro"}, row([]byte("8.4.3"), []byte(""), int64(0)))
	sdb := sql.OpenDB(fakeConnector{srv: srv})
	defer sdb.Close()
	if _, err := open(context.Background(), sdb, "t"); err == nil || !strings.Contains(err.Error(), "not read-only") {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenPingFails(t *testing.T) {
	srv := &fakeServer{ping: errors.New("dial tcp: connection refused")}
	sdb := sql.OpenDB(fakeConnector{srv: srv})
	defer sdb.Close()
	if _, err := open(context.Background(), sdb, "t"); err == nil || !strings.Contains(err.Error(), "mysql: connect") {
		t.Fatalf("err = %v", err)
	}
}

func TestSlotsAndDDLNotSupported(t *testing.T) {
	srv := &fakeServer{}
	d := newFake(t, srv)
	n := len(srv.queries())
	if _, err := d.ReplicationSlots(context.Background()); !errors.Is(err, clients.ErrNotSupported) {
		t.Errorf("ReplicationSlots: %v", err)
	}
	if _, err := d.RecentDDL(context.Background(), time.Now(), 10); !errors.Is(err, clients.ErrNotSupported) {
		t.Errorf("RecentDDL: %v", err)
	}
	if len(srv.queries()) != n {
		t.Error("ErrNotSupported reads did I/O")
	}
}

func TestLongTransactions(t *testing.T) {
	srv := (&fakeServer{}).on("information_schema.innodb_trx x", []string{"a", "b", "c", "d", "e", "f", "g", "h"},
		row(int64(42), []byte("app"), []byte("orders-svc"), []byte("Query"), int64(900), []byte("LOCK WAIT"), []byte("  UPDATE"), int64(1)),
		row(int64(43), []byte(""), []byte(""), []byte("Sleep"), int64(600), []byte("RUNNING"), []byte(""), int64(0)),
	)
	d := newFake(t, srv)
	got, err := d.LongTransactions(context.Background(), 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	want := []clients.Transaction{
		{PID: 42, Username: "app", ApplicationName: "orders-svc", State: "Query", Age: 900 * time.Second, WaitEvent: "lock:LOCK WAIT", Query: "UPDATE", HasXID: true},
		{PID: 43, State: "Sleep", Age: 600 * time.Second},
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %+v", got)
	}
	q := srv.queries()[len(srv.queries())-1]
	if len(q.args) != 1 || q.args[0] != int64(300) {
		t.Errorf("age not bound as seconds: %v", q.args)
	}
	// Never the statement text, only its first keyword (selected in SQL).
	if !strings.Contains(q.query, "REGEXP_SUBSTR(t.PROCESSLIST_INFO") || strings.Count(q.query, "PROCESSLIST_INFO") != 1 {
		t.Errorf("long transactions select more than the verb: %s", q.query)
	}
	if _, err := d.LongTransactions(context.Background(), -time.Second); err == nil {
		t.Error("negative age accepted")
	}
}

func TestRow(t *testing.T) {
	ts := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	srv := (&fakeServer{}).onColumns(map[string][]string{"shop.orders": {"id"}}).on("SELECT * FROM `shop`.`orders` WHERE `id` = ? LIMIT 1",
		[]string{"id", "note", "blob", "amount", "ratio", "created_at", "gone"},
		row(int64(7), []byte("héllo"), []byte{0xff, 0xfe, 0x00}, []byte("12.50"), float64(0.25), ts, nil))
	d := newFake(t, srv)
	m, ok, err := d.Row(context.Background(), "shop.orders", "id", "7")
	if err != nil || !ok {
		t.Fatalf("Row: %v %v", ok, err)
	}
	want := map[string]any{"id": "7", "note": "héllo", "blob": "<binary 3 bytes>", "amount": "12.50", "ratio": "0.25", "created_at": "2026-10-01T12:00:00Z", "gone": nil}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s = %#v, want %#v", k, m[k], v)
		}
	}
	q := srv.queries()[len(srv.queries())-1]
	if len(q.args) != 1 || q.args[0] != "7" {
		t.Errorf("pk not bound: %v", q.args)
	}
}

func TestRowMissingAndErrors(t *testing.T) {
	srv := (&fakeServer{}).
		onColumns(map[string][]string{"shop.orders": {"id"}, "shop.secret": {"id"}, "shop.gone": {"id"}}).
		on("`shop`.`orders`", []string{"id"}).
		onErr("`shop`.`secret`", &gomysql.MySQLError{Number: 1142, Message: "SELECT command denied"}).
		// Dropped between the lookup and the read.
		onErr("`shop`.`gone`", &gomysql.MySQLError{Number: 1146, Message: "Table 'shop.gone' doesn't exist"})
	d := newFake(t, srv)
	if _, ok, err := d.Row(context.Background(), "shop.orders", "id", "1"); ok || err != nil {
		t.Errorf("missing row: ok=%v err=%v", ok, err)
	}
	if _, _, err := d.Row(context.Background(), "shop.secret", "id", "1"); !errors.Is(err, ErrPrivilege) || !strings.Contains(err.Error(), "SELECT on shop.secret") {
		t.Errorf("denied: %v", err)
	}
	if _, _, err := d.Row(context.Background(), "shop.nope", "id", "1"); !errors.Is(err, clients.ErrNotFound) || !strings.Contains(err.Error(), "table missing") {
		t.Errorf("no such table: %v", err)
	}
	if _, _, err := d.Row(context.Background(), "shop.gone", "id", "1"); !errors.Is(err, clients.ErrNotFound) {
		t.Errorf("table dropped after lookup: %v", err)
	}
	for _, q := range srv.queries() {
		if strings.Contains(q.query, "`nope`") {
			t.Errorf("a missing table was queried: %s", q.query)
		}
	}
}

// An unknown column is clients.ErrNotFound naming the column, and no row is
// read.
func TestRowUnknownColumn(t *testing.T) {
	srv := (&fakeServer{}).
		onColumns(map[string][]string{"shop.orders": {"id", "note"}}).
		on("`shop`.`orders`", []string{"id"})
	d := newFake(t, srv)
	_, ok, err := d.Row(context.Background(), "shop.orders", "order_id", "1")
	if ok || !errors.Is(err, clients.ErrNotFound) || !strings.Contains(err.Error(), `column "order_id"`) {
		t.Fatalf("unknown column: ok=%v err=%v", ok, err)
	}
	for _, q := range srv.queries() {
		if strings.Contains(q.query, "SELECT * FROM") {
			t.Errorf("row read for an unknown column: %s", q.query)
		}
	}
}

// A refused dictionary lookup takes the ErrPrivilege path.
func TestRowLookupRefused(t *testing.T) {
	srv := (&fakeServer{}).
		onErr("information_schema.COLUMNS", &gomysql.MySQLError{Number: 1044, Message: "Access denied for user to database 'information_schema'"}).
		on("`shop`.`orders`", []string{"id"})
	d := newFake(t, srv)
	_, _, err := d.Row(context.Background(), "shop.orders", "id", "1")
	if !errors.Is(err, ErrPrivilege) || !strings.Contains(err.Error(), "SELECT on shop.orders") {
		t.Fatalf("refused lookup: %v", err)
	}
	if q := srv.queries(); strings.Contains(q[len(q)-1].query, "SELECT * FROM") {
		t.Errorf("row read after a refused lookup: %s", q[len(q)-1].query)
	}
}

// The SELECT is built from the names the server returns, and the lookup binds
// the caller's names as parameters.
func TestRowUsesServerNames(t *testing.T) {
	srv := (&fakeServer{}).
		onColumns(map[string][]string{"shop.orders": {"id"}}).
		on("SELECT * FROM `shop`.`orders` WHERE `id` = ? LIMIT 1", []string{"id"}, row(int64(7)))
	d := newFake(t, srv)
	if _, ok, err := d.Row(context.Background(), "Shop.ORDERS", "ID", "7"); err != nil || !ok {
		t.Fatalf("Row: ok=%v err=%v", ok, err)
	}
	qs := srv.queries()
	lookup, sel := qs[len(qs)-2], qs[len(qs)-1]
	if !strings.Contains(lookup.query, "information_schema.COLUMNS") || len(lookup.args) != 3 ||
		lookup.args[0] != "ID" || lookup.args[1] != "Shop" || lookup.args[2] != "ORDERS" {
		t.Errorf("lookup = %+v", lookup)
	}
	if strings.Contains(lookup.query, "Shop") || strings.Contains(lookup.query, "ORDERS") {
		t.Errorf("caller names spliced into the lookup: %s", lookup.query)
	}
	if !strings.Contains(sel.query, "`shop`.`orders` WHERE `id` = ?") || strings.Contains(sel.query, "Shop") {
		t.Errorf("select not built from the server's names: %s", sel.query)
	}
}

// A dictionary name outside the identifier rule is refused, never quoted.
func TestRowRefusesBadServerName(t *testing.T) {
	srv := (&fakeServer{}).
		onFunc("information_schema.COLUMNS", func([]driver.NamedValue) result {
			return result{cols: []string{"a", "b", "c", "d"}, rows: [][]driver.Value{row([]byte("shop"), []byte("ord`ers"), []byte("id"), int64(1))}}
		})
	d := newFake(t, srv)
	if _, _, err := d.Row(context.Background(), "shop.orders", "id", "1"); err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("bad server name: %v", err)
	}
	for _, q := range srv.queries() {
		if strings.Contains(q.query, "SELECT * FROM") {
			t.Errorf("queried with a bad server name: %s", q.query)
		}
	}
}

// Identifiers are checked before any I/O (rule 6, design §5.6).
func TestIdentifiersRefusedBeforeIO(t *testing.T) {
	srv := &fakeServer{}
	d := newFake(t, srv)
	n := len(srv.queries())
	bad := []string{"orders", "shop.orders.x", "shop.ord`ers", "shop.ord ers", "1shop.orders", "shop." + strings.Repeat("t", 65), ".orders", "shop.", "sh-op.orders", "shop.orders;DROP"}
	ctx := context.Background()
	for _, tbl := range bad {
		if _, _, err := d.Row(ctx, tbl, "id", "1"); err == nil {
			t.Errorf("Row(%q) accepted", tbl)
		}
		if _, err := d.TableReplication(ctx, []string{tbl}); err == nil {
			t.Errorf("TableReplication(%q) accepted", tbl)
		}
		if _, err := d.RoleInfo(ctx, "debezium", []string{"shop.orders", tbl}); err == nil {
			t.Errorf("RoleInfo(%q) accepted", tbl)
		}
		if _, err := d.BlockersOnTables(ctx, time.Minute, []string{tbl}); err == nil {
			t.Errorf("BlockersOnTables(%q) accepted", tbl)
		}
	}
	for _, col := range []string{"", "id`", "id = 1 OR 1", "1id", strings.Repeat("c", 65)} {
		if _, _, err := d.Row(ctx, "shop.orders", col, "1"); err == nil {
			t.Errorf("Row column %q accepted", col)
		}
	}
	if got := len(srv.queries()); got != n {
		t.Fatalf("%d statements ran for refused identifiers", got-n)
	}
	if _, err := d.TableReplication(ctx, make([]string, maxTables+1)); err == nil {
		t.Error("too many tables accepted")
	}
}

func TestDeadlineAndServerTimeout(t *testing.T) {
	srv := &fakeServer{}
	d := newFake(t, srv)
	srv.mu.Lock()
	srv.block = true
	srv.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := d.LongTransactions(ctx, time.Minute)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "timed out after 10 s") {
		t.Fatalf("err = %v", err)
	}

	srv2 := (&fakeServer{}).onErr("innodb_trx", &gomysql.MySQLError{Number: 3024, Message: "Query execution was interrupted, maximum statement execution time exceeded"})
	d2 := newFake(t, srv2)
	if _, err := d2.LongTransactions(context.Background(), time.Minute); err == nil || !strings.Contains(err.Error(), "max_execution_time") {
		t.Fatalf("3024: %v", err)
	}
}

// The 10 s deadline is applied even when the caller's context has none.
func TestCallsCarryADeadline(t *testing.T) {
	srv := (&fakeServer{}).
		on("innodb_trx", []string{"a"}).
		on("global_variables", []string{"a", "b", "c"}).
		on("rds_show_configuration", []string{"name", "value"}).
		on("information_schema.TABLES", []string{"a", "b", "c", "d"}).
		on("FROM mysql.user", []string{"a"}).
		on("@@global.gtid_mode", []string{"a", "b", "c", "d", "e", "f"}, row([]byte("ON"), []byte(""), []byte(""), int64(1), []byte("u"), int64(0))).
		on("SHOW BINARY LOG STATUS", []string{"File", "Position"}).
		on("SHOW BINARY LOGS", []string{"Log_name", "File_size"}).
		onColumns(map[string][]string{"shop.orders": {"id"}}).
		on("`shop`.`orders`", []string{"id"})
	d := newFake(t, srv)
	ctx := context.Background()
	_, _ = d.LongTransactions(ctx, time.Minute)
	_, _ = d.Blockers(ctx, time.Minute)
	_, _ = d.Settings(ctx)
	_, _ = d.TableReplication(ctx, []string{"shop.orders"})
	_, _ = d.RoleInfo(ctx, "debezium", nil)
	_, _ = d.BinlogStatus(ctx)
	_, _, _ = d.Row(ctx, "shop.orders", "id", "1")
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.deadlines) < 10 {
		t.Fatalf("only %d statements ran", len(srv.deadlines))
	}
	for i, dl := range srv.deadlines {
		if dl <= 0 || dl > callTimeout {
			t.Errorf("statement %d (%s): deadline in %v, want within %v", i, firstLine(srv.calls[i].query), dl, callTimeout)
		}
	}
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func TestProvenanceRecorded(t *testing.T) {
	srv := (&fakeServer{}).onColumns(map[string][]string{"shop.orders": {"id"}}).on("`shop`.`orders`", []string{"id"}, row(int64(1)))
	d := newFake(t, srv)
	rec := tools.NewRecorder()
	ctx := tools.WithRecorder(context.Background(), rec)
	if _, _, err := d.Row(ctx, "shop.orders", "id", "1"); err != nil {
		t.Fatal(err)
	}
	calls := rec.Calls()
	if len(calls) != 2 {
		t.Fatalf("calls = %+v", calls)
	}
	if l := calls[0]; !strings.Contains(l.Text, "information_schema.COLUMNS") || len(l.ParamsRedacted) != 3 || l.ParamsRedacted[1] != "?2=shop" {
		t.Errorf("lookup call = %+v", l)
	}
	c := calls[1]
	if c.Kind != tools.CallSQL || c.Target != "mysql shop@db:3306" || !strings.Contains(c.Text, "WHERE `id` = ?") || len(c.ParamsRedacted) != 1 || c.ParamsRedacted[0] != "?1=1" {
		t.Fatalf("call = %+v", c)
	}
}

func TestRedactSessionQuery(t *testing.T) {
	q := RedactSessionQuery("UPDATE users SET password='hunter2' WHERE id=1 " + strings.Repeat("x", 400))
	if len(q) > MaxSessionQueryBytes || strings.Contains(q, "hunter2") {
		t.Fatalf("q = %q (%d bytes)", q, len(q))
	}
	if q := RedactSessionQuery(strings.Repeat("é", 300)); len(q) > MaxSessionQueryBytes || !strings.HasSuffix(q, "…") || !isValidUTF8(q) {
		t.Fatalf("cut splits a rune: %q", q)
	}
	if q := RedactSessionQuery("SELECT \xff"); !isValidUTF8(q) {
		t.Fatalf("invalid UTF-8 kept: %q", q)
	}
}

func isValidUTF8(s string) bool { return strings.ToValidUTF8(s, "") == s }

func TestFailMapping(t *testing.T) {
	for _, n := range []uint16{1044, 1142, 1143, 1227} {
		err := fail("read x", "PROCESS", &gomysql.MySQLError{Number: n})
		if !errors.Is(err, ErrPrivilege) || !strings.Contains(err.Error(), "needs PROCESS") {
			t.Errorf("%d: %v", n, err)
		}
		var me *gomysql.MySQLError
		if !errors.As(err, &me) || me.Number != n {
			t.Errorf("%d: the driver error is not kept in the chain", n)
		}
	}
	if err := fail("read x", "", &gomysql.MySQLError{Number: 1146}); !errors.Is(err, clients.ErrNotFound) {
		t.Errorf("1146: %v", err)
	}
	if err := fail("read x", "", errors.New("boom")); err.Error() != "mysql: read x: boom" {
		t.Errorf("plain: %v", err)
	}
}

func TestDriverLoggerRedacts(t *testing.T) {
	var buf strings.Builder
	l := newTestLogger(&buf)
	driverLogger{l: l}.Print("packets.go:58 unexpected EOF password=hunter2 ", strings.Repeat("y", 500))
	out := buf.String()
	if strings.Contains(out, "hunter2") || strings.Count(out, "y") > 200 {
		t.Fatalf("log = %s", out)
	}
}

func TestToJSON(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 600, time.FixedZone("x", 3600))
	tests := []struct {
		in   any
		want any
	}{
		{nil, nil},
		{[]byte("ok"), "ok"},
		{[]byte{0xc3}, "<binary 1 bytes>"},
		{sql.RawBytes("raw"), "raw"},
		{"s", "s"},
		{int64(-9007199254740993), "-9007199254740993"},
		{uint64(18446744073709551615), "18446744073709551615"},
		{int32(-3), "-3"}, {uint32(3), "3"}, {int16(-2), "-2"}, {uint16(2), "2"},
		{int8(-1), "-1"}, {uint8(1), "1"}, {int(5), "5"},
		{float64(1e21), "1000000000000000000000"}, {float32(0.5), "0.5"},
		{true, true},
		{ts, "2026-01-02T02:04:05.0000006Z"},
		{struct{ A int }{1}, "{1}"},
	}
	for _, tt := range tests {
		if got := toJSON(tt.in); got != tt.want {
			t.Errorf("toJSON(%#v) = %#v, want %#v", tt.in, got, tt.want)
		}
	}
}

func TestSmallHelpers(t *testing.T) {
	var o options
	WithLogger(nil)(&o)
	if o.logger != nil {
		t.Error("WithLogger(nil) set a logger")
	}
	WithLogger(newTestLogger(io.Discard))(&o)
	if o.logger == nil {
		t.Error("WithLogger did not set the logger")
	}
	if n := SettingNames(); len(n) != len(settingNames) || &n[0] == &settingNames[0] {
		t.Error("SettingNames must return a copy")
	}
	var d *DB
	d.Close() // nil-safe
	newFake(t, &fakeServer{}).Close()
	for _, tt := range []struct {
		in   any
		want string
	}{{nil, "NULL"}, {"x", "x"}, {[]byte("pw"), "<2 bytes>"}, {7, "7"}, {strings.Repeat("a", 70), strings.Repeat("a", 64)}} {
		if got := renderArg(tt.in); got != tt.want {
			t.Errorf("renderArg(%v) = %q", tt.in, got)
		}
	}
}
