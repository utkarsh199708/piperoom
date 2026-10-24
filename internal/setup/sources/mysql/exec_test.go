package mysql

import (
	"context"
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

func unscoped() context.Context { return tools.WithUnscoped(context.Background()) }

// newTestWriter builds a SetupWriter over a fake server that is a primary.
func newTestWriter(t *testing.T, srv *fakeServer, store SecretStore, opts ...SetupWriterOption) *SetupWriter {
	t.Helper()
	writableRoute(srv)
	opts = append(opts, WithSetupSecretStore(store))
	w, err := NewSetupWriter(context.Background(), srv.open(t), opts...)
	if err != nil {
		t.Fatalf("NewSetupWriter: %v", err)
	}
	return w
}

func mysqlErr(n uint16) error { return &gomysql.MySQLError{Number: n, Message: "server said no"} }

func TestSetupWriterCreatesCanaryAndInserts(t *testing.T) {
	srv := &fakeServer{}
	w := newTestWriter(t, srv, nil)
	if err := w.CreateCanaryTable(unscoped(), "shop.it_canary"); err != nil {
		t.Fatalf("CreateCanaryTable: %v", err)
	}
	if err := w.InsertCanary(unscoped(), "shop.it_canary", goodNonce); err != nil {
		t.Fatalf("InsertCanary: %v", err)
	}
	calls := srv.recorded()
	ddl, ins := calls[len(calls)-2], calls[len(calls)-1]
	if !strings.HasPrefix(ddl.query, "CREATE TABLE IF NOT EXISTS `shop`.`it_canary`") {
		t.Errorf("DDL = %s", ddl.query)
	}
	if ins.query != "INSERT INTO `shop`.`it_canary` (nonce) VALUES (?)" || len(ins.args) != 1 || ins.args[0] != goodNonce {
		t.Errorf("insert = %q args %v: the nonce must be a bound parameter", ins.query, ins.args)
	}
	if strings.Contains(ins.query, goodNonce) {
		t.Error("the nonce is in the statement text")
	}
	for _, c := range calls {
		if c.left < 0 || c.left > 10*time.Second {
			t.Errorf("statement %q ran with deadline %v, want within 10 s", c.query, c.left)
		}
	}
}

func TestSetupWriterNotSupported(t *testing.T) {
	srv := &fakeServer{}
	w := newTestWriter(t, srv, nil)
	before := len(srv.recorded())
	if err := w.CreatePublication(unscoped(), "p", []string{"shop.orders"}); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("CreatePublication = %v", err)
	}
	if _, err := w.AlterSystem(unscoped(), "binlog_format", "ROW"); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("AlterSystem = %v", err)
	}
	if got := len(srv.recorded()); got != before {
		t.Fatalf("a not-supported call issued %d statements", got-before)
	}
	for _, s := range srv.texts() {
		if strings.Contains(strings.ToUpper(s), "PERSIST") {
			t.Fatalf("a SET PERSIST was issued: %s", s)
		}
	}
}

func TestSetupWriterCreateReadRole(t *testing.T) {
	srv := &fakeServer{}
	store := &memStore{}
	w := newTestWriter(t, srv, store)
	name, err := w.CreateReadRole(unscoped(), "debezium", []string{"shop.orders", "shop.order_items"})
	if err != nil {
		t.Fatalf("CreateReadRole: %v", err)
	}
	if name != readPasswordSecretName {
		t.Fatalf("secret name = %q", name)
	}
	password := store.values[readPasswordSecretName]
	if len(password) != 48 {
		t.Fatalf("stored password has %d characters, want 48 hex", len(password))
	}
	texts := srv.texts()
	create := texts[len(texts)-4]
	if create != "CREATE USER 'debezium'@'%' IDENTIFIED BY '"+password+"'" {
		t.Errorf("create statement = %q", create)
	}
	for _, s := range texts[len(texts)-3:] {
		if !strings.HasPrefix(s, "GRANT ") || strings.Contains(s, password) {
			t.Errorf("grant statement %q", s)
		}
	}
	// The password is in no return value: only the store name came back.
	if strings.Contains(name, password) {
		t.Error("the secret name contains the password")
	}
}

func TestSetupWriterReadRoleExistingUser(t *testing.T) {
	srv := (&fakeServer{}).onErr("CREATE USER", mysqlErr(errCannotUser))
	store := &memStore{}
	w := newTestWriter(t, srv, store)
	_, err := w.CreateReadRole(unscoped(), "debezium", []string{"shop.orders"})
	if !errors.Is(err, errUserExists) {
		t.Fatalf("CreateReadRole = %v, want errUserExists", err)
	}
	if len(store.values) != 0 {
		t.Fatal("a password was stored for an existing user")
	}
	for _, s := range srv.texts() {
		if strings.HasPrefix(s, "GRANT") || strings.HasPrefix(s, "DROP") || strings.HasPrefix(s, "ALTER") {
			t.Fatalf("an existing user was touched: %s", s)
		}
	}
}

func TestSetupWriterReadRoleCreateFailureDoesNotEchoPassword(t *testing.T) {
	srv := &fakeServer{}
	srv.routes = append(srv.routes, fakeRoute{substr: "CREATE USER", err: errors.New("syntax error near IDENTIFIED BY")})
	w := newTestWriter(t, srv, &memStore{})
	_, err := w.CreateReadRole(unscoped(), "debezium", []string{"shop.orders"})
	if !errors.Is(err, errCreateUser) {
		t.Fatalf("CreateReadRole = %v, want errCreateUser", err)
	}
	if strings.Contains(err.Error(), "syntax error") {
		t.Fatalf("the server text was wrapped into the error: %v", err)
	}
}

func TestSetupWriterReadRoleNeedsCreateUserPrivilege(t *testing.T) {
	srv := (&fakeServer{}).onErr("CREATE USER", mysqlErr(errSpecificDenied))
	w := newTestWriter(t, srv, &memStore{})
	_, err := w.CreateReadRole(unscoped(), "debezium", []string{"shop.orders"})
	if err == nil || !strings.Contains(err.Error(), "CREATE USER privilege") {
		t.Fatalf("CreateReadRole = %v, want the missing privilege named", err)
	}
}

func TestSetupWriterReadRoleDropsTheUserWhenAGrantFails(t *testing.T) {
	srv := (&fakeServer{}).onErr("GRANT RELOAD", mysqlErr(errSpecificDenied))
	store := &memStore{}
	w := newTestWriter(t, srv, store)
	_, err := w.CreateReadRole(unscoped(), "debezium", []string{"shop.orders"})
	if err == nil || !strings.Contains(err.Error(), "WITH GRANT OPTION") {
		t.Fatalf("CreateReadRole = %v, want the grant failure", err)
	}
	if len(store.values) != 0 {
		t.Fatal("a password was stored although the grants failed")
	}
	texts := srv.texts()
	if last := texts[len(texts)-1]; last != "DROP USER IF EXISTS 'debezium'@'%'" {
		t.Fatalf("last statement = %q, want the cleanup drop", last)
	}
}

func TestSetupWriterReadRoleDropsTheUserWhenTheStoreFails(t *testing.T) {
	srv := &fakeServer{}
	storeErr := errors.New("store is full")
	w := newTestWriter(t, srv, &memStore{err: storeErr})
	_, err := w.CreateReadRole(unscoped(), "debezium", []string{"shop.orders"})
	if !errors.Is(err, storeErr) {
		t.Fatalf("CreateReadRole = %v, want the store error", err)
	}
	texts := srv.texts()
	if last := texts[len(texts)-1]; last != "DROP USER IF EXISTS 'debezium'@'%'" {
		t.Fatalf("last statement = %q, want the cleanup drop", last)
	}
}

func TestSetupWriterReadRoleNeedsASecretStore(t *testing.T) {
	srv := &fakeServer{}
	w := newTestWriter(t, srv, nil)
	w.secret = nil
	before := len(srv.recorded())
	if _, err := w.CreateReadRole(unscoped(), "debezium", []string{"shop.orders"}); err == nil || !strings.Contains(err.Error(), "secret store") {
		t.Fatalf("CreateReadRole = %v", err)
	}
	if len(srv.recorded()) != before {
		t.Fatal("a user was created without a place to keep its password")
	}
}

func TestSetupWriterBadInputNeverReachesTheServer(t *testing.T) {
	srv := &fakeServer{}
	w := newTestWriter(t, srv, &memStore{})
	before := len(srv.recorded())
	ctx := unscoped()
	checks := []error{
		w.CreateCanaryTable(ctx, "shop.a`b"),
		w.CreateCanaryTable(ctx, "nodb"),
		w.InsertCanary(ctx, "shop.c", "not-a-nonce"),
		w.InsertCanary(ctx, "shop.c;x", goodNonce),
	}
	_, e1 := w.CreateReadRole(ctx, "a'b", []string{"shop.orders"})
	_, e2 := w.CreateReadRole(ctx, "debezium", []string{"x"})
	_, e3 := w.CreateReadRole(ctx, "debezium", nil)
	checks = append(checks, e1, e2, e3)
	for i, err := range checks {
		if err == nil {
			t.Errorf("case %d: accepted bad input", i)
		}
	}
	if got := len(srv.recorded()); got != before {
		t.Fatalf("bad input issued %d statements: %v", got-before, srv.texts()[before:])
	}
}

func TestSetupWriterErrorMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
		has  string
	}{
		{"no database", mysqlErr(errBadDB), clients.ErrNotFound, ""},
		{"no table", mysqlErr(errNoSuchTable), clients.ErrNotFound, ""},
		{"denied", mysqlErr(errDBAccessDenied), nil, "write user needs"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := (&fakeServer{}).onErr("INSERT INTO", tc.err)
			w := newTestWriter(t, srv, nil)
			err := w.InsertCanary(unscoped(), "shop.c", goodNonce)
			if err == nil {
				t.Fatal("no error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want %v", err, tc.want)
			}
			if tc.has != "" && !strings.Contains(err.Error(), tc.has) {
				t.Errorf("err = %v, want it to mention %q", err, tc.has)
			}
			var me *gomysql.MySQLError
			if !errors.As(err, &me) {
				t.Error("the server error is not kept in the chain")
			}
		})
	}
}

func TestSetupWriterTimeout(t *testing.T) {
	srv := &fakeServer{}
	w := newTestWriter(t, srv, nil, WithSetupTimeout(50*time.Millisecond))
	srv.mu.Lock()
	srv.block = true
	srv.mu.Unlock()
	start := time.Now()
	err := w.InsertCanary(unscoped(), "shop.c", goodNonce)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("err = %v, want a deadline", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("the call took %v", time.Since(start))
	}
}

func TestSetupWriterTargetDown(t *testing.T) {
	srv := writableRoute(&fakeServer{})
	db := srv.open(t)
	w, err := NewSetupWriter(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	srv.onErr("INSERT INTO", driver.ErrBadConn)
	if err := w.InsertCanary(unscoped(), "shop.c", goodNonce); err == nil {
		t.Fatal("a down server was not an error")
	}
}

func TestNewSetupWriterChecks(t *testing.T) {
	if _, err := NewSetupWriter(context.Background(), nil); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("nil pool: %v", err)
	}
	if _, err := OpenSetupWriter(context.Background(), "  "); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("empty URL: %v", err)
	}
	cases := []struct {
		name string
		srv  *fakeServer
		has  string
	}{
		{"read-only session", (&fakeServer{}).on("@@session.transaction_read_only", []string{"a", "b", "c"}, []driver.Value{int64(1), int64(0), int64(0)}), "transaction_read_only"},
		{"read-only server", (&fakeServer{}).on("@@session.transaction_read_only", []string{"a", "b", "c"}, []driver.Value{int64(0), int64(1), int64(0)}), "replica"},
		{"super read-only server", (&fakeServer{}).on("@@session.transaction_read_only", []string{"a", "b", "c"}, []driver.Value{int64(0), int64(0), int64(1)}), "replica"},
		{"server down", (&fakeServer{}).onErr("@@session.transaction_read_only", errors.New("connection refused")), "connect"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewSetupWriter(context.Background(), tc.srv.open(t))
			if err == nil || !strings.Contains(err.Error(), tc.has) {
				t.Fatalf("NewSetupWriter = %v, want %q", err, tc.has)
			}
		})
	}
}

func TestOpenSetupWriterRefusesBadURLsWithoutLeakingThem(t *testing.T) {
	for _, raw := range []string{
		"postgres://u:hunter2@h/db",
		"mysql://u:hunter2@h:3306/db?allowAllFiles=true",
		"mysql://u:hunter2@example.com:3306/db?tls=false",
		"u:hunter2@tcp(h:3306)/db",
	} {
		_, err := OpenSetupWriter(context.Background(), raw)
		if err == nil {
			t.Errorf("accepted %q", raw)
			continue
		}
		if strings.Contains(err.Error(), "hunter2") {
			t.Errorf("error leaks the password: %v", err)
		}
	}
}

func TestSetupScope(t *testing.T) {
	srv := &fakeServer{}
	w := newTestWriter(t, srv, &memStore{})
	mysqlPipeline := owners.Pipeline{ID: "p1", SourceKind: owners.SourceKindMySQL, SourceTables: []string{"shop.orders", "shop.it_canary"}}
	pgPipeline := owners.Pipeline{ID: "p2", SourceTables: []string{"shop.orders", "shop.it_canary"}}
	before := len(srv.recorded())

	if err := w.CreateCanaryTable(context.Background(), "shop.it_canary"); !errors.Is(err, tools.ErrNoScope) {
		t.Errorf("no scope: %v, want ErrNoScope", err)
	}
	if err := w.CreateCanaryTable(tools.WithPipelineID(context.Background(), "p1"), "shop.it_canary"); !errors.Is(err, tools.ErrNoScope) {
		t.Errorf("pipeline id only: %v, want ErrNoScope", err)
	}
	if err := w.CreateCanaryTable(tools.WithPipeline(context.Background(), mysqlPipeline), "shop.other"); !errors.Is(err, tools.ErrOutOfScope) {
		t.Errorf("table outside the pipeline: %v, want ErrOutOfScope", err)
	}
	if err := w.InsertCanary(tools.WithPipeline(context.Background(), mysqlPipeline), "shop.other", goodNonce); !errors.Is(err, tools.ErrOutOfScope) {
		t.Errorf("insert outside the pipeline: %v", err)
	}
	if _, err := w.CreateReadRole(tools.WithPipeline(context.Background(), mysqlPipeline), "debezium", []string{"shop.orders", "shop.other"}); !errors.Is(err, tools.ErrOutOfScope) {
		t.Errorf("read role on a table outside the pipeline: %v", err)
	}
	if err := w.CreateCanaryTable(tools.WithPipeline(context.Background(), pgPipeline), "shop.it_canary"); !errors.Is(err, tools.ErrOutOfScope) {
		t.Errorf("a non-MySQL pipeline: %v, want ErrOutOfScope", err)
	}
	if got := len(srv.recorded()); got != before {
		t.Fatalf("a refused call issued %d statements", got-before)
	}
	if err := w.CreateCanaryTable(tools.WithPipeline(context.Background(), mysqlPipeline), "shop.it_canary"); err != nil {
		t.Errorf("in-scope write refused: %v", err)
	}
}
