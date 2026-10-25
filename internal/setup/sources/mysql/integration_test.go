//go:build integration

package mysql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"piperoom/internal/clients"
	mysqlclient "piperoom/internal/clients/mysql"
	"piperoom/internal/tools"
)

// The dev compose profile "mysql" (Q8a, dev/mysql/README.md) is in the
// full-stack job, which sets the first two (the same URLs the Q8 client's
// integration tests use). The third is optional: a root URL, e.g.
// mysql://root:piperoom-dev@127.0.0.1:13306/shop, for the part that creates a
// user (the profile's piperoom_write cannot CREATE USER).
const (
	readURLEnv  = "PIPEROOM_IT_MYSQL_URL"
	writeURLEnv = "PIPEROOM_IT_MYSQL_WRITE_URL"
	adminURLEnv = "PIPEROOM_IT_MYSQL_ADMIN_URL"
	// itDBEnv names the database the test creates its it_ objects in (default
	// shop, the profile's database).
	itDBEnv = "PIPEROOM_IT_MYSQL_DB"
)

// requireReachable skips, as testenv.Require does for the dev stack, when the
// server the URL names does not answer on its port: the profile is down or was
// never started. Only the host and port are dialled and named; the URL's
// password reaches no message. A value that is not a URL is left to the client,
// whose errors carry no URL either.
func requireReachable(t *testing.T, rawURL string) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return
	}
	port := u.Port()
	if port == "" {
		port = "3306"
	}
	addr := net.JoinHostPort(u.Hostname(), port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Skipf("mysql setup integration: the mysql profile is not reachable at %s (`docker compose -f dev/docker-compose.yml --profile mysql up -d --wait mysql`): %v", addr, err)
	}
	_ = conn.Close()
}

func randomNonce() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func itName() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return "it_" + hex.EncodeToString(b)
}

// TestPrerequisitesIntegration runs the driver and the executing setup writer
// against the Q8a mysql profile: the seeded server and connector user pass every
// check, the writer creates an it_ canary table and inserts a bound nonce, and
// the two refusals hold. With PIPEROOM_IT_MYSQL_ADMIN_URL set it also creates a
// connector user and watches the user checks go from failing to passing.
func TestPrerequisitesIntegration(t *testing.T) {
	readURL, writeURL := os.Getenv(readURLEnv), os.Getenv(writeURLEnv)
	if readURL == "" || writeURL == "" {
		// A skip, never a silent pass (docs/collab.md §5: an integration test
		// skips when the stack is unreachable). The setup matrix's Compose job
		// runs ./internal/setup/... with -tags integration and no MySQL, where
		// this skip is tolerated through .github/integration-skip-allow.txt,
		// as the Postgres and Oracle drivers' are; the full-stack job starts
		// the profile, sets both URLs and fails on a skip, so there it runs.
		t.Skipf("mysql setup integration: %s and %s are not set (the dev compose profile \"mysql\", Q8a, is not in this run: `docker compose -f dev/docker-compose.yml --profile mysql up -d --wait mysql`, then set both as dev/mysql/README.md lists them)", readURLEnv, writeURLEnv)
	}
	requireReachable(t, readURL)
	db := os.Getenv(itDBEnv)
	if db == "" {
		db = "shop"
	}
	ctx, cancel := context.WithTimeout(tools.WithUnscoped(context.Background()), 90*time.Second)
	defer cancel()

	reader, err := mysqlclient.New(ctx, readURL)
	if err != nil {
		t.Fatalf("read client: %v", err)
	}
	t.Cleanup(reader.Close)

	// The seeded profile is ready: GTIDs on, ROW/FULL, server_id 223344, one day
	// of retention, and the debezium user with the connector's grants.
	seeded := Driver{Role: "debezium", Tables: []string{db + ".orders", db + ".order_items"}, ConnectorServerID: 5401}
	results, err := seeded.Run(ctx, reader)
	if err != nil {
		t.Fatalf("Run on the seeded profile: %v", err)
	}
	for _, r := range results {
		if !r.OK {
			t.Errorf("%s fails on the seeded profile: err=%v ask=%s", r.Check.ID, r.Err, firstAsk(seeded, r))
		}
	}

	sw, err := OpenSetupWriter(ctx, writeURL)
	if err != nil {
		t.Fatalf("OpenSetupWriter: %v", err)
	}
	t.Cleanup(func() { _ = sw.Close() })
	pool, err := mysqlclient.OpenSetupPool(ctx, writeURL, nil)
	if err != nil {
		t.Fatalf("OpenSetupPool: %v", err)
	}
	table := db + "." + itName() + "_canary"
	q, err := quoteTable(table)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		if _, err := pool.ExecContext(cctx, "DROP TABLE IF EXISTS "+q); err != nil {
			t.Logf("cleanup: %v", err)
		}
		_ = pool.Close()
	})

	for i := 0; i < 2; i++ { // idempotent
		if err := sw.CreateCanaryTable(ctx, table); err != nil {
			t.Fatalf("CreateCanaryTable (call %d): %v", i+1, err)
		}
	}
	hexNonce := randomNonce()
	// The profile's piperoom_write has SELECT, CREATE and DROP on shop but no
	// INSERT (dev/mysql/init, A1's file): the setup needs INSERT on the canary
	// table, so the denial is expected here and the insert itself is covered by
	// the create-user flow, whose admin account can.
	if err := sw.InsertCanary(ctx, table, hexNonce); err != nil {
		if errNumber(err) != errTableDenied {
			t.Fatalf("InsertCanary: %v", err)
		}
		t.Logf("InsertCanary denied for the profile's write user (needs INSERT on %s): %v", db, err)
	} else {
		var n int
		if err := pool.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+q+" WHERE nonce = ?", hexNonce).Scan(&n); err != nil || n != 1 {
			t.Fatalf("canary row count = %d, %v; want 1", n, err)
		}
	}
	if err := sw.CreatePublication(ctx, "p", seeded.Tables); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("CreatePublication = %v", err)
	}
	if _, err := sw.AlterSystem(ctx, "binlog_format", "ROW"); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("AlterSystem = %v", err)
	}
	if _, err := sw.CreateReadRole(ctx, itName(), []string{table}); err == nil {
		t.Fatal("CreateReadRole without a secret store was accepted")
	}

	cfg, err := ConnectorConfig(ConnectorOptions{
		Host: "mysql", Port: 3306, User: "debezium", PasswordSecret: "source_read_password", ServerID: 5401,
		TopicPrefix: "shop", Tables: seeded.Tables, HistoryBootstrap: "kafka:9092",
	})
	if err != nil || cfg["database.password"] != "${secret:source_read_password}" {
		t.Fatalf("ConnectorConfig = %v, %v", cfg, err)
	}

	if adminURL := os.Getenv(adminURLEnv); adminURL != "" {
		createUserFlow(ctx, t, adminURL, db)
	} else {
		t.Logf("%s is not set: the create-user flow was not run", adminURLEnv)
	}
}

// createUserFlow creates a connector user with an admin account and makes the
// user checks pass; the user and its table are dropped afterwards.
func createUserFlow(ctx context.Context, t *testing.T, adminURL, db string) {
	t.Helper()
	name := itName()
	table := db + "." + name
	tables := []string{table}

	reader, err := mysqlclient.New(ctx, adminURL)
	if err != nil {
		t.Fatalf("admin read client: %v", err)
	}
	t.Cleanup(reader.Close)
	store := &memStore{}
	sw, err := OpenSetupWriter(ctx, adminURL, WithSetupSecretStore(store))
	if err != nil {
		t.Fatalf("admin OpenSetupWriter: %v", err)
	}
	t.Cleanup(func() { _ = sw.Close() })
	pool, err := mysqlclient.OpenSetupPool(ctx, adminURL, nil)
	if err != nil {
		t.Fatalf("admin OpenSetupPool: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		if drop, err := DropUserSQL(name); err == nil {
			if _, err := pool.ExecContext(cctx, drop); err != nil {
				t.Logf("cleanup: %v", err)
			}
		}
		if q, err := quoteTable(table); err == nil {
			if _, err := pool.ExecContext(cctx, "DROP TABLE IF EXISTS "+q); err != nil {
				t.Logf("cleanup: %v", err)
			}
		}
		_ = pool.Close()
	})

	if err := sw.CreateCanaryTable(ctx, table); err != nil {
		t.Fatalf("CreateCanaryTable: %v", err)
	}
	nonce := randomNonce()
	if err := sw.InsertCanary(ctx, table, nonce); err != nil {
		t.Fatalf("InsertCanary: %v", err)
	}
	if q, err := quoteTable(table); err == nil {
		var n int
		if err := pool.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+q+" WHERE nonce = ?", nonce).Scan(&n); err != nil || n != 1 {
			t.Fatalf("canary row count = %d, %v; want 1", n, err)
		}
	}
	if err := sw.InsertCanary(ctx, db+".it_does_not_exist", nonce); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("insert into a missing table = %v, want ErrNotFound", err)
	}
	d := Driver{Role: name, Tables: tables, ConnectorServerID: 5401}
	before, err := d.Run(ctx, reader)
	if err != nil {
		t.Fatalf("Run before: %v", err)
	}
	for _, id := range []string{CheckReadRole, CheckReadRoleGrants, CheckReadRolePrivileges} {
		if r := resultOf(t, before, id); r.Err != nil || r.OK {
			t.Fatalf("%s before the user exists: OK=%v err=%v, want a plain failure", id, r.OK, r.Err)
		}
	}
	if r := resultOf(t, before, CheckReadRole); true {
		if op, _ := r.Check.Fix(d, r.Evidence); op != opCreateReadRole {
			t.Fatalf("read_role fix = %q, want %q", op, opCreateReadRole)
		}
	}

	secret, err := sw.CreateReadRole(ctx, name, tables)
	if err != nil {
		t.Fatalf("CreateReadRole: %v", err)
	}
	if secret != readPasswordSecretName || len(store.values[secret]) != 48 {
		t.Fatalf("secret %q, stored %d characters", secret, len(store.values[secret]))
	}
	if _, err := sw.CreateReadRole(ctx, name, tables); !errors.Is(err, errUserExists) {
		t.Fatalf("second CreateReadRole = %v, want errUserExists", err)
	}
	after, err := d.Run(ctx, reader)
	if err != nil {
		t.Fatalf("Run after: %v", err)
	}
	for _, r := range after {
		if !r.OK {
			t.Errorf("%s still fails after the setup: err=%v ask=%s", r.Check.ID, r.Err, firstAsk(d, r))
		}
	}
}

func firstAsk(d Driver, r Result) string {
	op, ask := r.Check.Fix(d, r.Evidence)
	if op != "" {
		return "op " + op
	}
	return ask
}
