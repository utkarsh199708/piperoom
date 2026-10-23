//go:build integration

package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/pgconv"
	pgclient "piperoom/internal/clients/postgres"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// TestPrerequisitesIntegration drives the driver and the executing setup
// writer against the dev stack: it creates an it_ source table, then lets
// SetupWriter create the publication, the canary table and the read role (with
// the generated password in a test secret store), makes every check pass, and
// asks the dev Connect to validate the built connector config the way Q4a does
// (with ${secret:} materialised, as the runner does).
func TestPrerequisitesIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	ctx, cancel := context.WithTimeout(tools.WithUnscoped(context.Background()), 90*time.Second)
	defer cancel()

	name := testenv.Name("pgdrv")
	table := "public." + name
	canary := "public." + name + "_canary"
	tables := []string{table, canary}
	publication, slot, role := name, name, name
	roleCreated := false

	admin, err := pgx.Connect(ctx, cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	// One cleanup, and every drop is best effort: a failure must not skip the
	// drops after it, or it_ objects leak on the shared dev stack.
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		echo(t, cctx, admin, "DROP PUBLICATION IF EXISTS "+publication)
		echo(t, cctx, admin, "DROP TABLE IF EXISTS "+canary)
		echo(t, cctx, admin, "DROP TABLE IF EXISTS "+table)
		if roleCreated {
			echo(t, cctx, admin, "DROP OWNED BY "+role)
			echo(t, cctx, admin, "DROP ROLE IF EXISTS "+role)
		}
	})

	if _, err := admin.Exec(ctx, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, note text)"); err != nil {
		t.Fatalf("create source table: %v", err)
	}

	// The writes run through the executing clients.SetupSourceWriter on the dev
	// stack's writable source role, with the explicit unscoped opt-out.
	store := &testSecretStore{values: map[string]string{}}
	sw, err := NewSetupWriter(ctx, cfg.SourceDBURL, WithSetupSecretStore(store))
	if err != nil {
		t.Fatalf("setup writer: %v", err)
	}
	t.Cleanup(func() { _ = sw.Close() })

	// The canary table first: the publication lists it, and CREATE PUBLICATION
	// needs every listed table to exist.
	if err := sw.CreateCanaryTable(ctx, canary); err != nil {
		t.Fatalf("CreateCanaryTable: %v", err)
	}
	if err := sw.CreatePublication(ctx, publication, tables); err != nil {
		t.Fatalf("CreatePublication: %v", err)
	}
	nonce := randomNonce(t)
	if err := sw.InsertCanary(ctx, canary, nonce); err != nil {
		t.Fatalf("InsertCanary: %v", err)
	}
	var got string
	if err := admin.QueryRow(ctx, "SELECT nonce FROM "+canary).Scan(&got); err != nil || got != nonce {
		t.Fatalf("canary row = %q, %v; want the inserted nonce", got, err)
	}

	secretName, err := sw.CreateReadRole(ctx, role, tables)
	if err != nil {
		t.Fatalf("CreateReadRole: %v", err)
	}
	roleCreated = true
	password := store.values[secretName]
	if password == "" {
		t.Fatalf("the secret store has no value under %q: %v", secretName, store.values)
	}
	if strings.Contains(secretName, password) {
		t.Fatal("the returned secret name carries the password")
	}

	// The generated password logs the connector role in, and the grants are
	// exactly SELECT on the listed tables plus pg_read_all_stats.
	roleConn := roleConn(t, ctx, cfg.SourceDBURL, role, password)
	t.Cleanup(func() { _ = roleConn.Close(context.Background()) })
	for _, tbl := range tables {
		var selectOK bool
		if err := roleConn.QueryRow(ctx, "SELECT has_table_privilege($1, $2, 'SELECT')", role, tbl).Scan(&selectOK); err != nil || !selectOK {
			t.Fatalf("%s: SELECT = %v, %v", tbl, selectOK, err)
		}
		var insertOK bool
		if err := roleConn.QueryRow(ctx, "SELECT has_table_privilege($1, $2, 'INSERT')", role, tbl).Scan(&insertOK); err != nil || insertOK {
			t.Fatalf("%s: INSERT = %v, %v; the read role must not write", tbl, insertOK, err)
		}
	}
	var statsMember bool
	if err := admin.QueryRow(ctx, `SELECT pg_has_role($1, 'pg_read_all_stats', 'MEMBER')`, role).Scan(&statsMember); err != nil || !statsMember {
		t.Fatalf("pg_read_all_stats membership = %v, %v", statsMember, err)
	}

	db, err := pgclient.New(ctx, cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("source client: %v", err)
	}
	t.Cleanup(db.Close)

	d := Driver{Publication: publication, Slot: slot, Role: role, Tables: tables}
	results, err := d.Run(ctx, db)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, r := range results {
		if !r.OK {
			t.Errorf("%s failed: %v (note %q)", r.Check.ID, r.Err, r.Note)
		}
	}
	if t.Failed() {
		t.Fatal("the driver did not accept the objects it created")
	}

	// The connector config, validated by the dev Connect. The runner
	// materialises ${secret:} before Connect sees a config; the test does the
	// same with the password it just stored. The host is the one the Connect
	// container reaches the source at, never the host's localhost.
	u, err := url.Parse(cfg.SourceDBURL)
	if err != nil || u.User == nil {
		t.Fatalf("SOURCE_DB_URL: %v", err)
	}
	port := 5432
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil {
			t.Fatalf("SOURCE_DB_URL port: %v", err)
		}
		port = n
	}
	connCfg, err := ConnectorConfig(ConnectorOptions{
		Host:           connectDBHost(),
		Port:           port,
		Database:       strings.TrimPrefix(u.Path, "/"),
		User:           role,
		PasswordSecret: secretName,
		TopicPrefix:    "itprefix",
		Publication:    publication,
		Slot:           slot,
		Tables:         tables,
	})
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	if connCfg["database.password"] != "${secret:"+secretName+"}" {
		t.Fatalf("password = %q, want a secret reference", connCfg["database.password"])
	}
	connCfg["database.password"] = password
	// Connect's validate endpoint requires the connector name as "name", which
	// is not part of the config POST /connectors carries separately (Q4a's
	// connect_create_connector adds it the same way).
	connCfg["name"] = name

	c := connect.New(cfg.ConnectURL, &http.Client{Timeout: 10 * time.Second})
	vr, err := c.ValidateConfig(ctx, ConnectorClass, connCfg)
	if err != nil {
		t.Fatalf("Connect validate: %v", err)
	}
	if vr.ErrorCount != 0 {
		t.Fatalf("Connect validate: %d errors: %v", vr.ErrorCount, vr.Errors)
	}
}

// testSecretStore is the runner's secret store in miniature: CreateReadRole
// stores the generated password here and returns only its name.
type testSecretStore struct{ values map[string]string }

func (s *testSecretStore) Store(_ context.Context, name, value string) error {
	s.values[name] = value
	return nil
}

// roleConn opens a connection as the generated connector role. The DSN is the
// dev stack's with the user and password replaced; it is never logged.
func roleConn(t *testing.T, ctx context.Context, dsn, role, password string) *pgx.Conn {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("SOURCE_DB_URL: %v", err)
	}
	u.User = url.UserPassword(role, password)
	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		// The DSN holds the generated password; never put it in the message.
		t.Fatalf("connect as %s: %v", role, pgconv.RedactDSN(err, u.String()))
	}
	return conn
}

// connectDBHost is the source host as the Connect container sees it: the
// compose service name, not the host's localhost (inside Connect, localhost is
// Connect itself). PIPEROOM_TEST_CONNECT_DB_HOST overrides it for another
// stack.
func connectDBHost() string {
	if h := strings.TrimSpace(os.Getenv("PIPEROOM_TEST_CONNECT_DB_HOST")); h != "" {
		return h
	}
	return "postgres"
}

// echo runs one cleanup statement. It reports a failure with t.Errorf and
// keeps going: t.Fatalf here would stop the cleanup and leak the it_ objects
// after the first failure.
func echo(t *testing.T, ctx context.Context, conn *pgx.Conn, stmt string) {
	t.Helper()
	if _, err := conn.Exec(ctx, stmt); err != nil {
		t.Errorf("cleanup %s: %v", stmt, err)
	}
}

func randomNonce(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("nonce: %v", err)
	}
	return hex.EncodeToString(b[:])
}
