//go:build integration

package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients/connect"
	pgclient "piperoom/internal/clients/postgres"
	"piperoom/internal/testenv"
)

// TestPrerequisitesIntegration drives the driver against the dev stack: it
// creates an it_ table, publication, canary table and role, makes every check
// pass, and asks the dev Connect to validate the built connector config the
// way Q4a does (with the ${secret:} materialised, as the runner does).
func TestPrerequisitesIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	name := testenv.Name("pgdrv")
	table := "public." + name
	canary := "public." + name + "_canary"
	publication, slot, role := name, name, name
	roleCreated := false

	conn, err := pgx.Connect(ctx, cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer ccancel()
		mustExec(t, cctx, conn, "DROP PUBLICATION IF EXISTS "+publication)
		mustExec(t, cctx, conn, "DROP TABLE IF EXISTS "+canary)
		mustExec(t, cctx, conn, "DROP TABLE IF EXISTS "+table)
		if roleCreated {
			mustExec(t, cctx, conn, "REVOKE "+StatsRole+" FROM "+role)
			mustExec(t, cctx, conn, "DROP OWNED BY "+role)
			mustExec(t, cctx, conn, "DROP ROLE IF EXISTS "+role)
		}
	})

	mustExec(t, ctx, conn, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, note text)")
	mustExec(t, ctx, conn, "CREATE PUBLICATION "+publication+" FOR TABLE "+table)
	canarySQL, err := CanaryTableSQL(canary)
	if err != nil {
		t.Fatalf("CanaryTableSQL: %v", err)
	}
	mustExec(t, ctx, conn, canarySQL)
	insertSQL, err := InsertCanarySQL(canary)
	if err != nil {
		t.Fatalf("InsertCanarySQL: %v", err)
	}
	nonce := randomNonce(t)
	if _, err := conn.Exec(ctx, insertSQL, nonce); err != nil {
		t.Fatalf("insert canary: %v", err)
	}
	var got string
	if err := conn.QueryRow(ctx, "SELECT nonce FROM "+canary).Scan(&got); err != nil || got != nonce {
		t.Fatalf("canary row = %q, %v; want nonce", got, err)
	}

	// The read role: LOGIN for the connector, REPLICATION for its slot, and
	// exactly the ReadRoleGrants statements. Dev-only credentials.
	mustExec(t, ctx, conn, "CREATE ROLE "+role+" LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD 'it-dev-only'")
	roleCreated = true
	grants, err := ReadRoleGrants(role, []string{table})
	if err != nil {
		t.Fatalf("ReadRoleGrants: %v", err)
	}
	for _, stmt := range grants {
		mustExec(t, ctx, conn, stmt)
	}
	var statsMember bool
	if err := conn.QueryRow(ctx, `SELECT pg_has_role($1, 'pg_read_all_stats', 'MEMBER')`, role).Scan(&statsMember); err != nil || !statsMember {
		t.Fatalf("pg_read_all_stats membership = %v, %v", statsMember, err)
	}

	db, err := pgclient.New(ctx, cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("source client: %v", err)
	}
	t.Cleanup(db.Close)

	d := Driver{Publication: publication, Slot: slot, Role: role, Tables: []string{table}}
	results, err := d.Run(ctx, db)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, r := range results {
		if !r.OK {
			t.Errorf("%s failed: %v", r.Check.ID, r.Err)
		}
	}
	if t.Failed() {
		t.Fatal("the driver did not accept the objects it created")
	}

	// The connector config, validated by the dev Connect. The runner
	// materialises ${secret:} before Connect sees a config; the test does the
	// same with the dev stack's own password.
	u, err := url.Parse(cfg.SourceDBURL)
	if err != nil || u.User == nil {
		t.Fatalf("SOURCE_DB_URL: %v", err)
	}
	password, _ := u.User.Password()
	port := 5432
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil {
			t.Fatalf("SOURCE_DB_URL port: %v", err)
		}
		port = n
	}
	connCfg, err := ConnectorConfig(ConnectorOptions{
		Host:            u.Hostname(),
		Port:            port,
		Database:        strings.TrimPrefix(u.Path, "/"),
		User:            role,
		PasswordSecret:  "it_read_password",
		TopicPrefix:     "itprefix",
		Publication:     publication,
		Slot:            slot,
		Tables:          []string{table},
		PublicationMode: PublicationFiltered,
	})
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	if connCfg["database.password"] != "${secret:it_read_password}" {
		t.Fatalf("password = %q, want a secret reference", connCfg["database.password"])
	}
	connCfg["database.password"] = password

	c := connect.New(cfg.ConnectURL, &http.Client{Timeout: 10 * time.Second})
	vr, err := c.ValidateConfig(ctx, ConnectorClass, connCfg)
	if err != nil {
		t.Fatalf("Connect validate: %v", err)
	}
	if vr.ErrorCount != 0 {
		t.Fatalf("Connect validate: %d errors: %v", vr.ErrorCount, vr.Errors)
	}
}

func mustExec(t *testing.T, ctx context.Context, conn *pgx.Conn, stmt string) {
	t.Helper()
	if _, err := conn.Exec(ctx, stmt); err != nil {
		t.Fatalf("%s: %v", stmt, err)
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
