//go:build integration

package mysql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"piperoom/internal/clients"
	mysqlclient "piperoom/internal/clients/mysql"
	"piperoom/internal/tools"
)

// adminURLEnv is the dev compose profile "mysql" (Q8a, dev/mysql/README.md)
// root user's mysql:// URL, e.g. mysql://root:piperoom-dev@127.0.0.1:13306/shop.
// The test needs an account that can CREATE USER and grant the connector's
// global privileges (the profile's piperoom_write cannot), and that can read
// the grant tables, so one admin URL serves the reads and the writes. Until the
// profile is in the full-stack CI job the test skips, like the Q8 client's.
const adminURLEnv = "PIPEROOM_IT_MYSQL_ADMIN_URL"

// itDBEnv names the database the test creates its it_ table in (default shop).
const itDBEnv = "PIPEROOM_IT_MYSQL_DB"

// TestPrerequisitesIntegration drives the driver and the executing setup writer
// against the Q8a mysql profile: it makes an it_ table (through the canary
// writer, which is all the table the checks need), watches the user checks fail,
// lets SetupWriter create the connector's user with its grants, and makes every
// check pass.
func TestPrerequisitesIntegration(t *testing.T) {
	raw := os.Getenv(adminURLEnv)
	if raw == "" {
		t.Skipf("mysql setup integration: %s is not set (the dev compose profile \"mysql\", Q8a, is not in this run)", adminURLEnv)
	}
	db := os.Getenv(itDBEnv)
	if db == "" {
		db = "shop"
	}
	ctx, cancel := context.WithTimeout(tools.WithUnscoped(context.Background()), 90*time.Second)
	defer cancel()

	b := make([]byte, 4)
	_, _ = rand.Read(b)
	name := "it_" + hex.EncodeToString(b)
	table := db + "." + name
	tables := []string{table}

	reader, err := mysqlclient.New(ctx, raw)
	if err != nil {
		t.Fatalf("read client: %v", err)
	}
	t.Cleanup(reader.Close)

	store := &memStore{}
	sw, err := OpenSetupWriter(ctx, raw, WithSetupSecretStore(store))
	if err != nil {
		t.Fatalf("OpenSetupWriter: %v", err)
	}
	t.Cleanup(func() { _ = sw.Close() })

	// Cleanup drops the user and the table on a pool of its own, best effort and
	// on a fresh context, so a failure above cannot leak it_ objects on the
	// shared dev stack.
	pool, err := mysqlclient.OpenSetupPool(ctx, raw, nil)
	if err != nil {
		t.Fatalf("OpenSetupPool: %v", err)
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
	if err := sw.CreateCanaryTable(ctx, table); err != nil {
		t.Fatalf("CreateCanaryTable is not idempotent: %v", err)
	}

	d := Driver{Role: name, Tables: tables, ConnectorServerID: 5401}
	before, err := d.Run(ctx, reader)
	if err != nil {
		t.Fatalf("Run before: %v", err)
	}
	for _, id := range []string{CheckReadRole, CheckReadRoleGrants, CheckReadRolePrivileges} {
		r := resultOf(t, before, id)
		if r.Err != nil || r.OK {
			t.Fatalf("%s before the user exists: OK=%v err=%v, want a plain failure", id, r.OK, r.Err)
		}
	}
	if op, _ := resultOf(t, before, CheckReadRole).Check.Fix(d, resultOf(t, before, CheckReadRole).Evidence); op != opCreateReadRole {
		t.Fatalf("read_role fix = %q, want %q", op, opCreateReadRole)
	}
	// The dev profile is ready on the server side; an unready server would make
	// the assertion below name the check.
	for _, id := range []string{CheckLogBin, CheckBinlogFormat, CheckBinlogRowImage, CheckGTID, CheckServerID, CheckBinlogRetention, CheckTables} {
		if r := resultOf(t, before, id); !r.OK {
			t.Fatalf("%s on the dev profile: OK=false err=%v evidence=%+v", id, r.Err, r.Evidence.Settings)
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
			t.Errorf("%s still fails after the setup: err=%v ask=%v", r.Check.ID, r.Err, firstAsk(d, r))
		}
	}

	nonce := make([]byte, 16)
	_, _ = rand.Read(nonce)
	hexNonce := hex.EncodeToString(nonce)
	if err := sw.InsertCanary(ctx, table, hexNonce); err != nil {
		t.Fatalf("InsertCanary: %v", err)
	}
	q, _ := quoteTable(table)
	var n int
	if err := pool.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+q+" WHERE nonce = ?", hexNonce).Scan(&n); err != nil || n != 1 {
		t.Fatalf("canary row count = %d, %v; want 1", n, err)
	}

	cfg, err := ConnectorConfig(ConnectorOptions{
		Host: "mysql", Port: 3306, User: name, PasswordSecret: secret, ServerID: 5401,
		TopicPrefix: name, Tables: tables, HistoryBootstrap: "kafka:9092",
	})
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	if cfg["database.password"] != "${secret:source_read_password}" {
		t.Fatalf("password key = %q", cfg["database.password"])
	}

	if err := sw.CreatePublication(ctx, "p", tables); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("CreatePublication = %v", err)
	}
	if _, err := sw.AlterSystem(ctx, "binlog_format", "ROW"); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("AlterSystem = %v", err)
	}
}

func firstAsk(d Driver, r Result) string {
	op, ask := r.Check.Fix(d, r.Evidence)
	if op != "" {
		return "op " + op
	}
	return ask
}
