//go:build integration

package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"piperoom/internal/tools"
)

// writeURLEnv is the write user's mysql:// URL on the dev compose profile
// "mysql" (Q8a). Until that profile is in the full-stack CI job the test
// skips, and the skip is on .github/integration-skip-allow.txt.
const writeURLEnv = "PIPEROOM_IT_MYSQL_WRITE_URL"

// victimURLEnv is an optional URL of a user that is neither the read nor the
// write user (e.g. the dev profile's app user); EndSession's kill path runs
// only when it is set.
const victimURLEnv = "PIPEROOM_IT_MYSQL_VICTIM_URL"

// itDBEnv names the database the test creates its it_ tables in (default
// shop, the dev profile's database).
const itDBEnv = "PIPEROOM_IT_MYSQL_DB"

func TestMySQLWriterIntegration(t *testing.T) {
	raw := os.Getenv(writeURLEnv)
	if raw == "" {
		t.Skipf("mysql writer integration: %s is not set (the dev compose profile \"mysql\", Q8a, is not in this run)", writeURLEnv)
	}
	db := os.Getenv(itDBEnv)
	if db == "" {
		db = "shop"
	}
	connUser := os.Getenv(connectorUserEnv)
	if connUser == "" {
		connUser = "debezium"
	}
	ctx := tools.WithUnscoped(context.Background())
	w, err := NewWriter(ctx, raw)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(w.Close)
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	prefix := "it_" + hex.EncodeToString(b)

	t.Run("heartbeat table", func(t *testing.T) {
		table := db + "." + prefix + "_heartbeat"
		t.Cleanup(func() {
			_, _ = w.db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+quoteIdent(db)+"."+quoteIdent(prefix+"_heartbeat"))
		})
		for i := 0; i < 2; i++ { // idempotent
			if err := w.CreateHeartbeatTable(ctx, table); err != nil {
				t.Fatalf("create #%d: %v", i+1, err)
			}
		}
		var n int
		if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`, db, prefix+"_heartbeat").Scan(&n); err != nil || n != 2 {
			t.Fatalf("columns %d, %v", n, err)
		}
	})

	t.Run("grant select", func(t *testing.T) {
		name := prefix + "_t"
		q := quoteIdent(db) + "." + quoteIdent(name)
		if _, err := w.db.ExecContext(ctx, "CREATE TABLE "+q+" (id INT PRIMARY KEY)"); err != nil {
			t.Fatalf("create: %v", err)
		}
		t.Cleanup(func() { _, _ = w.db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+q) })
		err := w.GrantSelect(ctx, connUser, []string{db + "." + name})
		if errors.Is(err, ErrAmbiguousAccount) {
			t.Logf("connector user has several accounts on this server; refusal checked: %v", err)
			return
		}
		if err != nil {
			t.Fatalf("grant: %v", err)
		}
		var priv string
		if err := w.db.QueryRowContext(ctx, `SELECT Table_priv FROM mysql.tables_priv WHERE User = ? AND Db = ? AND Table_name = ?`, connUser, db, name).Scan(&priv); err != nil {
			t.Logf("tables_priv not readable by the write user (%v); grant returned no error", err)
		} else if priv != "Select" {
			t.Fatalf("Table_priv %q, want Select only", priv)
		}
		// The bound form: if the server takes a ? for the account, the
		// writer could bind it (design §6.8); logged, not required.
		if _, err := w.db.ExecContext(ctx, "GRANT SELECT ON "+q+" TO ?@?", connUser, "%"); err != nil {
			t.Logf("bound GRANT ... TO ?@? refused by the server (the literal form stays): %v", err)
		} else {
			t.Log("bound GRANT ... TO ?@? accepted by the server")
		}
	})

	t.Run("end session refusals", func(t *testing.T) {
		// Another session of the write user is protected.
		conn, err := w.db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		var pid int64
		var started int64
		if _, err := tx.ExecContext(ctx, "SELECT 1 FROM information_schema.TABLES LIMIT 1 FOR SHARE"); err != nil {
			t.Logf("open transaction: %v", err)
		}
		if err := tx.QueryRowContext(ctx, `SELECT CONNECTION_ID()`).Scan(&pid); err != nil {
			t.Fatal(err)
		}
		_ = w.db.QueryRowContext(ctx, `SELECT COALESCE(CAST(UNIX_TIMESTAMP(trx_started) AS SIGNED), 0) FROM information_schema.innodb_trx WHERE trx_mysql_thread_id = ?`, pid).Scan(&started)
		if started == 0 {
			if err := w.EndSession(ctx, pid, time.Unix(1, 0)); !errors.Is(err, ErrSessionChanged) {
				t.Fatalf("no transaction: %v", err)
			}
			return
		}
		if err := w.EndSession(ctx, pid, time.Unix(started, 0)); !errors.Is(err, ErrSessionProtected) {
			t.Fatalf("write user's session: %v", err)
		}
		if err := w.EndSession(ctx, 1<<40, time.Unix(started, 0)); !errors.Is(err, ErrSessionChanged) {
			t.Fatalf("unknown pid: %v", err)
		}
	})

	t.Run("end session kill", func(t *testing.T) {
		vraw := os.Getenv(victimURLEnv)
		if vraw == "" {
			t.Logf("%s not set: the kill path is covered by the unit tests only", victimURLEnv)
			return
		}
		// Not NewWriter: its program_name piperoom-writer is protected.
		u, err := parseURL(vraw)
		if err != nil {
			t.Fatalf("victim URL: %v", err)
		}
		cfg, err := u.config("it-victim", nil, driverLogger{l: slog.Default()})
		if err != nil {
			t.Fatal(err)
		}
		connector, err := gomysql.NewConnector(cfg)
		if err != nil {
			t.Fatal(err)
		}
		vdb := sql.OpenDB(connector)
		defer vdb.Close()
		conn, err := vdb.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		hb := quoteIdent(db) + "." + quoteIdent(prefix+"_heartbeat")
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+hb+" (id, ts) VALUES (1, NOW(6))"); err != nil {
			t.Fatalf("victim write: %v", err)
		}
		var pid, started int64
		if err := tx.QueryRowContext(ctx, `SELECT CONNECTION_ID()`).Scan(&pid); err != nil {
			t.Fatal(err)
		}
		if err := w.db.QueryRowContext(ctx, `SELECT CAST(UNIX_TIMESTAMP(trx_started) AS SIGNED) FROM information_schema.innodb_trx WHERE trx_mysql_thread_id = ?`, pid).Scan(&started); err != nil {
			t.Fatal(err)
		}
		if err := w.EndSession(ctx, pid, time.Unix(started-60, 0)); !errors.Is(err, ErrSessionChanged) {
			t.Fatalf("wrong start: %v", err)
		}
		if err := w.EndSession(ctx, pid, time.Unix(started, 0)); err != nil {
			t.Fatalf("kill: %v", err)
		}
		if err := tx.Commit(); err == nil {
			t.Fatal("the victim's transaction survived the kill")
		}
	})
}
