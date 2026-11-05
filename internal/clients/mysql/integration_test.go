//go:build integration

package mysql

import (
	"context"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/mysql/gtid"
)

// mysqlURLEnv is the read user's mysql:// URL on the dev compose profile
// "mysql" (Q8a), e.g. mysql://piperoom_read:...@localhost:3306/shop. Until
// that profile is in the full-stack CI job the test skips, and the skip is on
// .github/integration-skip-allow.txt.
const mysqlURLEnv = "PIPEROOM_IT_MYSQL_URL"

// connectorUserEnv names the connector's database user (default debezium,
// design §10.2).
const connectorUserEnv = "PIPEROOM_IT_MYSQL_CONNECTOR_USER"

func TestMySQLIntegration(t *testing.T) {
	raw := os.Getenv(mysqlURLEnv)
	if raw == "" {
		t.Skipf("mysql integration: %s is not set (the dev compose profile \"mysql\", Q8a, is not in this run)", mysqlURLEnv)
	}
	host := "(not a URL)"
	if u, err := url.Parse(raw); err == nil {
		host = u.Host // never the password
	}
	ctx := context.Background()
	d, err := New(ctx, raw)
	if err != nil {
		t.Fatalf("New(%s): %v", host, err)
	}
	t.Cleanup(d.Close)
	connUser := os.Getenv(connectorUserEnv)
	if connUser == "" {
		connUser = "debezium"
	}

	t.Run("read-only session", func(t *testing.T) {
		var ro, met int64
		if err := d.db.QueryRowContext(ctx, `SELECT @@session.transaction_read_only, @@session.max_execution_time`).Scan(&ro, &met); err != nil {
			t.Fatal(err)
		}
		if ro != 1 || met != 10000 {
			t.Fatalf("transaction_read_only=%d max_execution_time=%d", ro, met)
		}
		if _, err := d.db.ExecContext(ctx, "CREATE TABLE it_q8b_ro (id INT PRIMARY KEY)"); err == nil {
			_, _ = d.db.ExecContext(ctx, "DROP TABLE it_q8b_ro")
			t.Fatal("the read pool created a table")
		}
	})

	t.Run("not supported", func(t *testing.T) {
		if _, err := d.ReplicationSlots(ctx); !errors.Is(err, clients.ErrNotSupported) {
			t.Fatal(err)
		}
		if _, err := d.RecentDDL(ctx, time.Now().Add(-time.Hour), 10); !errors.Is(err, clients.ErrNotSupported) {
			t.Fatal(err)
		}
	})

	t.Run("settings", func(t *testing.T) {
		got, err := d.Settings(ctx)
		if err != nil {
			t.Fatal(err)
		}
		vals := map[string]string{}
		for _, s := range got {
			vals[s.Name] = s.Value
		}
		// The profile runs --gtid-mode=ON --binlog-format=ROW --binlog-row-image=FULL (design §10.1).
		for name, want := range map[string]string{"log_bin": "ON", "binlog_format": "ROW", "binlog_row_image": "FULL", "gtid_mode": "ON"} {
			if vals[name] != want {
				t.Errorf("%s = %q, want %q (all: %+v)", name, vals[name], want, got)
			}
		}
		if vals["version"] == "" || vals["server_id"] == "" {
			t.Errorf("version or server_id missing: %+v", got)
		}
	})

	t.Run("table replication", func(t *testing.T) {
		got, err := d.TableReplication(ctx, []string{"shop.orders"})
		if err != nil {
			t.Fatal(err)
		}
		if !got[0].HasPrimaryKey || got[0].Engine != "InnoDB" || got[0].ReplicaIdentity != "" {
			t.Fatalf("got %+v", got)
		}
		if _, err := d.TableReplication(ctx, []string{"shop.it_q8b_missing"}); !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("missing table: %v", err)
		}
	})

	t.Run("role info", func(t *testing.T) {
		got, err := d.RoleInfo(ctx, connUser, []string{"shop.orders"})
		if err != nil {
			t.Fatal(err)
		}
		if !got.Exists || !got.CanLogin || !got.Replication || !got.Selectable["shop.orders"] || len(got.Accounts) == 0 {
			t.Fatalf("connector user %s: %+v", connUser, got)
		}
		none, err := d.RoleInfo(ctx, "it_q8b_nobody", []string{"shop.orders"})
		if err != nil || none.Exists {
			t.Fatalf("missing user: %+v %v", none, err)
		}
	})

	t.Run("row", func(t *testing.T) {
		if _, _, err := d.Row(ctx, "shop.orders", "id", "1"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("binlog status", func(t *testing.T) {
		st, err := d.BinlogStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if st.File == "" || st.Pos <= 0 || len(st.Binlogs) == 0 || st.ServerID == 0 || st.GTIDMode != "ON" {
			t.Fatalf("st = %+v", st)
		}
		if _, err := gtid.Parse(st.GTIDExecuted); err != nil {
			t.Fatalf("gtid_executed %q: %v", st.GTIDExecuted, err)
		}
		if _, err := gtid.Parse(st.GTIDPurged); err != nil {
			t.Fatalf("gtid_purged %q: %v", st.GTIDPurged, err)
		}
	})

	t.Run("gtid arithmetic matches the server", func(t *testing.T) {
		const u1, u2 = "3e11fa47-71ca-11e1-9e33-c80aa9429562", "4f22fb58-82db-22f2-af44-d91bba530673"
		pairs := [][2]string{
			{"", ""},
			{u1 + ":1-10", ""},
			{"", u1 + ":1"},
			{u1 + ":1-10", u1 + ":3-5"},
			{u1 + ":1-10", u1 + ":5-12"},
			{u1 + ":1-3:7-9", u1 + ":2:8"},
			{u1 + ":1-20", u1 + ":2-3:5:9-30"},
			{u1 + ":1-5," + u2 + ":1-5", u2 + ":2-3"},
			{u1 + ":1-5", u2 + ":1"},
		}
		for _, p := range pairs {
			var subset int64
			var diff string
			if err := d.db.QueryRowContext(ctx, `SELECT GTID_SUBSET(?, ?), GTID_SUBTRACT(?, ?)`, p[1], p[0], p[0], p[1]).Scan(&subset, &diff); err != nil {
				t.Fatalf("%v: %v", p, err)
			}
			a, err := gtid.Parse(p[0])
			if err != nil {
				t.Fatal(err)
			}
			b, err := gtid.Parse(p[1])
			if err != nil {
				t.Fatal(err)
			}
			if a.Contains(b) != (subset == 1) {
				t.Errorf("GTID_SUBSET(%q, %q) = %d, gtid says %v", p[1], p[0], subset, a.Contains(b))
			}
			want, err := gtid.Parse(diff)
			if err != nil {
				t.Fatalf("server's GTID_SUBTRACT %q: %v", diff, err)
			}
			if got := a.Subtract(b); !got.Equal(want) {
				t.Errorf("GTID_SUBTRACT(%q, %q) = %q, gtid says %q", p[0], p[1], diff, got)
			}
		}
	})

	t.Run("blockers and long transactions", func(t *testing.T) {
		conn, err := d.db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := conn.ExecContext(ctx, "START TRANSACTION READ ONLY"); err != nil {
			t.Fatal(err)
		}
		// A read-only InnoDB transaction is listed once it has read a table.
		var n int64
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM shop.orders").Scan(&n); err != nil {
			t.Fatal(err)
		}
		defer func() { _, _ = conn.ExecContext(ctx, "ROLLBACK") }()
		var pid int64
		if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&pid); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1100 * time.Millisecond)

		txs, err := d.LongTransactions(ctx, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, tx := range txs {
			found = found || int64(tx.PID) == pid
		}
		if !found {
			t.Errorf("transaction of pid %d not in %+v", pid, txs)
		}

		all, err := d.Blockers(ctx, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		scoped, err := d.BlockersOnTables(ctx, time.Second, []string{"shop.order_items"})
		if err != nil {
			t.Fatal(err)
		}
		var full, anon *DBSession
		for i := range all {
			if all[i].PID == pid {
				full = &all[i]
			}
		}
		for i := range scoped {
			if scoped[i].PID == pid {
				anon = &scoped[i]
			}
		}
		if full == nil || !full.InScope || full.User == "" || full.App != readerProgram || full.StartedAt.IsZero() || !full.OldXact {
			t.Errorf("unscoped: %+v", full)
		}
		if anon == nil || anon.InScope || anon.User != "" || anon.App != "" || anon.Query != "" {
			t.Errorf("scoped to another table: %+v", anon)
		}
	})
}
