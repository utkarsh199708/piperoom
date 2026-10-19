//go:build integration

package clickhouse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	driver "github.com/ClickHouse/clickhouse-go/v2"
)

func TestClickHouseIntegration(t *testing.T) {
	readDSN := os.Getenv("CLICKHOUSE_INTEGRATION_DSN")
	adminDSN := os.Getenv("CLICKHOUSE_INTEGRATION_ADMIN_DSN")
	if readDSN == "" || adminDSN == "" {
		t.Skip("set CLICKHOUSE_INTEGRATION_DSN and CLICKHOUSE_INTEGRATION_ADMIN_DSN to run ClickHouse integration tests")
	}

	readOpts, err := driver.ParseDSN(readDSN)
	if err != nil {
		t.Fatalf("parse integration read DSN: %v", Redact(err, readDSN))
	}
	if readOpts.Auth.Database == "" {
		t.Fatal("integration read DSN must name a database")
	}
	adminOpts, err := driver.ParseDSN(adminDSN)
	if err != nil {
		t.Fatalf("parse integration admin DSN: %v", Redact(err, adminDSN))
	}

	admin := driver.OpenDB(adminOpts)
	pingCtx, pingCancel := context.WithTimeout(context.Background(), defaultTimeout)
	if err := admin.PingContext(pingCtx); err != nil {
		pingCancel()
		_ = admin.Close()
		t.Fatalf("connect integration admin target: %v", Redact(err, adminDSN))
	}
	pingCancel()

	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		_ = admin.Close()
		t.Fatalf("generate integration table name: %v", err)
	}
	tableName := "it_" + hex.EncodeToString(random) + "_sink"
	table := readOpts.Auth.Database + "." + tableName
	qualified, err := quoteTable(table)
	if err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()
		_, _ = admin.ExecContext(ctx, "DROP TABLE IF EXISTS "+qualified)
		_ = admin.Close()
	})

	create := fmt.Sprintf(
		"CREATE TABLE %s (id UInt64, version UInt64, amount Decimal(18, 4), loaded_at DateTime64(6, 'Asia/Kolkata')) ENGINE = ReplacingMergeTree(version) ORDER BY id",
		qualified,
	)
	execCtx, execCancel := context.WithTimeout(context.Background(), defaultTimeout)
	_, err = admin.ExecContext(execCtx, create)
	execCancel()
	if err != nil {
		t.Fatalf("create integration table: %v", Redact(err, adminDSN))
	}

	insert := "INSERT INTO " + qualified + " (id, version, amount, loaded_at) VALUES " +
		"(1, 1, 1.2500, '2026-10-19 12:00:00.123456'), " +
		"(1, 2, 2.7500, '2026-10-19 12:00:01.123456')"
	execCtx, execCancel = context.WithTimeout(context.Background(), defaultTimeout)
	_, err = admin.ExecContext(execCtx, insert)
	execCancel()
	if err != nil {
		t.Fatalf("insert integration rows: %v", Redact(err, adminDSN))
	}

	readClient, err := New(context.Background(), readDSN)
	if err != nil {
		t.Fatalf("open read-only client: %v", err)
	}
	t.Cleanup(func() { _ = readClient.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	fresh, err := readClient.Freshness(ctx, table, "loaded_at")
	cancel()
	wantFresh := time.Date(2026, 10, 19, 12, 0, 1, 123456000, time.FixedZone("IST", 5*60*60+30*60)).UTC()
	if err != nil || !fresh.Equal(wantFresh) {
		t.Fatalf("Freshness = %s, %v; want %s", fresh.Format(time.RFC3339Nano), err, wantFresh.Format(time.RFC3339Nano))
	}

	ctx, cancel = context.WithTimeout(context.Background(), defaultTimeout)
	row, found, err := readClient.Row(ctx, table, "id", "1")
	cancel()
	if err != nil || !found {
		t.Fatalf("Row found=%t, err=%v; want one row", found, err)
	}
	if row["version"] != "2" {
		t.Fatalf("ReplacingMergeTree version = %#v, want newest version 2", row["version"])
	}
	amount, ok := row["amount"].(string)
	if !ok || strings.TrimRight(strings.TrimRight(amount, "0"), ".") != "2.75" {
		t.Fatalf("ReplacingMergeTree amount = %#v, want exact decimal 2.75", row["amount"])
	}

	since := time.Date(2026, 10, 19, 11, 59, 30, 250000000, time.FixedZone("IST", 5*60*60+30*60))
	ctx, cancel = context.WithTimeout(context.Background(), defaultTimeout)
	counts, err := readClient.CountByBucket(ctx, table, "loaded_at", since, since.Add(2*time.Minute), time.Minute)
	cancel()
	if err != nil {
		t.Fatalf("CountByBucket: %v", err)
	}
	if len(counts) != 1 || counts[0].Count != 2 || !counts[0].Bucket.Equal(since.UTC()) {
		t.Fatalf("CountByBucket = %#v, want one since-anchored bucket with 2 rows", counts)
	}

	loadSince := time.Now().UTC().Add(-time.Minute)
	badInsert := "INSERT INTO " + qualified + " (id, version, amount, loaded_at) VALUES ('not_uint', 3, 3.0, '2026-10-19 12:00:02')"
	execCtx, execCancel = context.WithTimeout(context.Background(), defaultTimeout)
	_, insertErr := admin.ExecContext(execCtx, badInsert)
	execCancel()
	if insertErr == nil {
		t.Fatal("invalid integration insert succeeded")
	}
	execCtx, execCancel = context.WithTimeout(context.Background(), defaultTimeout)
	_, err = admin.ExecContext(execCtx, "SYSTEM FLUSH LOGS")
	execCancel()
	if err != nil {
		t.Fatalf("flush integration query log: %v", Redact(err, adminDSN))
	}

	ctx, cancel = context.WithTimeout(context.Background(), defaultTimeout)
	loadErrors, err := readClient.LoadErrors(ctx, table, loadSince, 20)
	cancel()
	if err != nil {
		t.Fatalf("LoadErrors: %v", err)
	}
	foundFailure := false
	for _, loadError := range loadErrors {
		if loadError.Status == "LOAD_FAILED" && strings.Contains(loadError.FirstError, "code") {
			foundFailure = true
			if strings.Contains(loadError.FirstError, "not_uint") {
				t.Fatalf("load error leaked failed row text: %q", loadError.FirstError)
			}
		}
	}
	if !foundFailure {
		t.Fatalf("LoadErrors did not report the failed INSERT: %#v", loadErrors)
	}

}
