//go:build integration

package oracle

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	go_ora "github.com/sijms/go-ora/v2"

	oracleclient "piperoom/internal/clients/oracle"
	"piperoom/internal/tools"
)

func TestOracleSetupDriverIntegration(t *testing.T) {
	dsn := os.Getenv("ORACLE_DSN")
	if dsn == "" {
		t.Skip("set ORACLE_DSN to the Oracle dev profile URL")
	}
	ctx, cancel := context.WithTimeout(tools.WithUnscoped(context.Background()), 90*time.Second)
	defer cancel()

	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatalf("random table suffix: %v", err)
	}
	source := "PIPEROOM.it_" + hex.EncodeToString(suffix[:])
	canary := source + "_canary"
	quotedSource := "\"PIPEROOM\".\"" + source[len("PIPEROOM."):] + "\""
	quotedCanary := "\"PIPEROOM\".\"" + canary[len("PIPEROOM."):] + "\""

	admin := sql.OpenDB(go_ora.NewConnector(dsn))
	admin.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("Oracle profile unreachable: %v", err)
	}

	sourceCreated := false
	canaryCreated := false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(tools.WithUnscoped(context.Background()), 20*time.Second)
		defer cleanupCancel()
		if canaryCreated {
			if _, err := admin.ExecContext(cleanupCtx, "DROP TABLE "+quotedCanary+" PURGE"); err != nil {
				t.Errorf("cleanup canary: %v", err)
			}
		}
		if sourceCreated {
			if _, err := admin.ExecContext(cleanupCtx, "DROP TABLE "+quotedSource+" PURGE"); err != nil {
				t.Errorf("cleanup source table: %v", err)
			}
		}
	})

	if _, err := admin.ExecContext(ctx, "CREATE TABLE "+quotedSource+" (\"ID\" NUMBER PRIMARY KEY, \"NOTE\" VARCHAR2(64))"); err != nil {
		t.Fatalf("create unique source table: %v", err)
	}
	sourceCreated = true

	inner, err := oracleclient.NewWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("open Oracle setup writer: %v", err)
	}
	t.Cleanup(func() { _ = inner.Close() })
	writer, err := NewWriter(inner)
	if err != nil {
		t.Fatalf("wrap Oracle setup writer: %v", err)
	}
	if err := writer.CreateCanaryTable(ctx, canary); err != nil {
		t.Fatalf("create unique canary table: %v", err)
	}
	canaryCreated = true
	if err := writer.CreatePublication(ctx, "", []string{source}); err != nil {
		t.Fatalf("add source table supplemental logging: %v", err)
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("random canary nonce: %v", err)
	}
	nonceHex := hex.EncodeToString(nonce)
	if err := writer.InsertCanary(ctx, canary, nonceHex); err != nil {
		t.Fatalf("insert unique canary: %v", err)
	}
	if _, err := admin.ExecContext(ctx, "GRANT SELECT ON "+quotedSource+" TO C##DBZUSER"); err != nil {
		t.Fatalf("grant LogMiner user source table read: %v", err)
	}
	if _, err := admin.ExecContext(ctx, "GRANT SELECT ON "+quotedCanary+" TO C##DBZUSER"); err != nil {
		t.Fatalf("grant LogMiner user canary read: %v", err)
	}
	var got string
	if err := admin.QueryRowContext(ctx, "SELECT \"NONCE\" FROM "+quotedCanary).Scan(&got); err != nil {
		t.Fatalf("read unique canary: %v", err)
	}
	if got != nonceHex {
		t.Fatalf("canary nonce = %q, want generated nonce", got)
	}

	reader, err := oracleclient.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open Oracle setup reader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	driver := Driver{
		Database: "FREE", PDB: "FREEPDB1", LogMinerUser: "C##DBZUSER",
		Tables: []string{source}, CanaryTable: canary,
	}
	results, err := driver.Run(ctx, reader)
	if err != nil {
		t.Fatalf("run Oracle prerequisites: %v", err)
	}
	for _, result := range results {
		if !result.OK {
			t.Errorf("%s failed: %v", result.Check.ID, result.Err)
		}
	}

	config, err := ConnectorConfig(ConnectorOptions{
		Host: "oracle", Port: 1521, Database: "FREE", PDB: "FREEPDB1",
		User: "C##DBZUSER", PasswordSecret: "source_read_password",
		TopicPrefix: "it_" + hex.EncodeToString(suffix[:]), Tables: []string{source}, CanaryTable: canary,
	})
	if err != nil {
		t.Fatalf("build Oracle connector config: %v", err)
	}
	if config["table.include.list"] != fmt.Sprintf("%s,%s", source, canary) {
		t.Fatalf("table.include.list = %q", config["table.include.list"])
	}
	if config["database.password"] != "$"+"{secret:source_read_password}" {
		t.Fatal("connector config did not retain a secret reference")
	}
}
