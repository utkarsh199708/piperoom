//go:build integration

package oracle

import (
	"context"
	"testing"
	"time"

	oraclient "piperoom/internal/clients/oracle"
	"piperoom/internal/testenv"
)

// TestPrerequisitesIntegration runs every check against the `oracle` dev
// profile (dev/oracle/README.md) after dev/oracle/cdc-setup.sh: Oracle
// Database Free in ARCHIVELOG mode, minimal supplemental logging, the common
// LogMiner user C##DBZUSER with the Debezium docs' grants, and PIPEROOM.ORDERS
// in FREEPDB1 with its ALL COLUMNS log group. Every check must pass there; the
// devctl breaks (no-archivelog, no-supplemental-logging,
// missing-logminer-grant) each fail one of them (cmd/devctl's own integration
// test drives those).
func TestPrerequisitesIntegration(t *testing.T) {
	dsn := testenv.RequireOracle(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := oraclient.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	d := Driver{User: "C##DBZUSER", Tables: []string{"PIPEROOM.ORDERS"}, PDB: "FREEPDB1"}
	results, err := d.Run(ctx, c)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, r := range results {
		if !r.OK {
			fix, ask := r.Check.Fix(d, r.Evidence)
			t.Errorf("%s failed on the dev profile: err=%v fix=%q ask=%q", r.Check.ID, r.Err, fix, ask)
		}
	}

	db := results[0].Evidence.DB
	if _, err := ConnectorConfig(ConnectorOptions{
		Host: "oracle", Database: db.Name, PDB: d.PDB, User: d.User,
		PasswordSecret: "source_read_password", TopicPrefix: "it_oracle",
		Tables: d.Tables, SchemaHistoryServers: []string{"kafka:9092"},
	}); err != nil {
		t.Fatalf("ConnectorConfig from the profile's database name %q: %v", db.Name, err)
	}
}
