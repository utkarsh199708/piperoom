//go:build integration && standby

package postgres

import (
	"context"
	"os"
	"testing"
)

// TestSlotReadsOnHotStandby runs against a hot standby that has a logical
// slot (PG 16+, #420). The dev stack has no standby, and the full-stack CI job
// fails on any skipped test, so it has its own tag:
//
//	PIPEROOM_IT_STANDBY_URL=<standby DSN> go test -tags integration,standby -run TestSlotReadsOnHotStandby ./internal/clients/postgres/
//
// The standby: a wal_level=logical primary, pg_basebackup -R, and
// pg_create_logical_replication_slot on the standby.
func TestSlotReadsOnHotStandby(t *testing.T) {
	dsn := os.Getenv("PIPEROOM_IT_STANDBY_URL")
	if dsn == "" {
		t.Fatal("PIPEROOM_IT_STANDBY_URL not set: the standby tag needs a hot standby to test against")
	}
	ctx := context.Background()
	db, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("standby unreachable (%s): %v", hostPort(dsn), err)
	}
	t.Cleanup(db.Close)

	var inRecovery bool
	if err := db.queryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&inRecovery); err != nil {
		t.Fatalf("pg_is_in_recovery: %v", err)
	}
	if !inRecovery {
		t.Fatalf("PIPEROOM_IT_STANDBY_URL (%s) is not a standby", hostPort(dsn))
	}

	slots, err := db.ReplicationSlots(ctx)
	if err != nil {
		t.Fatalf("ReplicationSlots on a standby: %v", err)
	}
	var logical []string
	for _, s := range slots {
		if s.RetainedWALBytes < 0 {
			t.Fatalf("slot %s: retained %d < 0", s.Name, s.RetainedWALBytes)
		}
		if s.Plugin != "" {
			logical = append(logical, s.Name)
		}
	}
	if len(logical) == 0 {
		t.Fatalf("the standby has no logical slot to read: %+v", slots)
	}

	named, other, err := db.ReplicationSlotsNamed(ctx, logical[:1])
	if err != nil {
		t.Fatalf("ReplicationSlotsNamed on a standby: %v", err)
	}
	if len(named) != 1 || named[0].Name != logical[0] || other != len(slots)-1 {
		t.Fatalf("named = %+v, other = %d, want %s and %d", named, other, logical[0], len(slots)-1)
	}
}
