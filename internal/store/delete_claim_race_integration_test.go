//go:build integration

package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/config"
	"piperoom/internal/store"
)

// A claim another replica is writing while the delete runs must not be lost:
// the delete waits for it and refuses, instead of erasing the proposal under
// an executor that is about to act. Without the row lock, the delete does not
// see the uncommitted claim and deletes the proposal once it commits.
func TestDeleteRoomWaitsForConcurrentClaim(t *testing.T) {
	s, _, schema := requireStoreSchema(t)
	roomID := createTestRoom(t, s)
	createTestProposal(t, s, roomID, "p_race")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cc, err := pgx.ParseConfig(config.Load().RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	cc.RuntimeParams["search_path"] = schema
	conn, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	// Replica B's claim, written but not yet committed.
	claim, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = claim.Rollback(context.Background()) }()
	if tag, err := claim.Exec(ctx,
		`UPDATE proposals SET claimed_by = 'replica-b', claimed_at = now() WHERE proposal_id = $1 AND claimed_by IS NULL AND status = 'open'`,
		"p_race"); err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("claim: %v (%d rows)", err, tag.RowsAffected())
	}

	done := make(chan error, 1)
	go func() {
		d := store.RoomDeletion{RoomID: roomID, DeletedBy: "ada", DeletedAt: time.Now(), Reason: "cleanup"}
		_, _, err := s.DeleteRoom(ctx, d, -1)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("delete finished while a claim was being written: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	if err := claim.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, store.ErrRoomExecuting) {
		t.Fatalf("delete after a concurrent claim: %v, want ErrRoomExecuting", err)
	}
	if _, err := s.GetRoom(ctx, roomID); err != nil {
		t.Fatalf("room gone: %v", err)
	}
}
