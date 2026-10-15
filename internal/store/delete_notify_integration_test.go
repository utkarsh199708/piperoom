//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/store"
)

// A room deleted on replica A ends replica B's live subscription within ~2 s
// through the delete's NOTIFY, far inside the 15 s safety poll. With LISTEN
// off, the poll fallback ends it.
func TestSubscribeEndsOnRemoteDelete(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts store.Options
		wait time.Duration
	}{
		{"notify", store.Options{}, 2 * time.Second},
		{"poll fallback", store.Options{NoListen: true, PollInterval: 300 * time.Millisecond}, 3 * time.Second},
	} {
		for _, how := range []string{"delete", "purge", "empty"} {
			t.Run(tc.name+"/"+how, func(t *testing.T) {
				a, _, schema := requireStoreSchema(t)
				b := openReplica(t, schema, tc.opts)
				roomID := createTestRoom(t, a)
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				if how != "empty" {
					appendComment(ctx, t, a, roomID, 0)
				}
				if how == "purge" {
					if err := a.CloseRoom(ctx, roomID, time.Now().Add(-48*time.Hour)); err != nil {
						t.Fatal(err)
					}
				}
				ch, err := b.Subscribe(ctx, roomID, 1)
				if err != nil {
					t.Fatal(err)
				}
				if !tc.opts.NoListen {
					if err := b.WaitHubListening(ctx); err != nil {
						t.Fatalf("replica b never started listening: %v", err)
					}
				}
				// Let the feed's first read finish so the wake comes from the notify.
				time.Sleep(300 * time.Millisecond)
				switch how {
				case "delete":
					_, _, err = a.DeleteRoom(ctx, store.RoomDeletion{RoomID: roomID, DeletedBy: "admin", Reason: "test"}, -1)
				case "purge":
					_, err = a.PurgeClosedBefore(ctx, time.Now().Add(-24*time.Hour), 10, nil)
				case "empty":
					err = a.DeleteEmptyRoom(ctx, roomID)
				}
				if err != nil {
					t.Fatal(err)
				}
				start := time.Now()
				select {
				case e, ok := <-ch:
					if ok {
						t.Fatalf("got event %+v, want the channel closed", e)
					}
				case <-time.After(tc.wait):
					t.Fatalf("subscription still open %s after the remote delete", tc.wait)
				}
				t.Logf("closed %s after the delete", time.Since(start))
			})
		}
	}
}
