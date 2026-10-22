package room_test

import (
	"context"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/store/memstore"
)

// sinceRecorder records the limit each Since call asks for, so a test can see
// the clamp the service applies before it reads the log. Events is called
// synchronously in these tests, so the slice needs no lock.
type sinceRecorder struct {
	*memstore.Store
	limits []int
}

func (r *sinceRecorder) Since(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error) {
	r.limits = append(r.limits, limit)
	return r.Store.Since(ctx, roomID, afterSeq, limit)
}

// TestEventsLimit: KAN-395. An explicit limit=0 is an empty page carrying the
// room's last_seq rather than the 500 default; a negative limit is invalid;
// an absent limit (500, as the HTTP layer passes it) and the 1000 cap are
// unchanged, and so is paging after a seq.
func TestEventsLimit(t *testing.T) {
	cases := []struct {
		name       string
		after      int64
		limit      int
		wantEvents int
		wantErr    string // error code, when the limit is invalid
		wantSince  int    // limit passed to Store.Since; -1 = no log read
	}{
		{"zero is an empty page", 0, 0, 0, "", -1},
		{"zero with after is still an empty page", 1, 0, 0, "", -1},
		{"negative is invalid", 0, -1, 0, "validation", -1},
		{"one event", 0, 1, 1, "", 2},
		{"page after a seq", 1, 1, 1, "", 2},
		{"default page size", 0, 500, 2, "", 501},
		{"at the cap", 0, 1000, 2, "", 1001},
		{"over the cap is clamped", 0, 1001, 2, "", 1001},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := &sinceRecorder{Store: memstore.New()}
			svc, _, _ := guardSetup(t, st)
			roomID := createRoom(t, svc, "arjun").ID

			head, err := svc.GetRoom(ctx, roomID)
			if err != nil {
				t.Fatalf("get room: %v", err)
			}
			if head.LastSeq == 0 {
				t.Fatalf("room has no events: %+v", head)
			}

			evs, last, err := svc.Events(ctx, roomID, tc.after, tc.limit)
			if tc.wantErr != "" {
				wantCode(t, err, tc.wantErr)
				if evs != nil || last != 0 {
					t.Fatalf("invalid limit returned events %v and last_seq %d, want nil and 0", evs, last)
				}
			} else {
				if err != nil {
					t.Fatalf("events: %v", err)
				}
				if evs == nil {
					t.Fatal("events is nil, want a JSON array")
				}
				if len(evs) != tc.wantEvents {
					t.Fatalf("events = %d, want %d", len(evs), tc.wantEvents)
				}
				// Every page holds the room's current last_seq, including the
				// empty one: that is what lets limit=0 poll the log head.
				if last != head.LastSeq {
					t.Fatalf("last_seq = %d, want the room's %d", last, head.LastSeq)
				}
			}

			if tc.wantSince < 0 {
				if len(st.limits) != 0 {
					t.Fatalf("Store.Since called with limits %v, want no read", st.limits)
				}
			} else if len(st.limits) != 1 || st.limits[0] != tc.wantSince {
				t.Fatalf("Store.Since limits = %v, want [%d]", st.limits, tc.wantSince)
			}
		})
	}
}
