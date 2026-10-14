package memstore_test

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

func TestPurgeClosedBeforeExcludeAndExamined(t *testing.T) {
	st := memstore.New()
	ctx := context.Background()
	closed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{"r1", "r2", "r3"} {
		if err := st.CreateRoom(ctx, store.Room{ID: id, Status: "open", OpenedAt: closed}); err != nil {
			t.Fatal(err)
		}
		if err := st.CloseRoom(ctx, id, closed); err != nil {
			t.Fatal(err)
		}
	}
	cutoff := closed.Add(time.Hour)

	res, err := st.PurgeClosedBefore(ctx, cutoff, 10, []string{"r2"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Examined != 2 || len(res.RoomIDs) != 2 {
		t.Fatalf("want r1 and r3 purged, got %+v", res)
	}
	if _, err := st.GetRoom(ctx, "r2"); err != nil {
		t.Fatalf("excluded room must stay: %v", err)
	}
	res, err = st.PurgeClosedBefore(ctx, cutoff, 10, nil)
	if err != nil || res.Examined != 1 || len(res.RoomIDs) != 1 {
		t.Fatalf("second call: %+v %v", res, err)
	}
}
