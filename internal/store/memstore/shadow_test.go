package memstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

var agentActor = events.Actor{Kind: events.ActorAgent, ID: "agent", Name: "Agent"}

func shadowRoom(t *testing.T, st *memstore.Store, id string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if err := st.CreateRoom(ctx, store.Room{ID: id, PipelineID: "orders", Status: "open", OpenedAt: at}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, id, events.RoomCreated, events.Actor{Kind: events.ActorSystem, ID: "system"},
		events.RoomCreatedPayload{Title: "t", PipelineID: "orders", AlertSource: "watcher", Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateShadowRun(ctx, store.ShadowRun{RoomID: id, Status: store.ShadowQueued}); err != nil {
		t.Fatal(err)
	}
}

func TestShadowStoreSemantics(t *testing.T) {
	now := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); now = now.Add(time.Second); return now }
	st := memstore.NewWithClock(clock)
	ctx := context.Background()
	shadowRoom(t, st, "r1", now)

	if err := st.CreateShadowRun(ctx, store.ShadowRun{RoomID: "missing", Status: "queued"}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("run for unknown room: %v", err)
	}
	if err := st.CreateShadowRun(ctx, store.ShadowRun{RoomID: "r1", Status: "running"}); err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	if r, _ := st.GetShadowRun(ctx, "r1"); r.Status != store.ShadowQueued {
		t.Fatalf("second create replaced the row: %+v", r)
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := st.AppendShadow(ctx, "r1", events.AgentToolCall, agentActor, events.AgentToolCallPayload{Tool: "x"}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	evs, err := st.ShadowSince(ctx, "r1", 0, 100)
	if err != nil || len(evs) != 20 {
		t.Fatalf("shadow log: %d %v", len(evs), err)
	}
	for i, e := range evs {
		if e.Seq != int64(i+1) {
			t.Fatalf("seq %d at %d", e.Seq, i)
		}
	}
	if room, _ := st.Since(ctx, "r1", 0, 100); len(room) != 1 {
		t.Fatalf("room log grew: %d events", len(room))
	}
	if _, err := st.AppendShadow(ctx, "r1", events.AgentMessage, agentActor, events.AgentMessagePayload{Text: "a\x00"}); !errors.Is(err, store.ErrUnstorable) {
		t.Fatalf("NUL: %v", err)
	}

	for _, txt := range []string{"first", "final"} {
		if _, err := st.AppendShadow(ctx, "r1", events.AgentHypothesis, agentActor, events.AgentHypothesisPayload{Text: txt}); err != nil {
			t.Fatal(err)
		}
	}
	r, _ := st.GetShadowRun(ctx, "r1")
	var h events.AgentHypothesisPayload
	_ = json.Unmarshal(r.FinalHypothesis, &h)
	if h.Text != "final" || r.FirstHypothesisAt == nil || !r.FinalHypothesisAt.After(*r.FirstHypothesisAt) || r.LastSeq != 22 {
		t.Fatalf("run after hypotheses: %+v", r)
	}
	if n, _ := st.CountShadowEvents(ctx, "r1", events.AgentToolCall); n != 20 {
		t.Fatalf("count = %d", n)
	}

	fin := now
	r.Status, r.FinishedAt = store.ShadowHypothesised, &fin
	if err := st.UpdateShadowRun(ctx, r); err != nil {
		t.Fatal(err)
	}
	if un, _ := st.UnrevealedShadowRuns(ctx); len(un) != 0 {
		t.Fatalf("open room listed as unrevealed: %v", un)
	}
	if list, _ := st.ListShadowRuns(ctx, "", time.Time{}); len(list) != 0 {
		t.Fatalf("open room listed: %v", list)
	}
	closedAt := now.Add(time.Minute)
	if err := st.CloseRoom(ctx, "r1", closedAt); err != nil {
		t.Fatal(err)
	}
	if un, _ := st.UnrevealedShadowRuns(ctx); len(un) != 1 {
		t.Fatalf("unrevealed = %v", un)
	}
	list, err := st.ListShadowRuns(ctx, "orders", closedAt)
	if err != nil || len(list) != 1 || list[0].AlertSource != "watcher" || !list[0].ClosedAt.Equal(closedAt) {
		t.Fatalf("list = %+v %v", list, err)
	}
	if list, _ := st.ListShadowRuns(ctx, "payments", time.Time{}); len(list) != 0 {
		t.Fatal("pipeline filter")
	}
	if list, _ := st.ListShadowRuns(ctx, "", closedAt.Add(time.Second)); len(list) != 0 {
		t.Fatal("since filter")
	}
	if first, err := st.MarkRevealed(ctx, "r1", now); !first || err != nil {
		t.Fatalf("first reveal %v %v", first, err)
	}
	if first, err := st.MarkRevealed(ctx, "r1", now); first || err != nil {
		t.Fatalf("second reveal %v %v", first, err)
	}
	r.RevealedAt = nil
	if err := st.UpdateShadowRun(ctx, r); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.GetShadowRun(ctx, "r1"); got.RevealedAt == nil {
		t.Fatal("update cleared revealed_at")
	}

	res, err := st.PurgeClosedBefore(ctx, closedAt.Add(time.Hour), 10, nil)
	if err != nil || res.ShadowRuns != 1 || res.ShadowEvents != 22 {
		t.Fatalf("purge = %+v %v", res, err)
	}
	if _, err := st.GetShadowRun(ctx, "r1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("run survived purge: %v", err)
	}
}
