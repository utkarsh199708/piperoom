package room

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/store/memstore"
)

func TestApplyAgentTriage(t *testing.T) {
	at := time.Date(2026, 10, 14, 3, 12, 0, 0, time.UTC)
	raw, _ := json.Marshal(events.AgentTriagePayload{Status: "ok", Headline: "task 0 FAILED", SuspectedLayer: "kafka-connect",
		Confidence: "medium", EvidenceRefs: []string{"s_1"}, NextCheck: "diff", Model: "m", TookMS: 900})
	snap := Apply(NewSnapshot(), events.Event{Seq: 1, Type: events.AgentTriage, Actor: events.AgentActor, Payload: raw, At: at})
	if snap.Triage == nil || snap.Triage.Headline != "task 0 FAILED" || snap.Triage.At != at || snap.Triage.SuspectedLayer != "kafka-connect" {
		t.Fatalf("triage = %+v", snap.Triage)
	}
	// A hypothesis does not clear it; a newer triage replaces it.
	hyp, _ := json.Marshal(events.AgentHypothesisPayload{Text: "h", BasedOnSteps: []string{}, Confidence: "low"})
	snap = Apply(snap, events.Event{Seq: 2, Type: events.AgentHypothesis, Actor: events.AgentActor, Payload: hyp, At: at})
	if snap.Triage == nil {
		t.Fatal("agent.hypothesis cleared triage")
	}
	failed, _ := json.Marshal(events.AgentTriagePayload{Status: "failed", Reason: "timeout"})
	snap = Apply(snap, events.Event{Seq: 3, Type: events.AgentTriage, Actor: events.AgentActor, Payload: failed, At: at})
	if snap.Triage.Status != "failed" || snap.Triage.Reason != "timeout" || snap.Triage.EvidenceRefs == nil {
		t.Fatalf("triage = %+v (evidence_refs must never be null)", snap.Triage)
	}
	b, _ := json.Marshal(NewSnapshot())
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if v, ok := m["triage"]; !ok || v != nil {
		t.Fatalf("empty snapshot triage = %v (present %v), want null", v, ok)
	}
	// Clone does not share the refs.
	c := snap.Clone()
	c.Triage.EvidenceRefs = append(c.Triage.EvidenceRefs, "x")
	if len(snap.Triage.EvidenceRefs) != 0 {
		t.Fatal("Clone shares evidence_refs")
	}
}

func newHubService(t *testing.T) *Service {
	t.Helper()
	s := New(Deps{Store: memstore.New(), Users: DefaultUsers(), Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
		<-ctx.Done()
		return nil
	}})
	t.Cleanup(s.Close)
	return s
}

func TestListHubCoalesces(t *testing.T) {
	s := newHubService(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.SubscribeRooms(ctx)
	u, _ := s.users.Get("arjun")
	snap, err := s.CreateRoom(context.Background(), u, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	// Hold the service lock while many events fold: the hub cannot copy
	// until it is released, so the burst becomes few updates.
	s.mu.Lock()
	st := s.rooms[snap.ID]
	for i := range 50 {
		if _, err := s.emitLocked(context.Background(), st, snap.ID, events.AgentMessage, events.AgentActor, events.AgentMessagePayload{Text: "m" + string(rune('a'+i%26))}); err != nil {
			s.mu.Unlock()
			t.Fatal(err)
		}
	}
	s.mu.Unlock()

	got := 0
	var last *RoomSnapshot
	deadline := time.After(2 * time.Second)
	for last == nil || last.LastSeq < st.snap.LastSeq {
		select {
		case up := <-ch:
			if up.Room != nil && up.Room.ID == snap.ID {
				got++
				last = up.Room
			}
		case <-deadline:
			t.Fatalf("no update with the last seq; got %d updates", got)
		}
	}
	if got > 5 {
		t.Fatalf("%d updates for a burst of 52 events; want them coalesced", got)
	}

	// A room gone from the cache is a deletion.
	s.evictRooms([]string{snap.ID}, func(string, int64) events.Event { return events.Event{} })
	for deleted := false; !deleted; {
		select {
		case up := <-ch:
			deleted = up.DeletedID == snap.ID
		case <-time.After(2 * time.Second):
			t.Fatal("no deletion update")
		}
	}

	// The channel closes with the subscriber's context.
	cancel()
	for range ch {
	}
}

func TestListHubOverflowResyncs(t *testing.T) {
	h := newListHub()
	sub := &listSub{ch: make(chan RoomUpdate, listSubBuffer)}
	h.subs[sub] = struct{}{}
	ups := make([]RoomUpdate, listSubBuffer+10)
	for i := range ups {
		ups[i] = RoomUpdate{DeletedID: "r"}
	}
	h.publish(ups)
	if n := len(sub.ch); n != 1 {
		t.Fatalf("queue = %d, want only the resync", n)
	}
	if up := <-sub.ch; !up.Resync {
		t.Fatalf("update = %+v, want Resync", up)
	}
}

func TestSubscribeRoomsAfterClose(t *testing.T) {
	s := New(Deps{Store: memstore.New()})
	s.Close()
	if _, ok := <-s.SubscribeRooms(context.Background()); ok {
		t.Fatal("subscription after Close delivered an update")
	}
}
