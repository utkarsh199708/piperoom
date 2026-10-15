package room

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"piperoom/internal/owners"
	"piperoom/internal/store/memstore"
)

func incidentEvidence(signal string) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"source": "watcher", "pipeline": "orders",
		"signals": []map[string]any{{"signal": signal, "severity": "sev2"}}})
	return raw
}

// TestAutoPageJoinedIncident (Q5 T3): consumer_lag opens the room and its
// layer is auto-paged; connector_down joins as a new incident and its layer
// (kafka-connect, nobody paged it yet) gets its own automatic page at the next
// check. Once per layer: a later check and a restart write no more, and a
// joined incident of a layer already paged gets none.
func TestAutoPageJoinedIncident(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	r := newPageRig(t, ms, c, 5*time.Minute, nil)
	r.load()
	roomID := openWatcherRoom(t, r.svc, owners.SignalConsumerLag)
	c.Advance(5 * time.Minute)
	r.tick()
	if got := autoPages(t, ms, roomID); len(got) != 1 || got[0].Layer != "consumer" {
		t.Fatalf("opening page = %+v", got)
	}
	c.Advance(13 * time.Second)
	if err := r.svc.PostIncidentJSON(ctx, roomID, "watcher: orders: connector down (new signal)", incidentEvidence(owners.SignalConnectorDown)); err != nil {
		t.Fatal(err)
	}
	r.tick()
	got := autoPages(t, ms, roomID)
	if len(got) != 2 {
		t.Fatalf("auto pages = %+v, want the joined layer paged", got)
	}
	if got[1].Layer != "kafka-connect" || !reflect.DeepEqual(got[1].Owners, []string{"arjun"}) || !strings.Contains(got[1].Reason, "connector_down") {
		t.Fatalf("joined page = %+v", got[1])
	}
	// A second incident of an already paged layer pages nobody again.
	if err := r.svc.PostIncidentJSON(ctx, roomID, "watcher: orders: lag again (new signal)", incidentEvidence(owners.SignalGroupUnstable)); err != nil {
		t.Fatal(err)
	}
	c.Advance(20 * time.Minute)
	r.tick()
	if n := len(autoPages(t, ms, roomID)); n != 2 {
		t.Fatalf("after more checks: %d auto pages, want 2", n)
	}
	r.svc.Close()
	r2 := newPageRig(t, ms, c, 5*time.Minute, nil)
	r2.load()
	r2.tick()
	if n := len(autoPages(t, ms, roomID)); n != 2 {
		t.Fatalf("after a restart: %d auto pages, want 2", n)
	}
}

// TestAutoPageJoinedIncidentDriven (Q5 T3): a room with a driver pages nobody
// on its own, joined incident or not.
func TestAutoPageJoinedIncidentDriven(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	r := newPageRig(t, ms, c, 5*time.Minute, nil)
	r.load()
	roomID := openWatcherRoom(t, r.svc, owners.SignalConsumerLag)
	c.Advance(5 * time.Minute)
	r.tick()
	if _, err := r.svc.GrantControl(ctx, leaseUser(t, r.svc, "kabir"), roomID, "kabir"); err != nil {
		t.Fatal(err)
	}
	if err := r.svc.PostIncidentJSON(ctx, roomID, "watcher: orders: connector down (new signal)", incidentEvidence(owners.SignalConnectorDown)); err != nil {
		t.Fatal(err)
	}
	r.tick()
	if n := len(autoPages(t, ms, roomID)); n != 1 {
		t.Fatalf("driven room: %d auto pages, want 1", n)
	}
}
