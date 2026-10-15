package room_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

// sinceGateStore holds Since for one room until released, so a load of that
// room stays in flight.
type sinceGateStore struct {
	*shadowStore
	mu   sync.Mutex
	room string
	gate chan struct{}
	held chan struct{}
}

func (g *sinceGateStore) Since(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error) {
	g.mu.Lock()
	gate := g.gate
	if roomID != g.room {
		gate = nil
	}
	g.mu.Unlock()
	if gate != nil {
		select {
		case g.held <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.shadowStore.Since(ctx, roomID, afterSeq, limit)
}

// ShadowMetrics waits for a room another caller is loading instead of
// counting its run as unlabelled.
func TestShadowMetricsCountsRoomMidLoad(t *testing.T) {
	ctx := context.Background()
	env := newShadowEnv(t, shadowCfg{})
	snap := env.open(t, "midload")
	env.waitFinished(t, snap.ID)
	env.close(t, snap.ID, "resolved")
	if _, err := env.svc.ShadowLabel(ctx, env.user(t, "priya"), snap.ID, "kafka-connect", []string{"x"}); err != nil {
		t.Fatal(err)
	}
	env.svc.Close()

	g := &sinceGateStore{shadowStore: env.st, room: snap.ID, gate: make(chan struct{}), held: make(chan struct{}, 1)}
	var once sync.Once
	release := func() { once.Do(func() { close(g.gate) }) }
	t.Cleanup(release)
	deps := env.deps
	deps.Store = g
	svc := room.New(deps)
	t.Cleanup(svc.Close)

	loaded := make(chan error, 1)
	go func() {
		_, err := svc.GetRoom(ctx, snap.ID)
		loaded <- err
	}()
	select {
	case <-g.held:
	case <-time.After(5 * time.Second):
		t.Fatal("the load was not held")
	}
	type result struct {
		m   room.ShadowMetrics
		err error
	}
	done := make(chan result, 1)
	go func() {
		m, err := svc.ShadowMetrics(ctx, "", time.Time{})
		done <- result{m, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("metrics returned while the room was loading: %+v %v", r.m, r.err)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case r := <-done:
		if r.err != nil || r.m.Rooms != 1 || r.m.Labelled != 1 {
			t.Fatalf("metrics = %+v, %v; want one labelled room", r.m, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("metrics did not finish after the load")
	}
	if err := <-loaded; err != nil {
		t.Fatal(err)
	}
}
