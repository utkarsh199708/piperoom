package room_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/room"
)

// hookRecorder counts OnRoomOpened calls per room.
type hookRecorder struct {
	mu    sync.Mutex
	calls map[string]int
	got   chan string
}

func newHookRecorder() *hookRecorder {
	return &hookRecorder{calls: map[string]int{}, got: make(chan string, 16)}
}

func (h *hookRecorder) hook(_ context.Context, id string) {
	h.mu.Lock()
	h.calls[id]++
	h.mu.Unlock()
	h.got <- id
}

func (h *hookRecorder) count(id string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls[id]
}

func waitHook(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case id := <-ch:
		if id != want {
			t.Fatalf("hook called for %s, want %s", id, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("hook not called for %s", want)
	}
}

func TestOnRoomOpenedFiresOncePerRoom(t *testing.T) {
	h := newHookRecorder()
	e := newEnv(t, false, func(c *room.Config) { c.OnRoomOpened = h.hook })

	api := e.open(kabir)
	waitHook(t, h.got, api.ID)

	a := alertin.Alert{Source: alertin.Generic, Title: "Orders stale", Severity: "sev2", PipelineID: "orders", DedupKey: "generic:hook"}
	hooked, err := e.svc.IngestAlert(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	waitHook(t, h.got, hooked.ID)
	// A repeated alert reuses the open room and must not fire the hook again.
	again, err := e.svc.IngestAlert(ctx, a)
	if err != nil || again.ID != hooked.ID {
		t.Fatalf("repeat alert = %s, %v; want %s", again.ID, err, hooked.ID)
	}
	// An alert for an unknown pipeline still opens a room.
	ghost, err := e.svc.IngestAlert(ctx, alertin.Alert{Title: "x", Severity: "sev3", PipelineID: "ghost"})
	if err != nil {
		t.Fatal(err)
	}
	waitHook(t, h.got, ghost.ID)

	select {
	case id := <-h.got:
		t.Fatalf("unexpected extra hook call for %s", id)
	case <-time.After(100 * time.Millisecond):
	}
	for _, id := range []string{api.ID, hooked.ID, ghost.ID} {
		if n := h.count(id); n != 1 {
			t.Fatalf("hook ran %d times for %s, want 1", n, id)
		}
	}
}

func TestOnRoomOpenedNilIsFine(t *testing.T) {
	e := newEnv(t, false)
	e.open(kabir)
	if _, err := e.svc.IngestAlert(ctx, alertin.Alert{Title: "x", Severity: "sev3", PipelineID: "orders"}); err != nil {
		t.Fatal(err)
	}
}

func TestOnRoomOpenedBlockingHookDoesNotBlockCreate(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan string, 1)
	ctxEnded := make(chan struct{})
	e := newEnv(t, false, func(c *room.Config) {
		c.OnRoomOpened = func(ctx context.Context, id string) {
			entered <- id
			select {
			case <-release:
			case <-ctx.Done():
				close(ctxEnded)
			}
		}
	})

	done := make(chan room.Snapshot, 1)
	go func() {
		s, err := e.svc.CreateRoom(context.Background(), kabir, room.CreateRoomInput{Title: "Orders stale", Severity: "sev2", PipelineID: "orders"})
		if err != nil {
			t.Error(err)
		}
		done <- s
	}()
	var snap room.Snapshot
	select {
	case snap = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("CreateRoom waited for a blocking OnRoomOpened hook")
	}
	waitHook(t, entered, snap.ID)

	// Shutdown ends the hook's ctx and waits for it: no goroutine outlives the service.
	shut := make(chan struct{})
	go func() { e.svc.Shutdown(); close(shut) }()
	select {
	case <-shut:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("Shutdown did not end the hook's context")
	}
	select {
	case <-ctxEnded:
	default:
		t.Fatal("hook returned without seeing its context end")
	}
}
