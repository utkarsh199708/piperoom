package room_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// hookRecorder counts OnRoomOpened calls per room.
type hookRecorder struct {
	mu    sync.Mutex
	calls map[string]int
}

func (h *hookRecorder) hook(_ context.Context, roomID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls[roomID]++
}

func (h *hookRecorder) count(roomID string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls[roomID]
}

func TestOnRoomOpenedOncePerRoom(t *testing.T) {
	rec := &hookRecorder{calls: map[string]int{}}
	svc := room.New(room.Deps{
		Store: memstore.New(), Users: room.DefaultUsers(), Owners: testOwners(),
		Run: fakeRun, WriteHandoff: fakeWrite, OnRoomOpened: rec.hook,
	})
	t.Cleanup(svc.Close)

	api := createRoom(t, svc, "arjun")
	alert, _, err := svc.CreateRoomFromAlert(context.Background(), "lag", "sev2", "orders", json.RawMessage(`{"x":1}`))
	if err != nil {
		t.Fatalf("create from alert: %v", err)
	}
	// An unknown pipeline still opens a room on the webhook path.
	unknown, _, err := svc.CreateRoomFromAlert(context.Background(), "lag", "sev2", "nope", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("create from alert (unknown pipeline): %v", err)
	}
	for _, id := range []string{api.ID, alert.ID, unknown.ID} {
		waitFor(t, 2*time.Second, func() bool { return rec.count(id) >= 1 })
	}
	// Rejected creates and other room actions never fire the hook.
	if _, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "x", "sev9", "orders"); err == nil {
		t.Fatal("bad severity: want validation error")
	}
	if _, err := svc.CloseRoom(context.Background(), mustUser(t, svc, "arjun"), api.ID, "done"); err != nil {
		t.Fatalf("close: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.calls) != 3 {
		t.Fatalf("hook fired for %d rooms, want 3: %v", len(rec.calls), rec.calls)
	}
	for id, n := range rec.calls {
		if n != 1 {
			t.Fatalf("room %s: hook fired %d times, want 1", id, n)
		}
	}
}

func TestOnRoomOpenedNilHook(t *testing.T) {
	svc, _, _ := newTestService(t)
	createRoom(t, svc, "arjun")
	if _, _, err := svc.CreateRoomFromAlert(context.Background(), "lag", "sev2", "orders", nil); err != nil {
		t.Fatalf("create from alert: %v", err)
	}
}

// A hook that blocks until the service closes must not delay either HTTP
// response, and Close must still return (the hook gets the service ctx).
func TestOnRoomOpenedBlockingHookDoesNotBlockHTTP(t *testing.T) {
	entered := make(chan string, 4)
	block := func(ctx context.Context, roomID string) {
		entered <- roomID
		<-ctx.Done()
	}
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Owners: testOwners(),
		Run: fakeRun, WriteHandoff: fakeWrite, OnRoomOpened: block,
	})
	api := httpapi.New(svc, st, nil)
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})

	for _, tc := range []struct{ path, body string }{
		{"/api/rooms", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`},
		{"/api/alerts/webhook", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+tc.path, strings.NewReader(tc.body))
		if err != nil {
			cancel()
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-User", "arjun")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			t.Fatalf("%s: %v (blocked by the hook?)", tc.path, err)
		}
		resp.Body.Close()
		cancel()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("%s: status %d, want 201", tc.path, resp.StatusCode)
		}
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: hook never called", tc.path)
		}
	}

	done := make(chan struct{})
	go func() { svc.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return while hooks were blocked on ctx")
	}
}
