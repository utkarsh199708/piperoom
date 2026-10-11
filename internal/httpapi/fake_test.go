package httpapi

import (
	"context"
	"net/http"
	"sync"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/topology"
)

// call is one Service method call as the fake saw it.
type call struct {
	Method string
	Args   []any
}

// fakeService implements Service. Every method records its arguments, returns
// the canned result and, when err is set, that error instead.
type fakeService struct {
	mu    sync.Mutex
	users map[string]room.User
	calls []call
	// deadlines holds, per method, how long before the context deadline the
	// call arrived (zero when the context had none).
	deadlines map[string]time.Duration

	err     error
	panicIn string

	snap  room.Snapshot
	snaps []room.Snapshot
	ev    events.Event
	page  room.EventsPage
	graph topology.Graph
}

var _ Service = (*fakeService)(nil)

var (
	alice = room.User{ID: "alice", Name: "Alice", Layers: []string{"kafka"}}
	bob   = room.User{ID: "bob", Name: "Bob", Layers: []string{}}
)

func newFakeService() *fakeService {
	return &fakeService{
		users:     map[string]room.User{"alice": alice, "bob": bob},
		deadlines: map[string]time.Duration{},
		ev: events.Event{
			RoomID:  "r_1",
			Seq:     7,
			Type:    events.CommentAdded,
			Actor:   events.Actor{Kind: events.ActorHuman, ID: "alice", Name: "Alice"},
			Payload: []byte(`{"text":"hi"}`),
			At:      time.Date(2026, 10, 11, 10, 59, 3, 0, time.UTC),
		},
		snap: room.Snapshot{ID: "r_1", Title: "Warehouse orders stale", Status: room.StatusOpen},
	}
}

// record notes the call and returns what the method should fail with.
func (f *fakeService) record(ctx context.Context, method string, args ...any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call{Method: method, Args: args})
	if d, ok := ctx.Deadline(); ok {
		f.deadlines[method] = time.Until(d)
	} else {
		f.deadlines[method] = 0
	}
	if f.panicIn == method {
		panic("fake service: boom in " + method)
	}
	return f.err
}

func (f *fakeService) lastCall() (call, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return call{}, false
	}
	return f.calls[len(f.calls)-1], true
}

func (f *fakeService) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeService) deadline(method string) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deadlines[method]
}

func (f *fakeService) Authenticate(id string) (room.User, bool) {
	u, ok := f.users[id]
	return u, ok
}

func (f *fakeService) CreateRoom(ctx context.Context, u room.User, in room.CreateRoomInput) (room.Snapshot, error) {
	if err := f.record(ctx, "CreateRoom", u, in); err != nil {
		return room.Snapshot{}, err
	}
	return f.snap, nil
}

func (f *fakeService) IngestAlert(ctx context.Context, a alertin.Alert) (room.Snapshot, error) {
	if err := f.record(ctx, "IngestAlert", a); err != nil {
		return room.Snapshot{}, err
	}
	return f.snap, nil
}

func (f *fakeService) ListRooms(ctx context.Context) ([]room.Snapshot, error) {
	if err := f.record(ctx, "ListRooms"); err != nil {
		return nil, err
	}
	return f.snaps, nil
}

func (f *fakeService) GetRoom(ctx context.Context, roomID string) (room.Snapshot, error) {
	if err := f.record(ctx, "GetRoom", roomID); err != nil {
		return room.Snapshot{}, err
	}
	return f.snap, nil
}

func (f *fakeService) Events(ctx context.Context, roomID string, after int64, limit int) (room.EventsPage, error) {
	if err := f.record(ctx, "Events", roomID, after, limit); err != nil {
		return room.EventsPage{}, err
	}
	return f.page, nil
}

func (f *fakeService) event(ctx context.Context, method string, args ...any) (events.Event, error) {
	if err := f.record(ctx, method, args...); err != nil {
		return events.Event{}, err
	}
	return f.ev, nil
}

func (f *fakeService) Comment(ctx context.Context, u room.User, roomID string, in room.CommentInput) (events.Event, error) {
	return f.event(ctx, "Comment", u, roomID, in)
}

func (f *fakeService) Steer(ctx context.Context, u room.User, roomID, text string) (events.Event, error) {
	return f.event(ctx, "Steer", u, roomID, text)
}

func (f *fakeService) SendSuggestion(ctx context.Context, u room.User, roomID, suggestionID string) (events.Event, error) {
	return f.event(ctx, "SendSuggestion", u, roomID, suggestionID)
}

func (f *fakeService) RequestControl(ctx context.Context, u room.User, roomID string) (events.Event, error) {
	return f.event(ctx, "RequestControl", u, roomID)
}

func (f *fakeService) GrantControl(ctx context.Context, u room.User, roomID, to string) (events.Event, error) {
	return f.event(ctx, "GrantControl", u, roomID, to)
}

func (f *fakeService) Close(ctx context.Context, u room.User, roomID, resolution string) (events.Event, error) {
	return f.event(ctx, "Close", u, roomID, resolution)
}

func (f *fakeService) Approve(ctx context.Context, u room.User, proposalID, layer string) (events.Event, error) {
	return f.event(ctx, "Approve", u, proposalID, layer)
}

func (f *fakeService) Reject(ctx context.Context, u room.User, proposalID, layer, reason string) (events.Event, error) {
	return f.event(ctx, "Reject", u, proposalID, layer, reason)
}

func (f *fakeService) CreateHandoff(ctx context.Context, u room.User, roomID string, in room.HandoffInput) (events.Event, error) {
	return f.event(ctx, "CreateHandoff", u, roomID, in)
}

func (f *fakeService) AcceptHandoff(ctx context.Context, u room.User, handoffID string) (events.Event, error) {
	return f.event(ctx, "AcceptHandoff", u, handoffID)
}

func (f *fakeService) Topology(ctx context.Context, pipelineID string) (topology.Graph, error) {
	if err := f.record(ctx, "Topology", pipelineID); err != nil {
		return topology.Graph{}, err
	}
	return f.graph, nil
}

func (f *fakeService) Trace(ctx context.Context, u room.User, roomID string, in room.TraceInput) (events.Event, error) {
	return f.event(ctx, "Trace", u, roomID, in)
}

// fakeVerifier implements Verifier and remembers what it was given.
type fakeVerifier struct {
	mu     sync.Mutex
	alert  alertin.Alert
	err    error
	calls  int
	source alertin.Source
	header http.Header
	body   []byte
}

var _ Verifier = (*fakeVerifier)(nil)

func (v *fakeVerifier) Ingest(src alertin.Source, h http.Header, body []byte) (alertin.Alert, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.calls++
	v.source, v.header, v.body = src, h.Clone(), append([]byte(nil), body...)
	return v.alert, v.err
}
