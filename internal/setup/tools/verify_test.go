package setuptools

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

type fakeRequester struct {
	mu    sync.Mutex
	rooms []string
	err   error
}

func (f *fakeRequester) RequestVerification(_ context.Context, roomID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rooms = append(f.rooms, roomID)
	return f.err
}

func TestVerificationToolOnlyRequests(t *testing.T) {
	req := &fakeRequester{}
	tl, err := NewVerificationTool(VerifyConfig{Requester: req, RoomID: "r_1"})
	if err != nil {
		t.Fatal(err)
	}
	if tl.Name() != VerifyToolName || tl.Kind() != tools.KindRoom {
		t.Fatalf("tool = %s %s", tl.Name(), tl.Kind())
	}
	for _, in := range []string{``, `{}`} {
		res, err := tl.Run(context.Background(), json.RawMessage(in))
		if err != nil {
			t.Fatalf("Run(%q): %v", in, err)
		}
		if res.Summary == "" || res.Data == nil {
			t.Fatalf("result = %+v", res)
		}
	}
	if len(req.rooms) != 2 || req.rooms[0] != "r_1" {
		t.Fatalf("requests = %v", req.rooms)
	}

	// The tool can emit nothing: it holds no value that could append an
	// event (no Sink, nothing with an Emit method), only the requester.
	v := reflect.ValueOf(tl).Elem()
	sinkType := reflect.TypeOf((*Sink)(nil)).Elem()
	for i := 0; i < v.NumField(); i++ {
		ft := v.Type().Field(i)
		if ft.Type.Implements(sinkType) || (ft.Type.Kind() == reflect.Interface && reflect.PointerTo(ft.Type).Implements(sinkType)) {
			t.Fatalf("setup_request_verification holds %s (%s), which can emit events", ft.Name, ft.Type)
		}
	}
	if _, ok := any(&fakeRequester{}).(Sink); ok {
		t.Fatal("the requester the test uses can emit")
	}
}

func TestVerificationToolErrors(t *testing.T) {
	if _, err := NewVerificationTool(VerifyConfig{}); err == nil {
		t.Fatal("no requester accepted")
	}
	req := &fakeRequester{}
	tl, _ := NewVerificationTool(VerifyConfig{Requester: req})
	if _, err := tl.Run(context.Background(), json.RawMessage(`{"verified":true}`)); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("parameters: %v", err)
	}
	if _, err := tl.Run(context.Background(), json.RawMessage(`{}`)); !errors.Is(err, ErrNoRoom) {
		t.Fatalf("no room: %v", err)
	}
	if len(req.rooms) != 0 {
		t.Fatalf("a refused call reached the ladder runner: %v", req.rooms)
	}
	req.err = protocol.ErrNoRunner
	if _, err := tl.Run(room.WithRoomID(context.Background(), "r_7"), json.RawMessage(`{}`)); !errors.Is(err, protocol.ErrNoRunner) {
		t.Fatalf("ladder error: %v", err)
	}
	if len(req.rooms) != 1 || req.rooms[0] != "r_7" {
		t.Fatalf("requests = %v", req.rooms)
	}
}
