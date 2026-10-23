package forward

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

type fakeExec struct {
	req  protocol.OpRequest
	ctx  context.Context
	res  protocol.OpResult
	err  error
	room string
}

func (f *fakeExec) Execute(ctx context.Context, roomID string, req protocol.OpRequest) (protocol.OpResult, error) {
	f.req, f.ctx, f.room = req, ctx, roomID
	return f.res, f.err
}

func TestRoom(t *testing.T) {
	ex := &fakeExec{}
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		s     *Sender
		ctx   context.Context
		want  string
		noRun bool
	}{
		{"bound", New(ex, "r_1"), ctx, "r_1", false},
		{"bound, same room in ctx", New(ex, "r_1"), room.WithRoomID(ctx, "r_1"), "r_1", false},
		{"from ctx", New(ex, ""), room.WithRoomID(ctx, "r_2"), "r_2", false},
		{"another room in ctx", New(ex, "r_1"), room.WithRoomID(ctx, "r_2"), "", true},
		{"no room", New(ex, ""), ctx, "", true},
		{"no executor", New(nil, "r_1"), ctx, "", true},
		{"nil sender", nil, ctx, "", true},
	} {
		got, err := tc.s.Room(tc.ctx)
		if tc.noRun != errors.Is(err, ErrNoRunner) || got != tc.want {
			t.Errorf("%s: Room = %q, %v", tc.name, got, err)
		}
	}
}

func TestSend(t *testing.T) {
	ex := &fakeExec{res: protocol.OpResult{OK: true, Summary: "ok"}}
	s := New(ex, "r_1")
	before := time.Now()
	if _, err := s.Send(context.Background(), "r_1", protocol.OpRequest{Op: "env_detect"}, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ex.req.ID, "su_") || ex.req.Deadline.Before(before.Add(9*time.Second)) || ex.req.Deadline.After(before.Add(11*time.Second)) {
		t.Fatalf("id %q deadline %v", ex.req.ID, ex.req.Deadline)
	}
	// A caller's earlier deadline wins.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := s.Send(ctx, "r_1", protocol.OpRequest{Op: "env_detect"}, time.Hour); err != nil {
		t.Fatal(err)
	}
	if dl, _ := ctx.Deadline(); !ex.req.Deadline.Equal(dl) {
		t.Fatalf("deadline %v, want the caller's %v", ex.req.Deadline, dl)
	}

	for _, tc := range []struct {
		name string
		res  protocol.OpResult
		err  error
		want []error
	}{
		{"no runner", protocol.OpResult{}, protocol.ErrNoRunner, []error{ErrNoRunner}},
		{"refused", protocol.OpResult{}, &protocol.RefusedError{Refusal: protocol.Refusal{Reason: protocol.RefuseDuplicateStep}}, []error{protocol.ErrDuplicateStep}},
		{"failed", protocol.OpResult{OK: false, Summary: "boom\x1b[2J"}, nil, []error{ErrOpFailed}},
		{"not supported", protocol.OpResult{OK: false, Data: json.RawMessage(`{"error_code":"not_supported"}`)}, nil, []error{ErrOpFailed, clients.ErrNotSupported}},
		{"not found", protocol.OpResult{OK: false, Data: json.RawMessage(`{"error_code":"not_found"}`)}, nil, []error{ErrOpFailed, clients.ErrNotFound}},
		{"out of scope", protocol.OpResult{OK: false, Data: json.RawMessage(`{"error_code":"out_of_scope"}`)}, nil, []error{ErrOpFailed, tools.ErrOutOfScope}},
		{"data over the cap", protocol.OpResult{OK: true, Data: json.RawMessage(`"` + strings.Repeat("x", protocol.MaxDataBytes) + `"`)}, nil, []error{ErrBadResult}},
	} {
		ex := &fakeExec{res: tc.res, err: tc.err}
		_, err := New(ex, "r_1").Send(context.Background(), "r_1", protocol.OpRequest{Op: "x"}, time.Second)
		for _, w := range tc.want {
			if !errors.Is(err, w) {
				t.Errorf("%s: err = %v, want %v", tc.name, err, w)
			}
		}
		if err != nil && strings.ContainsRune(err.Error(), 0x1b) {
			t.Errorf("%s: a control character reached the error", tc.name)
		}
	}
	if _, err := New(nil, "r").Send(context.Background(), "r", protocol.OpRequest{}, time.Second); !errors.Is(err, ErrNoRunner) {
		t.Fatalf("nil executor: %v", err)
	}
}
