package room_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/room"
)

// cancelAppendStore fails the append of one event type the way a request whose
// client went away does: the context ends and the append reports it. Its
// side-table undo calls honour the context like the Postgres store, which
// refuses a call on a dead context.
type cancelAppendStore struct {
	*lostWriteStore
	mu     sync.Mutex
	failOn events.Type
	cancel context.CancelFunc
}

func (f *cancelAppendStore) arm(typ events.Type, cancel context.CancelFunc) {
	f.mu.Lock()
	f.failOn, f.cancel = typ, cancel
	f.mu.Unlock()
}

func (f *cancelAppendStore) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	f.mu.Lock()
	fail, cancel := typ == f.failOn, f.cancel
	if fail {
		f.failOn, f.cancel = "", nil
	}
	f.mu.Unlock()
	if fail {
		cancel()
		return events.Event{}, ctx.Err()
	}
	return f.Store.Append(ctx, roomID, typ, actor, payload)
}

func (f *cancelAppendStore) DeleteApproval(ctx context.Context, proposalID, layer string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.Store.DeleteApproval(ctx, proposalID, layer)
}

func (f *cancelAppendStore) SetSuggestionStatus(ctx context.Context, suggestionID, status string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.Store.SetSuggestionStatus(ctx, suggestionID, status)
}

func newCancelAppendService(t *testing.T) (*room.Service, *cancelAppendStore) {
	t.Helper()
	lw, c := newLostWriteStore()
	st := &cancelAppendStore{lostWriteStore: lw}
	reg := actions.NewRegistry()
	for _, a := range []actions.Action{
		&fakeAction{name: "pg_act", layer: "postgres"},
		&fakeAction{name: "kafka_act", layer: "kafka"},
	} {
		if err := reg.Register(a); err != nil {
			t.Fatalf("register action: %v", err)
		}
	}
	svc := room.New(room.Deps{
		Store:        st,
		Actions:      reg,
		Users:        room.DefaultUsers(),
		Owners:       testOwners(),
		Run:          fakeRun,
		WriteHandoff: fakeWrite,
		Now:          c.Now,
	})
	t.Cleanup(svc.Close)
	return svc, st
}

// An approval whose event append fails because the request ended must not
// leave its approvals row behind: that row has no event, so the layer shows
// pending for ever while every retry answers already_decided.
func TestFailedApprovalAppendUndoesRowOnDeadContext(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  events.Type
		call func(ctx context.Context, svc *room.Service, pid, hash string) error
	}{
		{"approve", events.ApprovalGiven, func(ctx context.Context, svc *room.Service, pid, hash string) error {
			_, err := svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres", hash)
			return err
		}},
		{"reject", events.ApprovalRejected, func(ctx context.Context, svc *room.Service, pid, hash string) error {
			_, err := svc.RejectProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres", "no", hash)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, st := newCancelAppendService(t)
			_, pid, hash := twoLayerProposal(t, svc)

			cctx, cancel := context.WithCancel(context.Background())
			st.arm(tc.typ, cancel)
			if err := tc.call(cctx, svc, pid, hash); err == nil {
				t.Fatal("decision with a failing append succeeded")
			}
			// The owner retries once the connection is back.
			if err := tc.call(context.Background(), svc, pid, hash); err != nil {
				t.Fatalf("retry after a failed append: %v (the undo ran on the dead request context)", err)
			}
		})
	}
}

// A send whose steer.sent append fails on a dead request context must put the
// suggestion back to pending, or it can be neither sent nor declined.
func TestFailedSendAppendRevertsSuggestionOnDeadContext(t *testing.T) {
	svc, st := newCancelAppendService(t)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	sid := suggest(t, svc, roomID, "check the slot")
	arjun := mustUser(t, svc, "arjun")

	cctx, cancel := context.WithCancel(ctx)
	st.arm(events.SteerSent, cancel)
	if _, err := svc.SendSuggestion(cctx, arjun, roomID, sid); err == nil {
		t.Fatal("send with a failing append succeeded")
	}
	if _, err := svc.SendSuggestion(ctx, arjun, roomID, sid); err != nil {
		t.Fatalf("retry after a failed send: %v", err)
	}
}

// A suggestion id from another room answers exactly like a missing one, so
// ids of rooms the caller cannot see cannot be probed (tenancy.md §6).
func TestForeignSuggestionIDLooksMissing(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()
	arjun := mustUser(t, svc, "arjun")
	roomA := createRoom(t, svc, "arjun").ID
	roomB := createRoom(t, svc, "arjun").ID
	sid := suggest(t, svc, roomB, "x")

	msg := func(err error) string {
		t.Helper()
		var re *room.Error
		if !errors.As(err, &re) || re.Code != "not_found" {
			t.Fatalf("err = %v, want not_found", err)
		}
		return re.Msg
	}
	_, errForeign := svc.DeclineSuggestion(ctx, arjun, roomA, sid, "")
	_, errMissing := svc.DeclineSuggestion(ctx, arjun, roomA, "g_missing", "")
	if a, b := msg(errForeign), msg(errMissing); a != b {
		t.Fatalf("decline: foreign id %q, missing id %q: must not differ", a, b)
	}
	_, errForeign = svc.SendSuggestion(ctx, arjun, roomA, sid)
	_, errMissing = svc.SendSuggestion(ctx, arjun, roomA, "g_missing")
	if a, b := msg(errForeign), msg(errMissing); a != b {
		t.Fatalf("send: foreign id %q, missing id %q: must not differ", a, b)
	}
}
