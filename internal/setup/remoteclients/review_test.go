package remoteclients

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/room"
)

// TestBlockersScopedEvenWithoutTables: BlockersOnTables tells the runner it is
// scoped even with no tables (a pipeline without source_tables), so its
// params never equal the unscoped Blockers read, which shows every identity.
func TestBlockersScopedEvenWithoutTables(t *testing.T) {
	f := newFakeRunner()
	src := New(f, "r_setup").Source()
	if _, err := src.Blockers(roomCtx(t), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := src.BlockersOnTables(roomCtx(t), time.Minute, nil); err != nil {
		t.Fatal(err)
	}
	calls := f.callsFor(OpSourceBlockers)
	var unscoped, scoped map[string]any
	if err := json.Unmarshal(calls[0].req.Params, &unscoped); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(calls[1].req.Params, &scoped); err != nil {
		t.Fatal(err)
	}
	if _, ok := unscoped["scoped"]; ok {
		t.Fatalf("Blockers params = %s, want no scoped", calls[0].req.Params)
	}
	if scoped["scoped"] != true {
		t.Fatalf("BlockersOnTables(nil) params = %s, want scoped true", calls[1].req.Params)
	}
}

// TestBlockersOnTablesBlanksOutOfScope: a row the runner marks out of scope
// never carries user, application or query, whatever the runner sent.
func TestBlockersOnTablesBlanksOutOfScope(t *testing.T) {
	f := newFakeRunner()
	f.overrides[OpSourceBlockers] = json.RawMessage(`{"sessions":[` +
		`{"pid":1,"user":"alice","application":"billing","query":"SELECT 1","in_scope":true},` +
		`{"pid":2,"user":"bob","application":"payroll","query":"UPDATE pay","in_scope":false}]}`)
	got, err := New(f, "r_setup").Source().BlockersOnTables(roomCtx(t), time.Minute, []string{"public.orders"})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].User != "alice" || got[0].App != "billing" || got[0].Query != "SELECT 1" {
		t.Fatalf("in-scope row = %+v, want its identity", got[0])
	}
	if got[1].User != "" || got[1].App != "" || got[1].Query != "" {
		t.Fatalf("out-of-scope row = %+v, want identity blanked", got[1])
	}
}

// TestBoundRoomWins: a session bound to one room refuses a call whose context
// names another room, before anything is sent; a session wired without a
// room uses the context's.
func TestBoundRoomWins(t *testing.T) {
	f := newFakeRunner()
	ctx := room.WithRoomID(context.Background(), "r_other")
	if _, err := New(f, "r_setup").Connect().Status(ctx, "pg-orders-connector"); !errors.Is(err, ErrNoRunner) {
		t.Fatalf("Status in another room = %v, want ErrNoRunner", err)
	}
	if n := len(f.allCalls()); n != 0 {
		t.Fatalf("%d calls sent for a mismatched room", n)
	}
	if _, err := New(f, "").Connect().Status(ctx, "pg-orders-connector"); err != nil {
		t.Fatalf("Status with the room from ctx: %v", err)
	}
	if got := f.allCalls()[0].roomID; got != "r_other" {
		t.Fatalf("room = %q, want r_other", got)
	}
	if _, err := New(f, "r_setup").Connect().Status(context.Background(), "pg-orders-connector"); err != nil {
		t.Fatalf("Status with the bound room only: %v", err)
	}
	if got := f.allCalls()[1].roomID; got != "r_setup" {
		t.Fatalf("room = %q, want r_setup", got)
	}
}
