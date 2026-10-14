package httpapi_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

func wantErrorBody(t *testing.T, resp *http.Response, status int, code string) {
	t.Helper()
	if resp.StatusCode != status {
		t.Fatalf("status = %d, want %d", resp.StatusCode, status)
	}
	body := decode[map[string]map[string]string](t, resp)
	if body["error"]["code"] != code {
		t.Fatalf("error = %+v, want code %q", body["error"], code)
	}
}

func roomLastSeq(t *testing.T, srvURL, roomID string) int64 {
	t.Helper()
	snap := decode[map[string]any](t, doJSON(t, http.MethodGet, srvURL+"/api/rooms/"+roomID, "arjun", ""))
	return int64(snap["last_seq"].(float64))
}

// A NUL in any person-typed field is a 400 validation error over HTTP, and the
// room's log does not move. The JSON escape reaches the service as a real NUL.
func TestUnstorableUserTextIs400(t *testing.T) {
	srv, svc := newServer(t)

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"a\u0000b","severity":"sev2","pipeline_id":"orders"}`)
	wantErrorBody(t, resp, http.StatusBadRequest, "validation")
	if snaps, _ := svc.ListRooms(context.Background()); len(snaps) != 0 {
		t.Fatalf("refused title left %d rooms", len(snaps))
	}

	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	roomID := decode[map[string]any](t, resp)["id"].(string)

	before := roomLastSeq(t, srv.URL, roomID)
	calls := []struct {
		name, user, path, body string
	}{
		{"comment", "arjun", "/api/rooms/" + roomID + "/comments", `{"text":"a\u0000b"}`},
		{"comment step id", "arjun", "/api/rooms/" + roomID + "/comments", `{"step_id":"s\u0000","text":"hello"}`},
		{"steer by driver", "arjun", "/api/rooms/" + roomID + "/steer", `{"text":"a\u0000b"}`},
		{"suggestion by non-driver", "priya", "/api/rooms/" + roomID + "/steer", `{"text":"a\u0000b"}`},
		{"trace key", "arjun", "/api/rooms/" + roomID + "/trace", `{"table":"public.orders","key":"k\u0000"}`},
		{"close resolution", "arjun", "/api/rooms/" + roomID + "/close", `{"resolution":"a\u0000b"}`},
	}
	for _, c := range calls {
		resp := doJSON(t, http.MethodPost, srv.URL+c.path, c.user, c.body)
		wantErrorBody(t, resp, http.StatusBadRequest, "validation")
		if got := roomLastSeq(t, srv.URL, roomID); got != before {
			t.Fatalf("%s: last_seq %d -> %d, want unchanged", c.name, before, got)
		}
	}
	// The room is still open and takes a normal close.
	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/close", "arjun", `{"resolution":"fixed"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("close after refusals: status %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// unstorableStore refuses event appends with store.ErrUnstorable once armed, the
// way a store write does for content that slipped past the room's own checks.
type unstorableStore struct {
	*memstore.Store
	armed atomic.Bool
}

func (f *unstorableStore) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	if f.armed.Load() {
		return events.Event{}, fmt.Errorf("store: append to %s: %w", roomID, store.ErrUnstorable)
	}
	return f.Store.Append(ctx, roomID, typ, actor, payload)
}

// pgFailStore fails event appends with a raw Postgres error once armed.
type pgFailStore struct {
	*memstore.Store
	code  string
	armed atomic.Bool
}

func (f *pgFailStore) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	if f.armed.Load() {
		return events.Event{}, fmt.Errorf("store: insert event: %w", &pgconn.PgError{Code: f.code, Message: "invalid input syntax"})
	}
	return f.Store.Append(ctx, roomID, typ, actor, payload)
}

// SQLSTATE 22P02 (invalid text representation) after the pre-checks means our
// own bug, not bad client text: it stays a logged 500 internal_error and is not
// turned into a silent 400.
func TestPostgres22P02StaysA500(t *testing.T) {
	st := &pgFailStore{Store: memstore.New(), code: "22P02"}
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	srv := httptest.NewServer(httpapi.New(svc, st, nil))
	t.Cleanup(srv.Close)

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	roomID := decode[map[string]any](t, resp)["id"].(string)

	st.armed.Store(true)
	resp = doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/comments", "arjun", `{"text":"fine"}`)
	wantErrorBody(t, resp, http.StatusInternalServerError, "internal_error")
}

// An ErrUnstorable that comes back from the store is a 400, never a 500, and
// the response does not echo the error text.
func TestStoreErrUnstorableMapsTo400(t *testing.T) {
	st := &unstorableStore{Store: memstore.New()}
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	srv := httptest.NewServer(httpapi.New(svc, st, nil))
	t.Cleanup(srv.Close)

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	roomID := decode[map[string]any](t, resp)["id"].(string)

	st.armed.Store(true)
	for _, c := range []struct{ path, body string }{
		{"/comments", `{"text":"fine"}`},
		{"/steer", `{"text":"fine"}`},
		{"/close", `{"resolution":"fine"}`},
	} {
		resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+c.path, "arjun", c.body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", c.path, resp.StatusCode)
		}
		body := decode[map[string]map[string]string](t, resp)
		if body["error"]["code"] != "validation" || body["error"]["message"] != "text contains characters that cannot be stored" {
			t.Fatalf("%s: error = %+v", c.path, body["error"])
		}
	}
}
