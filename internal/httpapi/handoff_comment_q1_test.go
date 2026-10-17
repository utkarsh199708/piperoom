package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// switchStore makes Since fail on demand, as a database that went away.
type switchStore struct {
	*memstore.Store
	failSince atomic.Bool
}

func (s *switchStore) Since(ctx context.Context, roomID string, after int64, limit int) ([]events.Event, error) {
	if s.failSince.Load() {
		return nil, errors.New("db down: connection refused")
	}
	return s.Store.Since(ctx, roomID, after, limit)
}

// handoffEnv is a server whose hand-off writer a test swaps between requests.
type handoffEnv struct {
	srv   *httptest.Server
	svc   *room.Service
	st    *switchStore
	calls atomic.Int32
	write atomic.Pointer[agent.HandoffWriter]
}

func newHandoffEnv(t *testing.T, w agent.HandoffWriter) *handoffEnv {
	t.Helper()
	e := &handoffEnv{st: &switchStore{Store: memstore.New()}}
	e.setWriter(w)
	e.svc = room.New(room.Deps{
		Store: e.st, Users: room.DefaultUsers(), Run: fakeRun,
		WriteHandoff: func(ctx context.Context, evs []events.Event, o agent.Options) (events.HandoffSummary, error) {
			e.calls.Add(1)
			return (*e.write.Load())(ctx, evs, o)
		},
	})
	api := httpapi.New(e.svc, e.st, nil)
	e.srv = httptest.NewServer(api)
	t.Cleanup(func() { e.srv.Close(); api.Close() })
	return e
}

func (e *handoffEnv) setWriter(w agent.HandoffWriter) { e.write.Store(&w) }

func goodWriter(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
	return events.HandoffSummary{WhatBroke: "schema change", Why: "int->bigint"}, nil
}

func (e *handoffEnv) newRoom(t *testing.T) string {
	t.Helper()
	arjun, _ := e.svc.LookupUser("arjun")
	snap, err := e.svc.CreateRoom(context.Background(), arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	return snap.ID
}

func (e *handoffEnv) events(t *testing.T, roomID string, typ events.Type) []events.Event {
	t.Helper()
	evs, err := e.st.Store.Since(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var out []events.Event
	for _, ev := range evs {
		if ev.Type == typ {
			out = append(out, ev)
		}
	}
	return out
}

// Any failure of the hand-off writer answers 503 summary_failed with a message
// that says what to do, and the driver can retry at once.
func TestHandoffRouteSummaryFailedIs503(t *testing.T) {
	cases := []struct {
		name  string
		write agent.HandoffWriter
	}{
		{"model omitted a field", func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, errors.New("room: handoff: write summary: model omitted what_broke")
		}},
		{"timeout", func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, fmt.Errorf("agent: handoff request: %w", context.DeadlineExceeded)
		}},
		{"provider error quoting content", func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, errors.New("529 overloaded: row 4111-1111")
		}},
		{"incomplete summary", func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, nil
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newHandoffEnv(t, c.write)
			roomID := e.newRoom(t)
			url := e.srv.URL + "/api/rooms/" + roomID + "/handoffs"

			resp := doJSON(t, http.MethodPost, url, "arjun", `{"to":"priya"}`)
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", resp.StatusCode)
			}
			b := readErr(t, resp)
			resp.Body.Close()
			msg := strings.ToLower(b.Error.Message)
			if b.Error.Code != "summary_failed" || !strings.Contains(msg, "try again") || !strings.Contains(msg, "yourself") || strings.Contains(msg, "4111") {
				t.Fatalf("error = %+v", b.Error)
			}
			fails := e.events(t, roomID, events.HandoffFailed)
			if len(fails) != 1 || !strings.Contains(string(fails[0].Payload), `"summary_failed"`) {
				t.Fatalf("handoff.failed = %v", fails)
			}

			// Retry: the request ended, so a working writer goes through.
			e.setWriter(goodWriter)
			resp = doJSON(t, http.MethodPost, url, "arjun", `{"to":"priya"}`)
			wantStatus(t, resp, http.StatusOK)
			resp.Body.Close()
		})
	}
}

// A store failure is the server's: 500 internal_error, no detail.
func TestHandoffRouteStoreFailureIs500(t *testing.T) {
	e := newHandoffEnv(t, goodWriter)
	roomID := e.newRoom(t)
	e.st.failSince.Store(true)
	resp := doJSON(t, http.MethodPost, e.srv.URL+"/api/rooms/"+roomID+"/handoffs", "arjun", `{"to":"priya"}`)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	b := readErr(t, resp)
	resp.Body.Close()
	if b.Error.Code != "internal_error" || strings.Contains(b.Error.Message, "db down") {
		t.Fatalf("error = %+v", b.Error)
	}
	if e.calls.Load() != 0 {
		t.Fatal("the writer ran without a readable log")
	}
}

// The driver's own summary: no model call, manual: true, no violations.
func TestHandoffRouteManualSummary(t *testing.T) {
	e := newHandoffEnv(t, func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
		return events.HandoffSummary{}, errors.New("must not be called")
	})
	roomID := e.newRoom(t)
	url := e.srv.URL + "/api/rooms/" + roomID + "/handoffs"
	body := `{"to":"priya","stay_as_watcher":true,"summary":{
		"what_broke":"orders consumer stopped","why":"amount became numeric",
		"done":["restarted the connector"],"ruled_out":["broker health"],
		"open":["watch the lag"],"risks":["another alter may break it"],
		"violations":[{"rule":"unknown_number","field":"why","detail":"spoofed"}]}}`
	resp := doJSON(t, http.MethodPost, url, "arjun", body)
	wantStatus(t, resp, http.StatusOK)
	ev := decode[events.Event](t, resp)
	if ev.Type != events.HandoffCreated {
		t.Fatalf("event = %s", ev.Type)
	}
	if !strings.Contains(string(ev.Payload), `"manual":true`) || strings.Contains(string(ev.Payload), "violations") {
		t.Fatalf("payload = %s", ev.Payload)
	}
	p, err := events.Decode[events.HandoffCreatedPayload](ev)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Manual || !p.StayAsWatcher || p.Summary.WhatBroke != "orders consumer stopped" || p.Summary.Why != "amount became numeric" ||
		len(p.Summary.Done) != 1 || len(p.Summary.RuledOut) != 1 || len(p.Summary.Open) != 1 || len(p.Summary.Risks) != 1 || p.Summary.Violations != nil {
		t.Fatalf("payload = %+v", p)
	}
	if e.calls.Load() != 0 {
		t.Fatal("the model was called for a manual summary")
	}
	// It is accepted like any other.
	resp = doJSON(t, http.MethodPost, e.srv.URL+"/api/handoffs/"+p.HandoffID+"/accept", "priya", "")
	wantStatus(t, resp, http.StatusOK)
	resp.Body.Close()
	// The wire shape of the stored hand-off: lists, never null.
	var raw map[string]any
	if err := json.Unmarshal(ev.Payload, &raw); err != nil {
		t.Fatal(err)
	}
	sum := raw["summary"].(map[string]any)
	for _, k := range []string{"done", "ruled_out", "open", "risks"} {
		if _, ok := sum[k].([]any); !ok {
			t.Fatalf("summary.%s = %v, want a list", k, sum[k])
		}
	}
}

// Without a summary (or with null) the agent writes it; a manual summary may
// leave the optional lists out.
func TestHandoffRouteSummaryOptional(t *testing.T) {
	e := newHandoffEnv(t, goodWriter)
	roomID := e.newRoom(t)
	url := e.srv.URL + "/api/rooms/" + roomID + "/handoffs"
	resp := doJSON(t, http.MethodPost, url, "arjun", `{"to":"priya","summary":null}`)
	wantStatus(t, resp, http.StatusOK)
	if p, _ := events.Decode[events.HandoffCreatedPayload](decode[events.Event](t, resp)); p.Manual {
		t.Fatal("summary:null made a manual hand-off")
	}
	if e.calls.Load() != 1 {
		t.Fatalf("model calls = %d, want 1", e.calls.Load())
	}
	resp = doJSON(t, http.MethodPost, url, "arjun", `{"to":"meera","summary":{"what_broke":"a","why":"b"}}`)
	wantStatus(t, resp, http.StatusOK)
	if p, _ := events.Decode[events.HandoffCreatedPayload](decode[events.Event](t, resp)); !p.Manual {
		t.Fatal("a summary without the optional lists is manual")
	}
	if e.calls.Load() != 1 {
		t.Fatalf("model calls = %d after a manual hand-off, want 1", e.calls.Load())
	}
}

func TestHandoffRouteManualSummaryValidation(t *testing.T) {
	rep := func(n int) string { return strings.Repeat("a", n) }
	list := func(n int) string {
		items := make([]string, n)
		for i := range items {
			items[i] = fmt.Sprintf("%q", fmt.Sprintf("item %d", i))
		}
		return "[" + strings.Join(items, ",") + "]"
	}
	sum := func(extra string) string {
		return `{"to":"priya","summary":{"what_broke":"a","why":"b"` + extra + `}}`
	}
	cases := []struct {
		name, body string
		status     int
		code       string
	}{
		{"ok", sum(""), 200, ""},
		{"limits", `{"to":"priya","summary":{"what_broke":"` + rep(1000) + `","why":"` + rep(1000) + `","done":` + list(20) + `,"open":["` + rep(500) + `"]}}`, 200, ""},
		{"empty summary object", `{"to":"priya","summary":{}}`, 400, "validation"},
		{"what_broke missing", `{"to":"priya","summary":{"why":"b"}}`, 400, "validation"},
		{"why missing", `{"to":"priya","summary":{"what_broke":"a"}}`, 400, "validation"},
		{"what_broke blank", `{"to":"priya","summary":{"what_broke":"  ","why":"b"}}`, 400, "validation"},
		{"what_broke too long", `{"to":"priya","summary":{"what_broke":"` + rep(1001) + `","why":"b"}}`, 400, "validation"},
		{"why too long", `{"to":"priya","summary":{"what_broke":"a","why":"` + rep(1001) + `"}}`, 400, "validation"},
		{"item too long", sum(`,"risks":["` + rep(501) + `"]`), 400, "validation"},
		{"too many items", sum(`,"ruled_out":` + list(21)), 400, "validation"},
		{"NUL in a list item", sum(`,"open":["bad\u0000"]`), 400, "validation"},
		{"NUL in why", `{"to":"priya","summary":{"what_broke":"a","why":"b\u0000"}}`, 400, "validation"},
		{"summary is a string", `{"to":"priya","summary":"just write it"}`, 400, "validation"},
		{"list is a string", sum(`,"done":"one"`), 400, "validation"},
		{"list item is a number", sum(`,"done":[1]`), 400, "validation"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newHandoffEnv(t, goodWriter)
			roomID := e.newRoom(t)
			resp := doJSON(t, http.MethodPost, e.srv.URL+"/api/rooms/"+roomID+"/handoffs", "arjun", c.body)
			if c.code == "" {
				wantStatus(t, resp, c.status)
				resp.Body.Close()
				return
			}
			wantHTTPError(t, resp, c.status, c.code)
			if n := len(e.events(t, roomID, events.HandoffRequested)); n != 0 {
				t.Fatalf("%d handoff.requested after a 400", n)
			}
			if e.calls.Load() != 0 {
				t.Fatal("the model ran for a refused summary")
			}
		})
	}
}

// The rules around a manual summary: only the driver, only an open room, one at
// a time.
func TestHandoffRouteManualSummaryRules(t *testing.T) {
	e := newHandoffEnv(t, goodWriter)
	roomID := e.newRoom(t)
	url := e.srv.URL + "/api/rooms/" + roomID + "/handoffs"
	body := `{"to":"priya","summary":{"what_broke":"a","why":"b"}}`
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "kabir", body), http.StatusForbidden, "not_driver")
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "arjun", `{"to":"arjun","summary":{"what_broke":"a","why":"b"}}`), http.StatusBadRequest, "validation")
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "arjun", `{"to":"nobody","summary":{"what_broke":"a","why":"b"}}`), http.StatusBadRequest, "validation")
	arjun, _ := e.svc.LookupUser("arjun")
	if _, err := e.svc.CloseRoom(context.Background(), arjun, roomID, "done"); err != nil {
		t.Fatal(err)
	}
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "arjun", body), http.StatusConflict, "room_closed")
}

// kind is "" or "fix_note"; anything else is 400.
func TestCommentRouteKind(t *testing.T) {
	srv, svc := newServer(t)
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(context.Background(), arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/api/rooms/" + snap.ID + "/comments"
	for _, bad := range []string{`"note"`, `"FIX_NOTE"`, `"fix note"`, `5`, `["fix_note"]`} {
		resp := doJSON(t, http.MethodPost, url, "kabir", `{"text":"restarted by hand","kind":`+bad+`}`)
		wantHTTPError(t, resp, http.StatusBadRequest, "validation")
	}
	for _, c := range []struct{ kind, want string }{{"", ""}, {`"kind":""`, ""}, {`"kind":"fix_note"`, "fix_note"}} {
		body := `{"text":"restarted by hand"`
		if c.kind != "" {
			body += "," + c.kind
		}
		resp := doJSON(t, http.MethodPost, url, "kabir", body+"}")
		wantStatus(t, resp, http.StatusOK)
		p, err := events.Decode[events.CommentAddedPayload](decode[events.Event](t, resp))
		if err != nil || p.Kind != c.want || p.Text != "restarted by hand" {
			t.Fatalf("payload = %+v, %v; want kind %q", p, err, c.want)
		}
	}
	// A plain comment omits kind on the wire.
	resp := doJSON(t, http.MethodPost, url, "kabir", `{"text":"plain"}`)
	wantStatus(t, resp, http.StatusOK)
	if ev := decode[events.Event](t, resp); strings.Contains(string(ev.Payload), `"kind"`) {
		t.Fatalf("payload = %s", ev.Payload)
	}
}

// A closed room takes a fix note from a member of its pipeline, not a plain
// comment; and not after 24 hours.
func TestCommentRouteFixNoteOnClosedRoom(t *testing.T) {
	srv, svc := newServer(t)
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(context.Background(), arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CloseRoom(context.Background(), arjun, snap.ID, "fixed by hand"); err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/api/rooms/" + snap.ID + "/comments"
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "kabir", `{"text":"late"}`), http.StatusConflict, "room_closed")
	wantHTTPError(t, doJSON(t, http.MethodPost, url, "kabir", `{"text":"late","kind":""}`), http.StatusConflict, "room_closed")
	resp := doJSON(t, http.MethodPost, url, "kabir", `{"text":"restarted order-sync by hand","kind":"fix_note"}`)
	wantStatus(t, resp, http.StatusOK)
	p, err := events.Decode[events.CommentAddedPayload](decode[events.Event](t, resp))
	if err != nil || p.Kind != events.CommentKindFixNote {
		t.Fatalf("payload = %+v, %v", p, err)
	}
	// The note is in the post-mortem draft.
	pm := doJSON(t, http.MethodGet, srv.URL+"/api/rooms/"+snap.ID+"/postmortem.md", "arjun", "")
	wantStatus(t, pm, http.StatusOK)
	if md := readAll(t, pm); !strings.Contains(md, "Kabir: restarted order-sync by hand") {
		t.Fatalf("post-mortem lacks the fix note:\n%s", md)
	}
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
