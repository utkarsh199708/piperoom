package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/topology"
)

// ---- helpers ----

type harness struct {
	t   *testing.T
	svc *fakeService
	ver *fakeVerifier
	h   http.Handler
	log *bytes.Buffer
}

func newHarness(t *testing.T, mod func(*Config)) *harness {
	t.Helper()
	h := &harness{t: t, svc: newFakeService(), ver: &fakeVerifier{}, log: &bytes.Buffer{}}
	cfg := Config{
		Service:  h.svc,
		Verifier: h.ver,
		Logger:   slog.New(slog.NewJSONHandler(h.log, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	if mod != nil {
		mod(&cfg)
	}
	h.h = New(cfg)
	return h
}

type req struct {
	method string
	path   string
	body   string
	user   string // X-User; "" sends none
	header map[string]string
}

func (h *harness) do(r req) *httptest.ResponseRecorder {
	h.t.Helper()
	var body io.Reader
	if r.body != "" {
		body = strings.NewReader(r.body)
	}
	rq := httptest.NewRequest(r.method, r.path, body)
	if r.user != "" {
		rq.Header.Set("X-User", r.user)
	}
	for k, v := range r.header {
		rq.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.h.ServeHTTP(rec, rq)
	return rec
}

// as sends an authenticated request as alice.
func (h *harness) as(method, path, body string) *httptest.ResponseRecorder {
	h.t.Helper()
	return h.do(req{method: method, path: path, body: body, user: "alice"})
}

func decodeMap(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("body is not a JSON object: %v\n%s", err, rec.Body.String())
	}
	return m
}

// wantError checks the status and the docs/api.md error shape.
func wantError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d\n%s", rec.Code, status, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON: %v\n%s", err, rec.Body.String())
	}
	if body.Error.Code != code {
		t.Fatalf("error code = %q, want %q\n%s", body.Error.Code, code, rec.Body.String())
	}
	if body.Error.Message == "" {
		t.Fatalf("error message is empty\n%s", rec.Body.String())
	}
}

func (h *harness) wantLastCall(method string, args ...any) {
	h.t.Helper()
	c, ok := h.svc.lastCall()
	if !ok {
		h.t.Fatalf("service was not called; want %s", method)
	}
	if c.Method != method || !reflect.DeepEqual(c.Args, args) {
		h.t.Fatalf("last call = %s%v, want %s%v", c.Method, c.Args, method, args)
	}
}

// ---- routes ----

type route struct {
	name   string
	method string
	path   string
	body   string
	call   string
	args   []any
	// event routes answer the service's Event.
	event bool
}

var routes = []route{
	{name: "create room", method: "POST", path: "/api/rooms",
		body: `{"title":"Orders stale","severity":"sev2","pipeline_id":"orders"}`,
		call: "CreateRoom", args: []any{alice, room.CreateRoomInput{Title: "Orders stale", Severity: "sev2", PipelineID: "orders"}}},
	{name: "get room", method: "GET", path: "/api/rooms/r_1", call: "GetRoom", args: []any{"r_1"}},
	{name: "comment", method: "POST", path: "/api/rooms/r_1/comments",
		body: `{"step_id":"s_2","text":"Wrong service? @agent"}`, event: true,
		call: "Comment", args: []any{alice, "r_1", room.CommentInput{StepID: "s_2", Text: "Wrong service? @agent"}}},
	{name: "comment without step", method: "POST", path: "/api/rooms/r_1/comments",
		body: `{"text":"hello"}`, event: true,
		call: "Comment", args: []any{alice, "r_1", room.CommentInput{Text: "hello"}}},
	{name: "steer", method: "POST", path: "/api/rooms/r_1/steer", body: `{"text":"check the slot"}`, event: true,
		call: "Steer", args: []any{alice, "r_1", "check the slot"}},
	{name: "send suggestion", method: "POST", path: "/api/rooms/r_1/suggestions/g_9/send", event: true,
		call: "SendSuggestion", args: []any{alice, "r_1", "g_9"}},
	{name: "request control", method: "POST", path: "/api/rooms/r_1/control/request", event: true,
		call: "RequestControl", args: []any{alice, "r_1"}},
	{name: "grant control", method: "POST", path: "/api/rooms/r_1/control/grant", body: `{"to":"bob"}`, event: true,
		call: "GrantControl", args: []any{alice, "r_1", "bob"}},
	{name: "close", method: "POST", path: "/api/rooms/r_1/close", body: `{"resolution":"Schema registered"}`, event: true,
		call: "Close", args: []any{alice, "r_1", "Schema registered"}},
	{name: "approve", method: "POST", path: "/api/proposals/p_3/approve", body: `{"layer":"kafka"}`, event: true,
		call: "Approve", args: []any{alice, "p_3", "kafka"}},
	{name: "reject", method: "POST", path: "/api/proposals/p_3/reject", body: `{"layer":"kafka","reason":"too risky"}`, event: true,
		call: "Reject", args: []any{alice, "p_3", "kafka", "too risky"}},
	{name: "create handoff", method: "POST", path: "/api/rooms/r_1/handoffs", body: `{"to":"bob","stay_as_watcher":true}`, event: true,
		call: "CreateHandoff", args: []any{alice, "r_1", room.HandoffInput{To: "bob", StayAsWatcher: true}}},
	{name: "create handoff without stay", method: "POST", path: "/api/rooms/r_1/handoffs", body: `{"to":"bob"}`, event: true,
		call: "CreateHandoff", args: []any{alice, "r_1", room.HandoffInput{To: "bob"}}},
	{name: "accept handoff", method: "POST", path: "/api/handoffs/h_4/accept", event: true,
		call: "AcceptHandoff", args: []any{alice, "h_4"}},
	{name: "topology", method: "GET", path: "/api/pipelines/orders/topology", call: "Topology", args: []any{"orders"}},
	{name: "trace", method: "POST", path: "/api/rooms/r_1/trace", body: `{"table":"public.orders","key":"1042"}`, event: true,
		call: "Trace", args: []any{alice, "r_1", room.TraceInput{Table: "public.orders", Key: "1042"}}},
}

func TestRoutesCallTheServiceAndAnswerJSON(t *testing.T) {
	for _, tc := range routes {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			rec := h.as(tc.method, tc.path, tc.body)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200\n%s", rec.Code, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q", ct)
			}
			h.wantLastCall(tc.call, tc.args...)
			m := decodeMap(t, rec)
			if tc.event {
				// Exactly events.Event.
				for _, k := range []string{"room_id", "seq", "type", "actor", "payload", "at"} {
					if _, ok := m[k]; !ok {
						t.Fatalf("event has no %q: %s", k, rec.Body.String())
					}
				}
				if m["at"] != "2026-10-11T10:59:03Z" {
					t.Fatalf("at = %v, want UTC RFC 3339", m["at"])
				}
			}
		})
	}
}

func TestSimpleReads(t *testing.T) {
	t.Run("me", func(t *testing.T) {
		h := newHarness(t, nil)
		m := decodeMap(t, h.as("GET", "/api/me", ""))
		want := map[string]any{"id": "alice", "name": "Alice", "layers": []any{"kafka"}}
		if !reflect.DeepEqual(m, want) {
			t.Fatalf("me = %v, want %v", m, want)
		}
		if h.svc.callCount() != 0 {
			t.Fatal("/api/me should not need a service call beyond Authenticate")
		}
	})
	t.Run("me with no layers is an empty array", func(t *testing.T) {
		h := newHarness(t, nil)
		h.svc.users["carol"] = room.User{ID: "carol", Name: "Carol"} // nil Layers
		rec := h.do(req{method: "GET", path: "/api/me", user: "carol"})
		if !strings.Contains(rec.Body.String(), `"layers":[]`) {
			t.Fatalf("body = %s", rec.Body.String())
		}
	})
	t.Run("list rooms", func(t *testing.T) {
		h := newHarness(t, nil)
		h.svc.snaps = []room.Snapshot{{ID: "r_1"}, {ID: "r_2"}}
		rec := h.as("GET", "/api/rooms", "")
		var got []room.Snapshot
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || len(got) != 2 || got[0].ID != "r_1" || got[1].ID != "r_2" {
			t.Fatalf("rooms = %v (%v)\n%s", got, err, rec.Body.String())
		}
		h.wantLastCall("ListRooms")
	})
}

// ---- auth ----

func TestEveryAuthenticatedRouteNeedsAKnownUser(t *testing.T) {
	for _, tc := range routes {
		for _, user := range []string{"", "mallory"} {
			t.Run(fmt.Sprintf("%s as %q", tc.name, user), func(t *testing.T) {
				h := newHarness(t, nil)
				rec := h.do(req{method: tc.method, path: tc.path, body: tc.body, user: user})
				wantError(t, rec, http.StatusUnauthorized, "unauthenticated")
				if h.svc.callCount() != 0 {
					t.Fatalf("service was called by an unauthenticated request: %v", h.svc.calls)
				}
			})
		}
	}
	for _, p := range []string{"/api/me", "/api/rooms", "/api/rooms/r_1/events"} {
		t.Run("GET "+p, func(t *testing.T) {
			h := newHarness(t, nil)
			wantError(t, h.do(req{method: "GET", path: p}), http.StatusUnauthorized, "unauthenticated")
			wantError(t, h.do(req{method: "GET", path: p, user: "mallory"}), http.StatusUnauthorized, "unauthenticated")
		})
	}
}

// ---- errors ----

func TestServiceErrorsMapToTheErrorShape(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		code   string
		msg    string // must appear in the body (message passes through)
	}{
		{"not driver", &room.Error{Status: 403, Code: "not_driver", Message: "Only the driver can send steers."}, 403, "not_driver", "Only the driver"},
		{"not layer owner", &room.Error{Status: 403, Code: "not_layer_owner", Message: "You do not own that layer."}, 403, "not_layer_owner", "own that layer"},
		{"not recipient", &room.Error{Status: 403, Code: "not_recipient", Message: "Only Bob can accept."}, 403, "not_recipient", "Only Bob"},
		{"not found", &room.Error{Status: 404, Code: "not_found", Message: "No such room."}, 404, "not_found", "No such room"},
		{"already decided", &room.Error{Status: 409, Code: "already_decided", Message: "Already decided."}, 409, "already_decided", "Already decided"},
		{"room closed", &room.Error{Status: 409, Code: "room_closed", Message: "Room is closed."}, 409, "room_closed", "closed"},
		{"lease held", &room.Error{Status: 409, Code: "lease_held", Message: "Arjun holds control."}, 409, "lease_held", "Arjun"},
		{"stale handoff", &room.Error{Status: 409, Code: "stale_handoff", Message: "Out of date."}, 409, "stale_handoff", "Out of date"},
		{"validation", &room.Error{Status: 400, Code: "validation", Message: "Unknown pipeline."}, 400, "validation", "Unknown pipeline"},
		{"wrapped", fmt.Errorf("room: steer: %w", &room.Error{Status: 403, Code: "not_driver", Message: "Only the driver."}), 403, "not_driver", "Only the driver"},
		{"timeout", fmt.Errorf("room: handoff: %w", context.DeadlineExceeded), 504, "timeout", ""},
		{"bogus status falls back to 500", &room.Error{Status: 200, Code: "x", Message: "y"}, 500, "internal", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			h.svc.err = tc.err
			rec := h.as("POST", "/api/rooms/r_1/steer", `{"text":"go"}`)
			wantError(t, rec, tc.status, tc.code)
			if tc.msg != "" && !strings.Contains(rec.Body.String(), tc.msg) {
				t.Fatalf("message not passed through: %s", rec.Body.String())
			}
		})
	}
}

func TestEveryRouteMapsServiceErrors(t *testing.T) {
	for _, tc := range routes {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			h.svc.err = &room.Error{Status: 404, Code: "not_found", Message: "No such thing."}
			wantError(t, h.as(tc.method, tc.path, tc.body), 404, "not_found")
		})
	}
	t.Run("events", func(t *testing.T) {
		h := newHarness(t, nil)
		h.svc.err = &room.Error{Status: 404, Code: "not_found", Message: "No such room."}
		wantError(t, h.as("GET", "/api/rooms/r_1/events", ""), 404, "not_found")
	})
	t.Run("list rooms", func(t *testing.T) {
		h := newHarness(t, nil)
		h.svc.err = &room.Error{Status: 404, Code: "not_found", Message: "x"}
		wantError(t, h.as("GET", "/api/rooms", ""), 404, "not_found")
	})
}

func TestUnexpectedErrorsAreGenericAndLogged(t *testing.T) {
	h := newHarness(t, nil)
	h.svc.err = errors.New(`pq: password authentication failed for user "piperoom" (password=hunter2)`)
	rec := h.as("POST", "/api/rooms/r_1/comments", `{"text":"my private comment"}`)
	wantError(t, rec, http.StatusInternalServerError, "internal")
	if strings.Contains(rec.Body.String(), "hunter2") || strings.Contains(rec.Body.String(), "pq:") {
		t.Fatalf("internal error text leaked to the client: %s", rec.Body.String())
	}
	logs := h.log.String()
	if !strings.Contains(logs, "hunter2") {
		t.Fatalf("the real error was not logged: %s", logs)
	}
	if strings.Contains(logs, "my private comment") {
		t.Fatalf("request body was logged: %s", logs)
	}
}

func TestPanicBecomesInternalError(t *testing.T) {
	h := newHarness(t, nil)
	h.svc.panicIn = "GetRoom"
	wantError(t, h.as("GET", "/api/rooms/r_1", ""), http.StatusInternalServerError, "internal")
	// The router still works afterwards.
	h.svc.panicIn = ""
	if rec := h.as("GET", "/api/rooms/r_1", ""); rec.Code != 200 {
		t.Fatalf("status after panic = %d", rec.Code)
	}
}

func TestNotFoundAndMethodNotAllowedAreJSON(t *testing.T) {
	h := newHarness(t, nil)
	wantError(t, h.as("GET", "/api/nope", ""), http.StatusNotFound, "not_found")
	wantError(t, h.as("GET", "/api/rooms/r_1/steer", ""), http.StatusMethodNotAllowed, "method_not_allowed")
	wantError(t, h.as("DELETE", "/api/rooms/r_1", ""), http.StatusMethodNotAllowed, "method_not_allowed")
	wantError(t, h.do(req{method: "GET", path: "/api/alerts/webhook"}), http.StatusMethodNotAllowed, "method_not_allowed")
}

// ---- request validation ----

func TestBadBodiesAreRejectedBeforeTheService(t *testing.T) {
	type bodyCase struct {
		name string
		body string
		code string
	}
	common := []bodyCase{
		{"empty body", "", "bad_request"},
		{"not JSON", "{", "bad_request"},
		{"array", "[]", "bad_request"},
		{"string", `"text"`, "bad_request"},
		{"wrong field type", `{"text":42}`, "bad_request"},
		{"two objects", `{"text":"a"}{"text":"b"}`, "bad_request"},
		{"trailing garbage", `{"text":"a"} x`, "bad_request"},
	}
	endpoints := []struct {
		name     string
		path     string
		field    string // a required text field
		valid    string // valid body
		unknown  string // body with an unknown field
		allEmpty string // every required field empty
	}{
		{"create room", "/api/rooms", "title", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`, `{"title":"t","severity":"sev2","pipeline_id":"orders","x":1}`, `{"title":" ","severity":"sev2","pipeline_id":"orders"}`},
		{"comment", "/api/rooms/r_1/comments", "text", `{"text":"t"}`, `{"text":"t","author":"x"}`, `{"text":"   "}`},
		{"steer", "/api/rooms/r_1/steer", "text", `{"text":"t"}`, `{"text":"t","force":true}`, `{"text":""}`},
		{"grant", "/api/rooms/r_1/control/grant", "to", `{"to":"bob"}`, `{"to":"bob","x":1}`, `{"to":""}`},
		{"close", "/api/rooms/r_1/close", "resolution", `{"resolution":"done"}`, `{"resolution":"done","x":1}`, `{"resolution":"\n"}`},
		{"approve", "/api/proposals/p_1/approve", "layer", `{"layer":"kafka"}`, `{"layer":"kafka","x":1}`, `{"layer":""}`},
		{"reject", "/api/proposals/p_1/reject", "layer", `{"layer":"kafka","reason":"r"}`, `{"layer":"kafka","reason":"r","x":1}`, `{"layer":"kafka","reason":" "}`},
		{"handoff", "/api/rooms/r_1/handoffs", "to", `{"to":"bob"}`, `{"to":"bob","x":1}`, `{"to":""}`},
		{"trace", "/api/rooms/r_1/trace", "table", `{"table":"public.orders","key":"1"}`, `{"table":"public.orders","key":"1","x":1}`, `{"table":"public.orders","key":""}`},
	}
	for _, ep := range endpoints {
		cases := append([]bodyCase{}, common...)
		cases = append(cases,
			bodyCase{"unknown field", ep.unknown, "bad_request"},
			bodyCase{"empty required text", ep.allEmpty, "validation"},
			bodyCase{"empty object", `{}`, "validation"},
		)
		for _, tc := range cases {
			t.Run(ep.name+"/"+tc.name, func(t *testing.T) {
				h := newHarness(t, nil)
				wantError(t, h.as("POST", ep.path, tc.body), http.StatusBadRequest, tc.code)
				if h.svc.callCount() != 0 {
					t.Fatalf("service called for a bad body: %v", h.svc.calls)
				}
			})
		}
		t.Run(ep.name+"/valid body still works", func(t *testing.T) {
			h := newHarness(t, nil)
			if rec := h.as("POST", ep.path, ep.valid); rec.Code != 200 {
				t.Fatalf("status = %d\n%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestBodyIsCappedAtOneMegabyte(t *testing.T) {
	t.Run("just under the limit passes", func(t *testing.T) {
		h := newHarness(t, nil)
		body := `{"text":"` + strings.Repeat("a", maxBodyBytes-20) + `"}`
		if rec := h.as("POST", "/api/rooms/r_1/comments", body); rec.Code != 200 {
			t.Fatalf("status = %d\n%.200s", rec.Code, rec.Body.String())
		}
	})
	t.Run("over the limit is 413", func(t *testing.T) {
		h := newHarness(t, nil)
		body := `{"text":"` + strings.Repeat("a", maxBodyBytes) + `"}`
		wantError(t, h.as("POST", "/api/rooms/r_1/comments", body), http.StatusRequestEntityTooLarge, "payload_too_large")
		if h.svc.callCount() != 0 {
			t.Fatal("service called with an oversized body")
		}
	})
	t.Run("trailing bytes past the limit are caught too", func(t *testing.T) {
		h := newHarness(t, nil)
		body := `{"text":"a"}` + strings.Repeat(" ", maxBodyBytes)
		wantError(t, h.as("POST", "/api/rooms/r_1/comments", body), http.StatusRequestEntityTooLarge, "payload_too_large")
	})
}

// ---- events query ----

func TestEventsQuery(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		wantAfter int64
		wantLimit int
		status    int
	}{
		{"defaults", "", 0, 500, 200},
		{"after and limit", "?after=42&limit=10", 42, 10, 200},
		{"limit at the max", "?limit=1000", 0, 1000, 200},
		{"limit above the max is clamped", "?limit=5000", 0, 1000, 200},
		{"huge limit is clamped", "?limit=99999999999", 0, 1000, 200},
		{"limit past int64 is rejected", "?limit=99999999999999999999", 0, 0, 400},
		{"after only", "?after=7", 7, 500, 200},
		{"limit zero", "?limit=0", 0, 0, 400},
		{"limit negative", "?limit=-5", 0, 0, 400},
		{"limit not a number", "?limit=abc", 0, 0, 400},
		{"after negative", "?after=-1", 0, 0, 400},
		{"after not a number", "?after=x", 0, 0, 400},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			rec := h.as("GET", "/api/rooms/r_1/events"+tc.query, "")
			if tc.status != 200 {
				wantError(t, rec, tc.status, "bad_request")
				if h.svc.callCount() != 0 {
					t.Fatal("service called with a bad query")
				}
				return
			}
			if rec.Code != 200 {
				t.Fatalf("status = %d\n%s", rec.Code, rec.Body.String())
			}
			h.wantLastCall("Events", "r_1", tc.wantAfter, tc.wantLimit)
		})
	}
}

func TestEventsPage(t *testing.T) {
	t.Run("nothing after is an empty array, not null", func(t *testing.T) {
		h := newHarness(t, nil)
		h.svc.page = room.EventsPage{LastSeq: 12} // Events is nil
		rec := h.as("GET", "/api/rooms/r_1/events?after=12", "")
		if !strings.Contains(rec.Body.String(), `"events":[]`) || strings.Contains(rec.Body.String(), "null") {
			t.Fatalf("body = %s", rec.Body.String())
		}
		if m := decodeMap(t, rec); m["last_seq"] != float64(12) {
			t.Fatalf("last_seq = %v", m["last_seq"])
		}
	})
	t.Run("events pass through in order", func(t *testing.T) {
		h := newHarness(t, nil)
		e1, e2 := h.svc.ev, h.svc.ev
		e1.Seq, e2.Seq = 1, 2
		h.svc.page = room.EventsPage{Events: []events.Event{e1, e2}, LastSeq: 2}
		var got room.EventsPage
		if err := json.Unmarshal(h.as("GET", "/api/rooms/r_1/events", "").Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if len(got.Events) != 2 || got.Events[0].Seq != 1 || got.Events[1].Seq != 2 || got.LastSeq != 2 {
			t.Fatalf("page = %+v", got)
		}
	})
}

// ---- arrays are never null ----

func TestArraysAreNeverNull(t *testing.T) {
	t.Run("list rooms with none", func(t *testing.T) {
		h := newHarness(t, nil)
		h.svc.snaps = nil
		if body := strings.TrimSpace(h.as("GET", "/api/rooms", "").Body.String()); body != "[]" {
			t.Fatalf("body = %q, want []", body)
		}
	})
	t.Run("snapshot slices", func(t *testing.T) {
		h := newHarness(t, nil)
		h.svc.snap = room.Snapshot{
			ID:            "r_1",
			Hypothesis:    &room.Hypothesis{Text: "x"},
			OpenProposals: []room.OpenProposal{{ProposalID: "p_1"}},
		}
		rec := h.as("GET", "/api/rooms/r_1", "")
		m := decodeMap(t, rec)
		for _, k := range []string{"open_proposals", "pending_suggestions", "members"} {
			if arr, ok := m[k].([]any); !ok || arr == nil {
				t.Fatalf("%s = %v, want []", k, m[k])
			}
		}
		p := m["open_proposals"].([]any)[0].(map[string]any)
		for _, k := range []string{"steps", "required_layers"} {
			if arr, ok := p[k].([]any); !ok || arr == nil {
				t.Fatalf("proposal %s = %v, want []", k, p[k])
			}
		}
		if obj, ok := p["approvals"].(map[string]any); !ok || obj == nil {
			t.Fatalf("approvals = %v, want {}", p["approvals"])
		}
		hy := m["hypothesis"].(map[string]any)
		if arr, ok := hy["based_on_steps"].([]any); !ok || arr == nil {
			t.Fatalf("based_on_steps = %v", hy["based_on_steps"])
		}
		// Nullable objects stay null.
		if m["driver"] != nil || m["closed_at"] != nil {
			t.Fatalf("driver/closed_at = %v/%v, want null", m["driver"], m["closed_at"])
		}
	})
	t.Run("snapshot from create room", func(t *testing.T) {
		h := newHarness(t, nil)
		rec := h.as("POST", "/api/rooms", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
		if !strings.Contains(rec.Body.String(), `"members":[]`) {
			t.Fatalf("body = %s", rec.Body.String())
		}
	})
	t.Run("topology", func(t *testing.T) {
		h := newHarness(t, nil)
		h.svc.graph = topology.Graph{PipelineID: "orders"}
		rec := h.as("GET", "/api/pipelines/orders/topology", "")
		if !strings.Contains(rec.Body.String(), `"nodes":[]`) || !strings.Contains(rec.Body.String(), `"edges":[]`) {
			t.Fatalf("body = %s", rec.Body.String())
		}
	})
	t.Run("the service's cached snapshot is not modified", func(t *testing.T) {
		h := newHarness(t, nil)
		cached := []room.OpenProposal{{ProposalID: "p_1"}}
		h.svc.snap = room.Snapshot{ID: "r_1", OpenProposals: cached}
		h.as("GET", "/api/rooms/r_1", "")
		if cached[0].Steps != nil {
			t.Fatal("handler edited the service's slice in place")
		}
	})
}

// ---- CORS ----

func TestCORS(t *testing.T) {
	const origin = "http://localhost:5173"

	t.Run("preflight from the allowed origin", func(t *testing.T) {
		h := newHarness(t, nil)
		rec := h.do(req{method: "OPTIONS", path: "/api/rooms/r_1/steer", header: map[string]string{
			"Origin":                         origin,
			"Access-Control-Request-Method":  "POST",
			"Access-Control-Request-Headers": "x-user,content-type",
		}})
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", rec.Code)
		}
		hd := rec.Header()
		if hd.Get("Access-Control-Allow-Origin") != origin {
			t.Fatalf("Allow-Origin = %q", hd.Get("Access-Control-Allow-Origin"))
		}
		for _, want := range []string{"X-User", "Content-Type"} {
			if !strings.Contains(hd.Get("Access-Control-Allow-Headers"), want) {
				t.Fatalf("Allow-Headers = %q, missing %s", hd.Get("Access-Control-Allow-Headers"), want)
			}
		}
		for _, want := range []string{"GET", "POST"} {
			if !strings.Contains(hd.Get("Access-Control-Allow-Methods"), want) {
				t.Fatalf("Allow-Methods = %q, missing %s", hd.Get("Access-Control-Allow-Methods"), want)
			}
		}
		if !strings.Contains(hd.Get("Vary"), "Origin") {
			t.Fatalf("Vary = %q", hd.Get("Vary"))
		}
		if h.svc.callCount() != 0 {
			t.Fatal("preflight reached the service")
		}
	})
	t.Run("preflight needs no X-User", func(t *testing.T) {
		h := newHarness(t, nil)
		rec := h.do(req{method: "OPTIONS", path: "/api/me", header: map[string]string{
			"Origin": origin, "Access-Control-Request-Method": "GET"}})
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d", rec.Code)
		}
	})
	t.Run("preflight from another origin gets no CORS headers", func(t *testing.T) {
		h := newHarness(t, nil)
		for _, o := range []string{"http://evil.example", "http://localhost:5174", "https://localhost:5173", "*", "null"} {
			rec := h.do(req{method: "OPTIONS", path: "/api/rooms", header: map[string]string{
				"Origin": o, "Access-Control-Request-Method": "POST"}})
			if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
				t.Fatalf("origin %q was allowed: %q", o, got)
			}
			if rec.Header().Get("Access-Control-Allow-Methods") != "" {
				t.Fatalf("origin %q got Allow-Methods", o)
			}
		}
	})
	t.Run("actual requests carry the header, also when they fail", func(t *testing.T) {
		h := newHarness(t, nil)
		ok := h.do(req{method: "GET", path: "/api/me", user: "alice", header: map[string]string{"Origin": origin}})
		if ok.Header().Get("Access-Control-Allow-Origin") != origin {
			t.Fatalf("200 response: Allow-Origin = %q", ok.Header().Get("Access-Control-Allow-Origin"))
		}
		denied := h.do(req{method: "GET", path: "/api/me", header: map[string]string{"Origin": origin}})
		if denied.Code != 401 || denied.Header().Get("Access-Control-Allow-Origin") != origin {
			t.Fatalf("401 response: status %d, Allow-Origin = %q", denied.Code, denied.Header().Get("Access-Control-Allow-Origin"))
		}
		missing := h.do(req{method: "GET", path: "/api/missing", user: "alice", header: map[string]string{"Origin": origin}})
		if missing.Code != 404 || missing.Header().Get("Access-Control-Allow-Origin") != origin {
			t.Fatalf("404 response: Allow-Origin = %q", missing.Header().Get("Access-Control-Allow-Origin"))
		}
	})
	t.Run("no Origin header, no CORS header", func(t *testing.T) {
		h := newHarness(t, nil)
		rec := h.as("GET", "/api/me", "")
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("Allow-Origin = %q", got)
		}
	})
	t.Run("foreign origin on an actual request gets no header", func(t *testing.T) {
		h := newHarness(t, nil)
		rec := h.do(req{method: "GET", path: "/api/me", user: "alice", header: map[string]string{"Origin": "http://evil.example"}})
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("Allow-Origin = %q", got)
		}
	})
	t.Run("configurable list", func(t *testing.T) {
		h := newHarness(t, func(c *Config) { c.AllowedOrigins = []string{"https://piperoom.example", "http://localhost:3000"} })
		for _, o := range []string{"https://piperoom.example", "http://localhost:3000"} {
			rec := h.do(req{method: "OPTIONS", path: "/api/me", header: map[string]string{
				"Origin": o, "Access-Control-Request-Method": "GET"}})
			if rec.Header().Get("Access-Control-Allow-Origin") != o {
				t.Fatalf("origin %q not allowed", o)
			}
		}
		// The default origin is replaced, not added to.
		rec := h.do(req{method: "OPTIONS", path: "/api/me", header: map[string]string{
			"Origin": origin, "Access-Control-Request-Method": "GET"}})
		if rec.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatal("default origin still allowed after configuring a list")
		}
	})
}

// ---- timeouts ----

func TestRequestTimeouts(t *testing.T) {
	t.Run("ordinary routes get 60 s, hand-offs get more than 3 min", func(t *testing.T) {
		h := newHarness(t, nil)
		h.as("GET", "/api/rooms/r_1", "")
		h.as("POST", "/api/rooms/r_1/handoffs", `{"to":"bob"}`)
		if d := h.svc.deadline("GetRoom"); d < 55*time.Second || d > 60*time.Second {
			t.Fatalf("GetRoom deadline in %v, want about 60s", d)
		}
		if d := h.svc.deadline("CreateHandoff"); d < 3*time.Minute {
			t.Fatalf("CreateHandoff deadline in %v, want at least 3 min", d)
		}
	})
	t.Run("every other route is bounded", func(t *testing.T) {
		for _, tc := range routes {
			if tc.call == "CreateHandoff" {
				continue
			}
			h := newHarness(t, nil)
			h.as(tc.method, tc.path, tc.body)
			if d := h.svc.deadline(tc.call); d <= 0 || d > 60*time.Second {
				t.Fatalf("%s deadline in %v, want at most 60s", tc.call, d)
			}
		}
	})
	t.Run("webhooks are bounded", func(t *testing.T) {
		h := newHarness(t, nil)
		h.ver.alert = alertin.Alert{Status: alertin.StatusFiring, Title: "x"}
		h.do(req{method: "POST", path: "/api/alerts/webhook", body: `{}`})
		if d := h.svc.deadline("IngestAlert"); d <= 0 || d > 60*time.Second {
			t.Fatalf("IngestAlert deadline in %v", d)
		}
	})
	t.Run("configurable", func(t *testing.T) {
		h := newHarness(t, func(c *Config) {
			c.RequestTimeout = 5 * time.Second
			c.HandoffTimeout = 20 * time.Second
		})
		h.as("GET", "/api/rooms/r_1", "")
		h.as("POST", "/api/rooms/r_1/handoffs", `{"to":"bob"}`)
		if d := h.svc.deadline("GetRoom"); d > 5*time.Second || d < 3*time.Second {
			t.Fatalf("GetRoom deadline in %v", d)
		}
		if d := h.svc.deadline("CreateHandoff"); d > 20*time.Second || d < 15*time.Second {
			t.Fatalf("CreateHandoff deadline in %v", d)
		}
	})
	t.Run("a handler that runs out of time answers 504", func(t *testing.T) {
		h := newHarness(t, func(c *Config) { c.RequestTimeout = 20 * time.Millisecond })
		h.svc.err = context.DeadlineExceeded
		wantError(t, h.as("GET", "/api/rooms/r_1", ""), http.StatusGatewayTimeout, "timeout")
	})
}

// ---- WebSocket route ----

func TestWebSocketRouteIsDelegated(t *testing.T) {
	var gotID, gotQuery string
	var hadDeadline bool
	ws := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID, gotQuery = r.PathValue("id"), r.URL.RawQuery
		_, hadDeadline = r.Context().Deadline()
		w.WriteHeader(http.StatusTeapot)
	})
	h := newHarness(t, func(c *Config) { c.WS = ws })
	// No X-User: the WS handler authenticates ?user= itself.
	rec := h.do(req{method: "GET", path: "/api/rooms/r_7/ws?user=alice&after=3"})
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want the WS handler's 418", rec.Code)
	}
	if gotID != "r_7" || gotQuery != "user=alice&after=3" {
		t.Fatalf("WS handler saw id %q query %q", gotID, gotQuery)
	}
	if hadDeadline {
		t.Fatal("WebSocket requests must not carry the request timeout")
	}
	if h.svc.callCount() != 0 {
		t.Fatal("the service was called for the WS route")
	}
	// An unknown X-User does not matter either.
	if rec := h.do(req{method: "GET", path: "/api/rooms/r_7/ws?user=alice", user: "mallory"}); rec.Code != http.StatusTeapot {
		t.Fatalf("status with a bad X-User = %d", rec.Code)
	}
	// The query string (it holds ?user=) stays out of the request log.
	if strings.Contains(h.log.String(), "user=alice") {
		t.Fatalf("query string was logged: %s", h.log.String())
	}
}

func TestWebSocketRouteWithoutHandlerIs404(t *testing.T) {
	h := newHarness(t, nil)
	wantError(t, h.do(req{method: "GET", path: "/api/rooms/r_1/ws?user=alice"}), http.StatusNotFound, "not_found")
}

// ---- webhooks ----

func firingAlert() alertin.Alert {
	return alertin.Alert{
		Source:     alertin.Generic,
		Status:     alertin.StatusFiring,
		Title:      "Warehouse orders stale",
		Severity:   "sev2",
		PipelineID: "orders",
		DedupKey:   "generic:abc",
		Raw:        json.RawMessage(`{"title":"Warehouse orders stale"}`),
	}
}

func TestWebhookSources(t *testing.T) {
	cases := []struct {
		path string
		want alertin.Source
		code int
	}{
		{"/api/alerts/webhook", alertin.Generic, 200},
		{"/api/alerts/generic", alertin.Generic, 200},
		{"/api/alerts/alertmanager", alertin.Alertmanager, 200},
		{"/api/alerts/grafana", alertin.Grafana, 200},
		{"/api/alerts/pagerduty", alertin.PagerDuty, 200},
		{"/api/alerts/datadog", alertin.Datadog, 200},
		{"/api/alerts/nagios", "", 404},
		{"/api/alerts/Webhook", "", 404},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			h := newHarness(t, nil)
			h.ver.alert = firingAlert()
			rec := h.do(req{method: "POST", path: tc.path, body: `{"a":1}`})
			if tc.code == 404 {
				wantError(t, rec, 404, "not_found")
				if h.ver.calls != 0 {
					t.Fatal("verifier called for an unknown source")
				}
				return
			}
			if rec.Code != 200 {
				t.Fatalf("status = %d\n%s", rec.Code, rec.Body.String())
			}
			if h.ver.source != tc.want {
				t.Fatalf("source = %q, want %q", h.ver.source, tc.want)
			}
			h.wantLastCall("IngestAlert", firingAlert())
			if m := decodeMap(t, rec); m["id"] != "r_1" {
				t.Fatalf("not a RoomSnapshot: %s", rec.Body.String())
			}
		})
	}
}

func TestWebhookPassesExactBytesAndHeaders(t *testing.T) {
	h := newHarness(t, nil)
	h.ver.alert = firingAlert()
	body := "  {\"title\" :\"x\" ,\n\t\"n\":1.0}\r\n\xff\xfe " // whitespace and invalid UTF-8 must survive
	h.do(req{method: "POST", path: "/api/alerts/webhook", body: body, header: map[string]string{
		"X-Piperoom-Signature": "t=1,v1=abc",
		"Authorization":        "Bearer s3cret",
	}})
	if string(h.ver.body) != body {
		t.Fatalf("verifier saw %q, want %q", h.ver.body, body)
	}
	if h.ver.header.Get("X-Piperoom-Signature") != "t=1,v1=abc" || h.ver.header.Get("Authorization") != "Bearer s3cret" {
		t.Fatalf("headers not passed: %v", h.ver.header)
	}
}

func TestWebhookDoesNotNeedXUser(t *testing.T) {
	h := newHarness(t, nil)
	h.ver.alert = firingAlert()
	for _, user := range []string{"", "mallory", "alice"} {
		rec := h.do(req{method: "POST", path: "/api/alerts/webhook", body: `{}`, user: user})
		if rec.Code != 200 {
			t.Fatalf("X-User %q: status = %d", user, rec.Code)
		}
	}
}

func TestWebhookStatuses(t *testing.T) {
	resolved := firingAlert()
	resolved.Status = alertin.StatusResolved

	cases := []struct {
		name       string
		alert      alertin.Alert
		err        error
		svcErr     error
		status     int
		code       string // error code, "" for non-error bodies
		wantIgnore bool   // body is {"status":"ignored"}
		ingested   bool
	}{
		{name: "firing opens a room", alert: firingAlert(), status: 200, ingested: true},
		{name: "unauthenticated", err: alertin.ErrUnauthenticated, status: 401, code: "unauthenticated"},
		{name: "wrapped unauthenticated", err: fmt.Errorf("%w: wrong bearer token", alertin.ErrUnauthenticated), status: 401, code: "unauthenticated"},
		{name: "stale", err: fmt.Errorf("%w: occurred 2h0m0s ago", alertin.ErrStale), status: 401, code: "unauthenticated"},
		{name: "too large", err: alertin.ErrTooLarge, status: 413, code: "payload_too_large"},
		{name: "ignored", err: fmt.Errorf("%w: pagerduty event incident.acknowledged", alertin.ErrIgnored), status: 202, wantIgnore: true},
		{name: "resolved is ignored", alert: resolved, status: 202, wantIgnore: true},
		{name: "unparseable", err: errors.New(`alertin: decode generic payload: invalid character 'x' (body: SECRET-PAYLOAD)`), status: 400, code: "bad_request"},
		{name: "service validation error", alert: firingAlert(), svcErr: &room.Error{Status: 400, Code: "validation", Message: "Unknown pipeline."}, status: 400, code: "validation", ingested: true},
		{name: "service internal error", alert: firingAlert(), svcErr: errors.New("db down"), status: 500, code: "internal", ingested: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			h.ver.alert, h.ver.err, h.svc.err = tc.alert, tc.err, tc.svcErr
			rec := h.do(req{method: "POST", path: "/api/alerts/alertmanager", body: `{"k":"v"}`})
			if tc.code != "" {
				wantError(t, rec, tc.status, tc.code)
			} else if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d\n%s", rec.Code, tc.status, rec.Body.String())
			}
			if tc.wantIgnore {
				if m := decodeMap(t, rec); !reflect.DeepEqual(m, map[string]any{"status": "ignored"}) {
					t.Fatalf("body = %v, want {status: ignored}", m)
				}
			}
			if got := h.svc.callCount() == 1; got != tc.ingested {
				t.Fatalf("IngestAlert called = %v, want %v", got, tc.ingested)
			}
			// Nothing the verifier said about the request leaks to the sender.
			for _, leak := range []string{"bearer", "SECRET-PAYLOAD", "invalid character", "2h0m0s", "pagerduty"} {
				if strings.Contains(strings.ToLower(rec.Body.String()), strings.ToLower(leak)) {
					t.Fatalf("response leaks %q: %s", leak, rec.Body.String())
				}
			}
		})
	}
}

func TestWebhookBodyOverTheLimitIs413WithoutCallingTheVerifier(t *testing.T) {
	h := newHarness(t, nil)
	h.ver.alert = firingAlert()
	body := strings.Repeat("a", alertin.MaxBodyBytes+1)
	wantError(t, h.do(req{method: "POST", path: "/api/alerts/webhook", body: body}), http.StatusRequestEntityTooLarge, "payload_too_large")
	if h.ver.calls != 0 || h.svc.callCount() != 0 {
		t.Fatal("verifier or service called for an oversized webhook")
	}
	// Exactly at the limit is accepted.
	rec := h.do(req{method: "POST", path: "/api/alerts/webhook", body: strings.Repeat("a", alertin.MaxBodyBytes)})
	if rec.Code != 200 || len(h.ver.body) != alertin.MaxBodyBytes {
		t.Fatalf("status = %d, verifier saw %d bytes", rec.Code, len(h.ver.body))
	}
}

func TestWebhookWithoutVerifierFailsClosed(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.Verifier = nil })
	wantError(t, h.do(req{method: "POST", path: "/api/alerts/webhook", body: `{}`}), http.StatusUnauthorized, "unauthenticated")
	if h.svc.callCount() != 0 {
		t.Fatal("service called without a verifier")
	}
}

func TestWebhookRequestsAreNotLoggedWithBodiesOrCredentials(t *testing.T) {
	h := newHarness(t, nil)
	h.ver.err = fmt.Errorf("%w: wrong bearer token", alertin.ErrUnauthenticated)
	h.do(req{method: "POST", path: "/api/alerts/alertmanager", body: `{"note":"PAYLOAD-MARKER"}`,
		header: map[string]string{"Authorization": "Bearer TOKEN-MARKER", "X-Piperoom-Signature": "SIG-MARKER"}})
	logs := h.log.String()
	for _, secret := range []string{"PAYLOAD-MARKER", "TOKEN-MARKER", "SIG-MARKER"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("log contains %q: %s", secret, logs)
		}
	}
	if !strings.Contains(logs, "alertmanager") {
		t.Fatalf("rejection was not logged: %s", logs)
	}
}

// ---- logging ----

func TestRequestLogHasNoBodiesAndOnlyTheUserID(t *testing.T) {
	h := newHarness(t, nil)
	h.as("POST", "/api/rooms/r_1/comments", `{"text":"BODY-MARKER"}`)
	h.do(req{method: "GET", path: "/api/me", user: "HEADER-MARKER"})

	logs := h.log.String()
	for _, secret := range []string{"BODY-MARKER", "HEADER-MARKER"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("log contains %q:\n%s", secret, logs)
		}
	}
	var entries []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if m["msg"] == "http request" {
			entries = append(entries, m)
		}
	}
	if len(entries) != 2 {
		t.Fatalf("got %d request log lines, want 2:\n%s", len(entries), logs)
	}
	first := entries[0]
	if first["method"] != "POST" || first["route"] != "/api/rooms/{id}/comments" || first["status"] != float64(200) || first["user"] != "alice" {
		t.Fatalf("first request log = %v", first)
	}
	if first["level"] != "INFO" || entries[1]["level"] != "WARN" || entries[1]["user"] != "" {
		t.Fatalf("levels/user wrong: %v / %v", first, entries[1])
	}
}

func TestResponsesAreJSONWithNoStore(t *testing.T) {
	h := newHarness(t, nil)
	for _, rec := range []*httptest.ResponseRecorder{
		h.as("GET", "/api/me", ""),
		h.as("GET", "/api/rooms/r_1/events", ""),
		h.do(req{method: "GET", path: "/api/me"}),
	} {
		if rec.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("Content-Type = %q", rec.Header().Get("Content-Type"))
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("Cache-Control = %q", rec.Header().Get("Cache-Control"))
		}
	}
}

// ---- end to end through a real server ----

func TestOverRealHTTP(t *testing.T) {
	h := newHarness(t, nil)
	srv := httptest.NewServer(h.h)
	defer srv.Close()

	post := func(path, user, body string) (*http.Response, string) {
		t.Helper()
		rq, err := http.NewRequest("POST", srv.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		rq.Header.Set("X-User", user)
		rq.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(rq)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp, string(b)
	}
	resp, body := post("/api/rooms/r_1/steer", "alice", `{"text":"go"}`)
	if resp.StatusCode != 200 || !strings.Contains(body, `"seq":7`) {
		t.Fatalf("status %d body %s", resp.StatusCode, body)
	}
	// An oversized body over a real connection is answered, not reset.
	resp, body = post("/api/rooms/r_1/comments", "alice", `{"text":"`+strings.Repeat("a", 2*maxBodyBytes)+`"}`)
	if resp.StatusCode != http.StatusRequestEntityTooLarge || !strings.Contains(body, "payload_too_large") {
		t.Fatalf("status %d body %.200s", resp.StatusCode, body)
	}
}
