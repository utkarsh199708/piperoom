package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

const shadowHTTPOwners = `pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
    shadow: {enabled: true}
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
`

const shadowHTTPUsers = `users:
  - {id: arjun, name: Arjun, layers: [kafka-connect, kafka]}
  - {id: priya, name: Priya, layers: [postgres, schema-registry]}
  - {id: meera, name: Meera, layers: [warehouse]}
  - {id: kabir, name: Kabir, layers: [consumer]}
  - {id: nolayers, name: No Layers}
`

const (
	httpSecretText = "SECRET-SHADOW-TEXT"
	httpSecretData = "SECRET-SHADOW-DATA"
)

// shadowAgent emits one of each agent event, then ends at end_turn on the
// closed inbox, the way agent.Run does.
func shadowAgent(ctx context.Context, _ agent.Alert, d agent.Deps, _ agent.Options) error {
	_ = d.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working"})
	_ = d.Sink.Emit(ctx, events.AgentStepStart, events.AgentStepStartPayload{StepID: "s_1", Title: "look"})
	_ = d.Sink.Emit(ctx, events.AgentToolCall, events.AgentToolCallPayload{StepID: "s_1", ToolUseID: "tu", Tool: "connect_status", Input: json.RawMessage(`{}`)})
	_ = d.Sink.Emit(ctx, events.AgentToolResult, events.AgentToolResultPayload{StepID: "s_1", ToolUseID: "tu", Tool: "connect_status",
		Summary: "failed", Data: json.RawMessage(`{"row":"` + httpSecretData + `"}`)})
	_ = d.Sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: httpSecretText})
	_ = d.Sink.Emit(ctx, events.AgentHypothesis, events.AgentHypothesisPayload{Text: httpSecretText, Confidence: "high"})
	_ = d.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle"})
	if err := d.Inbox.Wait(ctx); errors.Is(err, agent.ErrInboxClosed) {
		return nil
	}
	return errors.New("test: shadow inbox not closed")
}

func newShadowServer(t *testing.T) (*httptest.Server, *memstore.Store) {
	t.Helper()
	users, err := room.ParseUsers([]byte(shadowHTTPUsers))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(shadowHTTPOwners), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st, Users: users, Owners: own, Run: shadowAgent,
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{WhatBroke: "x"}, nil
		},
	})
	api := httpapi.New(svc, st, nil)
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})
	return srv, st
}

func code(t *testing.T, resp *http.Response) string {
	t.Helper()
	body := decode[map[string]map[string]string](t, resp)
	return body["error"]["code"]
}

func wantErr(t *testing.T, resp *http.Response, status int, c string) {
	t.Helper()
	if resp.StatusCode != status {
		t.Fatalf("status %d, want %d", resp.StatusCode, status)
	}
	if got := code(t, resp); got != c {
		t.Fatalf("code %q, want %q", got, c)
	}
}

// wsCollector reads every event the room's WebSocket sends.
type wsCollector struct {
	mu  sync.Mutex
	evs []events.Event
}

func (c *wsCollector) all() []events.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]events.Event{}, c.evs...)
}

func collectWS(t *testing.T, ctx context.Context, srv *httptest.Server, roomID string) *wsCollector {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/rooms/" + roomID + "/ws?after=0"
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {"kabir"}}})
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	conn.SetReadLimit(1 << 20)
	c := &wsCollector{}
	go func() {
		defer conn.Close(websocket.StatusNormalClosure, "")
		for {
			_, b, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var msg struct {
				Type  string        `json:"type"`
				Event *events.Event `json:"event"`
			}
			if json.Unmarshal(b, &msg) == nil && msg.Type == "event" && msg.Event != nil {
				c.mu.Lock()
				c.evs = append(c.evs, *msg.Event)
				c.mu.Unlock()
			}
		}
	}()
	return c
}

func noAgentEvents(t *testing.T, where string, evs []events.Event) {
	t.Helper()
	for _, e := range evs {
		if e.Actor == events.AgentActor {
			t.Fatalf("%s: agent-actor event %d %s", where, e.Seq, e.Type)
		}
		if strings.Contains(string(e.Payload), httpSecretText) || strings.Contains(string(e.Payload), httpSecretData) {
			t.Fatalf("%s: agent text in event %d %s", where, e.Seq, e.Type)
		}
	}
}

// The leak test over the wire: a shadow room's WebSocket stream and GET /events
// carry no agent-actor event, before and after the reveal, label and grade.
// Then the shadow routes work end to end.
func TestShadowRoomOverHTTP(t *testing.T) {
	srv, st := newShadowServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/alerts/webhook", "arjun", `{"title":"orders stale","pipeline_id":"orders","dedup_key":"k1"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("webhook status %d", resp.StatusCode)
	}
	snap := decode[map[string]any](t, resp)
	roomID, _ := snap["id"].(string)
	if snap["shadow"] != true {
		t.Fatalf("snapshot shadow = %v", snap["shadow"])
	}
	ws := collectWS(t, ctx, srv, roomID)

	waitRun := func() store.ShadowRun {
		var r store.ShadowRun
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			got, err := st.GetShadowRun(ctx, roomID)
			if err == nil && got.Finished() {
				return got
			}
			r = got
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("run did not finish: %+v", r)
		return r
	}
	if r := waitRun(); r.Status != store.ShadowHypothesised {
		t.Fatalf("run status %q", r.Status)
	}

	path := srv.URL + "/api/rooms/" + roomID
	wantErr(t, doJSON(t, http.MethodGet, path+"/shadow", "kabir", ""), http.StatusConflict, "room_open")
	wantErr(t, doJSON(t, http.MethodPost, path+"/shadow/blind", "kabir", `{"actual_layer":"kafka","root_cause_terms":["x"]}`), http.StatusConflict, "room_open")
	wantStatus(t, doJSON(t, http.MethodPost, path+"/control/grant", "arjun", `{"to":"arjun"}`), http.StatusOK)
	wantErr(t, doJSON(t, http.MethodPost, path+"/close", "arjun", `{"resolution":"  "}`), http.StatusBadRequest, "validation")
	wantStatus(t, doJSON(t, http.MethodPost, path+"/close", "arjun", `{"resolution":"schema change"}`), http.StatusOK)

	wantErr(t, doJSON(t, http.MethodGet, path+"/shadow", "kabir", ""), http.StatusConflict, "not_labelled")
	wantErr(t, doJSON(t, http.MethodGet, path+"/export?include=shadow", "priya", ""), http.StatusConflict, "not_labelled")
	wantErr(t, doJSON(t, http.MethodPost, path+"/shadow/grade", "kabir", `{"layer_correct":true,"root_cause":"yes"}`), http.StatusConflict, "not_labelled")
	wantErr(t, doJSON(t, http.MethodPost, path+"/shadow/blind", "nolayers", `{"actual_layer":"kafka","root_cause_terms":["x"]}`), http.StatusForbidden, "not_allowed")
	wantErr(t, doJSON(t, http.MethodPost, path+"/shadow/blind", "kabir", `{"actual_layer":"nope","root_cause_terms":["x"]}`), http.StatusBadRequest, "validation")
	wantErr(t, doJSON(t, http.MethodPost, path+"/shadow/blind", "kabir", `{"actual_layer":"kafka","root_cause_terms":[]}`), http.StatusBadRequest, "validation")

	resp = doJSON(t, http.MethodPost, path+"/shadow/blind", "kabir", `{"actual_layer":"kafka-connect","root_cause_terms":["schema","avro"]}`)
	wantStatus(t, resp, http.StatusOK)
	label := decode[events.Event](t, resp)
	var lp events.ShadowLabelledPayload
	_ = json.Unmarshal(label.Payload, &lp)
	if label.Type != events.ShadowLabelled || lp.Identity == nil || lp.Identity.Grant != "user:kabir" || lp.Identity.Method != "dev" {
		t.Fatalf("label event = %+v %+v", label, lp.Identity)
	}
	wantErr(t, doJSON(t, http.MethodPost, path+"/shadow/blind", "priya", `{"actual_layer":"kafka","root_cause_terms":["x"]}`), http.StatusConflict, "already_decided")

	wantErr(t, doJSON(t, http.MethodGet, path+"/shadow?include=bogus", "kabir", ""), http.StatusBadRequest, "validation")
	resp = doJSON(t, http.MethodGet, path+"/shadow", "kabir", "")
	wantStatus(t, resp, http.StatusOK)
	var view struct {
		Run    map[string]any   `json:"run"`
		Events []map[string]any `json:"events"`
		Room   *json.RawMessage `json:"room"`
	}
	raw := decode[json.RawMessage](t, resp)
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatal(err)
	}
	if view.Run["status"] != "hypothesised" || view.Run["final_hypothesis"] == nil || len(view.Events) == 0 || view.Room != nil {
		t.Fatalf("view = %s", raw)
	}
	for _, k := range []string{"room_id", "status", "model", "pipeline_map", "started_at", "first_hypothesis_at", "final_hypothesis_at",
		"finished_at", "finished_after_close", "final_hypothesis", "tool_calls", "tokens", "revealed_at"} {
		if _, ok := view.Run[k]; !ok {
			t.Fatalf("run has no %q: %v", k, view.Run)
		}
	}
	if _, ok := view.Run["last_seq"]; ok {
		t.Fatal("run exposes last_seq")
	}
	for _, e := range view.Events {
		if e["shadow"] != true || e["seq"] == nil {
			t.Fatalf("shadow event = %v", e)
		}
	}
	if strings.Contains(string(raw), httpSecretData) {
		t.Fatal("tool_result.data without include=data")
	}
	resp = doJSON(t, http.MethodGet, path+"/shadow?include=data,room", "kabir", "")
	wantStatus(t, resp, http.StatusOK)
	raw = decode[json.RawMessage](t, resp)
	if !strings.Contains(string(raw), httpSecretData) || !strings.Contains(string(raw), `"room":{"created":{`) {
		t.Fatalf("include=data,room: %s", raw)
	}

	wantErr(t, doJSON(t, http.MethodPost, path+"/shadow/grade", "kabir", `{"root_cause":"yes"}`), http.StatusBadRequest, "validation")
	wantErr(t, doJSON(t, http.MethodPost, path+"/shadow/grade", "kabir", `{"layer_correct":true,"root_cause":"maybe"}`), http.StatusBadRequest, "validation")
	wantErr(t, doJSON(t, http.MethodPost, path+"/shadow/grade", "nolayers", `{"layer_correct":true,"root_cause":"yes"}`), http.StatusForbidden, "not_allowed")
	resp = doJSON(t, http.MethodPost, path+"/shadow/grade", "kabir", `{"layer_correct":true,"root_cause":"partial","notes":"close"}`)
	wantStatus(t, resp, http.StatusOK)
	if g := decode[events.Event](t, resp); g.Type != events.ShadowGraded {
		t.Fatalf("grade event = %+v", g)
	}

	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/shadow/metrics?since=yesterday", "kabir", ""), http.StatusBadRequest, "validation")
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/shadow/metrics?pipeline=orders&since=2000-01-01T00:00:00Z", "kabir", "")
	wantStatus(t, resp, http.StatusOK)
	m := decode[map[string]any](t, resp)
	if m["rooms"] != float64(1) || m["graded"] != float64(1) || m["layer_correct"] != float64(1) || m["tokens_available"] != false {
		t.Fatalf("metrics = %v", m)
	}
	if rows, ok := m["per_room"].([]any); !ok || len(rows) != 1 {
		t.Fatalf("per_room = %v", m["per_room"])
	}

	// Unknown include= values on the export are ignored, as before shadow mode.
	resp = doJSON(t, http.MethodGet, path+"/export?include=nope", "priya", "")
	wantStatus(t, resp, http.StatusOK)
	if b := decode[map[string]json.RawMessage](t, resp); b["shadow"] != nil {
		t.Fatal("include=nope added the shadow member")
	}
	resp = doJSON(t, http.MethodGet, path+"/export?include=nope,shadow", "priya", "")
	wantStatus(t, resp, http.StatusOK)
	if b := decode[map[string]json.RawMessage](t, resp); b["shadow"] == nil {
		t.Fatal("include=nope,shadow lost the shadow member")
	}
	resp = doJSON(t, http.MethodGet, path+"/export?include=shadow", "priya", "")
	wantStatus(t, resp, http.StatusOK)
	withShadow := resp.Header.Get("X-Piperoom-Export-SHA256")
	body := decode[map[string]json.RawMessage](t, resp)
	if _, ok := body["shadow"]; !ok || withShadow == "" {
		t.Fatal("export with include=shadow has no shadow member")
	}
	resp = doJSON(t, http.MethodGet, path+"/export", "priya", "")
	wantStatus(t, resp, http.StatusOK)
	plain := decode[map[string]json.RawMessage](t, resp)
	if _, ok := plain["shadow"]; ok || resp.Header.Get("X-Piperoom-Export-SHA256") == withShadow {
		t.Fatal("plain export must not carry the shadow member")
	}

	// GET /events and the WebSocket never carried an agent event.
	resp = doJSON(t, http.MethodGet, path+"/events?after=0", "kabir", "")
	wantStatus(t, resp, http.StatusOK)
	page := decode[struct {
		Events []events.Event `json:"events"`
	}](t, resp)
	noAgentEvents(t, "GET /events", page.Events)
	var sawReveal bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		evs := ws.all()
		if len(evs) > 0 && evs[len(evs)-1].Type == events.ShadowGraded {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	wsEvs := ws.all()
	noAgentEvents(t, "WebSocket", wsEvs)
	for _, e := range wsEvs {
		sawReveal = sawReveal || e.Type == events.ShadowRevealed
	}
	if !sawReveal || len(wsEvs) != len(page.Events) {
		t.Fatalf("ws saw %d events (reveal %v), GET /events %d", len(wsEvs), sawReveal, len(page.Events))
	}
	snapNow := decode[map[string]any](t, doJSON(t, http.MethodGet, path, "kabir", ""))
	if as, _ := snapNow["agent_status"].(map[string]any); as["state"] != "idle" || snapNow["hypothesis"] != nil {
		t.Fatalf("snapshot shows agent state: %v", snapNow)
	}
}

// Shadow routes on a live room are 404.
func TestShadowRoutesLiveRoom404(t *testing.T) {
	srv, _ := newServer(t)
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	snap := decode[map[string]any](t, resp)
	if snap["shadow"] != false {
		t.Fatalf("POST /api/rooms shadow = %v", snap["shadow"])
	}
	path := srv.URL + "/api/rooms/" + snap["id"].(string)
	wantStatus(t, doJSON(t, http.MethodPost, path+"/close", "arjun", `{"resolution":""}`), http.StatusOK)
	wantStatus(t, doJSON(t, http.MethodGet, path+"/export?include=nope", "arjun", ""), http.StatusOK)
	wantErr(t, doJSON(t, http.MethodGet, path+"/shadow", "arjun", ""), http.StatusNotFound, "not_found")
	wantErr(t, doJSON(t, http.MethodPost, path+"/shadow/blind", "arjun", `{"actual_layer":"kafka","root_cause_terms":["x"]}`), http.StatusNotFound, "not_found")
	wantErr(t, doJSON(t, http.MethodPost, path+"/shadow/grade", "arjun", `{"layer_correct":true,"root_cause":"yes"}`), http.StatusNotFound, "not_found")
}

// Viewers and auditor-only principals get 403 viewer on the label and grade
// routes; GET routes stay open to them.
func TestShadowViewerCannotLabelOrGrade(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	snap := e.newRoom(t, "priya")
	for name, u := range map[string]struct{ c *http.Cookie }{
		"viewer":       {e.mustLogin(t, idpViewer)},
		"auditor-only": {e.mustLogin(t, idpAuditor)},
	} {
		h := ck(u.c)
		for _, p := range []string{"/shadow/blind", "/shadow/grade"} {
			resp := e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+p, `{"actual_layer":"kafka","root_cause_terms":["x"],"layer_correct":true,"root_cause":"yes"}`, h)
			wantStatus(t, resp, http.StatusForbidden)
			if c, _ := errCode(t, resp); c != "viewer" {
				t.Errorf("%s POST %s: code %q, want viewer", name, p, c)
			}
		}
		wantStatus(t, e.do(t, http.MethodGet, "/api/shadow/metrics", "", h), http.StatusOK)
	}
}
