package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// Two tenants (docs/design/tenancy.md): pa in tenant a (alice owns every
// layer, ann is a listed member), pb and the shadow pipeline pbs in tenant b
// (bob owns every layer, ben is a listed member). mallory is in no tenant.
func tenancyOwnersYAML(withTenants bool) string {
	own := func(user string) string {
		var b strings.Builder
		b.WriteString("\n    owners:")
		for _, l := range events.Layers {
			b.WriteString("\n      " + l + ": {approvers: [user:" + user + "]}")
		}
		return b.String()
	}
	conns := func(p string) string {
		return "\n    connections: {source_db_url: {env: " + p + "_SRC}, warehouse_db_url: {env: " + p + "_WH}, kafka_brokers: {env: " + p +
			"_K}, connect_url: {env: " + p + "_C}, schema_registry_url: {env: " + p + "_SR}}"
	}
	tenant := func(t string) string {
		if !withTenants {
			return ""
		}
		return "\n    tenant: " + t
	}
	y := "pipelines:" +
		"\n  - id: pa" + tenant("a") + "\n    topic_prefix: pa\n    source_tables: [public.a]\n    connectors: [pa-c]" + conns("A") + own("alice") +
		"\n  - id: pb" + tenant("b") + "\n    topic_prefix: pb\n    source_tables: [public.b]\n    connectors: [pb-c]" + conns("B") + own("bob") +
		"\n  - id: pbs" + tenant("b") + "\n    topic_prefix: pbs\n    source_tables: [public.s]\n    connectors: [pbs-c]\n    shadow: {enabled: true}" + conns("B") + own("bob") + "\n"
	if withTenants {
		y = "tenants:\n  a: {members: [user:ann, user:adam]}\n  b: {members: [user:ben]}\n" + y
	}
	return y
}

const tenancyUsers = `users:
  - {id: alice, name: Alice}
  - {id: bob, name: Bob}
  - {id: ann, name: Ann}
  - {id: ben, name: Ben}
  - {id: mallory, name: Mallory}
  - {id: adam, name: Adam, admin: true}
`

type tenancyEnv struct {
	srv *httptest.Server
	api *httpapi.Server
	svc *room.Service
}

func newTenancyEnv(t *testing.T, withTenants bool) *tenancyEnv {
	t.Helper()
	users, err := room.ParseUsers([]byte(tenancyUsers))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(tenancyOwnersYAML(withTenants)), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	reg := actions.NewRegistry()
	if err := reg.Register(&testAction{layer: "kafka-connect"}); err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st, Users: users, Owners: own, Actions: reg,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{WhatBroke: "x"}, nil
		},
	})
	now := time.Now()
	api := httpapi.New(svc, st, nil, httpapi.WithAlertIngest(alertin.Config{GenericSecret: genericSecret, Now: func() time.Time { return now }}))
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})
	return &tenancyEnv{srv: srv, api: api, svc: svc}
}

func (e *tenancyEnv) do(t *testing.T, user, method, path, body string) (int, string) {
	t.Helper()
	resp := doJSON(t, method, e.srv.URL+path, user, body)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (e *tenancyEnv) mustDo(t *testing.T, user, method, path, body string, want int) string {
	t.Helper()
	got, b := e.do(t, user, method, path, body)
	if got != want {
		t.Fatalf("%s %s %s as %s: %d %s, want %d", method, path, body, user, got, b, want)
	}
	return b
}

// fixture is one room of each kind in tenant b, plus a room in tenant a.
type fixture struct {
	ra, rb, rbs string // rooms: pa, pb, the closed shadow room of pbs
	proposal    string // an executed proposal of rb
	open        string // an open proposal of rb
	handoff     string // a pending hand-off of rb, to ben
	voided      string // an older hand-off of rb, voided by handoff
}

func (e *tenancyEnv) build(t *testing.T) fixture {
	t.Helper()
	var f fixture
	f.ra = decodeID(t, e.mustDo(t, "alice", http.MethodPost, "/api/rooms", `{"title":"a","severity":"sev2","pipeline_id":"pa"}`, http.StatusCreated))
	f.rb = decodeID(t, e.mustDo(t, "bob", http.MethodPost, "/api/rooms", `{"title":"b","severity":"sev2","pipeline_id":"pb"}`, http.StatusCreated))

	newProposal := func() string {
		pid, err := e.svc.CreateProposal(context.Background(), f.rb, events.ProposalCreatedPayload{
			Title: "fix", Risk: "low risk", Rollback: "undo it",
			Steps: []events.ProposalStep{{Action: "fix_kafka-connect", Layer: "kafka-connect", Params: json.RawMessage(`{}`), Description: "d"}},
		}, events.AgentActor)
		if err != nil {
			t.Fatal(err)
		}
		return pid
	}
	f.proposal = newProposal()
	e.mustDo(t, "bob", http.MethodPost, "/api/proposals/"+f.proposal+"/approve",
		`{"layer":"kafka-connect","params_hash":"`+hashOfOpen(t, e.svc, f.proposal)+`"}`, http.StatusOK)
	f.open = newProposal()

	// Two hand-offs to ben: the second voids the first.
	e.mustDo(t, "bob", http.MethodPost, "/api/rooms/"+f.rb+"/handoffs", `{"to":"ben"}`, http.StatusOK)
	e.mustDo(t, "bob", http.MethodPost, "/api/rooms/"+f.rb+"/handoffs", `{"to":"ben"}`, http.StatusOK)
	evs, _, err := e.svc.Events(context.Background(), f.rb, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var hids []string
	for _, ev := range evs {
		if ev.Type == events.HandoffCreated {
			var p events.HandoffCreatedPayload
			_ = json.Unmarshal(ev.Payload, &p)
			hids = append(hids, p.HandoffID)
		}
	}
	if len(hids) != 2 {
		t.Fatalf("hand-offs = %v", hids)
	}
	f.voided, f.handoff = hids[0], hids[1]

	// A closed shadow room in tenant b (dev webhook as bob).
	f.rbs = decodeID(t, e.mustDo(t, "bob", http.MethodPost, "/api/alerts/webhook", `{"title":"s","pipeline_id":"pbs"}`, http.StatusCreated))
	e.mustDo(t, "bob", http.MethodPost, "/api/rooms/"+f.rbs+"/control/grant", `{"to":"bob"}`, http.StatusOK)
	e.mustDo(t, "bob", http.MethodPost, "/api/rooms/"+f.rbs+"/close", `{"resolution":"fixed the slot"}`, http.StatusOK)
	return f
}

func decodeID(t *testing.T, body string) string {
	t.Helper()
	var v struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &v); err != nil || v.ID == "" {
		t.Fatalf("no id in %s", body)
	}
	return v.ID
}

// hiddenRoutes are every route that names a room, proposal, hand-off or
// pipeline, filled with tenant b's objects. TestTenantRoutesClassified fails
// when a new route with an id is added without a line here.
func hiddenRoutes(f fixture) []struct{ method, pattern, path, body string } {
	return []struct{ method, pattern, path, body string }{
		{"GET", "/api/rooms/{id}", "/api/rooms/" + f.rb, ""},
		{"GET", "/api/rooms/{id}/events", "/api/rooms/" + f.rb + "/events", ""},
		{"GET", "/api/rooms/{id}/export", "/api/rooms/" + f.rb + "/export", ""},
		{"GET", "/api/rooms/{id}/postmortem.md", "/api/rooms/" + f.rb + "/postmortem.md", ""},
		{"GET", "/api/rooms/{id}/ws", "/api/rooms/" + f.rb + "/ws", ""},
		{"POST", "/api/rooms/{id}/comments", "/api/rooms/" + f.rb + "/comments", `{"text":"hi"}`},
		{"POST", "/api/rooms/{id}/steer", "/api/rooms/" + f.rb + "/steer", `{"text":"go"}`},
		{"POST", "/api/rooms/{id}/suggestions/{sid}/send", "/api/rooms/" + f.rb + "/suggestions/g_x/send", ""},
		{"POST", "/api/rooms/{id}/suggestions/{sid}/decline", "/api/rooms/" + f.rb + "/suggestions/g_x/decline", ""},
		{"POST", "/api/rooms/{id}/hypothesis", "/api/rooms/" + f.rb + "/hypothesis", `{"verdict":"confirmed"}`},
		{"POST", "/api/rooms/{id}/nudge", "/api/rooms/" + f.rb + "/nudge", `{"proposal_id":"p_x","layer":"kafka"}`},
		{"POST", "/api/rooms/{id}/page", "/api/rooms/" + f.rb + "/page", `{"layer":"kafka","reason":"x"}`},
		{"POST", "/api/rooms/{id}/control/request", "/api/rooms/" + f.rb + "/control/request", ""},
		{"POST", "/api/rooms/{id}/control/grant", "/api/rooms/" + f.rb + "/control/grant", `{"to":"alice"}`},
		{"POST", "/api/rooms/{id}/close", "/api/rooms/" + f.rb + "/close", `{"resolution":"x"}`},
		{"POST", "/api/rooms/{id}/extend", "/api/rooms/" + f.rb + "/extend", ""},
		{"POST", "/api/rooms/{id}/handoffs", "/api/rooms/" + f.rb + "/handoffs", `{"to":"alice"}`},
		{"POST", "/api/rooms/{id}/label", "/api/rooms/" + f.rb + "/label", `{"cause":"x"}`},
		{"POST", "/api/rooms/{id}/checklist", "/api/rooms/" + f.rb + "/checklist", `{"step_id":"s","tool_use_id":"t","hit":0,"item":0,"done":true}`},
		{"POST", "/api/rooms/{id}/trace", "/api/rooms/" + f.rb + "/trace", `{"table":"public.b","key":"1"}`},
		{"GET", "/api/rooms/{id}/shadow", "/api/rooms/" + f.rbs + "/shadow", ""},
		{"POST", "/api/rooms/{id}/shadow/blind", "/api/rooms/" + f.rbs + "/shadow/blind", `{"actual_layer":"postgres","root_cause_terms":["slot"]}`},
		{"POST", "/api/rooms/{id}/shadow/grade", "/api/rooms/" + f.rbs + "/shadow/grade", `{"layer_correct":true,"root_cause":"yes"}`},
		{"POST", "/api/proposals/{pid}/approve", "/api/proposals/" + f.open + "/approve", `{"layer":"kafka-connect","params_hash":"x"}`},
		{"POST", "/api/proposals/{pid}/reject", "/api/proposals/" + f.open + "/reject", `{"layer":"kafka-connect","reason":"no","params_hash":"x"}`},
		{"POST", "/api/handoffs/{hid}/accept", "/api/handoffs/" + f.handoff + "/accept", ""},
		{"GET", "/api/pipelines/{pid}/topology", "/api/pipelines/pb/topology", ""},
		{"GET", "/api/pipelines/{pid}/agent-stats", "/api/pipelines/pb/agent-stats", ""},
		{"GET", "/api/rooms/{id}/usage", "/api/rooms/" + f.rb + "/usage", ""},
		{"GET", "/api/usage", "/api/usage?pipeline=pb", ""},
	}
}

// aggregateRoutes return or act on several pipelines' objects; each filters by
// tenant and has its own test here (TestRoomListAndMetricsAreFiltered,
// TestCreateAndAlertRoutingRespectTenants, TestListGatewayTenancyFilter).
var aggregateRoutes = []string{
	"GET /api/rooms",
	"GET /api/rooms/ws",
	"GET /api/shadow/metrics",
	"POST /api/rooms",
	"GET /api/pipelines",
	"POST /api/alerts/webhook",
	"POST /api/alerts/{source}",
}

// adminRoutes answer 403 to everyone but an admin whatever the id (the role
// is checked first, so no id leaks), then 404 to an admin outside the room's
// tenant (TestDeleteHiddenFromAdminOfOtherTenant).
var adminRoutes = []string{
	"DELETE /api/rooms/{id}",
}

// publicRoutes hold no tenant's data: probes, login, the caller's own
// identity, and the people list (global by design: docs/design/tenancy.md
// section 8).
var publicRoutes = []string{
	"GET /healthz",
	"GET /readyz",
	"POST /auth/logout",
	"POST /auth/dev-login",
	"GET /auth/users",
	"GET /rooms/{id}",
	"HEAD /rooms/{id}",
	"GET /api/me",
	"GET /api/people",
}

// TestTenantRoutesClassified walks the router: every route must be a room-id
// route (hiddenRoutes), an aggregate that filters by tenant, or public. A new
// route of any kind fails here until someone decides which it is.
func TestTenantRoutesClassified(t *testing.T) {
	e := newTenancyEnv(t, true)
	known := map[string]bool{}
	for _, r := range hiddenRoutes(fixture{}) {
		known[r.method+" "+r.pattern] = true
	}
	for _, r := range slices.Concat(aggregateRoutes, adminRoutes, publicRoutes) {
		known[r] = true
	}
	table := httpapi.RouteTable(e.api)
	if !slices.Contains(table, "GET /api/rooms/{id}/ws") || !slices.Contains(table, "POST /api/handoffs/{hid}/accept") || !slices.Contains(table, "GET /healthz") {
		t.Fatalf("route table looks wrong: %v", table)
	}
	var missing []string
	for _, r := range table {
		if !known[r] {
			missing = append(missing, r)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("routes not classified as room-id, aggregate or public: %v", missing)
	}
}

// TestOtherTenantGets404Everywhere: a user of tenant a, and a user in no
// tenant, get exactly the missing-object 404 on every route of tenant b's
// objects, and the WebSocket upgrade is refused. Tenant b's members still
// reach them.
func TestOtherTenantGets404Everywhere(t *testing.T) {
	e := newTenancyEnv(t, true)
	f := e.build(t)

	_, missingRoom := e.do(t, "alice", "GET", "/api/rooms/r_doesnotexist", "")
	for _, user := range []string{"alice", "ann", "mallory"} {
		for _, r := range hiddenRoutes(f) {
			status, body := e.do(t, user, r.method, r.path, r.body)
			if status != http.StatusNotFound || !strings.Contains(body, `"code":"not_found"`) {
				t.Errorf("%s %s as %s: %d %s, want 404 not_found", r.method, r.pattern, user, status, body)
			}
			if strings.HasPrefix(r.pattern, "/api/rooms/") && body != missingRoom {
				t.Errorf("%s %s as %s: body %s differs from a missing room's %s", r.method, r.pattern, user, body, missingRoom)
			}
		}
	}

	// The proposal that is already decided is still 404, not 409, for
	// approve and reject, with the body of a proposal that does not exist.
	for _, verb := range []string{"approve", "reject"} {
		body := `{"layer":"kafka-connect","reason":"no","params_hash":"x"}`
		_, missing := e.do(t, "alice", http.MethodPost, "/api/proposals/p_nope/"+verb, body)
		for _, pid := range []string{f.proposal, f.open} {
			status, got := e.do(t, "alice", http.MethodPost, "/api/proposals/"+pid+"/"+verb, body)
			if status != http.StatusNotFound || got != missing {
				t.Fatalf("%s %s as alice: %d %s, want the missing proposal's 404 %s", verb, pid, status, got, missing)
			}
		}
	}
	// A voided hand-off is 404 too, not 409 stale_handoff.
	_, missingHandoff := e.do(t, "alice", http.MethodPost, "/api/handoffs/h_nope/accept", "")
	if status, got := e.do(t, "alice", http.MethodPost, "/api/handoffs/"+f.voided+"/accept", ""); status != http.StatusNotFound || got != missingHandoff {
		t.Fatalf("voided hand-off as alice: %d %s, want %s", status, got, missingHandoff)
	}
	if status, _ := e.do(t, "ben", http.MethodPost, "/api/handoffs/"+f.voided+"/accept", ""); status != http.StatusConflict {
		t.Fatalf("voided hand-off as ben: %d, want 409 for a member", status)
	}
	// The topology of a hidden pipeline answers exactly as an unknown one.
	_, missingPipeline := e.do(t, "alice", "GET", "/api/pipelines/nope/topology", "")
	if status, got := e.do(t, "alice", "GET", "/api/pipelines/pb/topology", ""); status != http.StatusNotFound || got != missingPipeline {
		t.Fatalf("topology of pb as alice: %d %s, want the unknown pipeline's %s", status, got, missingPipeline)
	}

	// WebSocket: refused before the upgrade for alice, accepted for ben.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(e.srv.URL, "http") + "/api/rooms/" + f.rb + "/ws?after=0"
	_, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {"alice"}}})
	if err == nil || resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("ws as alice: err=%v resp=%v, want a refused 404 handshake", err, resp)
	}
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: http.Header{"X-User": {"ben"}}})
	if err != nil {
		t.Fatalf("ws as ben (tenant b member): %v", err)
	}
	_ = conn.Close(websocket.StatusNormalClosure, "")

	// Members of tenant b see the room and its objects.
	for _, user := range []string{"bob", "ben"} {
		e.mustDo(t, user, "GET", "/api/rooms/"+f.rb, "", http.StatusOK)
		e.mustDo(t, user, "GET", "/api/rooms/"+f.rb+"/events", "", http.StatusOK)
	}
	e.mustDo(t, "bob", "GET", "/api/pipelines/pb/topology", "", http.StatusOK)
	if status, _ := e.do(t, "bob", "GET", "/api/rooms/"+f.rbs+"/shadow", ""); status == http.StatusNotFound {
		t.Fatal("bob must reach the shadow room (409 not_labelled is fine, 404 is not)")
	}
	e.mustDo(t, "ben", http.MethodPost, "/api/handoffs/"+f.handoff+"/accept", "", http.StatusOK)
	// An accepted hand-off is 404 for alice, not 409 already_decided.
	if status, got := e.do(t, "alice", http.MethodPost, "/api/handoffs/"+f.handoff+"/accept", ""); status != http.StatusNotFound || got != missingHandoff {
		t.Fatalf("accepted hand-off as alice: %d %s, want %s", status, got, missingHandoff)
	}
	// alice's own room is fine.
	e.mustDo(t, "alice", "GET", "/api/rooms/"+f.ra, "", http.StatusOK)
}

// TestDeleteHiddenFromAdminOfOtherTenant: an admin of tenant a gets the
// missing-room 404 for a delete of tenant b's room, and the room stays; a
// non-admin gets 403 whatever the id.
func TestDeleteHiddenFromAdminOfOtherTenant(t *testing.T) {
	e := newTenancyEnv(t, true)
	f := e.build(t)
	status, missing := e.do(t, "adam", http.MethodDelete, "/api/rooms/r_doesnotexist?reason=cleanup", "")
	if status != http.StatusNotFound || !strings.Contains(missing, `"code":"not_found"`) {
		t.Fatalf("delete of a missing room as adam: %d %s", status, missing)
	}
	if status, got := e.do(t, "adam", http.MethodDelete, "/api/rooms/"+f.rb+"?reason=cleanup", ""); status != http.StatusNotFound || got != missing {
		t.Fatalf("delete of tenant b's room as adam: %d %s, want the missing room's 404 %s", status, got, missing)
	}
	e.mustDo(t, "bob", "GET", "/api/rooms/"+f.rb, "", http.StatusOK)
	for _, id := range []string{f.rb, "r_doesnotexist"} {
		if status, _ := e.do(t, "alice", http.MethodDelete, "/api/rooms/"+id+"?reason=cleanup", ""); status != http.StatusForbidden {
			t.Fatalf("delete %s as non-admin alice: %d, want 403", id, status)
		}
	}
	// adam's own tenant's room is deleted.
	e.mustDo(t, "adam", http.MethodDelete, "/api/rooms/"+f.ra+"?reason=cleanup", "", http.StatusOK)
}

func roomIDs(t *testing.T, body string) []string {
	t.Helper()
	var rooms []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &rooms); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, r := range rooms {
		ids = append(ids, r.ID)
	}
	slices.Sort(ids)
	return ids
}

func TestRoomListAndMetricsAreFiltered(t *testing.T) {
	e := newTenancyEnv(t, true)
	f := e.build(t)
	want := map[string][]string{
		"alice":   {f.ra},
		"ann":     {f.ra},
		"bob":     {f.rb, f.rbs},
		"ben":     {f.rb, f.rbs},
		"mallory": nil,
	}
	for user, w := range want {
		slices.Sort(w)
		got := roomIDs(t, e.mustDo(t, user, "GET", "/api/rooms", "", http.StatusOK))
		if !slices.Equal(got, w) {
			t.Errorf("%s lists %v, want %v", user, got, w)
		}
	}
	metrics := func(user, q string) int {
		var m struct {
			Rooms int `json:"rooms"`
		}
		_ = json.Unmarshal([]byte(e.mustDo(t, user, "GET", "/api/shadow/metrics"+q, "", http.StatusOK)), &m)
		return m.Rooms
	}
	if metrics("bob", "") != 1 || metrics("bob", "?pipeline=pbs") != 1 {
		t.Fatal("bob must count the closed shadow room")
	}
	if n := metrics("alice", ""); n != 0 {
		t.Fatalf("alice counts %d shadow rooms of tenant b", n)
	}
	if n := metrics("alice", "?pipeline=pbs"); n != 0 {
		t.Fatalf("alice counts %d with ?pipeline=pbs", n)
	}
}

func TestCreateAndAlertRoutingRespectTenants(t *testing.T) {
	e := newTenancyEnv(t, true)
	// POST /api/rooms: tenant b's pipeline reads as unknown, word for word.
	_, unknown := e.do(t, "alice", http.MethodPost, "/api/rooms", `{"title":"x","severity":"sev2","pipeline_id":"zz"}`)
	status, hidden := e.do(t, "alice", http.MethodPost, "/api/rooms", `{"title":"x","severity":"sev2","pipeline_id":"pb"}`)
	if status != http.StatusBadRequest || strings.ReplaceAll(hidden, `\"pb\"`, `\"zz\"`) != unknown {
		t.Fatalf("create in tenant b as alice: %d %s (unknown pipeline: %s)", status, hidden, unknown)
	}
	// The dev webhook: another tenant's or an unknown pipeline is 404 and
	// opens nothing; the member opens the room.
	for _, body := range []string{`{"title":"x","pipeline_id":"pb","dedup_key":"k"}`, `{"title":"x","pipeline_id":"nope"}`, `{"title":"x"}`} {
		if status, b := e.do(t, "alice", http.MethodPost, "/api/alerts/webhook", body); status != http.StatusNotFound {
			t.Fatalf("webhook %s as alice: %d %s", body, status, b)
		}
	}
	rooms, err := e.svc.ListRooms(context.Background())
	if err != nil || len(rooms) != 0 {
		t.Fatalf("a refused webhook opened %d rooms (%v)", len(rooms), err)
	}
	rb := decodeID(t, e.mustDo(t, "bob", http.MethodPost, "/api/alerts/webhook", `{"title":"x","pipeline_id":"pb","dedup_key":"k"}`, http.StatusCreated))
	// A repeat by alice would join rb and return its snapshot: refused.
	if status, b := e.do(t, "alice", http.MethodPost, "/api/alerts/webhook", `{"title":"x","pipeline_id":"pb","dedup_key":"k"}`); status != http.StatusNotFound || strings.Contains(b, rb) {
		t.Fatalf("repeat as alice: %d %s", status, b)
	}

	// Signed alerts: the shared secret gets the room id, not its content.
	now := time.Now()
	body := []byte(`{"title":"lag on pb","severity":"sev2","pipeline_id":"pb","dedup_key":"k"}`)
	resp := postAlert(t, e.srv, alertin.Generic, hdrs(alertin.GenericSignatureHeader, alertin.SignGeneric(genericSecret, now, body)), body)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var reply map[string]any
	if err := json.Unmarshal(b, &reply); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated || len(reply) != 2 || reply["room_id"] == "" || reply["created"] != true {
		t.Fatalf("signed alert: %d %s, want 201 {room_id, created}", resp.StatusCode, b)
	}
	if strings.Contains(string(b), "lag on pb") || strings.Contains(string(b), "title") {
		t.Fatalf("signed reply carries room content: %s", b)
	}
}

func TestGrantAndHandoffTargetsMustSeeTheRoom(t *testing.T) {
	e := newTenancyEnv(t, true)
	rb := decodeID(t, e.mustDo(t, "bob", http.MethodPost, "/api/rooms", `{"title":"b","severity":"sev2","pipeline_id":"pb"}`, http.StatusCreated))
	_, unknownPerson := e.do(t, "bob", http.MethodPost, "/api/rooms/"+rb+"/control/grant", `{"to":"nobody-at-all"}`)
	for _, path := range []string{"/control/grant", "/handoffs"} {
		status, b := e.do(t, "bob", http.MethodPost, "/api/rooms/"+rb+path, `{"to":"alice"}`)
		if status != http.StatusBadRequest || b != unknownPerson {
			t.Fatalf("%s to alice: %d %s, want the unknown-person answer %s", path, status, b, unknownPerson)
		}
	}
	e.mustDo(t, "bob", http.MethodPost, "/api/rooms/"+rb+"/control/grant", `{"to":"ben"}`, http.StatusOK)
}

// TestNoTenantsEveryoneSeesEverything: the same file without tenants: keeps
// today's behaviour.
func TestNoTenantsEveryoneSeesEverything(t *testing.T) {
	e := newTenancyEnv(t, false)
	f := e.build(t)
	got := roomIDs(t, e.mustDo(t, "mallory", "GET", "/api/rooms", "", http.StatusOK))
	if len(got) != 3 {
		t.Fatalf("mallory lists %v, want all three rooms", got)
	}
	e.mustDo(t, "alice", "GET", "/api/rooms/"+f.rb, "", http.StatusOK)
	e.mustDo(t, "mallory", "GET", "/api/rooms/"+f.rb+"/events", "", http.StatusOK)
	e.mustDo(t, "alice", "GET", "/api/pipelines/pb/topology", "", http.StatusOK)
	e.mustDo(t, "mallory", http.MethodPost, "/api/alerts/webhook", `{"title":"x","pipeline_id":"nope"}`, http.StatusCreated)

	now := time.Now()
	body := []byte(`{"title":"lag","severity":"sev2","pipeline_id":"pb","dedup_key":"z"}`)
	resp := postAlert(t, e.srv, alertin.Generic, hdrs(alertin.GenericSignatureHeader, alertin.SignGeneric(genericSecret, now, body)), body)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated || !strings.Contains(string(b), `"title":"lag"`) {
		t.Fatalf("signed alert without tenants: %d %s, want the RoomSnapshot", resp.StatusCode, b)
	}
}
