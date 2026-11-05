package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

const usageOwners = `pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
  - id: shadowed
    topic_prefix: sh
    source_tables: [public.items]
    connectors: [c2]
    shadow: {enabled: true}
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
auditors: [user:audrey]
`

const usageUsers = `users:
  - {id: arjun, name: Arjun, layers: [kafka-connect, kafka]}
  - {id: priya, name: Priya, layers: [postgres, schema-registry]}
  - {id: meera, name: Meera, layers: [warehouse]}
  - {id: kabir, name: Kabir, layers: [consumer]}
  - {id: nolayers, name: No Layers}
  - {id: audrey, name: Audrey}
`

// newUsageServer serves the usage routes over a memstore, on a fixed clock.
func newUsageServer(t *testing.T, withSpend bool) (*httptest.Server, *memstore.Store) {
	t.Helper()
	return newUsageServerWith(t, withSpend, usageOwners)
}

func newUsageServerWith(t *testing.T, withSpend bool, ownersYAML string) (*httptest.Server, *memstore.Store) {
	t.Helper()
	users, err := room.ParseUsers([]byte(usageUsers))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(ownersYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st, Users: users, Owners: own, Run: fakeRun,
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{WhatBroke: "x"}, nil
		},
	})
	var opts []httpapi.Option
	if withSpend {
		opts = append(opts, httpapi.WithSpend(httpapi.Spend{
			Store: st, RoomTokens: 2_000_000, PipelineDailyTokens: 5_000_000, PipelineDailyCostUSD: 50,
			Now: func() time.Time { return time.Date(2026, 10, 13, 23, 0, 0, 0, time.UTC) },
		}))
	}
	api := httpapi.New(svc, st, nil, opts...)
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})
	return srv, st
}

func addSpend(t *testing.T, st *memstore.Store, c store.SpendChange) {
	t.Helper()
	if _, err := st.AddSpend(context.Background(), c); err != nil {
		t.Fatal(err)
	}
}

func TestUsageRoutesNeedTheSpendStore(t *testing.T) {
	srv, _ := newUsageServer(t, false)
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=orders", "arjun", ""), http.StatusNotFound, "not_found")
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/rooms/r1/usage", "arjun", ""), http.StatusNotFound, "not_found")
}

func TestRoomUsage(t *testing.T) {
	srv, st := newUsageServer(t, true)
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	wantStatus(t, resp, http.StatusCreated)
	roomID := decode[room.RoomSnapshot](t, resp).ID
	url := srv.URL + "/api/rooms/" + roomID + "/usage"

	// No spend yet: zeros, no cost, an empty breakdown.
	resp = doJSON(t, http.MethodGet, url, "nolayers", "")
	wantStatus(t, resp, http.StatusOK)
	empty := decode[map[string]any](t, resp)
	if empty["charged"].(map[string]any)["cost_usd"] != nil || len(empty["by_model"].([]any)) != 0 || empty["room_id"] != roomID {
		t.Fatalf("empty usage = %v", empty)
	}

	addSpend(t, st, store.SpendChange{RoomID: roomID, PipelineID: "orders", Day: "2026-10-13", Tokens: 300}) // in flight
	addSpend(t, st, store.SpendChange{RoomID: roomID, PipelineID: "orders", Day: "2026-10-13", Tokens: 110, CostMicros: 1100,
		Call: &store.SpendCall{Model: "opus", InputTokens: 80, OutputTokens: 30, CostMicros: 1100, Priced: true}})
	addSpend(t, st, store.SpendChange{RoomID: roomID, PipelineID: "orders", Day: "2026-10-13", Tokens: 5,
		Call: &store.SpendCall{Model: "haiku", InputTokens: 5}})

	// Any logged-in user who may read the room, layers or not.
	resp = doJSON(t, http.MethodGet, url, "nolayers", "")
	wantStatus(t, resp, http.StatusOK)
	var got struct {
		RoomID     string `json:"room_id"`
		PipelineID string `json:"pipeline_id"`
		Charged    struct {
			Tokens  int64    `json:"tokens"`
			CostUSD *float64 `json:"cost_usd"`
		} `json:"charged"`
		Limit struct {
			Tokens int64 `json:"tokens"`
		} `json:"limit"`
		Totals  map[string]any   `json:"totals"`
		ByModel []map[string]any `json:"by_model"`
	}
	raw := decode[json.RawMessage](t, resp)
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.PipelineID != "orders" || got.Charged.Tokens != 415 || got.Charged.CostUSD == nil || *got.Charged.CostUSD != 0.0011 || got.Limit.Tokens != 2_000_000 {
		t.Fatalf("usage = %s", raw)
	}
	if got.Totals["calls"] != 2.0 || got.Totals["unpriced_calls"] != 1.0 || got.Totals["tokens"] != 115.0 || got.Totals["cost_usd"] != 0.0011 {
		t.Fatalf("totals = %v", got.Totals)
	}
	if len(got.ByModel) != 2 || got.ByModel[0]["model"] != "haiku" || got.ByModel[0]["cost_usd"] != nil || got.ByModel[1]["cost_usd"] != 0.0011 {
		t.Fatalf("by_model = %v: want sorted, with the unpriced model's cost null", got.ByModel)
	}

	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/rooms/r_nope/usage", "arjun", ""), http.StatusNotFound, "not_found")
	wantErr(t, doJSON(t, http.MethodGet, url, "", ""), http.StatusUnauthorized, "session_expired")
}

func TestRoomUsageOfAShadowRoomWaitsForTheClose(t *testing.T) {
	srv, st := newUsageServer(t, true)
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/alerts/webhook", "arjun", `{"title":"items stale","pipeline_id":"shadowed","dedup_key":"k1"}`)
	wantStatus(t, resp, http.StatusCreated)
	snap := decode[map[string]any](t, resp)
	roomID, _ := snap["id"].(string)
	if snap["shadow"] != true {
		t.Fatalf("not a shadow room: %v", snap)
	}
	addSpend(t, st, store.SpendChange{RoomID: roomID, PipelineID: "shadowed", Day: "2026-10-13", Tokens: 50,
		Call: &store.SpendCall{Model: "opus", InputTokens: 50}})
	path := srv.URL + "/api/rooms/" + roomID
	wantErr(t, doJSON(t, http.MethodGet, path+"/usage", "arjun", ""), http.StatusConflict, "room_open")
	wantStatus(t, doJSON(t, http.MethodPost, path+"/control/grant", "arjun", `{"to":"arjun"}`), http.StatusOK)
	wantStatus(t, doJSON(t, http.MethodPost, path+"/close", "arjun", `{"resolution":"fixed"}`), http.StatusOK)
	resp = doJSON(t, http.MethodGet, path+"/usage", "arjun", "")
	wantStatus(t, resp, http.StatusOK)
	if got := decode[map[string]any](t, resp); got["charged"].(map[string]any)["tokens"] != 50.0 {
		t.Fatalf("closed shadow room usage = %v", got)
	}
}

func TestPipelineUsage(t *testing.T) {
	srv, st := newUsageServer(t, true)
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	wantStatus(t, resp, http.StatusCreated)
	roomID := decode[room.RoomSnapshot](t, resp).ID
	addSpend(t, st, store.SpendChange{RoomID: roomID, PipelineID: "orders", Day: "2026-10-13", Tokens: 110,
		Call: &store.SpendCall{Model: "opus", InputTokens: 100, OutputTokens: 10, CostMicros: 2500, Priced: true}, CostMicros: 2500})
	addSpend(t, st, store.SpendChange{RoomID: roomID, PipelineID: "orders", Day: "2026-10-12", Tokens: 7,
		Call: &store.SpendCall{Model: "opus", InputTokens: 7}})

	// The default day is today in UTC (the server clock says 2026-10-13).
	for _, user := range []string{"arjun", "priya", "audrey"} {
		resp := doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=orders", user, "")
		wantStatus(t, resp, http.StatusOK)
		got := decode[map[string]any](t, resp)
		if got["day"] != "2026-10-13" || got["charged"].(map[string]any)["tokens"] != 110.0 ||
			got["limit"].(map[string]any)["tokens"] != 5_000_000.0 || got["limit"].(map[string]any)["cost_usd"] != 50.0 ||
			got["totals"].(map[string]any)["cost_usd"] != 0.0025 {
			t.Fatalf("%s: usage = %v", user, got)
		}
	}
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=orders&day=2026-10-12", "arjun", "")
	wantStatus(t, resp, http.StatusOK)
	if got := decode[map[string]any](t, resp); got["charged"].(map[string]any)["tokens"] != 7.0 || got["totals"].(map[string]any)["cost_usd"] != nil {
		t.Fatalf("2026-10-12 = %v", got)
	}
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=orders&day=2026-01-01", "arjun", "")
	wantStatus(t, resp, http.StatusOK)
	if got := decode[map[string]any](t, resp); got["charged"].(map[string]any)["tokens"] != 0.0 || len(got["by_model"].([]any)) != 0 {
		t.Fatalf("a day with no spend = %v, want zeros", got)
	}

	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage", "arjun", ""), http.StatusBadRequest, "validation")
	for _, bad := range []string{"13-10-2026", "2026-10-13T00:00:00Z", "2026-02-30", "today"} {
		wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=orders&day="+bad, "arjun", ""), http.StatusBadRequest, "validation")
	}
	for _, bad := range []string{"0000-01-01", "1999-12-31", "3000-01-01"} {
		wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=orders&day="+bad, "arjun", ""), http.StatusBadRequest, "validation")
	}
	for _, edge := range []string{"2000-01-01", "2999-12-31"} {
		wantStatus(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=orders&day="+edge, "arjun", ""), http.StatusOK)
	}
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=%FF", "arjun", ""), http.StatusBadRequest, "validation")
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=or%00ders", "arjun", ""), http.StatusBadRequest, "validation")
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline="+strings.Repeat("p", 201), "arjun", ""), http.StatusBadRequest, "validation")
	// 200 bytes is a valid id; it names no pipeline.
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline="+strings.Repeat("p", 200), "arjun", ""), http.StatusNotFound, "not_found")
	// No layer of this pipeline: 403.
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=orders", "nolayers", ""), http.StatusForbidden, "not_allowed")
	// An unknown pipeline is 404 before the role check, for everyone.
	for _, user := range []string{"nolayers", "arjun", "audrey"} {
		resp := doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=unknown", user, "")
		wantStatus(t, resp, http.StatusNotFound)
		if body := decode[map[string]map[string]string](t, resp); body["error"]["message"] != "pipeline not found" {
			t.Fatalf("%s: body %v", user, body)
		}
	}
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=orders", "", ""), http.StatusUnauthorized, "session_expired")
}

// usageTenantOwners puts orders in tenant a (arjun owns its layers) and
// other in tenant b (kabir owns its layers); audrey is an auditor in no
// tenant. Each pipeline has its own connection references, as tenancy needs.
func usageTenantOwners() string {
	own := func(user string) string {
		var b strings.Builder
		b.WriteString("\n    owners:")
		for _, l := range events.LayersOf(events.PipelineKindCDC) {
			b.WriteString("\n      " + l + ": {approvers: [user:" + user + "]}")
		}
		return b.String()
	}
	conns := func(p string) string {
		return "\n    connections: {source_db_url: {env: " + p + "_SRC}, warehouse_db_url: {env: " + p + "_WH}, kafka_brokers: {env: " + p +
			"_K}, connect_url: {env: " + p + "_C}, schema_registry_url: {env: " + p + "_SR}}"
	}
	return "tenants:\n  a: {members: []}\n  b: {members: []}\nauditors: [user:audrey]\npipelines:" +
		"\n  - id: orders\n    tenant: a\n    topic_prefix: pg\n    source_tables: [public.orders]\n    connectors: [c1]" + conns("A") + own("arjun") +
		"\n  - id: other\n    tenant: b\n    topic_prefix: ot\n    source_tables: [public.other]\n    connectors: [c2]" + conns("B") + own("kabir") + "\n"
}

// TestUsageTenancyComesBeforeTheRoleCheck uses #112's real tenancy: an owner
// of another tenant's pipeline and an auditor outside the tenant get the
// missing room's and the unknown pipeline's 404, never a 403 that would
// confirm the object exists.
func TestUsageTenancyComesBeforeTheRoleCheck(t *testing.T) {
	srv, st := newUsageServerWith(t, true, usageTenantOwners())
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	wantStatus(t, resp, http.StatusCreated)
	roomID := decode[room.RoomSnapshot](t, resp).ID
	addSpend(t, st, store.SpendChange{RoomID: roomID, PipelineID: "orders", Day: "2026-10-13", Tokens: 9})

	for _, user := range []string{"kabir", "audrey"} {
		missing := doJSON(t, http.MethodGet, srv.URL+"/api/rooms/r_nope/usage", user, "")
		hidden := doJSON(t, http.MethodGet, srv.URL+"/api/rooms/"+roomID+"/usage", user, "")
		wantStatus(t, hidden, http.StatusNotFound)
		if a, b := decode[json.RawMessage](t, missing), decode[json.RawMessage](t, hidden); string(a) != string(b) {
			t.Fatalf("%s: a hidden room's body %s differs from a missing room's %s", user, b, a)
		}
		unknown := doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=nope", user, "")
		hiddenP := doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=orders", user, "")
		wantStatus(t, hiddenP, http.StatusNotFound)
		if a, b := decode[json.RawMessage](t, unknown), decode[json.RawMessage](t, hiddenP); string(a) != string(b) {
			t.Fatalf("%s: a hidden pipeline's body %s differs from an unknown one's %s", user, b, a)
		}
	}
	wantStatus(t, doJSON(t, http.MethodGet, srv.URL+"/api/rooms/"+roomID+"/usage", "arjun", ""), http.StatusOK)
	wantStatus(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=orders", "arjun", ""), http.StatusOK)
	// kabir sees his own tenant's pipeline (no spend yet: an empty report).
	wantStatus(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=other", "kabir", ""), http.StatusOK)
}

func TestUsageRoutesStayMountedWithoutAStore(t *testing.T) {
	srv, _ := newUsageServer(t, false)
	// A mounted GET route answers 405 to POST; an unmounted path answers 404.
	for _, path := range []string{"/api/usage", "/api/rooms/r1/usage"} {
		resp := doJSON(t, http.MethodPost, srv.URL+path, "arjun", "{}")
		wantStatus(t, resp, http.StatusMethodNotAllowed)
		resp.Body.Close()
	}
}
