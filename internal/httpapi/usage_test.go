package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	users, err := room.ParseUsers([]byte(usageUsers))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(usageOwners), 0o600); err != nil {
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
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=%FF", "arjun", ""), http.StatusBadRequest, "validation")
	// No layer anywhere, or layers only on pipelines that are not this one.
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=orders", "nolayers", ""), http.StatusForbidden, "not_allowed")
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=unknown", "arjun", ""), http.StatusForbidden, "not_allowed")
	// The auditor reads any pipeline.
	wantStatus(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=unknown", "audrey", ""), http.StatusOK)
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/usage?pipeline=orders", "", ""), http.StatusUnauthorized, "session_expired")
}
