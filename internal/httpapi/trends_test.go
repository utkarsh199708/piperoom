package httpapi_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// newTrendsServer is newUsageServer with the sample reader wired.
func newTrendsServer(t *testing.T, samples clients.SampleReader) *httptest.Server {
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
	if samples != nil {
		opts = append(opts, httpapi.WithSamples(samples))
	}
	api := httpapi.New(svc, st, nil, opts...)
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})
	return srv
}

func TestTrendsRoute(t *testing.T) {
	samples := memstore.New()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	for _, h := range []int{50, 30, 2, 1} { // 50 h and 30 h are outside days=1
		if err := samples.AppendSample(ctx, "orders", clients.WatchMetricSlotWALBytes, now.Add(-time.Duration(h)*time.Hour), float64(100-h)); err != nil {
			t.Fatal(err)
		}
	}
	srv := newTrendsServer(t, samples)
	url := srv.URL + "/api/pipelines/orders/trends?metric=slot_wal_bytes"

	// Default one day, oldest first; a viewer-like user with no layers may read.
	resp := doJSON(t, http.MethodGet, url, "nolayers", "")
	wantStatus(t, resp, http.StatusOK)
	got := decode[[]clients.SamplePoint](t, resp)
	if len(got) != 2 || got[0].Value != 98 || got[1].Value != 99 || !got[0].At.Before(got[1].At) {
		t.Fatalf("days=1: %+v", got)
	}
	resp = doJSON(t, http.MethodGet, url+"&days=3", "arjun", "")
	wantStatus(t, resp, http.StatusOK)
	if got := decode[[]clients.SamplePoint](t, resp); len(got) != 4 || got[0].Value != 50 {
		t.Fatalf("days=3: %+v", got)
	}
	// Another metric with no samples is [] (not null).
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/pipelines/orders/trends?metric=group_lag", "arjun", "")
	wantStatus(t, resp, http.StatusOK)
	if got := decode[[]clients.SamplePoint](t, resp); got == nil || len(got) != 0 {
		t.Fatalf("no samples: %#v", got)
	}

	for _, bad := range []string{"0", "8", "-1", "x", "1.5"} {
		wantErr(t, doJSON(t, http.MethodGet, url+"&days="+bad, "arjun", ""), http.StatusBadRequest, "validation")
	}
	for _, q := range []string{"", "?metric=", "?metric=cpu", "?metric=SLOT_WAL_BYTES"} {
		wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/pipelines/orders/trends"+q, "arjun", ""), http.StatusBadRequest, "validation")
	}
	// TC5: an unknown pipeline is 404, before the query is even checked.
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/pipelines/nope/trends?metric=slot_wal_bytes", "arjun", ""), http.StatusNotFound, "not_found")
	wantErr(t, doJSON(t, http.MethodGet, srv.URL+"/api/pipelines/nope/trends?metric=cpu", "arjun", ""), http.StatusNotFound, "not_found")
}

// Without a sample reader (watcher never wired) the route still validates and
// answers [].
func TestTrendsRouteNoReader(t *testing.T) {
	srv := newTrendsServer(t, nil)
	resp := doJSON(t, http.MethodGet, srv.URL+"/api/pipelines/orders/trends?metric=dlq_records&days=7", "arjun", "")
	wantStatus(t, resp, http.StatusOK)
	if got := decode[[]clients.SamplePoint](t, resp); got == nil || len(got) != 0 {
		t.Fatalf("got %#v", got)
	}
}

type failingSamples struct{}

func (failingSamples) Samples(context.Context, string, string, time.Time) ([]clients.SamplePoint, error) {
	return nil, errors.New("store: list samples: connection refused")
}

// A store failure is a 500 with the generic message, never the store's text.
func TestTrendsRouteStoreError(t *testing.T) {
	srv := newTrendsServer(t, failingSamples{})
	resp := doJSON(t, http.MethodGet, srv.URL+"/api/pipelines/orders/trends?metric=dlq_records", "arjun", "")
	wantErr(t, resp, http.StatusInternalServerError, "internal_error")
}

// TC5 (contract wins over the ticket's 403): another tenant's pipeline answers
// exactly like one that does not exist.
func TestTrendsTenants(t *testing.T) {
	e := newTenancyEnv(t, true)
	e.mustDo(t, "ann", http.MethodGet, "/api/pipelines/pa/trends?metric=group_lag", "", http.StatusOK)
	other := e.mustDo(t, "mallory", http.MethodGet, "/api/pipelines/pa/trends?metric=group_lag", "", http.StatusNotFound)
	missing := e.mustDo(t, "mallory", http.MethodGet, "/api/pipelines/zzz/trends?metric=group_lag", "", http.StatusNotFound)
	if other != missing {
		t.Fatalf("tenant 404 differs from unknown 404: %q vs %q", other, missing)
	}
}

func TestTrendsViewer(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	v := e.mustLogin(t, idpViewer)
	wantStatus(t, e.do(t, http.MethodGet, "/api/pipelines/orders/trends?metric=slot_wal_bytes", "", ck(v)), http.StatusOK)
}
