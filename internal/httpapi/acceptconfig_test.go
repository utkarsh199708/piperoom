package httpapi_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// fakeAccepter records AcceptConfig calls and answers with err or two hashes.
type fakeAccepter struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (f *fakeAccepter) AcceptConfig(_ context.Context, pid, name string) (string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, pid+"/"+name)
	if f.err != nil {
		return "", "", f.err
	}
	return "oldhash", "newhash", nil
}

func (f *fakeAccepter) n() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// newAcceptServer serves the usage fixtures (arjun owns kafka-connect, the
// orders pipeline has connector c1) with acc wired, or none when nil.
func newAcceptServer(t *testing.T, acc httpapi.ConfigAccepter, log *strings.Builder) *httptest.Server {
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
	if acc != nil {
		opts = append(opts, httpapi.WithConfigAccept(acc))
	}
	var lg *slog.Logger
	if log != nil {
		lg = slog.New(slog.NewTextHandler(&acceptLogWriter{b: log}, nil))
	}
	api := httpapi.New(svc, st, lg, opts...)
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})
	return srv
}

type acceptLogWriter struct {
	mu sync.Mutex
	b  *strings.Builder
}

func (w *acceptLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

// X3b TC5: a non-owner is 403, the kafka-connect owner 200 with the new
// hash; visibility answers before the role and the watcher.
func TestAcceptConfigRoute(t *testing.T) {
	acc := &fakeAccepter{}
	var log strings.Builder
	srv := newAcceptServer(t, acc, &log)
	url := func(pid, name string) string {
		return fmt.Sprintf("%s/api/pipelines/%s/connectors/%s/accept-config", srv.URL, pid, name)
	}

	wantErr(t, doJSON(t, http.MethodPost, url("orders", "c1"), "priya", ""), http.StatusForbidden, "not_layer_owner")
	wantErr(t, doJSON(t, http.MethodPost, url("orders", "c1"), "nolayers", ""), http.StatusForbidden, "not_layer_owner")
	wantErr(t, doJSON(t, http.MethodPost, url("nope", "c1"), "priya", ""), http.StatusNotFound, "not_found")
	// c2 belongs to another pipeline: not this one's.
	wantErr(t, doJSON(t, http.MethodPost, url("orders", "c2"), "arjun", ""), http.StatusNotFound, "not_found")
	if acc.n() != 0 {
		t.Fatalf("the watcher was called %d times on refused requests", acc.n())
	}

	resp := doJSON(t, http.MethodPost, url("orders", "c1"), "arjun", "")
	wantStatus(t, resp, http.StatusOK)
	got := decode[map[string]string](t, resp)
	if got["connector"] != "c1" || got["hash"] != "newhash" || len(got) != 2 {
		t.Fatalf("body = %v", got)
	}
	if acc.n() != 1 || acc.calls[0] != "orders/c1" {
		t.Fatalf("watcher calls = %v", acc.calls)
	}
	for _, want := range []string{"user=arjun", "pipeline=orders", "connector=c1", "old_hash=oldhash", "new_hash=newhash"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log has no %q:\n%s", want, log.String())
		}
	}
	// GET is not the route.
	if resp := doJSON(t, http.MethodGet, url("orders", "c1"), "arjun", ""); resp.StatusCode == http.StatusOK {
		t.Fatalf("GET accept-config = 200")
	}
}

// X3b: 409 watch_off with no watcher, 503 connect_unavailable when Connect
// cannot be read, a generic 500 for anything else.
func TestAcceptConfigRouteErrors(t *testing.T) {
	url := "/api/pipelines/orders/connectors/c1/accept-config"
	srv := newAcceptServer(t, nil, nil)
	wantErr(t, doJSON(t, http.MethodPost, srv.URL+url, "arjun", ""), http.StatusConflict, "watch_off")
	// The checks before it still answer first.
	wantErr(t, doJSON(t, http.MethodPost, srv.URL+url, "priya", ""), http.StatusForbidden, "not_layer_owner")

	for _, c := range []struct {
		err    error
		status int
		code   string
	}{
		{fmt.Errorf("server: %w", httpapi.ErrWatchOff), http.StatusConflict, "watch_off"},
		{fmt.Errorf("server: %w: dial tcp 10.0.0.1:8083: refused", httpapi.ErrConnectUnavailable), http.StatusServiceUnavailable, "connect_unavailable"},
		{errors.New("boom"), http.StatusInternalServerError, "internal_error"},
	} {
		srv := newAcceptServer(t, &fakeAccepter{err: c.err}, nil)
		resp := doJSON(t, http.MethodPost, srv.URL+url, "arjun", "")
		if resp.StatusCode != c.status {
			t.Fatalf("%v: status %d, want %d", c.err, resp.StatusCode, c.status)
		}
		code, msg := errCode(t, resp)
		if code != c.code || strings.Contains(msg, "10.0.0.1") {
			t.Fatalf("%v: code %q message %q", c.err, code, msg)
		}
	}
}
