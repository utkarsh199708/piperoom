package httpapi_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/adminconfig"
	"piperoom/internal/connprobe"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// fakeAdmin is the admin provider the routes run over.
type fakeAdmin struct {
	mu    sync.Mutex
	tests []string
}

func (f *fakeAdmin) Config() adminconfig.AdminConfig {
	return adminconfig.AdminConfig{
		Source: "file", OwnersFile: "owners.yaml", UsersFile: "users.yaml",
		Connections: []adminconfig.Connection{{
			ID: "process-wide.kafka", Scope: "process-wide", Kind: connprobe.KindKafka, UsedBy: []string{"orders"},
			Settings: []adminconfig.Setting{{Key: "kafka_brokers", Display: "localhost:29092", From: "env KAFKA_BROKERS"}},
		}},
		Pipelines: []adminconfig.PipelineView{}, People: []adminconfig.Person{},
	}
}

func (f *fakeAdmin) Test(_ context.Context, id string) (connprobe.Result, error) {
	f.mu.Lock()
	f.tests = append(f.tests, id)
	f.mu.Unlock()
	if id != "process-wide.kafka" {
		return connprobe.Result{}, adminconfig.ErrUnknownConnection
	}
	return connprobe.Result{Kind: connprobe.KindKafka, OK: false, TookMS: 3, Checks: []connprobe.Check{
		{ID: "reach", Status: connprobe.StatusFail, MS: 3, Problem: "no broker answered", Fix: "check KAFKA_BROKERS"},
	}}, nil
}

func (f *fakeAdmin) Export() ([]byte, error) {
	return []byte("pipelines: []\n\n---\nusers: []\n"), nil
}

func (f *fakeAdmin) ran() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.tests)
}

const adminUsersYAML = `
users:
  - id: arjun
    name: Arjun
    layers: [kafka-connect, kafka]
  - id: ada
    name: Ada
    admin: true
`

// newAdminServer is a dev-mode server with an admin (ada) and a member
// (arjun); opts add WithAdmin or not.
func newAdminServer(t *testing.T, opts ...httpapi.Option) (*httptest.Server, *syncBuf) {
	t.Helper()
	users, err := room.ParseUsers([]byte(adminUsersYAML))
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuf{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: users, Run: fakeRun})
	h := httpapi.New(svc, st, log, opts...)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	t.Cleanup(h.Close)
	return srv, logs
}

// adminReq sends a request with an explicit content type ("" = none).
func adminReq(t *testing.T, method, url, user, ctype, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if user != "" {
		req.Header.Set("X-User", user)
	}
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func wantErrCode(t *testing.T, resp *http.Response, status int, code string) {
	t.Helper()
	if resp.StatusCode != status {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d; body %s", resp.StatusCode, status, b)
	}
	if got, _ := errCode(t, resp); got != code {
		t.Fatalf("code = %q, want %q", got, code)
	}
}

const testJSON = "application/json"

// Without WithAdmin the routes do not exist, even for an admin.
func TestAdminRoutesOffWithoutOption(t *testing.T) {
	srv, _ := newAdminServer(t)
	for _, c := range []struct{ method, path, body string }{
		{http.MethodGet, "/api/admin/config", ""},
		{http.MethodPost, "/api/admin/connections/test", `{"id":"process-wide.kafka"}`},
		{http.MethodGet, "/api/admin/config/export", ""},
	} {
		wantErrCode(t, adminReq(t, c.method, srv.URL+c.path, "ada", testJSON, c.body), http.StatusNotFound, "not_found")
	}
}

// A member who is not an admin gets 403 not_allowed on every route, before
// the content type or body is looked at; no test runs.
func TestAdminRoutesMemberNotAllowed(t *testing.T) {
	fa := &fakeAdmin{}
	srv, _ := newAdminServer(t, httpapi.WithAdmin(fa))
	for _, c := range []struct{ method, path, ctype, body string }{
		{http.MethodGet, "/api/admin/config", "", ""},
		{http.MethodGet, "/api/admin/config/export", "", ""},
		{http.MethodPost, "/api/admin/connections/test", testJSON, `{"id":"process-wide.kafka"}`},
		{http.MethodPost, "/api/admin/connections/test", "text/plain", `not json`},
		{http.MethodPost, "/api/admin/connections/test", testJSON, `{"id":"nope"}`},
	} {
		wantErrCode(t, adminReq(t, c.method, srv.URL+c.path, "arjun", c.ctype, c.body), http.StatusForbidden, "not_allowed")
	}
	if fa.ran() != 0 {
		t.Fatalf("a member ran %d tests", fa.ran())
	}
	// No credentials: 401 before anything.
	if resp := adminReq(t, http.MethodGet, srv.URL+"/api/admin/config", "", "", ""); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous: %d, want 401", resp.StatusCode)
	}
}

// A viewer gets 403 viewer, GET and POST alike.
func TestAdminRoutesViewer(t *testing.T) {
	fa := &fakeAdmin{}
	e := newOIDCEnv(t, oidcOpts{extra: []httpapi.Option{httpapi.WithAdmin(fa)}})
	c := e.mustLogin(t, idpViewer)
	for _, r := range []struct{ method, path, body string }{
		{http.MethodGet, "/api/admin/config", ""},
		{http.MethodGet, "/api/admin/config/export", ""},
		{http.MethodPost, "/api/admin/connections/test", `{"id":"process-wide.kafka"}`},
	} {
		wantErrCode(t, e.do(t, r.method, r.path, r.body, ck(c)), http.StatusForbidden, "viewer")
	}
	// A member who is not an admin (users.yaml has no admin here).
	m := e.mustLogin(t, idpPriya)
	wantErrCode(t, e.do(t, http.MethodGet, "/api/admin/config", "", ck(m)), http.StatusForbidden, "not_allowed")
	if fa.ran() != 0 {
		t.Fatalf("ran %d tests", fa.ran())
	}
}

func TestAdminConfigRoute(t *testing.T) {
	srv, _ := newAdminServer(t, httpapi.WithAdmin(&fakeAdmin{}))
	resp := adminReq(t, http.MethodGet, srv.URL+"/api/admin/config", "ada", "", "")
	wantStatus(t, resp, http.StatusOK)
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q", cc)
	}
	ac := decode[adminconfig.AdminConfig](t, resp)
	if ac.Source != "file" || len(ac.Connections) != 1 || ac.Connections[0].ID != "process-wide.kafka" ||
		ac.Connections[0].Settings[0].Display != "localhost:29092" {
		t.Fatalf("body = %+v", ac)
	}
}

func TestAdminTestRoute(t *testing.T) {
	fa := &fakeAdmin{}
	srv, logs := newAdminServer(t, httpapi.WithAdmin(fa))
	url := srv.URL + "/api/admin/connections/test"

	resp := adminReq(t, http.MethodPost, url, "ada", testJSON, `{"id":"process-wide.kafka"}`)
	wantStatus(t, resp, http.StatusOK) // a failed check is ok: false, not a 5xx
	res := decode[connprobe.Result](t, resp)
	if res.OK || res.Kind != connprobe.KindKafka || len(res.Checks) != 1 || res.Checks[0].Status != "fail" {
		t.Fatalf("result = %+v", res)
	}
	if l := logs.String(); !strings.Contains(l, `msg="admin: connection test" actor=ada id=process-wide.kafka ok=false`) {
		t.Errorf("no test log line:\n%s", l)
	}

	wantErrCode(t, adminReq(t, http.MethodPost, url, "ada", testJSON, `{"id":"pipeline.nope.kafka"}`), http.StatusNotFound, "not_found")
	if strings.Contains(logs.String(), "pipeline.nope.kafka") {
		t.Error("an unknown id was logged as a test")
	}
	for _, body := range []string{`not json`, `{"id":`, `{"id":""}`, `{}`, `{"id":"` + strings.Repeat("x", 5000) + `"}`} {
		wantErrCode(t, adminReq(t, http.MethodPost, url, "ada", testJSON, body), http.StatusBadRequest, "validation")
	}
	// The content type rule of every other POST.
	wantErrCode(t, adminReq(t, http.MethodPost, url, "ada", "text/plain", `{"id":"process-wide.kafka"}`), http.StatusBadRequest, "bad_request")
	wantErrCode(t, adminReq(t, http.MethodPost, url, "ada", "", `{"id":"process-wide.kafka"}`), http.StatusBadRequest, "bad_request")
	// Wrong method.
	wantErrCode(t, adminReq(t, http.MethodGet, url, "ada", "", ""), http.StatusMethodNotAllowed, "method_not_allowed")
}

// 10 tests per minute per admin, then 429 rate_limited without running one.
func TestAdminTestRateLimit(t *testing.T) {
	fa := &fakeAdmin{}
	users := adminUsersYAML + `  - id: grace
    name: Grace
    admin: true
`
	parsed, err := room.ParseUsers([]byte(users))
	if err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	h := httpapi.New(room.New(room.Deps{Store: st, Users: parsed, Run: fakeRun}), st, nil, httpapi.WithAdmin(fa))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	t.Cleanup(h.Close)
	url := srv.URL + "/api/admin/connections/test"

	for i := 0; i < 10; i++ {
		wantStatus(t, adminReq(t, http.MethodPost, url, "ada", testJSON, `{"id":"process-wide.kafka"}`), http.StatusOK)
	}
	wantErrCode(t, adminReq(t, http.MethodPost, url, "ada", testJSON, `{"id":"process-wide.kafka"}`), http.StatusTooManyRequests, "rate_limited")
	if fa.ran() != 10 {
		t.Fatalf("ran %d tests, want 10", fa.ran())
	}
	// The limit is per admin.
	wantStatus(t, adminReq(t, http.MethodPost, url, "grace", testJSON, `{"id":"process-wide.kafka"}`), http.StatusOK)
	// Reading the settings is not limited.
	wantStatus(t, adminReq(t, http.MethodGet, srv.URL+"/api/admin/config", "ada", "", ""), http.StatusOK)
}

func TestAdminExportRoute(t *testing.T) {
	srv, _ := newAdminServer(t, httpapi.WithAdmin(&fakeAdmin{}))
	resp := adminReq(t, http.MethodGet, srv.URL+"/api/admin/config/export", "ada", "", "")
	wantStatus(t, resp, http.StatusOK)
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/yaml") {
		t.Errorf("Content-Type = %q", ct)
	}
	if cd := resp.Header.Get("Content-Disposition"); cd != `attachment; filename="piperoom-config.yaml"` {
		t.Errorf("Content-Disposition = %q", cd)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q", cc)
	}
	b, _ := io.ReadAll(resp.Body)
	if string(b) != "pipelines: []\n\n---\nusers: []\n" {
		t.Errorf("body = %q", b)
	}
}
