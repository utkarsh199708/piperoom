package httpapi_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/auth"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/integrations"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// intCanary is a dummy secret; no integrations response may contain it,
// except the one rotate response that hands out a new alert secret.
const intCanary = "CANARY-int-s3cr3t-7d1e"

// fakeConnectKind is a connect kind whose test fails when url is "fail" and
// whose check details echo nothing secret.
func fakeConnectKind() spec.Kind {
	return spec.Kind{
		ID: "fake-connect", Category: spec.CategoryConnect, Title: "Kafka Connect", Summary: "test kind",
		Fields: []spec.Field{
			{Name: "url", Label: "URL", Type: spec.FieldText, Required: true},
			{Name: "password", Label: "Password", Type: spec.FieldSecret},
		},
		SetupScript: func(v spec.Values) string { return "grant to " + v.Get("url") },
		Test: func(_ context.Context, v spec.Values) []spec.Check {
			if v.Get("url") == "fail" {
				return []spec.Check{{ID: "reach", Title: "Can reach host", Status: spec.CheckFail, Detail: "connection refused", Fix: "check the url"}}
			}
			return []spec.Check{{ID: "reach", Title: "Can reach host", Status: spec.CheckPass}}
		},
		Build: func(context.Context, spec.Values) (any, func(), error) { return nil, func() {}, nil },
	}
}

type intEnv struct {
	srv      *httptest.Server
	room     *room.Service
	sessions auth.Store
}

type intEnvOpts struct {
	settings auth.Settings
	solo     string
	noKey    bool
}

func newIntEnv(t *testing.T, o intEnvOpts) *intEnv {
	t.Helper()
	st := memstore.New()
	sessions := auth.NewMemStore()
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), SoloUser: o.solo, ViewerGroups: []string{"watchers"}, Sessions: sessions,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, nil
		},
	})
	reg := integrations.NewRegistry()
	if err := reg.Register(fakeConnectKind()); err != nil {
		t.Fatal(err)
	}
	// The built-in alert kinds back the alert setup and rotate routes.
	for _, k := range integrations.BuiltinKinds(integrations.Deps{}) {
		if k.Category == spec.CategoryAlerts {
			if err := reg.Register(k); err != nil {
				t.Fatal(err)
			}
		}
	}
	var keys *integrations.Keyring
	if !o.noKey {
		raw := make([]byte, 32)
		if _, err := rand.Read(raw); err != nil {
			t.Fatal(err)
		}
		k, err := integrations.NewKeyring(netsec.Secret(base64.StdEncoding.EncodeToString(raw)), "")
		if err != nil {
			t.Fatal(err)
		}
		keys = k
	}
	isvc, err := integrations.NewService(integrations.Options{
		Store: memstore.NewIntegrations(), Registry: reg, Keys: keys, Notifier: svc,
		PublicURL: "https://piperoom.example", TestsPerMinute: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	admins, err := httpapi.ParseAdmins("arjun, group:platform")
	if err != nil {
		t.Fatal(err)
	}
	settings := o.settings
	if settings.Mode == "" {
		settings = auth.Default()
	}
	api := httpapi.New(svc, st, nil, httpapi.WithAuth(settings), httpapi.WithSessions(sessions),
		httpapi.WithIntegrations(httpapi.Integrations{Service: isvc, Admins: admins}))
	srv := httptest.NewServer(api)
	t.Cleanup(func() { srv.Close(); api.Close() })
	return &intEnv{srv: srv, room: svc, sessions: sessions}
}

// call sends one request as user (X-User) and returns status and body.
func (e *intEnv) call(t *testing.T, method, path, user, body string, hdr ...string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if user != "" {
		req.Header.Set("X-User", user)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func intErrCode(t *testing.T, body string) string {
	t.Helper()
	var b struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &b); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	return b.Error.Code
}

func wantCode(t *testing.T, status int, body string, wantStatus int, code string) {
	t.Helper()
	if status != wantStatus {
		t.Fatalf("status %d, want %d: %s", status, wantStatus, body)
	}
	if code != "" && intErrCode(t, body) != code {
		t.Fatalf("code %q, want %q: %s", intErrCode(t, body), code, body)
	}
}

// viewerCookie seeds a session for a viewer (in the viewer group only).
func (e *intEnv) viewerCookie(t *testing.T) string {
	t.Helper()
	token, hash, err := auth.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := e.sessions.Create(context.Background(), auth.Session{
		ID: room.NewID("ses_"), TokenHash: hash, UserID: "vic", Name: "Vic", Groups: []string{"watchers"},
		Method: auth.MethodDev, CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	return auth.DevSessionCookie + "=" + token
}

func meAdmin(t *testing.T, e *intEnv, user string, hdr ...string) bool {
	t.Helper()
	status, body := e.call(t, http.MethodGet, "/api/me", user, "", hdr...)
	if status != http.StatusOK {
		t.Fatalf("me: %d %s", status, body)
	}
	var me map[string]any
	if err := json.Unmarshal([]byte(body), &me); err != nil {
		t.Fatal(err)
	}
	v, ok := me["admin"].(bool)
	if !ok {
		t.Fatalf("me has no admin bool: %s", body)
	}
	return v
}

func TestIntegrationsAdminRule(t *testing.T) {
	e := newIntEnv(t, intEnvOpts{solo: ""})

	// The catalog is open to every logged-in user.
	status, body := e.call(t, http.MethodGet, "/api/integrations/catalog", "priya", "")
	wantCode(t, status, body, http.StatusOK, "")
	if !strings.Contains(body, `"has_setup_script":true`) || !strings.Contains(body, `"options":[]`) {
		t.Fatalf("catalog: %s", body)
	}

	// PIPEROOM_ADMINS names arjun.
	status, body = e.call(t, http.MethodGet, "/api/integrations", "arjun", "")
	wantCode(t, status, body, http.StatusOK, "")
	if strings.TrimSpace(body) != "[]" {
		t.Fatalf("empty list should be []: %s", body)
	}
	if !meAdmin(t, e, "arjun") {
		t.Fatal("arjun should be admin")
	}

	// Anyone else: 403 not_admin on every route but the catalog.
	for _, r := range []struct{ method, path, body string }{
		{http.MethodGet, "/api/integrations", ""},
		{http.MethodPost, "/api/integrations/test", `{"kind":"fake-connect","config":{"url":"x"}}`},
		{http.MethodPost, "/api/integrations", `{"kind":"fake-connect","config":{"url":"x"}}`},
		{http.MethodPut, "/api/integrations/int_x", `{}`},
		{http.MethodDelete, "/api/integrations/int_x", ""},
		{http.MethodPost, "/api/integrations/catalog/fake-connect/setup-script", `{"config":{}}`},
		{http.MethodGet, "/api/integrations/alerts/grafana/setup", ""},
		{http.MethodGet, "/api/integrations/int_x/audit", ""},
	} {
		status, body := e.call(t, r.method, r.path, "priya", r.body)
		wantCode(t, status, body, http.StatusForbidden, "not_admin")
	}
	if meAdmin(t, e, "priya") {
		t.Fatal("priya is no admin")
	}

	// A viewer: catalog yes, GET routes not_admin, writes 403 viewer.
	vc := e.viewerCookie(t)
	status, body = e.call(t, http.MethodGet, "/api/integrations/catalog", "", "", "Cookie", vc)
	wantCode(t, status, body, http.StatusOK, "")
	status, body = e.call(t, http.MethodGet, "/api/integrations", "", "", "Cookie", vc)
	wantCode(t, status, body, http.StatusForbidden, "not_admin")
	status, body = e.call(t, http.MethodPost, "/api/integrations", "", `{"kind":"fake-connect"}`, "Cookie", vc)
	wantCode(t, status, body, http.StatusForbidden, "viewer")
	if meAdmin(t, e, "", "Cookie", vc) {
		t.Fatal("a viewer is never admin")
	}
}

func TestIntegrationsSoloUserIsAdmin(t *testing.T) {
	e := newIntEnv(t, intEnvOpts{solo: "priya"})
	status, body := e.call(t, http.MethodGet, "/api/integrations", "priya", "")
	wantCode(t, status, body, http.StatusOK, "")
	if !meAdmin(t, e, "priya") {
		t.Fatal("the solo user is admin")
	}
}

func TestIntegrationsDevNonLoopbackNeedsOIDC(t *testing.T) {
	s := auth.Default()
	s.DevNonLoopback = true
	e := newIntEnv(t, intEnvOpts{settings: s})
	status, body := e.call(t, http.MethodGet, "/api/integrations", "arjun", "")
	wantCode(t, status, body, http.StatusForbidden, "admin_needs_oidc")
	status, body = e.call(t, http.MethodPost, "/api/integrations", "arjun", `{"kind":"fake-connect","config":{"url":"x"}}`)
	wantCode(t, status, body, http.StatusForbidden, "admin_needs_oidc")
	status, body = e.call(t, http.MethodGet, "/api/integrations/catalog", "arjun", "")
	wantCode(t, status, body, http.StatusOK, "")
	if meAdmin(t, e, "arjun") {
		t.Fatal("dev mode on a non-loopback listen reports admin false")
	}
}

type viewJSON struct {
	ID         string         `json:"id"`
	Version    int64          `json:"version"`
	Config     map[string]any `json:"config"`
	SecretsSet []string       `json:"secrets_set"`
	Status     string         `json:"status"`
	LastTest   []spec.Check   `json:"last_test"`
}

func createFake(t *testing.T, e *intEnv, url string) viewJSON {
	t.Helper()
	status, body := e.call(t, http.MethodPost, "/api/integrations", "arjun",
		`{"kind":"fake-connect","config":{"url":"`+url+`"},"secrets":{"password":"`+intCanary+`"}}`)
	wantCode(t, status, body, http.StatusCreated, "")
	if strings.Contains(body, intCanary) {
		t.Fatalf("create response leaks the secret: %s", body)
	}
	var out struct {
		Integration viewJSON     `json:"integration"`
		Checks      []spec.Check `json:"checks"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	if out.Integration.Version != 1 || len(out.Checks) != 1 || len(out.Integration.SecretsSet) != 1 {
		t.Fatalf("create: %s", body)
	}
	return out.Integration
}

func TestIntegrationsIfMatch(t *testing.T) {
	e := newIntEnv(t, intEnvOpts{})
	v := createFake(t, e, "http://connect:8083")
	path := "/api/integrations/" + v.ID
	// A changed url needs the password again (integrations.md §8).
	upd := `{"kind":"fake-connect","config":{"url":"http://connect2:8083"},"secrets":{"password":"typed-again"}}`

	status, body := e.call(t, http.MethodPut, path, "arjun", upd)
	wantCode(t, status, body, http.StatusBadRequest, "validation")
	status, body = e.call(t, http.MethodPut, path, "arjun", upd, "If-Match", "abc")
	wantCode(t, status, body, http.StatusBadRequest, "validation")
	status, body = e.call(t, http.MethodPut, path, "arjun", upd, "If-Match", "7")
	wantCode(t, status, body, http.StatusConflict, "version_conflict")

	status, body = e.call(t, http.MethodPut, path, "arjun", upd, "If-Match", `"1"`)
	wantCode(t, status, body, http.StatusOK, "")
	if !strings.Contains(body, `"version":2`) || !strings.Contains(body, "connect2") {
		t.Fatalf("update: %s", body)
	}
	// The same stale version again is a conflict now.
	status, body = e.call(t, http.MethodPut, path, "arjun", upd, "If-Match", "1")
	wantCode(t, status, body, http.StatusConflict, "version_conflict")
	// Unknown row.
	status, body = e.call(t, http.MethodPut, "/api/integrations/int_nope", "arjun", upd, "If-Match", "1")
	wantCode(t, status, body, http.StatusNotFound, "not_found")
}

func TestIntegrationsChecksFailed(t *testing.T) {
	e := newIntEnv(t, intEnvOpts{})
	status, body := e.call(t, http.MethodPost, "/api/integrations", "arjun",
		`{"kind":"fake-connect","config":{"url":"fail"},"secrets":{"password":"`+intCanary+`"}}`)
	wantCode(t, status, body, http.StatusUnprocessableEntity, "checks_failed")
	var b struct {
		Checks []spec.Check `json:"checks"`
	}
	if err := json.Unmarshal([]byte(body), &b); err != nil || len(b.Checks) != 1 || b.Checks[0].Status != spec.CheckFail {
		t.Fatalf("422 body: %s", body)
	}
	if strings.Contains(body, intCanary) {
		t.Fatalf("422 leaks the secret: %s", body)
	}
	// Nothing was saved.
	_, body = e.call(t, http.MethodGet, "/api/integrations", "arjun", "")
	if strings.TrimSpace(body) != "[]" {
		t.Fatalf("a failed create saved a row: %s", body)
	}
	// POST /test answers 200 even when a check fails.
	status, body = e.call(t, http.MethodPost, "/api/integrations/test", "arjun", `{"kind":"fake-connect","config":{"url":"fail"}}`)
	wantCode(t, status, body, http.StatusOK, "")
	if !strings.Contains(body, `"status":"fail"`) {
		t.Fatalf("test: %s", body)
	}
}

func TestIntegrationsBadInput(t *testing.T) {
	e := newIntEnv(t, intEnvOpts{})
	cases := []struct {
		name, body, field string
	}{
		{"unknown kind", `{"kind":"nope","config":{}}`, "kind"},
		{"secret is a number", `{"kind":"fake-connect","config":{"url":"x"},"secrets":{"password":42}}`, "password"},
		{"ref without scheme", `{"kind":"fake-connect","config":{"url":"x"},"secrets":{"password":{"$ref":"vault/x"}}}`, "password"},
		{"missing required", `{"kind":"fake-connect","config":{}}`, "url"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, body := e.call(t, http.MethodPost, "/api/integrations", "arjun", c.body)
			wantCode(t, status, body, http.StatusBadRequest, "validation")
			var b struct {
				Error struct {
					Field string `json:"field"`
				} `json:"error"`
			}
			_ = json.Unmarshal([]byte(body), &b)
			if b.Error.Field != c.field {
				t.Fatalf("field %q, want %q: %s", b.Error.Field, c.field, body)
			}
		})
	}
	status, body := e.call(t, http.MethodPost, "/api/integrations", "arjun", `{not json`)
	wantCode(t, status, body, http.StatusBadRequest, "validation")
	status, body = e.call(t, http.MethodPost, "/api/integrations/catalog/nope/setup-script", "arjun", `{"config":{}}`)
	wantCode(t, status, body, http.StatusNotFound, "not_found")
	status, body = e.call(t, http.MethodGet, "/api/integrations/alerts/nagios/setup", "arjun", "")
	wantCode(t, status, body, http.StatusNotFound, "not_found")
	status, body = e.call(t, http.MethodGet, "/api/integrations/int_x/audit?limit=-1", "arjun", "")
	wantCode(t, status, body, http.StatusBadRequest, "validation")
	status, body = e.call(t, http.MethodDelete, "/api/integrations/int_x", "arjun", "")
	wantCode(t, status, body, http.StatusNotFound, "not_found")
	status, body = e.call(t, http.MethodPost, "/api/integrations/int_x/enable", "arjun", "")
	wantCode(t, status, body, http.StatusNotFound, "not_found")
}

func TestIntegrationsSecretKeyMissing(t *testing.T) {
	e := newIntEnv(t, intEnvOpts{noKey: true})
	status, body := e.call(t, http.MethodPost, "/api/integrations", "arjun",
		`{"kind":"fake-connect","config":{"url":"x"},"secrets":{"password":"`+intCanary+`"}}`)
	wantCode(t, status, body, http.StatusConflict, "secret_key_missing")
	status, body = e.call(t, http.MethodPost, "/api/integrations/alerts/grafana/rotate", "arjun", "")
	wantCode(t, status, body, http.StatusConflict, "secret_key_missing")
}

// TestIntegrationsNoSecretInAnyResponse walks every route after a create with
// a canary secret and checks that no response but rotate carries a secret.
func TestIntegrationsNoSecretInAnyResponse(t *testing.T) {
	e := newIntEnv(t, intEnvOpts{})
	v := createFake(t, e, "http://connect:8083")
	id := v.ID
	upd := `{"kind":"fake-connect","config":{"url":"http://c3:8083"},"secrets":{"password":"` + intCanary + `x"}}`
	reqs := []struct {
		method, path, body string
		hdr                []string
	}{
		{http.MethodGet, "/api/integrations", "", nil},
		{http.MethodGet, "/api/integrations/catalog", "", nil},
		{http.MethodPost, "/api/integrations/catalog/fake-connect/setup-script", `{"config":{"url":"u","password":"` + intCanary + `"}}`, nil},
		{http.MethodPost, "/api/integrations/test", `{"kind":"fake-connect","id":"` + id + `","config":{"url":"u"}}`, nil},
		{http.MethodPut, "/api/integrations/" + id, upd, []string{"If-Match", "1"}},
		{http.MethodPost, "/api/integrations/" + id + "/disable", "", nil},
		{http.MethodPost, "/api/integrations/" + id + "/enable", "", nil},
		{http.MethodGet, "/api/integrations/" + id + "/audit", "", nil},
		{http.MethodGet, "/api/integrations/" + id + "/secrets", "", nil},
		{http.MethodGet, "/api/integrations/" + id + "/secrets/keys?ref=secret%3A%2F%2Ffiles%2Fx", "", nil},
		{http.MethodGet, "/api/integrations/alerts/grafana/setup", "", nil},
		{http.MethodGet, "/api/me", "", nil},
	}
	for _, r := range reqs {
		status, body := e.call(t, r.method, r.path, "arjun", r.body, r.hdr...)
		if status >= 500 {
			t.Fatalf("%s %s: %d %s", r.method, r.path, status, body)
		}
		if strings.Contains(body, intCanary) {
			t.Fatalf("%s %s leaks the secret: %s", r.method, r.path, body)
		}
	}
	// The secrets route on a row that is no secret manager is a 400.
	status, body := e.call(t, http.MethodGet, "/api/integrations/"+id+"/secrets", "arjun", "")
	wantCode(t, status, body, http.StatusBadRequest, "validation")

	// Rotate hands out a secret once; setup never shows it.
	status, body = e.call(t, http.MethodPost, "/api/integrations/alerts/grafana/rotate", "arjun", "")
	wantCode(t, status, body, http.StatusOK, "")
	var rot struct {
		Secret    string `json:"secret"`
		SecretSet bool   `json:"secret_set"`
	}
	if err := json.Unmarshal([]byte(body), &rot); err != nil || rot.Secret == "" || !rot.SecretSet {
		t.Fatalf("rotate: %s", body)
	}
	_, body = e.call(t, http.MethodGet, "/api/integrations/alerts/grafana/setup", "arjun", "")
	if strings.Contains(body, rot.Secret) || strings.Contains(body, `"secret":`) {
		t.Fatalf("setup shows the rotated secret: %s", body)
	}
	_, body = e.call(t, http.MethodGet, "/api/integrations", "arjun", "")
	if strings.Contains(body, rot.Secret) {
		t.Fatalf("list shows the rotated secret: %s", body)
	}

	// A reference is not a secret: it is shown, and clearing works with null.
	status, body = e.call(t, http.MethodPut, "/api/integrations/"+id, "arjun",
		`{"kind":"fake-connect","config":{"url":"http://c3:8083"},"secrets":{"password":{"$ref":"secret://files/x"}}}`, "If-Match", "4")
	// No resolver is configured, so the reference fails its check: 422.
	wantCode(t, status, body, http.StatusUnprocessableEntity, "checks_failed")
	status, body = e.call(t, http.MethodPut, "/api/integrations/"+id, "arjun",
		`{"kind":"fake-connect","config":{"url":"http://c3:8083"},"secrets":{"password":null}}`, "If-Match", "4")
	wantCode(t, status, body, http.StatusOK, "")
	if !strings.Contains(body, `"secrets_set":[]`) {
		t.Fatalf("null should clear the secret: %s", body)
	}

	status, body = e.call(t, http.MethodDelete, "/api/integrations/"+id, "arjun", "")
	if status != http.StatusNoContent {
		t.Fatalf("delete: %d %s", status, body)
	}
}

func TestIntegrationChangePostsInOpenRooms(t *testing.T) {
	e := newIntEnv(t, intEnvOpts{})
	arjun, _ := e.room.LookupUser("arjun")
	snap, err := e.room.CreateRoom(context.Background(), arjun, "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	createFake(t, e, "http://connect:8083")
	evs, _, err := e.room.Events(context.Background(), snap.ID, 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	var found string
	for _, ev := range evs {
		if ev.Type == events.AgentMessage && ev.Actor.Kind == events.ActorSystem {
			found = string(ev.Payload)
		}
	}
	if !strings.Contains(found, "Kafka Connect integration connected by arjun (password, url)") {
		t.Fatalf("no system message in the open room: %q", found)
	}
	if strings.Contains(found, intCanary) {
		t.Fatal("the room message leaks the secret")
	}
}

func TestParseAdmins(t *testing.T) {
	for _, c := range []struct {
		in   string
		n    int
		fail bool
	}{
		{"", 0, false},
		{"arjun", 1, false},
		{" user:arjun , group:platform ,", 2, false},
		{"team:x", 0, true},
		{"group:", 0, true},
	} {
		got, err := httpapi.ParseAdmins(c.in)
		if (err != nil) != c.fail || len(got) != c.n {
			t.Fatalf("ParseAdmins(%q) = %v, %v", c.in, got, err)
		}
	}
}
