package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// The password inside a JDBC URL and a credential key's value: neither may
// be stored or answered (X5c TC2).
const cvSecret = "hunter2-jdbc-pw"

// newConfigVersionsServer serves the usage fixtures (orders lists connector
// c1; arjun and priya own layers, nolayers owns none, audrey is an auditor)
// with the config-versions store wired when withStore.
func newConfigVersionsServer(t *testing.T, withStore bool) (*httptest.Server, *memstore.Store) {
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
	if withStore {
		opts = append(opts, httpapi.WithConfigVersions(st))
	}
	api := httpapi.New(svc, st, nil, opts...)
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})
	return srv, st
}

// cvConfigs are two redacted configs of c1, as the watcher stores them:
// one key changed, one added, one removed, a credential key added.
func cvConfigs() (from, to map[string]string) {
	raw := map[string]string{
		"connector.class":       "io.debezium.connector.postgresql.PostgresConnector",
		"decimal.handling.mode": "precise",
		"connection.url":        "jdbc:postgresql://db:5432/orders?user=app&password=" + cvSecret,
		"database.password":     cvSecret,
		"snapshot.mode":         "initial",
	}
	from = actions.RedactConfig(raw)
	raw["decimal.handling.mode"] = "double"
	raw["transforms"] = "unwrap"
	raw["database.sslpassword"] = cvSecret
	delete(raw, "snapshot.mode")
	to = actions.RedactConfig(raw)
	return from, to
}

func recordCV(t *testing.T, st *memstore.Store, connector string, cfg map[string]string, at time.Time, keys ...string) string {
	t.Helper()
	if keys == nil {
		keys = []string{}
	}
	if _, err := st.RecordConfigVersion(context.Background(), clients.ConnectorConfigVersion{
		PipelineID: "orders", Connector: connector, At: at, ChangedKeys: keys, Config: cfg}); err != nil {
		t.Fatal(err)
	}
	return actions.ConfigHash(cfg)
}

func body(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// E1, E2: the list is newest first with key names only, for any logged-in
// user; limit is checked; a connector the watcher recorded but owners.yaml
// does not list is known; the 404s name the pipeline or the connector.
func TestConfigVersionsList(t *testing.T) {
	srv, st := newConfigVersionsServer(t, true)
	url := func(pid, name, q string) string {
		return srv.URL + "/api/pipelines/" + pid + "/connectors/" + name + "/config-versions" + q
	}
	// A known connector with no version yet.
	resp := doJSON(t, http.MethodGet, url("orders", "c1", ""), "nolayers", "")
	wantStatus(t, resp, http.StatusOK)
	if got := body(t, resp); strings.TrimSpace(got) != "[]" {
		t.Fatalf("no versions = %s", got)
	}

	from, to := cvConfigs()
	t0 := time.Date(2026, 10, 20, 14, 3, 0, 0, time.UTC)
	h1 := recordCV(t, st, "c1", from, t0)
	h2 := recordCV(t, st, "c1", to, t0.Add(time.Hour), "database.sslpassword", "decimal.handling.mode", "snapshot.mode", "transforms")
	recordCV(t, st, "discovered", from, t0)

	resp = doJSON(t, http.MethodGet, url("orders", "c1", ""), "nolayers", "")
	wantStatus(t, resp, http.StatusOK)
	raw := body(t, resp)
	if strings.Contains(raw, cvSecret) || strings.Contains(raw, "precise") || strings.Contains(raw, `"config"`) {
		t.Fatalf("the list shows values: %s", raw)
	}
	var rows []map[string]any
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0]["hash"] != h2 || rows[1]["hash"] != h1 || len(rows[0]) != 4 {
		t.Fatalf("rows = %v", rows)
	}
	keys, _ := rows[0]["changed_keys"].([]any)
	if len(keys) != 4 || keys[0] != events.SecretKeyChanged || rows[0]["by"] != "" || rows[0]["at"] != "2026-10-20T15:03:00Z" {
		t.Fatalf("newest = %v", rows[0])
	}
	if k, _ := rows[1]["changed_keys"].([]any); k == nil || len(k) != 0 {
		t.Fatalf("first version's changed_keys = %v, want []", rows[1]["changed_keys"])
	}

	resp = doJSON(t, http.MethodGet, url("orders", "c1", "?limit=1"), "nolayers", "")
	wantStatus(t, resp, http.StatusOK)
	if got := decode[[]map[string]any](t, resp); len(got) != 1 || got[0]["hash"] != h2 {
		t.Fatalf("limit 1 = %v", got)
	}
	for _, bad := range []string{"0", "201", "-1", "x", "1.5"} {
		wantErr(t, doJSON(t, http.MethodGet, url("orders", "c1", "?limit="+bad), "nolayers", ""), http.StatusBadRequest, "validation")
	}
	resp = doJSON(t, http.MethodGet, url("orders", "discovered", ""), "nolayers", "")
	wantStatus(t, resp, http.StatusOK)
	if got := decode[[]map[string]any](t, resp); len(got) != 1 {
		t.Fatalf("a recorded connector = %v", got)
	}

	for _, c := range []struct{ pid, name, msg string }{
		{"nope", "c1", "pipeline not found"},
		{"orders", "c2", "connector not found"}, // the shadowed pipeline's
		{"orders", "zzz", "connector not found"},
		{"orders", "a%01b", "connector not found"},
		{"orders", strings.Repeat("c", 300), "connector not found"},
	} {
		resp := doJSON(t, http.MethodGet, url(c.pid, c.name, "?limit=0"), "nolayers", "")
		wantStatus(t, resp, http.StatusNotFound)
		if code, msg := errCode(t, resp); code != "not_found" || msg != c.msg {
			t.Fatalf("%s/%s: %s %q, want not_found %q", c.pid, c.name, code, msg, c.msg)
		}
	}
}

// X5c TC1, TC2, TC5: the diff lists exactly the changed keys with their
// stored values (a credential key by name, "<redacted>"; the password in the
// JDBC URL masked); from/to follow the query; a == b is no key; an unknown
// hash is 404; a malformed diff is 400.
func TestConfigVersionsDiff(t *testing.T) {
	srv, st := newConfigVersionsServer(t, true)
	url := func(pid, name, q string) string {
		return srv.URL + "/api/pipelines/" + pid + "/connectors/" + name + "/config-versions" + q
	}
	from, to := cvConfigs()
	t0 := time.Date(2026, 10, 20, 14, 3, 0, 0, time.UTC)
	a := recordCV(t, st, "c1", from, t0)
	b := recordCV(t, st, "c1", to, t0.Add(time.Hour), "decimal.handling.mode")

	resp := doJSON(t, http.MethodGet, url("orders", "c1", "?diff="+a+".."+b+"&limit=0"), "arjun", "")
	wantStatus(t, resp, http.StatusOK)
	raw := body(t, resp)
	if strings.Contains(raw, cvSecret) {
		t.Fatalf("the diff shows a secret: %s", raw)
	}
	var d struct {
		Connector string `json:"connector"`
		From      struct {
			Hash string    `json:"hash"`
			At   time.Time `json:"at"`
		} `json:"from"`
		To struct {
			Hash string `json:"hash"`
		} `json:"to"`
		Keys []map[string]string `json:"keys"`
	}
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatal(err)
	}
	if d.Connector != "c1" || d.From.Hash != a || !d.From.At.Equal(t0) || d.To.Hash != b {
		t.Fatalf("diff ends = %+v", d)
	}
	want := []map[string]string{
		{"key": "database.sslpassword", "change": "added", "new": actions.RedactedValue},
		{"key": "decimal.handling.mode", "change": "changed", "old": "precise", "new": "double"},
		{"key": "snapshot.mode", "change": "removed", "old": "initial"},
		{"key": "transforms", "change": "added", "new": "unwrap"},
	}
	if len(d.Keys) != len(want) {
		t.Fatalf("keys = %v, want %v", d.Keys, want)
	}
	for i := range want {
		if len(d.Keys[i]) != len(want[i]) {
			t.Fatalf("key %d = %v, want %v", i, d.Keys[i], want[i])
		}
		for k, v := range want[i] {
			if d.Keys[i][k] != v {
				t.Fatalf("key %d = %v, want %v", i, d.Keys[i], want[i])
			}
		}
	}
	// The stored form holds no secret either (TC2).
	for _, h := range []string{a, b} {
		v, err := st.ConfigVersion(context.Background(), "orders", "c1", h)
		if err != nil {
			t.Fatal(err)
		}
		for k, val := range v.Config {
			if strings.Contains(val, cvSecret) {
				t.Fatalf("stored %s = %q", k, val)
			}
		}
	}

	// Reversed: from is always a.
	resp = doJSON(t, http.MethodGet, url("orders", "c1", "?diff="+b+".."+a), "audrey", "")
	wantStatus(t, resp, http.StatusOK)
	rev := decode[map[string]any](t, resp)
	if rev["from"].(map[string]any)["hash"] != b || len(rev["keys"].([]any)) != 4 {
		t.Fatalf("reversed = %v", rev)
	}
	resp = doJSON(t, http.MethodGet, url("orders", "c1", "?diff="+a+".."+a), "priya", "")
	wantStatus(t, resp, http.StatusOK)
	if same := decode[map[string]any](t, resp); same["keys"] == nil || len(same["keys"].([]any)) != 0 {
		t.Fatalf("a..a = %v", same)
	}
	// Upper-case hex names the same version.
	wantStatus(t, doJSON(t, http.MethodGet, url("orders", "c1", "?diff="+strings.ToUpper(a)+".."+b), "arjun", ""), http.StatusOK)

	unknown := strings.Repeat("0", 64)
	for _, q := range []string{unknown + ".." + b, a + ".." + unknown} {
		resp := doJSON(t, http.MethodGet, url("orders", "c1", "?diff="+q), "arjun", "")
		wantStatus(t, resp, http.StatusNotFound)
		if _, msg := errCode(t, resp); msg != "version not found" {
			t.Fatalf("unknown hash: %q", msg)
		}
	}
	// A hash of another connector is no version of this one.
	other := recordCV(t, st, "discovered", map[string]string{"x": "1"}, t0)
	wantErr(t, doJSON(t, http.MethodGet, url("orders", "c1", "?diff="+a+".."+other), "arjun", ""), http.StatusNotFound, "not_found")

	for _, bad := range []string{"", a, a + "." + b, a + ".." + b[:63], a + ".." + b + "0", a + "...." + b, strings.Repeat("g", 64) + ".." + b} {
		wantErr(t, doJSON(t, http.MethodGet, url("orders", "c1", "?diff="+bad), "arjun", ""), http.StatusBadRequest, "validation")
	}
}

// TC4 (role) and the check order: 404s first, then the role (403
// not_allowed for a logged-in user who owns no layer), then 400, then the
// version lookup.
func TestConfigVersionsDiffRoleAndOrder(t *testing.T) {
	srv, _ := newConfigVersionsServer(t, true)
	base := srv.URL + "/api/pipelines/"
	ok := strings.Repeat("a", 64) + ".." + strings.Repeat("b", 64)
	wantErr(t, doJSON(t, http.MethodGet, base+"nope/connectors/c1/config-versions?diff=bad", "nolayers", ""), http.StatusNotFound, "not_found")
	wantErr(t, doJSON(t, http.MethodGet, base+"orders/connectors/zzz/config-versions?diff=bad", "nolayers", ""), http.StatusNotFound, "not_found")
	wantErr(t, doJSON(t, http.MethodGet, base+"orders/connectors/c1/config-versions?diff=bad", "nolayers", ""), http.StatusForbidden, "not_allowed")
	wantErr(t, doJSON(t, http.MethodGet, base+"orders/connectors/c1/config-versions?diff="+ok, "nolayers", ""), http.StatusForbidden, "not_allowed")
	wantErr(t, doJSON(t, http.MethodGet, base+"orders/connectors/c1/config-versions?diff=bad", "kabir", ""), http.StatusBadRequest, "validation")
	wantErr(t, doJSON(t, http.MethodGet, base+"orders/connectors/c1/config-versions?diff="+ok, "kabir", ""), http.StatusNotFound, "not_found")
	// The list stays open to the same user.
	wantStatus(t, doJSON(t, http.MethodGet, base+"orders/connectors/c1/config-versions", "nolayers", ""), http.StatusOK)
}

// Without a store: the list of a listed connector is [], a diff is 404.
func TestConfigVersionsNoStore(t *testing.T) {
	srv, _ := newConfigVersionsServer(t, false)
	base := srv.URL + "/api/pipelines/orders/connectors/"
	resp := doJSON(t, http.MethodGet, base+"c1/config-versions", "arjun", "")
	wantStatus(t, resp, http.StatusOK)
	if got := body(t, resp); strings.TrimSpace(got) != "[]" {
		t.Fatalf("list = %s", got)
	}
	wantErr(t, doJSON(t, http.MethodGet, base+"zzz/config-versions", "arjun", ""), http.StatusNotFound, "not_found")
	wantErr(t, doJSON(t, http.MethodGet, base+"c1/config-versions?diff="+strings.Repeat("a", 64)+".."+strings.Repeat("b", 64), "arjun", ""), http.StatusNotFound, "not_found")
}

// TC4 (tenancy): another tenant's pipeline answers exactly like an unknown
// one on both forms; the tenant's own member reaches it.
func TestConfigVersionsTenants(t *testing.T) {
	e := newTenancyEnv(t, true)
	diff := "?diff=" + strings.Repeat("a", 64) + ".." + strings.Repeat("b", 64)
	for _, q := range []string{"", diff} {
		missing := e.mustDo(t, "mallory", http.MethodGet, "/api/pipelines/zzz/connectors/pb-c/config-versions"+q, "", http.StatusNotFound)
		for _, user := range []string{"alice", "mallory"} {
			if got := e.mustDo(t, user, http.MethodGet, "/api/pipelines/pb/connectors/pb-c/config-versions"+q, "", http.StatusNotFound); got != missing {
				t.Fatalf("%s %q: %s, want the unknown pipeline's %s", user, q, got, missing)
			}
		}
	}
	e.mustDo(t, "bob", http.MethodGet, "/api/pipelines/pb/connectors/pb-c/config-versions", "", http.StatusOK)
	// bob owns pb's layers: past the role check, the (absent) store has no version.
	if got := e.mustDo(t, "bob", http.MethodGet, "/api/pipelines/pb/connectors/pb-c/config-versions"+diff, "", http.StatusNotFound); !strings.Contains(got, "version not found") {
		t.Fatalf("bob's diff: %s", got)
	}
}

// A viewer reads the list but never a diff (values are shown there).
func TestConfigVersionsViewer(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	v := e.mustLogin(t, idpViewer)
	wantStatus(t, e.do(t, http.MethodGet, "/api/pipelines/orders/connectors/c1/config-versions", "", ck(v)), http.StatusOK)
	resp := e.do(t, http.MethodGet, "/api/pipelines/orders/connectors/c1/config-versions?diff="+strings.Repeat("a", 64)+".."+strings.Repeat("b", 64), "", ck(v))
	wantErr(t, resp, http.StatusForbidden, "not_allowed")
}
