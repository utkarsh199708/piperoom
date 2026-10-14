package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/auth"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

func TestAuthUsersDevPicker(t *testing.T) {
	const usersYAML = `users:
  - {id: zed, name: Zed, layers: [kafka], email: zed@example.com, admin: true}
  - {id: amy, name: Amy}
`
	users, err := room.ParseUsers([]byte(usersYAML))
	if err != nil {
		t.Fatal(err)
	}
	newEnv := func(solo string) *httptest.Server {
		st := memstore.New()
		svc := room.New(room.Deps{Store: st, Users: users, SoloUser: solo,
			Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
				<-ctx.Done()
				return nil
			}})
		api := httpapi.New(svc, st, nil)
		srv := httptest.NewServer(api)
		t.Cleanup(func() { srv.Close(); api.Close() })
		return srv
	}

	// No session, no X-User: the picker is read before login. File order,
	// only id/name/layers, layers [] when none.
	resp, err := http.Get(newEnv("").URL + "/auth/users")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	wantStatus(t, resp, http.StatusOK)
	var raw []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{
		{"id": "zed", "name": "Zed", "layers": []any{"kafka"}},
		{"id": "amy", "name": "Amy", "layers": []any{}},
	}
	if !reflect.DeepEqual(raw, want) {
		t.Fatalf("picker = %v, want %v", raw, want)
	}

	// Solo: just that user.
	resp2, err := http.Get(newEnv("amy").URL + "/auth/users")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	var solo []room.PickerUser
	if err := json.NewDecoder(resp2.Body).Decode(&solo); err != nil {
		t.Fatal(err)
	}
	if len(solo) != 1 || solo[0].ID != "amy" {
		t.Fatalf("solo picker = %+v", solo)
	}
}

func TestAuthUsersNotMountedInOIDC(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	resp := e.do(t, http.MethodGet, "/auth/users", "")
	wantStatus(t, resp, http.StatusNotFound)
	if code, _ := errCode(t, resp); code != "not_found" {
		t.Fatalf("code %q", code)
	}
}

func TestPipelinesList(t *testing.T) {
	type pipe struct {
		ID     string `json:"id"`
		Title  string `json:"title"`
		Shadow bool   `json:"shadow"`
	}
	list := func(t *testing.T, e *tenancyEnv, user string) []pipe {
		t.Helper()
		var out []pipe
		if err := json.Unmarshal([]byte(e.mustDo(t, user, http.MethodGet, "/api/pipelines", "", http.StatusOK)), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	pa, pb, pbs := pipe{ID: "pa"}, pipe{ID: "pb"}, pipe{ID: "pbs", Shadow: true}

	t.Run("tenants", func(t *testing.T) {
		e := newTenancyEnv(t, true)
		for user, want := range map[string][]pipe{
			"alice":   {pa},
			"ann":     {pa},
			"ben":     {pb, pbs},
			"mallory": {},
		} {
			if got := list(t, e, user); !reflect.DeepEqual(got, want) {
				t.Errorf("%s sees %+v, want %+v", user, got, want)
			}
		}
	})
	t.Run("no tenants", func(t *testing.T) {
		e := newTenancyEnv(t, false)
		if got := list(t, e, "mallory"); !reflect.DeepEqual(got, []pipe{pa, pb, pbs}) {
			t.Fatalf("got %+v", got)
		}
	})
	t.Run("title, no extra fields, signed out", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "owners.yaml")
		src := "pipelines:\n  - id: orders\n    title: Orders CDC\n    topic_prefix: pg\n    source_tables: [public.orders]\n    connectors: [c1]\nlayers:\n  postgres: priya\n"
		if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
		own, err := owners.Load(path)
		if err != nil {
			t.Fatal(err)
		}
		st := memstore.New()
		svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Owners: own,
			Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
				<-ctx.Done()
				return nil
			}})
		api := httpapi.New(svc, st, nil, httpapi.WithSessions(auth.NewMemStore()))
		srv := httptest.NewServer(api)
		t.Cleanup(func() { srv.Close(); api.Close() })

		resp := doJSON(t, http.MethodGet, srv.URL+"/api/pipelines", "kabir", "")
		defer resp.Body.Close()
		wantStatus(t, resp, http.StatusOK)
		var raw []map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			t.Fatal(err)
		}
		want := []map[string]any{{"id": "orders", "title": "Orders CDC", "shadow": false}}
		if !reflect.DeepEqual(raw, want) {
			t.Fatalf("got %v", raw)
		}
		out := doJSON(t, http.MethodGet, srv.URL+"/api/pipelines", "", "")
		defer out.Body.Close()
		wantStatus(t, out, http.StatusUnauthorized)
	})
	t.Run("no owners config", func(t *testing.T) {
		srv, _ := newServer(t)
		resp := doJSON(t, http.MethodGet, srv.URL+"/api/pipelines", "priya", "")
		defer resp.Body.Close()
		var b strings.Builder
		var raw json.RawMessage
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			t.Fatal(err)
		}
		b.Write(raw)
		if b.String() != "[]" {
			t.Fatalf("got %s, want []", b.String())
		}
	})
}
