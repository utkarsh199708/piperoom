package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"piperoom/internal/integrations/spec"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// errService is a spec.Service whose every call fails with err, so each
// sentinel can be pushed through the handlers' error mapping.
type errService struct{ err error }

var _ spec.Service = errService{}

func (f errService) Catalog() []spec.Kind { return nil }
func (f errService) SetupScript(string, map[string]any) (string, error) {
	return "", f.err
}
func (f errService) List(context.Context, string) ([]spec.View, error) { return nil, f.err }
func (f errService) Test(context.Context, string, string, spec.Input) ([]spec.Check, error) {
	return nil, f.err
}
func (f errService) Create(context.Context, string, string, spec.Input) (spec.View, []spec.Check, error) {
	return spec.View{}, nil, f.err
}
func (f errService) Update(context.Context, string, string, string, int64, spec.Input) (spec.View, []spec.Check, error) {
	return spec.View{}, nil, f.err
}
func (f errService) SetEnabled(context.Context, string, string, string, bool) (spec.View, error) {
	return spec.View{}, f.err
}
func (f errService) Delete(context.Context, string, string, string) error { return f.err }
func (f errService) Audit(context.Context, string, string, int) ([]spec.AuditEntry, error) {
	return nil, f.err
}
func (f errService) AlertSetup(context.Context, string, string) (spec.AlertSetup, error) {
	return spec.AlertSetup{}, f.err
}
func (f errService) RotateAlertSecret(context.Context, string, string, string) (spec.AlertSetup, netsec.Secret, error) {
	if f.err != nil {
		return spec.AlertSetup{}, "", f.err
	}
	return spec.AlertSetup{Source: "grafana", SecretSet: true}, netsec.Secret("new-secret"), nil
}
func (f errService) SecretNames(context.Context, string, string, string) ([]string, error) {
	return nil, f.err
}
func (f errService) SecretKeys(context.Context, string, string, string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return []string{"password", "username"}, nil
}

// newSvcEnv serves the integrations routes over svc, with arjun and the
// platform group as admins.
func newSvcEnv(t *testing.T, svc spec.Service, restart bool) *intEnv {
	t.Helper()
	st := memstore.New()
	sessions := auth.NewMemStore()
	rs := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), ViewerGroups: []string{"watchers"}, Sessions: sessions,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, nil
		},
	})
	admins, err := httpapi.ParseAdmins("arjun, group:platform")
	if err != nil {
		t.Fatal(err)
	}
	api := httpapi.New(rs, st, nil, httpapi.WithAuth(auth.Default()), httpapi.WithSessions(sessions),
		httpapi.WithIntegrations(httpapi.Integrations{Service: svc, Admins: admins, AlertRestartRequired: restart}))
	srv := httptest.NewServer(api)
	t.Cleanup(func() { srv.Close(); api.Close() })
	return &intEnv{srv: srv, room: rs, sessions: sessions}
}

// TestIntegrationsSentinelErrors checks the status and code of every
// service sentinel the handlers map (docs/api.md "Errors").
func TestIntegrationsSentinelErrors(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		method     string
		path, body string
		status     int
		code       string
	}{
		{"rate limited", fmt.Errorf("wrapped: %w", spec.ErrRateLimited), http.MethodPost, "/api/integrations/test", `{"kind":"k","config":{}}`, http.StatusTooManyRequests, "rate_limited"},
		{"duplicate", spec.ErrDuplicate, http.MethodPost, "/api/integrations", `{"kind":"k","config":{}}`, http.StatusConflict, "duplicate"},
		{"managed by env", spec.ErrManagedByEnv, http.MethodPost, "/api/integrations", `{"kind":"k","config":{}}`, http.StatusConflict, "managed_by_env"},
		{"managed by env rotate", spec.ErrManagedByEnv, http.MethodPost, "/api/integrations/alerts/grafana/rotate", "", http.StatusConflict, "managed_by_env"},
		{"version conflict", spec.ErrVersionConflict, http.MethodPut, "/api/integrations/int_x", `{"kind":"k","config":{}}`, http.StatusConflict, "version_conflict"},
		{"secret key missing", spec.ErrSecretKeyMissing, http.MethodPost, "/api/integrations", `{"kind":"k","config":{}}`, http.StatusConflict, "secret_key_missing"},
		{"provider unreachable names", spec.ErrProviderUnreachable, http.MethodGet, "/api/integrations/int_x/secrets?prefix=db", "", http.StatusBadGateway, "provider_unreachable"},
		{"provider unreachable keys", spec.ErrProviderUnreachable, http.MethodGet, "/api/integrations/int_x/secrets/keys?ref=secret%3A%2F%2Fv%2Fa", "", http.StatusBadGateway, "provider_unreachable"},
		{"not secret manager", spec.ErrNotSecretManager, http.MethodGet, "/api/integrations/int_x/secrets", "", http.StatusBadRequest, "validation"},
		{"unknown kind", spec.ErrUnknownKind, http.MethodPost, "/api/integrations/test", `{"kind":"nope","config":{}}`, http.StatusBadRequest, "validation"},
		{"unknown alert source", spec.ErrUnknownAlertSource, http.MethodGet, "/api/integrations/alerts/nagios/setup", "", http.StatusNotFound, "not_found"},
		{"not found", spec.ErrNotFound, http.MethodPost, "/api/integrations/int_x/disable", "", http.StatusNotFound, "not_found"},
		{"unexpected", errors.New("dial tcp 10.0.0.7:5432: refused"), http.MethodGet, "/api/integrations", "", http.StatusInternalServerError, "internal_error"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newSvcEnv(t, errService{err: c.err}, false)
			var hdr []string
			if c.method == http.MethodPut {
				hdr = []string{"If-Match", "3"}
			}
			status, body := e.call(t, c.method, c.path, "arjun", c.body, hdr...)
			wantCode(t, status, body, c.status, c.code)
			if strings.Contains(body, "10.0.0.7") {
				t.Fatalf("an internal error leaks its cause: %s", body)
			}
		})
	}
}

// TestIntegrationsSecretKeysRoute covers the key picker's success path and
// its missing ref.
func TestIntegrationsSecretKeysRoute(t *testing.T) {
	e := newSvcEnv(t, errService{}, false)
	status, body := e.call(t, http.MethodGet, "/api/integrations/int_x/secrets/keys?ref=secret%3A%2F%2Fv%2Fa", "arjun", "")
	wantCode(t, status, body, http.StatusOK, "")
	if !strings.Contains(body, `"keys":["password","username"]`) {
		t.Fatalf("keys: %s", body)
	}
	status, body = e.call(t, http.MethodGet, "/api/integrations/int_x/secrets/keys", "arjun", "")
	wantCode(t, status, body, http.StatusBadRequest, "validation")

	// ErrListUnsupported is no error for the picker: it falls back to typing.
	e = newSvcEnv(t, errService{err: spec.ErrListUnsupported}, false)
	status, body = e.call(t, http.MethodGet, "/api/integrations/int_x/secrets/keys?ref=secret%3A%2F%2Fv%2Fa", "arjun", "")
	wantCode(t, status, body, http.StatusOK, "")
	if !strings.Contains(body, `"keys":[]`) {
		t.Fatalf("keys: %s", body)
	}
	status, body = e.call(t, http.MethodGet, "/api/integrations/int_x/secrets", "arjun", "")
	wantCode(t, status, body, http.StatusOK, "")
	if !strings.Contains(body, `"list_supported":false`) {
		t.Fatalf("names: %s", body)
	}
}

// TestIntegrationsRotateRestartRequired: while alert secrets are read only at
// start, the rotate response says a restart is needed.
func TestIntegrationsRotateRestartRequired(t *testing.T) {
	for _, restart := range []bool{true, false} {
		e := newSvcEnv(t, errService{}, restart)
		status, body := e.call(t, http.MethodPost, "/api/integrations/alerts/grafana/rotate", "arjun", "")
		wantCode(t, status, body, http.StatusOK, "")
		var got map[string]any
		if err := json.Unmarshal([]byte(body), &got); err != nil {
			t.Fatal(err)
		}
		v, has := got["restart_required"]
		if restart && v != true {
			t.Fatalf("restart_required should be true: %s", body)
		}
		if !restart && has {
			t.Fatalf("restart_required should be absent: %s", body)
		}
	}
}

// TestIntegrationsGroupAdmin: a member matched by a group: entry of
// PIPEROOM_ADMINS is an admin.
func TestIntegrationsGroupAdmin(t *testing.T) {
	e := newSvcEnv(t, errService{}, false)
	cookie := func(id string, groups []string) string {
		token, hash, err := auth.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now()
		if err := e.sessions.Create(context.Background(), auth.Session{
			ID: room.NewID("ses_"), TokenHash: hash, UserID: id, Name: id, Groups: groups,
			Method: auth.MethodDev, CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
		return auth.DevSessionCookie + "=" + token
	}
	plat := cookie("meera", []string{"platform"})
	status, body := e.call(t, http.MethodGet, "/api/integrations", "", "", "Cookie", plat)
	wantCode(t, status, body, http.StatusOK, "")
	if !meAdmin(t, e, "", "Cookie", plat) {
		t.Fatal("a member of group platform should be admin")
	}
	other := cookie("kabir", []string{"sales"})
	status, body = e.call(t, http.MethodGet, "/api/integrations", "", "", "Cookie", other)
	wantCode(t, status, body, http.StatusForbidden, "not_admin")
	if meAdmin(t, e, "", "Cookie", other) {
		t.Fatal("kabir is no admin")
	}
}
