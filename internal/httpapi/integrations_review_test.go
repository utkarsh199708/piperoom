package httpapi_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/integrations"
	"piperoom/internal/integrations/spec"
)

// TestIntegrationsSecretErrorsAreNot500 covers E5, E6 and E10: refusals an
// admin can act on answer a documented 4xx (or 502), never internal_error.
func TestIntegrationsSecretErrorsAreNot500(t *testing.T) {
	ref := func(reason spec.SecretRefReason) error {
		return fmt.Errorf("integrations: int_x: %w", &spec.SecretRefError{Field: "password", Reason: reason, Err: errors.New("cause")})
	}
	cases := []struct {
		name         string
		err          error
		method, path string
		status       int
		code         string
	}{
		{"key mismatch enable", fmt.Errorf("integrations: int_x: %w", integrations.ErrOpen), http.MethodPost, "/api/integrations/int_x/enable", http.StatusConflict, "secret_key_mismatch"},
		{"unknown provider", ref(spec.SecretRefUnknownProvider), http.MethodPost, "/api/integrations/int_x/enable", http.StatusBadRequest, "validation"},
		{"denied", ref(spec.SecretRefDenied), http.MethodPost, "/api/integrations/int_x/enable", http.StatusForbidden, "secret_access_denied"},
		{"not found", ref(spec.SecretRefNotFound), http.MethodGet, "/api/integrations/int_x/secrets/keys?ref=secret%3A%2F%2Fv%2Fa", http.StatusNotFound, "not_found"},
		{"invalid prefix", ref(spec.SecretRefInvalid), http.MethodGet, "/api/integrations/int_x/secrets?prefix=a/../b", http.StatusBadRequest, "validation"},
		{"unreachable", ref(spec.SecretRefUnreachable), http.MethodPost, "/api/integrations/int_x/enable", http.StatusBadGateway, "provider_unreachable"},
		{"in use", &spec.InUseError{Dependents: []spec.Dependent{{ID: "int_dep", Kind: "postgres-source", Title: "PostgreSQL"}}}, http.MethodDelete, "/api/integrations/int_x", http.StatusConflict, "in_use"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newSvcEnv(t, errService{err: c.err}, false)
			status, body := e.call(t, c.method, c.path, "arjun", "")
			wantCode(t, status, body, c.status, c.code)
			if c.code == "in_use" && !strings.Contains(body, `"dependents":[{"id":"int_dep"`) {
				t.Fatalf("dependents missing: %s", body)
			}
			if c.code == "validation" && !strings.Contains(body, `"field":"password"`) {
				t.Fatalf("field missing: %s", body)
			}
		})
	}
}

// TestIntegrationsEnvRowIfMatchZero covers E20: an env row's advertised
// version 0 reaches the service and gets managed_by_env.
func TestIntegrationsEnvRowIfMatchZero(t *testing.T) {
	e := newIntEnv(t, intEnvOpts{env: integrations.Env{Categories: map[spec.Category]integrations.EnvCategory{
		spec.CategoryConnect: {Kind: "fake-connect"},
	}}})
	upd := `{"kind":"fake-connect","config":{"url":"http://c:8083"}}`
	status, body := e.call(t, http.MethodPut, "/api/integrations/env_connect", "arjun", upd, "If-Match", "0")
	wantCode(t, status, body, http.StatusConflict, "managed_by_env")
	status, body = e.call(t, http.MethodPut, "/api/integrations/env_connect", "arjun", upd, "If-Match", "-1")
	wantCode(t, status, body, http.StatusBadRequest, "validation")
}

// TestIntegrationsStatusRoute covers E22: the page can tell that typed-in
// secrets cannot be saved before the admin types one.
func TestIntegrationsStatusRoute(t *testing.T) {
	for _, noKey := range []bool{false, true} {
		e := newIntEnv(t, intEnvOpts{noKey: noKey})
		status, body := e.call(t, http.MethodGet, "/api/integrations/status", "arjun", "")
		wantCode(t, status, body, http.StatusOK, "")
		var got map[string]bool
		if err := json.Unmarshal([]byte(body), &got); err != nil {
			t.Fatal(err)
		}
		if v, ok := got["secret_key_set"]; !ok || v == noKey {
			t.Fatalf("noKey=%v: %s", noKey, body)
		}
		status, body = e.call(t, http.MethodGet, "/api/integrations/status", "priya", "")
		wantCode(t, status, body, http.StatusForbidden, "not_admin")
	}
}
