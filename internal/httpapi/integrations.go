package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

// The Integrations page API (docs/api.md "Integrations"). Handlers are thin:
// the rules live in spec.Service (internal/integrations). This file adds the
// admin rule, the workspace, If-Match, the body decoding and the error
// mapping, and every response goes through writeIntegrationJSON.

// Integrations configures the /api/integrations routes (WithIntegrations).
type Integrations struct {
	// Service is the integrations logic. nil = the routes are not mounted.
	Service spec.Service
	// Admins are the PIPEROOM_ADMINS principals (ParseAdmins).
	Admins owners.Principals
	// IsAdmin, when set, is an extra admin source (owners.yaml admins:).
	IsAdmin func(userID string, groups []string) bool
	// AlertRestartRequired tells the rotate response to say that a new alert
	// secret is accepted only after a server restart: the signed alert routes
	// read their secrets once, at start, until httpapi can swap them live.
	AlertRestartRequired bool
	// SecretKeySet is whether PIPEROOM_SECRET_KEY is set: without it typed-in
	// secrets cannot be saved (references still can), and the page says so
	// before the admin types one (GET /api/integrations/status).
	SecretKeySet bool
}

// WithIntegrations mounts /api/integrations/* and turns on the admin field of
// /api/me.
func WithIntegrations(cfg Integrations) Option {
	return func(s *Server) {
		if cfg.Service != nil {
			c := cfg
			s.ints = &c
		}
	}
}

// ParseAdmins parses PIPEROOM_ADMINS: a comma list of "<id>", "user:<id>" or
// "group:<name>". Blank entries are skipped; a bad entry is an error naming it.
func ParseAdmins(raw string) (owners.Principals, error) {
	var out owners.Principals
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		p, err := owners.ParsePrincipal(part)
		if err != nil {
			return nil, fmt.Errorf("httpapi: PIPEROOM_ADMINS: %w", err)
		}
		out = append(out, p)
	}
	return out, nil
}

// integrationsMaxAudit caps GET /{id}/audit?limit (docs/api.md).
const integrationsMaxAudit = 500

// workspaceFrom is the caller's workspace. Sessions carry no workspace yet:
// a self-hosted install is one workspace, so every caller is in "default".
// Hosted mode will read it from the session here, and only here; it is never
// taken from the URL or the body.
func workspaceFrom(context.Context) string { return spec.DefaultWorkspace }

// isAdmin reports whether u may change integrations, before the dev-mode
// rule: PIPEROOM_ADMINS, the extra source, or the solo user. A viewer is
// never an admin.
func (s *Server) isAdmin(ctx context.Context, u room.User) bool {
	if s.ints == nil || roleFrom(ctx) != room.RoleMember || u.ID == "" {
		return false
	}
	if _, ok := s.ints.Admins.Matches(u.ID, u.Groups); ok {
		return true
	}
	if s.ints.IsAdmin != nil && s.ints.IsAdmin(u.ID, u.Groups) {
		return true
	}
	return s.svc.IsSolo(u.ID)
}

// devAdminBlocked: in dev auth mode anyone can pick any user, so integration
// changes work only on a loopback listen (docs/design/integrations.md §7).
func (s *Server) devAdminBlocked() bool {
	return !s.authSettings.IsOIDC() && s.authSettings.DevNonLoopback
}

// meAdmin is /api/me's admin field.
func (s *Server) meAdmin(ctx context.Context, u room.User) bool {
	return !s.devAdminBlocked() && s.isAdmin(ctx, u)
}

// requireAdmin guards every integrations route except GET /catalog. The
// viewer guard has already answered non-GET requests of viewers.
func (s *Server) requireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.devAdminBlocked() {
			writeError(w, http.StatusForbidden, "admin_needs_oidc", "integration changes are off: dev sign-in on a non-loopback address; use PIPEROOM_AUTH_MODE=oidc")
			return
		}
		if !s.isAdmin(r.Context(), userFrom(r.Context())) {
			writeError(w, http.StatusForbidden, "not_admin", "only an admin can manage integrations")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// mountIntegrations registers the routes under /api/integrations.
func (s *Server) mountIntegrations(r chi.Router) {
	r.Get("/catalog", s.handleIntCatalog)
	r.Group(func(a chi.Router) {
		a.Use(s.requireAdmin)
		a.Post("/catalog/{kind}/setup-script", s.handleIntSetupScript)
		a.Get("/", s.handleIntList)
		a.Get("/status", s.handleIntStatus)
		a.Post("/", s.handleIntCreate)
		a.Post("/test", s.handleIntTest)
		a.Get("/alerts/{source}/setup", s.handleIntAlertSetup)
		a.Post("/alerts/{source}/rotate", s.handleIntAlertRotate)
		a.Put("/{id}", s.handleIntUpdate)
		a.Delete("/{id}", s.handleIntDelete)
		a.Post("/{id}/enable", s.handleIntEnable)
		a.Post("/{id}/disable", s.handleIntDisable)
		a.Get("/{id}/audit", s.handleIntAudit)
		a.Get("/{id}/secrets", s.handleIntSecretNames)
		a.Get("/{id}/secrets/keys", s.handleIntSecretKeys)
	})
}

// ---- encoding ----

// writeIntegrationJSON is the one encoder of every integrations response.
// Views are re-filtered against their kind first (sanitizeView), so a typed-in
// secret that reached a View by mistake is dropped here too.
func (s *Server) writeIntegrationJSON(w http.ResponseWriter, status int, v any) {
	switch x := v.(type) {
	case spec.View:
		v = s.sanitizeView(x)
	case []spec.View:
		out := make([]spec.View, len(x))
		for i := range x {
			out[i] = s.sanitizeView(x[i])
		}
		v = out
	case intWriteResponse:
		x.Integration = s.sanitizeView(x.Integration)
		v = x
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		s.log.Error("http: encode integrations response", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

// sanitizeView drops every config entry of a secret field that is not a
// {"$ref": "secret://…"} reference, and every entry of a field the kind does
// not have. Unknown kinds keep no config at all.
func (s *Server) sanitizeView(v spec.View) spec.View {
	fields := map[string]spec.Field{}
	for _, k := range s.ints.Service.Catalog() {
		if k.ID == v.Kind {
			for _, f := range k.Fields {
				fields[f.Name] = f
			}
			break
		}
	}
	cfg := map[string]any{}
	for name, val := range v.Config {
		f, ok := fields[name]
		if !ok {
			continue
		}
		if f.Type.IsSecret() {
			if ref, ok := refValue(val); ok {
				cfg[name] = map[string]any{spec.RefKey: ref}
			}
			continue
		}
		switch val.(type) {
		case string, bool, float64, json.Number:
			cfg[name] = val
		}
	}
	v.Config = cfg
	if v.SecretsSet == nil {
		v.SecretsSet = []string{}
	}
	if v.LastTest == nil {
		v.LastTest = []spec.Check{}
	}
	return v
}

// refValue reads {"$ref": "secret://…"}.
func refValue(v any) (string, bool) {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return "", false
	}
	ref, ok := m[spec.RefKey].(string)
	return ref, ok && strings.HasPrefix(ref, spec.RefPrefix)
}

// intWriteResponse is the body of create and update.
type intWriteResponse struct {
	Integration spec.View    `json:"integration"`
	Checks      []spec.Check `json:"checks"`
}

func nonNilChecks(c []spec.Check) []spec.Check {
	if c == nil {
		return []spec.Check{}
	}
	return c
}

// writeIntegrationErr maps service errors to docs/api.md codes. Messages
// are fixed text or a ValidationError message, which never quotes a value.
func (s *Server) writeIntegrationErr(w http.ResponseWriter, err error) {
	var ve *spec.ValidationError
	var cf *spec.ChecksFailedError
	var re *room.Error
	var iu *spec.InUseError
	var sre *spec.SecretRefError
	switch {
	case errors.As(err, &cf):
		s.writeIntegrationJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"error":  map[string]string{"code": "checks_failed", "message": "a connection check failed; nothing was saved"},
			"checks": nonNilChecks(cf.Checks),
		})
	case errors.As(err, &ve):
		e := map[string]string{"code": "validation", "message": ve.Message}
		if ve.Field != "" {
			e["field"] = ve.Field
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": e})
	case errors.As(err, &re):
		writeError(w, re.Status, re.Code, re.Msg)
	case errors.Is(err, spec.ErrUnknownKind):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"code": "validation", "message": "unknown integration kind", "field": "kind"}})
	case errors.Is(err, spec.ErrNotSecretManager):
		writeError(w, http.StatusBadRequest, "validation", "this integration is not a secret manager")
	case errors.Is(err, spec.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "integration not found")
	case errors.Is(err, spec.ErrUnknownAlertSource):
		writeError(w, http.StatusNotFound, "not_found", "unknown alert source")
	case errors.Is(err, spec.ErrVersionConflict):
		writeError(w, http.StatusConflict, "version_conflict", "the integration was changed by someone else; reload and try again")
	case errors.Is(err, spec.ErrDuplicate):
		writeError(w, http.StatusConflict, "duplicate", "this is already connected")
	case errors.Is(err, spec.ErrManagedByEnv):
		writeError(w, http.StatusConflict, "managed_by_env", "this is set by environment variables on the server")
	case errors.Is(err, spec.ErrSecretKeyMissing):
		writeError(w, http.StatusConflict, "secret_key_missing", "typed-in secrets need PIPEROOM_SECRET_KEY on the server")
	case errors.Is(err, spec.ErrSecretKeyMismatch):
		writeError(w, http.StatusConflict, "secret_key_mismatch",
			"the saved typed-in secrets cannot be opened with the server's PIPEROOM_SECRET_KEY or PIPEROOM_SECRET_KEY_PREVIOUS; set the key they were saved with as PIPEROOM_SECRET_KEY_PREVIOUS, or enter them again")
	case errors.As(err, &iu):
		deps := iu.Dependents
		if deps == nil {
			deps = []spec.Dependent{}
		}
		names := make([]string, 0, len(deps))
		for _, d := range deps {
			names = append(names, d.Title+" ("+d.ID+")")
		}
		s.writeIntegrationJSON(w, http.StatusConflict, map[string]any{
			"error": map[string]string{"code": "in_use", "message": "enabled integrations read secrets from this manager: " +
				strings.Join(names, ", ") + "; move them to another secret manager or disable them first"},
			"dependents": deps,
		})
	case errors.As(err, &sre):
		s.writeSecretRefErr(w, sre)
	case errors.Is(err, spec.ErrRateLimited):
		writeError(w, http.StatusTooManyRequests, "rate_limited", "at most 10 tests a minute")
	case errors.Is(err, spec.ErrProviderUnreachable):
		writeError(w, http.StatusBadGateway, "provider_unreachable", "the secret manager did not answer")
	default:
		// The cause may name a host or a provider; it is logged, not returned.
		s.log.Error("http: integrations request failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
	}
}

// writeSecretRefErr answers a reference (or listing prefix) that cannot be
// read. Messages are fixed text naming the field, never the reference.
func (s *Server) writeSecretRefErr(w http.ResponseWriter, e *spec.SecretRefError) {
	status, code, msg := http.StatusBadRequest, "validation", "the secret reference cannot be read"
	switch e.Reason {
	case spec.SecretRefInvalid:
		msg = "not a valid secret:// reference or prefix"
	case spec.SecretRefUnknownProvider:
		msg = "the reference names a secret manager that is not connected in this workspace"
	case spec.SecretRefDenied:
		status, code, msg = http.StatusForbidden, "secret_access_denied",
			"the secret manager refused this read: it is outside the provider's prefix or file roots, or its policy does not allow it"
	case spec.SecretRefNotFound:
		status, code, msg = http.StatusNotFound, "not_found", "the referenced secret, or its key, was not found"
	case spec.SecretRefUnreachable:
		writeError(w, http.StatusBadGateway, "provider_unreachable", "the secret manager did not answer")
		return
	default:
		s.log.Warn("http: integrations: a secret reference cannot be read", "field", e.Field, "err", e.Err)
	}
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg, "field": e.Field}})
}

// ---- body ----

// intBody is the write body of test, create and update.
type intBody struct {
	Kind    string                     `json:"kind"`
	ID      string                     `json:"id"`
	Config  map[string]any             `json:"config"`
	Secrets map[string]json.RawMessage `json:"secrets"`
}

// decodeIntBody reads the write body into a spec.Input.
func decodeIntBody(w http.ResponseWriter, r *http.Request) (spec.Input, error) {
	var b intBody
	if err := decodeBody(w, r, &b); err != nil {
		return spec.Input{}, err
	}
	in := spec.Input{Kind: b.Kind, ID: b.ID, Config: b.Config, Secrets: map[string]spec.SecretInput{}}
	if in.Config == nil {
		in.Config = map[string]any{}
	}
	for name, raw := range b.Secrets {
		si, err := secretInput(name, raw)
		if err != nil {
			return spec.Input{}, err
		}
		in.Secrets[name] = si
	}
	return in, nil
}

// secretInput decodes one secrets.<field>: absent or "" keep, a string set,
// {"$ref": "secret://…"} a reference, null clear.
func secretInput(name string, raw json.RawMessage) (spec.SecretInput, error) {
	raw = bytes.TrimSpace(raw)
	bad := &spec.ValidationError{Field: name, Message: `must be a string, {"$ref": "secret://…"} or null`}
	switch {
	case len(raw) == 0:
		return spec.SecretInput{Op: spec.SecretKeep}, nil
	case bytes.Equal(raw, []byte("null")):
		return spec.SecretInput{Op: spec.SecretClear}, nil
	case raw[0] == '"':
		var v string
		if err := json.Unmarshal(raw, &v); err != nil {
			return spec.SecretInput{}, bad
		}
		if v == "" {
			return spec.SecretInput{Op: spec.SecretKeep}, nil
		}
		return spec.SecretInput{Op: spec.SecretSet, Value: netsec.Secret(v)}, nil
	case raw[0] == '{':
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil || len(m) != 1 {
			return spec.SecretInput{}, bad
		}
		var ref string
		if err := json.Unmarshal(m[spec.RefKey], &ref); err != nil || !strings.HasPrefix(ref, spec.RefPrefix) {
			return spec.SecretInput{}, &spec.ValidationError{Field: name, Message: `a reference must be {"$ref": "secret://<provider>/<path>"}`}
		}
		return spec.SecretInput{Op: spec.SecretRef, Ref: ref}, nil
	}
	return spec.SecretInput{}, bad
}

// ifMatch reads If-Match: <version> (a quoted ETag form is accepted too).
func ifMatch(r *http.Request) (int64, error) {
	raw := strings.TrimSpace(r.Header.Get("If-Match"))
	if raw == "" {
		return 0, &spec.ValidationError{Field: "If-Match", Message: "header If-Match: <version> is required"}
	}
	raw = strings.TrimPrefix(raw, "W/")
	raw = strings.Trim(raw, `"`)
	n, err := strconv.ParseInt(raw, 10, 64)
	// 0 is the version of env rows (docs/api.md), so a PUT that sends it
	// reaches the service and gets 409 managed_by_env; a db row starts at 1,
	// so 0 still fails its version check.
	if err != nil || n < 0 {
		return 0, &spec.ValidationError{Field: "If-Match", Message: "If-Match must be the integration's version, a non-negative integer"}
	}
	return n, nil
}

// ---- handlers ----

func (s *Server) handleIntCatalog(w http.ResponseWriter, _ *http.Request) {
	kinds := s.ints.Service.Catalog()
	out := make([]spec.CatalogKind, 0, len(kinds))
	for _, k := range kinds {
		out = append(out, spec.ToCatalog(k))
	}
	s.writeIntegrationJSON(w, http.StatusOK, out)
}

func (s *Server) handleIntSetupScript(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Config map[string]any `json:"config"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	script, err := s.ints.Service.SetupScript(chi.URLParam(r, "kind"), body.Config)
	if errors.Is(err, spec.ErrUnknownKind) {
		writeError(w, http.StatusNotFound, "not_found", "unknown integration kind")
		return
	}
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	s.writeIntegrationJSON(w, http.StatusOK, map[string]string{"script": script})
}

func (s *Server) handleIntList(w http.ResponseWriter, r *http.Request) {
	views, err := s.ints.Service.List(r.Context(), workspaceFrom(r.Context()))
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	if views == nil {
		views = []spec.View{}
	}
	s.writeIntegrationJSON(w, http.StatusOK, views)
}

func (s *Server) handleIntStatus(w http.ResponseWriter, _ *http.Request) {
	s.writeIntegrationJSON(w, http.StatusOK, map[string]bool{"secret_key_set": s.ints.SecretKeySet})
}

func (s *Server) handleIntTest(w http.ResponseWriter, r *http.Request) {
	in, err := decodeIntBody(w, r)
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	checks, err := s.ints.Service.Test(r.Context(), workspaceFrom(r.Context()), userFrom(r.Context()).ID, in)
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	s.writeIntegrationJSON(w, http.StatusOK, map[string]any{"checks": nonNilChecks(checks)})
}

func (s *Server) handleIntCreate(w http.ResponseWriter, r *http.Request) {
	in, err := decodeIntBody(w, r)
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	// id is read by POST /test only; a create always gets a new id.
	in.ID = ""
	view, checks, err := s.ints.Service.Create(r.Context(), workspaceFrom(r.Context()), userFrom(r.Context()).ID, in)
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	s.writeIntegrationJSON(w, http.StatusCreated, intWriteResponse{Integration: view, Checks: nonNilChecks(checks)})
}

func (s *Server) handleIntUpdate(w http.ResponseWriter, r *http.Request) {
	version, err := ifMatch(r)
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	in, err := decodeIntBody(w, r)
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	in.ID = ""
	view, checks, err := s.ints.Service.Update(r.Context(), workspaceFrom(r.Context()), userFrom(r.Context()).ID, chi.URLParam(r, "id"), version, in)
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	s.writeIntegrationJSON(w, http.StatusOK, intWriteResponse{Integration: view, Checks: nonNilChecks(checks)})
}

func (s *Server) handleIntEnable(w http.ResponseWriter, r *http.Request) { s.setIntEnabled(w, r, true) }
func (s *Server) handleIntDisable(w http.ResponseWriter, r *http.Request) {
	s.setIntEnabled(w, r, false)
}

func (s *Server) setIntEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	view, err := s.ints.Service.SetEnabled(r.Context(), workspaceFrom(r.Context()), userFrom(r.Context()).ID, chi.URLParam(r, "id"), enabled)
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	s.writeIntegrationJSON(w, http.StatusOK, view)
}

func (s *Server) handleIntDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.ints.Service.Delete(r.Context(), workspaceFrom(r.Context()), userFrom(r.Context()).ID, chi.URLParam(r, "id")); err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleIntAudit(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if q := r.URL.Query().Get("limit"); q != "" {
		n, err := strconv.Atoi(q)
		if err != nil || n < 1 {
			s.writeIntegrationErr(w, &spec.ValidationError{Field: "limit", Message: "limit must be a positive integer"})
			return
		}
		limit = min(n, integrationsMaxAudit)
	}
	entries, err := s.ints.Service.Audit(r.Context(), workspaceFrom(r.Context()), chi.URLParam(r, "id"), limit)
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	if entries == nil {
		entries = []spec.AuditEntry{}
	}
	s.writeIntegrationJSON(w, http.StatusOK, entries)
}

func (s *Server) handleIntAlertSetup(w http.ResponseWriter, r *http.Request) {
	a, err := s.ints.Service.AlertSetup(r.Context(), workspaceFrom(r.Context()), chi.URLParam(r, "source"))
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	s.writeIntegrationJSON(w, http.StatusOK, a)
}

// rotateResponse is the one response that carries a secret (docs/api.md):
// the alert source's new secret, shown once.
type rotateResponse struct {
	spec.AlertSetup
	Secret string `json:"secret"`
	// RestartRequired is true while a rotated secret takes effect only after
	// a server restart (Integrations.AlertRestartRequired).
	RestartRequired bool `json:"restart_required,omitempty"`
}

func (s *Server) handleIntAlertRotate(w http.ResponseWriter, r *http.Request) {
	a, secret, err := s.ints.Service.RotateAlertSecret(r.Context(), workspaceFrom(r.Context()), userFrom(r.Context()).ID, chi.URLParam(r, "source"))
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	s.writeIntegrationJSON(w, http.StatusOK, rotateResponse{AlertSetup: a, Secret: secret.Reveal(), RestartRequired: s.ints.AlertRestartRequired})
}

func (s *Server) handleIntSecretNames(w http.ResponseWriter, r *http.Request) {
	names, err := s.ints.Service.SecretNames(r.Context(), workspaceFrom(r.Context()), chi.URLParam(r, "id"), r.URL.Query().Get("prefix"))
	if errors.Is(err, spec.ErrListUnsupported) {
		s.writeIntegrationJSON(w, http.StatusOK, map[string]any{"names": []string{}, "list_supported": false})
		return
	}
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	if names == nil {
		names = []string{}
	}
	s.writeIntegrationJSON(w, http.StatusOK, map[string]any{"names": names, "list_supported": true})
}

func (s *Server) handleIntSecretKeys(w http.ResponseWriter, r *http.Request) {
	ref := r.URL.Query().Get("ref")
	if ref == "" {
		s.writeIntegrationErr(w, &spec.ValidationError{Field: "ref", Message: "ref is required"})
		return
	}
	keys, err := s.ints.Service.SecretKeys(r.Context(), workspaceFrom(r.Context()), chi.URLParam(r, "id"), ref)
	if errors.Is(err, spec.ErrListUnsupported) {
		// The picker falls back to a typed key, as for names.
		keys, err = []string{}, nil
	}
	if err != nil {
		s.writeIntegrationErr(w, err)
		return
	}
	if keys == nil {
		keys = []string{}
	}
	s.writeIntegrationJSON(w, http.StatusOK, map[string]any{"keys": keys})
}
