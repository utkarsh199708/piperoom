package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"piperoom/internal/adminconfig"
	"piperoom/internal/connprobe"
	"piperoom/internal/room"
)

// Admin settings and connection tests (docs/api.md, "Admin: settings and
// connection tests"; docs/design/onboarding.md phase 1).

// AdminSettings is what the admin routes need: the read-only view, a live test
// of one connection and the YAML export. *adminconfig.Provider implements it.
type AdminSettings interface {
	Config() adminconfig.AdminConfig
	// Test returns adminconfig.ErrUnknownConnection for an id not in Config.
	Test(ctx context.Context, id string) (connprobe.Result, error)
	Export() ([]byte, error)
}

// WithAdmin mounts the admin routes over a. Without it (or with nil) they are
// not mounted and answer 404, as in --demo.
func WithAdmin(a AdminSettings) Option {
	return func(s *Server) { s.admin = a }
}

// Admin test rate limit (docs/api.md): 10 tests per minute per admin. Each
// test makes the server dial out, so it is bounded per person.
const (
	adminTestLimit  = 10
	adminTestWindow = time.Minute
)

// maxAdminTestBody caps the test request body: {"id": "<connection id>"}.
const maxAdminTestBody = 4 << 10

// mountAdmin adds the admin routes to api. They sit in their own group, not
// the protected one, so the role check runs right after authentication and
// before the content type and body checks: anyone but an admin gets 403
// whatever else the request says (docs/api.md).
func (s *Server) mountAdmin(api chi.Router) {
	if s.admin == nil {
		return
	}
	s.adminLimit = newRateLimiter(adminTestLimit, adminTestWindow)
	api.Group(func(adm chi.Router) {
		adm.Use(s.authenticate)
		adm.Use(s.adminOnly)
		adm.Use(requireJSON)
		adm.Use(middleware.Timeout(requestTimeout))
		adm.Get("/admin/config", s.handleAdminConfig)
		adm.Post("/admin/connections/test", s.handleAdminTest)
		adm.Get("/admin/config/export", s.handleAdminExport)
	})
}

// adminOnly answers 403 viewer for a viewer and 403 not_allowed for any other
// non-admin, before anything else, the order of handlePeople and
// handleDeleteRoom.
func (s *Server) adminOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if roleFrom(r.Context()) != room.RoleMember {
			writeError(w, http.StatusForbidden, "viewer", "viewers cannot see or test the server settings")
			return
		}
		if !s.svc.IsAdmin(userFrom(r.Context())) {
			writeError(w, http.StatusForbidden, "not_allowed", "the server settings need the admin role (admin: true in users.yaml)")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// handleAdminConfig is GET /api/admin/config.
func (s *Server) handleAdminConfig(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, s.admin.Config())
}

// handleAdminTest is POST /api/admin/connections/test {"id": "..."}. A failed
// check is a 200 with ok: false, never a 5xx.
func (s *Server) handleAdminTest(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r.Context())
	var body struct {
		ID string `json:"id"`
	}
	if err := decodeBodyLimit(w, r, &body, maxAdminTestBody); err != nil {
		s.writeErr(w, err)
		return
	}
	if strings.TrimSpace(body.ID) == "" {
		s.writeErr(w, validation("id is required"))
		return
	}
	if !s.adminLimit.allow(u.ID, s.now()) {
		writeError(w, http.StatusTooManyRequests, "rate_limited", "at most 10 connection tests per minute; try again shortly")
		return
	}
	res, err := s.admin.Test(r.Context(), body.ID)
	if errors.Is(err, adminconfig.ErrUnknownConnection) {
		writeError(w, http.StatusNotFound, "not_found", "unknown connection id")
		return
	}
	if err != nil {
		s.writeErr(w, err)
		return
	}
	// The id is one of the view's own ids here (an unknown one returned
	// above), so the log holds no client text beyond it; never settings.
	s.log.Info("admin: connection test", "actor", u.ID, "id", body.ID, "ok", res.OK)
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, res)
}

// handleAdminExport is GET /api/admin/config/export: the owners file, ---,
// the users file, as a YAML attachment.
func (s *Server) handleAdminExport(w http.ResponseWriter, _ *http.Request) {
	b, err := s.admin.Export()
	if err != nil {
		s.writeErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="piperoom-config.yaml"`)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}
