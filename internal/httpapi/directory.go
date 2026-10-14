package httpapi

import "net/http"

// handleAuthUsers is GET /auth/users (dev mode only; not mounted in oidc
// mode): the dev login picker, called before login, so it needs no session.
func (s *Server) handleAuthUsers(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, s.svc.LoginPicker())
}

// handlePipelines is GET /api/pipelines: the pipelines the caller may see, for
// the new-room form. Viewers included; only id, title and shadow.
func (s *Server) handlePipelines(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.svc.PipelinesFor(userFrom(r.Context())))
}
