package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handlePageTeam serves POST /api/rooms/{id}/page-team (X4d). Viewers are
// refused by viewerGuard before this runs; the service checks the driver.
func (s *Server) handlePageTeam(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Team string `json:"team"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.PageTeam(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.Team)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}
