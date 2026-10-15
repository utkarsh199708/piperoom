package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleHypothesisVerdict serves POST /api/rooms/{id}/hypothesis. Viewers are
// refused by viewerGuard before this runs; any member may decide.
func (s *Server) handleHypothesisVerdict(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Verdict string `json:"verdict"`
		Note    string `json:"note"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.DecideHypothesis(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.Verdict, body.Note)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}
