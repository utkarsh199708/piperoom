package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleNudge serves POST /api/rooms/{id}/nudge.
func (s *Server) handleNudge(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ProposalID string `json:"proposal_id"`
		Layer      string `json:"layer"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.Nudge(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.ProposalID, body.Layer)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}
