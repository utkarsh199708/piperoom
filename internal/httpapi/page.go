package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handlePage serves POST /api/rooms/{id}/page (Q0, QA G2). The rules and the
// error codes (400 validation, 409 room_closed, 409 already_paged) are the
// service's; viewers are refused by viewerGuard (403 viewer).
func (s *Server) handlePage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Layer      string `json:"layer"`
		Reason     string `json:"reason"`
		ProposalID string `json:"proposal_id"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.PageOwners(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.Layer, body.Reason, body.ProposalID)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}
