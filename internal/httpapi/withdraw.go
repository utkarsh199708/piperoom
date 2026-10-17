package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleWithdrawProposal serves POST /api/rooms/{id}/proposals/{pid}/withdraw
// (Q0, QA G9): the driver ends an open proposal without running it. The body
// {reason?} is optional.
func (s *Server) handleWithdrawProposal(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Reason string `json:"reason"`
	}
	if err := decodeOptionalBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.WithdrawProposal(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), chi.URLParam(r, "pid"), body.Reason)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}
