package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleStageProposal serves POST /api/proposals/{pid}/stage (X8c): the
// driver tries the proposal's stageable steps on the pipeline's staging stack.
// It answers 202 with proposal.stage_started at once; the result arrives
// later as proposal.staged over the WebSocket.
func (s *Server) handleStageProposal(w http.ResponseWriter, r *http.Request) {
	// The body is {} (docs/api.md); decoding it keeps the size cap and the
	// JSON check of every other POST route.
	var body struct{}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.StageProposal(r.Context(), userFrom(r.Context()), chi.URLParam(r, "pid"))
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusAccepted, ev)
}
