package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/room"
)

// Label and checklist routes (docs/api.md, F8). Viewers never reach them:
// viewerGuard answers 403 viewer first. The service checks the room state and
// the caller.

func (s *Server) handleLabel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ActualLayer   string `json:"actual_layer"`
		Cause         string `json:"cause"`
		FixThatWorked string `json:"fix_that_worked"`
		FixProposalID string `json:"fix_proposal_id"`
		AgentHelped   *bool  `json:"agent_helped"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	e, err := s.svc.Label(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), room.LabelInput{
		ActualLayer: body.ActualLayer, Cause: body.Cause, FixThatWorked: body.FixThatWorked,
		FixProposalID: body.FixProposalID, AgentHelped: body.AgentHelped,
	})
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, e)
}

func (s *Server) handleChecklist(w http.ResponseWriter, r *http.Request) {
	var body struct {
		StepID    string `json:"step_id"`
		ToolUseID string `json:"tool_use_id"`
		Hit       *int   `json:"hit"`
		Item      *int   `json:"item"`
		Done      *bool  `json:"done"`
		Note      string `json:"note"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	if body.Hit == nil || body.Item == nil || body.Done == nil {
		s.writeErr(w, validation("hit, item and done are required"))
		return
	}
	e, err := s.svc.ChecklistStep(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), room.ChecklistInput{
		StepID: body.StepID, ToolUseID: body.ToolUseID, Hit: *body.Hit, Item: *body.Item, Done: *body.Done, Note: body.Note,
	})
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, e)
}
