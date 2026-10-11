package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/events"
)

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, userFrom(r.Context()))
}

func (s *Server) handleListRooms(w http.ResponseWriter, r *http.Request) {
	snaps, err := s.svc.ListRooms(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snaps)
}

func (s *Server) handleCreateRoom(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Title      string `json:"title"`
		Severity   string `json:"severity"`
		PipelineID string `json:"pipeline_id"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		writeErr(w, err)
		return
	}
	snap, err := s.svc.CreateRoom(r.Context(), userFrom(r.Context()), body.Title, body.Severity, body.PipelineID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, snap)
}

func (s *Server) handleAlertWebhook(w http.ResponseWriter, r *http.Request) {
	var raw map[string]any
	if err := decodeBody(w, r, &raw); err != nil {
		writeErr(w, err)
		return
	}
	title, _ := raw["title"].(string)
	severity, _ := raw["severity"].(string)
	pipelineID, _ := raw["pipeline_id"].(string)
	snap, err := s.svc.CreateRoomFromAlert(r.Context(), title, severity, pipelineID, raw)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, snap)
}

func (s *Server) handleGetRoom(w http.ResponseWriter, r *http.Request) {
	snap, err := s.svc.GetRoom(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleRoomEvents(w http.ResponseWriter, r *http.Request) {
	after, err := parseAfter(r)
	if err != nil {
		writeErr(w, validation(err.Error()))
		return
	}
	limit, err := parseLimit(r)
	if err != nil {
		writeErr(w, validation(err.Error()))
		return
	}
	evs, lastSeq, err := s.svc.Events(r.Context(), chi.URLParam(r, "id"), after, limit)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, eventsResponse{Events: evs, LastSeq: lastSeq})
}

type eventsResponse struct {
	Events  []events.Event `json:"events"`
	LastSeq int64          `json:"last_seq"`
}

func (s *Server) handleAddComment(w http.ResponseWriter, r *http.Request) {
	var body struct {
		StepID string `json:"step_id"`
		Text   string `json:"text"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		writeErr(w, err)
		return
	}
	ev, err := s.svc.AddComment(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.StepID, body.Text)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleSteer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text string `json:"text"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		writeErr(w, err)
		return
	}
	ev, err := s.svc.Steer(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.Text)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleSendSuggestion(w http.ResponseWriter, r *http.Request) {
	ev, err := s.svc.SendSuggestion(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), chi.URLParam(r, "sid"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleRequestControl(w http.ResponseWriter, r *http.Request) {
	ev, err := s.svc.RequestControl(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleGrantControl(w http.ResponseWriter, r *http.Request) {
	var body struct {
		To string `json:"to"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		writeErr(w, err)
		return
	}
	ev, err := s.svc.GrantControl(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.To)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleCloseRoom(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Resolution string `json:"resolution"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		writeErr(w, err)
		return
	}
	ev, err := s.svc.CloseRoom(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.Resolution)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleCreateHandoff(w http.ResponseWriter, r *http.Request) {
	var body struct {
		To            string `json:"to"`
		StayAsWatcher bool   `json:"stay_as_watcher"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		writeErr(w, err)
		return
	}
	ev, err := s.svc.CreateHandoff(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.To, body.StayAsWatcher)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleTrace(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Table string `json:"table"`
		Key   string `json:"key"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		writeErr(w, err)
		return
	}
	ev, err := s.svc.Trace(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.Table, body.Key)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleApproveProposal(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Layer string `json:"layer"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		writeErr(w, err)
		return
	}
	ev, err := s.svc.ApproveProposal(r.Context(), userFrom(r.Context()), chi.URLParam(r, "pid"), body.Layer)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleRejectProposal(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Layer  string `json:"layer"`
		Reason string `json:"reason"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		writeErr(w, err)
		return
	}
	ev, err := s.svc.RejectProposal(r.Context(), userFrom(r.Context()), chi.URLParam(r, "pid"), body.Layer, body.Reason)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleAcceptHandoff(w http.ResponseWriter, r *http.Request) {
	ev, err := s.svc.AcceptHandoff(r.Context(), userFrom(r.Context()), chi.URLParam(r, "hid"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleTopology(w http.ResponseWriter, r *http.Request) {
	g, err := s.svc.Topology(r.Context(), chi.URLParam(r, "pid"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, g)
}
