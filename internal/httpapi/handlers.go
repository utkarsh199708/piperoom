package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
)

// meResponse is GET /api/me. layers is what this session may approve, computed
// per request from users.yaml, owners.yaml (user, group and backup) and solo
// mode; role is member or viewer; groups_missing is true when the IdP sent no
// groups claim (docs/design/auth.md sections 2.1 to 2.3).
type meResponse struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	Layers        []string `json:"layers"`
	Role          string   `json:"role"`
	GroupsMissing bool     `json:"groups_missing"`
	// Verified is true for a users.yaml person only (docs/design/auth.md
	// section 2.1).
	Verified bool `json:"verified"`
	// Solo is true only for the PIPEROOM_SOLO_USER; left out for everyone else
	// so the web app shows the typed-confirm box for this user alone.
	Solo bool `json:"solo,omitempty"`
	// Admin is true only for a users.yaml admin (may delete rooms); left out
	// for everyone else.
	Admin bool `json:"admin,omitempty"`
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r.Context())
	writeJSON(w, http.StatusOK, meResponse{
		ID: u.ID, Name: u.Name, Layers: s.svc.LayersFor(u), Role: roleFrom(r.Context()), GroupsMissing: u.GroupsMissing,
		Verified: u.Verified, Solo: s.svc.IsSolo(u.ID), Admin: s.svc.IsAdmin(u),
	})
}

// handlePeople is GET /api/people: who a driver may give control or a hand-off
// to (docs/api.md). Members only: the list holds emails, and a viewer can
// neither grant nor hand off.
func (s *Server) handlePeople(w http.ResponseWriter, r *http.Request) {
	if roleFrom(r.Context()) != room.RoleMember {
		writeError(w, http.StatusForbidden, "viewer", "viewers cannot see the list of people")
		return
	}
	people, err := s.svc.People(r.Context())
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, people)
}

func (s *Server) handleListRooms(w http.ResponseWriter, r *http.Request) {
	snaps, err := s.svc.ListRoomsFor(r.Context(), userFrom(r.Context()))
	if err != nil {
		s.writeErr(w, err)
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
		s.writeErr(w, err)
		return
	}
	snap, err := s.svc.CreateRoom(r.Context(), userFrom(r.Context()), body.Title, body.Severity, body.PipelineID)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, snap)
}

// handleAlertWebhook accepts any JSON body, maps title/severity/pipeline_id
// (or, without a valid pipeline_id, a top-level dag_id) when present and stores the raw body verbatim in room.created.alert
// (docs/api.md). A resolved alert with no open room to join is 202 and opens
// nothing.
func (s *Server) handleAlertWebhook(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeErr(w, validation("request body too large or unreadable"))
		return
	}
	if !json.Valid(raw) {
		s.writeErr(w, validation("invalid JSON body"))
		return
	}
	// Field mapping is best effort: a non-object body simply maps nothing.
	var probe struct {
		Title      string `json:"title"`
		Severity   string `json:"severity"`
		PipelineID string `json:"pipeline_id"`
		DAGID      string `json:"dag_id"`
	}
	_ = json.Unmarshal(raw, &probe)
	// The signed routes' order (alertin resolvePipeline): a valid explicit
	// pipeline_id first, then the one batch pipeline whose dags list dag_id.
	// An unknown or shared dag_id changes nothing, so the body resolves as it
	// did before dag_id was read.
	if !alertin.ValidPipelineID(probe.PipelineID) {
		if id := s.svc.PipelineForDAG(probe.DAGID); id != "" {
			probe.PipelineID = id
		}
	}
	// The dev webhook has a session: it may open or join rooms only in the
	// caller's tenants (an unknown pipeline is in none). The answer is the
	// 404 of an unknown pipeline.
	if !s.svc.CanSeePipeline(userFrom(r.Context()), probe.PipelineID) {
		writeError(w, http.StatusNotFound, "not_found", "pipeline not found")
		return
	}
	snap, created, err := s.svc.CreateRoomFromAlert(r.Context(), probe.Title, probe.Severity, probe.PipelineID, json.RawMessage(raw))
	s.writeAlertRoom(w, snap, created, err)
}

func (s *Server) handleGetRoom(w http.ResponseWriter, r *http.Request) {
	// The caller's view: mode and open_proposals[].blocked (P0 item 1).
	snap, err := s.svc.RoomFor(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"))
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleRoomEvents(w http.ResponseWriter, r *http.Request) {
	after, err := parseAfter(r)
	if err != nil {
		s.writeErr(w, validation(err.Error()))
		return
	}
	limit, err := parseLimit(r)
	if err != nil {
		s.writeErr(w, validation(err.Error()))
		return
	}
	evs, lastSeq, err := s.svc.Events(r.Context(), chi.URLParam(r, "id"), after, limit)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, eventsResponse{Events: evs, LastSeq: lastSeq})
}

type eventsResponse struct {
	Events  []events.Event `json:"events"`
	LastSeq int64          `json:"last_seq"`
}

// checkClientID refuses a malformed idempotency key with 400 validation, after
// the body is decoded and before the repeat check (docs/api.md, Idempotent
// posts). An absent key (or JSON null) is "", no key; a present "" is
// malformed.
func checkClientID(id *string) (string, error) {
	if id == nil {
		return "", nil
	}
	if !room.ValidClientID(*id) {
		return "", validation(fmt.Sprintf("client_id must be 1-%d characters of A-Z, a-z, 0-9, _ or -", room.MaxClientIDLen))
	}
	return *id, nil
}

func (s *Server) handleAddComment(w http.ResponseWriter, r *http.Request) {
	var body struct {
		StepID   string  `json:"step_id"`
		Text     string  `json:"text"`
		ClientID *string `json:"client_id"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	clientID, err := checkClientID(body.ClientID)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.AddCommentKeyed(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.StepID, body.Text, clientID)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleSteer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text     string  `json:"text"`
		ClientID *string `json:"client_id"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	clientID, err := checkClientID(body.ClientID)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.SteerKeyed(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.Text, clientID)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleSendSuggestion(w http.ResponseWriter, r *http.Request) {
	ev, err := s.svc.SendSuggestion(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), chi.URLParam(r, "sid"))
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleRequestControl(w http.ResponseWriter, r *http.Request) {
	ev, err := s.svc.RequestControl(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"))
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleGrantControl(w http.ResponseWriter, r *http.Request) {
	var body struct {
		To string `json:"to"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.GrantControl(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.To)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleCloseRoom(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Resolution string `json:"resolution"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.CloseRoom(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.Resolution)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleCreateHandoff(w http.ResponseWriter, r *http.Request) {
	// In flight comes before every other check but visibility (docs/api.md),
	// so before the body is even read.
	if err := s.svc.HandoffInFlight(r.Context(), chi.URLParam(r, "id")); err != nil {
		s.writeErr(w, err)
		return
	}
	var body struct {
		To            string `json:"to"`
		StayAsWatcher bool   `json:"stay_as_watcher"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.CreateHandoff(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.To, body.StayAsWatcher)
	if err != nil {
		s.writeErr(w, err)
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
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.Trace(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.Table, body.Key)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleApproveProposal(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Layer string `json:"layer"`
		// ParamsHash is the proposal's params_hash as the approver saw it; it
		// is required (design §3.2): missing is 400, a different one 409.
		ParamsHash string `json:"params_hash"`
		// Confirm is the solo-mode speed bump: "<pipeline_id>/<layer>", typed
		// by hand (design §5.3). Ignored for anyone but the solo user.
		Confirm string `json:"confirm"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.ApproveProposalConfirm(r.Context(), userFrom(r.Context()), chi.URLParam(r, "pid"), body.Layer, body.ParamsHash, body.Confirm)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

// handleApproveAllProposal serves POST /api/proposals/{pid}/approve-all
// (P0 item 4): one approval.given per required layer still pending, answered
// as the list of those events in required_layers order.
func (s *Server) handleApproveAllProposal(w http.ResponseWriter, r *http.Request) {
	var body struct {
		// ParamsHash follows approve's rule: missing is 400, different 409.
		ParamsHash string `json:"params_hash"`
		// Confirm is the solo user's "<pipeline_id>/*", typed once for every
		// layer of the call. Ignored for anyone else.
		Confirm string `json:"confirm"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	evs, err := s.svc.ApproveAllProposal(r.Context(), userFrom(r.Context()), chi.URLParam(r, "pid"), body.ParamsHash, body.Confirm)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, evs)
}

func (s *Server) handleRejectProposal(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Layer      string `json:"layer"`
		Reason     string `json:"reason"`
		ParamsHash string `json:"params_hash"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.RejectProposal(r.Context(), userFrom(r.Context()), chi.URLParam(r, "pid"), body.Layer, body.Reason, body.ParamsHash)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleAcceptHandoff(w http.ResponseWriter, r *http.Request) {
	ev, err := s.svc.AcceptHandoff(r.Context(), userFrom(r.Context()), chi.URLParam(r, "hid"))
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

func (s *Server) handleTopology(w http.ResponseWriter, r *http.Request) {
	g, err := s.svc.Topology(r.Context(), chi.URLParam(r, "pid"))
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, g)
}
