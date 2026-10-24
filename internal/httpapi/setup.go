package httpapi

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

// Setup rooms (U wave; docs/api.md "Setup rooms"): POST /api/setup and the
// driver's clicks on a setup room. Every rule lives in package room; these
// handlers decode, call and write, nothing else. The routes are always
// mounted: with PIPEROOM_SETUP_ENABLED off the service answers 404
// setup_disabled, which is the documented answer (a route that is simply
// absent would answer not_found).

// maxSetupBody caps a setup route's request body. A SetupRequest names tables,
// connectors and targets; 64 KiB holds every honest one, and the value lists
// are capped in the service as well.
const maxSetupBody = 64 << 10

// setupRoomResponse is POST /api/setup's answer: the new room, its single-use
// pairing code, when the code expires, and the runner command to paste.
type setupRoomResponse struct {
	Room      room.RoomSnapshot `json:"room"`
	Code      string            `json:"code"`
	ExpiresAt time.Time         `json:"expires_at"`
	Command   string            `json:"command"`
}

// handleSetupCreate is POST /api/setup: open a setup room and return the code
// the runner pairs with.
func (s *Server) handleSetupCreate(w http.ResponseWriter, r *http.Request) {
	var req events.SetupRequest
	if err := decodeBodyLimit(w, r, &req, maxSetupBody); err != nil {
		s.writeErr(w, err)
		return
	}
	res, err := s.svc.CreateSetupRoom(r.Context(), userFrom(r.Context()), req)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, setupRoomResponse{
		Room:      res.Room,
		Code:      res.Code,
		ExpiresAt: res.ExpiresAt.UTC(),
		Command:   setupCommand(s.setupOrigin(r), res.Code),
	})
}

// setupOrigin is the server origin the runner command names: the configured
// public origin, else the one this request arrived on.
func (s *Server) setupOrigin(r *http.Request) string {
	if s.public.Scheme != "" && s.public.Host != "" {
		return s.public.Scheme + "://" + s.public.Host
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

// setupCommand is the line a person pastes into a terminal to pair the runner.
// The code is 8 characters of protocol.CodeAlphabet, and the origin is a host
// the server validated, so the line needs no quoting.
func setupCommand(origin, code string) string {
	if origin == "" {
		return "piperoom-setup --code " + code
	}
	return "piperoom-setup --server " + origin + " --code " + code
}

// handleSetupAddTarget is POST /api/rooms/{id}/setup/targets.
func (s *Server) handleSetupAddTarget(w http.ResponseWriter, r *http.Request) {
	var body events.SetupTarget
	if err := decodeBodyLimit(w, r, &body, maxSetupBody); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.AddSetupTarget(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

// handleSetupRaiseClass is POST /api/rooms/{id}/setup/targets/{t}/class, where
// t is the target's address as registered (URL-encoded).
func (s *Server) handleSetupRaiseClass(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Class string `json:"class"`
	}
	if err := decodeBodyLimit(w, r, &body, maxSetupBody); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.RaiseSetupTargetClass(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), targetParam(r), body.Class)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

// targetParam is the class route's {t}: the target's address as registered,
// URL-encoded by the client (an address may hold ':', '/', '[' and ']', and
// chi hands the raw segment back).
func targetParam(r *http.Request) string {
	raw := chi.URLParam(r, "t")
	if decoded, err := url.PathUnescape(raw); err == nil {
		return decoded
	}
	return raw
}

// handleSetupExtendPipeline is POST /api/rooms/{id}/setup/pipeline.
func (s *Server) handleSetupExtendPipeline(w http.ResponseWriter, r *http.Request) {
	var body struct {
		AddConnectors     []string `json:"add_connectors"`
		AddTables         []string `json:"add_tables"`
		AddConsumerGroups []string `json:"add_consumer_groups"`
		AddSinkTables     []string `json:"add_sink_tables"`
	}
	if err := decodeBodyLimit(w, r, &body, maxSetupBody); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.ExtendSetupPipeline(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), room.SetupPipelineExtension{
		AddConnectors:     body.AddConnectors,
		AddTables:         body.AddTables,
		AddConsumerGroups: body.AddConsumerGroups,
		AddSinkTables:     body.AddSinkTables,
	})
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

// handleSetupMaintenanceWindow is POST /api/rooms/{id}/setup/maintenance-window.
// The times are RFC 3339 strings decoded here, so a malformed one is 400
// validation rather than a decode failure.
func (s *Server) handleSetupMaintenanceWindow(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Start string `json:"start"`
		End   string `json:"end"`
	}
	if err := decodeBodyLimit(w, r, &body, maxSetupBody); err != nil {
		s.writeErr(w, err)
		return
	}
	start, err := parseSetupTime("start", body.Start)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	end, err := parseSetupTime("end", body.End)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.RecordMaintenanceWindow(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), start, end)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

// handleSetupRollback is POST /api/rooms/{id}/setup/rollback: "roll back this
// setup" (U1-14b). It carries no body: the proposal is built from the
// rollbacks the room recorded for the steps the current plan executed, in
// reverse order, so there is nothing for a caller to name. Driver only, on an
// open setup room; a step the guard forbids on its class refuses the whole
// proposal here with the guard row's reason code.
func (s *Server) handleSetupRollback(w http.ResponseWriter, r *http.Request) {
	ev, err := s.svc.CreateSetupRollback(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"))
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

// handleSetupSecret is POST /api/rooms/{id}/setup/secret: the driver typed a
// value in the UI. It is forwarded to the paired runner in one secret frame
// and stored nowhere on the server (docs/api.md). The value never reaches a
// log line, an event or an error; the route is outside request logging
// (setupSecretPath), so even a future change to that middleware cannot log a
// body.
func (s *Server) handleSetupSecret(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	if err := decodeBodyLimit(w, r, &body, maxSetupBody); err != nil {
		s.writeErr(w, err)
		return
	}
	if err := s.svc.SetSetupSecret(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.Name, body.Value); err != nil {
		s.writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleSetupCredentials is POST /api/rooms/{id}/setup/credentials: it asks
// the paired runner for its read credentials and carries no body. The values
// come back in the runner's credentials frame (runnerLoop), never here.
func (s *Server) handleSetupCredentials(w http.ResponseWriter, r *http.Request) {
	if err := s.svc.RegisterSetupCredentials(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id")); err != nil {
		s.writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// handleSetupVerify is POST /api/rooms/{id}/setup/verify: the driver asks the
// server's ladder runner to run the verification ladder now, the person's
// equivalent of the agent's setup_request_verification. It carries no body and
// answers 202: the outcome is setup.verified, written by the ladder runner
// only (U1-8b). A room without a paired runner is 409 no_runner and a second
// click while a run is in flight is 409 already_decided.
func (s *Server) handleSetupVerify(w http.ResponseWriter, r *http.Request) {
	if err := s.svc.RequestSetupVerification(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id")); err != nil {
		s.writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// setupSecretPath reports whether path is the one route whose request body
// carries a secret value (POST /api/rooms/{id}/setup/secret). The route leaves
// no request log line at all: the log already names no body, and this keeps it
// that way if the middleware ever grows one.
func setupSecretPath(path string) bool {
	path = strings.TrimSuffix(path, "/")
	const roomPrefix = "/api/rooms/"
	return strings.HasPrefix(path, roomPrefix) &&
		strings.HasSuffix(path, "/setup/secret") &&
		len(path) > len(roomPrefix)+len("/setup/secret")
}

// parseSetupTime parses one RFC 3339 time of a maintenance window.
func parseSetupTime(field, raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, validation(field + " is required (RFC 3339)")
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, validation(field + " must be an RFC 3339 time")
	}
	return t, nil
}
