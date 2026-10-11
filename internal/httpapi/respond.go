package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/topology"
)

// Limits from docs/api.md and AGENTS.md.
const (
	// maxBodyBytes caps every JSON request body (rule 7 in AGENTS.md; the API
	// has no endpoint that needs more).
	maxBodyBytes = 1 << 20

	defaultEventsLimit = 500
	maxEventsLimit     = 1000
)

// Error codes the handlers produce themselves. Codes that come from the room
// service (not_driver, ...) pass through unchanged.
const (
	codeBadRequest      = "bad_request"
	codeValidation      = "validation"
	codeUnauthenticated = "unauthenticated"
	codeNotFound        = "not_found"
	codeMethod          = "method_not_allowed"
	codeTooLarge        = "payload_too_large"
	codeTimeout         = "timeout"
	codeInternal        = "internal"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// apiErr builds the error the handlers return for their own rule violations;
// it travels through the same path as errors from the service.
func apiErr(status int, code, msg string) error {
	return &room.Error{Status: status, Code: code, Message: msg}
}

func badRequest(msg string) error { return apiErr(http.StatusBadRequest, codeBadRequest, msg) }

func validation(msg string) error { return apiErr(http.StatusBadRequest, codeValidation, msg) }

// writeJSON marshals first so a marshal failure can still become a clean 500
// instead of a half-written body.
func writeJSON(w http.ResponseWriter, log *slog.Logger, status int, v any) {
	buf, err := json.Marshal(v)
	if err != nil {
		log.Error("httpapi: marshal response", "err", err)
		writeError(w, http.StatusInternalServerError, codeInternal, genericInternalMessage)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(buf, '\n'))
}

// writeError writes the docs/api.md error shape. It never fails.
func writeError(w http.ResponseWriter, status int, code, msg string) {
	buf, _ := json.Marshal(errorBody{Error: errorDetail{Code: code, Message: msg}})
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(buf, '\n'))
}

const genericInternalMessage = "Something went wrong on the server."

// fail maps err to the error JSON. A *room.Error carries its own status, code
// and message. Anything else is unexpected: the client gets a generic message
// and the real error goes to the log (never the request body).
func (a *api) fail(w http.ResponseWriter, r *http.Request, err error) {
	if re, ok := room.AsError(err); ok && re.Status >= 400 && re.Status <= 599 {
		writeError(w, re.Status, re.Code, re.Message)
		return
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		a.log.Warn("httpapi: request timed out", "method", r.Method, "route", routePattern(r))
		writeError(w, http.StatusGatewayTimeout, codeTimeout, "The request took too long. Try again.")
	case errors.Is(err, context.Canceled):
		// The client went away; nobody reads the answer.
		a.log.Debug("httpapi: request canceled", "method", r.Method, "route", routePattern(r))
		writeError(w, http.StatusInternalServerError, codeInternal, genericInternalMessage)
	default:
		a.log.Error("httpapi: request failed", "method", r.Method, "route", routePattern(r), "err", err)
		writeError(w, http.StatusInternalServerError, codeInternal, genericInternalMessage)
	}
}

// decodeBody reads one JSON object from the (already size-limited) body into
// dst. Unknown fields, trailing data and an empty body are rejected so a
// misspelled field is an error, not a silently ignored one.
func decodeBody(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return bodyError(err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		if tooLarge(err) {
			return bodyError(err)
		}
		return badRequest("The request body must be a single JSON object.")
	}
	return nil
}

func tooLarge(err error) bool {
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}

// bodyError turns a decode or read error into a client error. The parser's
// own text is not echoed: it may quote parts of the body.
func bodyError(err error) error {
	switch {
	case tooLarge(err):
		return apiErr(http.StatusRequestEntityTooLarge, codeTooLarge, "The request body is too large (limit 1 MB).")
	case errors.Is(err, io.EOF):
		return badRequest("The request body is empty; a JSON object is required.")
	default:
		return badRequest("The request body is not valid JSON for this endpoint.")
	}
}

// requireText returns a validation error when v is empty or only whitespace.
func requireText(field, v string) error {
	if strings.TrimSpace(v) == "" {
		return validation(field + " is required.")
	}
	return nil
}

// routePattern is the matched route ("/api/rooms/{id}"), which is safe to log;
// the raw path is not used for grouping.
func routePattern(r *http.Request) string {
	if rc := routeContext(r); rc != nil {
		return rc.RoutePattern()
	}
	return ""
}

// ---- "arrays are never null" ----

// snapshotOut returns s with every nil slice and map replaced by an empty one,
// so the JSON has [] and {} where docs/api.md promises arrays.
func snapshotOut(s room.Snapshot) room.Snapshot {
	if s.OpenProposals == nil {
		s.OpenProposals = []room.OpenProposal{}
	} else {
		// Copy before editing: the service may hand out a cached slice.
		props := make([]room.OpenProposal, len(s.OpenProposals))
		for i, p := range s.OpenProposals {
			if p.Steps == nil {
				p.Steps = []events.ProposalStep{}
			}
			if p.RequiredLayers == nil {
				p.RequiredLayers = []string{}
			}
			if p.Approvals == nil {
				p.Approvals = map[string]string{}
			}
			props[i] = p
		}
		s.OpenProposals = props
	}
	if s.PendingSuggestions == nil {
		s.PendingSuggestions = []room.Suggestion{}
	}
	if s.Members == nil {
		s.Members = []room.Member{}
	}
	if s.Hypothesis != nil && s.Hypothesis.BasedOnSteps == nil {
		h := *s.Hypothesis
		h.BasedOnSteps = []string{}
		s.Hypothesis = &h
	}
	return s
}

func snapshotsOut(in []room.Snapshot) []room.Snapshot {
	out := make([]room.Snapshot, len(in))
	for i, s := range in {
		out[i] = snapshotOut(s)
	}
	return out
}

func graphOut(g topology.Graph) topology.Graph {
	if g.Nodes == nil {
		g.Nodes = []topology.Node{}
	}
	if g.Edges == nil {
		g.Edges = []topology.Edge{}
	}
	return g
}

func userOut(u room.User) room.User {
	if u.Layers == nil {
		u.Layers = []string{}
	}
	return u
}

func pageOut(p room.EventsPage) room.EventsPage {
	if p.Events == nil {
		p.Events = []events.Event{}
	}
	return p
}
