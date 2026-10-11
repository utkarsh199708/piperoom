package httpapi

import (
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
)

// ---- identity and rooms ----

func (a *api) me(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, a.log, http.StatusOK, userOut(userFrom(r.Context())))
}

func (a *api) listRooms(w http.ResponseWriter, r *http.Request) {
	rooms, err := a.svc.ListRooms(r.Context())
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, a.log, http.StatusOK, snapshotsOut(rooms))
}

func (a *api) createRoom(w http.ResponseWriter, r *http.Request) {
	var in room.CreateRoomInput
	if err := decodeBody(r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	// Severity and whether the pipeline exists are rules; the Service owns them.
	if err := firstErr(requireText("title", in.Title), requireText("pipeline_id", in.PipelineID)); err != nil {
		a.fail(w, r, err)
		return
	}
	snap, err := a.svc.CreateRoom(r.Context(), userFrom(r.Context()), in)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, a.log, http.StatusOK, snapshotOut(snap))
}

func (a *api) getRoom(w http.ResponseWriter, r *http.Request) {
	snap, err := a.svc.GetRoom(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, a.log, http.StatusOK, snapshotOut(snap))
}

func (a *api) listEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	after, limit := int64(0), defaultEventsLimit
	if v := q.Get("after"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			a.fail(w, r, badRequest("after must be a non-negative integer."))
			return
		}
		after = n
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 {
			a.fail(w, r, badRequest("limit must be a positive integer."))
			return
		}
		limit = int(min(n, maxEventsLimit))
	}
	page, err := a.svc.Events(r.Context(), chi.URLParam(r, "id"), after, limit)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, a.log, http.StatusOK, pageOut(page))
}

// ---- actions in a room ----

// writeEvent writes the Event a room action produced.
func (a *api) writeEvent(w http.ResponseWriter, r *http.Request, ev events.Event, err error) {
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, a.log, http.StatusOK, ev)
}

func (a *api) comment(w http.ResponseWriter, r *http.Request) {
	var in room.CommentInput
	if err := decodeBody(r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	if err := requireText("text", in.Text); err != nil {
		a.fail(w, r, err)
		return
	}
	ev, err := a.svc.Comment(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), in)
	a.writeEvent(w, r, ev, err)
}

func (a *api) steer(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Text string `json:"text"`
	}
	if err := decodeBody(r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	if err := requireText("text", in.Text); err != nil {
		a.fail(w, r, err)
		return
	}
	ev, err := a.svc.Steer(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), in.Text)
	a.writeEvent(w, r, ev, err)
}

func (a *api) sendSuggestion(w http.ResponseWriter, r *http.Request) {
	ev, err := a.svc.SendSuggestion(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), chi.URLParam(r, "sid"))
	a.writeEvent(w, r, ev, err)
}

func (a *api) requestControl(w http.ResponseWriter, r *http.Request) {
	ev, err := a.svc.RequestControl(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"))
	a.writeEvent(w, r, ev, err)
}

func (a *api) grantControl(w http.ResponseWriter, r *http.Request) {
	var in struct {
		To string `json:"to"`
	}
	if err := decodeBody(r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	if err := requireText("to", in.To); err != nil {
		a.fail(w, r, err)
		return
	}
	ev, err := a.svc.GrantControl(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), in.To)
	a.writeEvent(w, r, ev, err)
}

func (a *api) closeRoom(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Resolution string `json:"resolution"`
	}
	if err := decodeBody(r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	if err := requireText("resolution", in.Resolution); err != nil {
		a.fail(w, r, err)
		return
	}
	ev, err := a.svc.Close(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), in.Resolution)
	a.writeEvent(w, r, ev, err)
}

func (a *api) trace(w http.ResponseWriter, r *http.Request) {
	var in room.TraceInput
	if err := decodeBody(r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	if err := firstErr(requireText("table", in.Table), requireText("key", in.Key)); err != nil {
		a.fail(w, r, err)
		return
	}
	ev, err := a.svc.Trace(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), in)
	a.writeEvent(w, r, ev, err)
}

// ---- proposals ----

func (a *api) approve(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Layer string `json:"layer"`
	}
	if err := decodeBody(r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	if err := requireText("layer", in.Layer); err != nil {
		a.fail(w, r, err)
		return
	}
	ev, err := a.svc.Approve(r.Context(), userFrom(r.Context()), chi.URLParam(r, "pid"), in.Layer)
	a.writeEvent(w, r, ev, err)
}

func (a *api) reject(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Layer  string `json:"layer"`
		Reason string `json:"reason"`
	}
	if err := decodeBody(r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	if err := firstErr(requireText("layer", in.Layer), requireText("reason", in.Reason)); err != nil {
		a.fail(w, r, err)
		return
	}
	ev, err := a.svc.Reject(r.Context(), userFrom(r.Context()), chi.URLParam(r, "pid"), in.Layer, in.Reason)
	a.writeEvent(w, r, ev, err)
}

// ---- hand-off ----

func (a *api) createHandoff(w http.ResponseWriter, r *http.Request) {
	var in room.HandoffInput
	if err := decodeBody(r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	if err := requireText("to", in.To); err != nil {
		a.fail(w, r, err)
		return
	}
	ev, err := a.svc.CreateHandoff(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), in)
	a.writeEvent(w, r, ev, err)
}

func (a *api) acceptHandoff(w http.ResponseWriter, r *http.Request) {
	ev, err := a.svc.AcceptHandoff(r.Context(), userFrom(r.Context()), chi.URLParam(r, "hid"))
	a.writeEvent(w, r, ev, err)
}

// ---- pipeline map ----

func (a *api) topology(w http.ResponseWriter, r *http.Request) {
	g, err := a.svc.Topology(r.Context(), chi.URLParam(r, "pid"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, a.log, http.StatusOK, graphOut(g))
}

// ---- alert webhooks ----

// webhookSources are the {source} values of POST /api/alerts/{source}.
// POST /api/alerts/webhook is the docs/api.md name for the generic source.
var webhookSources = map[string]alertin.Source{
	"webhook":                    alertin.Generic,
	string(alertin.Alertmanager): alertin.Alertmanager,
	string(alertin.Grafana):      alertin.Grafana,
	string(alertin.PagerDuty):    alertin.PagerDuty,
	string(alertin.Datadog):      alertin.Datadog,
	string(alertin.Generic):      alertin.Generic,
}

type ignoredBody struct {
	Status string `json:"status"`
}

// webhook handles both webhook routes. The sender proves itself with a
// signature or token that Verifier.Ingest checks over the exact bytes; there
// is no X-User here. Errors never echo the body or the sender's headers.
func (a *api) webhook(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "source")
	if name == "" {
		name = "webhook"
	}
	src, ok := webhookSources[name]
	if !ok {
		writeError(w, http.StatusNotFound, codeNotFound, "Unknown alert source.")
		return
	}
	if a.ver == nil {
		writeError(w, http.StatusUnauthorized, codeUnauthenticated, "Alert ingestion is not configured.")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, alertin.MaxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		if tooLarge(err) {
			writeError(w, http.StatusRequestEntityTooLarge, codeTooLarge, "The alert body is too large.")
			return
		}
		writeError(w, http.StatusBadRequest, codeBadRequest, "The alert body could not be read.")
		return
	}

	alert, err := a.ver.Ingest(src, r.Header, body)
	switch {
	case errors.Is(err, alertin.ErrUnauthenticated), errors.Is(err, alertin.ErrStale):
		// Same answer for "bad signature" and "too old": say nothing that helps a guesser.
		// err's text says why but holds no secret (alertin docs), so it is safe to log.
		a.log.Warn("httpapi: alert rejected", "source", src, "reason", err.Error())
		writeError(w, http.StatusUnauthorized, codeUnauthenticated, "The alert could not be authenticated.")
		return
	case errors.Is(err, alertin.ErrTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, codeTooLarge, "The alert body is too large.")
		return
	case errors.Is(err, alertin.ErrIgnored):
		a.log.Info("httpapi: alert ignored", "source", src)
		writeJSON(w, a.log, http.StatusAccepted, ignoredBody{Status: "ignored"})
		return
	case err != nil:
		a.log.Warn("httpapi: alert not parsed", "source", src, "reason", err.Error())
		writeError(w, http.StatusBadRequest, codeBadRequest, "The alert could not be parsed.")
		return
	case alert.Status != alertin.StatusFiring:
		// A resolution does not open a room.
		a.log.Info("httpapi: alert not firing", "source", src, "status", alert.Status)
		writeJSON(w, a.log, http.StatusAccepted, ignoredBody{Status: "ignored"})
		return
	}

	snap, err := a.svc.IngestAlert(r.Context(), alert)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, a.log, http.StatusOK, snapshotOut(snap))
}

// firstErr returns the first non-nil error.
func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
