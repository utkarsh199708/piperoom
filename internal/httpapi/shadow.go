package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/room"
)

// Shadow mode routes (docs/api.md, docs/design/shadow.md 3.5 and 3.6). Viewers
// never reach the two POSTs: viewerGuard answers 403 viewer first. The service
// checks the room (404 live, 409 room_open, 409 not_labelled) and the
// pipeline-member rule (403 not_allowed).

// parseInclude splits ?include=a,b and refuses values outside allowed.
func parseInclude(r *http.Request, allowed ...string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, raw := range r.URL.Query()["include"] {
		for _, v := range strings.Split(raw, ",") {
			v = strings.TrimSpace(v)
			if v == "" {
				continue
			}
			ok := false
			for _, a := range allowed {
				ok = ok || v == a
			}
			if !ok {
				return nil, validation("include must be one or more of: " + strings.Join(allowed, ", "))
			}
			out[v] = true
		}
	}
	return out, nil
}

// hasInclude reports whether ?include= lists name; other values are ignored.
func hasInclude(r *http.Request, name string) bool {
	for _, raw := range r.URL.Query()["include"] {
		for _, v := range strings.Split(raw, ",") {
			if strings.TrimSpace(v) == name {
				return true
			}
		}
	}
	return false
}

func (s *Server) handleShadowBlind(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ActualLayer    string   `json:"actual_layer"`
		RootCauseTerms []string `json:"root_cause_terms"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	e, err := s.svc.ShadowLabel(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), body.ActualLayer, body.RootCauseTerms)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, e)
}

func (s *Server) handleShadowGrade(w http.ResponseWriter, r *http.Request) {
	var body struct {
		LayerCorrect *bool  `json:"layer_correct"`
		RootCause    string `json:"root_cause"`
		Notes        string `json:"notes"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	if body.LayerCorrect == nil {
		s.writeErr(w, validation("layer_correct (true or false) is required"))
		return
	}
	e, err := s.svc.ShadowGrade(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), *body.LayerCorrect, body.RootCause, body.Notes)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, e)
}

// handleShadow serves GET /api/rooms/{id}/shadow. Any logged-in user may read
// it once the room is labelled, include=data too (as tool_result.data on GET
// /events).
func (s *Server) handleShadow(w http.ResponseWriter, r *http.Request) {
	inc, err := parseInclude(r, "data", "room")
	if err != nil {
		s.writeErr(w, err)
		return
	}
	v, err := s.svc.Shadow(r.Context(), chi.URLParam(r, "id"), room.ShadowReadOptions{Data: inc["data"], Room: inc["room"]})
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

// handleShadowMetrics serves GET /api/shadow/metrics?pipeline=&since=.
func (s *Server) handleShadowMetrics(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var since time.Time
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			s.writeErr(w, validation("since must be an RFC 3339 time"))
			return
		}
		since = t
	}
	m, err := s.svc.ShadowMetrics(r.Context(), q.Get("pipeline"), since)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}
