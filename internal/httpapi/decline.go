package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleDeclineSuggestion serves POST /api/rooms/{id}/suggestions/{sid}/decline.
func (s *Server) handleDeclineSuggestion(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Reason string `json:"reason"`
	}
	if err := decodeOptionalBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	ev, err := s.svc.DeclineSuggestion(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"), chi.URLParam(r, "sid"), body.Reason)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

// decodeOptionalBody is decodeBody for a route whose body is optional
// ({reason?}): an empty body leaves dst at its zero value, anything else must
// be valid JSON within maxBody.
func decodeOptionalBody(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	err := json.NewDecoder(r.Body).Decode(dst)
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	return validation("invalid JSON body")
}
