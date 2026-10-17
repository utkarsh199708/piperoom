package qamodel

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

// serveMessages is the model endpoint: it parks the request as a turn and
// answers when a reply is posted, the deadline passes, or the client leaves.
func (p *Puppet) serveMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAPIError(w, http.StatusMethodNotAllowed, "invalid_request_error", "qamodel serves POST /v1/messages")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxModelRequestBytes+1))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request_error", "cannot read the request body")
		return
	}
	if len(body) > MaxModelRequestBytes {
		writeAPIError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body is larger than 16 MiB")
		return
	}
	var req apiRequest
	if err := json.Unmarshal(body, &req); err != nil || len(req.Messages) == 0 {
		writeAPIError(w, http.StatusBadRequest, "invalid_request_error", "want a JSON body with messages")
		return
	}
	if !req.Stream {
		// The agent streams every call (internal/agent/model.go); a plain JSON
		// answer would be code nobody runs.
		writeAPIError(w, http.StatusBadRequest, "invalid_request_error", "qamodel serves streaming requests only (stream: true)")
		return
	}

	t := p.enqueue(&req, body)
	timer := time.NewTimer(p.cfg.ReplyTimeout)
	defer timer.Stop()

	why := StateTimeout
	select {
	case <-t.done:
		why = StateReplied
	case <-timer.C:
	case <-r.Context().Done():
		why = StateCanceled
	case <-p.closed:
		why = StateCanceled
	}
	rep := p.settle(t, why)
	if rep == nil {
		p.log.Info("qamodel: turn canceled", "turn", t.id)
		if r.Context().Err() == nil { // shutting down, the client is still there
			writeAPIError(w, http.StatusServiceUnavailable, "api_error", "qamodel is shutting down")
		}
		return
	}
	if why == StateTimeout && rep.Fallback {
		p.log.Warn("qamodel: turn timed out, sent the fallback text", "turn", t.id, "after", p.cfg.ReplyTimeout)
	}
	p.delivery(t, p.writeReply(w, t, rep))
}

// writeReply sends rep and reports whether the client got it.
func (p *Puppet) writeReply(w http.ResponseWriter, t *turn, rep *Reply) bool {
	if rep.Error != nil {
		retry := "false"
		if rep.Error.Retry {
			retry = "true"
		}
		// The SDK retries 408, 409, 429 and 5xx on its own, and each attempt
		// would park another turn; x-should-retry says otherwise.
		w.Header().Set("x-should-retry", retry)
		writeAPIError(w, rep.Error.Status, rep.Error.Type, rep.Error.Message)
		return true
	}
	out := renderSSE(t, rep)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(out); err != nil {
		return false
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	return true
}

// writeAPIError writes an error body in the Messages API's shape, which is what
// the SDK parses.
func writeAPIError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]string{"type": typ, "message": msg},
	})
}

// isTooLarge reports whether err comes from http.MaxBytesReader.
func isTooLarge(err error) bool {
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}
