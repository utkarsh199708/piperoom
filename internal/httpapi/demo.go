package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/room"
)

// Presenter controls for the scripted demo (KAN-398). These routes exist only
// when cmd/server runs with --demo and passes WithDemoControls, so a real
// server never serves them: without the option they are not registered and
// answer 404 like any unknown path.

// DemoControls is the scripted demo's control surface. *demo.Controls
// implements it; the server holds it only in --demo.
type DemoControls interface {
	// Skip advances the story to the next step without waiting for the delay.
	Skip(roomID string) error
	// Pause holds the story before the next step.
	Pause(roomID string) error
	// Resume continues a paused story.
	Resume(roomID string) error
	// Reset restores the room's fake state and replays the story from step 1.
	Reset(roomID string) error
}

// WithDemoControls mounts the --demo presenter routes over c. Without it (or
// with nil) they are not mounted and answer 404, as on every real server.
func WithDemoControls(c DemoControls) Option {
	return func(s *Server) { s.demo = c }
}

// demoControlResponse is a presenter control's answer. What the control did is
// in the room log as an agent.message; this only names the call.
type demoControlResponse struct {
	RoomID  string `json:"room_id"`
	Control string `json:"control"`
}

// mountDemo adds the four control routes to the room routes. Each one is
// driver-only and open-room-only, checked like the other room commands.
func (s *Server) mountDemo(r chi.Router) {
	if s.demo == nil {
		return
	}
	visible := s.roomVisible
	r.Post("/rooms/{id}/demo/skip", visible(s.handleDemoControl("skip", func(c DemoControls, roomID string) error {
		return c.Skip(roomID)
	})))
	r.Post("/rooms/{id}/demo/pause", visible(s.handleDemoControl("pause", func(c DemoControls, roomID string) error {
		return c.Pause(roomID)
	})))
	r.Post("/rooms/{id}/demo/resume", visible(s.handleDemoControl("resume", func(c DemoControls, roomID string) error {
		return c.Resume(roomID)
	})))
	r.Post("/rooms/{id}/demo/reset", visible(s.handleDemoControl("reset", func(c DemoControls, roomID string) error {
		return c.Reset(roomID)
	})))
}

// handleDemoControl checks the caller is the room's driver and the room is
// open, then runs op. The demo's own errors (demo_idle, demo_finished) are
// 409s from the demo package.
func (s *Server) handleDemoControl(control string, op func(DemoControls, string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := userFrom(r.Context())
		roomID := chi.URLParam(r, "id")
		snap, err := s.svc.RoomFor(r.Context(), user, roomID)
		if err != nil {
			s.writeErr(w, err)
			return
		}
		if snap.Status != "open" {
			writeError(w, http.StatusConflict, "room_closed", "room is closed")
			return
		}
		if snap.Driver == nil || snap.Driver.ID != user.ID {
			s.writeErr(w, room.ErrNotDriver())
			return
		}
		if err := checkDemoBody(w, r); err != nil {
			s.writeErr(w, err)
			return
		}
		if err := op(s.demo, roomID); err != nil {
			s.writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, demoControlResponse{RoomID: roomID, Control: control})
	}
}

// checkDemoBody accepts an empty body or an empty JSON object. The controls
// take no parameters, so anything else is a clear 400.
func checkDemoBody(w http.ResponseWriter, r *http.Request) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return validation("request body too large or unreadable")
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&struct{}{}); err != nil {
		return validation("this control takes no parameters: send an empty JSON object")
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return validation("this control takes no parameters: send an empty JSON object")
	}
	return nil
}
