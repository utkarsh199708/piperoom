package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

// deleteTimeout bounds DELETE /api/rooms/{id}: with PIPEROOM_ARCHIVE_DIR set it
// writes the whole export before the delete, like the export route.
const deleteTimeout = exportTimeout

// handleExtendRoom is POST /api/rooms/{id}/extend: the driver's one extension
// of the open-room limit (docs/api.md, Room limits).
func (s *Server) handleExtendRoom(w http.ResponseWriter, r *http.Request) {
	ev, err := s.svc.ExtendRoom(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id"))
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeEvent(w, http.StatusOK, ev)
}

// handleDeleteRoom is DELETE /api/rooms/{id}?reason=&archive=: an admin deletes one
// room and its open WebSockets get the room.deleted frame and close
// (docs/api.md). A repeat for a deleted room answers its audit row again; its
// sockets were closed the first time.
func (s *Server) handleDeleteRoom(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	u := userFrom(ctx)
	roomID := chi.URLParam(r, "id")
	// The role first (docs/api.md): a non-admin gets 403 whatever else the
	// request says.
	if !s.svc.IsAdmin(u) {
		s.writeErr(w, room.ErrDeleteNotAllowed())
		return
	}
	// A delete is an erase; ?archive=true keeps a copy first (docs/api.md).
	var archive bool
	switch v := r.URL.Query().Get("archive"); v {
	case "", "false":
	case "true":
		archive = true
	default:
		s.writeErr(w, validation("archive must be true or false"))
		return
	}
	d, err := s.svc.DeleteRoom(ctx, u, roomID, r.URL.Query().Get("reason"), archive)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	payload, err := json.Marshal(events.RoomDeletedPayload{Reason: d.Reason})
	if err != nil {
		// Cannot happen for a struct of one string; the room is gone either way.
		s.log.Error("http: encode room.deleted", "room", roomID, "err", err)
		payload = []byte("{}")
	}
	s.gw.CloseRoom(roomID, events.Event{
		RoomID:  roomID,
		Seq:     d.EventCount + 1,
		Type:    events.RoomDeleted,
		Actor:   events.Actor{Kind: events.ActorHuman, ID: u.ID, Name: u.Name},
		Payload: payload,
		At:      d.DeletedAt.UTC(),
	})
	writeJSON(w, http.StatusOK, d)
}
