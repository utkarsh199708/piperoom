package httpapi

import (
	"mime"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/room"
)

// handleRoomPostmortem serves GET /api/rooms/{id}/postmortem.md: the room as a
// Markdown post-mortem draft. Anyone who may read the room's events may read it
// (viewers included; roomVisible has answered 404 for another tenant): it
// renders what the room already shows, unlike the export, which is a signed
// audit document. Every string from the log is escaped by the renderer, and the
// response is served as text with nosniff, never as HTML.
func (s *Server) handleRoomPostmortem(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	roomID := chi.URLParam(r, "id")
	var opts []room.PostmortemOption
	if r.URL.Query().Get("with_hash") == "1" {
		opts = append(opts, room.WithPostmortemHash())
	}
	md, err := s.svc.RenderPostmortem(ctx, roomID, opts...)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/markdown; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	if r.URL.Query().Get("download") == "1" {
		h.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": postmortemFilename(roomID)}))
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(md); err != nil {
		s.log.Warn("http: postmortem write failed", "room", roomID, "err", err)
	}
}

// postmortemFilename uses the export's filename rule.
func postmortemFilename(roomID string) string {
	base := strings.TrimSuffix(strings.TrimPrefix(exportFilename(roomID), "piperoom-"), ".json")
	return "piperoom-" + base + "-postmortem.md"
}
