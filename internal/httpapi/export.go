package httpapi

import (
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/room"
)

// exportTimeout bounds one audit export. It replaces the 60 s default because a
// room with a long log is read three times (derived sections, hash, stream).
//
// Known wart: chi's Timeout middleware writes a 504 header after the handler
// returns when the deadline passed. Once the export has started streaming the
// status is already sent, so that write only logs net/http's "superfluous
// WriteHeader" line; the client still sees the aborted connection (see
// handleExportRoom), which is what matters.
const exportTimeout = 5 * time.Minute

// exportHashHeader carries the SHA-256 of the export, also stored in the
// document under integrity.sha256.
const exportHashHeader = "X-Piperoom-Export-SHA256"

// handleExportRoom serves GET /api/rooms/{id}/export: the whole room as one
// JSON document, with its hash known before the first byte. Any user in
// users.yaml may call it (the authenticate middleware has already run).
func (s *Server) handleExportRoom(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// A viewer may export only if also named in auditors: (design §3.4). An
	// auditor-only principal is a viewer plus export. The wider PR 4 rule
	// (approvers, backups and auditors only) is not applied yet: a member can
	// still export today.
	if u := userFrom(ctx); roleFrom(ctx) == room.RoleViewer && !s.svc.IsAuditor(u.ID, u.Groups) {
		writeError(w, http.StatusForbidden, "not_allowed", "exporting a room needs the auditor role")
		return
	}
	roomID := chi.URLParam(r, "id")
	ex, err := s.svc.PrepareExport(ctx, roomID)
	if err != nil {
		s.writeErr(w, err)
		return
	}

	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": exportFilename(ex.RoomID())}))
	h.Set(exportHashHeader, ex.SHA256())
	// Incident evidence: never cached by a proxy or the browser.
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	if err := ex.Write(ctx, w); err != nil {
		// Headers are gone, so the status cannot change. Aborting the
		// connection makes the client see a failed download instead of a
		// truncated or mismatching file that looks valid.
		s.log.Error("http: room export failed mid-stream", "room", roomID, "user", userFrom(ctx).ID, "err", err)
		panic(http.ErrAbortHandler)
	}
	// Counts and ids only: the audit trail of who exported what, never content.
	s.log.Info("room exported", "room", roomID, "user", userFrom(ctx).ID, "events", ex.EventCount(), "sha256", ex.SHA256())
}

// exportFilename makes a download name from a room id, keeping only characters
// that are safe in a header and a file name.
func exportFilename(roomID string) string {
	var b strings.Builder
	for _, c := range roomID {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-', c == '.':
			b.WriteRune(c)
		default:
			b.WriteByte('_')
		}
	}
	return "piperoom-" + b.String() + ".json"
}
