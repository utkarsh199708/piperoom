package room

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/store"
)

// maxDeleteReason bounds the reason of a deletion, in characters.
const maxDeleteReason = 500

// Deletion is the answer of DELETE /api/rooms/{id}: the room_deletions row,
// and whether the archive got the room's export first.
type Deletion struct {
	store.RoomDeletion
	Archived bool `json:"archived"`
}

// ErrDeleteNotAllowed is the 403 for a delete by anyone but an admin.
func ErrDeleteNotAllowed() *Error {
	return &Error{Status: 403, Code: "not_allowed", Msg: "deleting a room needs the admin role (admin: true in users.yaml)"}
}

func archiveFailed() *Error {
	return &Error{Status: 500, Code: "archive_failed", Msg: "the room's archive could not be written, so the room was not deleted"}
}

// IsAdmin reports whether the user has admin: true in users.yaml. It reads the
// directory, not the passed value, so only users.yaml can make an admin.
func (s *Service) IsAdmin(user User) bool {
	u, ok := s.users.Get(user.ID)
	return ok && u.Admin
}

// DeleteRoom is DELETE /api/rooms/{id}?reason=&archive= (docs/design/retention.md
// section 6): an admin hard-deletes one room, open or closed, with every row
// keyed by it, and leaves a content-free audit row. A delete is an erase: no
// copy is kept unless archive is true, and then the room's export is written to
// PIPEROOM_ARCHIVE_DIR first (archive without that directory is 400); if the
// write fails nothing is deleted. A room that is already deleted, by an admin
// or by retention, answers with its audit row (idempotent); an id that never
// existed is 404. The caller closes the room's WebSockets.
func (s *Service) DeleteRoom(ctx context.Context, user User, roomID, reason string, archive bool) (Deletion, error) {
	// The role first, so a non-admin learns nothing about which ids exist.
	if !s.IsAdmin(user) {
		return Deletion{}, ErrDeleteNotAllowed()
	}
	if archive && s.limits.ArchiveDir == "" {
		return Deletion{}, validation("archive=true needs PIPEROOM_ARCHIVE_DIR on the server")
	}
	if strings.TrimSpace(reason) == "" {
		return Deletion{}, validation("reason is required")
	}
	if utf8.RuneCountInString(reason) > maxDeleteReason {
		return Deletion{}, validation(fmt.Sprintf("reason is longer than %d characters", maxDeleteReason))
	}
	if err := rejectUnstorable(reason); err != nil {
		return Deletion{}, err
	}
	del, ok := s.store.(store.Deleter)
	if !ok {
		return Deletion{}, errors.New("room: store cannot delete rooms")
	}
	s.deleteMu.Lock()
	defer s.deleteMu.Unlock()

	s.mu.Lock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		s.mu.Unlock()
		var re *Error
		if errors.As(err, &re) && re.Code == codeNotFound {
			return s.priorDeletion(ctx, del, roomID)
		}
		return Deletion{}, err
	}
	// From here every append through the service is refused, so the archive
	// and the delete cover the same log.
	st.deleting = true
	wasOpen := s.roomOpen(st)
	pipelineID := st.snap.PipelineID
	hookKey := hookDedupKey(pipelineID, st.dedupKey)
	s.mu.Unlock()

	undo := func() {
		s.mu.Lock()
		st.deleting = false
		s.mu.Unlock()
	}
	wantSeq := int64(-1)
	archived := false
	if archive {
		seq, err := s.archiveRoom(ctx, roomID)
		if err != nil {
			undo()
			s.log.Error("room: delete: archive failed; room kept", "room", roomID, "err", err)
			return Deletion{}, archiveFailed()
		}
		wantSeq, archived = seq, true
	}
	d, res, err := del.DeleteRoom(ctx, store.RoomDeletion{
		// Postgres keeps microseconds; the first answer must equal a repeat's.
		RoomID: roomID, DeletedBy: user.ID, DeletedAt: s.now().UTC().Truncate(time.Microsecond), Reason: reason,
	}, wantSeq)
	if err != nil {
		undo()
		if errors.Is(err, store.ErrNotFound) {
			// Retention removed it in between.
			return s.priorDeletion(ctx, del, roomID)
		}
		return Deletion{}, fmt.Errorf("room: delete %s: %w", roomID, err)
	}
	s.evictRooms(res.RoomIDs)
	// An open room ends here: forget its token count and let the watcher's
	// cooldown start, as a close would.
	if wasOpen && s.onRoomClosed != nil && !s.isClosed() {
		s.onRoomClosed(s.ctx, roomID, pipelineID, hookKey)
	}
	// Ids and counts only; the reason may name a person, so it stays out.
	s.log.Info("room deleted", "room", roomID, "pipeline", d.PipelineID, "by", d.DeletedBy,
		"events", res.Events, "proposals", res.Proposals, "approvals", res.Approvals, "archived", archived)
	return Deletion{RoomDeletion: d, Archived: archived}, nil
}

// priorDeletion answers a delete of a room that is not there: its audit row
// when an admin deleted it before, 404 otherwise.
func (s *Service) priorDeletion(ctx context.Context, del store.Deleter, roomID string) (Deletion, error) {
	d, err := del.GetRoomDeletion(ctx, roomID)
	if errors.Is(err, store.ErrNotFound) {
		return Deletion{}, notFound("room not found")
	}
	if err != nil {
		return Deletion{}, fmt.Errorf("room: delete %s: %w", roomID, err)
	}
	return Deletion{RoomDeletion: d, Archived: s.archiveExists(d.PipelineID, roomID)}, nil
}

func (s *Service) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// ---- archive ----

// archiveSegment makes one path element of an archive path: characters
// outside [A-Za-z0-9_.-] become "_", and "", "." and ".." become "_". Every
// file operation also goes through os.Root, so even a bad element cannot
// leave the directory.
func archiveSegment(s string) string {
	var b strings.Builder
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-', c == '.':
			b.WriteRune(c)
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "" || out == "." || out == ".." {
		return "_"
	}
	return out
}

// archivePath is <pipeline>/<room>.json inside the archive directory.
func archivePath(pipelineID, roomID string) string {
	return path.Join(archiveSegment(pipelineID), archiveSegment(roomID)+".json")
}

// archiveRoom writes the room's export to PIPEROOM_ARCHIVE_DIR and returns the
// last seq it covers. The file is written under a temporary name, synced and
// renamed, so a crash never leaves a half file under the final name; the
// directory is 0700 and the file 0600. It is store.ArchiveFunc for retention.
func (s *Service) archiveRoom(ctx context.Context, roomID string) (int64, error) {
	ex, err := s.PrepareExport(ctx, roomID)
	if err != nil {
		return 0, fmt.Errorf("room: archive %s: export: %w", roomID, err)
	}
	root, err := os.OpenRoot(s.limits.ArchiveDir)
	if err != nil {
		return 0, fmt.Errorf("room: archive %s: open dir: %w", roomID, err)
	}
	defer root.Close()
	final := archivePath(ex.row.PipelineID, roomID)
	dir := path.Dir(final)
	if err := root.MkdirAll(dir, 0o700); err != nil {
		return 0, fmt.Errorf("room: archive %s: make dir: %w", roomID, err)
	}
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return 0, fmt.Errorf("room: archive %s: temp name: %w", roomID, err)
	}
	tmp := path.Join(dir, "."+archiveSegment(roomID)+".tmp-"+hex.EncodeToString(rnd[:]))
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, fmt.Errorf("room: archive %s: create: %w", roomID, err)
	}
	werr := ex.Write(ctx, f)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = root.Rename(tmp, final)
	}
	if werr != nil {
		_ = root.Remove(tmp)
		return 0, fmt.Errorf("room: archive %s: write: %w", roomID, werr)
	}
	return ex.upTo, nil
}

// archiveExists reports whether the archive directory holds the room's export.
func (s *Service) archiveExists(pipelineID, roomID string) bool {
	if s.limits.ArchiveDir == "" {
		return false
	}
	root, err := os.OpenRoot(s.limits.ArchiveDir)
	if err != nil {
		return false
	}
	defer root.Close()
	_, err = root.Stat(archivePath(pipelineID, roomID))
	return err == nil
}
