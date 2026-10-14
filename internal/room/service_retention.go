package room

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
)

// ErrRetentionUnsupported is returned by PurgeExpired when the store cannot
// delete (it does not implement store.Purger).
var ErrRetentionUnsupported = errors.New("room: store does not support retention")

// purgeBatchRooms is how many rooms one store call removes. PurgeExpired loops
// until nothing is left, so this only bounds how long one call holds the pool.
const purgeBatchRooms = 50

// RetentionPolicy is the closed-room retention of every pipeline
// (docs/design/retention.md section 3): PIPEROOM_RETENTION_DAYS, and the
// effective days of each pipeline that sets retention_days in owners.yaml.
type RetentionPolicy struct {
	// GlobalDays is PIPEROOM_RETENTION_DAYS; 0 keeps rooms of pipelines
	// without their own value forever.
	GlobalDays int
	// PipelineDays holds, for each pipeline with retention_days, the days that
	// apply: retention_days, or PIPEROOM_RETENTION_DAYS when that is shorter.
	PipelineDays map[string]int
}

// NewRetentionPolicy combines PIPEROOM_RETENTION_DAYS with the pipelines'
// retention_days; the shorter wins when both are set. own may be nil.
func NewRetentionPolicy(globalDays int, own *owners.Owners) RetentionPolicy {
	p := RetentionPolicy{GlobalDays: globalDays, PipelineDays: map[string]int{}}
	if own == nil {
		return p
	}
	for _, pl := range own.Pipelines {
		d, ok := pl.Retention()
		if !ok {
			continue
		}
		if globalDays > 0 && globalDays < d {
			d = globalDays
		}
		p.PipelineDays[pl.ID] = d
	}
	return p
}

// Off reports whether the policy keeps every closed room forever.
func (p RetentionPolicy) Off() bool {
	return p.GlobalDays <= 0 && len(p.PipelineDays) == 0
}

// PurgeByPolicy runs one retention pass at now: each pipeline with its own
// days first, then every pipeline with PIPEROOM_RETENTION_DAYS. A pipeline's
// own days are never longer than the global ones when both are set, so the
// second pass needs to skip nothing. Totals come back even when a pass fails;
// the errors are joined.
func (s *Service) PurgeByPolicy(ctx context.Context, now time.Time, p RetentionPolicy) (store.PurgeResult, error) {
	var (
		total store.PurgeResult
		errs  []error
	)
	ids := make([]string, 0, len(p.PipelineDays))
	for id := range p.PipelineDays {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		days := p.PipelineDays[id]
		res, err := s.purge(ctx, store.PurgeQuery{Cutoff: now.UTC().AddDate(0, 0, -days), PipelineID: id, AuditReason: retentionReason(days)})
		total.Add(res)
		if err != nil {
			errs = append(errs, fmt.Errorf("pipeline %s: %w", id, err))
		}
		if ctx.Err() != nil {
			return total, errors.Join(errs...)
		}
	}
	if p.GlobalDays > 0 {
		res, err := s.purge(ctx, store.PurgeQuery{Cutoff: now.UTC().AddDate(0, 0, -p.GlobalDays), AuditReason: retentionReason(p.GlobalDays)})
		total.Add(res)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return total, errors.Join(errs...)
}

// PurgeExpired deletes every closed room whose closed_at is before cutoff, with
// all its events and child rows, and drops those rooms from the in-memory
// cache so lists and lookups stop showing them. Open rooms are never touched.
// With PIPEROOM_ARCHIVE_DIR set each room is archived first and a room whose
// archive fails is kept. It returns the totals (counts and room ids only) even
// when it stops early on an error or a done context, so the caller can log how
// far it got.
//
// One failing room does not stop the run: its id and error are logged, it is
// left out for the rest of the run (it would fail the same way again and block
// every room behind it), and the others carry on. The returned error joins
// every room's error; the next run tries the failed rooms again.
func (s *Service) PurgeExpired(ctx context.Context, cutoff time.Time) (store.PurgeResult, error) {
	days := int((s.now().Sub(cutoff) + 12*time.Hour) / (24 * time.Hour))
	return s.purge(ctx, store.PurgeQuery{Cutoff: cutoff, AuditReason: retentionReason(days)})
}

// retentionReason is room_deletions.reason of a retention purge.
func retentionReason(days int) string {
	return fmt.Sprintf("retention: closed %d days", days)
}

// purge loops PurgeClosed in batches of purgeBatchRooms; see PurgeExpired.
func (s *Service) purge(ctx context.Context, q store.PurgeQuery) (store.PurgeResult, error) {
	var total store.PurgeResult
	p, ok := s.store.(store.Purger)
	if !ok {
		return total, ErrRetentionUnsupported
	}
	var errs []error
	q.MaxRooms = purgeBatchRooms
	q.Exclude = []string{}
	q.DeletedAt = s.now().UTC()
	if s.limits.ArchiveDir != "" {
		q.Archive = s.archiveRoom
	}
	for {
		res, err := p.PurgeClosed(ctx, q)
		// The store's audit row of each room is the one q describes; the
		// event count is the last seq this replica folded.
		s.evictRooms(res.RoomIDs, func(id string, lastSeq int64) events.Event {
			return s.deletedEvent(q.Audit(id, "", lastSeq, s.now))
		})
		total.Add(res)
		for _, f := range res.Failed {
			// The room id is an opaque identifier, not content.
			s.log.Error("room: retention could not purge a room", "room", f.RoomID, "err", f.Err)
			q.Exclude = append(q.Exclude, f.RoomID)
			errs = append(errs, fmt.Errorf("room %s: %w", f.RoomID, f.Err))
		}
		if err != nil {
			errs = append(errs, err)
			break
		}
		if res.Examined < purgeBatchRooms {
			break
		}
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
	}
	if len(errs) > 0 {
		return total, fmt.Errorf("room: purge expired: %w", errors.Join(errs...))
	}
	return total, nil
}

// evictRooms forgets deleted rooms: the cache entry, the alert key, and any
// agent, executor or shadow grace timer still running for them. Then, outside
// s.mu, it hands each room's room.deleted frame to the OnRoomDeleted hook, so
// its WebSockets close; frame builds it from the last seq this replica had
// folded (0 for a room it had not cached).
func (s *Service) evictRooms(ids []string, frame func(roomID string, lastSeq int64) events.Event) {
	if len(ids) == 0 {
		return
	}
	lastSeqs := make(map[string]int64, len(ids))
	s.mu.Lock()
	for _, id := range ids {
		st, ok := s.rooms[id]
		if !ok {
			continue
		}
		lastSeqs[id] = st.snap.LastSeq
		if st.agentCancel != nil {
			st.agentCancel()
		}
		if st.execCancel != nil {
			st.execCancel()
		}
		if st.graceTimer != nil {
			st.graceTimer.Stop()
		}
		s.releaseAlertKeyLocked(id, st)
		if st.dedupKey != "" && s.alertRooms[st.dedupKey] == id {
			delete(s.alertRooms, st.dedupKey)
		}
		delete(s.rooms, id)
		s.markListDirtyLocked(id)
	}
	s.mu.Unlock()
	for _, id := range ids {
		s.roomDeleted(id, frame(id, lastSeqs[id]))
	}
}
