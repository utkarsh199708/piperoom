package room

import (
	"context"
	"errors"
	"fmt"
	"time"

	"piperoom/internal/store"
)

// ErrRetentionUnsupported is returned by PurgeExpired when the store cannot
// delete (it does not implement store.Purger).
var ErrRetentionUnsupported = errors.New("room: store does not support retention")

// purgeBatchRooms is how many rooms one store call removes. PurgeExpired loops
// until nothing is left, so this only bounds how long one call holds the pool.
const purgeBatchRooms = 50

// PurgeExpired deletes every closed room whose closed_at is before cutoff, with
// all its events and child rows, and drops those rooms from the in-memory
// cache so lists and lookups stop showing them. Open rooms are never touched.
// It returns the totals (counts and room ids only) even when it stops early on
// an error or a done context, so the caller can log how far it got.
func (s *Service) PurgeExpired(ctx context.Context, cutoff time.Time) (store.PurgeResult, error) {
	var total store.PurgeResult
	p, ok := s.store.(store.Purger)
	if !ok {
		return total, ErrRetentionUnsupported
	}
	for {
		res, err := p.PurgeClosedBefore(ctx, cutoff, purgeBatchRooms)
		s.evictRooms(res.RoomIDs)
		total.Add(res)
		if err != nil {
			return total, fmt.Errorf("room: purge expired: %w", err)
		}
		if len(res.RoomIDs) < purgeBatchRooms {
			return total, nil
		}
		if err := ctx.Err(); err != nil {
			return total, fmt.Errorf("room: purge expired: %w", err)
		}
	}
}

// evictRooms forgets purged rooms. They are closed, so no agent or executor
// runs; the cancels are defensive.
func (s *Service) evictRooms(ids []string) {
	if len(ids) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		st, ok := s.rooms[id]
		if !ok {
			continue
		}
		if st.agentCancel != nil {
			st.agentCancel()
		}
		if st.execCancel != nil {
			st.execCancel()
		}
		if st.dedupKey != "" && s.alertRooms[st.dedupKey] == id {
			delete(s.alertRooms, st.dedupKey)
		}
		delete(s.rooms, id)
	}
}
