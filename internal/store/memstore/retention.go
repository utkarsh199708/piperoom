package memstore

import (
	"context"
	"sort"
	"strings"
	"time"

	"piperoom/internal/store"
)

var _ store.Purger = (*Store)(nil)

// PurgeClosedBefore implements store.Purger with the same rules as the
// Postgres store: only closed rooms with closed_at before cutoff, oldest
// first, at most maxRooms, and every row keyed by the room goes with it.
func (s *Store) PurgeClosedBefore(ctx context.Context, cutoff time.Time, maxRooms int, exclude []string) (store.PurgeResult, error) {
	var res store.PurgeResult
	if err := ctx.Err(); err != nil {
		return res, err
	}
	if maxRooms <= 0 {
		return res, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	type cand struct {
		id string
		at time.Time
	}
	var cands []cand
	skip := make(map[string]bool, len(exclude))
	for _, id := range exclude {
		skip[id] = true
	}
	for id, log := range s.rooms {
		if skip[id] {
			continue
		}
		m := log.meta
		if m.Status == "closed" && m.ClosedAt != nil && m.ClosedAt.Before(cutoff) {
			cands = append(cands, cand{id, *m.ClosedAt})
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if !cands[i].at.Equal(cands[j].at) {
			return cands[i].at.Before(cands[j].at)
		}
		return cands[i].id < cands[j].id
	})
	if len(cands) > maxRooms {
		cands = cands[:maxRooms]
	}
	res.Examined = len(cands)

	for _, c := range cands {
		log := s.rooms[c.id]
		res.Events += int64(len(log.events))
		delete(s.rooms, c.id)

		for pid, p := range s.proposals {
			if p.RoomID != c.id {
				continue
			}
			res.Proposals++
			for key := range s.approvals {
				if strings.HasPrefix(key, pid+"\x00") {
					res.Approvals++
					delete(s.approvals, key)
				}
			}
			for key := range s.steps {
				if strings.HasPrefix(key, pid+"\x00") {
					res.Steps++
					delete(s.steps, key)
				}
			}
			delete(s.proposals, pid)
		}
		for id, r := range s.suggestions {
			if r.RoomID == c.id {
				res.Suggestions++
				delete(s.suggestions, id)
			}
		}
		for id, h := range s.handoffs {
			if h.RoomID == c.id {
				res.Handoffs++
				delete(s.handoffs, id)
			}
		}
		if _, ok := s.shadowRuns[c.id]; ok {
			res.ShadowEvents += int64(len(s.shadowEvents[c.id]))
			res.ShadowRuns++
			delete(s.shadowEvents, c.id)
			delete(s.shadowRuns, c.id)
		}
		s.dropRoomSpendLocked(c.id)
		res.RoomIDs = append(res.RoomIDs, c.id)
	}
	return res, nil
}
