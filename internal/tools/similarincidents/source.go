package similarincidents

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Source is what the tool reads: closed rooms and a few event types of each.
// *store.PG and *memstore.Store implement it directly; FromStore wraps any
// other store.Store.
type Source = store.ClosedRoomReader

const (
	// fallbackPage is the page size of the Since scan in the fallback.
	fallbackPage = 500
	// fallbackMaxScan bounds how many events the fallback reads per room.
	fallbackMaxScan = 5000
)

// FromStore returns st as a Source: st itself when it implements
// store.ClosedRoomReader, otherwise an adapter over ListRooms and Since that
// scans at most fallbackMaxScan events per room.
func FromStore(st store.Store) Source {
	if r, ok := st.(store.ClosedRoomReader); ok {
		return r
	}
	return scanSource{st: st}
}

// scanSource implements Source with the base Store methods only.
type scanSource struct{ st store.Store }

func (s scanSource) ListClosedRooms(ctx context.Context, pipelineID string, limit int) ([]store.Room, error) {
	all, err := s.st.ListRooms(ctx)
	if err != nil {
		return nil, fmt.Errorf("similarincidents: list rooms: %w", err)
	}
	var out []store.Room
	for _, r := range all {
		if r.Status == "closed" && r.ClosedAt != nil && (pipelineID == "" || r.PipelineID == pipelineID) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ClosedAt.Equal(*out[j].ClosedAt) {
			return out[i].ClosedAt.After(*out[j].ClosedAt)
		}
		return out[i].ID < out[j].ID
	})
	if limit < 0 {
		limit = 0
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s scanSource) EventsOfTypes(ctx context.Context, roomID string, types []events.Type, limit int) ([]events.Event, error) {
	var out []events.Event
	var after int64
	for scanned := 0; scanned < fallbackMaxScan && len(out) < limit; {
		page, err := s.st.Since(ctx, roomID, after, fallbackPage)
		if err != nil {
			return nil, fmt.Errorf("similarincidents: read room %s: %w", roomID, err)
		}
		if len(page) == 0 {
			break
		}
		for _, e := range page {
			after = e.Seq
			scanned++
			if len(out) < limit && slices.Contains(types, e.Type) {
				e, err := store.StripToolResultData(e)
				if err != nil {
					return nil, fmt.Errorf("similarincidents: %w", err)
				}
				out = append(out, e)
			}
		}
	}
	return out, nil
}
