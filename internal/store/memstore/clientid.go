package memstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

var _ store.ClientIDs = (*Store)(nil)

// clientIDKey is the index key of an event whose payload has a client_id, with
// the Postgres index's rule: a payload without the key, or with a JSON null
// (NULL in an index never collides), is not indexed.
func clientIDKey(actor events.Actor, payload []byte) (string, bool) {
	if !bytes.Contains(payload, []byte(`"client_id"`)) {
		return "", false // most events: skip the decode
	}
	var p struct {
		ClientID *string `json:"client_id"`
	}
	if json.Unmarshal(payload, &p) != nil || p.ClientID == nil {
		return "", false
	}
	return actor.ID + "\x00" + *p.ClientID, true
}

// EventByClientID implements store.ClientIDs.
func (s *Store) EventByClientID(ctx context.Context, roomID, actorID, clientID string) (events.Event, error) {
	if err := ctx.Err(); err != nil {
		return events.Event{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.rooms[roomID]
	if !ok {
		return events.Event{}, fmt.Errorf("memstore: room %q not found: %w", roomID, store.ErrNotFound)
	}
	seq, ok := log.clientIDs[actorID+"\x00"+clientID]
	if !ok {
		return events.Event{}, fmt.Errorf("memstore: event by client_id in %s: %w", roomID, store.ErrNotFound)
	}
	evs := log.events
	i := sort.Search(len(evs), func(i int) bool { return evs[i].Seq >= seq })
	if i == len(evs) || evs[i].Seq != seq {
		return events.Event{}, fmt.Errorf("memstore: event by client_id in %s: %w", roomID, store.ErrNotFound)
	}
	return evs[i], nil
}
