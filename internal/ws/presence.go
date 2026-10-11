package ws

import (
	"encoding/json"
	"slices"
	"sort"
	"sync"
	"time"
)

// Presence states (docs/api.md §WebSocket).
const (
	StateViewing = "viewing"
	StateAway    = "away"
)

// PresenceMember is one person in a room's presence list.
type PresenceMember struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	State string `json:"state"` // StateViewing | StateAway
}

type presenceFrame struct {
	Type    string           `json:"type"`
	Members []PresenceMember `json:"members"`
}

// encodePresence builds the server → client presence message. Plain strings
// cannot fail to marshal; the fallback only keeps the code free of a swallowed
// error.
func encodePresence(members []PresenceMember) []byte {
	buf, err := json.Marshal(presenceFrame{Type: "presence", Members: members})
	if err != nil {
		return []byte(`{"type":"presence","members":[]}`)
	}
	return buf
}

// hub is the in-memory presence registry of one process, keyed by room.
// Presence is deliberately not in the event log (docs/api.md): it is
// ephemeral and would only add noise to the replay.
//
// A person can have several tabs open. They are one member, shown as viewing
// when any of their tabs is.
type hub struct {
	now       func() time.Time
	awayAfter time.Duration

	mu    sync.Mutex
	rooms map[string]*roomPresence
}

type roomPresence struct {
	conns map[*conn]struct{}
	// last is the list most recently sent, so a change is broadcast only once.
	last []PresenceMember
}

func newHub(now func() time.Time, awayAfter time.Duration) *hub {
	return &hub{now: now, awayAfter: awayAfter, rooms: make(map[string]*roomPresence)}
}

// join registers c as viewing. c always gets the current list; the others get
// it too when it changed.
func (h *hub) join(roomID string, c *conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rp := h.rooms[roomID]
	if rp == nil {
		rp = &roomPresence{conns: make(map[*conn]struct{})}
		h.rooms[roomID] = rp
	}
	c.declared = StateViewing
	c.lastSeen = h.now()
	rp.conns[c] = struct{}{}
	h.refreshLocked(rp, c)
}

// leave removes c and tells the rest of the room if the list changed.
func (h *hub) leave(roomID string, c *conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rp := h.rooms[roomID]
	if rp == nil {
		return
	}
	delete(rp.conns, c)
	if len(rp.conns) == 0 {
		delete(h.rooms, roomID)
		return
	}
	h.refreshLocked(rp, nil)
}

// touch records that c sent a message. state is the state it declared, or ""
// when the message declared none (a malformed message still proves the tab is
// alive).
func (h *hub) touch(roomID string, c *conn, state string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rp := h.rooms[roomID]
	if rp == nil {
		return
	}
	c.lastSeen = h.now()
	if state != "" {
		c.declared = state
	}
	h.refreshLocked(rp, nil)
}

// sweep moves members that went quiet to away. Every connection calls it on
// its own timer, so no goroutine outlives the connections of a room.
func (h *hub) sweep(roomID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rp := h.rooms[roomID]; rp != nil {
		h.refreshLocked(rp, nil)
	}
}

// members returns the current list for tests and diagnostics; never nil.
func (h *hub) members(roomID string) []PresenceMember {
	h.mu.Lock()
	defer h.mu.Unlock()
	rp := h.rooms[roomID]
	if rp == nil {
		return []PresenceMember{}
	}
	return h.membersLocked(rp, h.now())
}

// membersLocked computes the list: one entry per person, sorted by id so the
// order is stable between broadcasts. h.mu must be held.
func (h *hub) membersLocked(rp *roomPresence, now time.Time) []PresenceMember {
	byID := make(map[string]*PresenceMember, len(rp.conns))
	for c := range rp.conns {
		state := StateAway
		if c.declared == StateViewing && now.Sub(c.lastSeen) < h.awayAfter {
			state = StateViewing
		}
		if m, ok := byID[c.user.ID]; ok {
			if state == StateViewing {
				m.State = StateViewing
			}
			continue
		}
		byID[c.user.ID] = &PresenceMember{ID: c.user.ID, Name: c.user.Name, State: state}
	}
	out := make([]PresenceMember, 0, len(byID))
	for _, m := range byID {
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// refreshLocked recomputes the list and sends it to the whole room when it
// changed. If only is set and nothing changed, only is sent the list anyway
// (a new connection needs its first presence message). h.mu must be held.
func (h *hub) refreshLocked(rp *roomPresence, only *conn) {
	members := h.membersLocked(rp, h.now())
	changed := !slices.Equal(members, rp.last)
	if !changed && only == nil {
		return
	}
	frame := encodePresence(members)
	rp.last = members
	if changed {
		for c := range rp.conns {
			c.setPresence(frame)
		}
		return
	}
	only.setPresence(frame)
}
