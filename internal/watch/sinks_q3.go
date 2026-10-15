package watch

import (
	"encoding/json"
	"slices"
)

// ClosedRoomKeysSink is an optional RoomSink extension (Q3, QA S1). After a
// restart the watcher does not know which signals a closed watcher room
// carried. With this sink it asks the room service, so the cooldown covers
// that room's incident only. Without it (or when ok is false) a close the
// watcher did not see cools only the signals that were already firing at the
// close: a signal firing now that was not firing then opens a room at once.
type ClosedRoomKeysSink interface {
	// RoomSignalKeys returns the incident keys the room carried: the union of
	// the "keys" arrays (EvidenceKeys) of every watcher evidence JSON in its
	// log (room.created.alert of a watcher room, and the evidence of each
	// PostEvidenceJSON / PostIncidentJSON note). ok is false when the room is
	// unknown or holds no watcher evidence with keys.
	RoomSignalKeys(roomID string) (keys []string, ok bool)
}

// RoomTitleFlagsSink is an optional RoomSink extension (Q3, QA S2): the
// injectscan kinds found in a room's title (the snapshot's title_flags). The
// watcher never joins a room whose title is flagged; it opens its own. ok is
// false when the room is unknown. Without the sink titles are not checked.
type RoomTitleFlagsSink interface {
	RoomTitleFlags(roomID string) (flags []string, ok bool)
}

// maxEvidenceKeys caps the "keys" array of the evidence, so it stays small.
const maxEvidenceKeys = 32

// EvidenceKeys reads the incident keys from one watcher evidence JSON (the
// "keys" array compose writes). It returns nil for anything else, so the room
// service can feed it every evidence a room holds.
func EvidenceKeys(raw json.RawMessage) []string {
	var body struct {
		Source string   `json:"source"`
		Keys   []string `json:"keys"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &body) != nil || body.Source != "watcher" {
		return nil
	}
	var out []string
	for _, k := range body.Keys {
		if k != "" && len(k) <= 256 && !slices.Contains(out, k) && len(out) < maxToldKeys {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

// evidenceKeys is the sorted, de-duplicated, capped "keys" array of an
// evidence body.
func evidenceKeys(keys []string) []string {
	out := slices.Clone(keys)
	slices.Sort(out)
	out = slices.Compact(out)
	if len(out) > maxEvidenceKeys {
		out = out[:maxEvidenceKeys]
	}
	return out
}
