package plan

import (
	"encoding/json"
	"fmt"
	"slices"
)

// The embedded target's ladder (row U6b). The person runs the reference
// project (or their own Debezium Engine application) themselves, so there is
// no Connect status, no topic the room can scan and no registry: the ladder
// reads the log file the person named, through the runner's
// embedded_log_read, and judges only the reference app's own
// "piperoom-reference ..." lines (internal/setup/ops ParseEmbeddedLog).
const (
	RungEngineStarted = "engine_started" // 1: the engine started and has not stopped with an error
	RungEngineEvents  = "engine_events"  // 2: change events reach the engine (the snapshot's)
	// Rung 3 is RungCanaryEvent: the reference app printed the ladder's
	// nonce. Rung 4 is RungPersonChange: an event arrived after the canary.
)

// ReadEmbeddedLog is the runner read every embedded rung issues (the setup
// read tool and runner op of the same name).
const ReadEmbeddedLog = "embedded_log_read"

// embeddedLog is the part of embedded_log_read's Data the predicates read.
type embeddedLog struct {
	Started          *bool  `json:"started"`
	Stopped          bool   `json:"stopped_with_error"`
	Error            string `json:"error"`
	Events           int    `json:"events"`
	NonceSeen        bool   `json:"nonce_seen"`
	EventsAfterNonce int    `json:"events_after_nonce"`
}

func decodeEmbeddedLog(data json.RawMessage) (embeddedLog, bool) {
	var l embeddedLog
	if err := json.Unmarshal(data, &l); err != nil || l.Started == nil {
		return embeddedLog{}, false
	}
	return l, true
}

// EmbeddedLadder returns the four rungs of an embedded setup for source. It
// is not part of Ladder: the room's ladder runner must supply the log path
// the person named to each read (readInput), which it does not carry yet, so
// Ladder keeps refusing the embedded target rather than issue reads it cannot
// fill.
func EmbeddedLadder(source string) ([]Rung, error) {
	if !slices.Contains(ladderSources, source) {
		return nil, fmt.Errorf("%w: unknown source %q", ErrLadder, cleanText(source, 32))
	}
	rungs := []Rung{
		{Name: RungEngineStarted, Reads: []string{ReadEmbeddedLog}, Predicate: engineStarted},
		{Name: RungEngineEvents, Reads: []string{ReadEmbeddedLog}, Predicate: engineEvents},
		{Name: RungCanaryEvent, Reads: []string{ReadEmbeddedLog}, Predicate: embeddedCanary},
		{Name: RungPersonChange, Reads: []string{ReadEmbeddedLog}, Predicate: embeddedPersonChange},
	}
	for i := range rungs {
		rungs[i].Index = i + 1
	}
	return rungs, nil
}

func engineStarted(data json.RawMessage) (bool, string) {
	l, ok := decodeEmbeddedLog(data)
	if !ok {
		return false, "the read did not return the reference app's log"
	}
	switch {
	case !*l.Started:
		return false, "the log has no \"piperoom-reference started\" line yet"
	case l.Stopped:
		return false, "the engine stopped with an error: " + note(l.Error)
	}
	return true, "the engine started and is running"
}

func engineEvents(data json.RawMessage) (bool, string) {
	l, ok := decodeEmbeddedLog(data)
	if !ok {
		return false, "the read did not return the reference app's log"
	}
	if l.Events == 0 {
		return false, "no change event reached the engine since it started"
	}
	return true, fmt.Sprintf("%d change events reached the engine", l.Events)
}

func embeddedCanary(data json.RawMessage) (bool, string) {
	l, ok := decodeEmbeddedLog(data)
	if !ok {
		return false, "the read did not return the reference app's log"
	}
	if !l.NonceSeen {
		return false, "the reference app has not printed the canary nonce yet"
	}
	return true, "the reference app saw the canary event with the nonce"
}

func embeddedPersonChange(data json.RawMessage) (bool, string) {
	l, ok := decodeEmbeddedLog(data)
	if !ok {
		return false, "the read did not return the reference app's log"
	}
	if !l.NonceSeen || l.EventsAfterNonce == 0 {
		return false, "waiting for a change the person makes after the canary"
	}
	return true, "a change arrived after the canary"
}
