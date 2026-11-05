package standalone

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"piperoom/internal/events"
)

// LogFileName is the event log of the standalone server, next to owners.yaml:
// one JSON object per room event, appended across restarts, so a laptop that
// sleeps keeps the record of its setup room (docs/design/setup-agent.md
// section 9).
const LogFileName = "setup-log.jsonl"

// errLogClosed reports an EventLog used after Close.
var errLogClosed = errors.New("setup: the event log is closed")

// EventLog appends every room event to one JSON Lines file. It is the
// standalone server's record: each event is written as one line and flushed to
// the disk (fsync) before the next, so a crash keeps every event the room
// accepted.
//
// The lines are the room log itself. The server never writes a secret value
// into a room event (the setup secret routes keep values out of events; U1-5d
// puts names only), so the file needs no extra redaction — a value that is a
// secret here is a contract violation elsewhere.
type EventLog struct {
	mu     sync.Mutex
	f      *os.File
	path   string
	closed bool
}

// OpenEventLog opens (creating it if needed) the log at path for appending:
// an earlier run's lines are kept. The file is 0600, like the rest of the
// standalone server's state.
func OpenEventLog(path string) (*EventLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("setup: open the event log %s: %w", path, err)
	}
	return &EventLog{f: f, path: path}, nil
}

// Path is the file the log appends to.
func (l *EventLog) Path() string { return l.path }

// Append writes one event as one JSON line and syncs the file, so the line
// survives a crash that follows. Callers may be several rooms' goroutines at
// once.
func (l *EventLog) Append(e events.Event) error {
	line, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("setup: encode event %s #%d of room %s: %w", e.Type, e.Seq, e.RoomID, err)
	}
	line = append(line, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.f == nil {
		return errLogClosed
	}
	if _, err := l.f.Write(line); err != nil {
		return fmt.Errorf("setup: append to the event log %s: %w", l.path, err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("setup: sync the event log %s: %w", l.path, err)
	}
	return nil
}

// Hook returns the room service's OnRoomOpened callback for the store src: it
// subscribes to the room's event stream from its first event and appends every
// event. The replay is why the room.created that opened the room is logged:
// the hook runs after that append, and Subscribe replays from seq 0 first. The
// channel closes when the service stops or the room is deleted, which ends the
// loop and its goroutine.
func (l *EventLog) Hook(src events.Store, log *slog.Logger) func(context.Context, string) {
	if log == nil {
		log = slog.Default()
	}
	return func(ctx context.Context, roomID string) {
		ch, err := src.Subscribe(ctx, roomID, 0)
		if err != nil {
			log.Warn("setup: event log: subscribe to the room", "room", roomID, "err", err)
			return
		}
		for e := range ch {
			if err := l.Append(e); err != nil {
				log.Warn("setup: event log: append", "room", roomID, "seq", e.Seq, "err", err)
			}
		}
	}
}

// Close closes the file. It is safe to call more than once; a later Append
// reports errLogClosed.
func (l *EventLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.f == nil {
		return nil
	}
	l.closed = true
	if err := l.f.Close(); err != nil {
		return fmt.Errorf("setup: close the event log %s: %w", l.path, err)
	}
	return nil
}
