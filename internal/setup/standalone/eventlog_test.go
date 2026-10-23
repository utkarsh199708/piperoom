package standalone

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
)

// TestEventLogAppendsAcrossRestarts: the file is append-only over the
// process's lifetimes, one complete JSON event per line, and a closed log
// refuses further appends instead of dropping them.
func TestEventLogAppendsAcrossRestarts(t *testing.T) {
	path := filepath.Join(t.TempDir(), LogFileName)
	l, err := OpenEventLog(path)
	if err != nil {
		t.Fatal(err)
	}
	first := events.Event{RoomID: "r1", Seq: 1, Type: events.RoomCreated, At: time.Unix(1700000000, 0)}
	if err := l.Append(first); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("a second Close: %v", err)
	}
	if err := l.Append(first); !errors.Is(err, errLogClosed) {
		t.Fatalf("Append after Close = %v, want errLogClosed", err)
	}

	// A second lifetime (--watch restart, or a new run) keeps the lines.
	l2, err := OpenEventLog(path)
	if err != nil {
		t.Fatal(err)
	}
	second := events.Event{RoomID: "r1", Seq: 2, Type: events.CommentAdded, At: time.Unix(1700000001, 0)}
	if err := l2.Append(second); err != nil {
		t.Fatal(err)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("the log has %d lines, want 2:\n%s", len(lines), data)
	}
	for i, want := range []events.Event{first, second} {
		var got events.Event
		if err := json.Unmarshal([]byte(lines[i]), &got); err != nil {
			t.Fatalf("line %d is not JSON: %v", i+1, err)
		}
		if got.RoomID != want.RoomID || got.Seq != want.Seq || got.Type != want.Type {
			t.Fatalf("line %d = %+v, want %+v", i+1, got, want)
		}
	}
}

// TestOpenEventLogRefusesADirectory: a bad path is an error naming the path,
// never a panic or a silently missing log.
func TestOpenEventLogRefusesADirectory(t *testing.T) {
	dir := t.TempDir()
	if _, err := OpenEventLog(dir); err == nil {
		t.Fatal("want an error for a directory path")
	}
}
