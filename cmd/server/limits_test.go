package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// TestLimitsSetupRefusesBadValues: every bad retention or limit value stops
// the start, and the error names the variable.
func TestLimitsSetupRefusesBadValues(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		cfg  config.Config
		want string
	}{
		{"retention days", config.Config{RetentionDays: "30d"}, "PIPEROOM_RETENTION_DAYS"},
		{"max open days", config.Config{MaxOpenDays: "two weeks"}, "PIPEROOM_MAX_OPEN_DAYS"},
		{"room max events", config.Config{RoomMaxEvents: "0"}, "PIPEROOM_ROOM_MAX_EVENTS"},
		{"room max bytes", config.Config{RoomMaxBytes: "256MB"}, "PIPEROOM_ROOM_MAX_BYTES"},
		{"event max data", config.Config{EventMaxDataBytes: "1"}, "PIPEROOM_EVENT_MAX_DATA_BYTES"},
		{"archive relative", config.Config{ArchiveDir: "archive"}, "PIPEROOM_ARCHIVE_DIR"},
		{"archive missing", config.Config{ArchiveDir: filepath.Join(t.TempDir(), "nope")}, "PIPEROOM_ARCHIVE_DIR"},
		{"archive is a file", config.Config{ArchiveDir: file}, "PIPEROOM_ARCHIVE_DIR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := limitsSetup(tc.cfg, nil, log)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one naming %s", err, tc.want)
			}
		})
	}
}

func TestLimitsSetupDefaultsAndPolicy(t *testing.T) {
	dir := t.TempDir()
	ten := owners.WholeDays(10)
	own := &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", RetentionDays: &ten}, {ID: "billing"}}}
	l, p, err := limitsSetup(config.Config{RetentionDays: "30", ArchiveDir: dir}, own, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if l.MaxOpenDays != 14 || l.RoomMaxEvents != 20000 || l.RoomMaxBytes != 256<<20 || l.EventMaxDataBytes != 64<<10 || l.ArchiveDir != dir {
		t.Fatalf("limits = %+v", l)
	}
	if p.GlobalDays != 30 || p.PipelineDays["orders"] != 10 || len(p.PipelineDays) != 1 {
		t.Fatalf("policy = %+v", p)
	}
	// The write check leaves nothing behind.
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("archive dir not left empty: %v", entries)
	}
}

// TestStoreLimitsReachTheStore: the server's caps are the store's caps.
func TestStoreLimitsReachTheStore(t *testing.T) {
	st := memstore.New()
	storeLimits(st, config.Limits{RoomMaxEvents: 100, RoomMaxBytes: 1 << 20})
	ctx := context.Background()
	if err := st.CreateRoom(ctx, store.Room{ID: "r1", Title: "t", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	u, err := st.RoomUsage(ctx, "r1")
	if err != nil || u.MaxEvents != 100 || u.MaxBytes != 1<<20 {
		t.Fatalf("usage = %+v %v", u, err)
	}
	for i := 0; i < 100; i++ {
		if _, err := st.Append(ctx, "r1", events.AgentMessage, events.AgentActor, map[string]int{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Append(ctx, "r1", events.AgentMessage, events.AgentActor, map[string]int{"i": 100}); !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("101st agent event: %v", err)
	}
}

type fakeOpenLimiter struct {
	mu    sync.Mutex
	calls int
	res   room.OpenLimitResult
	err   error
}

func (f *fakeOpenLimiter) EnforceOpenLimit(ctx context.Context) (room.OpenLimitResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.res, f.err
}

func (f *fakeOpenLimiter) n() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestOpenLimitLoop(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	off := &fakeOpenLimiter{}
	stop := startOpenLimit(context.Background(), off, 0, time.Millisecond, time.Second, log)
	time.Sleep(20 * time.Millisecond)
	stop()
	if off.n() != 0 {
		t.Fatalf("limit off ran %d passes", off.n())
	}

	on := &fakeOpenLimiter{}
	stop = startOpenLimit(context.Background(), on, 14, 5*time.Millisecond, time.Second, log)
	waitUntil(t, func() bool { return on.n() >= 3 }) // start-up pass + ticks
	stop()
	n := on.n()
	time.Sleep(30 * time.Millisecond)
	if on.n() != n {
		t.Fatal("pass ran after stop")
	}
}

func TestOpenLimitPassLogsCounts(t *testing.T) {
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, nil))
	runOpenLimit(context.Background(), &fakeOpenLimiter{res: room.OpenLimitResult{Warned: 2, Closed: 1}}, time.Second, log)
	runOpenLimit(context.Background(), &fakeOpenLimiter{res: room.OpenLimitResult{Failed: 1}}, time.Second, log)
	runOpenLimit(context.Background(), &fakeOpenLimiter{err: errors.New("db down")}, time.Second, log)
	out := buf.String()
	for _, want := range []string{"warned=2", "closed=1", "level=WARN", "failed=1", "level=ERROR", "db down"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log misses %q: %s", want, out)
		}
	}
}
