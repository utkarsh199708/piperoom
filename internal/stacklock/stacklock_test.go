package stacklock

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func tmpLock(t *testing.T) *Lock {
	t.Helper()
	return New(filepath.Join(t.TempDir(), "stack.lock"))
}

// writeRecord writes a lock record with an explicit timestamp (used to simulate
// a stale lock without waiting 15 minutes).
func writeRecord(t *testing.T, path string, rec Record) {
	t.Helper()
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write record: %v", err)
	}
}

func TestAcquireRelease(t *testing.T) {
	l := tmpLock(t)
	if rec, err := l.Acquire("alice"); err != nil {
		t.Fatalf("Acquire: %v", err)
	} else if rec.Owner != "alice" || rec.At.IsZero() {
		t.Fatalf("Acquire record = %+v", rec)
	}

	got, err := l.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got == nil || got.Owner != "alice" {
		t.Fatalf("Status = %+v, want owner alice", got)
	}

	if err := l.Release("alice"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	got, err = l.Status()
	if err != nil {
		t.Fatalf("Status after release: %v", err)
	}
	if got != nil {
		t.Fatalf("Status after release = %+v, want nil", got)
	}
}

func TestAcquireEmptyOwner(t *testing.T) {
	l := tmpLock(t)
	if _, err := l.Acquire(""); err == nil {
		t.Fatal("Acquire(\"\") = nil, want error")
	}
}

func TestAcquireConflictNamesHolder(t *testing.T) {
	l := tmpLock(t)
	if _, err := l.Acquire("alice"); err != nil {
		t.Fatalf("Acquire alice: %v", err)
	}
	_, err := l.Acquire("bob")
	if err == nil {
		t.Fatal("Acquire bob = nil, want conflict error")
	}
	if !strings.Contains(err.Error(), "alice") {
		t.Fatalf("conflict error %q does not name holder alice", err)
	}
}

func TestAcquireReentrant(t *testing.T) {
	l := tmpLock(t)
	if _, err := l.Acquire("alice"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	rec, err := l.Acquire("alice")
	if err != nil {
		t.Fatalf("re-entrant Acquire: %v", err)
	}
	if rec == nil || rec.Owner != "alice" {
		t.Fatalf("re-entrant record = %+v, want alice", rec)
	}
}

func TestReleaseWrongOwner(t *testing.T) {
	l := tmpLock(t)
	if _, err := l.Acquire("alice"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	err := l.Release("bob")
	if err == nil {
		t.Fatal("Release by bob = nil, want error")
	}
	if !strings.Contains(err.Error(), "alice") {
		t.Fatalf("release error %q does not name holder alice", err)
	}
	// Lock must survive a failed release.
	if rec, _ := l.Status(); rec == nil || rec.Owner != "alice" {
		t.Fatalf("lock lost after failed release: %+v", rec)
	}
}

func TestReleaseNoLock(t *testing.T) {
	l := tmpLock(t)
	if err := l.Release("alice"); err == nil {
		t.Fatal("Release with no lock = nil, want error")
	}
}

func TestStatusNoLock(t *testing.T) {
	l := tmpLock(t)
	rec, err := l.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if rec != nil {
		t.Fatalf("Status = %+v, want nil", rec)
	}
}

func TestStealStale(t *testing.T) {
	l := tmpLock(t)
	if _, err := l.Acquire("alice"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	// Backdate the lock to make it stale.
	writeRecord(t, l.path, Record{Owner: "alice", At: time.Now().Add(-20 * time.Minute)})

	rec, err := l.Steal(15 * time.Minute)
	if err != nil {
		t.Fatalf("Steal stale: %v", err)
	}
	if rec == nil || rec.Owner != "alice" {
		t.Fatalf("stolen record = %+v, want alice", rec)
	}
	got, err := l.Status()
	if err != nil {
		t.Fatalf("Status after steal: %v", err)
	}
	if got != nil {
		t.Fatalf("lock still present after steal: %+v", got)
	}
}

func TestStealFresh(t *testing.T) {
	l := tmpLock(t)
	if _, err := l.Acquire("alice"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if _, err := l.Steal(15 * time.Minute); err == nil {
		t.Fatal("Steal fresh = nil, want error")
	}
	if rec, _ := l.Status(); rec == nil {
		t.Fatal("fresh lock was removed by steal")
	}
}

func TestStealNoLock(t *testing.T) {
	l := tmpLock(t)
	if _, err := l.Steal(15 * time.Minute); err == nil {
		t.Fatal("Steal with no lock = nil, want error")
	}
}

func TestPathEnvOverride(t *testing.T) {
	t.Setenv(EnvPath, "")
	if Path() != DefaultPath {
		t.Fatalf("Path() = %q, want default %q", Path(), DefaultPath)
	}
	custom := filepath.Join(t.TempDir(), "custom.lock")
	t.Setenv(EnvPath, custom)
	if Path() != custom {
		t.Fatalf("Path() = %q, want %q", Path(), custom)
	}
	// New("") must also respect the env override.
	if l := New(""); l.Path() != custom {
		t.Fatalf("New(\"\").Path() = %q, want %q", l.Path(), custom)
	}
}

func TestStatusCorruptFile(t *testing.T) {
	l := tmpLock(t)
	if err := os.WriteFile(l.path, []byte("not json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := l.Status(); err == nil {
		t.Fatal("Status on corrupt file = nil, want error")
	}
}

func TestConflictErrorDoesNotWrapOSError(t *testing.T) {
	l := tmpLock(t)
	if _, err := l.Acquire("alice"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	_, err := l.Acquire("bob")
	if err == nil {
		t.Fatal("want conflict error")
	}
	// The conflict is reported as a wrapped, human-readable message; the raw
	// os "file exists" error must not leak through to callers.
	if errors.Is(err, os.ErrExist) {
		t.Fatal("conflict error must not wrap the raw os error")
	}
}
