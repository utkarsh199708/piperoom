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
	if Path() != DefaultPath() {
		t.Fatalf("Path() = %q, want default %q", Path(), DefaultPath())
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

func TestDefaultPathDerivation(t *testing.T) {
	// The git common dir is <repo>/.git; the bus directory is a sibling of the
	// repo, so the lock is <repo-parent>/piperoom-bus/stack.lock.
	common := filepath.Join("work", "piperoom", ".git")
	got := defaultPath(common)
	want := filepath.Join("work", "piperoom-bus", "stack.lock")
	if got != want {
		t.Fatalf("defaultPath(%q) = %q, want %q", common, got, want)
	}
}

func TestLockFailsClosedWithoutPath(t *testing.T) {
	// A lock with no resolved path (git unavailable and $PIPEROOM_STACK_LOCK
	// unset) must refuse every operation instead of writing a per-cwd file.
	l := &Lock{path: ""}
	if _, err := l.Acquire("alice"); err == nil || !strings.Contains(err.Error(), "no lock path") {
		t.Fatalf("Acquire with empty path = %v, want 'no lock path'", err)
	}
	if _, err := l.Status(); err == nil || !strings.Contains(err.Error(), "no lock path") {
		t.Fatalf("Status with empty path = %v, want 'no lock path'", err)
	}
	if err := l.Release("alice"); err == nil || !strings.Contains(err.Error(), "no lock path") {
		t.Fatalf("Release with empty path = %v, want 'no lock path'", err)
	}
	if _, err := l.Steal(time.Minute); err == nil || !strings.Contains(err.Error(), "no lock path") {
		t.Fatalf("Steal with empty path = %v, want 'no lock path'", err)
	}
}

func TestStatusEmptyFile(t *testing.T) {
	l := tmpLock(t)
	if err := os.WriteFile(l.path, nil, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	rec, err := l.Status()
	if err != nil {
		t.Fatalf("Status on empty file = %v, want nil error", err)
	}
	if rec == nil || rec.Owner != "?" {
		t.Fatalf("Status on empty file = %+v, want owner %q", rec, "?")
	}
	if rec != nil && rec.At.IsZero() {
		t.Fatalf("Status on empty file At = zero, want file mtime")
	}
}

func TestStatusCorruptFile(t *testing.T) {
	l := tmpLock(t)
	if err := os.WriteFile(l.path, []byte("not json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	// An unreadable lock is reported as an unknown holder, not an error, so the
	// lock remains stealable and callers skip instead of failing.
	rec, err := l.Status()
	if err != nil {
		t.Fatalf("Status on corrupt file = %v, want nil error", err)
	}
	if rec == nil || rec.Owner != "?" {
		t.Fatalf("Status on corrupt file = %+v, want owner %q", rec, "?")
	}
}

func TestStealEmptyFileAfterAge(t *testing.T) {
	l := tmpLock(t)
	if err := os.WriteFile(l.path, nil, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Backdate the mtime so the empty remnant counts as stale.
	old := time.Now().Add(-20 * time.Minute)
	if err := os.Chtimes(l.path, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	rec, err := l.Steal(15 * time.Minute)
	if err != nil {
		t.Fatalf("Steal empty stale lock: %v", err)
	}
	if rec == nil || rec.Owner != "?" {
		t.Fatalf("stolen record = %+v, want owner %q", rec, "?")
	}
	got, err := l.Status()
	if err != nil {
		t.Fatalf("Status after steal: %v", err)
	}
	if got != nil {
		t.Fatalf("lock still present after steal: %+v", got)
	}
}

func TestStealEmptyFileFreshRefuses(t *testing.T) {
	l := tmpLock(t)
	if err := os.WriteFile(l.path, nil, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	// A freshly-created empty remnant (mtime now) must not be stealable.
	if _, err := l.Steal(15 * time.Minute); err == nil {
		t.Fatal("Steal empty fresh lock = nil, want error")
	}
	if rec, _ := l.Status(); rec == nil {
		t.Fatal("empty lock was removed by steal of a fresh file")
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
