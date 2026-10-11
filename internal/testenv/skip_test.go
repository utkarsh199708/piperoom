package testenv

import (
	"os"
	"path/filepath"
	"testing"

	"piperoom/internal/stacklock"
)

func TestLockConflictNoLockProceeds(t *testing.T) {
	t.Setenv(stackOwner, "")
	lk := stacklock.New(filepath.Join(t.TempDir(), "stack.lock"))
	if msg := lockConflict(lk, os.Getenv(stackOwner)); msg != "" {
		t.Fatalf("lockConflict(no lock) = %q, want \"\"", msg)
	}
}

func TestLockConflictMatchingOwnerProceeds(t *testing.T) {
	t.Setenv(stackOwner, "alice")
	lk := stacklock.New(filepath.Join(t.TempDir(), "stack.lock"))
	if _, err := lk.Acquire("alice"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if msg := lockConflict(lk, os.Getenv(stackOwner)); msg != "" {
		t.Fatalf("lockConflict(owner match) = %q, want \"\"", msg)
	}
}

func TestLockConflictOtherOwnerSkips(t *testing.T) {
	t.Setenv(stackOwner, "alice")
	lk := stacklock.New(filepath.Join(t.TempDir(), "stack.lock"))
	if _, err := lk.Acquire("bob"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if msg := lockConflict(lk, os.Getenv(stackOwner)); msg == "" {
		t.Fatal("lockConflict(owner mismatch) = \"\", want skip message")
	}
}

func TestLockConflictEmptyLockSkips(t *testing.T) {
	t.Setenv(stackOwner, "")
	lk := stacklock.New(filepath.Join(t.TempDir(), "stack.lock"))
	if err := os.WriteFile(lk.Path(), nil, 0o644); err != nil {
		t.Fatalf("write empty lock: %v", err)
	}
	// A crash remnant must skip (not fail): owner "?" never matches the env tag.
	if msg := lockConflict(lk, os.Getenv(stackOwner)); msg == "" {
		t.Fatal("lockConflict(empty lock) = \"\", want skip message")
	}
}
