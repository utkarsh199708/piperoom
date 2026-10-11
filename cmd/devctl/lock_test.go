package main

import (
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/stacklock"
)

func TestSplitOwner(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		want     string
		wantRest string
		wantErr  string
	}{
		{"none", []string{"up"}, "", "up", ""},
		{"leading flag", []string{"--owner", "alice", "up"}, "alice", "up", ""},
		{"trailing flag", []string{"schema-change", "--owner", "alice"}, "alice", "schema-change", ""},
		{"missing value", []string{"--owner"}, "", "", "--owner requires a value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(ownerEnv, "")
			owner, rest, err := splitOwner(tt.args)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("splitOwner(%v) err = %v, want containing %q", tt.args, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("splitOwner(%v) err = %v", tt.args, err)
			}
			if owner != tt.want {
				t.Fatalf("splitOwner(%v) owner = %q, want %q", tt.args, owner, tt.want)
			}
			if strings.Join(rest, "|") != tt.wantRest {
				t.Fatalf("splitOwner(%v) rest = %v, want %q", tt.args, rest, tt.wantRest)
			}
		})
	}
}

func TestSplitOwnerEnvFallback(t *testing.T) {
	t.Setenv(ownerEnv, "bob")
	owner, rest, err := splitOwner([]string{"up"})
	if err != nil {
		t.Fatalf("splitOwner err = %v", err)
	}
	if owner != "bob" || strings.Join(rest, "|") != "up" {
		t.Fatalf("owner = %q, rest = %v", owner, rest)
	}
}

func TestRequireLock(t *testing.T) {
	t.Setenv(stacklock.EnvPath, filepath.Join(t.TempDir(), "stack.lock"))

	if err := requireLock(""); err == nil || !strings.Contains(err.Error(), ownerEnv) {
		t.Fatalf("requireLock(\"\") = %v, want owner hint", err)
	}

	if err := requireLock("alice"); err == nil || !strings.Contains(err.Error(), "not locked") {
		t.Fatalf("requireLock before lock = %v, want 'not locked'", err)
	}

	lk := stacklock.New(stacklock.Path())
	if _, err := lk.Acquire("alice"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := requireLock("alice"); err != nil {
		t.Fatalf("requireLock(alice) = %v, want nil", err)
	}
	if err := requireLock("bob"); err == nil || !strings.Contains(err.Error(), "alice") {
		t.Fatalf("requireLock(bob) = %v, want naming holder alice", err)
	}
}

func TestLockUnlockRoundTrip(t *testing.T) {
	t.Setenv(stacklock.EnvPath, filepath.Join(t.TempDir(), "stack.lock"))
	if err := lockCmd([]string{"--owner", "alice"}); err != nil {
		t.Fatalf("lockCmd: %v", err)
	}
	if err := requireLock("alice"); err != nil {
		t.Fatalf("requireLock after lockCmd: %v", err)
	}
	if err := unlockCmd([]string{"--owner", "alice"}); err != nil {
		t.Fatalf("unlockCmd: %v", err)
	}
	if err := requireLock("alice"); err == nil {
		t.Fatal("requireLock after unlock = nil, want 'not locked'")
	}
}

func TestLockCommandArgErrors(t *testing.T) {
	t.Setenv(stacklock.EnvPath, filepath.Join(t.TempDir(), "stack.lock"))
	if err := lockCmd([]string{"--owner"}); err == nil {
		t.Fatal("lock --owner (no value) = nil, want error")
	}
	if err := lockCmd([]string{"frobnicate"}); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Fatalf("lock frobnicate = %v, want usage error", err)
	}
	if err := unlockCmd([]string{"frobnicate"}); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Fatalf("unlock frobnicate = %v, want usage error", err)
	}
}
