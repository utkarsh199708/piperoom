package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
		{"leading equals flag", []string{"--owner=alice", "up"}, "alice", "up", ""},
		{"trailing equals flag", []string{"schema-change", "--owner=alice"}, "alice", "schema-change", ""},
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

func TestLockStealAndAcquire(t *testing.T) {
	t.Setenv(stacklock.EnvPath, filepath.Join(t.TempDir(), "stack.lock"))
	lk := stacklock.New(stacklock.Path())
	if _, err := lk.Acquire("bob"); err != nil {
		t.Fatalf("Acquire bob: %v", err)
	}
	// Backdate the lock so it is stale and stealable.
	b, err := json.Marshal(stacklock.Record{Owner: "bob", At: time.Now().Add(-20 * time.Minute)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(lk.Path(), b, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := lockCmd([]string{"--steal", "--owner", "alice"}); err != nil {
		t.Fatalf("lockCmd --steal --owner: %v", err)
	}
	if err := requireLock("alice"); err != nil {
		t.Fatalf("requireLock after steal+acquire: %v", err)
	}
}

func TestGuardedCommandsRequireLock(t *testing.T) {
	t.Setenv(stacklock.EnvPath, filepath.Join(t.TempDir(), "stack.lock"))
	st := &stack{}
	tests := []struct {
		cmd  string
		args []string
	}{
		{"up", []string{"--owner=alice"}},
		{"down", []string{"--owner", "alice"}},
		{"reset", []string{"--owner=alice"}},
		{"load", []string{"--owner=alice"}},
		{"break", []string{"--owner=alice", "schema-change"}},
		{"fix", []string{"--owner=alice", "schema-change"}},
	}
	for _, tt := range tests {
		err := runCommand(context.Background(), st, tt.cmd, tt.args)
		if err == nil || !strings.Contains(err.Error(), "not locked") {
			t.Fatalf("%s %v = %v, want 'not locked' (owner flag not recognized)", tt.cmd, tt.args, err)
		}
	}
}
