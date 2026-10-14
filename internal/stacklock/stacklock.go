// Package stacklock coordinates the shared dev stack with a single lock file.
//
// Several agents share one local dev stack (Docker Compose). Destructive
// commands such as `devctl reset` must be serialized behind an explicit owner,
// otherwise two agents can reset the stack at the same time and corrupt both
// runs. The lock is a small JSON file ({owner, time}) published atomically via
// a hard link, so readers never observe a partially-written lock.
package stacklock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// EnvPath overrides the lock file location (used by tests and non-default
// checkouts).
const EnvPath = "PIPEROOM_STACK_LOCK"

// Record is the content of the lock file.
type Record struct {
	Owner string    `json:"owner"`
	At    time.Time `json:"at"`
}

// Lock is a handle on a lock file at a fixed path.
type Lock struct {
	path string
}

// Path returns the configured lock file path: $PIPEROOM_STACK_LOCK if set,
// otherwise the default derived from the git common dir.
func Path() string {
	if p := os.Getenv(EnvPath); p != "" {
		return p
	}
	return DefaultPath()
}

// DefaultPath derives the shared lock file location from the git common dir.
// The bus directory (piperoom-bus) is a sibling of the shared repo, so the lock
// lives at <repo-parent>/piperoom-bus/stack.lock, where <repo-parent> is the
// parent of the repo root (the parent of the .git directory). When git is
// unavailable it returns "" (fail closed): the lock operations refuse an empty
// path, so two agents can never silently fall back to different per-cwd lock
// files.
func DefaultPath() string {
	common, err := gitCommonDir()
	if err != nil {
		return ""
	}
	return defaultPath(common)
}

// defaultPath maps a git common dir (<repo>/.git) to the bus lock path:
// <repo-parent>/piperoom-bus/stack.lock.
func defaultPath(commonDir string) string {
	repoRoot := filepath.Dir(filepath.Clean(commonDir))
	return filepath.Join(filepath.Dir(repoRoot), "piperoom-bus", "stack.lock")
}

// gitCommonDir runs `git rev-parse --git-common-dir` and resolves it to an
// absolute path.
func gitCommonDir() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "--git-common-dir").Output()
	if err != nil {
		return "", fmt.Errorf("stacklock: git rev-parse --git-common-dir: %w", err)
	}
	dir := strings.TrimSpace(string(out))
	if dir == "" {
		return "", errors.New("stacklock: empty git common dir")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("stacklock: resolve git common dir %q: %w", dir, err)
	}
	return abs, nil
}

// New returns a Lock for path. An empty path means Path().
func New(path string) *Lock {
	if path == "" {
		path = Path()
	}
	return &Lock{path: path}
}

// Path returns the lock file path this Lock manages.
func (l *Lock) Path() string { return l.path }

// requirePath fails closed when the lock has no resolved path: without git and
// without $PIPEROOM_STACK_LOCK there is no single shared lock file to agree on,
// so mutating the shared stack must be refused rather than silently using a
// per-cwd relative file.
func (l *Lock) requirePath() error {
	if l.path == "" {
		return fmt.Errorf("stacklock: no lock path (git unavailable and %s is not set)", EnvPath)
	}
	return nil
}

// Acquire atomically creates the lock file for owner. Acquiring a lock already
// held by the same owner is a no-op (re-entrant). On success it returns the
// current record (the new one, or the existing one on re-entry).
func (l *Lock) Acquire(owner string) (*Record, error) {
	if err := l.requirePath(); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, errors.New("stacklock: empty owner")
	}
	rec := Record{Owner: owner, At: time.Now()}
	b, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("stacklock: encode lock: %w", err)
	}
	// Write the record to a temp file in the same directory, then hard-link it
	// into place. os.Link fails atomically if the destination already exists
	// (works on NTFS), so a crash between the two steps leaves only the temp
	// file, and readers never observe an empty or partially-written lock.
	dir := filepath.Dir(l.path)
	tmp, err := os.CreateTemp(dir, ".stacklock-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("stacklock: create temp lock: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // best-effort cleanup of the temp hard link
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return nil, fmt.Errorf("stacklock: write temp lock: %w", err)
	}
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return nil, fmt.Errorf("stacklock: chmod temp lock: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("stacklock: close temp lock: %w", err)
	}
	if err := os.Link(tmpName, l.path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			cur, rerr := l.Status()
			if rerr != nil {
				return nil, fmt.Errorf("stacklock: lock exists but cannot be read: %w", rerr)
			}
			if cur != nil && cur.Owner == owner {
				return cur, nil // re-entrant
			}
			return nil, fmt.Errorf("stacklock: already locked by %q", holderName(cur))
		}
		return nil, fmt.Errorf("stacklock: link lock: %w", err)
	}
	return &rec, nil
}

// Release removes the lock file, but only when owner currently holds it. The
// check and the removal run under the guard file (withGuard), so they are one
// step against Steal and other Releases.
func (l *Lock) Release(owner string) error {
	if err := l.requirePath(); err != nil {
		return err
	}
	return l.withGuard(func() error {
		cur, err := l.Status()
		if err != nil {
			return fmt.Errorf("stacklock: read lock: %w", err)
		}
		if cur == nil {
			return errors.New("stacklock: no lock held")
		}
		if cur.Owner != owner {
			return fmt.Errorf("stacklock: lock held by %q, not %q", cur.Owner, owner)
		}
		if err := os.Remove(l.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("stacklock: remove lock: %w", err)
		}
		return nil
	})
}

// Status returns the current lock record, or (nil, nil) when there is no lock.
// A lock file that cannot be decoded (empty or partial, e.g. left behind by a
// crash during an old non-atomic write) is reported as an unknown holder "?"
// timestamped with the file's modification time, so it stays stealable and
// callers can skip rather than fail.
func (l *Lock) Status() (*Record, error) {
	if err := l.requirePath(); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(l.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("stacklock: read lock: %w", err)
	}
	var rec Record
	if err := json.Unmarshal(b, &rec); err != nil {
		st, sterr := os.Stat(l.path)
		if sterr != nil {
			return nil, fmt.Errorf("stacklock: stat lock: %w", sterr)
		}
		return &Record{Owner: "?", At: st.ModTime()}, nil
	}
	return &rec, nil
}

// Steal removes a lock that is older than maxAge, returning the record it
// removed. A fresh lock is never stolen.
//
// The age check and the removal run under the guard file (withGuard). Only a
// guard holder ever removes the lock file, and Acquire can only create it when
// it is absent, so while the guard is held the record read here is the one
// removed: a fresh lock taken after a release is never deleted (QA sweep
// 2026-10-14; the earlier re-read narrowed this race but did not close it).
func (l *Lock) Steal(maxAge time.Duration) (*Record, error) {
	if err := l.requirePath(); err != nil {
		return nil, err
	}
	var stolen *Record
	err := l.withGuard(func() error {
		cur, err := l.Status()
		if err != nil {
			return fmt.Errorf("stacklock: read lock: %w", err)
		}
		if cur == nil {
			return errors.New("stacklock: no lock to steal")
		}
		if age := time.Since(cur.At); age < maxAge {
			return fmt.Errorf("stacklock: lock held by %q is %s old; only steal locks older than %s",
				cur.Owner, age.Round(time.Second), maxAge)
		}
		if err := os.Remove(l.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("stacklock: remove stale lock: %w", err)
		}
		stolen = cur
		return nil
	})
	if err != nil {
		return nil, err
	}
	return stolen, nil
}

// guardWait bounds how long Release and Steal wait for the guard file.
const guardWait = 5 * time.Second

// guardStale is the age after which a guard file is taken to be left behind by
// a process that died while holding it. A guard is held for a read and a
// remove only, so a live holder never comes close.
const guardStale = 30 * time.Second

// withGuard runs fn while holding <path>.guard, created with O_EXCL (atomic on
// every OS, NTFS included). Every removal of the lock file (Release, Steal)
// runs under it, which makes "check the record, then remove the file" one
// step. Acquire does not need it: os.Link only creates the lock file when it
// is absent, never replaces it.
func (l *Lock) withGuard(fn func() error) error {
	guard := l.path + ".guard"
	deadline := time.Now().Add(guardWait)
	for {
		f, err := os.OpenFile(guard, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			if cerr := f.Close(); cerr != nil {
				_ = os.Remove(guard)
				return fmt.Errorf("stacklock: close guard: %w", cerr)
			}
			defer os.Remove(guard)
			return fn()
		}
		if !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("stacklock: create guard: %w", err)
		}
		if st, serr := os.Stat(guard); serr == nil && time.Since(st.ModTime()) > guardStale {
			// Left by a process that died holding it.
			_ = os.Remove(guard)
			continue
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("stacklock: guard %s is busy; retry", guard)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func holderName(cur *Record) string {
	if cur == nil {
		return "?"
	}
	return cur.Owner
}
