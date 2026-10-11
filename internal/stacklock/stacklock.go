// Package stacklock coordinates the shared dev stack with a single lock file.
//
// Several agents share one local dev stack (Docker Compose). Destructive
// commands such as `devctl reset` must be serialized behind an explicit owner,
// otherwise two agents can reset the stack at the same time and corrupt both
// runs. The lock is a small file created atomically with O_EXCL and holding a
// JSON record of {owner, time}.
package stacklock

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"
)

// EnvPath overrides the lock file location (used by tests and non-default
// checkouts).
const EnvPath = "PIPEROOM_STACK_LOCK"

// DefaultPath is the lock file shared by all agents through the bus directory.
const DefaultPath = `C:\Users\Utkarsh\Desktop\interview\piperoom-bus\stack.lock`

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
// otherwise DefaultPath.
func Path() string {
	if p := os.Getenv(EnvPath); p != "" {
		return p
	}
	return DefaultPath
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

// Acquire atomically creates the lock file for owner. Acquiring a lock already
// held by the same owner is a no-op (re-entrant). On success it returns the
// current record (the new one, or the existing one on re-entry).
func (l *Lock) Acquire(owner string) (*Record, error) {
	if owner == "" {
		return nil, errors.New("stacklock: empty owner")
	}
	rec := Record{Owner: owner, At: time.Now()}
	b, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("stacklock: encode lock: %w", err)
	}
	f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
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
		return nil, fmt.Errorf("stacklock: create lock: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		_ = os.Remove(l.path) // best-effort cleanup of a partial lock
		return nil, fmt.Errorf("stacklock: write lock: %w", err)
	}
	return &rec, nil
}

// Release removes the lock file, but only when owner currently holds it.
func (l *Lock) Release(owner string) error {
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
}

// Status returns the current lock record, or (nil, nil) when there is no lock.
func (l *Lock) Status() (*Record, error) {
	b, err := os.ReadFile(l.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("stacklock: read lock: %w", err)
	}
	var rec Record
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, fmt.Errorf("stacklock: decode lock %s: %w", l.path, err)
	}
	return &rec, nil
}

// Steal removes a lock that is older than maxAge, returning the record it
// removed. A fresh lock is never stolen.
func (l *Lock) Steal(maxAge time.Duration) (*Record, error) {
	cur, err := l.Status()
	if err != nil {
		return nil, fmt.Errorf("stacklock: read lock: %w", err)
	}
	if cur == nil {
		return nil, errors.New("stacklock: no lock to steal")
	}
	if age := time.Since(cur.At); age < maxAge {
		return nil, fmt.Errorf("stacklock: lock held by %q is %s old; only steal locks older than %s",
			cur.Owner, age.Round(time.Second), maxAge)
	}
	if err := os.Remove(l.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("stacklock: remove stale lock: %w", err)
	}
	return cur, nil
}

func holderName(cur *Record) string {
	if cur == nil {
		return "?"
	}
	return cur.Owner
}
