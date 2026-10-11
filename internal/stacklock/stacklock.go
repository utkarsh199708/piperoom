// Package stacklock coordinates the shared dev stack with a single lock file.
//
// Several agents share one local dev stack (Docker Compose). Destructive
// commands such as `devctl reset` must be serialized behind an explicit owner,
// otherwise two agents can reset the stack at the same time and corrupt both
// runs. The lock is a small JSON file ({owner, time}) published atomically via
// a hard link, so readers never observe a partially-written lock.
package stacklock

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
// A lock file that cannot be decoded (empty or partial, e.g. left behind by a
// crash during an old non-atomic write) is reported as an unknown holder "?"
// timestamped with the file's modification time, so it stays stealable and
// callers can skip rather than fail.
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
	// Re-read just before removing: the stale holder may have released and a
	// third agent re-acquired since the first Status. Only remove when the
	// record is unchanged, so a fresh lock is never deleted.
	cur2, err := l.Status()
	if err != nil {
		return nil, fmt.Errorf("stacklock: re-read lock: %w", err)
	}
	if cur2 == nil {
		return nil, errors.New("stacklock: no lock to steal")
	}
	if cur2.Owner != cur.Owner || !cur2.At.Equal(cur.At) {
		return nil, fmt.Errorf("stacklock: lock changed while stealing (%q -> %q); retry", cur.Owner, cur2.Owner)
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
