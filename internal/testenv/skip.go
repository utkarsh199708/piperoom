package testenv

import (
	"fmt"

	"piperoom/internal/stacklock"
)

// stackOwner names the environment variable that says which agent may use the
// shared dev stack (mirrors PIPEROOM_STACK_OWNER in cmd/devctl).
const stackOwner = "PIPEROOM_STACK_OWNER"

// lockConflict returns a non-empty skip message when the lock at lk is held by
// someone other than owner, or "" when the test may proceed. An unreadable lock
// (e.g. an empty file left by a crash) is reported as an unknown holder "?", so
// tests skip rather than fail until a human steals the stale lock.
func lockConflict(lk *stacklock.Lock, owner string) string {
	rec, err := lk.Status()
	if err != nil {
		return fmt.Sprintf("read stack lock: %v", err)
	}
	if rec != nil && rec.Owner != owner {
		return fmt.Sprintf("stack is locked by %q (set %s=%s to use it, or wait for them to run `devctl unlock --owner %s`)",
			rec.Owner, stackOwner, rec.Owner, rec.Owner)
	}
	return ""
}
