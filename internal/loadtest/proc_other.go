//go:build !unix

package loadtest

// processAlive cannot be checked here; report every positive pid as alive
// so the stale-container cleanup never removes a container that is in use.
// Non-positive pids are never alive.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return true
}
