//go:build !unix

package loadtest

// processAlive cannot be checked here; report every owner as alive so the
// stale-container cleanup never removes a container that is in use.
func processAlive(int) bool { return true }
