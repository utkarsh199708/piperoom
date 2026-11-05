//go:build !windows

package host

// containChild is a no-op where the service host does not exist: other builds
// refuse run-service (winsvc_other.go), so no child is started here.
func containChild(int) (processContainment, error) { return nil, nil }
