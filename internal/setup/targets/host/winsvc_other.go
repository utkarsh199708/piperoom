//go:build !windows

package host

import (
	"fmt"
	"runtime"
)

// The non-Windows side of the service binding (row U8b). The Windows manager
// and the service host are platform-independent code tested on every OS with a
// fake; the two bindings below are the only pieces that need the real Windows
// API, so a build for another OS refuses them.

// defaultServiceAPI is the service control manager binding, which only the
// Windows build has: a test that forces Options.GOOS="windows" on another OS
// gets this refusal from the one call that needs the real SCM (the fake API
// covers the manager's own logic).
func defaultServiceAPI() (serviceAPI, error) {
	return nil, fmt.Errorf("%w: a %s build has no Windows service control manager", ErrUnsupported, runtime.GOOS)
}

// platformServiceHost is svc.Run and svc.IsWindowsService, which only the
// Windows build has: run-service refuses on every other OS.
type platformServiceHost struct{}

func (platformServiceHost) IsService() (bool, error) {
	return false, fmt.Errorf("%w: a %s build cannot run as a Windows service", ErrUnsupported, runtime.GOOS)
}

func (platformServiceHost) Run(string, *serviceSupervisor) error {
	return fmt.Errorf("%w: a %s build cannot run as a Windows service", ErrUnsupported, runtime.GOOS)
}
