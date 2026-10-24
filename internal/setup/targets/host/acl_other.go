//go:build !windows

package host

import (
	"fmt"
	"runtime"
)

// The non-Windows side of the ACL binding (row U8b). Only the Windows build has
// a service control manager to protect a service from, and only it has these
// security APIs; the fake in the tests covers the manager's own logic.

// defaultServiceACL refuses: another OS has no service control manager to
// install into, and its mode bits are covered by the project's own rules.
func defaultServiceACL() aclAPI { return unsupportedACL{goos: runtime.GOOS} }

// unsupportedACL is aclAPI on a build without the Windows security APIs.
type unsupportedACL struct{ goos string }

func (a unsupportedACL) Harden(string, []aclTarget) error {
	return fmt.Errorf("%w: a %s build cannot set the DACL of a Windows service's files", ErrUnsupported, a.goos)
}
