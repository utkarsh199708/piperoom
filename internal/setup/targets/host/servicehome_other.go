//go:build !windows

package host

import (
	"fmt"
	"runtime"
)

// The non-Windows side of the service home's bindings (row U8b): only Windows
// has a Program Files folder and DACLs to check, so another build refuses both,
// the way it refuses the SCM. The manager's logic is tested with fakes.

func defaultProgramFiles() (string, error) {
	return "", fmt.Errorf("%w: a %s build has no Program Files folder", ErrUnsupported, runtime.GOOS)
}

func defaultTrust() trustAPI { return unsupportedTrust{goos: runtime.GOOS} }

// unsupportedTrust is trustAPI on a build without the Windows security APIs.
type unsupportedTrust struct{ goos string }

func (t unsupportedTrust) CheckTrusted(string, bool) error {
	return fmt.Errorf("%w: a %s build cannot read Windows DACLs", ErrUnsupported, t.goos)
}
