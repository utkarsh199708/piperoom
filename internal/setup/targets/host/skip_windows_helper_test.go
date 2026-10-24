package host

import (
	"runtime"
	"testing"
)

// skipOnWindows skips a test of the systemd target. The target is Linux-only
// (unit files, POSIX modes and paths), and a Windows temp directory holds
// backslashes that a unit file refuses, so these tests cannot run there.
func skipOnWindows(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("systemd target is Linux-only: Windows paths and file modes do not apply")
	}
}
