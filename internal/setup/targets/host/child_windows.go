//go:build windows

package host

import "os"

// interruptChild is the graceful stop request, and Windows has none: a service
// process runs in session 0 with no console, os.Process rejects every signal
// but Kill, and a Ctrl-Break would need the host to allocate a console and the
// child to inherit it first (child.go, Terminate, says why that is not worth
// the step). TerminateProcess is therefore the only path, and false tells
// Terminate not to wait for an exit nothing asked for.
func interruptChild(*os.Process) bool { return false }
