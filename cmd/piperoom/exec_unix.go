//go:build unix

package main

import (
	"fmt"
	"os"
	"syscall"
)

// runBinary replaces this process with path, so the sibling keeps the PID
// (PID 1 in a container), receives signals directly and its exit code is the
// process's. It returns only when the exec fails.
func runBinary(path string, args []string) (int, error) {
	argv := append([]string{path}, args...)
	if err := syscall.Exec(path, argv, os.Environ()); err != nil {
		return exitFailed, fmt.Errorf("exec %s: %w", path, err)
	}
	return exitOK, nil // unreachable: a successful exec does not return
}
