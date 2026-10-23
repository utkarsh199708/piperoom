//go:build !unix

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
)

// runBinary runs path as a child with this process's stdio and environment
// and returns its exit code. There is no exec(2) here, so the umbrella stays
// alive: it ignores os.Interrupt while the child runs, because the console
// delivers Ctrl-C to the child too and the child decides how to stop; the
// umbrella then returns the child's code.
func runBinary(path string, args []string) (int, error) {
	cmd := exec.Command(path, args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	defer signal.Stop(sig)

	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if code := exitErr.ExitCode(); code >= 0 {
				return code, nil
			}
			return exitFailed, nil
		}
		return exitFailed, fmt.Errorf("run %s: %w", path, err)
	}
	return exitOK, nil
}
