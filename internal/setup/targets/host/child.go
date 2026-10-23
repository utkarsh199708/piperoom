package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
)

// The supervised child of the Windows service (row U8b). It is os/exec with an
// argv slice: the first element is the detected java.exe and every other one is
// a template argument, so there is no command string and no shell anywhere
// (the package's AST test allows os/exec in this file only). The service host
// starts java directly rather than the distribution's connect-*.bat, because a
// .bat needs cmd.exe.

// startChild starts argv and returns the running process.
func startChild(argv []string, out io.Writer) (child, error) {
	if len(argv) == 0 {
		return nil, errors.New("host: the service has no command to start")
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	// The JVM's console output (log4j's console appender, a startup crash's
	// stack trace) goes to the service log: a Windows service has no terminal.
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("host: starting %s: %w", q(argv[0]), err)
	}
	return &processChild{cmd: cmd, done: make(chan struct{})}, nil
}

// processChild is one running JVM.
type processChild struct {
	cmd  *exec.Cmd
	done chan struct{}
}

// Wait blocks until the process exits and returns its exit code. An exit with a
// non-zero code is not an error: it is the code the service reports.
func (c *processChild) Wait() (int, error) {
	err := c.cmd.Wait()
	code := 0
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		code, err = exit.ExitCode(), nil
	default:
		err = fmt.Errorf("host: waiting for %s: %w", q(c.cmd.Path), err)
	}
	close(c.done)
	return code, err
}

// Terminate ends the process and returns when it is gone or ctx is done.
//
// It kills rather than signals as a last resort: a Windows service runs in
// session 0 with no console, so a Ctrl-Break or a console control event has no
// listener, and TerminateProcess is what `taskkill /f` does. Kafka Connect's
// offsets are flushed on an interval (offset.flush.interval.ms) and commit
// durably, so a terminated worker re-reads its committed offsets on the next
// start; the SCM would kill the process at the wait hint regardless.
func (c *processChild) Terminate(ctx context.Context) error {
	if c.cmd.Process == nil {
		return nil
	}
	_ = c.cmd.Process.Kill()
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("host: %s did not exit: %w", q(c.cmd.Path), ctx.Err())
	}
}
