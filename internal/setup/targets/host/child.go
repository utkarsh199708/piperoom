package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"
)

// The supervised child of the Windows service (row U8b). It is os/exec with an
// argv slice: the first element is the detected java.exe and every other one is
// a template argument, so there is no command string and no shell anywhere
// (the package's AST test allows os/exec in this file only). The service host
// starts java directly rather than the distribution's connect-*.bat, because a
// .bat needs cmd.exe.

// terminateGrace is how long Terminate waits for the child to exit after it
// asked it to stop, before it kills it. It sits inside the service host's stop
// timeout (25 s) and its wait hint (30 s).
const terminateGrace = 5 * time.Second

// processContainment is a resource that ends the child when it is closed: the
// Job object the Windows build puts the JVM in, so the JVM cannot outlive the
// service host (file: job_windows.go). The other builds have no service host
// and return nil.
type processContainment interface {
	Close() error
}

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
	containment, err := containChild(cmd.Process.Pid)
	if err != nil {
		// A child the host cannot guarantee to stop with itself is not left
		// running: it would keep the REST port bound after the host is gone.
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("host: starting %s: %w", q(argv[0]), err)
	}
	return &processChild{cmd: cmd, done: make(chan struct{}), containment: containment}, nil
}

// processChild is one running JVM.
type processChild struct {
	cmd  *exec.Cmd
	done chan struct{}
	// containment is the Job object the child runs in (nil where the platform
	// has none): closing it kills whatever the job still holds.
	containment processContainment
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
	if c.containment != nil {
		if cerr := c.containment.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("host: releasing the JVM's job object: %w", cerr)
		}
	}
	close(c.done)
	return code, err
}

// Terminate ends the process and returns when it is gone or ctx is done.
//
// It asks first and kills as a last resort. The ask is os.Interrupt, which on
// Unix is SIGINT and reaches a JVM's shutdown hook. A Windows service runs in
// session 0 with no console, so there is neither a Ctrl-Break to send nor a
// window message to post, and Windows' os.Process rejects every signal but
// Kill: the request fails at once and the grace period elapses, after which
// TerminateProcess (what `taskkill /f` does) ends the JVM. Kafka Connect's
// offsets are flushed on an interval (offset.flush.interval.ms) and commit
// durably, so a terminated worker re-reads its committed offsets on the next
// start; the SCM would kill the process at the wait hint regardless.
//
// The Job object from startChild is the second half of that promise: if this
// process dies without running Stop, Windows closes the job and kills the JVM
// with it.
func (c *processChild) Terminate(ctx context.Context) error {
	if c.cmd.Process == nil {
		return nil
	}
	_ = c.cmd.Process.Signal(os.Interrupt)
	grace := time.NewTimer(terminateGrace)
	defer grace.Stop()
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
	case <-grace.C:
	}
	if err := c.cmd.Process.Kill(); err != nil {
		// The JVM may have exited between the grace period and the kill; that
		// is the outcome this call wants, so it is not an error.
		select {
		case <-c.done:
			return nil
		default:
			return fmt.Errorf("host: terminating %s: %w", q(c.cmd.Path), err)
		}
	}
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("host: %s did not exit: %w", q(c.cmd.Path), ctx.Err())
	}
}
