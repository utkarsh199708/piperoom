package ops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

// maxExecOutput caps what an exec op keeps from a child process (the rest is
// discarded, never buffered), so a chatty program cannot exhaust memory.
const maxExecOutput = 64 << 10

// runCommand runs name with an argv slice (never a shell string) and returns
// its combined output, exit code and a start error. A non-zero exit is not an
// error: the caller decides what it means. The program name is always a
// literal in this package; no request names a program.
func runCommand(ctx context.Context, name string, args ...string) ([]byte, int, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	out := &capWriter{max: maxExecOutput}
	cmd.Stdout = out
	cmd.Stderr = out
	cmd.WaitDelay = 2 * time.Second
	err := cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return out.Bytes(), exitErr.ExitCode(), nil
		}
		return out.Bytes(), -1, fmt.Errorf("ops: running %s: %w", name, err)
	}
	return out.Bytes(), 0, nil
}

// capWriter keeps at most max bytes and silently discards the rest. Write
// always reports the full length, so a child sees a successful write.
type capWriter struct {
	buf bytes.Buffer
	max int
	cut bool
}

func (w *capWriter) Write(p []byte) (int, error) {
	orig := len(p)
	if room := w.max - w.buf.Len(); room < len(p) {
		w.cut = true
		if room > 0 {
			w.buf.Write(p[:room])
		}
		return orig, nil
	}
	w.buf.Write(p)
	return orig, nil
}

func (w *capWriter) Bytes() []byte { return w.buf.Bytes() }

func (w *capWriter) Truncated() bool { return w.cut }
