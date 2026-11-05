//go:build windows

package host

import (
	"context"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"
)

// TestServiceChildStandIn is not a test of anything: it is the stand-in for a
// long-running JVM. The parent starts this test binary with
// PIPEROOM_CHILD_STANDIN=1 and kills it; without the variable it returns at once
// so a normal run of the package is unaffected.
func TestServiceChildStandIn(t *testing.T) {
	if os.Getenv("PIPEROOM_CHILD_STANDIN") != "1" {
		return
	}
	time.Sleep(time.Minute)
}

// TestProcessChildTerminateKillsAtOnceOnWindows: Windows has no stop request
// (interruptChild), so Terminate must go straight to TerminateProcess instead of
// spending the 5 s grace on a signal nothing answers — 5 s of the SCM's shutdown
// budget, which is a few seconds on current client Windows.
func TestProcessChildTerminateKillsAtOnceOnWindows(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(self, "-test.run=TestServiceChildStandIn")
	cmd.Env = append(os.Environ(), "PIPEROOM_CHILD_STANDIN=1")
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the stand-in child: %v", err)
	}
	containment, err := containChild(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("containChild: %v", err)
	}
	c := &processChild{cmd: cmd, done: make(chan struct{}), containment: containment}
	go func() { _, _ = c.Wait() }()
	if interruptChild(cmd.Process) {
		t.Fatal("interruptChild reported a stop request on Windows; Terminate would then wait for it")
	}
	start := time.Now()
	if err := c.Terminate(context.Background()); err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Terminate took %s: it must kill at once, not wait for a signal Windows does not deliver", elapsed)
	}
}
