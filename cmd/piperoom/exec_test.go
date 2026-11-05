package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// helperEnv makes the test binary act as a process in TestRunBinary instead of
// running the tests: "run" calls the real runBinary on the test binary itself
// with helperEnv set to "echo", and "echo" prints what it was started with and
// exits 3.
const helperEnv = "PIPEROOM_UMBRELLA_HELPER"

func TestMain(m *testing.M) {
	switch os.Getenv(helperEnv) {
	case "run":
		self, err := os.Executable()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(97)
		}
		os.Setenv(helperEnv, "echo")
		code, err := runBinary(self, []string{"alpha", "beta gamma"})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(98)
		}
		os.Exit(code)
	case "echo":
		fmt.Printf("%s|%q|%s\n", os.Args[0], os.Args[1:], os.Getenv("PIPEROOM_UMBRELLA_FOO"))
		os.Exit(3)
	}
	os.Exit(m.Run())
}

// The real runBinary of this OS (exec on Unix, a child elsewhere) starts the
// sibling with its path as argv[0], the arguments unchanged and this
// process's environment, and the sibling's exit code becomes the process's.
func TestRunBinary(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(self)
	cmd.Env = append(os.Environ(), helperEnv+"=run", "PIPEROOM_UMBRELLA_FOO=bar baz")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 3 {
		t.Fatalf("err = %v, want exit code 3 (stderr %q)", err, stderr.String())
	}
	if want := self + `|["alpha" "beta gamma"]|bar baz`; strings.TrimSpace(stdout.String()) != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
}
