package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/setup/setupcmd"
)

// recorder is a fake process edge: it records what the umbrella ran.
type recorder struct {
	setupArgs []string
	setupErr  error
	ranPath   string
	ranArgs   []string
	ranCode   int
	ranErr    error
	ran       bool
}

// newApp builds an app whose executable lives in dir; it and the named
// siblings are created there as empty files.
func newApp(t *testing.T, rec *recorder, siblings ...string) (app, *bytes.Buffer, *bytes.Buffer, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "piperoom"), nil, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, s := range siblings {
		if err := os.WriteFile(filepath.Join(dir, binaryName(s)), nil, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	var stdout, stderr bytes.Buffer
	a := app{
		stdout: &stdout,
		stderr: &stderr,
		setup: func(args []string, _, _ io.Writer) error {
			rec.setupArgs = append([]string{}, args...)
			return rec.setupErr
		},
		executable: func() (string, error) { return filepath.Join(dir, "piperoom"), nil },
		runBinary: func(path string, args []string) (int, error) {
			rec.ran, rec.ranPath, rec.ranArgs = true, path, append([]string{}, args...)
			return rec.ranCode, rec.ranErr
		},
	}
	return a, &stdout, &stderr, dir
}

func TestDispatchSiblings(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		sibling string
		rest    []string
	}{
		{"server", []string{"server", "--demo"}, "server", []string{"--demo"}},
		{"discover", []string{"discover", "-write-owners", "x.yaml"}, "discover", []string{"-write-owners", "x.yaml"}},
		{"investigate", []string{"investigate", "-pipeline", "orders"}, "investigate", []string{"-pipeline", "orders"}},
		{"no args is the server", nil, "server", nil},
		{"leading flag is the server", []string{"--healthcheck"}, "server", []string{"--healthcheck"}},
		{"subcommand help goes to the binary", []string{"discover", "--help"}, "discover", []string{"--help"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{ranCode: 7}
			a, _, stderr, dir := newApp(t, rec, "server", "discover", "investigate")
			if code := a.dispatch(tt.args); code != 7 {
				t.Fatalf("code = %d, want the sibling's 7 (stderr %q)", code, stderr)
			}
			if want := filepath.Join(dir, binaryName(tt.sibling)); rec.ranPath != want {
				t.Fatalf("ran %q, want %q", rec.ranPath, want)
			}
			if len(tt.rest) == 0 && len(rec.ranArgs) == 0 {
				return
			}
			if !reflect.DeepEqual(rec.ranArgs, tt.rest) {
				t.Fatalf("args = %q, want %q", rec.ranArgs, tt.rest)
			}
		})
	}
}

func TestDispatchSetupInProcess(t *testing.T) {
	rec := &recorder{}
	a, _, _, _ := newApp(t, rec) // no siblings: setup must not need one
	if code := a.dispatch([]string{"setup", "--standalone", "--watch"}); code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if rec.ran {
		t.Fatal("setup executed a binary; it runs in-process")
	}
	if want := []string{"--standalone", "--watch"}; !reflect.DeepEqual(rec.setupArgs, want) {
		t.Fatalf("setup args = %q, want %q", rec.setupArgs, want)
	}
}

func TestDispatchSetupExitCodes(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		code   int
		stderr string
	}{
		{"help", flag.ErrHelp, 0, ""},
		{"failure", errors.New("boom"), 1, "setup: boom\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{setupErr: tt.err}
			a, _, stderr, _ := newApp(t, rec)
			if code := a.dispatch([]string{"setup"}); code != tt.code {
				t.Fatalf("code = %d, want %d", code, tt.code)
			}
			if stderr.String() != tt.stderr {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tt.stderr)
			}
		})
	}
}

// The real setup command behind the umbrella: --help lists piperoom-setup's
// own flags and exits 0, the same as the piperoom-setup binary.
func TestSetupHelpIsPiperoomSetup(t *testing.T) {
	a, _, _, _ := newApp(t, &recorder{})
	var stderr bytes.Buffer
	a.stderr = &stderr
	a.setup = setupcmd.Run
	if code := a.dispatch([]string{"setup", "--help"}); code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	for _, f := range []string{"piperoom-setup", "-standalone", "-code", "-server", "-watch"} {
		if !strings.Contains(stderr.String(), f) {
			t.Fatalf("usage %q lacks %q", stderr.String(), f)
		}
	}
}

func TestHelp(t *testing.T) {
	for _, arg := range []string{"help", "-h", "-help", "--help"} {
		t.Run(arg, func(t *testing.T) {
			rec := &recorder{}
			a, stdout, stderr, _ := newApp(t, rec)
			if code := a.dispatch([]string{arg}); code != 0 {
				t.Fatalf("code = %d, want 0", code)
			}
			for _, sub := range []string{"server", "discover", "investigate", "setup"} {
				if !strings.Contains(stdout.String(), sub) {
					t.Fatalf("usage lacks %q: %q", sub, stdout)
				}
			}
			if stderr.Len() != 0 || rec.ran {
				t.Fatalf("help ran something or wrote stderr %q", stderr)
			}
		})
	}
}

func TestUnknownCommand(t *testing.T) {
	rec := &recorder{}
	a, stdout, stderr, _ := newApp(t, rec, "server")
	if code := a.dispatch([]string{"serve"}); code != exitUsage {
		t.Fatalf("code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), `unknown command "serve"`) || !strings.Contains(stderr.String(), "Usage:") {
		t.Fatalf("stderr = %q, want the command named and the usage", stderr)
	}
	if stdout.Len() != 0 || rec.ran {
		t.Fatal("an unknown command ran something or wrote stdout")
	}
}

func TestMissingSibling(t *testing.T) {
	rec := &recorder{}
	a, _, stderr, _ := newApp(t, rec, "server") // no piperoom-discover
	if code := a.dispatch([]string{"discover"}); code != exitFailed {
		t.Fatalf("code = %d, want %d", code, exitFailed)
	}
	if rec.ran {
		t.Fatal("ran a binary that does not exist")
	}
	msg := stderr.String()
	for _, want := range []string{binaryName("discover"), "not found", "go build -o " + binaryName("discover") + " ./cmd/discover"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("stderr = %q, want %q", msg, want)
		}
	}
}

func TestSiblingIsDirectory(t *testing.T) {
	rec := &recorder{}
	a, _, stderr, dir := newApp(t, rec)
	if err := os.Mkdir(filepath.Join(dir, binaryName("server")), 0o755); err != nil {
		t.Fatal(err)
	}
	if code := a.dispatch([]string{"server"}); code != exitFailed || rec.ran {
		t.Fatalf("code = %d ran = %v, want %d and nothing run (stderr %q)", code, rec.ran, exitFailed, stderr)
	}
}

func TestExecutableError(t *testing.T) {
	rec := &recorder{}
	a, _, stderr, _ := newApp(t, rec)
	a.executable = func() (string, error) { return "", errors.New("no proc") }
	if code := a.dispatch([]string{"investigate"}); code != exitFailed || rec.ran {
		t.Fatalf("code = %d ran = %v, want %d", code, rec.ran, exitFailed)
	}
	if !strings.Contains(stderr.String(), "no proc") {
		t.Fatalf("stderr = %q, want the cause", stderr)
	}
}

func TestRunBinaryError(t *testing.T) {
	rec := &recorder{ranErr: errors.New("exec format error")}
	a, _, stderr, _ := newApp(t, rec, "server")
	if code := a.dispatch([]string{"server"}); code != exitFailed {
		t.Fatalf("code = %d, want %d", code, exitFailed)
	}
	if !strings.Contains(stderr.String(), "exec format error") {
		t.Fatalf("stderr = %q, want the cause", stderr)
	}
}

// A symlinked umbrella (e.g. /usr/local/bin/piperoom -> /opt/piperoom/bin/
// piperoom) finds its siblings beside the real file, not beside the link.
func TestSiblingBesideResolvedExecutable(t *testing.T) {
	rec := &recorder{}
	a, _, stderr, dir := newApp(t, rec, "server")
	real := filepath.Join(dir, "piperoom")
	linkDir := t.TempDir()
	link := filepath.Join(linkDir, "piperoom")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	a.executable = func() (string, error) { return link, nil }
	if code := a.dispatch([]string{"server"}); code != 0 {
		t.Fatalf("code = %d, stderr %q", code, stderr)
	}
	want, err := filepath.EvalSymlinks(filepath.Join(dir, binaryName("server")))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := filepath.EvalSymlinks(rec.ranPath); got != want {
		t.Fatalf("ran %q, want %q", rec.ranPath, want)
	}
}

// An executable path that cannot be resolved is refused, not used as is: a
// sibling beside an unresolved path could come from a link's directory
// rather than the one the umbrella was shipped in.
func TestUnresolvableExecutable(t *testing.T) {
	rec := &recorder{}
	a, _, stderr, _ := newApp(t, rec, "server")
	linkDir := t.TempDir()
	// linkDir/piperoom does not exist, so it cannot be resolved; a sibling
	// sits beside it, which a fallback to the unresolved path would run.
	if err := os.WriteFile(filepath.Join(linkDir, binaryName("server")), nil, 0o755); err != nil {
		t.Fatal(err)
	}
	a.executable = func() (string, error) { return filepath.Join(linkDir, "piperoom"), nil }
	if code := a.dispatch([]string{"server"}); code != exitFailed || rec.ran {
		t.Fatalf("code = %d ran = %v (%q), want %d and nothing run", code, rec.ran, rec.ranPath, exitFailed)
	}
	if !strings.Contains(stderr.String(), "resolving this executable") {
		t.Fatalf("stderr = %q, want the resolve failure", stderr)
	}
}

// The real setup command behind the umbrella keeps piperoom-setup's guards:
// --standalone on a reachable address is refused with exit 1.
func TestSetupStandaloneRefusesNonLoopback(t *testing.T) {
	t.Setenv("PIPEROOM_HTTP_ADDR", "0.0.0.0:0")
	a, _, stderr, _ := newApp(t, &recorder{})
	a.setup = setupcmd.Run
	if code := a.dispatch([]string{"setup", "--standalone", "--project", t.TempDir()}); code != exitFailed {
		t.Fatalf("code = %d, want %d (stderr %q)", code, exitFailed, stderr)
	}
	if !strings.Contains(stderr.String(), "not loopback") {
		t.Fatalf("stderr = %q, want the non-loopback refusal", stderr)
	}
}
