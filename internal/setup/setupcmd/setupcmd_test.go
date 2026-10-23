package setupcmd

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/setup/targets/host"
)

// --help prints the usage under the piperoom-setup name and exits 0, from
// either binary.
func TestHelpExitsZero(t *testing.T) {
	var stderr bytes.Buffer
	err := Run([]string{"--help"}, io.Discard, &stderr)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
	if !strings.Contains(stderr.String(), "piperoom-setup") || !strings.Contains(stderr.String(), "-standalone") {
		t.Fatalf("usage = %q, want the piperoom-setup flags", stderr.String())
	}
	var out bytes.Buffer
	if code := ExitCode(err, &out); code != 0 || out.Len() != 0 {
		t.Fatalf("ExitCode(help) = %d, printed %q; want 0 and nothing", code, out.String())
	}
}

func TestExitCode(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		code  int
		print string
	}{
		{"nil", nil, 0, ""},
		{"help", flag.ErrHelp, 0, ""},
		{"wrapped help", errors.Join(errors.New("x"), flag.ErrHelp), 0, ""},
		{"failure", errors.New("boom"), 1, "setup: boom\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if got := ExitCode(tt.err, &buf); got != tt.code {
				t.Fatalf("code = %d, want %d", got, tt.code)
			}
			if buf.String() != tt.print {
				t.Fatalf("printed %q, want %q", buf.String(), tt.print)
			}
		})
	}
}

// A bad flag is a parse error, reported on stderr, exit 1.
func TestBadFlag(t *testing.T) {
	var stderr bytes.Buffer
	err := Run([]string{"--no-such-flag"}, io.Discard, &stderr)
	if err == nil || errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want a parse error", err)
	}
	if code := ExitCode(err, io.Discard); code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "no-such-flag") {
		t.Fatalf("stderr = %q, want the flag named", stderr.String())
	}
}

// --standalone with a reachable address is refused before the project is
// touched, for piperoom-setup and "piperoom setup" alike.
func TestStandaloneRefusesNonLoopback(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:0", "[::]:0", ":0", "192.168.1.5:8090", "piperoom.example.com:8090"} {
		t.Run(addr, func(t *testing.T) {
			t.Setenv("PIPEROOM_HTTP_ADDR", addr)
			dir := t.TempDir()
			err := Run([]string{"--standalone", "--project", dir}, io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "not loopback") {
				t.Fatalf("err = %v, want a non-loopback refusal", err)
			}
			if code := ExitCode(err, io.Discard); code != 1 {
				t.Fatalf("code = %d, want 1", code)
			}
			if _, statErr := os.Stat(filepath.Join(dir, "owners.yaml")); !os.IsNotExist(statErr) {
				t.Fatalf("a refused start wrote an owners file: %v", statErr)
			}
		})
	}
}

// --watch is a flag of the standalone server; alone it means nothing.
func TestWatchNeedsStandalone(t *testing.T) {
	err := Run([]string{"--watch"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "--watch needs --standalone") {
		t.Fatalf("err = %v, want a refusal naming --standalone", err)
	}
}

// --standalone runs its own server, so pairing flags cannot be combined with
// it: they would silently point nowhere.
func TestStandaloneRejectsPairingFlags(t *testing.T) {
	for _, args := range [][]string{
		{"--standalone", "--server", "http://127.0.0.1:8090"},
		{"--standalone", "--code", "ABCDEFGH"},
		{"--standalone", "--label", "laptop"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			err := Run(append(args, "--project", t.TempDir()), io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "--standalone runs its own server") {
				t.Fatalf("err = %v, want the pairing-flag refusal", err)
			}
		})
	}
}

// Unexpected positional arguments are refused in every mode.
func TestUnexpectedArguments(t *testing.T) {
	for _, args := range [][]string{{"--standalone", "extra"}, {"extra"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			err := Run(args, io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "unexpected arguments") {
				t.Fatalf("err = %v, want an unexpected-arguments refusal", err)
			}
		})
	}
}

// TestRunDispatchesRunService: the Windows service control manager starts
// `piperoom-setup run-service --project ...` (row U8b), so every build of the
// runner answers that subcommand before it parses the runner's own flags — a
// console run of it is refused with a reason instead of a flag error.
func TestRunDispatchesRunService(t *testing.T) {
	err := Run([]string{"run-service"}, io.Discard, io.Discard)
	if err == nil || !errors.Is(err, host.ErrParams) || !strings.Contains(err.Error(), "--project") {
		t.Fatalf("Run(run-service) = %v, want the run-service --project refusal, not a flag error", err)
	}
	err = Run([]string{"run-service", "--project", t.TempDir()}, io.Discard, io.Discard)
	if err == nil || !errors.Is(err, host.ErrParams) || !strings.Contains(err.Error(), "--java") {
		t.Fatalf("Run(run-service --project) = %v, want the run-service --java refusal", err)
	}
}
