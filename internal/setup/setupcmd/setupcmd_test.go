package setupcmd

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
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
