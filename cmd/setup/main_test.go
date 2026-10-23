package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestStandaloneRefusesNonLoopback: --standalone with a reachable address is
// refused before the project is touched, which is the row's "refuses a
// non-loopback address" at the command's own edge.
func TestStandaloneRefusesNonLoopback(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:0", "[::]:0", ":0", "192.168.1.5:8090", "piperoom.example.com:8090"} {
		t.Run(addr, func(t *testing.T) {
			t.Setenv("PIPEROOM_HTTP_ADDR", addr)
			dir := t.TempDir()
			err := runWith([]string{"--standalone", "--project", dir}, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "not loopback") {
				t.Fatalf("err = %v, want a non-loopback refusal", err)
			}
			if _, statErr := os.Stat(filepath.Join(dir, "owners.yaml")); !os.IsNotExist(statErr) {
				t.Fatalf("a refused start wrote an owners file: %v", statErr)
			}
		})
	}
}

// --watch is a flag of the standalone server; alone it means nothing.
func TestWatchNeedsStandalone(t *testing.T) {
	err := runWith([]string{"--watch"}, io.Discard)
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
			err := runWith(append(args, "--project", t.TempDir()), io.Discard)
			if err == nil || !strings.Contains(err.Error(), "--standalone runs its own server") {
				t.Fatalf("err = %v, want the pairing-flag refusal", err)
			}
		})
	}
}

// Unexpected positional arguments are refused in every mode.
func TestUnexpectedArguments(t *testing.T) {
	err := runWith([]string{"--standalone", "extra"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "unexpected arguments") {
		t.Fatalf("err = %v, want an unexpected-arguments refusal", err)
	}
}
