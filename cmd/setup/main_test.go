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
