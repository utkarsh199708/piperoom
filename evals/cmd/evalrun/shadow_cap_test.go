package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/evals"
)

// A response larger than the cap is refused by the byte cap, not by a parse
// error: the file is valid JSON, and only the size is wrong. This is the
// io.LimitReader in runFromShadow.
func TestRunFromShadowInputCap(t *testing.T) {
	raw, err := os.ReadFile(shadowSample)
	if err != nil {
		t.Fatal(err)
	}
	pad := `,"pad":"` + strings.Repeat("x", evals.MaxShadowInputBytes) + `"}`
	big := append(bytes.TrimRight(bytes.TrimSpace(raw), "}"), pad...)
	path := filepath.Join(t.TempDir(), "big.json")
	if err := os.WriteFile(path, big, 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	code, err := run(flags{fromShadow: path, owners: devOwners}, &out, &out)
	if err == nil || code == 0 || !strings.Contains(err.Error(), "exceeds the") {
		t.Fatalf("code %d err %v", code, err)
	}
	if out.Len() != 0 {
		t.Errorf("wrote %d bytes on failure", out.Len())
	}
}
