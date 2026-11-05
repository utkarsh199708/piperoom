//go:build windows

package ops

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// A junction is a reparse point and is refused by the walk (ModeIrregular
// since Go 1.23). Windows only, so this file is build-tagged rather than
// skipped: the full-stack CI job fails on any skipped test, and off Windows
// there is nothing for this test to prove. Still skipped when mklink /J is
// unavailable on Windows itself.
func TestConfinerRefusesJunction(t *testing.T) {
	proj, outside := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	junction := filepath.Join(proj, "jn")
	if out, err := exec.Command("cmd", "/c", "mklink", "/J", junction, outside).CombinedOutput(); err != nil {
		t.Skipf("mklink /J: %v: %s", err, out)
	}
	c, err := OpenConfiner(proj)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err := c.prepare("jn/secret.txt"); !errors.Is(err, ErrPath) {
		t.Fatalf("prepare = %v, want ErrPath", err)
	}
}
