package host

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
)

// TestHardenOrder: every file and directory under a target is hardened, each
// before the directory that holds it, so a jar the person's account extracted
// gets its own owner and DACL instead of keeping theirs.
func TestHardenOrder(t *testing.T) {
	root := filepath.Join(t.TempDir(), DistDir)
	libs := filepath.Join(root, "libs")
	jar := filepath.Join(libs, "kafka-clients.jar")
	if err := os.MkdirAll(libs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(jar, []byte("jar"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := hardenOrder(root)
	if err != nil {
		t.Fatalf("hardenOrder: %v", err)
	}
	want := []string{jar, libs, root}
	if !slices.Equal(got, want) {
		t.Fatalf("hardenOrder = %v, want %v", got, want)
	}

	// A single file (a standalone worker's offset file) is its own list.
	got, err = hardenOrder(jar)
	if err != nil || !slices.Equal(got, []string{jar}) {
		t.Fatalf("hardenOrder(file) = %v, %v; want [%s]", got, err, jar)
	}

	if _, err := hardenOrder(filepath.Join(root, "gone")); err == nil {
		t.Error("hardenOrder of a missing path returned no error")
	}
}

// TestHardenOrderRefusesLinks: a link under a target is refused, never
// followed, since setting its security would change the file it points at.
// Creating a symlink on Windows needs a privilege a CI runner may not hold, so
// the case runs where os.Symlink needs none; the walk is the same code there.
func TestHardenOrderRefusesLinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		return
	}
	root := filepath.Join(t.TempDir(), PluginDir)
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "elsewhere.jar")
	if err := os.WriteFile(outside, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "planted.jar")); err != nil {
		t.Fatal(err)
	}
	_, err := hardenOrder(root)
	if err == nil || !errors.Is(err, ErrDetect) {
		t.Fatalf("hardenOrder with a symlink = %v, want an ErrDetect refusal", err)
	}
}
