//go:build !windows

package osroot

import (
	"os"
	"testing"
)

// On Linux and macOS the Windows forms are ordinary names: os.Root creates
// them inside the root without complaint. Only explicitRefusal makes the
// three CI legs agree.
func TestRootAcceptsWindowsFormsOffWindows(t *testing.T) {
	r, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, name := range []string{"C:foo", "a.yaml:x", "CON", "nul.yaml", `\\host\share`, ".git. ", "PIPERO~1"} {
		if err := r.WriteFile(name, []byte("x"), 0o600); err != nil {
			t.Errorf("os.Root refused %q off Windows: %v", name, err)
		}
		if explicitRefusal(name) == "" {
			t.Errorf("explicitRefusal accepts %q", name)
		}
	}
	// Escapes are os.Root's own job on every OS.
	for _, name := range []string{"../x", "/etc/passwd"} {
		if _, err := r.Open(name); err == nil {
			t.Errorf("os.Root opened %q", name)
		}
	}
}
