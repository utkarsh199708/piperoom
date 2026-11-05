//go:build !windows

package host

import (
	"errors"
	"os"
	"strings"
	"testing"

	"piperoom/internal/setup/ops"
)

// TestJavaHomeConfinedRefusesAnUnresolvableProject: when the project
// directory can no longer be resolved, a JDK reached through a symlinked
// parent could not be told apart from one outside the project, so the check
// fails closed instead of comparing the unresolved spelling only. (The
// directory is moved while its root is open, which Windows does not allow.)
func TestJavaHomeConfinedRefusesAnUnresolvableProject(t *testing.T) {
	dir := t.TempDir() + "/proj"
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	c, err := ops.OpenConfiner(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := os.Rename(dir, dir+"-moved"); err != nil {
		t.Fatal(err)
	}
	err = javaHomeConfined(ops.Env{Confine: c}, t.TempDir()+"/jdk")
	if !errors.Is(err, ErrDetect) || !strings.Contains(err.Error(), "resolving the project directory") {
		t.Fatalf("javaHomeConfined with an unresolvable project: %v, want ErrDetect", err)
	}
}
