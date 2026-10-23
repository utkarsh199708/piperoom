package setuptools

import (
	"os/exec"
	"strings"
	"testing"
)

// forbiddenDeps are packages this one must never import, directly or
// transitively. internal/room imports internal/agent, and internal/wiring
// imports internal/room through alertin/connprobe; either way one import of
// room would make both internal/agent (U1-10's tools) and internal/wiring
// (SetupStack) unable to import this package. remoteclients is listed because
// it still imports room.
var forbiddenDeps = []string{
	"piperoom/internal/room",
	"piperoom/internal/agent",
	"piperoom/internal/wiring",
	"piperoom/internal/setup/remoteclients",
}

// TestPackageHasNoRoomDependency is the import-cycle guard: it asks the go
// tool for this package's full build dependency list (test files excluded) and
// fails if any package the room side owns appears. go list runs the same
// module resolution a build does, so the test fails exactly when the import
// comes back.
func TestPackageHasNoRoomDependency(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps .: %v\n%s", err, out)
	}
	deps := make(map[string]bool)
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			deps[line] = true
		}
	}
	// The probe must have listed this package's own dependencies, or the
	// check below would pass on an empty answer.
	if !deps["piperoom/internal/setup/protocol"] || !deps["piperoom/internal/tools"] {
		t.Fatalf("go list -deps . did not list this package's dependencies: %d lines", len(deps))
	}
	for _, forbidden := range forbiddenDeps {
		if deps[forbidden] {
			t.Errorf("this package depends on %s; internal/agent and internal/wiring could then never import it", forbidden)
		}
	}
}
