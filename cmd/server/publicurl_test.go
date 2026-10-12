package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestRunHelper is not a test: the tests below re-run this test binary with
// PIPEROOM_TEST_RUN=1 so run() executes in its own process (it parses flags and
// would otherwise clash with the test flags).
func TestRunHelper(t *testing.T) {
	if os.Getenv("PIPEROOM_TEST_RUN") != "1" {
		t.Skip("helper for subprocess tests")
	}
	os.Args = []string{"server", "--demo"}
	if err := run(); err != nil {
		os.Stdout.WriteString("RUN ERROR: " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(0)
}

func runServer(t *testing.T, publicURL string) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRunHelper$")
	cmd.Env = append(os.Environ(),
		"PIPEROOM_TEST_RUN=1",
		"PIPEROOM_PUBLIC_URL="+publicURL,
		"PIPEROOM_HTTP_ADDR=127.0.0.1:0",
		"OWNERS_FILE=../../dev/owners.yaml",
		"USERS_FILE=../../dev/users.yaml",
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestStartRefusesWildcardPublicURL(t *testing.T) {
	for _, u := range []string{"http://*", "https://*.corp.example", "https://piperoom.*", "https://user:pw@x.example"} {
		out, err := runServer(t, u)
		if err == nil {
			t.Errorf("PIPEROOM_PUBLIC_URL=%q: server started, want a start-up error\n%s", u, out)
			continue
		}
		if !strings.Contains(out, "RUN ERROR") || !strings.Contains(out, "PIPEROOM_PUBLIC_URL") {
			t.Errorf("PIPEROOM_PUBLIC_URL=%q: error does not name the variable:\n%s", u, out)
		}
	}
}
