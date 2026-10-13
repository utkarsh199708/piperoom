//go:build integration

package loadtest

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// serveSwitch makes the test binary act as a server process when the
// launcher re-executes it.
const serveSwitch = "LOADTEST_TEST_SERVE"

func TestMain(m *testing.M) {
	if os.Getenv(serveSwitch) == "1" {
		if err := ServeMain(os.Stdin, os.Stdout); err != nil {
			os.Stderr.WriteString(err.Error() + "\n")
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// TestSmokeAllProfiles runs every profile at its -short size against real
// server processes on a throwaway postgres:16 (or LOADTEST_DB_URL), and
// requires every correctness check to pass. It is the CI-sized proof that
// the harness works end to end, not a capacity measurement.
func TestSmokeAllProfiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dbURL := os.Getenv("LOADTEST_DB_URL")
	if dbURL == "" {
		if err := DockerAvailable(ctx); err != nil {
			t.Skipf("no LOADTEST_DB_URL and %v", err)
		}
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	env, err := NewEnv(ctx, EnvConfig{
		DBURL: dbURL, Prefix: "s2lt-it", Short: true, TmpfsData: true, LogDir: out + "/logs",
		Command: func() (string, []string, []string) {
			return self, []string{"-test.run=^$"}, []string{serveSwitch + "=1"}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	start := time.Now()
	results, err := RunProfiles(ctx, env, Profiles(), nil, out)
	if err != nil {
		t.Fatalf("RunProfiles: %v (server logs in %s)", err, out)
	}
	if len(results) != len(Profiles()) {
		t.Fatalf("got %d results, want %d", len(results), len(Profiles()))
	}
	for _, r := range results {
		if len(r.Checks) == 0 {
			t.Errorf("%s: no checks recorded", r.Profile)
		}
		for _, c := range r.Failed() {
			t.Errorf("%s: check %q failed: %s", r.Profile, c.Name, c.Detail)
		}
		if !strings.Contains(r.Markdown, "## Checks") {
			t.Errorf("%s: markdown report has no checks section", r.Profile)
		}
		if _, err := os.Stat(out + "/" + r.Profile + ".json"); err != nil {
			t.Errorf("%s: no JSON report: %v", r.Profile, err)
		}
	}
	t.Logf("smoke run took %s", time.Since(start).Round(time.Millisecond))
}
