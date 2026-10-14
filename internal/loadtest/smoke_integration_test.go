//go:build integration

package loadtest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
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
	// SIGINT/SIGTERM (a CI cancel) end the run through ctx, so the deferred
	// env.Close removes the container; a run killed outright leaves it for
	// the next NewEnv to remove (RemoveStale).
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(sigCtx, 3*time.Minute)
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

// TestRemoveStale starts a container labelled as owned by a process that has
// exited and one owned by this process, and checks that RemoveStale removes
// only the first.
func TestRemoveStale(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := DockerAvailable(ctx); err != nil {
		t.Skip(err)
	}
	dead := exec.Command("true")
	if err := dead.Run(); err != nil {
		t.Fatal(err)
	}
	prefix := "s2lt-stale-" + randHex(3)
	start := func(owner int) string {
		name := fmt.Sprintf("%s-pg-%s", prefix, randHex(3))
		if _, err := docker(ctx, "run", "-d", "--rm", "--name", name, "--label", labelPrefix+"="+prefix,
			"--label", fmt.Sprintf("%s=%d", labelOwner, owner), "--entrypoint", "sleep", PostgresImage, "120"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = docker(context.Background(), "rm", "-f", name) })
		return name
	}
	stale := start(dead.Process.Pid)
	live := start(os.Getpid())
	removed, err := RemoveStale(ctx, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0] != stale {
		t.Fatalf("removed %v, want only %s", removed, stale)
	}
	out, err := docker(ctx, "ps", "-a", "--filter", "label="+labelPrefix+"="+prefix, "--format", "{{.Names}}")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, stale) || !strings.Contains(out, live) {
		t.Fatalf("containers left: %q", out)
	}
}
