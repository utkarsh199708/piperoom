//go:build integration

package main

// The Linux run of the host-target setup breaks (U8c): on the fake host
// container (compose profile "host", dev/host), each break makes its named
// check fail and its fix makes it pass again. It starts the profile itself
// (`docker compose --profile host up -d --build --wait host`), so the job that
// runs it needs only a Docker daemon; without one the test skips like every
// Compose integration test.

import (
	"context"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// requireDockerDaemon skips the test when Docker is not installed or its
// daemon is unreachable. The fake host is a container, not a Compose service
// of the dev stack, so no other testenv helper applies.
func requireDockerDaemon(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker is not on PATH: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput(); err != nil {
		t.Skipf("the docker daemon is not reachable: %v\n%.200s", err, out)
	}
}

// hostBreakDetail is what each named check must report after its break: the
// argv, the properties key or the version (the words the U8d labels use).
var hostBreakDetail = map[string]string{
	"wrong-standalone-args": "connect-distributed.properties",
	"plugin-path-unset":     "no plugin.path",
	"java-too-old":          "reports java 11",
}

func TestIntegrationHostSetupBreaks(t *testing.T) {
	requireDockerDaemon(t)
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	s := &stack{root: root, log: slog.New(slog.NewTextHandler(io.Discard, nil)), poll: 500 * time.Millisecond}
	acquireStackLock(t)
	// The first prepare builds the image and pulls the JRE, so the budget is
	// generous; the breaks themselves are file writes.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	// The fake host keeps its project files across `devctl down`; a fix is the
	// way back, and it is idempotent, so heal whatever an earlier run left.
	for _, name := range hostSetupNames {
		if err := setupFixCmd(ctx, s, name); err != nil {
			t.Fatalf("baseline setup-fix %s: %v", name, err)
		}
	}
	for _, name := range hostSetupNames {
		sc := setupScenarios[name]
		if ok, detail, err := sc.verify(ctx, s); err != nil || !ok {
			t.Fatalf("check %s on the fake host as it comes up: ok=%v %s err=%v", sc.check, ok, detail, err)
		}
	}

	for _, name := range hostSetupNames {
		t.Run(name, func(t *testing.T) {
			sc := setupScenarios[name]
			t.Cleanup(func() {
				cctx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
				defer stop()
				if err := setupFixCmd(cctx, s, name); err != nil {
					t.Errorf("cleanup fix %s: %v", name, err)
				}
			})

			start := time.Now()
			if err := setupBreakCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-break %s: %v", name, err)
			}
			ok, detail, err := sc.verify(ctx, s)
			if err != nil || ok {
				t.Fatalf("check %s after the break: ok=%v %s err=%v", sc.check, ok, detail, err)
			}
			if want := hostBreakDetail[name]; !strings.Contains(detail, want) {
				t.Errorf("check %s reports %q, want it to name %q", sc.check, detail, want)
			}
			t.Logf("break %s: %s, check %s fails: %s", name, time.Since(start).Round(time.Millisecond), sc.check, detail)

			start = time.Now()
			if err := setupFixCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-fix %s: %v", name, err)
			}
			if ok, detail, err := sc.verify(ctx, s); err != nil || !ok {
				t.Fatalf("check %s after the fix: ok=%v %s err=%v", sc.check, ok, detail, err)
			}
			t.Logf("fix %s: %s", name, time.Since(start).Round(time.Millisecond))
		})
	}
}
