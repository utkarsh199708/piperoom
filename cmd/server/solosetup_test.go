package main

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// TestQuickstartSoloHelper is not a test: the --quickstart --solo tests re-run
// this test binary with PIPEROOM_TEST_QUICKSTART_SOLO=1 so runWith executes
// --quickstart --solo in its own process, with noDataStack in place of the real
// stack.
func TestQuickstartSoloHelper(t *testing.T) {
	if os.Getenv("PIPEROOM_TEST_QUICKSTART_SOLO") != "1" {
		return
	}
	os.Args = []string{"server", "--quickstart", "--solo", "--owners", os.Getenv("PIPEROOM_TEST_OWNERS")}
	if err := runWith(startDeps{openStacks: noDataStack, stdout: os.Stdout}); err != nil {
		os.Stdout.WriteString("RUN ERROR: " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(0)
}

// TestDemoSoloHelper is not a test: the --demo --solo tests re-run this test
// binary with PIPEROOM_TEST_DEMO_SOLO=1 so runWith executes --demo --solo in
// its own process (in-memory store, scripted agent, no stack).
func TestDemoSoloHelper(t *testing.T) {
	if os.Getenv("PIPEROOM_TEST_DEMO_SOLO") != "1" {
		return
	}
	os.Args = []string{"server", "--demo", "--solo"}
	if err := runWith(startDeps{openStacks: noDataStack, stdout: os.Stdout}); err != nil {
		os.Stdout.WriteString("RUN ERROR: " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(0)
}

// demoSoloCmd builds a subprocess running --demo --solo with OWNERS_FILE set to
// ownersPath and every other relevant setting cleared from the developer shell.
func demoSoloCmd(ctx context.Context, t *testing.T, ownersPath string, env ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDemoSoloHelper$")
	cmd.Env = append(os.Environ(),
		"PIPEROOM_TEST_DEMO_SOLO=1",
		"OWNERS_FILE="+ownersPath,
		"PIPEROOM_HTTP_ADDR=127.0.0.1:0",
		"USERS_FILE=",
		"PIPEROOM_SOLO_USER=",
		"PIPEROOM_WRITE_MODE=",
		"PIPEROOM_AUTH_MODE=",
		"PIPEROOM_WEB_DIR=",
		"PIPEROOM_LOG_FORMAT=",
		"PIPEROOM_LOG_LEVEL=",
		"PIPEROOM_DEV_ALLOW_NONLOOPBACK=",
		"PIPEROOM_DEV_ACK=",
	)
	cmd.Env = append(cmd.Env, env...)
	return cmd
}

// demoOwnersPath writes the demo's four-owner owners file (dev/owners.yaml
// layers), which agrees with room.DefaultUsers, so --demo --solo exercises the
// relaxed check against a real multi-owner file.
func demoOwnersPath(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	src := `pipelines:
  - id: orders
    title: Orders CDC
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]
    sink_ts_column: updated_at
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
`
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// startSoloServer starts cmd and waits until the server's log shows it
// listening (both --demo and --quickstart log "server: listening"; only
// --quickstart prints a URL). It returns the captured stderr and a stop
// function that shuts the process down (also registered as a cleanup for the
// failure paths).
func startSoloServer(t *testing.T, cmd *exec.Cmd) (stderr *lockedBuffer, stop func()) {
	t.Helper()
	stderr = &lockedBuffer{}
	cmd.Stderr = stderr
	cmd.Stdout = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(exited)
	}()
	stop = func() {
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			_ = cmd.Process.Kill()
		}
		select {
		case <-exited:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
		}
	}
	t.Cleanup(stop)

	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if strings.Contains(stderr.String(), "server: listening") {
			break
		}
		select {
		case <-exited:
			t.Fatalf("server exited before listening: %v\nstderr:\n%s", waitErr, stderr.String())
		case <-deadline:
			t.Fatalf("server did not listen within 10s\nstderr:\n%s", stderr.String())
		case <-ticker.C:
		}
	}
	return stderr, stop
}

func TestSoloFlagsUser(t *testing.T) {
	one := &owners.Owners{Layers: owners.UserLayers(map[string]string{"postgres": "me", "kafka": "me"})}
	many := &owners.Owners{Layers: owners.UserLayers(map[string]string{"postgres": "me", "kafka": "you"})}
	for _, tc := range []struct {
		name                       string
		cfg                        config.Config
		own                        *owners.Owners
		demo, quickstart, soloFlag bool
		want                       string
		wantErr                    string
	}{
		{name: "off", cfg: config.Config{SoloUser: " priya "}, want: "priya"},
		{name: "demo", cfg: config.Config{SoloUser: "priya"}, demo: true, soloFlag: true, want: demoPresenterID},
		{name: "quickstart one user", own: one, quickstart: true, soloFlag: true, want: "me"},
		{name: "quickstart several owners", own: many, quickstart: true, soloFlag: true, wantErr: "--solo needs an owners file that names one user"},
		{name: "quickstart no owners", own: &owners.Owners{}, quickstart: true, soloFlag: true, wantErr: "--solo needs an owners file that names one user"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := soloFlagsUser(tc.cfg, tc.own, tc.demo, tc.quickstart, tc.soloFlag)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("soloFlagsUser = %q, %v; want an error containing %q", got, err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("soloFlagsUser = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

// applySoloFlag resolves the solo user from the flags and warns when --solo
// overrides a PIPEROOM_SOLO_USER that was already set.
func TestApplySoloFlag(t *testing.T) {
	one := &owners.Owners{Layers: owners.UserLayers(map[string]string{"postgres": "me"})}
	many := &owners.Owners{Layers: owners.UserLayers(map[string]string{"postgres": "me", "kafka": "you"})}

	log, buf := testLog()
	cfg := config.Config{SoloUser: "priya"}
	if err := applySoloFlag(&cfg, one, true, false, true, log); err != nil {
		t.Fatalf("applySoloFlag demo: %v", err)
	}
	if cfg.SoloUser != demoPresenterID {
		t.Fatalf("cfg.SoloUser = %q, want %q", cfg.SoloUser, demoPresenterID)
	}
	if !strings.Contains(buf.String(), "both set") {
		t.Fatalf("no --solo/PIPEROOM_SOLO_USER warning:\n%s", buf.String())
	}

	cfg = config.Config{}
	if err := applySoloFlag(&cfg, one, false, true, true, discardLog()); err != nil {
		t.Fatalf("applySoloFlag quickstart: %v", err)
	}
	if cfg.SoloUser != "me" {
		t.Fatalf("cfg.SoloUser = %q, want me", cfg.SoloUser)
	}

	if err := applySoloFlag(&config.Config{}, many, false, true, true, discardLog()); err == nil || !strings.Contains(err.Error(), "--solo needs") {
		t.Fatalf("applySoloFlag several owners: %v", err)
	}

	// Without --solo nothing changes and nothing is logged.
	cfg = config.Config{SoloUser: " priya "}
	log2, buf2 := testLog()
	if err := applySoloFlag(&cfg, one, false, false, false, log2); err != nil || cfg.SoloUser != " priya " || buf2.Len() != 0 {
		t.Fatalf("no --solo: %q, %v, log %q", cfg.SoloUser, err, buf2.String())
	}
}

// --quickstart --solo with a one-user owners file starts, and that user is the
// solo user (the real single-owner check passes because the owners file names
// exactly that user).
func TestQuickstartSoloStartsWithSoleOwner(t *testing.T) {
	ownersPath := writeOwnersFile(t, "user:"+quickstartOwner)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := quickstartCmd(ctx, t, "^TestQuickstartSoloHelper$", ownersPath, t.TempDir(),
		"PIPEROOM_TEST_QUICKSTART_SOLO=1", "PIPEROOM_SOLO_USER=")
	stderr, stop := startSoloServer(t, cmd)
	stop()

	logs := stderr.String()
	for _, want := range []string{"SOLO MODE", "user=" + quickstartOwner} {
		if !strings.Contains(logs, want) {
			t.Errorf("the log lacks %q:\n%s", want, logs)
		}
	}
}

// --quickstart --solo with an owners file that names several owners is refused
// with the --solo message before the users file is written.
func TestQuickstartSoloRefusesMultipleOwners(t *testing.T) {
	ownersPath := writeOwnersFile(t, "user:priya", "user:arjun")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := quickstartCmd(ctx, t, "^TestQuickstartSoloHelper$", ownersPath, t.TempDir(),
		"PIPEROOM_TEST_QUICKSTART_SOLO=1", "PIPEROOM_SOLO_USER=")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "RUN ERROR") || !strings.Contains(string(out), "--solo needs an owners file that names one user") {
		t.Fatalf("--quickstart --solo on a multi-owner owners file: err %v, want a refusal naming --solo\n%s", err, out)
	}
	if strings.Contains(string(out), "http://") {
		t.Fatalf("the refused start printed a URL:\n%s", out)
	}
}

// The --demo --solo flag wiring: runWith resolves the demo presenter through
// the flags, passes demoMode as the relax switch to soloSetup, and the demo's
// multi-owner owners file still lets the presenter run solo.
func TestDemoSoloFlagWiring(t *testing.T) {
	ownersPath := demoOwnersPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := demoSoloCmd(ctx, t, ownersPath)
	stderr, stop := startSoloServer(t, cmd)
	stop()

	logs := stderr.String()
	for _, want := range []string{"SOLO MODE", "user=" + demoPresenterID} {
		if !strings.Contains(logs, want) {
			t.Errorf("the log lacks %q:\n%s", want, logs)
		}
	}
}
