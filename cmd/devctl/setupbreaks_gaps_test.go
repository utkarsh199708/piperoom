package main

// Tests for the setup-break paths the #880 mutation review found untested:
// the CLI dispatch and its lock guard, the guards that prove a check flipped,
// the wal-level-replica break and fix, the check branches, the half-applied
// plugin-after-scan recovery, and the host/port guard in front of the shell.

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/stacklock"
)

// lockStack takes the stack lock for owner in a temp lock file, so
// runCommand's requireLock passes.
func lockStack(t *testing.T, owner string) {
	t.Helper()
	t.Setenv(stacklock.EnvPath, filepath.Join(t.TempDir(), "stack.lock"))
	if err := lockCmd([]string{"--owner", owner}); err != nil {
		t.Fatalf("lock: %v", err)
	}
}

// TestSetupCommandsDispatch: setup-break breaks and setup-fix heals, through
// runCommand (not the command functions), with the lock held.
func TestSetupCommandsDispatch(t *testing.T) {
	lockStack(t, "alice")
	w := healthySetupWorld()
	s := testSetupStack(t, w)
	ctx := context.Background()
	sc := setupScenarios["flat-plugin-dir"]

	if err := runCommand(ctx, s, "setup-break", []string{"--owner=alice", "flat-plugin-dir"}); err != nil {
		t.Fatalf("setup-break: %v", err)
	}
	if ok, detail, err := sc.verify(ctx, s); err != nil || ok {
		t.Fatalf("after setup-break the check passes: ok=%v %s err=%v", ok, detail, err)
	}
	if err := runCommand(ctx, s, "setup-fix", []string{"--owner=alice", "flat-plugin-dir"}); err != nil {
		t.Fatalf("setup-fix: %v", err)
	}
	if ok, detail, err := sc.verify(ctx, s); err != nil || !ok {
		t.Fatalf("after setup-fix the check fails: ok=%v %s err=%v", ok, detail, err)
	}
}

// TestSetupCommandsRequireLock: neither command touches the stack without
// the lock.
func TestSetupCommandsRequireLock(t *testing.T) {
	t.Setenv(stacklock.EnvPath, filepath.Join(t.TempDir(), "stack.lock"))
	for _, cmd := range []string{"setup-break", "setup-fix"} {
		w := healthySetupWorld()
		s := testSetupStack(t, w)
		err := runCommand(context.Background(), s, cmd, []string{"--owner=alice", "flat-plugin-dir"})
		if err == nil || !strings.Contains(err.Error(), "not locked") {
			t.Fatalf("%s without the lock = %v, want 'not locked'", cmd, err)
		}
		if len(w.execUsers) != 0 || len(w.httpCalls) != 0 || len(w.composeCall) != 0 {
			t.Fatalf("%s without the lock reached the stack: exec %v http %v compose %v", cmd, w.execUsers, w.httpCalls, w.composeCall)
		}
	}
}

func TestSetupCommandArgErrors(t *testing.T) {
	t.Setenv(stacklock.EnvPath, filepath.Join(t.TempDir(), "stack.lock"))
	st := &stack{}
	for _, cmd := range []string{"setup-break", "setup-fix"} {
		for _, tc := range []struct {
			args []string
			want string
		}{
			{nil, "usage: devctl " + cmd + " <scenario>"},
			{[]string{"a", "b"}, "usage: devctl " + cmd + " <scenario>"},
			// The name is checked before the lock, like break/fix.
			{[]string{"no-such-break"}, `unknown setup-break scenario "no-such-break"`},
		} {
			err := runCommand(context.Background(), st, cmd, tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%s %v = %v, want containing %q", cmd, tc.args, err, tc.want)
			}
		}
	}
}

// withSetupScenario registers a temporary scenario for one test.
func withSetupScenario(t *testing.T, sc setupScenario) {
	t.Helper()
	if _, dup := setupScenarios[sc.name]; dup {
		t.Fatalf("scenario %q already registered", sc.name)
	}
	setupScenarios[sc.name] = sc
	t.Cleanup(func() { delete(setupScenarios, sc.name) })
}

// TestSetupCommandsProveTheCheckFlipped: a break or fix that changes nothing
// is an error, and so is a check that cannot run after it.
func TestSetupCommandsProveTheCheckFlipped(t *testing.T) {
	ctx := context.Background()
	s := testSetupStack(t, healthySetupWorld())
	noop := func(context.Context, *stack) error { return nil }
	state := true
	withSetupScenario(t, setupScenario{
		name: "test-noop", check: "test-check", breakIt: noop, fix: noop,
		verify: func(context.Context, *stack) (bool, string, error) { return state, "state", nil },
	})

	state = true
	if err := setupBreakCmd(ctx, s, "test-noop"); err == nil || !strings.Contains(err.Error(), "did not make check test-check fail") {
		t.Errorf("no-op break = %v, want 'did not make check test-check fail'", err)
	}
	state = false
	if err := setupFixCmd(ctx, s, "test-noop"); err == nil || !strings.Contains(err.Error(), "did not make check test-check pass") {
		t.Errorf("no-op fix = %v, want 'did not make check test-check pass'", err)
	}

	// A check that passes (break) or fails (fix) before the step and cannot
	// run after it is an error, not a proof.
	var first bool
	calls := 0
	withSetupScenario(t, setupScenario{
		name: "test-verify-error", check: "test-check-2", breakIt: noop, fix: noop,
		verify: func(context.Context, *stack) (bool, string, error) {
			calls++
			if calls == 1 {
				return first, "", nil
			}
			return false, "", errors.New("component down")
		},
	})
	first, calls = true, 0
	if err := setupBreakCmd(ctx, s, "test-verify-error"); err == nil || !strings.Contains(err.Error(), "check test-check-2 after the break") {
		t.Errorf("break with a failing check = %v", err)
	}
	first, calls = false, 0
	if err := setupFixCmd(ctx, s, "test-verify-error"); err == nil || !strings.Contains(err.Error(), "check test-check-2 after the fix") {
		t.Errorf("fix with a failing check = %v", err)
	}
}

// TestWALLevelOverrideSetsReplica reads the override as Compose would: the
// postgres command sets wal_level=replica and keeps the slot and sender
// limits of the base file.
func TestWALLevelOverrideSetsReplica(t *testing.T) {
	var doc struct {
		Services map[string]struct {
			Command []string `yaml:"command"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(walLevelOverride), &doc); err != nil {
		t.Fatal(err)
	}
	cmd := strings.Join(doc.Services["postgres"].Command, " ")
	for _, want := range []string{"postgres", "-c wal_level=replica", "-c max_replication_slots=", "-c max_wal_senders="} {
		if !strings.Contains(cmd, want) {
			t.Errorf("postgres command %q lacks %q", cmd, want)
		}
	}
	if len(doc.Services) != 1 {
		t.Errorf("override changes %d services, want only postgres", len(doc.Services))
	}
}

func TestComposeWithArgv(t *testing.T) {
	got := strings.Join(composeWithArgv("/repo", "/tmp/o.yaml", "up", "-d", "--no-deps", "postgres"), " ")
	if want := "compose -f /repo/dev/docker-compose.yml -f /tmp/o.yaml up -d --no-deps postgres"; got != want {
		t.Errorf("argv with override = %q, want %q", got, want)
	}
	got = strings.Join(composeWithArgv("/repo", "", "up", "-d", "--no-deps", "postgres"), " ")
	if want := "compose -f /repo/dev/docker-compose.yml up -d --no-deps postgres"; got != want {
		t.Errorf("argv without override = %q, want %q", got, want)
	}
}

// TestWALLevelFixOrder: the fix stops the connector, recreates the slot,
// clears the stored offsets (KAN-411: they point into the dropped slot), sets
// snapshot.mode=when_needed, then resumes, in that order.
func TestWALLevelFixOrder(t *testing.T) {
	w := healthySetupWorld()
	s := testSetupStack(t, w)
	ctx := context.Background()
	if err := setupBreakCmd(ctx, s, "wal-level-replica"); err != nil {
		t.Fatal(err)
	}
	if w.slotExists {
		t.Fatal("the break left the slot")
	}
	w.httpCalls, w.sqlCalls = nil, nil
	if err := setupFixCmd(ctx, s, "wal-level-replica"); err != nil {
		t.Fatal(err)
	}
	if mode := w.connector["snapshot.mode"]; mode != "when_needed" {
		t.Errorf("snapshot.mode = %v, want when_needed", mode)
	}
	created := false
	for _, q := range w.sqlCalls {
		created = created || q == sqlCreateSlot
	}
	if !created {
		t.Errorf("the fix did not create the slot: %v", w.sqlCalls)
	}
	base := "/connectors/" + connectorName
	order := []string{"PUT " + base + "/stop", "DELETE " + base + "/offsets", "PUT " + base + "/config", "PUT " + base + "/resume"}
	at := 0
	for _, c := range w.httpCalls {
		if at < len(order) && c == order[at] {
			at++
		}
	}
	if at != len(order) {
		t.Errorf("Connect calls %v do not hold %v in order (matched %d)", w.httpCalls, order, at)
	}
}

// TestSourceRoleGrantsBranches: a role that cannot log in, and one that does
// not exist, both fail the grants check and say why.
func TestSourceRoleGrantsBranches(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name, rolesRow, want string
	}{
		{"no login", "f|t", "missing LOGIN"},
		{"no such role", "", "does not exist"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := healthySetupWorld()
			s := testSetupStack(t, w)
			s.sqlFn = func(ctx context.Context, db string, qs ...string) (string, error) {
				if strings.HasPrefix(qs[0], "SELECT rolcanlogin, rolreplication") {
					return tc.rolesRow, nil
				}
				return w.sql(ctx, db, qs...)
			}
			ok, detail, err := verifySourceRoleGrants(ctx, s)
			if err != nil || ok || !strings.Contains(detail, tc.want) {
				t.Fatalf("ok=%v detail=%q err=%v, want a failure naming %q", ok, detail, err, tc.want)
			}
		})
	}
}

// TestConverterCheckNamesTheMissingJar: only debezium-api gone is a failure
// naming that jar alone.
func TestConverterCheckNamesTheMissingJar(t *testing.T) {
	w := healthySetupWorld()
	var kept []string
	for _, j := range w.dirJars {
		if !strings.HasPrefix(j, "debezium-api-") {
			kept = append(kept, j)
		}
	}
	w.dirJars = kept
	s := testSetupStack(t, w)
	ok, detail, err := verifyConverterJars(context.Background(), s)
	if err != nil || ok || !strings.Contains(detail, "debezium-api-3.2.0.Final.jar") || strings.Contains(detail, "debezium-core") {
		t.Fatalf("ok=%v detail=%q err=%v", ok, detail, err)
	}
}

// TestReachCheckWithoutBash: an image without bash (exit 127) falls back to
// the name lookup and to the task's own trace.
func TestReachCheckWithoutBash(t *testing.T) {
	ctx := context.Background()
	t.Run("host does not resolve", func(t *testing.T) {
		w := healthySetupWorld()
		s := testSetupStack(t, w)
		s.execAsFn = func(ctx context.Context, user, container string, argv ...string) (string, int, error) {
			switch argv[0] {
			case "bash":
				return "bash: not found", 127, nil
			case "getent":
				return "", 2, nil
			}
			return w.exec(ctx, container, argv...)
		}
		ok, detail, err := verifyConnectSourceReachable(ctx, s)
		if err != nil || ok || !strings.Contains(detail, "does not resolve") {
			t.Fatalf("ok=%v detail=%q err=%v", ok, detail, err)
		}
	})
	t.Run("task trace names an unknown host", func(t *testing.T) {
		w := healthySetupWorld()
		w.taskState, w.trace = "FAILED", "java.net.UnknownHostException: postgres"
		s := testSetupStack(t, w)
		s.execAsFn = func(ctx context.Context, user, container string, argv ...string) (string, int, error) {
			if argv[0] == "bash" {
				return "", 126, nil
			}
			return w.exec(ctx, container, argv...)
		}
		ok, detail, err := verifyConnectSourceReachable(ctx, s)
		if err != nil || ok || !strings.Contains(detail, "unknown to the Connect container") {
			t.Fatalf("ok=%v detail=%q err=%v", ok, detail, err)
		}
	})
}

// TestPluginAfterScanFixRestoresStash: a break interrupted after it stashed
// the plugin directory is healed by the fix, which restores the directory
// before the worker rescans.
func TestPluginAfterScanFixRestoresStash(t *testing.T) {
	w := healthySetupWorld()
	w.dirExists, w.stashed = false, true
	w.rescanPlugins()
	s := testSetupStack(t, w)
	if err := setupFixCmd(context.Background(), s, "plugin-after-scan"); err != nil {
		t.Fatalf("fix after a half-applied break: %v", err)
	}
	if !w.dirExists || w.stashed || !hasClass(w.plugins, postgresConnector) {
		t.Fatalf("dir=%v stashed=%v plugins=%v", w.dirExists, w.stashed, w.plugins)
	}
}

// TestDBUnreachableWritesHostsAsRoot: /etc/hosts is root-owned, so the
// break's write runs as user 0.
func TestDBUnreachableWritesHostsAsRoot(t *testing.T) {
	w := healthySetupWorld()
	s := testSetupStack(t, w)
	if err := breakDBUnreachable(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, u := range w.execUsers {
		if strings.HasSuffix(u, " sh") {
			found = true
			if u != "0 sh" {
				t.Errorf("the /etc/hosts write ran as %q, want user 0", u)
			}
		}
	}
	if !found {
		t.Fatalf("no sh exec recorded: %v", w.execUsers)
	}
}

// TestHostAndPortNeverReachAShell: a database.hostname or database.port that
// is not a plain host name or port fails the check and the break before any
// shell runs.
func TestHostAndPortNeverReachAShell(t *testing.T) {
	ctx := context.Background()
	shells := func(w *setupWorld) []string {
		var out []string
		for _, u := range w.execUsers {
			if f := strings.Fields(u); len(f) > 0 && (f[len(f)-1] == "bash" || f[len(f)-1] == "sh") {
				out = append(out, u)
			}
		}
		return out
	}
	for _, host := range []string{"x;id", "a/b", "a b", "a\nb", "$(id)", "x/1;echo INJECTED;#", "-oops", "`id`", ""} {
		w := healthySetupWorld()
		w.connector["database.hostname"] = host
		s := testSetupStack(t, w)
		ok, detail, err := verifyConnectSourceReachable(ctx, s)
		if err != nil || ok {
			t.Errorf("host %q: check ok=%v err=%v", host, ok, err)
		}
		if host != "" && !strings.Contains(detail, "is not a host name") {
			t.Errorf("host %q: detail %q", host, detail)
		}
		if host != "" {
			if err := breakDBUnreachable(ctx, s); err == nil || !strings.Contains(err.Error(), "is not a host name") {
				t.Errorf("host %q: break = %v", host, err)
			}
		}
		if sh := shells(w); len(sh) != 0 {
			t.Errorf("host %q reached a shell: %v", host, sh)
		}
	}
	for _, port := range []string{"5432;id", "54 32", "$(id)", "-1", "123456"} {
		w := healthySetupWorld()
		w.connector["database.port"] = port
		s := testSetupStack(t, w)
		ok, detail, err := verifyConnectSourceReachable(ctx, s)
		if err != nil || ok || !strings.Contains(detail, "is not a port") {
			t.Errorf("port %q: ok=%v detail=%q err=%v", port, ok, detail, err)
		}
		if sh := shells(w); len(sh) != 0 {
			t.Errorf("port %q reached a shell: %v", port, sh)
		}
	}
}
