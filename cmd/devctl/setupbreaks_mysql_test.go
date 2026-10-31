package main

// Unit tests for the MySQL setup breaks (setupbreaks_mysql.go): a fake
// `docker exec … mysql` plays the mysql profile, so every statement a break or
// fix issues is asserted without Docker. The integration test
// (setupbreaks_mysql_integration_test.go) runs the pairs on the real profile
// and reads the checks through the U2a driver over the MySQL client.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	mysqldriver "piperoom/internal/setup/sources/mysql"
)

// mysqlSetupWorld is the fake MySQL: the two globals and the connector user's
// replication privileges.
type mysqlSetupWorld struct {
	t            *testing.T
	binlogFormat string
	serverID     int64
	userExists   bool
	replSlave    bool
	replClient   bool
	// exitCode/output/dockerErr fail every call (profile down, docker down).
	exitCode  int
	output    string
	dockerErr error
	calls     []string
}

// freshMySQLSetupWorld is the profile as it comes up (dev/docker-compose.yml).
func freshMySQLSetupWorld(t *testing.T) *mysqlSetupWorld {
	return &mysqlSetupWorld{t: t, binlogFormat: "ROW", serverID: mysqlBaselineServerID, userExists: true, replSlave: true, replClient: true}
}

func (w *mysqlSetupWorld) exec(_ context.Context, container string, argv ...string) (string, int, error) {
	if container != mysqlContainer {
		w.t.Fatalf("exec in %q, want %q", container, mysqlContainer)
	}
	if len(argv) != 5 || argv[0] != "bash" || argv[1] != "-c" || argv[2] != scriptMySQLQuery || argv[3] != "_" {
		w.t.Fatalf("argv = %q, want bash -c scriptMySQLQuery _ <sql>", argv)
	}
	q := argv[4]
	w.calls = append(w.calls, q)
	if w.dockerErr != nil {
		return "", -1, w.dockerErr
	}
	if w.exitCode != 0 {
		return w.output, w.exitCode, nil
	}
	yn := func(b bool) string {
		if b {
			return "Y"
		}
		return "N"
	}
	switch q {
	case sqlMySQLSetupSettings:
		return w.binlogFormat + "\t" + strconv.FormatInt(w.serverID, 10) + "\n", 0, nil
	case sqlMySQLBinlogStmt:
		w.binlogFormat = "STATEMENT"
	case sqlMySQLBinlogRow:
		w.binlogFormat = "ROW"
	case sqlMySQLServerIDZero:
		w.serverID = 0
	case sqlMySQLServerIDBase:
		w.serverID = mysqlBaselineServerID
	case sqlMySQLReplPrivs:
		if !w.userExists {
			return "", 0, nil
		}
		return yn(w.replSlave) + "\t" + yn(w.replClient) + "\n", 0, nil
	case sqlMySQLRevokeReplClient:
		w.replClient = false
	case sqlMySQLGrantReplClient:
		w.replClient = true
	default:
		w.t.Fatalf("unexpected statement %q", q)
	}
	return "", 0, nil
}

func testMySQLSetupStack(w *mysqlSetupWorld) *stack {
	return &stack{
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		poll:     time.Millisecond,
		execInFn: w.exec,
	}
}

// The scenarios are registered on the mysql profile, and each named check is
// the U2a driver's own check id.
func TestMySQLSetupScenariosAreRegistered(t *testing.T) {
	for name, check := range map[string]string{
		"binlog-statement":     mysqldriver.CheckBinlogFormat,
		"no-replication-grant": mysqldriver.CheckReadRole,
		"server-id-zero":       mysqldriver.CheckServerID,
	} {
		sc, ok := setupScenarios[name]
		if !ok {
			t.Fatalf("setup-break %s is not registered", name)
		}
		if sc.name != name || sc.check != check || sc.profile != mysqlProfile {
			t.Errorf("%s: name=%q check=%q profile=%q, want check %q on profile %q", name, sc.name, sc.check, sc.profile, check, mysqlProfile)
		}
		if sc.breakIt == nil || sc.fix == nil || sc.verify == nil {
			t.Errorf("%s: incomplete scenario", name)
		}
	}
}

// TestMySQLSetupBreakFixPair: from the fresh profile, the break makes the
// named check fail with exactly these statements, and the fix makes it pass.
func TestMySQLSetupBreakFixPair(t *testing.T) {
	cases := []struct {
		name      string
		read      string
		write     string
		fixWrite  string
		broken    func(w *mysqlSetupWorld) bool
		detailHas string
	}{
		{"binlog-statement", sqlMySQLSetupSettings, sqlMySQLBinlogStmt, sqlMySQLBinlogRow,
			func(w *mysqlSetupWorld) bool { return w.binlogFormat == "STATEMENT" }, "STATEMENT"},
		{"no-replication-grant", sqlMySQLReplPrivs, sqlMySQLRevokeReplClient, sqlMySQLGrantReplClient,
			func(w *mysqlSetupWorld) bool { return !w.replClient && w.replSlave }, "REPLICATION CLIENT"},
		{"server-id-zero", sqlMySQLSetupSettings, sqlMySQLServerIDZero, sqlMySQLServerIDBase,
			func(w *mysqlSetupWorld) bool { return w.serverID == 0 }, "server_id is 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := freshMySQLSetupWorld(t)
			s := testMySQLSetupStack(w)
			ctx := context.Background()

			if err := setupBreakCmd(ctx, s, tc.name); err != nil {
				t.Fatalf("setup-break: %v", err)
			}
			want := []string{tc.read, tc.write, tc.read}
			if got := strings.Join(w.calls, "\n"); got != strings.Join(want, "\n") {
				t.Fatalf("break statements:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
			}
			if !tc.broken(w) {
				t.Fatalf("world not broken after the break: %+v", w)
			}
			ok, detail, err := setupScenarios[tc.name].verify(ctx, s)
			if err != nil || ok || !strings.Contains(detail, tc.detailHas) {
				t.Fatalf("check after the break: ok=%v detail=%q err=%v", ok, detail, err)
			}

			w.calls = nil
			if err := setupFixCmd(ctx, s, tc.name); err != nil {
				t.Fatalf("setup-fix: %v", err)
			}
			want = []string{tc.read, tc.fixWrite, tc.read}
			if got := strings.Join(w.calls, "\n"); got != strings.Join(want, "\n") {
				t.Fatalf("fix statements:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
			}
			if ok, detail, err := setupScenarios[tc.name].verify(ctx, s); err != nil || !ok {
				t.Fatalf("check after the fix: ok=%v detail=%q err=%v", ok, detail, err)
			}
			f := freshMySQLSetupWorld(t)
			if w.binlogFormat != f.binlogFormat || w.serverID != f.serverID || w.replSlave != f.replSlave || w.replClient != f.replClient {
				t.Fatalf("the fix did not restore the fresh profile: %+v", w)
			}
		})
	}
}

// TestMySQLSetupBreakFixIdempotent: a second break and a second fix only read
// the check; no write statement runs twice.
func TestMySQLSetupBreakFixIdempotent(t *testing.T) {
	for name, read := range map[string]string{
		"binlog-statement":     sqlMySQLSetupSettings,
		"no-replication-grant": sqlMySQLReplPrivs,
		"server-id-zero":       sqlMySQLSetupSettings,
	} {
		t.Run(name, func(t *testing.T) {
			w := freshMySQLSetupWorld(t)
			s := testMySQLSetupStack(w)
			ctx := context.Background()
			if err := setupBreakCmd(ctx, s, name); err != nil {
				t.Fatal(err)
			}
			w.calls = nil
			if err := setupBreakCmd(ctx, s, name); err != nil {
				t.Fatalf("second break: %v", err)
			}
			if len(w.calls) != 1 || w.calls[0] != read {
				t.Fatalf("second break ran %q, want only the check", w.calls)
			}
			if err := setupFixCmd(ctx, s, name); err != nil {
				t.Fatal(err)
			}
			w.calls = nil
			if err := setupFixCmd(ctx, s, name); err != nil {
				t.Fatalf("second fix: %v", err)
			}
			if len(w.calls) != 1 || w.calls[0] != read {
				t.Fatalf("second fix ran %q, want only the check", w.calls)
			}
		})
	}
}

// The error paths: the profile down (mysql exits non-zero) and docker down
// are errors naming how to start the profile, never a passed or failed check;
// a missing connector user fails the check with a reason; a malformed row is
// an error.
func TestMySQLSetupErrorPaths(t *testing.T) {
	ctx := context.Background()

	w := freshMySQLSetupWorld(t)
	w.exitCode, w.output = 1, "ERROR 2002 (HY000): Can't connect to local MySQL server"
	if _, _, err := verifyMySQLBinlogFormat(ctx, testMySQLSetupStack(w)); err == nil || !strings.Contains(err.Error(), mysqlUpHint) {
		t.Fatalf("profile down: err = %v, want one naming %q", err, mysqlUpHint)
	}
	if err := setupBreakCmd(ctx, testMySQLSetupStack(w), "server-id-zero"); err == nil {
		t.Fatal("a break on a down profile succeeded")
	}

	w = freshMySQLSetupWorld(t)
	w.dockerErr = errors.New("docker: not running")
	if _, _, err := verifyMySQLReadRole(ctx, testMySQLSetupStack(w)); err == nil {
		t.Fatal("docker down: no error")
	}

	w = freshMySQLSetupWorld(t)
	w.userExists = false
	ok, detail, err := verifyMySQLReadRole(ctx, testMySQLSetupStack(w))
	if err != nil || ok || !strings.Contains(detail, "does not exist") {
		t.Fatalf("missing user: ok=%v detail=%q err=%v", ok, detail, err)
	}

	for _, row := range []string{"ROW", "ROW\tnot-a-number", "ROW\t-1"} {
		if _, err := parseMySQLSetupSettings(row); err == nil {
			t.Errorf("parseMySQLSetupSettings(%q) accepted a malformed row", row)
		}
	}
	for _, row := range []string{"Y", "Y\tmaybe"} {
		if _, err := parseMySQLReplPrivs(row); err == nil {
			t.Errorf("parseMySQLReplPrivs(%q) accepted a malformed row", row)
		}
	}
}

// The fix restores the profile's own --server-id: the constant and the
// statement agree with dev/docker-compose.yml.
func TestMySQLBaselineServerIDMatchesTheProfile(t *testing.T) {
	raw, err := os.ReadFile("../../dev/docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Command []string `yaml:"command"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatal(err)
	}
	want := "--server-id=" + strconv.Itoa(mysqlBaselineServerID)
	found := false
	for _, arg := range compose.Services["mysql"].Command {
		found = found || arg == want
	}
	if !found {
		t.Fatalf("dev/docker-compose.yml mysql command %q has no %s", compose.Services["mysql"].Command, want)
	}
	if !strings.HasSuffix(sqlMySQLServerIDBase, " "+strconv.Itoa(mysqlBaselineServerID)) {
		t.Fatalf("sqlMySQLServerIDBase %q does not set %d", sqlMySQLServerIDBase, mysqlBaselineServerID)
	}
}
