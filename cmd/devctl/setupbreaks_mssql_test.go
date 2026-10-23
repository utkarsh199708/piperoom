package main

// Unit tests for the SQL Server setup breaks (setupbreaks_mssql.go) and the
// mssql compose profile: a fake `docker exec … sqlcmd` plays SQL Server, so
// every statement a break or fix issues is asserted without Docker. The
// integration test (setupbreaks_mssql_integration_test.go) runs the pairs on
// the real profile and reads the checks with the SQL Server client.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// isMSSQLSetupScenario reports the setup breaks that run on the mssql profile,
// not on the Postgres dev stack the shared setup-break tests fake.
func isMSSQLSetupScenario(name string) bool {
	switch setupScenarios[name].check {
	case checkMSSQLAgentRunning, checkMSSQLTableCDC:
		return true
	}
	return false
}

// mssqlWorld is the fake SQL Server: Agent state and the CDC flags.
type mssqlWorld struct {
	t           *testing.T
	agent       string // "Running", "Stopped", "Unknown"
	dbCDC       bool
	tableExists bool
	tracked     bool
	// instances are dbo.orders' capture instances; recorded is the break's
	// record of the ones it dropped (extended properties on dbo.orders).
	instances []string
	recorded  []string
	// stuckAgent ignores xp_servicecontrol (the wait must time out).
	stuckAgent bool
	// exitCode/output/dockerErr fail every call (container down, docker down).
	exitCode  int
	output    string
	dockerErr error
	// failOn fails one statement with a SQL error (exit 1).
	failOn string
	calls  []string
}

// freshMSSQLWorld is the profile as it comes up: Agent running, CDC off.
func freshMSSQLWorld(t *testing.T) *mssqlWorld {
	return &mssqlWorld{t: t, agent: "Running", tableExists: true}
}

func (w *mssqlWorld) exec(_ context.Context, container string, argv ...string) (string, int, error) {
	if container != mssqlContainer {
		w.t.Fatalf("exec in %q, want %q", container, mssqlContainer)
	}
	if len(argv) != 5 || argv[0] != "bash" || argv[1] != "-c" || argv[2] != scriptMSSQLQuery || argv[3] != "_" {
		w.t.Fatalf("argv = %q, want bash -c scriptMSSQLQuery _ <sql>", argv)
	}
	q := argv[4]
	w.calls = append(w.calls, q)
	if w.dockerErr != nil {
		return "", -1, w.dockerErr
	}
	if w.exitCode != 0 {
		return w.output, w.exitCode, nil
	}
	if q == w.failOn {
		return "Msg 22832, Level 16, State 1: Could not update the metadata", 1, nil
	}
	b2i := func(b bool) string {
		if b {
			return "1"
		}
		return "0"
	}
	switch q {
	case sqlMSSQLAgentState:
		return w.agent + "\n", 0, nil
	case sqlMSSQLStopAgent:
		if !w.stuckAgent {
			w.agent = "Stopped"
		}
		return "", 0, nil
	case sqlMSSQLStartAgent:
		if !w.stuckAgent {
			w.agent = "Running"
		}
		return "", 0, nil
	case sqlMSSQLTableCDCState:
		tbl := "-1"
		if w.tableExists {
			tbl = b2i(w.tracked)
		}
		return b2i(w.dbCDC) + "|" + tbl + "\n", 0, nil
	case sqlMSSQLBreakTableCDC:
		out := ""
		if !w.dbCDC {
			w.dbCDC = true
			out = "Job 'cdc.orders_capture' started successfully.\nJob 'cdc.orders_cleanup' started successfully.\n"
		}
		w.recorded = nil
		if w.tracked {
			w.recorded, w.instances = w.instances, nil
		}
		w.tracked = false
		return out, 0, nil
	case sqlMSSQLFixTableCDC:
		if !w.tracked {
			w.instances = w.recorded
			if len(w.instances) == 0 {
				w.instances = []string{"dbo_orders"}
			}
			w.tracked = true
		}
		w.recorded = nil
		return "Update mask evaluation will be disabled in net_changes_function because the CLR configuration option is disabled.\n", 0, nil
	}
	w.t.Fatalf("unexpected statement %q", q)
	return "", 0, nil
}

func testMSSQLStack(w *mssqlWorld) *stack {
	return &stack{
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		poll:     time.Millisecond,
		execInFn: w.exec,
	}
}

func TestMSSQLSetupScenariosAreRegistered(t *testing.T) {
	for name, check := range map[string]string{
		"agent-stopped":         checkMSSQLAgentRunning,
		"cdc-not-enabled-table": checkMSSQLTableCDC,
	} {
		sc, ok := setupScenarios[name]
		if !ok {
			t.Fatalf("setup-break %s is not registered", name)
		}
		if sc.name != name || sc.check != check {
			t.Errorf("%s: name=%q check=%q, want check %q", name, sc.name, sc.check, check)
		}
		if sc.breakIt == nil || sc.fix == nil || sc.verify == nil {
			t.Errorf("%s: incomplete scenario", name)
		}
		if !isMSSQLSetupScenario(name) {
			t.Errorf("%s: not recognised as an mssql scenario", name)
		}
	}
	if isMSSQLSetupScenario("flat-plugin-dir") {
		t.Error("flat-plugin-dir recognised as an mssql scenario")
	}
}

// TestMSSQLSetupBreakFixPair: from the fresh profile, the break makes the
// named check fail with the exact statements below, and the fix makes it pass.
func TestMSSQLSetupBreakFixPair(t *testing.T) {
	cases := []struct {
		name      string
		wantBreak []string
		wantFix   []string
		broken    func(w *mssqlWorld) bool
	}{
		{
			name:      "agent-stopped",
			wantBreak: []string{sqlMSSQLAgentState, sqlMSSQLStopAgent, sqlMSSQLAgentState, sqlMSSQLAgentState},
			wantFix:   []string{sqlMSSQLAgentState, sqlMSSQLStartAgent, sqlMSSQLAgentState, sqlMSSQLAgentState},
			broken:    func(w *mssqlWorld) bool { return w.agent == "Stopped" },
		},
		{
			name:      "cdc-not-enabled-table",
			wantBreak: []string{sqlMSSQLTableCDCState, sqlMSSQLBreakTableCDC, sqlMSSQLTableCDCState},
			wantFix:   []string{sqlMSSQLTableCDCState, sqlMSSQLFixTableCDC, sqlMSSQLTableCDCState},
			broken:    func(w *mssqlWorld) bool { return w.dbCDC && !w.tracked },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := freshMSSQLWorld(t)
			s := testMSSQLStack(w)
			ctx := context.Background()

			if err := setupBreakCmd(ctx, s, tc.name); err != nil {
				t.Fatalf("setup-break: %v", err)
			}
			if got := strings.Join(w.calls, "\n"); got != strings.Join(tc.wantBreak, "\n") {
				t.Fatalf("break statements:\n%s\nwant:\n%s", got, strings.Join(tc.wantBreak, "\n"))
			}
			if !tc.broken(w) {
				t.Fatalf("world not broken after the break: %+v", w)
			}
			if ok, detail, err := setupScenarios[tc.name].verify(ctx, s); err != nil || ok {
				t.Fatalf("check after the break: ok=%v detail=%q err=%v", ok, detail, err)
			}

			w.calls = nil
			if err := setupFixCmd(ctx, s, tc.name); err != nil {
				t.Fatalf("setup-fix: %v", err)
			}
			if got := strings.Join(w.calls, "\n"); got != strings.Join(tc.wantFix, "\n") {
				t.Fatalf("fix statements:\n%s\nwant:\n%s", got, strings.Join(tc.wantFix, "\n"))
			}
			if ok, detail, err := setupScenarios[tc.name].verify(ctx, s); err != nil || !ok {
				t.Fatalf("check after the fix: ok=%v detail=%q err=%v", ok, detail, err)
			}
		})
	}
}

// TestMSSQLSetupBreakFixIdempotent: a second break and a second fix only read
// the check; no write statement runs twice.
func TestMSSQLSetupBreakFixIdempotent(t *testing.T) {
	for name, read := range map[string]string{
		"agent-stopped":         sqlMSSQLAgentState,
		"cdc-not-enabled-table": sqlMSSQLTableCDCState,
	} {
		t.Run(name, func(t *testing.T) {
			w := freshMSSQLWorld(t)
			s := testMSSQLStack(w)
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

// TestMSSQLTableCDCCheckStates: the table check fails only for a CDC-enabled
// database whose table is untracked; a missing database or table is an error.
func TestMSSQLTableCDCCheckStates(t *testing.T) {
	cases := []struct {
		name                        string
		dbCDC, tableExists, tracked bool
		wantOK, wantErr             bool
	}{
		{name: "fresh profile, CDC off", tableExists: true, wantOK: true},
		{name: "half enabled", dbCDC: true, tableExists: true, wantOK: false},
		{name: "fully enabled", dbCDC: true, tableExists: true, tracked: true, wantOK: true},
		{name: "table missing", dbCDC: true, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := &mssqlWorld{t: t, agent: "Running", dbCDC: tc.dbCDC, tableExists: tc.tableExists, tracked: tc.tracked}
			ok, _, err := verifyMSSQLTableCDC(context.Background(), testMSSQLStack(w))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
		})
	}
}

func TestParseMSSQLCDCState(t *testing.T) {
	cases := []struct {
		in      string
		want    mssqlCDCState
		wantErr bool
	}{
		{in: "1|1", want: mssqlCDCState{DatabaseCDC: true, TableExists: true, TableTracked: true}},
		{in: "1|0", want: mssqlCDCState{DatabaseCDC: true, TableExists: true}},
		{in: "0|0\n", want: mssqlCDCState{TableExists: true}},
		{in: "0|-1", want: mssqlCDCState{}},
		{in: "Changed database context to 'orders'.\n1|1", want: mssqlCDCState{DatabaseCDC: true, TableExists: true, TableTracked: true}},
		{in: "", wantErr: true},
		{in: "1", wantErr: true},
		{in: "x|1", wantErr: true},
		{in: "2|1", wantErr: true},
		{in: "1|5", wantErr: true},
	}
	for _, tc := range cases {
		got, err := parseMSSQLCDCState(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("%q: err = %v, wantErr %v", tc.in, err, tc.wantErr)
			continue
		}
		if err == nil && got != tc.want {
			t.Errorf("%q: got %+v, want %+v", tc.in, got, tc.want)
		}
	}
}

// TestMSSQLSetupErrorPaths: a profile that is down, a docker that cannot run,
// a failing statement and an Agent that never stops are errors, never a pass.
func TestMSSQLSetupErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("profile down", func(t *testing.T) {
		w := freshMSSQLWorld(t)
		w.exitCode, w.output = 1, "Error response from daemon: No such container: "+mssqlContainer
		for _, name := range []string{"agent-stopped", "cdc-not-enabled-table"} {
			err := setupBreakCmd(ctx, testMSSQLStack(w), name)
			if err == nil || !strings.Contains(err.Error(), "--profile mssql") || !strings.Contains(err.Error(), "No such container") {
				t.Fatalf("%s: err = %v, want the profile hint and docker's message", name, err)
			}
		}
	})
	t.Run("docker down", func(t *testing.T) {
		w := freshMSSQLWorld(t)
		w.dockerErr = errors.New("cannot connect to the Docker daemon")
		if ok, _, err := verifyMSSQLAgentRunning(ctx, testMSSQLStack(w)); err == nil || ok {
			t.Fatalf("ok=%v err=%v, want an error", ok, err)
		}
	})
	t.Run("break statement fails", func(t *testing.T) {
		w := freshMSSQLWorld(t)
		w.failOn = sqlMSSQLBreakTableCDC
		err := setupBreakCmd(ctx, testMSSQLStack(w), "cdc-not-enabled-table")
		if err == nil || !strings.Contains(err.Error(), "Msg 22832") {
			t.Fatalf("err = %v, want the SQL error", err)
		}
	})
	t.Run("agent never stops", func(t *testing.T) {
		w := freshMSSQLWorld(t)
		w.stuckAgent = true
		tctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		err := setupBreakCmd(tctx, testMSSQLStack(w), "agent-stopped")
		if err == nil || !strings.Contains(err.Error(), "waiting for SQL Server Agent") {
			t.Fatalf("err = %v, want the wait to time out", err)
		}
	})
	t.Run("agent unknown fails the check", func(t *testing.T) {
		w := freshMSSQLWorld(t)
		w.agent = "Unknown"
		ok, detail, err := verifyMSSQLAgentRunning(ctx, testMSSQLStack(w))
		if err != nil || ok || !strings.Contains(detail, "Unknown") {
			t.Fatalf("ok=%v detail=%q err=%v", ok, detail, err)
		}
	})
}

// TestMSSQLStatementsAreFixed: the statements name only the dev objects and
// sqlcmd gets the password from the container's environment, never from
// devctl.
func TestMSSQLStatementsAreFixed(t *testing.T) {
	if !strings.Contains(scriptMSSQLQuery, `SQLCMDPASSWORD="$MSSQL_SA_PASSWORD"`) || strings.Contains(scriptMSSQLQuery, " -P ") {
		t.Fatalf("sqlcmd must read the password from the container env: %s", scriptMSSQLQuery)
	}
	if !strings.Contains(scriptMSSQLQuery, " -C ") || !strings.Contains(scriptMSSQLQuery, " -b ") {
		t.Fatalf("sqlcmd needs -C (self-signed cert) and -b (fail on SQL errors): %s", scriptMSSQLQuery)
	}
	for _, q := range []string{sqlMSSQLBreakTableCDC, sqlMSSQLFixTableCDC} {
		if !strings.Contains(q, "USE orders;") || !strings.Contains(q, "@source_schema = N'dbo', @source_name = N'orders'") {
			t.Errorf("statement does not target orders.dbo.orders: %s", q)
		}
	}
	// The break must leave the database enabled: only the table check fails.
	if strings.Contains(sqlMSSQLBreakTableCDC, "sp_cdc_disable_db") {
		t.Error("the break disables CDC on the database; only the table must be off")
	}
	// The break records the capture instances it drops, before it drops them,
	// and the fix recreates them under the recorded names.
	rec := strings.Index(sqlMSSQLBreakTableCDC, "sp_addextendedproperty @name = @prop, @value = @inst")
	read := strings.Index(sqlMSSQLBreakTableCDC, "SELECT capture_instance FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(N'dbo.orders')")
	drop := strings.Index(sqlMSSQLBreakTableCDC, "sp_cdc_disable_table")
	if rec < 0 || read < 0 || drop < 0 || read > rec || rec > drop {
		t.Errorf("the break must read and record the capture instances before sp_cdc_disable_table: %s", sqlMSSQLBreakTableCDC)
	}
	for _, want := range []string{"FROM sys.extended_properties " + sqlMSSQLInstanceRecordWhere, "@capture_instance = @inst", sqlMSSQLDropInstanceRecord} {
		if !strings.Contains(sqlMSSQLFixTableCDC, want) {
			t.Errorf("the fix lacks %q: %s", want, sqlMSSQLFixTableCDC)
		}
	}
	if !strings.HasPrefix(sqlMSSQLBreakTableCDC[strings.Index(sqlMSSQLBreakTableCDC, "sp_cdc_enable_db;")+len("sp_cdc_enable_db; "):], sqlMSSQLDropInstanceRecord) {
		t.Error("the break must drop an earlier record before it writes its own")
	}
}

// TestMSSQLTableCDCFixRestoresCaptureInstances: the fix brings back the
// capture instances the break dropped, under their names (the setup writer
// names them after the publication), and falls back to SQL Server's default
// name only when nothing was recorded.
func TestMSSQLTableCDCFixRestoresCaptureInstances(t *testing.T) {
	ctx := context.Background()
	t.Run("recorded", func(t *testing.T) {
		w := freshMSSQLWorld(t)
		w.dbCDC, w.tracked, w.instances = true, true, []string{"piperoom"}
		s := testMSSQLStack(w)
		if err := setupBreakCmd(ctx, s, "cdc-not-enabled-table"); err != nil {
			t.Fatal(err)
		}
		if len(w.instances) != 0 || strings.Join(w.recorded, ",") != "piperoom" {
			t.Fatalf("after the break: instances=%q recorded=%q, want none and [piperoom]", w.instances, w.recorded)
		}
		if err := setupFixCmd(ctx, s, "cdc-not-enabled-table"); err != nil {
			t.Fatal(err)
		}
		if strings.Join(w.instances, ",") != "piperoom" || len(w.recorded) != 0 {
			t.Fatalf("after the fix: instances=%q recorded=%q, want [piperoom] and no record", w.instances, w.recorded)
		}
	})
	t.Run("nothing recorded", func(t *testing.T) {
		w := freshMSSQLWorld(t)
		s := testMSSQLStack(w)
		if err := setupBreakCmd(ctx, s, "cdc-not-enabled-table"); err != nil {
			t.Fatal(err)
		}
		if err := setupFixCmd(ctx, s, "cdc-not-enabled-table"); err != nil {
			t.Fatal(err)
		}
		if strings.Join(w.instances, ",") != "dbo_orders" {
			t.Fatalf("after the fix: instances=%q, want [dbo_orders]", w.instances)
		}
	})
}

// mssqlComposeService is the part of the mssql service the tests read.
type mssqlComposeService struct {
	Profiles    []string          `yaml:"profiles"`
	Image       string            `yaml:"image"`
	Environment map[string]string `yaml:"environment"`
	Ports       []string          `yaml:"ports"`
	Volumes     []string          `yaml:"volumes"`
	DependsOn   any               `yaml:"depends_on"`
	Healthcheck struct {
		Test []string `yaml:"test"`
	} `yaml:"healthcheck"`
}

func TestMSSQLComposeProfile(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "dev", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]mssqlComposeService `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse dev/docker-compose.yml: %v", err)
	}
	svc, ok := doc.Services["mssql"]
	if !ok {
		t.Fatal("no mssql service in dev/docker-compose.yml")
	}
	if len(svc.Profiles) != 1 || svc.Profiles[0] != "mssql" {
		t.Errorf("profiles = %v, want [mssql] (the default stack must not start SQL Server)", svc.Profiles)
	}
	if !strings.HasPrefix(svc.Image, "mcr.microsoft.com/mssql/server:2022-CU") || !strings.Contains(svc.Image, "@sha256:") {
		t.Errorf("image %q: want an exact 2022 CU tag pinned by digest", svc.Image)
	}
	if svc.DependsOn != nil {
		t.Errorf("depends_on = %v: the profile starts alone", svc.DependsOn)
	}
	for k, want := range map[string]string{"ACCEPT_EULA": "Y", "MSSQL_PID": "Developer", "MSSQL_AGENT_ENABLED": "true"} {
		if svc.Environment[k] != want {
			t.Errorf("environment %s = %q, want %q", k, svc.Environment[k], want)
		}
	}
	if svc.Environment["MSSQL_SA_PASSWORD"] == "" {
		t.Error("MSSQL_SA_PASSWORD is not set; SQL Server refuses to start")
	}
	if len(svc.Ports) != 1 || !strings.HasPrefix(svc.Ports[0], "127.0.0.1:") {
		t.Errorf("ports = %v, want one host-loopback binding", svc.Ports)
	}
	if len(svc.Volumes) != 1 || svc.Volumes[0] != "./mssql/init:/mssql-server-setup-scripts.d:ro" {
		t.Errorf("volumes = %v, want the init scripts on the image's setup hook", svc.Volumes)
	}
	hc := strings.Join(svc.Healthcheck.Test, " ")
	for _, want := range []string{"/opt/mssql-tools18/bin/sqlcmd", " -C ", " -b ", "$$MSSQL_SA_PASSWORD", "orders.dbo.orders", "program_name LIKE N'SQLAgent - %'"} {
		if !strings.Contains(hc, want) {
			t.Errorf("healthcheck %q lacks %q", hc, want)
		}
	}

	// The init script creates the source and leaves CDC off.
	initSQL, err := os.ReadFile(filepath.Join(root, "dev", "mssql", "init", "01-orders.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(initSQL)
	for _, want := range []string{"CREATE DATABASE orders", "CREATE TABLE dbo.orders", "PRIMARY KEY", "INSERT INTO dbo.orders"} {
		if !strings.Contains(sqlText, want) {
			t.Errorf("init SQL lacks %q", want)
		}
	}
	for _, line := range strings.Split(sqlText, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		if strings.Contains(line, "sp_cdc_enable") {
			t.Errorf("init SQL enables CDC (%q); the profile starts with CDC off", strings.TrimSpace(line))
		}
	}
}
