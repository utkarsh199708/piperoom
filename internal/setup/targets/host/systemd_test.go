package host

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/setup/ops"
)

func TestSystemctlArgv(t *testing.T) {
	props := "--property=LoadState,ActiveState,SubState,UnitFileState,Result,MainPID,ExecMainStatus,NRestarts,FragmentPath"
	ok := []struct {
		verb, operand string
		want          []string
	}{
		{"show", "piperoom-connect.service", []string{"systemctl", "--user", "show", "--no-pager", props, "piperoom-connect.service"}},
		{"enable", "/srv/p/systemd/piperoom-connect.service", []string{"systemctl", "--user", "enable", "/srv/p/systemd/piperoom-connect.service"}},
		{"daemon-reload", "", []string{"systemctl", "--user", "daemon-reload"}},
		{"restart", "piperoom-it-a1.service", []string{"systemctl", "--user", "restart", "piperoom-it-a1.service"}},
	}
	for _, c := range ok {
		got, err := SystemctlArgv(c.verb, c.operand)
		if err != nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("SystemctlArgv(%q, %q) = %q, %v; want %q", c.verb, c.operand, got, err, c.want)
		}
	}
	bad := []struct{ verb, operand string }{
		{"stop", "piperoom-connect.service"}, {"disable", "piperoom-connect.service"},
		{"edit", "piperoom-connect.service"}, {"--system", ""}, {"", ""}, {"set-environment", "A=B"},
		{"restart", "sshd.service"}, {"restart", "piperoom-x.timer"}, {"restart", "piperoom-a@b.service"},
		{"restart", "piperoom-x.service\n"}, {"restart", "-piperoom-x.service"}, {"restart", "piperoom-.service"},
		{"restart", "../piperoom-x.service"}, {"restart", "piperoom-X.service"}, {"restart", ""},
		{"restart", "piperoom-x.service --now"}, {"show", "*"},
		{"enable", "systemd/piperoom-x.service"}, {"enable", "/srv/p/other/piperoom-x.service"},
		{"enable", "/srv/my p/systemd/piperoom-x.service"}, {"enable", "/srv/%h/systemd/piperoom-x.service"},
		{"enable", "/srv/p/systemd/../systemd/piperoom-x.service"}, {"enable", "/srv/p/systemd/sshd.service"},
		{"enable", "piperoom-x.service"}, {"daemon-reload", "piperoom-x.service"},
	}
	for _, c := range bad {
		if got, err := SystemctlArgv(c.verb, c.operand); err == nil {
			t.Errorf("SystemctlArgv(%q, %q) = %q, want a refusal", c.verb, c.operand, got)
		}
	}
}

func TestServiceStatus(t *testing.T) {
	f := newFixture(t, Options{})
	f.exec.respond = func(args []string) ([]byte, int, error) {
		return []byte("LoadState=loaded\nActiveState=active\nSubState=running\nUnitFileState=enabled\nResult=success\n" +
			"MainPID=4242\nExecMainStatus=0\nNRestarts=1\nFragmentPath=" + f.dir + "/systemd/piperoom-connect.service\nUnrelated=x\n"), 0, nil
	}
	res, err := f.run("service_status", UnitParams{})
	if err != nil {
		t.Fatalf("service_status: %v", err)
	}
	argv, _ := SystemctlArgv("show", "piperoom-connect.service")
	if calls := f.exec.argvs(); len(calls) != 1 || !reflect.DeepEqual(calls[0], argv) {
		t.Fatalf("commands %q, want only %q", calls, argv)
	}
	if !reflect.DeepEqual(res.Receipt.Argv, argv) || res.Receipt.Kind != "exec" {
		t.Errorf("receipt %+v", res.Receipt)
	}
	var data map[string]any
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["running"] != true || data["installed"] != true || data["main_pid"] != "4242" || data["unit_file_state"] != "enabled" || data["this_project"] != true {
		t.Errorf("data %v", data)
	}
	if _, leaked := data["Unrelated"]; leaked {
		t.Error("an unrequested property reached Data")
	}
	if res.Summary != "piperoom-connect.service: active (running), enabled" {
		t.Errorf("summary %q", res.Summary)
	}

	f.exec.respond = func([]string) ([]byte, int, error) {
		return []byte("LoadState=not-found\nActiveState=inactive\nSubState=dead\n"), 0, nil
	}
	if res, _ := f.run("service_status", UnitParams{Name: "other"}); res.Summary != "piperoom-other.service is not installed" {
		t.Errorf("not installed: %q", res.Summary)
	}

	// The same unit name installed by another project is reported as such,
	// not as this project's worker.
	f.exec.respond = func([]string) ([]byte, int, error) {
		return []byte("LoadState=loaded\nActiveState=active\nSubState=running\nFragmentPath=/srv/other/systemd/piperoom-connect.service\n"), 0, nil
	}
	res, err = f.run("service_status", UnitParams{})
	if err != nil {
		t.Fatal(err)
	}
	data = nil
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["this_project"] != false || !strings.Contains(res.Summary, "not this project's") || !strings.Contains(res.Summary, "/srv/other/") {
		t.Errorf("foreign unit: %q %v", res.Summary, data)
	}
}

func TestServiceStatusFailures(t *testing.T) {
	f := newFixture(t, Options{})
	f.exec.respond = func([]string) ([]byte, int, error) {
		return []byte("Failed to connect to bus: No medium found\n"), 1, nil
	}
	res, err := f.run("service_status", UnitParams{})
	if !errors.Is(err, ops.ErrExec) || !strings.Contains(res.Summary, "enable-linger") || res.Receipt.ExitCode != 1 {
		t.Errorf("bus failure: %v %q %+v", err, res.Summary, res.Receipt)
	}
	f.exec.respond = func([]string) ([]byte, int, error) { return nil, -1, errors.New("exec: systemctl: not found") }
	if _, err := f.run("service_status", UnitParams{}); !errors.Is(err, ops.ErrExec) {
		t.Errorf("missing systemctl: %v", err)
	}
	if _, err := f.run("service_status", map[string]string{"unit": "sshd.service"}); !errors.Is(err, ErrParams) {
		t.Errorf("unknown field: %v", err)
	}
}

// wantUnit is the unit service_install renders for the fixture's layout.
func wantUnit(dir, mode string) string {
	return `# Generated by piperoom setup (service_install); service_install rewrites this file.
[Unit]
Description=Piperoom Kafka Connect worker (` + mode + `)
StartLimitIntervalSec=300
StartLimitBurst=5

[Service]
Type=simple
WorkingDirectory=` + dir + `
Environment=JAVA_HOME=/opt/jdk-21
Environment=LOG_DIR=` + dir + `/logs
ExecStart=` + dir + `/kafka/bin/connect-` + mode + `.sh ` + dir + `/connect-` + mode + `.properties
SuccessExitStatus=143
Restart=on-failure
RestartSec=10
NoNewPrivileges=yes
UMask=0077

[Install]
WantedBy=default.target
`
}

func TestServiceInstall(t *testing.T) {
	for _, mode := range []string{ModeDistributed, ModeStandalone} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t, Options{})
			f.distribution()
			res, err := f.run("service_install", ServiceParams{Mode: mode})
			if err != nil {
				t.Fatalf("service_install: %v (%s)", err, res.Summary)
			}
			if got := f.read("systemd/piperoom-connect.service"); got != wantUnit(f.dir, mode) {
				t.Errorf("unit file:\n%s\nwant:\n%s", got, wantUnit(f.dir, mode))
			}
			show, _ := SystemctlArgv("show", "piperoom-connect.service")
			enable := []string{"systemctl", "--user", "enable", f.dir + "/systemd/piperoom-connect.service"}
			reload := []string{"systemctl", "--user", "daemon-reload"}
			if calls := f.exec.argvs(); !reflect.DeepEqual(calls, [][]string{show, enable, reload}) {
				t.Errorf("commands %q", calls)
			}
			if !reflect.DeepEqual(res.Receipt.Argv, enable) {
				t.Errorf("receipt argv %q", res.Receipt.Argv)
			}
			// A second install with the same inputs rewrites nothing.
			res, err = f.run("service_install", ServiceParams{Mode: mode})
			if err != nil || !strings.Contains(res.Summary, "unchanged") {
				t.Errorf("second install: %v %q", err, res.Summary)
			}
		})
	}
}

func TestServiceInstallJavaHomeIsResolved(t *testing.T) {
	f := newFixture(t, Options{ResolvePath: func(p string) (string, error) {
		if p != "/usr/bin/java" {
			t.Errorf("resolved %q", p)
		}
		return "/usr/lib/jvm/java-21-openjdk-amd64/bin/java", nil
	}})
	f.java = "/usr/bin/java"
	f.distribution()
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatal(err)
	}
	if u := f.read("systemd/piperoom-connect.service"); !strings.Contains(u, "\nEnvironment=JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64\n") {
		t.Errorf("JAVA_HOME not the resolved JDK:\n%s", u)
	}
}

func TestServiceInstallRefusals(t *testing.T) {
	cases := []struct {
		name    string
		params  any
		setup   func(f *fixture)
		wantErr error
	}{
		{"unknown field exec_start", map[string]string{"exec_start": "/bin/sh -c id"}, nil, ErrParams},
		{"unknown field user", map[string]string{"user": "root"}, nil, ErrParams},
		{"unknown field environment", map[string]string{"environment": "LD_PRELOAD=/x.so"}, nil, ErrParams},
		{"bad mode", ServiceParams{Mode: "cluster"}, nil, ErrParams},
		{"distribution missing", ServiceParams{}, func(f *fixture) {
			f.write("connect-distributed.properties", "x=y\n", 0o600)
		}, ErrDetect},
		{"script not executable", ServiceParams{}, func(f *fixture) {
			f.distribution()
			f.write("kafka/bin/connect-distributed.sh", "#!/bin/sh\n", 0o644)
		}, ErrDetect},
		{"worker file missing", ServiceParams{Mode: ModeStandalone}, func(f *fixture) {
			f.write("kafka/bin/connect-standalone.sh", "#!/bin/sh\n", 0o755)
		}, ErrDetect},
		{"java missing", ServiceParams{}, func(f *fixture) { f.distribution(); f.java = "" }, ErrDetect},
		{"java is a shim", ServiceParams{}, func(f *fixture) { f.distribution(); f.java = "/home/u/.asdf/shims/java" }, ErrDetect},
		{"JAVA_HOME with a space", ServiceParams{}, func(f *fixture) { f.distribution(); f.java = "/opt/my jdk/bin/java" }, ErrDetect},
		{"JAVA_HOME with a specifier", ServiceParams{}, func(f *fixture) { f.distribution(); f.java = "/opt/%h/bin/java" }, ErrDetect},
		{"JAVA_HOME with a variable", ServiceParams{}, func(f *fixture) { f.distribution(); f.java = "/opt/$HOME/bin/java" }, ErrDetect},
		{"JAVA_HOME with a newline", ServiceParams{}, func(f *fixture) {
			f.distribution()
			f.java = "/opt/x\nExecStartPre=/bin/sh/bin/java"
		}, ErrDetect},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t, Options{})
			if c.setup != nil {
				c.setup(f)
			}
			_, err := f.run("service_install", c.params)
			if !errors.Is(err, c.wantErr) {
				t.Fatalf("err %v, want %v", err, c.wantErr)
			}
			if calls := f.exec.argvs(); len(calls) != 0 {
				t.Errorf("commands ran: %q", calls)
			}
		})
	}
}

func TestServiceInstallRefusesSymlinks(t *testing.T) {
	f := newFixture(t, Options{})
	f.distribution()
	elsewhere := t.TempDir()
	mustSymlink(t, elsewhere, f.dir+"/systemd")
	if _, err := f.run("service_install", ServiceParams{}); !errors.Is(err, ErrPath) {
		t.Errorf("symlinked unit dir: %v", err)
	}
	// A distribution that is a symlink to a real one elsewhere.
	g := newFixture(t, Options{})
	g.write("connect-distributed.properties", "x=y\n", 0o600)
	target := &fixture{t: t, dir: t.TempDir()}
	target.write("bin/connect-distributed.sh", "#!/bin/sh\n", 0o755)
	mustSymlink(t, target.dir, g.dir+"/kafka")
	if _, err := g.run("service_install", ServiceParams{}); !errors.Is(err, ErrPath) {
		t.Errorf("symlinked distribution: %v", err)
	}
	// A script that is a symlink inside the project.
	h := newFixture(t, Options{})
	h.distribution()
	h.write("kafka/bin/real.sh", "#!/bin/sh\n", 0o755)
	if err := os.Remove(h.dir + "/kafka/bin/connect-distributed.sh"); err != nil {
		t.Fatal(err)
	}
	mustSymlink(t, "real.sh", h.dir+"/kafka/bin/connect-distributed.sh")
	if _, err := h.run("service_install", ServiceParams{}); !errors.Is(err, ErrPath) {
		t.Errorf("symlinked script: %v", err)
	}
	if n := len(f.exec.argvs()) + len(g.exec.argvs()) + len(h.exec.argvs()); n != 0 {
		t.Errorf("%d commands ran", n)
	}
}

func mustSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func TestProjectDirMustBeUnitSafe(t *testing.T) {
	dir := t.TempDir() + "/my project"
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	f := newFixtureIn(t, dir, Options{})
	f.distribution()
	if _, err := f.run("service_install", ServiceParams{}); !errors.Is(err, ErrDetect) {
		t.Errorf("project dir with a space: %v", err)
	}
}

func TestServiceRestart(t *testing.T) {
	f := newFixture(t, Options{})
	if _, err := f.run("service_restart", UnitParams{}); !errors.Is(err, ErrDetect) {
		t.Fatalf("restart before install: %v", err)
	}
	if len(f.exec.argvs()) != 0 {
		t.Fatal("restart of a unit this project did not install ran a command")
	}
	f.write("systemd/piperoom-connect.service", wantUnit(f.dir, ModeDistributed), 0o644)
	f.exec.respond = func(args []string) ([]byte, int, error) {
		if args[1] == "show" {
			return []byte("LoadState=loaded\nActiveState=active\nSubState=running\nUnitFileState=enabled\n" +
				"FragmentPath=" + f.dir + "/systemd/piperoom-connect.service\n"), 0, nil
		}
		return nil, 0, nil
	}
	res, err := f.run("service_restart", UnitParams{})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	restart := []string{"systemctl", "--user", "restart", "piperoom-connect.service"}
	show, _ := SystemctlArgv("show", "piperoom-connect.service")
	if calls := f.exec.argvs(); !reflect.DeepEqual(calls, [][]string{show, restart, show}) {
		t.Errorf("commands %q", calls)
	}
	if !reflect.DeepEqual(res.Receipt.Argv, restart) || !strings.Contains(res.Summary, "active (running)") {
		t.Errorf("result %q %+v", res.Summary, res.Receipt)
	}

	f.exec.respond = func(args []string) ([]byte, int, error) {
		if args[1] == "show" {
			return []byte("LoadState=loaded\nFragmentPath=" + f.dir + "/systemd/piperoom-connect.service\n"), 0, nil
		}
		return []byte("Job for piperoom-connect.service failed because the control process exited with error code.\n"), 1, nil
	}
	res, err = f.run("service_restart", UnitParams{})
	if !errors.Is(err, ops.ErrExec) || !strings.Contains(res.Summary, "journal_tail") || res.Receipt.ExitCode != 1 {
		t.Errorf("failed restart: %v %q %+v", err, res.Summary, res.Receipt)
	}
}

// TestNoParamReachesTheUnit is the row's injection test: every string field
// of every service op's params is tried with each payload that would change a
// unit (a new line, a specifier, a variable, a quote, an assignment, a
// separator). Each must be refused before any command runs and before a
// unit file is written; and a valid install's ExecStart, WorkingDirectory and
// Environment lines are exactly the detected paths, with no User= at all.
func TestNoParamReachesTheUnit(t *testing.T) {
	payloads := []string{
		"connect\nExecStartPre=/bin/sh -c id", "connect\n[Service]\nUser=root", "a=b", "User=root",
		"$HOME", "${HOME}", "%h", "%%", `"q"`, "'q'", `a\b`, "a b", "a;b", "a|b", "../x", "/etc/passwd",
		"connect.service", "x@y", "-H", "--system", "CONNECT", strings.Repeat("a", 41), "\x00", "connect\r",
	}
	for _, typ := range []reflect.Type{reflect.TypeOf(ServiceParams{}), reflect.TypeOf(UnitParams{})} {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.Type.Kind() != reflect.String {
				t.Fatalf("%s.%s is a %s: add injection rows for it", typ.Name(), field.Name, field.Type)
			}
			for _, p := range payloads {
				v := reflect.New(typ).Elem()
				v.Field(i).SetString(p)
				for _, op := range []string{"service_install", "service_restart", "service_status"} {
					if (op == "service_install") != (typ == reflect.TypeOf(ServiceParams{})) {
						continue
					}
					f := newFixture(t, Options{})
					f.distribution()
					f.write("systemd/piperoom-connect.service", "seed\n", 0o644)
					if _, err := f.run(op, v.Interface()); !errors.Is(err, ErrParams) {
						t.Errorf("%s with %s=%q: %v, want ErrParams", op, field.Name, p, err)
					}
					if calls := f.exec.argvs(); len(calls) != 0 {
						t.Errorf("%s with %s=%q ran %q", op, field.Name, p, calls)
					}
					if got := f.read("systemd/piperoom-connect.service"); got != "seed\n" {
						t.Errorf("%s with %s=%q wrote the unit", op, field.Name, p)
					}
				}
			}
		}
	}

	f := newFixture(t, Options{})
	f.distribution()
	if _, err := f.run("service_install", ServiceParams{Name: "it-a1", Mode: ModeDistributed}); err != nil {
		t.Fatal(err)
	}
	unit := f.read("systemd/piperoom-it-a1.service")
	want := map[string]bool{
		"WorkingDirectory=" + f.dir:              true,
		"Environment=JAVA_HOME=/opt/jdk-21":      true,
		"Environment=LOG_DIR=" + f.dir + "/logs": true,
		"ExecStart=" + f.dir + "/kafka/bin/connect-distributed.sh " + f.dir + "/connect-distributed.properties": true,
	}
	for _, line := range strings.Split(unit, "\n") {
		key, _, _ := strings.Cut(line, "=")
		switch key {
		case "User", "Group", "DynamicUser", "ExecStartPre", "ExecStartPost", "ExecStop", "ExecReload", "EnvironmentFile", "PAMName":
			t.Errorf("unit carries %q", line)
		case "WorkingDirectory", "Environment", "ExecStart":
			if !want[line] {
				t.Errorf("unexpected %q", line)
			}
			delete(want, line)
		}
	}
	if len(want) != 0 {
		t.Errorf("missing lines %v in\n%s", want, unit)
	}
}

func TestRenderUnitRefusesUnsafeValues(t *testing.T) {
	good := Unit{Description: "Piperoom worker (distributed)", WorkingDirectory: "/p",
		Environment: []EnvVar{{Name: "JAVA_HOME", Value: "/opt/jdk"}}, ExecStart: []string{"/p/kafka/bin/x.sh", "/p/w.properties"}}
	if _, err := RenderUnit(good); err != nil {
		t.Fatalf("good unit: %v", err)
	}
	mutations := []func(u *Unit, v string){
		func(u *Unit, v string) { u.WorkingDirectory = v },
		func(u *Unit, v string) { u.ExecStart = []string{v} },
		func(u *Unit, v string) { u.Environment = []EnvVar{{Name: "JAVA_HOME", Value: v}} },
		func(u *Unit, v string) { u.ExecStart = []string{"/p/x.sh", v} },
	}
	// Unsafe characters are refused in every position; a relative or unclean
	// path only where a path is required (the first two).
	unsafe := []string{"/p\nUser=root", "/p/%h", "/p/$X", "/p/${X}", "/p/a b", `/p/"x"`, `/p/'x'`, `/p/a\b`, "/p/a;b", "/p/a=b", "/p/\x00", ""}
	notPaths := []string{"p", "./p", "/p/../x", "/p/", "//p"}
	for _, v := range append(unsafe, notPaths...) {
		positions := len(mutations)
		if !containsString(unsafe, v) {
			positions = 2
		}
		for i := 0; i < positions; i++ {
			u := good
			u.Environment = append([]EnvVar(nil), good.Environment...)
			mutations[i](&u, v)
			if out, err := RenderUnit(u); err == nil {
				t.Errorf("position %d with %q rendered:\n%s", i, v, out)
			}
		}
	}
	for _, u := range []Unit{
		{Description: "x %h", WorkingDirectory: "/p", ExecStart: []string{"/p/x"}},
		{Description: "x\nUser=root", WorkingDirectory: "/p", ExecStart: []string{"/p/x"}},
		{Description: "x", WorkingDirectory: "/p", Environment: []EnvVar{{Name: "LD_PRELOAD=x", Value: "/a"}}, ExecStart: []string{"/p/x"}},
		{Description: "x", WorkingDirectory: "/p", ExecStart: nil},
		{Description: "x", WorkingDirectory: "/p", ExecStart: []string{"+/p/x"}},
		{Description: "x", WorkingDirectory: "/p", ExecStart: []string{"-/p/x"}},
	} {
		if out, err := RenderUnit(u); err == nil {
			t.Errorf("unit %+v rendered:\n%s", u, out)
		}
	}
}

// TestServiceRestartRefusesAnotherProjectsUnit: unit names are per OS user,
// so project B's systemd/<unit> file does not make the unit B's. A restart
// whose unit systemd loads from another file (or does not load) runs only
// the show, never the restart.
func TestServiceRestartRefusesAnotherProjectsUnit(t *testing.T) {
	for name, show := range map[string]string{
		"other project":   "LoadState=loaded\nFragmentPath=/srv/a/systemd/piperoom-connect.service\n",
		"masked":          "LoadState=masked\nFragmentPath=/dev/null\n",
		"not loaded":      "LoadState=not-found\n",
		"look-alike path": "LoadState=loaded\nFragmentPath=/systemd/piperoom-connect.service\n",
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, Options{})
			f.write("systemd/piperoom-connect.service", wantUnit(f.dir, ModeDistributed), 0o644)
			f.exec.respond = func([]string) ([]byte, int, error) { return []byte(show), 0, nil }
			if _, err := f.run("service_restart", UnitParams{}); !errors.Is(err, ErrDetect) {
				t.Fatalf("restart: %v, want ErrDetect", err)
			}
			for _, argv := range f.exec.argvs() {
				if argv[2] != "show" {
					t.Errorf("ran %q", argv)
				}
			}
		})
	}
	// A FragmentPath that is a link to this project's unit file is this
	// project's unit.
	f := newFixture(t, Options{ResolvePath: func(p string) (string, error) {
		if p == "/home/u/.config/systemd/user/piperoom-connect.service" {
			return "/real/systemd/piperoom-connect.service", nil
		}
		if strings.HasSuffix(p, "/systemd/piperoom-connect.service") {
			return "/real/systemd/piperoom-connect.service", nil
		}
		return p, nil
	}})
	f.write("systemd/piperoom-connect.service", wantUnit(f.dir, ModeDistributed), 0o644)
	f.exec.respond = func([]string) ([]byte, int, error) {
		return []byte("LoadState=loaded\nFragmentPath=/home/u/.config/systemd/user/piperoom-connect.service\n"), 0, nil
	}
	if _, err := f.run("service_restart", UnitParams{}); err != nil {
		t.Errorf("linked unit: %v", err)
	}
}

// TestServiceInstallRefusesATakenName: a unit name systemd already loads
// from another file is refused before the unit file is written or enable
// runs.
func TestServiceInstallRefusesATakenName(t *testing.T) {
	f := newFixture(t, Options{})
	f.distribution()
	f.exec.respond = func([]string) ([]byte, int, error) {
		return []byte("LoadState=loaded\nFragmentPath=/srv/a/systemd/piperoom-connect.service\n"), 0, nil
	}
	_, err := f.run("service_install", ServiceParams{})
	if !errors.Is(err, ErrDetect) || !strings.Contains(err.Error(), "/srv/a/") {
		t.Fatalf("install over another project's unit: %v", err)
	}
	if _, serr := os.Stat(f.dir + "/systemd/piperoom-connect.service"); !errors.Is(serr, os.ErrNotExist) {
		t.Errorf("the unit file was written: %v", serr)
	}
	show, _ := SystemctlArgv("show", "piperoom-connect.service")
	if calls := f.exec.argvs(); !reflect.DeepEqual(calls, [][]string{show}) {
		t.Errorf("commands %q", calls)
	}
	// Reinstalling this project's own unit is fine.
	g := newFixture(t, Options{})
	g.distribution()
	g.exec.respond = func(args []string) ([]byte, int, error) {
		if args[1] == "show" {
			return []byte("LoadState=loaded\nFragmentPath=" + g.dir + "/systemd/piperoom-connect.service\n"), 0, nil
		}
		return nil, 0, nil
	}
	if _, err := g.run("service_install", ServiceParams{}); err != nil {
		t.Errorf("reinstall: %v", err)
	}
}

// TestServiceInstallCommandFailures: a failed enable stops before
// daemon-reload and removes the unit file it created; a failed daemon-reload
// is reported the same way.
func TestServiceInstallCommandFailures(t *testing.T) {
	show, _ := SystemctlArgv("show", "piperoom-connect.service")
	for _, c := range []struct {
		verb     string
		want     int // commands run
		keepFile bool
	}{
		{"enable", 2, false},
		{"daemon-reload", 3, true},
	} {
		t.Run(c.verb, func(t *testing.T) {
			f := newFixture(t, Options{})
			f.distribution()
			f.exec.respond = func(args []string) ([]byte, int, error) {
				if args[1] == c.verb {
					return []byte("Failed to " + c.verb + ": Access denied\n"), 1, nil
				}
				return nil, 0, nil
			}
			res, err := f.run("service_install", ServiceParams{})
			if !errors.Is(err, ops.ErrExec) {
				t.Fatalf("err %v, want ErrExec", err)
			}
			if !strings.Contains(res.Summary, "systemctl --user "+c.verb+" failed (exit 1)") || !strings.Contains(res.Summary, "Access denied") || res.Receipt.ExitCode != 1 {
				t.Errorf("result %q %+v", res.Summary, res.Receipt)
			}
			calls := f.exec.argvs()
			if len(calls) != c.want || !reflect.DeepEqual(calls[0], show) || calls[len(calls)-1][2] != c.verb {
				t.Errorf("commands %q", calls)
			}
			_, serr := os.Stat(f.dir + "/systemd/piperoom-connect.service")
			if c.keepFile != (serr == nil) {
				t.Errorf("unit file after a failed %s: %v", c.verb, serr)
			}
		})
	}
	// A failed enable leaves a unit file that existed before in place.
	f := newFixture(t, Options{})
	f.distribution()
	f.write("systemd/piperoom-connect.service", "old\n", 0o644)
	f.exec.respond = func(args []string) ([]byte, int, error) {
		if args[1] == "enable" {
			return nil, 1, nil
		}
		return nil, 0, nil
	}
	if _, err := f.run("service_install", ServiceParams{}); !errors.Is(err, ops.ErrExec) {
		t.Fatalf("err %v", err)
	}
	if _, serr := os.Stat(f.dir + "/systemd/piperoom-connect.service"); serr != nil {
		t.Errorf("an existing unit file was removed: %v", serr)
	}
}

// TestJavaHomeInsideTheProject: a JDK under the project directory is what
// the unit runs, so it must be under a mount directory file_write cannot
// change.
func TestJavaHomeInsideTheProject(t *testing.T) {
	f := newFixture(t, Options{})
	f.distribution()
	for _, java := range []string{f.dir + "/jdk/bin/java", f.dir + "/bin/java", f.dir + "/logs/jdk/bin/java"} {
		f.java = java
		if _, err := f.run("service_install", ServiceParams{}); !errors.Is(err, ErrDetect) {
			t.Errorf("JAVA_HOME of %s: %v, want ErrDetect", java, err)
		}
	}
	if calls := f.exec.argvs(); len(calls) != 0 {
		t.Errorf("commands ran: %q", calls)
	}
	f.java = f.dir + "/kafka/jdk/bin/java"
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatalf("JDK under kafka/: %v", err)
	}
	if u := f.read("systemd/piperoom-connect.service"); !strings.Contains(u, "\nEnvironment=JAVA_HOME="+f.dir+"/kafka/jdk\n") {
		t.Errorf("unit:\n%s", u)
	}
	// The project reached through a symlink: the resolved JDK path is
	// still recognised as inside it.
	// EvalSymlinks: a temp dir can itself sit behind a symlink (macOS's
	// /var is /private/var), and the JDK path a unit runs is a resolved one.
	real, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	link := t.TempDir() + "/p"
	mustSymlink(t, real, link)
	g := newFixtureIn(t, link, Options{})
	g.distribution()
	g.java = real + "/jdk/bin/java"
	if _, err := g.run("service_install", ServiceParams{}); !errors.Is(err, ErrDetect) {
		t.Errorf("JAVA_HOME inside the symlinked project: %v", err)
	}
}

func TestRequireFileWithoutRoot(t *testing.T) {
	if err := requireFile(ops.Env{}, "connect-distributed.properties", false); err == nil || !strings.Contains(err.Error(), "not open") {
		t.Errorf("nil Confine: %v", err)
	}
}
