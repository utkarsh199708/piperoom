package host

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/setup/protocol"
)

func TestSelectScope(t *testing.T) {
	cases := []struct {
		goos  string
		euid  int
		class string
		want  UnitScope
	}{
		{"linux", 1000, "", ScopeUser},
		{"linux", 1000, protocol.ClassLocal, ScopeUser},
		{"linux", 1000, protocol.ClassShared, ScopeUser},
		{"linux", 1000, protocol.ClassProduction, ScopeSystem},
		{"linux", 0, protocol.ClassLocal, ScopeSystem},
		{"linux", 0, "", ScopeSystem},
		{"windows", 0, protocol.ClassProduction, ScopeUser}, // no systemd scopes
		{"darwin", 0, protocol.ClassProduction, ScopeUser},
	}
	for _, c := range cases {
		if got := selectScope(c.goos, c.euid, c.class); got != c.want {
			t.Errorf("selectScope(%s, %d, %q) = %v, want %v", c.goos, c.euid, c.class, got, c.want)
		}
	}
}

// TestSystemctlArgvScope is the argv of a user unit against a system unit:
// --user or not, and enable by path (user) or by name (system).
func TestSystemctlArgvScope(t *testing.T) {
	props := "--property=LoadState,ActiveState,SubState,UnitFileState,Result,MainPID,ExecMainStatus,NRestarts,FragmentPath"
	cases := []struct {
		scope         UnitScope
		verb, operand string
		want          []string
	}{
		{ScopeUser, "show", "piperoom-connect.service", []string{"systemctl", "--user", "show", "--no-pager", props, "piperoom-connect.service"}},
		{ScopeSystem, "show", "piperoom-connect.service", []string{"systemctl", "show", "--no-pager", props, "piperoom-connect.service"}},
		{ScopeUser, "enable", "/srv/p/systemd/piperoom-connect.service", []string{"systemctl", "--user", "enable", "/srv/p/systemd/piperoom-connect.service"}},
		{ScopeSystem, "enable", "piperoom-connect.service", []string{"systemctl", "enable", "piperoom-connect.service"}},
		{ScopeUser, "daemon-reload", "", []string{"systemctl", "--user", "daemon-reload"}},
		{ScopeSystem, "daemon-reload", "", []string{"systemctl", "daemon-reload"}},
		{ScopeUser, "restart", "piperoom-connect.service", []string{"systemctl", "--user", "restart", "piperoom-connect.service"}},
		{ScopeSystem, "restart", "piperoom-connect.service", []string{"systemctl", "restart", "piperoom-connect.service"}},
	}
	for _, c := range cases {
		got, err := SystemctlArgvScope(c.scope, c.verb, c.operand)
		if err != nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("SystemctlArgvScope(%v, %q, %q) = %q, %v; want %q", c.scope, c.verb, c.operand, got, err, c.want)
		}
	}
	bad := []struct {
		scope         UnitScope
		verb, operand string
	}{
		{ScopeSystem, "enable", "/etc/systemd/system/piperoom-connect.service"}, // a path is the user form
		{ScopeSystem, "enable", "/srv/p/systemd/piperoom-connect.service"},
		{ScopeSystem, "restart", "sshd.service"},
		{ScopeSystem, "--user", ""},
		{ScopeUser, "enable", "piperoom-connect.service"},
		{UnitScope(2), "show", "piperoom-connect.service"},
		{UnitScope(-1), "daemon-reload", ""},
	}
	for _, c := range bad {
		if got, err := SystemctlArgvScope(c.scope, c.verb, c.operand); err == nil {
			t.Errorf("SystemctlArgvScope(%v, %q, %q) = %q, want a refusal", c.scope, c.verb, c.operand, got)
		}
	}
}

// TestNoParamSelectsTheSystemUnit: the service ops' params have only a name
// and a mode, an extra field naming a scope or a unit directory is refused
// before anything runs, and every valid combination installs a user unit.
func TestNoParamSelectsTheSystemUnit(t *testing.T) {
	skipOnWindows(t)
	for _, typ := range []reflect.Type{reflect.TypeOf(ServiceParams{}), reflect.TypeOf(UnitParams{})} {
		for i := 0; i < typ.NumField(); i++ {
			if n := typ.Field(i).Name; n != "Name" && n != "Mode" {
				t.Errorf("%s.%s: a new service param; prove it cannot select the system scope", typ.Name(), n)
			}
		}
	}
	extras := []string{
		`"scope":"system"`, `"system":true`, `"user":false`, `"unit_dir":"/etc/systemd/system"`,
		`"systemUnitDir":"/etc/systemd/system"`, `"geteuid":0`, `"class":"production"`, `"Scope":1`,
	}
	for _, op := range []string{"service_install", "service_status", "service_restart"} {
		for _, extra := range extras {
			f := newFixture(t, Options{})
			f.distribution()
			raw := json.RawMessage(`{"name":"connect",` + extra + `}`)
			if _, err := f.reg.Execute(t.Context(), protocol.OpRequest{ID: "r", Op: op, Params: raw, Tier: protocol.TierT2, Class: protocol.ClassLocal}); !errors.Is(err, ErrParams) {
				t.Errorf("%s with %s: %v, want ErrParams", op, extra, err)
			}
			if calls := f.exec.argvs(); len(calls) != 0 {
				t.Errorf("%s with %s ran %q", op, extra, calls)
			}
		}
	}
	for _, name := range []string{"", "connect", "system", "root", "etc"} {
		for _, mode := range []string{"", ModeDistributed, ModeStandalone} {
			f := newFixture(t, Options{})
			f.distribution()
			res, err := f.run("service_install", ServiceParams{Name: name, Mode: mode})
			if err != nil {
				t.Fatalf("install %q/%q: %v", name, mode, err)
			}
			for _, argv := range f.exec.argvs() {
				if len(argv) < 2 || argv[1] != "--user" {
					t.Errorf("install %q/%q ran %q, not a user-scope command", name, mode, argv)
				}
			}
			var data map[string]any
			_ = json.Unmarshal(res.Data, &data)
			if data["scope"] != "user" || !strings.HasPrefix(data["unit_file"].(string), UnitDir+"/") {
				t.Errorf("install %q/%q: %v", name, mode, data)
			}
		}
	}
}

// systemFixture is a runner detected as root, with the system unit directory
// and a JDK in temporary directories and every file reported root-owned (an
// unprivileged test cannot create root-owned files; the mode bits are real).
func systemFixture(t *testing.T, opts Options) (*fixture, string) {
	t.Helper()
	unitDir := realTempDir(t)
	jdk := realTempDir(t) + "/jdk"
	if opts.geteuid == nil {
		opts.geteuid = func() int { return 0 }
	}
	opts.systemUnitDir = unitDir
	if opts.owner == nil {
		opts.owner = func(fs.FileInfo) (uint64, bool) { return 0, true }
	}
	f := newFixtureIn(t, realTempDir(t), opts)
	if err := os.MkdirAll(jdk+"/bin", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(jdk+"/bin/java", []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	f.java = jdk + "/bin/java"
	f.distribution()
	return f, unitDir
}

func TestServiceInstallSystemUnit(t *testing.T) {
	skipOnWindows(t)
	f, unitDir := systemFixture(t, Options{})
	unitPath := unitDir + "/piperoom-connect.service"
	res, err := f.run("service_install", ServiceParams{})
	if err != nil {
		t.Fatalf("install: %v (%s)", err, res.Summary)
	}
	text, err := os.ReadFile(unitPath)
	if err != nil {
		t.Fatalf("the system unit was not written: %v", err)
	}
	if !strings.Contains(string(text), "\nWantedBy=multi-user.target\n") || !strings.Contains(string(text), "\nWorkingDirectory="+f.dir+"\n") ||
		!strings.Contains(string(text), "\nExecStart="+f.dir+"/kafka/bin/connect-distributed.sh "+f.dir+"/connect-distributed.properties\n") {
		t.Errorf("unit:\n%s", text)
	}
	for _, line := range strings.Split(string(text), "\n") {
		if k, _, _ := strings.Cut(line, "="); k == "User" || k == "Group" || k == "ExecStartPre" {
			t.Errorf("unit carries %q", line)
		}
	}
	if _, err := os.Stat(f.dir + "/systemd/piperoom-connect.service"); err == nil {
		t.Error("a system install wrote a unit into the project")
	}
	show, _ := SystemctlArgvScope(ScopeSystem, "show", "piperoom-connect.service")
	want := [][]string{show, {"systemctl", "daemon-reload"}, {"systemctl", "enable", "piperoom-connect.service"}}
	if calls := f.exec.argvs(); !reflect.DeepEqual(calls, want) {
		t.Errorf("commands %q, want %q", calls, want)
	}
	if fi, _ := os.Stat(unitPath); fi.Mode().Perm() != 0o644 {
		t.Errorf("unit mode %v", fi.Mode().Perm())
	}

	// service_status and service_restart follow the installed unit's scope.
	f.exec.calls = nil
	f.exec.respond = func(args []string) ([]byte, int, error) {
		if args[0] == "show" {
			return []byte("LoadState=loaded\nActiveState=active\nSubState=running\nFragmentPath=" + unitPath + "\n"), 0, nil
		}
		return nil, 0, nil
	}
	res, err = f.run("service_status", UnitParams{})
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]any
	_ = json.Unmarshal(res.Data, &data)
	if data["scope"] != "system" || data["this_project"] != true {
		t.Errorf("status %v", data)
	}
	if _, err := f.run("service_restart", UnitParams{}); err != nil {
		t.Fatalf("restart: %v", err)
	}
	restart := []string{"systemctl", "restart", "piperoom-connect.service"}
	if calls := f.exec.argvs(); !reflect.DeepEqual(calls, [][]string{show, show, restart, show}) {
		t.Errorf("commands %q", calls)
	}

	// The preview diffs the system unit file.
	op, _ := f.reg.Get("service_install")
	pv, err := op.(serviceInstallOp).Preview(t.Context(), json.RawMessage(`{"mode":"standalone"}`), f.env())
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if pv.Target != unitPath || !strings.Contains(pv.Expected, "+ ExecStart="+f.dir+"/kafka/bin/connect-standalone.sh") {
		t.Errorf("preview %+v", pv)
	}
}

func TestServiceInstallSystemRefusals(t *testing.T) {
	skipOnWindows(t)
	t.Run("production class without root", func(t *testing.T) {
		f, unitDir := systemFixture(t, Options{Class: protocol.ClassProduction, geteuid: func() int { return 1000 }})
		if _, err := f.run("service_install", ServiceParams{}); !errors.Is(err, ErrDetect) {
			t.Fatalf("install: %v, want ErrDetect", err)
		}
		if calls := f.exec.argvs(); len(calls) != 0 {
			t.Errorf("ran %q", calls)
		}
		if _, err := os.Stat(unitDir + "/piperoom-connect.service"); err == nil {
			t.Error("a unit was written")
		}
	})
	t.Run("a tree another user owns", func(t *testing.T) {
		var dist string
		f, unitDir := systemFixture(t, Options{owner: func(fi fs.FileInfo) (uint64, bool) {
			if fi.Name() == "connect-distributed.sh" {
				return 1000, true
			}
			return 0, true
		}})
		dist = f.dir + "/kafka"
		if _, err := f.run("service_install", ServiceParams{}); !errors.Is(err, ErrDetect) || !strings.Contains(err.Error(), "uid 1000") {
			t.Fatalf("install: %v, want ErrDetect naming the owner", err)
		}
		if _, err := os.Stat(unitDir + "/piperoom-connect.service"); err == nil {
			t.Errorf("a unit was written over %s", dist)
		}
	})
	t.Run("a group-writable script", func(t *testing.T) {
		f, _ := systemFixture(t, Options{})
		if err := os.Chmod(f.dir+"/kafka/bin/connect-distributed.sh", 0o775); err != nil {
			t.Fatal(err)
		}
		if _, err := f.run("service_install", ServiceParams{}); !errors.Is(err, ErrDetect) || !strings.Contains(err.Error(), "writable by group or others") {
			t.Fatalf("install: %v", err)
		}
	})
	t.Run("an unreadable owner", func(t *testing.T) {
		f, _ := systemFixture(t, Options{owner: func(fs.FileInfo) (uint64, bool) { return 0, false }})
		if _, err := f.run("service_install", ServiceParams{}); !errors.Is(err, ErrDetect) {
			t.Fatalf("install: %v", err)
		}
	})
	t.Run("a symlinked system unit", func(t *testing.T) {
		f, unitDir := systemFixture(t, Options{})
		keep := realTempDir(t) + "/keep"
		if err := os.WriteFile(keep, []byte("keep\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		mustSymlink(t, keep, unitDir+"/piperoom-connect.service")
		if _, err := f.run("service_install", ServiceParams{}); err == nil {
			t.Fatal("install over a symlinked unit file succeeded")
		}
		if b, _ := os.ReadFile(keep); string(b) != "keep\n" {
			t.Error("the symlink target was written")
		}
	})
	t.Run("another project's system unit", func(t *testing.T) {
		f, unitDir := systemFixture(t, Options{})
		unitPath := unitDir + "/piperoom-connect.service"
		if err := os.WriteFile(unitPath, []byte("[Service]\nWorkingDirectory=/srv/other\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		f.exec.respond = func(args []string) ([]byte, int, error) {
			return []byte("LoadState=loaded\nFragmentPath=" + unitPath + "\n"), 0, nil
		}
		if _, err := f.run("service_install", ServiceParams{}); !errors.Is(err, ErrDetect) {
			t.Fatalf("install over another project's unit: %v", err)
		}
		if _, err := f.run("service_restart", UnitParams{}); !errors.Is(err, ErrDetect) {
			t.Fatalf("restart of another project's unit: %v", err)
		}
		for _, argv := range f.exec.argvs() {
			if argv[1] == "restart" {
				t.Errorf("ran %q", argv)
			}
		}
		if b, _ := os.ReadFile(unitPath); !strings.Contains(string(b), "/srv/other") {
			t.Error("another project's unit was overwritten")
		}
	})
	t.Run("restart re-checks the tree", func(t *testing.T) {
		f, unitDir := systemFixture(t, Options{})
		if _, err := f.run("service_install", ServiceParams{}); err != nil {
			t.Fatal(err)
		}
		unitPath := unitDir + "/piperoom-connect.service"
		f.exec.calls = nil
		f.exec.respond = func(args []string) ([]byte, int, error) {
			return []byte("LoadState=loaded\nFragmentPath=" + unitPath + "\n"), 0, nil
		}
		if err := os.Chmod(f.dir+"/kafka/bin", 0o777); err != nil {
			t.Fatal(err)
		}
		if _, err := f.run("service_restart", UnitParams{}); !errors.Is(err, ErrDetect) {
			t.Fatalf("restart over a world-writable bin/: %v", err)
		}
		for _, argv := range f.exec.argvs() {
			if argv[1] == "restart" {
				t.Errorf("ran %q", argv)
			}
		}
	})
}

func TestFileOwner(t *testing.T) {
	skipOnWindows(t)
	fi, err := os.Lstat(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uid, ok := fileOwner(fi)
	if !ok || int(uid) != os.Getuid() {
		t.Errorf("fileOwner = %d, %v; want %d", uid, ok, os.Getuid())
	}
	if _, ok := fileOwner(fakeOwnerless{}); ok {
		t.Error("a FileInfo without a stat struct reported an owner")
	}
}

type fakeOwnerless struct{ fs.FileInfo }

func (fakeOwnerless) Sys() any { return nil }
