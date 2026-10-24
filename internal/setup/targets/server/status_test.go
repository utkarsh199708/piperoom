package server

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// composeFixture is a compose-mode fixture with a rendered stack and a .env
// file to read.
func composeFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t, Options{})
	if _, err := f.run("server_compose_write", ComposeParams{}); err != nil {
		t.Fatalf("server_compose_write: %v", err)
	}
	f.write(EnvFileName, "POSTGRES_USER=piperoom\nPOSTGRES_DB=orders\nPOSTGRES_PASSWORD=piperoom-it\n", 0o600)
	return f
}

func TestStatusComposeRunning(t *testing.T) {
	f := composeFixture(t)
	f.exec.respond = func(argv []string) ([]byte, int, error) {
		return []byte(`[{"ID":"7f2d1a3c9b4e5f60718293a4b5c6d7e8f90123456789abcd","Name":"piperoom-dbzserver-dbzserver-1","Service":"dbzserver","State":"running","Health":"","Image":"quay.io/debezium/server","ExitCode":0}]`), 0, nil
	}
	res, err := f.run("server_status", nil)
	if err != nil {
		t.Fatalf("server_status: %v", err)
	}
	calls := f.exec.argvs()
	if len(calls) != 1 {
		t.Fatalf("commands = %q", calls)
	}
	want := []string{
		"docker", "compose", "--project-directory", f.dir, "-f", f.dir + "/" + ComposeFile,
		"--env-file", f.dir + "/" + EnvFileName, "ps", "--format", "json", DefaultService,
	}
	if !reflect.DeepEqual(calls[0], want) {
		t.Errorf("argv = %q, want %q", calls[0], want)
	}
	data := f.data(res)
	if data["running"] != true || data["state"] != "running" || data["installed"] != true || data["containers"] != float64(1) {
		t.Errorf("data = %v", data)
	}
	if data["container_id"] != "7f2d1a3c9b4e" {
		t.Errorf("container_id = %v", data["container_id"])
	}
	if res.Summary != "dbzserver: running" {
		t.Errorf("summary = %q", res.Summary)
	}
	if !reflect.DeepEqual(res.Receipt.Argv, want) || res.Receipt.Kind != "exec" {
		t.Errorf("receipt = %+v", res.Receipt)
	}
}

func TestStatusComposeNDJSONAndStopped(t *testing.T) {
	f := composeFixture(t)
	f.exec.respond = func([]string) ([]byte, int, error) {
		return []byte("{\"ID\":\"abc\",\"Service\":\"dbzserver\",\"State\":\"exited\",\"ExitCode\":1}\n" +
			"{\"ID\":\"def\",\"Service\":\"dbzserver\",\"State\":\"exited\",\"ExitCode\":1}\n"), 0, nil
	}
	res, err := f.run("server_status", nil)
	if err != nil {
		t.Fatalf("server_status: %v", err)
	}
	data := f.data(res)
	if data["running"] != false || data["exit_code"] != float64(1) || data["containers"] != float64(2) {
		t.Errorf("data = %v", data)
	}
	if !strings.Contains(res.Summary, "exited") || !strings.Contains(res.Summary, "exit code 1") || !strings.Contains(res.Summary, "2 containers") {
		t.Errorf("summary = %q", res.Summary)
	}
}

func TestStatusComposeNoContainer(t *testing.T) {
	f := composeFixture(t)
	f.exec.respond = func([]string) ([]byte, int, error) { return []byte("[]"), 0, nil }
	res, err := f.run("server_status", nil)
	if err != nil {
		t.Fatalf("server_status: %v", err)
	}
	if data := f.data(res); data["running"] != false || data["containers"] != float64(0) {
		t.Errorf("data = %v", data)
	}
	if !strings.Contains(res.Summary, "no container") {
		t.Errorf("summary = %q", res.Summary)
	}
}

func TestStatusComposeNotRendered(t *testing.T) {
	f := newFixture(t, Options{})
	res, err := f.run("server_status", nil)
	if err != nil {
		t.Fatalf("server_status: %v", err)
	}
	if data := f.data(res); data["installed"] != false || data["running"] != false {
		t.Errorf("data = %v", data)
	}
	if calls := f.exec.argvs(); len(calls) != 0 {
		t.Errorf("commands ran without a rendered stack: %q", calls)
	}
	if !strings.Contains(res.Summary, "not written") {
		t.Errorf("summary = %q", res.Summary)
	}
}

func TestStatusComposeFailure(t *testing.T) {
	f := composeFixture(t)
	f.exec.respond = func([]string) ([]byte, int, error) {
		return []byte("no configuration file provided: not found\nmore\n"), 1, nil
	}
	res, err := f.run("server_status", nil)
	if !errors.Is(err, ops.ErrExec) {
		t.Fatalf("err = %v, want ErrExec", err)
	}
	if !strings.Contains(res.Summary, "exit 1") || !strings.Contains(res.Summary, "no configuration file") {
		t.Errorf("summary = %q", res.Summary)
	}
	if data := f.data(res); data["exit_code"] != float64(1) {
		t.Errorf("data = %v", data)
	}
	if res.Receipt.ExitCode != 1 {
		t.Errorf("receipt = %+v", res.Receipt)
	}
	f.exec.respond = func([]string) ([]byte, int, error) { return nil, -1, errors.New("exec: docker: not found") }
	if _, err := f.run("server_status", nil); !errors.Is(err, ops.ErrExec) {
		t.Errorf("missing docker: %v", err)
	}
}

// TestStatusComposeTamperedFile refuses a compose file that no longer passes
// the validator: the read never hands it to docker.
func TestStatusComposeTamperedFile(t *testing.T) {
	f := composeFixture(t)
	doc := f.read(ComposeFile)
	f.write(ComposeFile, strings.Replace(doc, "    image:", "    privileged: true\n    image:", 1), 0o644)
	_, err := f.run("server_status", nil)
	if !errors.Is(err, ErrStatus) {
		t.Fatalf("err = %v, want ErrStatus", err)
	}
	if calls := f.exec.argvs(); len(calls) != 0 {
		t.Errorf("docker ran on an invalid file: %q", calls)
	}
}

func TestStatusComposeName(t *testing.T) {
	f := composeFixture(t)
	if _, err := f.run("server_status", StatusParams{Name: "dbz server"}); !errors.Is(err, ErrParams) {
		t.Errorf("a malformed service name: %v", err)
	}
	if _, err := f.run("server_status", map[string]any{"service": "dbzserver"}); !errors.Is(err, ErrParams) {
		t.Errorf("an unknown field: %v", err)
	}
}

// serviceFixture is a service-mode fixture with the unit file and a fake
// systemctl.
func serviceFixture(t *testing.T, opts Options) *fixture {
	t.Helper()
	if opts.Mode == "" {
		opts.Mode = ModeService
	}
	f := newFixture(t, opts)
	f.write(UnitDir+"/"+UnitName, "# unit\n", 0o644)
	return f
}

func TestStatusServiceRunning(t *testing.T) {
	f := serviceFixture(t, Options{})
	f.exec.respond = func([]string) ([]byte, int, error) {
		return []byte("LoadState=loaded\nActiveState=active\nSubState=running\nUnitFileState=enabled\nResult=success\n" +
			"MainPID=4242\nExecMainStatus=0\nNRestarts=1\nFragmentPath=" + f.dir + "/" + UnitDir + "/" + UnitName + "\nUnrelated=x\n"), 0, nil
	}
	res, err := f.run("server_status", nil)
	if err != nil {
		t.Fatalf("server_status: %v", err)
	}
	want := []string{"systemctl", "--user", "show", "--no-pager",
		"--property=LoadState,ActiveState,SubState,UnitFileState,Result,MainPID,ExecMainStatus,NRestarts,FragmentPath", UnitName}
	calls := f.exec.argvs()
	if len(calls) != 1 || !reflect.DeepEqual(calls[0], want) {
		t.Errorf("argv = %q, want %q", calls, want)
	}
	data := f.data(res)
	if data["running"] != true || data["installed"] != true || data["this_project"] != true || data["main_pid"] != "4242" {
		t.Errorf("data = %v", data)
	}
	if data["mode"] != ModeService {
		t.Errorf("mode = %v", data["mode"])
	}
	if _, leaked := data["Unrelated"]; leaked {
		t.Error("an unrequested property reached Data")
	}
	if res.Summary != UnitName+": active (running), enabled" {
		t.Errorf("summary = %q", res.Summary)
	}
}

func TestStatusServiceForeignAndMissing(t *testing.T) {
	f := serviceFixture(t, Options{})
	f.exec.respond = func([]string) ([]byte, int, error) {
		return []byte("LoadState=loaded\nActiveState=active\nSubState=running\nFragmentPath=/home/other/systemd/" + UnitName + "\n"), 0, nil
	}
	res, err := f.run("server_status", nil)
	if err != nil {
		t.Fatal(err)
	}
	if data := f.data(res); data["this_project"] != false || !strings.Contains(res.Summary, "not this project's") {
		t.Errorf("foreign unit: %q %v", res.Summary, data)
	}
	f.exec.respond = func([]string) ([]byte, int, error) {
		return []byte("LoadState=not-found\nActiveState=inactive\nSubState=dead\n"), 0, nil
	}
	res, err = f.run("server_status", StatusParams{Name: "dbzserver"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary != UnitName+" is not installed" {
		t.Errorf("not installed: %q", res.Summary)
	}
	f.exec.respond = func([]string) ([]byte, int, error) {
		return []byte("Failed to connect to bus: No such file or directory\n"), 1, nil
	}
	if _, err := f.run("server_status", nil); !errors.Is(err, ops.ErrExec) {
		t.Errorf("bus failure: %v", err)
	}
}

func TestStatusServiceUnsupportedOS(t *testing.T) {
	f := serviceFixture(t, Options{GOOS: "windows"})
	if _, err := f.run("server_status", nil); !errors.Is(err, ErrUnsupported) {
		t.Errorf("err = %v, want ErrUnsupported", err)
	}
	if calls := f.exec.argvs(); len(calls) != 0 {
		t.Errorf("commands ran: %q", calls)
	}
	if _, err := f.run("server_status", StatusParams{Name: "DBZ"}); !errors.Is(err, ErrParams) {
		t.Errorf("malformed unit segment: %v", err)
	}
}

func TestStatusBoundsOutput(t *testing.T) {
	f := composeFixture(t)
	big := make([]byte, 100<<10)
	for i := range big {
		big[i] = 'x'
	}
	f.exec.respond = func([]string) ([]byte, int, error) { return big, 0, nil }
	// Not JSON: the parse fails, but the op reads at most maxCommandOutput.
	if _, err := f.run("server_status", nil); !errors.Is(err, ErrStatus) {
		t.Errorf("garbage output: %v", err)
	}
	if _, err := composeArgv("relative/dir", false, ""); !errors.Is(err, ErrParams) {
		t.Errorf("a relative project directory: %v", err)
	}
	if _, err := composeArgv(f.dir, false, "a b"); !errors.Is(err, ErrParams) {
		t.Errorf("a spaced service name: %v", err)
	}
	// The status op is a read: it registers with the T0 floor.
	op, ok := f.reg.Get("server_status")
	if !ok || op.Tier() != protocol.TierT0 {
		t.Errorf("server_status tier = %v", op.Tier())
	}
}

// TestRegisterSurface pins the op surface this driver adds: the names, the
// tiers, and the mount directories that keep the generic file ops out of what
// the server executes and loads.
func TestRegisterSurface(t *testing.T) {
	f := newFixture(t, Options{})
	want := map[string]string{
		"server_properties_write": "T1",
		"server_compose_write":    "T1",
		"server_unit_write":       "T2",
		"server_status":           "T0",
	}
	for name, tier := range want {
		op, ok := f.reg.Get(name)
		if !ok {
			t.Fatalf("%s is not registered", name)
		}
		if op.Tier() != tier {
			t.Errorf("%s tier = %s, want %s", name, op.Tier(), tier)
		}
		if op.Target() != targetKind {
			t.Errorf("%s target = %q", name, op.Target())
		}
	}
	for _, path := range []string{ConfigFile, DistDir + "/run.sh", UnitDir + "/" + UnitName} {
		if _, err := f.run("file_write", map[string]string{"path": path, "content": "x"}); !errors.Is(err, ops.ErrExecutedName) {
			t.Errorf("file_write %s: %v, want ErrExecutedName", path, err)
		}
	}
	// A file outside the mount directories is still writable.
	if _, err := f.run("file_write", map[string]string{"path": "notes/readme.txt", "content": "hello\n"}); err != nil {
		t.Errorf("file_write of a plain file: %v", err)
	}
}
