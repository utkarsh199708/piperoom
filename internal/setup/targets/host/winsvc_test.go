package host

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// The Windows service target's unit tests (row U8b). The manager's logic is
// platform-independent code behind serviceAPI; fakeSCM and fakeService are the
// SCM, so the whole target runs on any OS, and the GOOS="windows" driver builds
// the same definitions the Windows build constructs (the tests run the Windows
// shape on Linux and the Linux shape on Windows).

// selfBytes is what the "running piperoom-setup.exe" holds in these tests.
var selfBytes = []byte("MZ fake piperoom-setup binary\n")

// createCall is one recorded Create.
type createCall struct {
	name string
	exe  string
	args []string
	cfg  serviceConfig
}

// fakeSCM is serviceAPI: it records every call and answers from services.
type fakeSCM struct {
	mu         sync.Mutex
	connectErr error
	createErrs []error // consumed one per Create attempt; nil = success
	services   map[string]*fakeService
	creates    []createCall
	opens      []string
	connects   int
	closes     int
}

func newFakeSCM() *fakeSCM {
	return &fakeSCM{services: map[string]*fakeService{}}
}

func (f *fakeSCM) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
	return nil
}

func (f *fakeSCM) Create(name, exe string, args []string, cfg serviceConfig) (serviceHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, createCall{name: name, exe: exe, args: append([]string(nil), args...), cfg: cfg})
	if len(f.createErrs) > 0 {
		err := f.createErrs[0]
		f.createErrs = f.createErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if _, dup := f.services[name]; dup {
		return nil, errServiceExists
	}
	s := &fakeService{scm: f, name: name, state: stateStopped}
	f.services[name] = s
	return s, nil
}

func (f *fakeSCM) Open(name string) (serviceHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opens = append(f.opens, name)
	if f.connectErr != nil {
		return nil, f.connectErr
	}
	s, ok := f.services[name]
	if !ok {
		return nil, errServiceNotExist
	}
	return s, nil
}

// add registers a service the fake SCM already knows.
func (f *fakeSCM) add(name string, state serviceState) *fakeService {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := &fakeService{scm: f, name: name, state: state}
	f.services[name] = s
	return s
}

func (f *fakeSCM) createdCalls() []createCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]createCall(nil), f.creates...)
}

func (f *fakeSCM) openedNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.opens...)
}

// fakeService is serviceHandle.
type fakeService struct {
	mu         sync.Mutex
	scm        *fakeSCM
	name       string
	state      serviceState
	exitCode   uint32
	specificEC uint32
	queryErr   error
	startErr   error
	controlErr error
	deleteErr  error
	starts     int
	stops      int
	deletes    int
	closes     int
	onStart    func()
}

func (s *fakeService) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.starts++
	if s.startErr != nil {
		return s.startErr
	}
	s.state = stateRunning
	if s.onStart != nil {
		s.onStart()
	}
	return nil
}

func (s *fakeService) Query() (serviceStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queryErr != nil {
		return serviceStatus{}, s.queryErr
	}
	return serviceStatus{State: s.state, Accepts: acceptStop | acceptShutdown, Win32ExitCode: s.exitCode, ServiceSpecificExitCode: s.specificEC}, nil
}

func (s *fakeService) Control(cmd serviceControl) (serviceStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.controlErr != nil {
		return serviceStatus{}, s.controlErr
	}
	if cmd == controlStop {
		s.stops++
		s.state = stateStopped
	}
	return serviceStatus{State: s.state}, nil
}

func (s *fakeService) Delete() error {
	s.mu.Lock()
	s.deletes++
	err := s.deleteErr
	s.state = stateStopped
	s.mu.Unlock()
	if err != nil {
		return err
	}
	// The SCM removes the name once the delete is processed.
	s.scm.mu.Lock()
	defer s.scm.mu.Unlock()
	if s.scm.services[s.name] == s {
		delete(s.scm.services, s.name)
	}
	return nil
}

func (s *fakeService) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes++
	return nil
}

func (s *fakeService) counts() (starts, stops, deletes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.starts, s.stops, s.deletes
}

// winFixture is a project directory with the host driver on a Windows GOOS and
// the fake SCM wired in.
type winFixture struct {
	t      *testing.T
	dir    string
	reg    *ops.Registry
	drv    *Driver
	scm    *fakeSCM
	self   string
	java   string
	noExec bool
}

func newWindowsFixture(t *testing.T) *winFixture {
	t.Helper()
	dir := t.TempDir()
	f := &winFixture{t: t, dir: dir, scm: newFakeSCM(), java: fakeJava}
	reg, err := ops.New(ops.Config{
		ProjectDir: dir,
		LookPath: func(name string) (string, error) {
			if name != "java" || f.java == "" {
				return "", errors.New("not found")
			}
			return f.java, nil
		},
		Exec: func(context.Context, string, ...string) ([]byte, int, error) {
			f.noExec = true
			return nil, 0, errors.New("the Windows service target runs no command")
		},
		Now: func() time.Time { return time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	drv, err := Register(reg, Options{GOOS: "windows", ResolvePath: func(p string) (string, error) { return p, nil }})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	self := filepath.Join(t.TempDir(), RunnerExe)
	if err := os.WriteFile(self, selfBytes, 0o755); err != nil {
		t.Fatal(err)
	}
	f.reg, f.drv, f.self = reg, drv, self
	mgr := newWindowsManager(func() (serviceAPI, error) {
		if f.scm.connectErr != nil {
			return nil, f.scm.connectErr
		}
		return f.scm, nil
	}, func() (string, error) { return f.self, nil })
	mgr.poll = time.Millisecond
	drv.mgr = mgr
	return f
}

// layout writes the distribution (with its log4j configuration, as a Kafka
// 4 tarball has) and both worker files.
func (f *winFixture) layout() {
	f.write("kafka/libs/kafka-clients-4.0.0.jar", "jar\n", 0o644)
	f.write("kafka/config/connect-log4j2.yaml", "appenders: {}\n", 0o644)
	f.write("connect-distributed.properties", "bootstrap.servers=127.0.0.1:9092\n", 0o600)
	f.write("connect-standalone.properties", "bootstrap.servers=127.0.0.1:9092\n", 0o600)
}

func (f *winFixture) write(name, content string, mode os.FileMode) {
	f.t.Helper()
	p := filepath.Join(f.dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		f.t.Fatal(err)
	}
}

func (f *winFixture) read(name string) string {
	f.t.Helper()
	b, err := os.ReadFile(filepath.Join(f.dir, filepath.FromSlash(name)))
	if err != nil {
		f.t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// writeFileAt writes an absolute path outside the project (the binary the
// manager copies is found through os.Executable, not through the project).
func (f *winFixture) writeFileAt(path string, content []byte) {
	f.t.Helper()
	if err := os.WriteFile(path, content, 0o755); err != nil {
		f.t.Fatal(err)
	}
}

func (f *winFixture) run(op string, params any) (protocol.OpResult, error) {
	f.t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		f.t.Fatalf("marshal: %v", err)
	}
	return f.reg.Execute(context.Background(), protocol.OpRequest{ID: "w1", Op: op, Params: raw, Tier: protocol.TierT2, Class: protocol.ClassLocal})
}

// fakeJavaHome is the JDK home the fixture's java resolves to (fakeJava is
// <home>/bin/java).
const fakeJavaHome = "/opt/jdk-21"

// wantServiceArgs is the command line service_install gives the SCM: the
// subcommand and the project directory only.
func wantServiceArgs(dir, mode string) []string {
	return []string{
		RunServiceSubcommand, "--project", dir,
		"--name", "connect", "--mode", mode, "--java", fakeJavaHome,
	}
}

// wantJavaArgv is the JVM argv the service host starts: the heap and log
// directory the distribution's connect-<mode>.sh sets, the log4j configuration
// from the distribution, and the wildcard classpath.
func wantJavaArgv(dir, mode, log4j string) []string {
	sep := "/"
	if isDrivePath(dir) {
		sep = `\`
	}
	argv := []string{
		nativeJoin(fakeJavaHome, "bin", "java.exe"),
		"-Xms256M", "-Xmx2G",
		"-Dkafka.logs.dir=" + nativeJoin(dir, LogDir),
	}
	if log4j != "" {
		argv = append(argv, log4j)
	}
	return append(argv,
		"-cp", nativeJoin(dir, DistDir, "libs")+sep+"*",
		connectMain[mode],
		nativeJoin(dir, "connect-"+mode+".properties"),
	)
}

// wantLog4jArg is the log4j argument for the configuration the fixture's
// distribution carries.
func wantLog4jArg(dir string) string {
	return "-Dlog4j2.configurationFile=" + nativeJoin(dir, DistDir, "config", "connect-log4j2.yaml")
}

func TestWindowsServiceInstall(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	res, err := f.run("service_install", ServiceParams{})
	if err != nil {
		t.Fatalf("service_install: %v (%s)", err, res.Summary)
	}
	calls := f.scm.createdCalls()
	if len(calls) != 1 {
		t.Fatalf("%d Create calls, want 1", len(calls))
	}
	c := calls[0]
	if c.name != "piperoom-connect" {
		t.Errorf("service name %q, want piperoom-connect (no .service suffix: that names nothing in the SCM)", c.name)
	}
	wantExe := filepath.Join(f.dir, BinDir, RunnerExe)
	if c.exe != wantExe {
		t.Errorf("exe %q, want the project's copy %q", c.exe, wantExe)
	}
	if got, want := c.args, wantServiceArgs(f.dir, ModeDistributed); !equalStrings(got, want) {
		t.Errorf("args %q, want %q", got, want)
	}
	if !c.cfg.Automatic || c.cfg.DisplayName != "Piperoom Kafka Connect worker (distributed)" {
		t.Errorf("config %+v", c.cfg)
	}
	// The service runs as its own virtual account, never LocalSystem, and the
	// SCM writes that account's SID into the token.
	if c.cfg.Account != `NT SERVICE\piperoom-connect` || c.cfg.Account == "LocalSystem" || !c.cfg.ServiceSID {
		t.Errorf("account %q (ServiceSID %v), want NT SERVICE\\piperoom-connect with the service SID", c.cfg.Account, c.cfg.ServiceSID)
	}
	if strings.Contains(c.cfg.Description, "services") || !strings.Contains(c.cfg.Description, f.dir) {
		t.Errorf("description %q, want the project directory", c.cfg.Description)
	}
	// The exe the SCM runs is a copy of the running binary.
	if got := f.read(BinDir + "/" + RunnerExe); got != string(selfBytes) {
		t.Errorf("bin copy %q, want the running binary's bytes", got)
	}
	// The record is the definition an approver saw; the JVM argv is in it.
	rec := f.read(ServiceDir + "/piperoom-connect.txt")
	for _, want := range append([]string{
		"service=piperoom-connect\n", "display=Piperoom Kafka Connect worker (distributed)\n",
		`account=NT SERVICE\piperoom-connect` + "\n", "start=automatic\n", "exe=" + wantExe + "\n",
		"project=" + f.dir + "\n", "log=" + filepath.Join(f.dir, LogDir, ServiceLogName) + "\n",
	}, argLines("arg", wantServiceArgs(f.dir, ModeDistributed))...) {
		if !strings.Contains(rec, want) {
			t.Errorf("record lacks %q:\n%s", want, rec)
		}
	}
	for _, want := range argLines("java-arg", wantJavaArgv(f.dir, ModeDistributed, wantLog4jArg(f.dir))) {
		if !strings.Contains(rec, want) {
			t.Errorf("record lacks %q:\n%s", want, rec)
		}
	}
	if !strings.Contains(res.Summary, "installed piperoom-connect (distributed)") || !strings.Contains(res.Summary, `account=NT SERVICE\piperoom-connect`) {
		t.Errorf("summary %q", res.Summary)
	}
	if res.Receipt.Kind != "file" || res.Receipt.Target != ServiceDir+"/piperoom-connect.txt" {
		t.Errorf("receipt %+v", res.Receipt)
	}
	var data map[string]any
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["replaced"] != false || data["account"] != `NT SERVICE\piperoom-connect` || data["start_type"] != "automatic" {
		t.Errorf("data %v", data)
	}
	if f.noExec {
		t.Error("service_install ran a command through ops.Env.Exec")
	}
}

// TestWindowsServiceInstallReplaces: a second install is this project's own
// service, so it stops, deletes and recreates it (CreateService builds the
// command line; an update would need a hand-built BinaryPathName, which this
// target never builds).
func TestWindowsServiceInstallReplaces(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatal(err)
	}
	// A fresh running service replaces the one install created (the record
	// is still there, so the name is this project's).
	old := f.scm.add("piperoom-connect", stateRunning)
	res, err := f.run("service_install", ServiceParams{})
	if err != nil {
		t.Fatalf("second install: %v (%s)", err, res.Summary)
	}
	starts, stops, deletes := old.counts()
	if starts != 0 || stops != 1 || deletes != 1 {
		t.Errorf("old service: %d starts, %d stops, %d deletes; want 0/1/1", starts, stops, deletes)
	}
	if !strings.Contains(res.Summary, "replaced piperoom-connect") {
		t.Errorf("summary %q", res.Summary)
	}
	if n := len(f.scm.createdCalls()); n != 2 {
		t.Errorf("%d Create calls, want 2", n)
	}
}

// TestWindowsServiceInstallRefusesAForeignName: a machine-wide service name
// another project (or a person) installed is never stopped or deleted.
func TestWindowsServiceInstallRefusesAForeignName(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	foreign := f.scm.add("piperoom-connect", stateRunning)
	_, err := f.run("service_install", ServiceParams{})
	if !errors.Is(err, ErrDetect) || !strings.Contains(err.Error(), "did not install it") {
		t.Fatalf("install over a foreign service: %v", err)
	}
	if starts, stops, deletes := foreign.counts(); starts+stops+deletes != 0 {
		t.Errorf("the foreign service was touched: %d/%d/%d", starts, stops, deletes)
	}
	if n := len(f.scm.createdCalls()); n != 0 {
		t.Errorf("%d Create calls, want 0", n)
	}
	if _, serr := os.Stat(filepath.Join(f.dir, BinDir, RunnerExe)); !os.IsNotExist(serr) {
		t.Errorf("the exe copy was written: %v", serr)
	}
}

// TestWindowsServiceInstallRunner: the SCM runs a copy of a binary that
// answers run-service, so install copies piperoom-setup — or the
// piperoom-setup next to an umbrella piperoom.exe — and refuses a binary that
// answers neither instead of installing a service that fails with 1053.
func TestWindowsServiceInstallRunner(t *testing.T) {
	t.Run("the umbrella finds the runner next to it", func(t *testing.T) {
		f := newWindowsFixture(t)
		f.layout()
		dir := t.TempDir()
		umbrella := filepath.Join(dir, "piperoom.exe")
		f.writeFileAt(umbrella, []byte("MZ umbrella\n"))
		f.writeFileAt(filepath.Join(dir, RunnerExe), []byte("MZ runner\n"))
		f.self = umbrella
		if _, err := f.run("service_install", ServiceParams{}); err != nil {
			t.Fatalf("service_install: %v", err)
		}
		if got := f.read(BinDir + "/" + RunnerExe); got != "MZ runner\n" {
			t.Errorf("bin copy %q, want the runner's bytes, not the umbrella's", got)
		}
		if calls := f.scm.createdCalls(); len(calls) != 1 || calls[0].exe != filepath.Join(f.dir, BinDir, RunnerExe) {
			t.Errorf("create calls %+v", calls)
		}
	})
	t.Run("a binary that cannot answer run-service is refused", func(t *testing.T) {
		f := newWindowsFixture(t)
		f.layout()
		umbrella := filepath.Join(t.TempDir(), "piperoom.exe")
		f.writeFileAt(umbrella, []byte("MZ umbrella\n"))
		f.self = umbrella
		_, err := f.run("service_install", ServiceParams{})
		if !errors.Is(err, ErrDetect) || !strings.Contains(err.Error(), "1053") {
			t.Fatalf("err %v, want ErrDetect naming Windows error 1053", err)
		}
		if len(f.scm.createdCalls()) != 0 || len(f.scm.openedNames()) != 0 {
			t.Error("the SCM was reached for a binary that cannot serve the service")
		}
		if _, serr := os.Stat(filepath.Join(f.dir, BinDir, RunnerExe)); !os.IsNotExist(serr) {
			t.Errorf("the exe copy was written: %v", serr)
		}
	})
}

// TestWindowsServiceInstallRestoresAFailedReplace: a replacement that cannot
// create the new service puts the previous definition back and re-creates the
// previous service, instead of leaving none.
func TestWindowsServiceInstallRestoresAFailedReplace(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatal(err)
	}
	f.scm.add("piperoom-connect", stateStopped)
	f.scm.createErrs = []error{errors.New("the service database is locked")}
	_, err := f.run("service_install", ServiceParams{Mode: ModeStandalone})
	if !errors.Is(err, ErrServiceManager) {
		t.Fatalf("second install: %v, want ErrServiceManager", err)
	}
	f.scm.mu.Lock()
	restored, ok := f.scm.services["piperoom-connect"]
	f.scm.mu.Unlock()
	if !ok {
		t.Fatal("the replaced service was not restored: the project has no service now")
	}
	if starts, _, _ := restored.counts(); starts != 0 {
		t.Errorf("restore started the service: %d starts", starts)
	}
	// The record that came back is the definition install replaced: the
	// distributed worker, not the standalone one the failed step wanted.
	rec := f.read(ServiceDir + "/piperoom-connect.txt")
	if !strings.Contains(rec, "connect-distributed.properties") || strings.Contains(rec, "connect-standalone.properties") {
		t.Errorf("record after the failed replace:\n%s", rec)
	}
	calls := f.scm.createdCalls()
	if len(calls) != 3 {
		t.Fatalf("%d Create calls, want 3 (install, failed replace, restore)", len(calls))
	}
	last := calls[len(calls)-1]
	if got, want := last.args, wantServiceArgs(f.dir, ModeDistributed); !equalStrings(got, want) {
		t.Errorf("restored args %q, want the previous definition's %q", got, want)
	}
	if last.cfg.Account != `NT SERVICE\piperoom-connect` || !last.cfg.ServiceSID {
		t.Errorf("restored config %+v, want the virtual account back too", last.cfg)
	}
}

// TestWindowsServiceCreateStopsOnCancel: the marked-for-deletion retries are
// bounded by the step's context, so a cancelled step stops calling the SCM.
func TestWindowsServiceCreateStopsOnCancel(t *testing.T) {
	f := newWindowsFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.scm.createErrs = []error{errServiceMarkedForDelete, errServiceMarkedForDelete, errServiceMarkedForDelete}
	m := f.drv.mgr.(*windowsManager)
	_, err := m.createService(ctx, f.scm, "piperoom-connect", `C:\p\bin\piperoom-setup.exe`, nil, serviceConfig{})
	if !errors.Is(err, ErrServiceManager) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want ErrServiceManager wrapping context.Canceled", err)
	}
	if n := len(f.scm.createdCalls()); n != 1 {
		t.Errorf("%d Create calls, want 1: the context ended the retries", n)
	}
}

// TestWindowsServiceInstallRefusals: no elevated prompt writes nothing, a
// failed Create removes what install wrote, and a name the SCM still holds is
// retried.
func TestWindowsServiceInstallRefusals(t *testing.T) {
	t.Run("no elevated prompt", func(t *testing.T) {
		f := newWindowsFixture(t)
		f.layout()
		f.scm.connectErr = errors.New("Access is denied.")
		_, err := f.run("service_install", ServiceParams{})
		if !errors.Is(err, ErrServiceManager) || !strings.Contains(err.Error(), "elevated prompt") {
			t.Fatalf("err %v, want ErrServiceManager with the elevation hint", err)
		}
		if n := len(f.scm.createdCalls()); n != 0 {
			t.Errorf("%d Create calls", n)
		}
		if _, serr := os.Stat(filepath.Join(f.dir, BinDir, RunnerExe)); !os.IsNotExist(serr) {
			t.Errorf("the exe copy was written without the SCM: %v", serr)
		}
	})
	t.Run("create fails then files are rolled back", func(t *testing.T) {
		f := newWindowsFixture(t)
		f.layout()
		f.scm.createErrs = []error{errors.New("the service database is locked")}
		_, err := f.run("service_install", ServiceParams{})
		if !errors.Is(err, ErrServiceManager) {
			t.Fatalf("err %v, want ErrServiceManager", err)
		}
		for _, name := range []string{BinDir + "/" + RunnerExe, ServiceDir + "/piperoom-connect.txt"} {
			if _, serr := os.Stat(filepath.Join(f.dir, filepath.FromSlash(name))); !os.IsNotExist(serr) {
				t.Errorf("%s was left behind: %v", name, serr)
			}
		}
	})
	t.Run("marked for deletion then retried", func(t *testing.T) {
		f := newWindowsFixture(t)
		f.layout()
		f.scm.createErrs = []error{errServiceMarkedForDelete, errServiceMarkedForDelete, nil}
		if _, err := f.run("service_install", ServiceParams{}); err != nil {
			t.Fatalf("install: %v", err)
		}
		if n := len(f.scm.createdCalls()); n != 3 {
			t.Errorf("%d Create attempts, want 3", n)
		}
	})
	t.Run("distribution missing", func(t *testing.T) {
		f := newWindowsFixture(t)
		f.write("connect-distributed.properties", "x=y\n", 0o600)
		if _, err := f.run("service_install", ServiceParams{}); !errors.Is(err, ErrDetect) {
			t.Fatalf("err %v, want ErrDetect", err)
		}
		if len(f.scm.openedNames()) != 0 || len(f.scm.createdCalls()) != 0 {
			t.Error("the SCM was reached without a distribution")
		}
	})
	t.Run("worker file missing", func(t *testing.T) {
		f := newWindowsFixture(t)
		f.write("kafka/libs/kafka-clients-4.0.0.jar", "jar\n", 0o644)
		if _, err := f.run("service_install", ServiceParams{Mode: ModeStandalone}); !errors.Is(err, ErrDetect) {
			t.Fatalf("err %v, want ErrDetect", err)
		}
	})
	t.Run("java is a shim", func(t *testing.T) {
		f := newWindowsFixture(t)
		f.layout()
		f.java = "/home/u/.asdf/shims/java"
		if _, err := f.run("service_install", ServiceParams{}); !errors.Is(err, ErrDetect) {
			t.Fatalf("err %v, want ErrDetect", err)
		}
	})
}

// TestWindowsServiceInstallPreviewNamesTheService: the preview's effect
// sentence names what the SCM starts. The unit wording reads
// s.Rendered.ExecStart, which a Windows definition does not have, and would
// have shown "... runs ." with nothing after it.
func TestWindowsServiceInstallPreviewNamesTheService(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	raw, err := json.Marshal(ServiceParams{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := (serviceInstallOp{d: f.drv}).Preview(context.Background(), raw, f.envForTest(t))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	exe := filepath.Join(f.dir, BinDir, RunnerExe)
	if !strings.Contains(p.Expected, "runs "+exe+" "+RunServiceSubcommand+" --project") {
		t.Errorf("preview %q, want the copied runner and run-service", p.Expected)
	}
	if strings.Contains(p.Expected, "runs .") || strings.HasSuffix(strings.TrimSpace(p.Expected), "runs") {
		t.Errorf("preview %q ends with an empty command", p.Expected)
	}
	if p.Target != ServiceDir+"/piperoom-connect.txt" {
		t.Errorf("preview target %q", p.Target)
	}
}

func TestWindowsServiceStatus(t *testing.T) {
	f := newWindowsFixture(t)
	res, err := f.run("service_status", UnitParams{})
	if err != nil {
		t.Fatalf("service_status: %v", err)
	}
	if res.Summary != "piperoom-connect is not installed" || !strings.Contains(string(res.Data), `"installed":false`) {
		t.Errorf("absent: %q %s", res.Summary, res.Data)
	}

	f.layout()
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatal(err)
	}
	svc := f.scm.add("piperoom-connect", stateRunning)
	svc.exitCode = 0
	res, err = f.run("service_status", UnitParams{})
	if err != nil {
		t.Fatalf("service_status: %v", err)
	}
	var data map[string]any
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["installed"] != true || data["running"] != true || data["state"] != "running" || data["this_project"] != true ||
		data["scope"] != "machine" {
		t.Errorf("data %v", data)
	}
	if res.Summary != "piperoom-connect: running" {
		t.Errorf("summary %q", res.Summary)
	}

	// A JVM that died: the service host returns the JVM's code as a
	// service-specific exit code, which is what the SCM stores (Win32ExitCode
	// ERROR_SERVICE_SPECIFIC_ERROR plus ServiceSpecificExitCode), so
	// service_status must read the second field.
	svc.mu.Lock()
	svc.state, svc.exitCode, svc.specificEC = stateStopped, win32ErrorServiceSpecific, 3
	svc.mu.Unlock()
	res, err = f.run("service_status", UnitParams{})
	if err != nil {
		t.Fatalf("service_status: %v", err)
	}
	if !strings.Contains(res.Summary, "stopped") || !strings.Contains(res.Summary, "last exit code 3") ||
		strings.Contains(res.Summary, "last exit code 1066") {
		t.Errorf("summary %q, want the service-specific code 3", res.Summary)
	}
	var died map[string]any
	if err := json.Unmarshal(res.Data, &died); err != nil {
		t.Fatal(err)
	}
	if died["exit_code"] != float64(3) {
		t.Errorf("exit_code %v, want 3", died["exit_code"])
	}

	// A Win32 code (a start the SCM timed out) is still reported as it comes.
	svc.mu.Lock()
	svc.exitCode, svc.specificEC = 1053, 0
	svc.mu.Unlock()
	res, err = f.run("service_status", UnitParams{})
	if err != nil {
		t.Fatalf("service_status: %v", err)
	}
	if !strings.Contains(res.Summary, "last exit code 1053") {
		t.Errorf("summary %q, want the Win32 code 1053", res.Summary)
	}

	// A name this project did not install: reported, never claimed.
	if err := removeDriverFile(f.envForTest(t), ServiceDir+"/piperoom-connect.txt"); err != nil {
		t.Fatal(err)
	}
	res, err = f.run("service_status", UnitParams{})
	if err != nil {
		t.Fatalf("service_status: %v", err)
	}
	if !strings.Contains(res.Summary, "no record of installing it") {
		t.Errorf("summary %q", res.Summary)
	}
}

func TestWindowsServiceStatusFailures(t *testing.T) {
	f := newWindowsFixture(t)
	f.scm.connectErr = errors.New("Access is denied.")
	if _, err := f.run("service_status", UnitParams{}); !errors.Is(err, ErrServiceManager) {
		t.Errorf("no elevated prompt: %v", err)
	}
	f.scm.connectErr = nil
	s := f.scm.add("piperoom-connect", stateRunning)
	s.queryErr = errors.New("the RPC server is unavailable")
	if _, err := f.run("service_status", UnitParams{}); !errors.Is(err, ErrServiceManager) {
		t.Errorf("query failure: %v", err)
	}
	if _, err := f.run("service_status", map[string]string{"unit": "sshd"}); !errors.Is(err, ErrParams) {
		t.Errorf("unknown field: %v", err)
	}
}

func TestWindowsServiceRestart(t *testing.T) {
	f := newWindowsFixture(t)
	if _, err := f.run("service_restart", UnitParams{}); !errors.Is(err, ErrDetect) {
		t.Fatalf("restart before install: %v", err)
	}
	if len(f.scm.openedNames()) != 0 {
		t.Fatal("a restart of a service this project did not install reached the SCM")
	}
	f.layout()
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatal(err)
	}

	running := f.scm.add("piperoom-connect", stateRunning)
	res, err := f.run("service_restart", UnitParams{})
	if err != nil {
		t.Fatalf("restart: %v (%s)", err, res.Summary)
	}
	starts, stops, _ := running.counts()
	if starts != 1 || stops != 1 {
		t.Errorf("running service: %d starts, %d stops; want 1/1", starts, stops)
	}
	if !strings.HasPrefix(res.Summary, "restarted piperoom-connect; now running") {
		t.Errorf("summary %q", res.Summary)
	}
	if res.Receipt.Kind != "" || res.Receipt.Target != "piperoom-connect" {
		t.Errorf("receipt %+v", res.Receipt)
	}

	stopped := f.scm.add("piperoom-connect", stateStopped)
	res, err = f.run("service_restart", UnitParams{})
	if err != nil {
		t.Fatalf("start: %v (%s)", err, res.Summary)
	}
	if starts, stops, _ := stopped.counts(); starts != 1 || stops != 0 {
		t.Errorf("stopped service: %d starts, %d stops; want 1/0", starts, stops)
	}
	if !strings.HasPrefix(res.Summary, "started piperoom-connect; now running") {
		t.Errorf("summary %q", res.Summary)
	}

	// The SCM has no service of that name any more.
	delete(f.scm.services, "piperoom-connect")
	if _, err := f.run("service_restart", UnitParams{}); !errors.Is(err, ErrDetect) || !strings.Contains(err.Error(), "service_install") {
		t.Errorf("missing service: %v", err)
	}

	failing := f.scm.add("piperoom-connect", stateStopped)
	failing.startErr = errors.New("the service did not respond in time")
	if _, err := f.run("service_restart", UnitParams{}); !errors.Is(err, ErrServiceManager) {
		t.Errorf("failed start: %v", err)
	}
}

// TestWindowsServiceRestartReportsADeadJVM: a JVM that dies at once is
// reported with its code at the next poll, not after the whole 30 s wait.
func TestWindowsServiceRestartReportsADeadJVM(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatal(err)
	}
	svc := f.scm.add("piperoom-connect", stateStopped)
	// Start holds the fake's lock while it calls onStart.
	svc.onStart = func() {
		svc.state, svc.exitCode, svc.specificEC = stateStopped, win32ErrorServiceSpecific, 7
	}
	start := time.Now()
	_, err := f.run("service_restart", UnitParams{})
	if !errors.Is(err, ErrServiceManager) || !strings.Contains(err.Error(), "exit code 7") {
		t.Fatalf("restart: %v, want ErrServiceManager naming the JVM's exit code 7", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the restart waited %s; a stopped service must end the wait at the next poll", elapsed)
	}
}

func TestWindowsServiceUninstall(t *testing.T) {
	f := newWindowsFixture(t)
	if _, err := f.run("service_uninstall", UnitParams{}); !errors.Is(err, ops.ErrUnknownOp) {
		t.Fatalf("service_uninstall should not be in the catalogue yet: %v", err)
	}
	f.layout()
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatal(err)
	}
	svc := f.scm.add("piperoom-connect", stateRunning)
	m := f.drv.mgr.(*windowsManager)
	env := f.envForTest(t)
	res, err := m.uninstall(context.Background(), env, "piperoom-connect")
	if err != nil {
		t.Fatalf("uninstall: %v (%s)", err, res.Summary)
	}
	if starts, stops, deletes := svc.counts(); starts != 0 || stops != 1 || deletes != 1 {
		t.Errorf("uninstall: %d starts, %d stops, %d deletes; want 0/1/1", starts, stops, deletes)
	}
	if _, serr := os.Stat(filepath.Join(f.dir, ServiceDir, "piperoom-connect.txt")); !os.IsNotExist(serr) {
		t.Errorf("the record survived the uninstall: %v", serr)
	}
	if _, serr := os.Stat(filepath.Join(f.dir, BinDir, RunnerExe)); serr != nil {
		t.Errorf("the bin copy should stay for a reinstall: %v", serr)
	}
	if _, err := m.uninstall(context.Background(), env, "piperoom-connect"); !errors.Is(err, ErrDetect) {
		t.Errorf("uninstall without a record: %v", err)
	}
}

// TestWindowsServiceMounts: what the SCM runs and what install records are out
// of file_write's and file_delete's reach.
func TestWindowsServiceMounts(t *testing.T) {
	f := newWindowsFixture(t)
	for _, p := range []string{BinDir + "/" + RunnerExe, ServiceDir + "/piperoom-connect.txt", DistDir + "/libs/x.jar"} {
		f.write(p, "keep\n", 0o644)
		if _, err := f.run("file_write", map[string]string{"path": p, "content": "x"}); !errors.Is(err, ops.ErrExecutedName) {
			t.Errorf("file_write %s: %v, want ErrExecutedName", p, err)
		}
		if _, err := f.run("file_delete", map[string]string{"path": p}); !errors.Is(err, ops.ErrExecutedName) {
			t.Errorf("file_delete %s: %v, want ErrExecutedName", p, err)
		}
		if got := f.read(p); got != "keep\n" {
			t.Errorf("%s changed: %q", p, got)
		}
	}
}

// TestWindowsServiceNames: the SCM's name has no unit suffix; the systemd one
// keeps it, on the same params.
func TestWindowsServiceNames(t *testing.T) {
	f := newWindowsFixture(t)
	got, err := f.drv.serviceName("")
	if err != nil || got != "piperoom-connect" {
		t.Errorf("windows serviceName(\"\") = %q, %v", got, err)
	}
	if got, err := f.drv.serviceName("it-a1"); err != nil || got != "piperoom-it-a1" {
		t.Errorf("windows serviceName(it-a1) = %q, %v", got, err)
	}
	unix := newFixture(t, Options{})
	if got, err := unix.drv.serviceName("it-a1"); err != nil || got != "piperoom-it-a1.service" {
		t.Errorf("linux serviceName(it-a1) = %q, %v", got, err)
	}
	for _, bad := range []string{"CONNECT", "a.b", "a@b", "-a", "", strings.Repeat("a", 41)} {
		if _, err := f.drv.serviceName(bad); err == nil && bad != "" {
			t.Errorf("serviceName(%q) accepted", bad)
		}
	}
}

// TestWindowsChildArgv: the JVM argv is the template's, with the detected
// paths; no arg is ever built from an op param. The heap and the log4j
// configuration match the distribution's connect-<mode>.sh, which the Linux unit
// runs; the log4j property follows the file the distribution ships.
func TestWindowsChildArgv(t *testing.T) {
	for _, mode := range []string{ModeDistributed, ModeStandalone} {
		t.Run(mode, func(t *testing.T) {
			argv, err := javaArgv("/srv/p", "/opt/jdk-21", mode)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := argv, wantJavaArgv("/srv/p", mode, ""); !equalStrings(got, want) {
				t.Errorf("argv %q, want %q", got, want)
			}
		})
	}
	// Kafka 4 ships config/connect-log4j2.yaml; Kafka 3 ships
	// config/connect-log4j.properties. Each is passed with its own property,
	// because without it no file appender is configured and connect.log is
	// never written.
	for _, c := range []struct{ file, want string }{
		{"kafka/config/connect-log4j2.yaml", "-Dlog4j2.configurationFile="},
		{"kafka/config/connect-log4j.properties", "-Dlog4j.configuration=file:"},
	} {
		dir := t.TempDir()
		p := filepath.Join(dir, filepath.FromSlash(c.file))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		argv, err := javaArgv(dir, "/opt/jdk-21", ModeDistributed)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, a := range argv {
			if strings.HasPrefix(a, c.want) && strings.HasSuffix(a, filepath.Base(c.file)) {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: argv %q lacks %s<path>", c.file, argv, c.want)
		}
	}
	// The classpath is one wildcard token the JVM expands; a jar list is never
	// globbed here.
	argv, _ := javaArgv("/srv/p", "/opt/jdk-21", ModeDistributed)
	found := false
	for _, a := range argv {
		if strings.HasSuffix(a, "libs/*") || strings.HasSuffix(a, `libs\*`) {
			found = true
		}
	}
	if !found {
		t.Errorf("no wildcard classpath in %q", argv)
	}
	if _, err := javaArgv("/srv/p", "/opt/jdk-21", "cluster"); !errors.Is(err, ErrParams) {
		t.Errorf("unknown mode: %v", err)
	}
	if _, err := javaArgv("/srv/my p", "/opt/jdk-21", ModeDistributed); !errors.Is(err, ErrDetect) {
		t.Errorf("a POSIX project path with a space (which the SCM escapes but a POSIX host cannot carry): %v", err)
	}
	if _, err := javaArgv("/srv/p", "opt/jdk", ModeDistributed); !errors.Is(err, ErrDetect) {
		t.Errorf("a relative JDK home: %v", err)
	}
}

// TestWindowsChildArgvFromProgramFiles: the JVM the service starts comes from
// the JDK java_version detected, which on Windows is usually under Program
// Files; the argv slice carries the space unescaped (CreateService and
// exec.Command escape it), so the path is served rather than refused.
func TestWindowsChildArgvFromProgramFiles(t *testing.T) {
	dir := `C:\Program Files\Piperoom\orders`
	javaHome := `C:\Program Files\Eclipse Adoptium\jdk-21`
	argv, err := javaArgv(dir, javaHome, ModeDistributed)
	if err != nil {
		t.Fatalf("javaArgv: %v", err)
	}
	if argv[0] != javaHome+`\bin\java.exe` {
		t.Errorf("java %q, want the Program Files JDK", argv[0])
	}
	found := false
	for _, a := range argv {
		if a == "-Dkafka.logs.dir="+dir+`\logs` {
			found = true
		}
	}
	if !found {
		t.Errorf("argv %q lacks the log directory under Program Files", argv)
	}
	// The install definition, the record and the args take the same shape.
	s := serviceSpec{
		Unit: "piperoom-connect", Mode: ModeDistributed, ProjectDir: dir,
		ExePath: dir + `\bin\piperoom-setup.exe`,
		Args:    []string{RunServiceSubcommand, "--project", dir, "--java", javaHome},
		Java:    argv, LogFile: dir + `\logs\` + ServiceLogName,
	}
	rec, err := renderServiceDefinition(s)
	if err != nil {
		t.Fatalf("renderServiceDefinition: %v", err)
	}
	for _, want := range []string{"project=" + dir + "\n", "exe=" + s.ExePath + "\n", "arg=--project\n", "arg=" + dir + "\n",
		"java-arg=" + javaHome + `\bin\java.exe` + "\n"} {
		if !strings.Contains(string(rec), want) {
			t.Errorf("record lacks %q:\n%s", want, rec)
		}
	}
}

// envForTest is an ops.Env over the fixture's project, for the manager methods
// that take one.
func (f *winFixture) envForTest(t *testing.T) ops.Env {
	t.Helper()
	c, err := ops.OpenConfiner(f.dir)
	if err != nil {
		t.Fatalf("OpenConfiner: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return ops.Env{ProjectDir: c.Dir(), Confine: c, MountDirs: []string{DistDir, PluginDir, BinDir, ServiceDir},
		LookPath: func(string) (string, error) { return f.java, nil }}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func argLines(key string, args []string) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		out = append(out, key+"="+a+"\n")
	}
	return out
}
