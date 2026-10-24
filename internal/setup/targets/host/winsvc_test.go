package host

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
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
	onStop     func()
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
		if s.onStop != nil {
			s.onStop()
		}
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

// aclCall is one recorded Harden call.
type aclCall struct {
	account string
	targets []aclTarget
}

// fakeACL is aclAPI: it records the DACLs install would set, and can fail.
type fakeACL struct {
	mu    sync.Mutex
	err   error
	calls []aclCall
}

func (a *fakeACL) Harden(account string, targets []aclTarget) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, aclCall{account: account, targets: append([]aclTarget(nil), targets...)})
	return a.err
}

func (a *fakeACL) recorded() []aclCall {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]aclCall(nil), a.calls...)
}

// fakeTrust is trustAPI: it records the paths checked and refuses the ones in
// untrusted.
type fakeTrust struct {
	mu        sync.Mutex
	untrusted map[string]bool
	checked   []string
}

func (f *fakeTrust) CheckTrusted(path string, tree bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checked = append(f.checked, path)
	if f.untrusted[path] {
		return ErrUntrusted
	}
	return nil
}

func (f *fakeTrust) refuse(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.untrusted == nil {
		f.untrusted = map[string]bool{}
	}
	f.untrusted[path] = true
}

func (f *fakeTrust) seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.checked...)
}

// winFixture is a project directory with the host driver on a Windows GOOS,
// the fake SCM, a temporary Program Files and fake DACL and trust bindings.
type winFixture struct {
	t      *testing.T
	dir    string
	pf     string
	reg    *ops.Registry
	drv    *Driver
	scm    *fakeSCM
	acl    *fakeACL
	trust  *fakeTrust
	self   string
	java   string
	noExec bool
}

func newWindowsFixture(t *testing.T) *winFixture {
	t.Helper()
	dir := t.TempDir()
	f := &winFixture{t: t, dir: dir, pf: t.TempDir(), scm: newFakeSCM(), acl: &fakeACL{}, trust: &fakeTrust{}, java: fakeJava}
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
	}, func() (string, error) { return f.self, nil }, func() (string, error) { return f.pf, nil }, f.acl, f.trust)
	mgr.poll = time.Millisecond
	drv.mgr = mgr
	return f
}

// distributedWorker and standaloneWorker are worker files the properties
// allow-list accepts.
const (
	distributedWorker = "bootstrap.servers=127.0.0.1:9092\ngroup.id=piperoom\nconfig.storage.topic=cfg\n" +
		"offset.storage.topic=off\nstatus.storage.topic=st\n" +
		"key.converter=org.apache.kafka.connect.json.JsonConverter\nvalue.converter=org.apache.kafka.connect.json.JsonConverter\n"
	standaloneWorker = "bootstrap.servers=127.0.0.1:9092\n" +
		"key.converter=org.apache.kafka.connect.json.JsonConverter\nvalue.converter=org.apache.kafka.connect.json.JsonConverter\n"
)

// layout writes the distribution (with its log4j configuration, as a Kafka
// 4 tarball has), a plugin and both worker files.
func (f *winFixture) layout() {
	f.write("kafka/libs/kafka-clients-4.0.0.jar", "jar\n", 0o644)
	f.write("kafka/config/connect-log4j2.yaml", "appenders: {}\n", 0o644)
	f.write("connect-plugins/debezium-connector-postgres/debezium-core.jar", "plugin\n", 0o644)
	f.write("connect-distributed.properties", distributedWorker, 0o600)
	f.write("connect-standalone.properties", standaloneWorker, 0o600)
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

// home is the fixture service's home under the temporary Program Files.
func (f *winFixture) home() string { return serviceHome(f.pf, "piperoom-connect") }

// releases lists the release directories in the home (staging leftovers
// included, so a test sees one that was not cleaned up).
func (f *winFixture) releases() []string {
	f.t.Helper()
	entries, err := os.ReadDir(filepath.Join(f.home(), releasesDir))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		f.t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// release is the one release in the home.
func (f *winFixture) release() string {
	f.t.Helper()
	ids := f.releases()
	if len(ids) != 1 {
		f.t.Fatalf("releases %q, want exactly one", ids)
	}
	return releasePath(f.home(), ids[0])
}

// fakeJavaHome is the JDK home the fixture's java resolves to (fakeJava is
// <home>/bin/java).
const fakeJavaHome = "/opt/jdk-21"

// wantServiceArgs is the command line service_install gives the SCM: the
// subcommand, the release and the service's own flags.
func wantServiceArgs(release, mode string) []string {
	return []string{
		RunServiceSubcommand, "--release", release,
		"--name", "connect", "--mode", mode, "--java", fakeJavaHome,
	}
}

// wantJavaArgv is the JVM argv the service host starts: the heap and log
// directory the distribution's connect-<mode>.sh sets, the log4j configuration
// from the release, and the wildcard classpath of the release.
func wantJavaArgv(release, home, mode, log4j string) []string {
	sep := "/"
	if isDrivePath(release) {
		sep = `\`
	}
	argv := []string{
		nativeJoin(fakeJavaHome, "bin", "java.exe"),
		"-Xms256M", "-Xmx2G",
		"-Dkafka.logs.dir=" + nativeJoin(home, homeLogDir),
	}
	if log4j != "" {
		argv = append(argv, log4j)
	}
	return append(argv,
		"-cp", nativeJoin(release, DistDir, "libs")+sep+"*",
		connectMain[mode],
		nativeJoin(release, releaseConfigDir, "connect-"+mode+".properties"),
	)
}

// wantLog4jArg is the log4j argument for the configuration the fixture's
// distribution carries, in the release.
func wantLog4jArg(release string) string {
	return "-Dlog4j2.configurationFile=" + nativeJoin(release, DistDir, "config", "connect-log4j2.yaml")
}

func TestWindowsServiceInstall(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	res, err := f.run("service_install", ServiceParams{})
	if err != nil {
		t.Fatalf("service_install: %v (%s)", err, res.Summary)
	}
	release := f.release()
	calls := f.scm.createdCalls()
	if len(calls) != 1 {
		t.Fatalf("%d Create calls, want 1", len(calls))
	}
	c := calls[0]
	if c.name != "piperoom-connect" {
		t.Errorf("service name %q, want piperoom-connect (no .service suffix: that names nothing in the SCM)", c.name)
	}
	wantExe := filepath.Join(release, BinDir, RunnerExe)
	if c.exe != wantExe {
		t.Errorf("exe %q, want the release's copy %q", c.exe, wantExe)
	}
	if strings.HasPrefix(c.exe, f.dir) {
		t.Errorf("exe %q is under the project: the service must never run from it", c.exe)
	}
	if got, want := c.args, wantServiceArgs(release, ModeDistributed); !equalStrings(got, want) {
		t.Errorf("args %q, want %q", got, want)
	}
	if !c.cfg.Automatic || c.cfg.DisplayName != "Piperoom Kafka Connect worker (distributed)" {
		t.Errorf("config %+v", c.cfg)
	}
	// The service runs as its own virtual account, never LocalSystem, with
	// one privilege.
	if c.cfg.Account != `NT SERVICE\piperoom-connect` || !c.cfg.ServiceSID || !equalStrings(c.cfg.Privileges, serviceRequiredPrivileges) {
		t.Errorf("config %+v, want NT SERVICE\\piperoom-connect, its SID and the reduced privilege set", c.cfg)
	}
	// The release holds the runner, the distribution, the plugins and the
	// worker file, byte for byte what the project had (the worker validated
	// and re-rendered).
	for rel, want := range map[string]string{
		BinDir + "/" + RunnerExe:                                        string(selfBytes),
		"kafka/libs/kafka-clients-4.0.0.jar":                            "jar\n",
		"kafka/config/connect-log4j2.yaml":                              "appenders: {}\n",
		"connect-plugins/debezium-connector-postgres/debezium-core.jar": "plugin\n",
	} {
		b, err := os.ReadFile(filepath.Join(release, filepath.FromSlash(rel)))
		if err != nil || string(b) != want {
			t.Errorf("release %s = %q (%v), want %q", rel, b, err, want)
		}
	}
	worker, err := os.ReadFile(filepath.Join(release, releaseConfigDir, "connect-distributed.properties"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "plugin.path=" + projectValue(release, PluginDir) + "\n"; !strings.Contains(string(worker), want) {
		t.Errorf("staged worker lacks %q (plugin.path must point into the release):\n%s", want, worker)
	}
	if strings.Contains(string(worker), strings.ReplaceAll(f.dir, "\\", "/")) {
		t.Errorf("staged worker names the project:\n%s", worker)
	}
	// Both records hold the definition an approver saw; the trusted one is
	// in the home.
	rec := f.read(ServiceDir + "/piperoom-connect.txt")
	homeRec, ok := readHomeRecord(f.home())
	if !ok || string(homeRec) != rec {
		t.Errorf("home record %q (%v), want the project marker's text", homeRec, ok)
	}
	for _, want := range append([]string{
		"service=piperoom-connect\n", `account=NT SERVICE\piperoom-connect` + "\n", "start=automatic\n",
		"exe=" + wantExe + "\n", "project=" + f.dir + "\n", "home=" + f.home() + "\n", "release=" + release + "\n",
		"log=" + filepath.Join(f.home(), homeLogDir, ServiceLogName) + "\n",
	}, argLines("arg", wantServiceArgs(release, ModeDistributed))...) {
		if !strings.Contains(rec, want) {
			t.Errorf("record lacks %q:\n%s", want, rec)
		}
	}
	for _, want := range argLines("java-arg", wantJavaArgv(release, f.home(), ModeDistributed, wantLog4jArg(release))) {
		if !strings.Contains(rec, want) {
			t.Errorf("record lacks %q:\n%s", want, rec)
		}
	}
	// The home is hardened for the service's account, the project never.
	acls := f.acl.recorded()
	if len(acls) != 1 || acls[0].account != `NT SERVICE\piperoom-connect` || !equalTargets(acls[0].targets, homeACLTargets(f.home())) {
		t.Errorf("Harden calls %+v, want the home's targets only", acls)
	}
	for _, c := range acls {
		for _, target := range c.targets {
			if strings.HasPrefix(target.Path, f.dir) {
				t.Errorf("the project path %s was re-permissioned", target.Path)
			}
		}
	}
	// The JDK is checked before anything, the staged release after the
	// DACLs.
	if seen := f.trust.seen(); len(seen) < 2 || seen[0] != fakeJavaHome || seen[len(seen)-1] != release {
		t.Errorf("trust checks %q, want the JDK first and the release last", seen)
	}
	for _, want := range []string{"installed piperoom-connect (distributed)", `account=NT SERVICE\piperoom-connect`, "next service_install"} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary %q lacks %q", res.Summary, want)
		}
	}
	if res.Receipt.Kind != "file" || res.Receipt.Target != ServiceDir+"/piperoom-connect.txt" {
		t.Errorf("receipt %+v", res.Receipt)
	}
	if f.noExec {
		t.Error("service_install ran a command through ops.Env.Exec")
	}
}

// TestWindowsServiceInstallStandalone: a standalone worker's offsets live in
// the home's data directory, the one place besides logs the service writes.
func TestWindowsServiceInstallStandalone(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	if _, err := f.run("service_install", ServiceParams{Mode: ModeStandalone}); err != nil {
		t.Fatal(err)
	}
	worker, err := os.ReadFile(filepath.Join(f.release(), releaseConfigDir, "connect-standalone.properties"))
	if err != nil {
		t.Fatal(err)
	}
	want := "offset.storage.file.filename=" + projectValue(f.home(), homeDataDir, OffsetsFile) + "\n"
	if !strings.Contains(string(worker), want) {
		t.Errorf("staged worker lacks %q:\n%s", want, worker)
	}
	if fi, err := os.Stat(filepath.Join(f.home(), homeDataDir)); err != nil || !fi.IsDir() {
		t.Errorf("the data directory: %v", err)
	}
}

// TestWindowsServiceInstallRefusesATamperedWorker: the worker file under the
// project is the person's to edit, so install validates it again; a key the
// allow-list does not have (config.providers can read any file the service
// can) or a plugin path outside the project's plugin directory never reaches
// the release.
func TestWindowsServiceInstallRefusesATamperedWorker(t *testing.T) {
	for name, extra := range map[string]string{
		"config provider": "config.providers=file\n",
		"plugin path":     "plugin.path=C:/Users/u/evil\n",
	} {
		t.Run(name, func(t *testing.T) {
			f := newWindowsFixture(t)
			f.layout()
			f.write("connect-distributed.properties", distributedWorker+extra, 0o600)
			if _, err := f.run("service_install", ServiceParams{}); !errors.Is(err, ErrProperties) {
				t.Fatalf("err %v, want ErrProperties", err)
			}
			if len(f.scm.openedNames()) != 0 || len(f.releases()) != 0 {
				t.Error("a tampered worker file reached the SCM or the home")
			}
		})
	}
}

// TestWindowsServiceInstallRefusesAnUntrustedJDK: the JDK is not staged, so
// one the person's account can change is refused before the SCM or the home is
// touched — at preview already, so it is never offered for approval.
func TestWindowsServiceInstallRefusesAnUntrustedJDK(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	f.trust.refuse(fakeJavaHome)
	raw, _ := json.Marshal(ServiceParams{})
	if _, err := (serviceInstallOp{d: f.drv}).Preview(context.Background(), raw, f.envForTest(t)); !errors.Is(err, ErrUntrusted) {
		t.Errorf("preview: %v, want ErrUntrusted", err)
	}
	_, err := f.run("service_install", ServiceParams{})
	if !errors.Is(err, ErrUntrusted) || !strings.Contains(err.Error(), "Program Files") {
		t.Fatalf("install: %v, want ErrUntrusted pointing at a Program Files JDK", err)
	}
	if len(f.scm.openedNames()) != 0 || len(f.releases()) != 0 {
		t.Error("an untrusted JDK reached the SCM or the home")
	}
}

// TestWindowsServiceInstallRefusesAnUntrustedRelease: a home whose release
// another account could change (a Program Files with a loosened DACL) is found
// after the DACLs are set, and the service is removed rather than recorded.
func TestWindowsServiceInstallRefusesAnUntrustedRelease(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	raw, _ := json.Marshal(ServiceParams{})
	s, mgr, err := (serviceInstallOp{d: f.drv}).prepare(raw, f.envForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	f.trust.refuse(s.Release)
	_, err = mgr.install(context.Background(), f.envForTest(t), s)
	if !errors.Is(err, ErrUntrusted) {
		t.Fatalf("install: %v, want ErrUntrusted", err)
	}
	f.scm.mu.Lock()
	_, left := f.scm.services["piperoom-connect"]
	f.scm.mu.Unlock()
	if left {
		t.Error("a service with an untrusted release was left installed")
	}
	if len(f.releases()) != 0 {
		t.Errorf("releases %q were left behind", f.releases())
	}
	if _, ok := readHomeRecord(f.home()); ok {
		t.Error("the untrusted service was recorded")
	}
}

// TestWindowsServiceInstallRefusesLinks: a link in what a release copies is
// refused, never followed.
func TestWindowsServiceInstallRefusesLinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		return // creating a symlink needs a privilege a CI runner may not hold
	}
	f := newWindowsFixture(t)
	f.layout()
	outside := filepath.Join(t.TempDir(), "evil.jar")
	f.writeFileAt(outside, []byte("evil"))
	if err := os.Symlink(outside, filepath.Join(f.dir, PluginDir, "evil.jar")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.run("service_install", ServiceParams{}); !errors.Is(err, ErrDetect) || !strings.Contains(err.Error(), "link") {
		t.Fatalf("err %v, want the link refusal", err)
	}
	if len(f.scm.openedNames()) != 0 || len(f.releases()) != 0 {
		t.Error("a link reached the SCM or the home")
	}
}

// TestReleaseStageRefusesAChangedFile: a file whose bytes change between the
// plan (what the preview named) and the copy is refused, and the half-copied
// release is removed.
func TestReleaseStageRefusesAChangedFile(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	env := f.envForTest(t)
	plan, err := planRelease(env, f.self, ModeDistributed, protocol.ClassLocal, f.home())
	if err != nil {
		t.Fatal(err)
	}
	f.write("kafka/libs/kafka-clients-4.0.0.jar", "swapped\n", 0o644)
	if _, err := plan.stage(env, f.home()); !errors.Is(err, ErrDetect) || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("stage: %v, want the changed-file refusal", err)
	}
	if ids := f.releases(); len(ids) != 0 {
		t.Errorf("releases %q left behind", ids)
	}
}

// TestReleaseIDIsTheContent: the same content makes the same release id, and
// any changed file a different one.
func TestReleaseIDIsTheContent(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	env := f.envForTest(t)
	plan := func() releasePlan {
		p, err := planRelease(env, f.self, ModeDistributed, protocol.ClassLocal, f.home())
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	a, b := plan(), plan()
	if a.id != b.id || !releaseIDRE.MatchString(a.id) || !strings.HasPrefix(a.digest, a.id) {
		t.Fatalf("ids %q and %q, want one 16-hex id", a.id, b.id)
	}
	f.write("connect-plugins/debezium-connector-postgres/extra.jar", "x\n", 0o644)
	if c := plan(); c.id == a.id {
		t.Error("a new plugin jar did not change the release id")
	}
}

// TestWindowsServiceInstallReplaces: a second install is this project's own
// service, so it stops, deletes and recreates it on the new release and
// removes the old one.
func TestWindowsServiceInstallReplaces(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatal(err)
	}
	first := f.release()
	old := f.scm.add("piperoom-connect", stateRunning)
	f.writeFileAt(f.self, []byte("MZ piperoom-setup v2\n"))
	res, err := f.run("service_install", ServiceParams{})
	if err != nil {
		t.Fatalf("second install: %v (%s)", err, res.Summary)
	}
	if starts, stops, deletes := old.counts(); starts != 0 || stops != 1 || deletes != 1 {
		t.Errorf("old service: %d starts, %d stops, %d deletes; want 0/1/1", starts, stops, deletes)
	}
	second := f.release()
	if second == first {
		t.Error("a new runner binary kept the release id")
	}
	if b, _ := os.ReadFile(filepath.Join(second, BinDir, RunnerExe)); string(b) != "MZ piperoom-setup v2\n" {
		t.Errorf("the new release's runner is %q", b)
	}
	f.scm.mu.Lock()
	replacement := f.scm.services["piperoom-connect"]
	f.scm.mu.Unlock()
	if starts, _, _ := replacement.counts(); starts != 1 {
		t.Errorf("the replacement was started %d times, want 1: it was running before", starts)
	}
	for _, want := range []string{"replaced piperoom-connect", "the running worker was stopped first", "started again"} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary %q lacks %q", res.Summary, want)
		}
	}
	// The same content again stages the same id, and the home keeps one.
	f.scm.add("piperoom-connect", stateStopped)
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatal(err)
	}
	if got := f.release(); got != second {
		t.Errorf("unchanged content staged %s, want %s again", got, second)
	}
}

// TestWindowsServiceInstallRefusesAForeignName: a machine-wide service name
// another project (or a person) installed is never stopped or deleted.
func TestWindowsServiceInstallRefusesAForeignName(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	foreign := f.scm.add("piperoom-connect", stateRunning)
	// A marker under the project proves nothing: the record that counts is
	// the home's.
	f.write(ServiceDir+"/piperoom-connect.txt", "service=piperoom-connect\n", 0o600)
	_, err := f.run("service_install", ServiceParams{})
	if !errors.Is(err, ErrDetect) || !strings.Contains(err.Error(), "did not install it") {
		t.Fatalf("install over a foreign service: %v", err)
	}
	if starts, stops, deletes := foreign.counts(); starts+stops+deletes != 0 {
		t.Errorf("the foreign service was touched: %d/%d/%d", starts, stops, deletes)
	}
	if n := len(f.scm.createdCalls()); n != 0 || len(f.releases()) != 0 {
		t.Errorf("%d Create calls, releases %q; want none", n, f.releases())
	}
}

// TestWindowsServiceInstallRunner: the SCM runs a copy of a binary that
// answers run-service, so install stages piperoom-setup — or the
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
		if b, _ := os.ReadFile(filepath.Join(f.release(), BinDir, RunnerExe)); string(b) != "MZ runner\n" {
			t.Errorf("release runner %q, want the runner's bytes, not the umbrella's", b)
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
		if len(f.scm.createdCalls()) != 0 || len(f.scm.openedNames()) != 0 || len(f.releases()) != 0 {
			t.Error("a binary that cannot serve the service reached the SCM or the home")
		}
	})
}

// TestWindowsServiceInstallRestoresAFailedReplace: a replacement that cannot
// create the new service re-creates the previous service from the home's
// record, on its own release, which the failed install leaves in place.
func TestWindowsServiceInstallRestoresAFailedReplace(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatal(err)
	}
	first := f.release()
	before, _ := readHomeRecord(f.home())
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
	if got := f.release(); got != first {
		t.Errorf("release after the failed replace %s, want the previous %s only", got, first)
	}
	if after, _ := readHomeRecord(f.home()); string(after) != string(before) {
		t.Errorf("home record changed by the failed replace:\n%s", after)
	}
	calls := f.scm.createdCalls()
	if len(calls) != 3 {
		t.Fatalf("%d Create calls, want 3 (install, failed replace, restore)", len(calls))
	}
	last := calls[len(calls)-1]
	if got, want := last.args, wantServiceArgs(first, ModeDistributed); !equalStrings(got, want) {
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
	_, err := m.createService(ctx, f.scm, "piperoom-connect", `C:\Program Files\Piperoom\piperoom-connect\releases\0123456789abcdef\bin\piperoom-setup.exe`, nil, serviceConfig{})
	if !errors.Is(err, ErrServiceManager) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want ErrServiceManager wrapping context.Canceled", err)
	}
	if n := len(f.scm.createdCalls()); n != 1 {
		t.Errorf("%d Create calls, want 1: the context ended the retries", n)
	}
}

// TestWindowsServiceInstallRefusals: no elevated prompt stages nothing, a
// failed Create removes what install staged, and a name the SCM still holds is
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
		if n := len(f.scm.createdCalls()); n != 0 || len(f.releases()) != 0 {
			t.Errorf("%d Create calls, releases %q", n, f.releases())
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
		if _, serr := os.Stat(filepath.Join(f.dir, ServiceDir, "piperoom-connect.txt")); !os.IsNotExist(serr) {
			t.Errorf("the marker was left behind: %v", serr)
		}
		if ids := f.releases(); len(ids) != 0 {
			t.Errorf("releases %q were left behind", ids)
		}
		if _, ok := readHomeRecord(f.home()); ok {
			t.Error("the home record was left behind")
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
		f.write("connect-distributed.properties", distributedWorker, 0o600)
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
// sentence names what the SCM starts: the release's runner, never a file
// under the project.
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
	if !strings.Contains(p.Expected, "runs "+f.home()) || !strings.Contains(p.Expected, RunServiceSubcommand+" --release") {
		t.Errorf("preview %q, want the release's runner and run-service --release", p.Expected)
	}
	if strings.Contains(p.Expected, "runs .") || strings.HasSuffix(strings.TrimSpace(p.Expected), "runs") {
		t.Errorf("preview %q ends with an empty command", p.Expected)
	}
	if p.Target != ServiceDir+"/piperoom-connect.txt" {
		t.Errorf("preview target %q", p.Target)
	}
	if len(f.releases()) != 0 || len(f.scm.openedNames()) != 0 {
		t.Error("the preview staged a release or reached the SCM")
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
		data["scope"] != "machine" || data["release"] != f.release() {
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
	if err := writeHomeRecord(f.home(), nil); err != nil {
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
	if !strings.HasPrefix(res.Summary, "restarted piperoom-connect; now running (from its staged release") {
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
	if !strings.HasPrefix(res.Summary, "started piperoom-connect; now running (from its staged release") {
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
		t.Errorf("the marker survived the uninstall: %v", serr)
	}
	if _, ok := readHomeRecord(f.home()); ok {
		t.Error("the home record survived the uninstall")
	}
	if ids := f.releases(); len(ids) != 0 {
		t.Errorf("releases %q survived the uninstall", ids)
	}
	if fi, serr := os.Stat(filepath.Join(f.home(), homeLogDir)); serr != nil || !fi.IsDir() {
		t.Errorf("the logs should stay for a reinstall: %v", serr)
	}
	if _, err := m.uninstall(context.Background(), env, "piperoom-connect"); !errors.Is(err, ErrDetect) {
		t.Errorf("uninstall without a record: %v", err)
	}
}

// TestWindowsServiceMounts: what install stages and marks is out of
// file_write's and file_delete's reach.
func TestWindowsServiceMounts(t *testing.T) {
	f := newWindowsFixture(t)
	for _, p := range []string{ServiceDir + "/piperoom-connect.txt", DistDir + "/libs/x.jar", PluginDir + "/p/x.jar"} {
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

// TestWindowsChildArgv: the JVM argv is the template's, with the release's
// paths; no arg is ever built from an op param. The heap and the log4j
// configuration match the distribution's connect-<mode>.sh, which the Linux unit
// runs; the log4j property follows the file the distribution ships.
func TestWindowsChildArgv(t *testing.T) {
	const release, home = "/srv/h/releases/0123456789abcdef", "/srv/h"
	for _, mode := range []string{ModeDistributed, ModeStandalone} {
		t.Run(mode, func(t *testing.T) {
			argv, err := javaArgv(release, home, "/opt/jdk-21", mode, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if got, want := argv, wantJavaArgv(release, home, mode, ""); !equalStrings(got, want) {
				t.Errorf("argv %q, want %q", got, want)
			}
		})
	}
	// Kafka 4 ships config/connect-log4j2.yaml; Kafka 3 ships
	// config/connect-log4j.properties. Each is passed with its own property and
	// the release's path, because without it no file appender is configured and
	// connect.log is never written.
	for _, c := range []struct{ file, want string }{
		{"connect-log4j2.yaml", "-Dlog4j2.configurationFile="},
		{"connect-log4j.properties", "-Dlog4j.configuration=file:"},
	} {
		probe := t.TempDir()
		if err := os.WriteFile(filepath.Join(probe, c.file), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		argv, err := javaArgv(release, home, "/opt/jdk-21", ModeDistributed, probe)
		if err != nil {
			t.Fatal(err)
		}
		want := c.want + nativeJoin(release, DistDir, "config", c.file)
		found := false
		for _, a := range argv {
			if a == want {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: argv %q lacks %s", c.file, argv, want)
		}
	}
	if _, err := javaArgv(release, home, "/opt/jdk-21", "cluster", t.TempDir()); !errors.Is(err, ErrParams) {
		t.Errorf("unknown mode: %v", err)
	}
	if _, err := javaArgv("/srv/my h/releases/0123456789abcdef", home, "/opt/jdk-21", ModeDistributed, t.TempDir()); !errors.Is(err, ErrDetect) {
		t.Errorf("a POSIX path with a space (which the SCM escapes but a POSIX host cannot carry): %v", err)
	}
	if _, err := javaArgv(release, home, "opt/jdk", ModeDistributed, t.TempDir()); !errors.Is(err, ErrDetect) {
		t.Errorf("a relative JDK home: %v", err)
	}
}

// TestWindowsChildArgvFromProgramFiles: the release and the JDK are under
// Program Files; the argv slice carries the space unescaped (CreateService and
// exec.Command escape it), so the paths are served rather than refused, and the
// record takes the same shape.
func TestWindowsChildArgvFromProgramFiles(t *testing.T) {
	home := serviceHome(`C:\Program Files`, "piperoom-connect")
	release := releasePath(home, "0123456789abcdef")
	javaHome := `C:\Program Files\Eclipse Adoptium\jdk-21`
	argv, err := javaArgv(release, home, javaHome, ModeDistributed, t.TempDir())
	if err != nil {
		t.Fatalf("javaArgv: %v", err)
	}
	if argv[0] != javaHome+`\bin\java.exe` || argv[3] != "-Dkafka.logs.dir="+home+`\logs` {
		t.Errorf("argv %q, want the Program Files JDK and the home's logs", argv)
	}
	s := serviceSpec{
		Unit: "piperoom-connect", Mode: ModeDistributed, ProjectDir: `C:\Users\First Last\orders`,
		Home: home, Release: release, Digest: "0123456789abcdef" + strings.Repeat("0", 48),
		ExePath: release + `\bin\piperoom-setup.exe`,
		Args:    []string{RunServiceSubcommand, "--release", release, "--name", "connect", "--mode", ModeDistributed, "--java", javaHome},
		Java:    argv, LogFile: home + `\logs\` + ServiceLogName,
	}
	rec, err := renderServiceDefinition(s)
	if err != nil {
		t.Fatalf("renderServiceDefinition: %v", err)
	}
	for _, want := range []string{"home=" + home + "\n", "release=" + release + "\n", "exe=" + s.ExePath + "\n",
		"arg=--release\n", "java-arg=" + javaHome + `\bin\java.exe` + "\n"} {
		if !strings.Contains(string(rec), want) {
			t.Errorf("record lacks %q:\n%s", want, rec)
		}
	}
}

// TestWindowsServiceRestoreRefusesAPlantedRecord: the home's record is
// admin-only, but it is still read as data, not as a command: a record that
// names another exe is refused instead of reaching CreateService.
func TestWindowsServiceRestoreRefusesAPlantedRecord(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatal(err)
	}
	valid, _ := readHomeRecord(f.home())
	planted := strings.Join(replaceRecordLine(splitLines(string(valid)), "exe=", `exe=C:\Windows\System32\cmd.exe`), "\n") + "\n"
	if err := writeHomeRecord(f.home(), []byte(planted)); err != nil {
		t.Fatal(err)
	}
	f.scm.add("piperoom-connect", stateStopped)
	_, err := f.run("service_install", ServiceParams{Mode: ModeStandalone})
	if !errors.Is(err, ErrDetect) || !strings.Contains(err.Error(), "did not install it") {
		t.Fatalf("install over a planted record: %v, want it treated as not this project's", err)
	}
	for _, c := range f.scm.createdCalls() {
		if strings.Contains(c.exe, "cmd.exe") {
			t.Errorf("CreateService was given exe %q", c.exe)
		}
	}
}

// TestParseRecord: only the exact text service_install writes for this project
// and service is read back, so a record cannot choose an exe, an argument, an
// account, a project, a home or a release.
func TestParseRecord(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatal(err)
	}
	env := f.envForTest(t)
	validBytes, _ := readHomeRecord(f.home())
	valid := string(validBytes)
	if spec, err := parseRecord(env, "piperoom-connect", f.home(), validBytes); err != nil {
		t.Fatalf("the record install wrote was refused: %v", err)
	} else if spec.Mode != ModeDistributed || spec.ExePath != filepath.Join(f.release(), BinDir, RunnerExe) {
		t.Errorf("parsed spec %+v", spec)
	}
	lines := splitLines(valid)
	other := releasePath(f.home(), "fedcba9876543210")
	cases := []struct {
		name string
		edit func([]string) []string
	}{
		{"an exe that is not the runner copy", func(l []string) []string {
			return replaceRecordLine(l, "exe=", `exe=C:\Windows\System32\cmd.exe`)
		}},
		{"an exe under the project", func(l []string) []string {
			return replaceRecordLine(l, "exe=", "exe="+filepath.Join(f.dir, BinDir, RunnerExe))
		}},
		{"an extra argument", func(l []string) []string { return insertRecordLine(l, "log=", "arg=/c") }},
		{"another project directory", func(l []string) []string {
			return replaceRecordLine(l, "project=", `project=C:\other`)
		}},
		{"another home", func(l []string) []string { return replaceRecordLine(l, "home=", `home=C:\Users\u\home`) }},
		{"a release outside the home", func(l []string) []string {
			return replaceRecordLine(l, "release=", "release="+filepath.Join(f.dir, "releases", "0123456789abcdef"))
		}},
		{"a release the arguments do not name", func(l []string) []string {
			return replaceRecordLine(l, "release=", "release="+other)
		}},
		{"a digest of another release", func(l []string) []string {
			return replaceRecordLine(l, "digest=", "digest="+strings.Repeat("f", 64))
		}},
		{"a different argument order", func(l []string) []string {
			return replaceRecordLine(l, "arg="+RunServiceSubcommand, "arg=run-service2")
		}},
		{"LocalSystem", func(l []string) []string { return replaceRecordLine(l, "account=", "account=LocalSystem") }},
		{"manual start", func(l []string) []string { return replaceRecordLine(l, "start=", "start=manual") }},
		{"an unknown key", func(l []string) []string { return insertRecordLine(l, "log=", "trust=me") }},
		{"no display name", func(l []string) []string { return dropRecordLine(l, "display=") }},
		{"another service name", func(l []string) []string {
			return replaceRecordLine(l, "service=", "service=piperoom-other")
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			record := strings.Join(c.edit(lines), "\n") + "\n"
			if _, err := parseRecord(env, "piperoom-connect", f.home(), []byte(record)); !errors.Is(err, ErrDetect) {
				t.Errorf("parseRecord accepted a planted record: %v", err)
			}
		})
	}
}

// TestWindowsServiceInstallACLFailure: a service whose home could not be
// hardened is not installed at all. The new service is removed and the previous
// one — which was running — comes back on its own release.
func TestWindowsServiceInstallACLFailure(t *testing.T) {
	f := newWindowsFixture(t)
	f.layout()
	if _, err := f.run("service_install", ServiceParams{}); err != nil {
		t.Fatal(err)
	}
	first := f.release()
	old := f.scm.add("piperoom-connect", stateRunning)
	f.acl.err = errors.New("Access is denied.")
	_, err := f.run("service_install", ServiceParams{Mode: ModeStandalone})
	if !errors.Is(err, ErrServiceManager) || !strings.Contains(err.Error(), "Access is denied") {
		t.Fatalf("install with a failing DACL: %v, want ErrServiceManager naming the failure", err)
	}
	if _, stops, _ := old.counts(); stops != 1 {
		t.Errorf("the running service was stopped %d times, want 1", stops)
	}
	f.scm.mu.Lock()
	restored := f.scm.services["piperoom-connect"]
	f.scm.mu.Unlock()
	if restored == nil {
		t.Fatal("the previous service was not restored")
	}
	if starts, _, _ := restored.counts(); starts != 1 {
		t.Errorf("the restored service was started %d times, want 1: it was running before", starts)
	}
	calls := f.scm.createdCalls()
	if len(calls) != 3 {
		t.Fatalf("%d Create calls, want 3 (install, the new definition, the restore): the unhardened service is deleted", len(calls))
	}
	if got, want := calls[len(calls)-1].args, wantServiceArgs(first, ModeDistributed); !equalStrings(got, want) {
		t.Errorf("the restored service's args %q, want the previous definition's %q", got, want)
	}
	if got := f.release(); got != first {
		t.Errorf("release %s after the failed upgrade, want the previous %s only", got, first)
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
	return ops.Env{ProjectDir: c.Dir(), Confine: c, MountDirs: []string{DistDir, PluginDir, ServiceDir},
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

// equalTargets compares two DACL target lists.
func equalTargets(a, b []aclTarget) bool {
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

// replaceRecordLine replaces every line that starts with prefix.
func replaceRecordLine(lines []string, prefix, replacement string) []string {
	out := append([]string(nil), lines...)
	for i, line := range out {
		if strings.HasPrefix(line, prefix) {
			out[i] = replacement
		}
	}
	return out
}

// insertRecordLine puts a line before the first one that starts with before.
func insertRecordLine(lines []string, before, line string) []string {
	out := make([]string, 0, len(lines)+1)
	for _, l := range lines {
		if strings.HasPrefix(l, before) {
			out = append(out, line)
		}
		out = append(out, l)
	}
	return out
}

// dropRecordLine removes the first line that starts with prefix.
func dropRecordLine(lines []string, prefix string) []string {
	for i, line := range lines {
		if strings.HasPrefix(line, prefix) {
			return append(append([]string(nil), lines[:i]...), lines[i+1:]...)
		}
	}
	return lines
}
