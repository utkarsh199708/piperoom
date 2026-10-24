package host

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The service host's unit tests (row U8b): the supervisor behind svc.Run and
// the run-service subcommand, with a fake child and a fake service host, on
// every OS.

// fakeChild is the supervised JVM: Wait blocks until the test ends it.
type fakeChild struct {
	mu         sync.Mutex
	code       int
	waitErr    error
	ignoreKill bool // Terminate only returns when the child exits or ctx ends
	exit       chan struct{}
	done       sync.Once
	terminated int
}

func newFakeChild(code int) *fakeChild {
	return &fakeChild{code: code, exit: make(chan struct{})}
}

func (c *fakeChild) exitNow() { c.done.Do(func() { close(c.exit) }) }

func (c *fakeChild) Wait() (int, error) {
	<-c.exit
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.code, c.waitErr
}

func (c *fakeChild) Terminate(ctx context.Context) error {
	c.mu.Lock()
	c.terminated++
	c.mu.Unlock()
	if c.ignoreKill {
		select {
		case <-c.exit:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.exitNow()
	return nil
}

func (c *fakeChild) terminations() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.terminated
}

// fakeStarter records the argv and returns the fake child.
type fakeStarter struct {
	mu    sync.Mutex
	argv  []string
	out   io.Writer
	child *fakeChild
	err   error
}

func (s *fakeStarter) start(argv []string, out io.Writer) (child, error) {
	s.mu.Lock()
	s.argv = append([]string(nil), argv...)
	s.out = out
	s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	return s.child, nil
}

func (s *fakeStarter) argvSeen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.argv...)
}

// statusRecorder is the sink the handler reports into. It also hands each
// status to a channel, so a test can wait for one instead of sleeping.
type statusRecorder struct {
	mu  sync.Mutex
	got []serviceStatus
	ch  chan serviceStatus
}

func newStatusRecorder(buffer int) *statusRecorder {
	return &statusRecorder{ch: make(chan serviceStatus, buffer)}
}

func (r *statusRecorder) report(st serviceStatus) {
	r.mu.Lock()
	r.got = append(r.got, st)
	r.mu.Unlock()
	r.ch <- st
}

func (r *statusRecorder) next(t *testing.T) serviceStatus {
	t.Helper()
	select {
	case st := <-r.ch:
		return st
	case <-time.After(5 * time.Second):
		t.Fatal("no status reported")
		return serviceStatus{}
	}
}

func (r *statusRecorder) all() []serviceStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]serviceStatus(nil), r.got...)
}

// supervisorFixture is a supervisor over a fake child and a recorder.
type supervisorFixture struct {
	sup     *serviceSupervisor
	starter *fakeStarter
	child   *fakeChild
	rec     *statusRecorder
	reqs    chan changeRequest
	result  chan serveResult
}

type serveResult struct {
	specific bool
	code     uint32
}

func newSupervisorFixture(t *testing.T, child *fakeChild) *supervisorFixture {
	t.Helper()
	f := &supervisorFixture{
		starter: &fakeStarter{child: child},
		child:   child,
		rec:     newStatusRecorder(8),
		reqs:    make(chan changeRequest),
		result:  make(chan serveResult, 1),
	}
	f.sup = &serviceSupervisor{
		name: "piperoom-connect", argv: []string{"/jdk/bin/java.exe", "-cp", "/p/kafka/libs/*", "Main", "/p/connect-distributed.properties"},
		start: f.starter.start, log: slog.New(slog.NewTextHandler(io.Discard, nil)), out: io.Discard,
		stopTimeout: 50 * time.Millisecond,
	}
	return f
}

func (f *supervisorFixture) serve() {
	go func() {
		specific, code := f.sup.serve(f.reqs, f.rec.report)
		f.result <- serveResult{specific: specific, code: code}
	}()
}

func (f *supervisorFixture) finish(t *testing.T) serveResult {
	t.Helper()
	select {
	case r := <-f.result:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return")
		return serveResult{}
	}
}

// TestServiceStop stops the JVM on Stop, reports the pending stop inside the
// wait hint, and returns a clean exit: svc.Run reports the final Stopped
// itself, so serve reports no Stopped of its own (a Stopped here would reach
// the SCM with NO_ERROR before the real one).
func TestServiceStop(t *testing.T) {
	f := newSupervisorFixture(t, newFakeChild(0))
	f.serve()
	if st := f.rec.next(t); st.State != stateRunning || st.Accepts&acceptStop == 0 || st.Accepts&acceptShutdown == 0 {
		t.Fatalf("first status %+v, want Running accepting Stop and Shutdown", st)
	}
	if got := f.starter.argvSeen(); len(got) != 5 || got[0] != "/jdk/bin/java.exe" {
		t.Errorf("child argv %q, want the supervisor's argv", got)
	}
	start := time.Now()
	f.reqs <- changeRequest{Cmd: controlStop}
	pending := f.rec.next(t)
	if pending.State != stateStopPending || pending.WaitHint < 30000 || pending.CheckPoint == 0 {
		t.Errorf("stop pending %+v, want a wait hint of at least 30 s and a checkpoint", pending)
	}
	if r := f.finish(t); r.specific || r.code != 0 {
		t.Errorf("serve returned (%v, %d), want (false, 0)", r.specific, r.code)
	}
	if elapsed := time.Since(start); elapsed >= time.Duration(pending.WaitHint)*time.Millisecond {
		t.Errorf("serve returned after %s, past the %d ms wait hint", elapsed, pending.WaitHint)
	}
	if got := f.rec.all(); len(got) != 2 {
		t.Errorf("reported statuses %+v, want exactly Running and stop pending", got)
	}
	if f.child.terminations() != 1 {
		t.Errorf("%d Terminate calls, want 1", f.child.terminations())
	}
}

// TestServiceShutdown is the same for a machine shutdown.
func TestServiceShutdown(t *testing.T) {
	f := newSupervisorFixture(t, newFakeChild(0))
	f.serve()
	f.rec.next(t)
	f.reqs <- changeRequest{Cmd: controlShutdown}
	if st := f.rec.next(t); st.State != stateStopPending {
		t.Fatalf("after Shutdown: %v, want stop pending", st.State)
	}
	if r := f.finish(t); r.code != 0 {
		t.Errorf("serve returned %d, want 0", r.code)
	}
	if got := f.rec.all(); len(got) != 2 {
		t.Errorf("reported statuses %+v, want exactly Running and stop pending", got)
	}
	if f.child.terminations() != 1 {
		t.Errorf("%d Terminate calls, want 1", f.child.terminations())
	}
}

// TestServiceInterrogate answers the SCM's status query with Running.
func TestServiceInterrogate(t *testing.T) {
	f := newSupervisorFixture(t, newFakeChild(0))
	f.serve()
	f.rec.next(t)
	f.reqs <- changeRequest{Cmd: controlInterrogate}
	if st := f.rec.next(t); st.State != stateRunning {
		t.Errorf("interrogate answer %v, want running", st.State)
	}
	f.reqs <- changeRequest{Cmd: controlStop}
	f.rec.next(t)
	f.finish(t)
}

// TestServiceChildExit: the JVM's exit code is reported as a service-specific
// exit code, with no Stopped of serve's own (svc.Run reports the final Stopped
// carrying the code).
func TestServiceChildExit(t *testing.T) {
	child := newFakeChild(3)
	f := newSupervisorFixture(t, child)
	f.serve()
	f.rec.next(t)
	child.exitNow()
	if r := f.finish(t); !r.specific || r.code != 3 {
		t.Errorf("serve returned (%v, %d), want (true, 3): svc.Run records it as ServiceSpecificExitCode", r.specific, r.code)
	}
	if got := f.rec.all(); len(got) != 1 || got[0].State != stateRunning {
		t.Errorf("reported statuses %+v, want only Running before the exit", got)
	}
	if f.child.terminations() != 0 {
		t.Errorf("%d Terminate calls, want 0: the child was gone already", f.child.terminations())
	}
}

// TestServiceStartFailure: a JVM that cannot start is a service-specific exit
// code 1, never a running service and never a clean stop.
func TestServiceStartFailure(t *testing.T) {
	f := newSupervisorFixture(t, newFakeChild(0))
	f.starter.err = errors.New("CreateProcess: the file was not found")
	f.serve()
	if r := f.finish(t); !r.specific || r.code != 1 {
		t.Errorf("serve returned (%v, %d), want (true, 1)", r.specific, r.code)
	}
	if got := f.rec.all(); len(got) != 0 {
		t.Errorf("reported statuses %+v, want none: the SCM gets the failure in the final Stopped", got)
	}
}

// TestServiceStopTimeout: a JVM that ignores the stop still ends the handler
// inside the wait hint, so the machine's shutdown is never blocked by a
// service stuck in "stop pending" (svc.Run then reports Stopped).
func TestServiceStopTimeout(t *testing.T) {
	child := newFakeChild(0)
	child.ignoreKill = true
	f := newSupervisorFixture(t, child)
	f.sup.stopTimeout = 20 * time.Millisecond
	f.serve()
	f.rec.next(t)
	start := time.Now()
	f.reqs <- changeRequest{Cmd: controlStop}
	pending := f.rec.next(t)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("serve returned after %s, want within the test's 20 ms stop timeout plus slack", elapsed)
	}
	if pending.WaitHint < 30000 {
		t.Errorf("wait hint %d, want at least the 30 s serviceStopWaitHint", pending.WaitHint)
	}
	if r := f.finish(t); r.specific || r.code != 0 {
		t.Errorf("serve returned (%v, %d), want (false, 0)", r.specific, r.code)
	}
	if child.terminations() != 1 {
		t.Errorf("%d Terminate calls, want 1", child.terminations())
	}
}

// TestServiceRequestsClosed: a closed request channel (the SCM is gone) stops
// the child and ends the service cleanly.
func TestServiceRequestsClosed(t *testing.T) {
	f := newSupervisorFixture(t, newFakeChild(0))
	f.serve()
	f.rec.next(t)
	close(f.reqs)
	if r := f.finish(t); r.code != 0 {
		t.Errorf("serve returned %d, want 0", r.code)
	}
	if f.child.terminations() != 1 {
		t.Errorf("%d Terminate calls, want 1", f.child.terminations())
	}
}

// fakeServiceHost is serviceHost: a Program Files directory, a trust check
// that fails for the paths in untrusted, and a Run that records the supervisor.
type fakeServiceHost struct {
	isService    bool
	isErr        error
	runErr       error
	programFiles string
	untrusted    map[string]bool
	checked      []string
	ranName      string
	sup          *serviceSupervisor
}

func (h *fakeServiceHost) IsService() (bool, error) { return h.isService, h.isErr }

func (h *fakeServiceHost) Run(name string, sup *serviceSupervisor) error {
	h.ranName, h.sup = name, sup
	return h.runErr
}

func (h *fakeServiceHost) ProgramFiles() (string, error) { return h.programFiles, nil }

func (h *fakeServiceHost) CheckTrusted(path string, tree bool) error {
	h.checked = append(h.checked, path)
	if h.untrusted[path] {
		return ErrUntrusted
	}
	return nil
}

// runServiceRelease lays out a release the subcommand accepts under a
// temporary Program Files: <pf>/Piperoom/piperoom-connect/releases/<id> with a
// distribution and a worker file. It returns the Program Files directory and
// the release.
func runServiceRelease(t *testing.T) (string, string) {
	t.Helper()
	pf := t.TempDir()
	release := releasePath(serviceHome(pf, "piperoom-connect"), "0123456789abcdef")
	for _, name := range []string{DistDir + "/libs/kafka-clients-4.0.0.jar", releaseConfigDir + "/connect-distributed.properties"} {
		p := filepath.Join(release, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return pf, release
}

// TestRunServiceRefusals: every bad input is refused before anything starts.
func TestRunServiceRefusals(t *testing.T) {
	pf, release := runServiceRelease(t)
	home := serviceHome(pf, "piperoom-connect")
	other := releasePath(serviceHome(pf, "piperoom-other"), "0123456789abcdef")
	cases := []struct {
		name string
		args []string
		want error
	}{
		{"no args", nil, ErrParams},
		{"no release", []string{"--java", fakeJavaHome}, ErrParams},
		{"no java", []string{"--release", release}, ErrParams},
		{"bad mode", []string{"--release", release, "--java", fakeJavaHome, "--mode", "cluster"}, ErrParams},
		{"bad name", []string{"--release", release, "--java", fakeJavaHome, "--name", "a.b"}, ErrParams},
		{"relative release", []string{"--release", "orders", "--java", fakeJavaHome}, ErrDetect},
		{"relative java", []string{"--release", release, "--java", "jdk"}, ErrDetect},
		{"extra argument", []string{"--release", release, "--java", fakeJavaHome, "extra"}, ErrParams},
		// The release must be one of this service's own, in its home.
		{"another service's release", []string{"--release", other, "--java", fakeJavaHome}, ErrParams},
		{"a release id that is not a digest", []string{"--release", releasePath(home, "latest"), "--java", fakeJavaHome}, ErrParams},
		{"a project directory", []string{"--release", t.TempDir(), "--java", fakeJavaHome}, ErrParams},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			host := &fakeServiceHost{isService: true, programFiles: pf}
			err := runService(c.args, io.Discard, host)
			if !errors.Is(err, c.want) {
				t.Fatalf("err %v, want %v", err, c.want)
			}
			if host.ranName != "" || host.sup != nil {
				t.Error("the service was run despite the refusal")
			}
		})
	}
}

// TestRunServiceRefusesAnUntrustedPath: the release and the JDK are checked
// at every start, so a DACL loosened after install stops the service instead
// of letting it run what another account changed; the refusal is logged in the
// home's service log, where the operator reads why the start failed.
func TestRunServiceRefusesAnUntrustedPath(t *testing.T) {
	for _, what := range []string{"release", "JDK"} {
		t.Run(what, func(t *testing.T) {
			pf, release := runServiceRelease(t)
			bad := release
			if what == "JDK" {
				bad = fakeJavaHome
			}
			host := &fakeServiceHost{isService: true, programFiles: pf, untrusted: map[string]bool{bad: true}}
			err := runService([]string{"--release", release, "--java", fakeJavaHome}, io.Discard, host)
			if !errors.Is(err, ErrUntrusted) {
				t.Fatalf("err %v, want ErrUntrusted", err)
			}
			if host.sup != nil {
				t.Error("the service ran from an untrusted path")
			}
			b, rerr := os.ReadFile(filepath.Join(serviceHome(pf, "piperoom-connect"), homeLogDir, ServiceLogName))
			if rerr != nil || !strings.Contains(string(b), "refusing to start") {
				t.Errorf("service log %q (%v), want the refusal", b, rerr)
			}
		})
	}
}

// TestRunServiceIsTheSCMsEntryPoint: started from a console the subcommand
// refuses, since there is no SCM channel to answer — and it refuses before it
// opens the service log.
func TestRunServiceIsTheSCMsEntryPoint(t *testing.T) {
	pf, release := runServiceRelease(t)
	host := &fakeServiceHost{isService: false, programFiles: pf}
	err := runService([]string{"--release", release, "--java", fakeJavaHome}, io.Discard, host)
	if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), "service control manager") {
		t.Fatalf("err %v, want the console refusal naming the SCM", err)
	}
	if _, serr := os.Stat(filepath.Join(serviceHome(pf, "piperoom-connect"), homeLogDir, ServiceLogName)); !os.IsNotExist(serr) {
		t.Errorf("a console run wrote the service log: %v", serr)
	}
	host = &fakeServiceHost{isErr: errors.New("IsWindowsService: Access is denied."), programFiles: pf}
	if err := runService([]string{"--release", release, "--java", fakeJavaHome}, io.Discard, host); err == nil {
		t.Fatal("an IsWindowsService failure was ignored")
	}
}

// TestRunServiceRuns: a service start checks the release and the JDK, builds
// the JVM argv from the release, logs the start in the home's logs and runs
// under the SCM with the service name the SCM knows.
func TestRunServiceRuns(t *testing.T) {
	pf, release := runServiceRelease(t)
	home := serviceHome(pf, "piperoom-connect")
	host := &fakeServiceHost{isService: true, programFiles: pf}
	if err := runService([]string{"--release", release, "--java", fakeJavaHome}, io.Discard, host); err != nil {
		t.Fatalf("runService: %v", err)
	}
	if host.ranName != "piperoom-connect" {
		t.Errorf("ran as %q, want piperoom-connect", host.ranName)
	}
	if !equalStrings(host.checked, []string{release, fakeJavaHome}) {
		t.Errorf("trust checks %q, want the release and the JDK", host.checked)
	}
	if host.sup == nil {
		t.Fatal("no supervisor reached the service host")
	}
	if got, want := host.sup.argv, wantJavaArgv(release, home, ModeDistributed, ""); !equalStrings(got, want) {
		t.Errorf("JVM argv %q, want %q", got, want)
	}
	b, err := os.ReadFile(filepath.Join(home, homeLogDir, ServiceLogName))
	if err != nil {
		t.Fatalf("service log: %v", err)
	}
	if !strings.Contains(string(b), "service host starting") || !strings.Contains(string(b), "argv=") {
		t.Errorf("service log %q, want the start line with the argv", b)
	}
	// A failed svc.Run is a failure the SCM has to see.
	host.runErr = errors.New("StartServiceCtrlDispatcher: the service did not respond")
	if err := runService([]string{"--release", release, "--java", fakeJavaHome}, io.Discard, host); !errors.Is(err, ErrServiceManager) {
		t.Errorf("svc.Run failure: %v, want ErrServiceManager", err)
	}
}

// TestRunServiceCommand: setupcmd.Run dispatches on the first argument only.
func TestRunServiceCommand(t *testing.T) {
	for _, args := range [][]string{nil, {}, {"--release", "x"}, {"run-services"}} {
		handled, err := RunServiceCommand(args, io.Discard)
		if handled || err != nil {
			t.Errorf("RunServiceCommand(%q) = %v, %v; want (false, nil)", args, handled, err)
		}
	}
	handled, err := RunServiceCommand([]string{RunServiceSubcommand}, io.Discard)
	if !handled || err == nil {
		t.Fatalf("RunServiceCommand(run-service) = %v, %v; want handled with the missing-release refusal", handled, err)
	}
	if !errors.Is(err, ErrParams) {
		t.Errorf("err %v, want ErrParams", err)
	}
}

// TestServiceLogIsBestEffort: a home that cannot be opened is not a reason to
// fail the service; the log is best effort.
func TestServiceLogIsBestEffort(t *testing.T) {
	w, closeFn := serviceLog(filepath.Join(t.TempDir(), "absent"))
	if w != io.Discard {
		t.Errorf("logger %T, want io.Discard for a home that does not exist", w)
	}
	closeFn()
}
