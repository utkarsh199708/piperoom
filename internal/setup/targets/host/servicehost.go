package host

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"
)

// The service-host role of row U8b. The service control manager cannot run
// java.exe: a process it starts must call into the SCM inside the start
// timeout, and java.exe never does. So the installed service is a copy of
// piperoom-setup (bin\piperoom-setup.exe in the release install staged under
// Program Files; servicehome.go) started with run-service; that process starts the Connect JVM as its child (file:
// child.go), reports Running, relays Stop and Shutdown to the child, and turns
// the child's exit into the service's service-specific exit code (svc.Run
// reports the final Stopped itself, with that code).
//
// Everything in this file is platform-independent: the SCM's types are mirrored
// in winsvc.go, and serviceHost is the one platform binding (svc.Run and
// svc.IsWindowsService, in the Windows-only files). The supervisor is therefore
// unit-tested on any OS with a fake child and a fake status sink.

// ServiceLogName is the file the service host writes its own lines and the
// JVM's console output to. A Windows service has no terminal, so a JVM that
// dies during startup would otherwise leave nothing to read (Kafka's log4j
// appenders still write their own files there, in the home's logs\).
const ServiceLogName = "piperoom-service.log"

// Timing of the service host. The wait hint is what the SCM is told a pending
// stop takes; the stop timeout is how long the handler waits for the JVM inside
// it. The stopped state is reported by svc.Run when the handler returns, even
// if the JVM outlived the hint: a service stuck in "stop pending" blocks the
// machine's shutdown, and the SCM kills the process at the hint anyway.
const (
	serviceStopWaitHint = 30 * time.Second
	defaultStopTimeout  = 25 * time.Second
)

// connectMain maps a worker mode to the class that starts the worker, the same
// pair the distribution's connect-*.sh scripts run. A .bat script is not an
// option: it needs cmd.exe, and this package never runs a shell.
var connectMain = map[string]string{
	ModeDistributed: "org.apache.kafka.connect.cli.ConnectDistributed",
	ModeStandalone:  "org.apache.kafka.connect.cli.ConnectStandalone",
}

// javaArgv builds the JVM command line of the service: the detected JDK,
// the release's kafka\libs as a wildcard classpath (the JVM expands it, so no
// jar list is globbed or joined here), the home's log directory and the
// release's worker file for the mode. It mirrors what the distribution's
// connect-<mode>.sh runs: the heap of KAFKA_HEAP_OPTS (-Xms256M -Xmx2G), and
// the log4j configuration without which connect.log is never written (file:
// connectLog4jArg). probe is the directory whose config files decide the log4j
// argument: the project's kafka\config while the release is being planned
// (it is staged from there), the release's own at the service's start. Every
// path is checked by CheckServicePath; the arguments are passed to exec.Command
// as a slice, never as a string.
func javaArgv(release, home, javaHome, mode, probe string) ([]string, error) {
	files, ok := modeFiles[mode]
	if !ok {
		return nil, fmt.Errorf("%w: mode %s is not distributed or standalone", ErrParams, q(mode))
	}
	libs := nativeJoin(release, DistDir, "libs")
	logs := nativeJoin(home, homeLogDir)
	worker := nativeJoin(release, releaseConfigDir, files[1])
	java := nativeJoin(javaHome, "bin", "java.exe")
	for _, p := range []string{release, home, javaHome, libs, logs, worker, java} {
		if err := CheckServicePath(p); err != nil {
			return nil, err
		}
	}
	log4j, err := connectLog4jArg(probe, nativeJoin(release, DistDir, "config"))
	if err != nil {
		return nil, err
	}
	// The classpath is one wildcard entry the JVM expands, in the shape of the
	// release path; a jar list is never globbed or joined here.
	sep := pathSeparator(release)
	argv := []string{
		java,
		"-Xms256M", "-Xmx2G",
		"-Dkafka.logs.dir=" + logs,
	}
	if log4j != "" {
		argv = append(argv, log4j)
	}
	return append(argv,
		"-cp", libs+sep+"*",
		connectMain[mode],
		worker,
	), nil
}

// connectLog4jArg is the JVM argument that points the worker at the
// distribution's logging configuration in config, or "" when probe holds none
// (log4j's default configuration then logs to the console, which the service
// host sends to logs\piperoom-service.log). Kafka 4 names the file
// connect-log4j2.yaml and selects it with -Dlog4j2.configurationFile; earlier
// distributions use connect-log4j.properties with -Dlog4j.configuration=file:.
// Without it no file appender is configured and connect.log is never written.
func connectLog4jArg(probe, config string) (string, error) {
	candidates := []struct{ file, property string }{
		{"connect-log4j2.yaml", "-Dlog4j2.configurationFile="},
		{"connect-log4j.properties", "-Dlog4j.configuration=file:"},
	}
	for _, c := range candidates {
		fi, err := os.Stat(nativeJoin(probe, c.file))
		if err != nil || !fi.Mode().IsRegular() {
			continue
		}
		p := nativeJoin(config, c.file)
		if err := CheckServicePath(p); err != nil {
			return "", err
		}
		return c.property + p, nil
	}
	return "", nil
}

// serviceHost is the platform's service control manager entry point: the
// Windows build binds Run to svc.Run and IsService to svc.IsWindowsService;
// every other build refuses (a service host exists only on Windows).
type serviceHost interface {
	// IsService reports whether this process was started by the SCM.
	IsService() (bool, error)
	// Run runs the process as service name.
	Run(name string, sup *serviceSupervisor) error
	// ProgramFiles is %ProgramFiles% from the known-folder API: the release
	// the SCM names must be under this service's home in it.
	ProgramFiles() (string, error)
	// CheckTrusted is trustAPI.CheckTrusted: the release and the JDK are
	// checked again at every start, so a DACL loosened after install stops
	// the service rather than letting it run what another account changed.
	CheckTrusted(path string, tree bool) error
}

// RunServiceCommand reports whether args name the run-service subcommand and,
// if so, runs it. setupcmd.Run dispatches here before it parses its own flags:
// the installed Windows service's command line is `<exe> run-service
// --release ...`, and the SCM starts it with no argument of its own.
func RunServiceCommand(args []string, stderr io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != RunServiceSubcommand {
		return false, nil
	}
	return true, RunService(args[1:], stderr)
}

// RunService is the service-host entry point: it validates the flags
// service_install wrote and runs the process as the service the SCM started.
func RunService(args []string, stderr io.Writer) error {
	return runService(args, stderr, platformServiceHost{})
}

// runService is RunService with the platform binding injected (tests).
func runService(args []string, stderr io.Writer, host serviceHost) error {
	fs := flag.NewFlagSet("piperoom-setup "+RunServiceSubcommand, flag.ContinueOnError)
	fs.SetOutput(stderr)
	release := fs.String("release", "", "release directory under the service's home (service_install writes it)")
	name := fs.String("name", DefaultService, "service name segment (the service is "+UnitPrefix+"<name>)")
	mode := fs.String("mode", ModeDistributed, "worker mode the service runs")
	javaHome := fs.String("java", "", "JDK home the JVM runs from (service_install writes it from the detected java)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("%w: unexpected arguments: %v", ErrParams, fs.Args())
	}
	p, err := ServiceParams{Name: *name, Mode: *mode}.normalize()
	if err != nil {
		return err
	}
	if *release == "" {
		return fmt.Errorf("%w: %s needs --release <directory>; service_install writes it", ErrParams, RunServiceSubcommand)
	}
	if err := CheckServicePath(*release); err != nil {
		return fmt.Errorf("--release: %w", err)
	}
	if *javaHome == "" {
		return fmt.Errorf("%w: %s needs --java <JDK home>; service_install writes it from the detected java", ErrParams, RunServiceSubcommand)
	}
	if err := CheckServicePath(*javaHome); err != nil {
		return fmt.Errorf("--java: %w", err)
	}
	unit := UnitPrefix + p.Name
	// IsService comes before anything is read or logged: a console run has
	// nothing to supervise.
	isService, err := host.IsService()
	if err != nil {
		return fmt.Errorf("host: asking the service control manager: %w", err)
	}
	if !isService {
		return fmt.Errorf("%w: %s is the installed service's own entry point; the service control manager starts it (service_install installs the service, service_restart starts it), so a console run has nothing to supervise", ErrUnsupported, RunServiceSubcommand)
	}
	// The release must be one of this service's own, under its home in
	// Program Files: the SCM's command line is admin-only, but the check is
	// cheap and keeps the service from running a release of another name.
	pf, err := host.ProgramFiles()
	if err != nil {
		return err
	}
	home := serviceHome(pf, unit)
	if !samePath(nativeDir(nativeDir(*release)), home) || nativeBase(nativeDir(*release)) != releasesDir ||
		!releaseIDRE.MatchString(nativeBase(*release)) {
		return fmt.Errorf("%w: --release %s is not a release under %s", ErrParams, q(*release), q(home))
	}
	out, closeLog := serviceLog(home)
	defer closeLog()
	log := slog.New(slog.NewTextHandler(out, nil))
	for _, c := range []struct {
		path, what string
	}{{*release, "release"}, {*javaHome, "JDK"}} {
		if err := host.CheckTrusted(c.path, true); err != nil {
			log.Error("host: refusing to start: the "+c.what+" can be changed by a non-administrator", "service", unit, "err", err)
			return fmt.Errorf("the %s at %s: %w", c.what, q(c.path), err)
		}
	}
	argv, err := javaArgv(*release, home, *javaHome, p.Mode, nativeJoin(*release, DistDir, "config"))
	if err != nil {
		return err
	}
	sup := &serviceSupervisor{
		name: unit, argv: argv, start: startChild, out: out,
		log: log, stopTimeout: defaultStopTimeout,
	}
	sup.log.Info("host: service host starting", "service", sup.name, "release", *release, "mode", p.Mode, "argv", argv)
	if err := host.Run(sup.name, sup); err != nil {
		return fmt.Errorf("%w: running %s under the service control manager: %w", ErrServiceManager, q(sup.name), err)
	}
	return nil
}

// serviceLog opens the service log in the home's logs directory, best effort:
// a service that cannot log still runs, and Kafka's own appenders write there
// too. os.Root keeps the write inside the home even if a component is a link.
func serviceLog(home string) (io.Writer, func()) {
	root, err := os.OpenRoot(home)
	if err != nil {
		return io.Discard, func() {}
	}
	defer root.Close()
	if err := root.MkdirAll(homeLogDir, 0o755); err != nil {
		return io.Discard, func() {}
	}
	f, err := root.OpenFile(homeLogDir+"/"+ServiceLogName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return io.Discard, func() {}
	}
	return f, func() { _ = f.Close() }
}

// child is the supervised Connect JVM: the service's whole job is to start it,
// report its state and stop it.
type child interface {
	// Wait blocks until the process exits and returns its exit code.
	Wait() (int, error)
	// Terminate ends the process and returns when it is gone, or when ctx is
	// done.
	Terminate(ctx context.Context) error
}

// childStarter starts the JVM with an argv slice.
type childStarter func(argv []string, out io.Writer) (child, error)

// serviceSupervisor is the service loop behind svc.Run.
type serviceSupervisor struct {
	name        string
	argv        []string
	start       childStarter
	log         *slog.Logger
	out         io.Writer
	stopTimeout time.Duration
}

// exitResult is what the wait goroutine reports.
type exitResult struct {
	code int
	err  error
}

// serve runs the service: start the child, report Running, then watch the SCM's
// requests and the child until one of them ends the service. It returns what
// svc.Handler.Execute returns: svcSpecificEC true and the JVM's exit code when
// the JVM exited (svc.Run records it as Win32ExitCode=ERROR_SERVICE_SPECIFIC_ERROR
// and ServiceSpecificExitCode=code, and service_status reads it back), false
// and 0 for a stop the SCM asked for.
//
// serve never reports Stopped itself. svc.Run reports the final Stopped with
// the returned code when Execute returns; a Stopped reported here would reach
// the SCM first, with NO_ERROR, and the real code would only follow in a second
// Stopped — by which time the SCM has already recorded a clean stop.
func (s *serviceSupervisor) serve(requests <-chan changeRequest, report func(serviceStatus)) (bool, uint32) {
	child, err := s.start(s.argv, s.out)
	if err != nil {
		s.log.Error("host: starting the Connect JVM failed", "service", s.name, "err", err)
		return true, 1
	}
	exited := make(chan exitResult, 1)
	go func() {
		code, err := child.Wait()
		exited <- exitResult{code: code, err: err}
	}()
	running := serviceStatus{State: stateRunning, Accepts: acceptStop | acceptShutdown}
	report(running)
	for {
		select {
		case res := <-exited:
			code := res.code
			if res.err != nil {
				s.log.Error("host: waiting for the Connect JVM failed", "service", s.name, "err", res.err)
				if code == 0 {
					code = 1
				}
			}
			s.log.Info("host: the Connect JVM exited", "service", s.name, "exit_code", code)
			return true, uint32(code)
		case req, ok := <-requests:
			if !ok {
				s.stop(child)
				return false, 0
			}
			switch req.Cmd {
			case controlInterrogate:
				report(running)
			case controlStop, controlShutdown:
				what := "stop"
				if req.Cmd == controlShutdown {
					what = "shutdown"
				}
				s.log.Info("host: the service control manager asked for "+what, "service", s.name)
				report(serviceStatus{State: stateStopPending, CheckPoint: 1, WaitHint: s.waitHintMS()})
				s.stop(child)
				// svc.Run reports Stopped when serve returns, even when the
				// JVM outlived the wait hint: the SCM kills the process at the
				// hint, and a service left in "stop pending" would block the
				// machine's shutdown.
				return false, 0
			}
			// Any other control is one this service does not accept; the SCM
			// only sends what the last status said it accepts.
		}
	}
}

// stop terminates the child within the wait hint.
func (s *serviceSupervisor) stop(c child) {
	timeout := s.stopTimeout
	if timeout <= 0 {
		timeout = defaultStopTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := c.Terminate(ctx); err != nil {
		s.log.Warn("host: the Connect JVM did not exit before the service's wait hint", "service", s.name, "err", err)
	}
}

// waitHintMS is what the SCM is told a pending stop takes: the fixed hint,
// never less than the time the handler waits for the JVM, in milliseconds (the
// SCM requires a wait hint on every pending state and rejects 0).
func (s *serviceSupervisor) waitHintMS() uint32 {
	d := serviceStopWaitHint
	if s.stopTimeout > d {
		d = s.stopTimeout
	}
	return uint32(d / time.Millisecond)
}
