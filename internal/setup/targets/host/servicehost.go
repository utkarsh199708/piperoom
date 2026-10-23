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
// piperoom-setup (bin/piperoom-setup.exe under the project) started with
// run-service; that process starts the Connect JVM as its child (file:
// child.go), reports Running, relays Stop and Shutdown to the child and
// reports Stopped, and turns the child's exit into the service's exit code.
//
// Everything in this file is platform-independent: the SCM's types are mirrored
// in winsvc.go, and serviceHost is the one platform binding (svc.Run and
// svc.IsWindowsService, in the Windows-only files). The supervisor is therefore
// unit-tested on any OS with a fake child and a fake status sink.

// ServiceLogName is the file the service host writes its own lines and the
// JVM's console output to. A Windows service has no terminal, so a JVM that
// dies during startup would otherwise leave nothing to read (Kafka's log4j
// appenders still write their own files under logs/).
const ServiceLogName = "piperoom-service.log"

// Timing of the service host. The wait hint is what the SCM is told a pending
// stop takes; the stop timeout is how long the handler waits for the JVM inside
// it. The stop is reported even if the JVM outlives it: a service stuck in
// "stop pending" blocks the machine's shutdown, and the SCM kills the process
// at the hint anyway.
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
// the distribution's libs/ as a wildcard classpath (the JVM expands it, so no
// jar list is globbed or joined here), the logs directory and the worker
// properties file the mode names. Every path is checked by CheckTemplatePath;
// the arguments are passed to exec.Command as a slice, never as a string.
func javaArgv(project, javaHome, mode string) ([]string, error) {
	files, ok := modeFiles[mode]
	if !ok {
		return nil, fmt.Errorf("%w: mode %s is not distributed or standalone", ErrParams, q(mode))
	}
	libs := nativeJoin(project, DistDir, "libs")
	logs := nativeJoin(project, LogDir)
	worker := nativeJoin(project, files[1])
	java := nativeJoin(javaHome, "bin", "java.exe")
	for _, p := range []string{project, javaHome, libs, logs, worker, java} {
		if err := CheckTemplatePath(p); err != nil {
			return nil, err
		}
	}
	// The classpath is one wildcard entry the JVM expands, in the shape of the
	// project path; a jar list is never globbed or joined here.
	sep := pathSeparator(project)
	return []string{
		java,
		"-Xmx256M", "-Xms256M",
		"-Dkafka.logs.dir=" + logs,
		"-cp", libs + sep + "*",
		connectMain[mode],
		worker,
	}, nil
}

// serviceHost is the platform's service control manager entry point: the
// Windows build binds Run to svc.Run and IsService to svc.IsWindowsService;
// every other build refuses (a service host exists only on Windows).
type serviceHost interface {
	// IsService reports whether this process was started by the SCM.
	IsService() (bool, error)
	// Run runs the process as service name.
	Run(name string, sup *serviceSupervisor) error
}

// RunServiceCommand reports whether args name the run-service subcommand and,
// if so, runs it. cmd/setup dispatches here before setupcmd.Run: the installed
// Windows service's command line is `<exe> run-service --project ...`, and the
// SCM starts it with no argument of its own.
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
	project := fs.String("project", "", "project directory (service_install writes it from the project)")
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
	if *project == "" {
		return fmt.Errorf("%w: %s needs --project <directory>; service_install writes it", ErrParams, RunServiceSubcommand)
	}
	if err := CheckTemplatePath(*project); err != nil {
		return fmt.Errorf("--project: %w", err)
	}
	if *javaHome == "" {
		return fmt.Errorf("%w: %s needs --java <JDK home>; service_install writes it from the detected java", ErrParams, RunServiceSubcommand)
	}
	if err := CheckTemplatePath(*javaHome); err != nil {
		return fmt.Errorf("--java: %w", err)
	}
	// The project must exist: a service that cannot find its worker file should
	// fail at start with that reason, not run a JVM that dies a second later.
	fi, err := os.Stat(*project)
	if err != nil {
		return fmt.Errorf("%w: the project directory %s: %w", ErrDetect, q(*project), err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("%w: %s is not a directory", ErrDetect, q(*project))
	}
	argv, err := javaArgv(*project, *javaHome, p.Mode)
	if err != nil {
		return err
	}
	out, closeLog := serviceLog(*project)
	defer closeLog()
	sup := &serviceSupervisor{
		name: UnitPrefix + p.Name, argv: argv, start: startChild, out: out,
		log: slog.New(slog.NewTextHandler(out, nil)), stopTimeout: defaultStopTimeout,
	}
	isService, err := host.IsService()
	if err != nil {
		return fmt.Errorf("host: asking the service control manager: %w", err)
	}
	if !isService {
		return fmt.Errorf("%w: %s is the installed service's own entry point; the service control manager starts it (service_install installs the service, service_restart starts it), so a console run has nothing to supervise", ErrUnsupported, RunServiceSubcommand)
	}
	sup.log.Info("host: service host starting", "service", sup.name, "project", *project, "mode", p.Mode, "argv", argv)
	if err := host.Run(sup.name, sup); err != nil {
		return fmt.Errorf("%w: running %s under the service control manager: %w", ErrServiceManager, q(sup.name), err)
	}
	return nil
}

// serviceLog opens the service log under the project, best effort: a service
// that cannot log still runs, and Kafka's own appenders write under logs/ too.
// os.Root keeps the write inside the project even if a component is a link.
func serviceLog(project string) (io.Writer, func()) {
	root, err := os.OpenRoot(project)
	if err != nil {
		return io.Discard, func() {}
	}
	defer root.Close()
	if err := root.MkdirAll(LogDir, 0o755); err != nil {
		return io.Discard, func() {}
	}
	f, err := root.OpenFile(LogDir+"/"+ServiceLogName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
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
// svc.Handler.Execute returns: svcSpecificEC false, and the exit code the SCM
// records as the service's own (the JVM's code when the JVM exited).
func (s *serviceSupervisor) serve(requests <-chan changeRequest, report func(serviceStatus)) (bool, uint32) {
	child, err := s.start(s.argv, s.out)
	if err != nil {
		s.log.Error("host: starting the Connect JVM failed", "service", s.name, "err", err)
		report(serviceStatus{State: stateStopped})
		return false, 1
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
			// The return value is the code the SCM records: a JVM that died
			// makes the service stopped with that code, never a clean stop.
			report(serviceStatus{State: stateStopped})
			return false, uint32(code)
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
				// Reported even when the JVM outlived the wait hint: the SCM
				// kills the process at the hint, and a service left in "stop
				// pending" would block the machine's shutdown.
				report(serviceStatus{State: stateStopped})
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
