package host

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
)

// The Windows service target (row U8b). The service control manager cannot run
// java.exe: a process the SCM starts must call into it (RegisterServiceCtrlHandler,
// SetServiceStatus) inside the start timeout, and java.exe does not. The
// installed service is therefore this binary — a copy at bin/piperoom-setup.exe
// under the project — started with the run-service subcommand; that process
// starts the Connect JVM as its child and answers the SCM itself (file:
// servicehost.go).
//
// service_install, service_status and service_restart reach the SCM through
// golang.org/x/sys/windows/svc/mgr. Every mgr call sits behind serviceAPI and
// serviceHandle below, which mirror the small part of that package this target
// uses; the manager, its path and argument rules, its state mapping and its
// refusals are platform-independent code tested with a fake, and the
// Windows-only files hold the thin adapter and the build tag.
//
// The service's binary path and arguments come from the template and the
// project directory only (specWindows): no op param names an exe or an
// argument. CreateService builds the command line itself with
// syscall.EscapeArg from the exe path and the argument slice, so this package
// never builds a BinaryPathName string by hand.
//
// The service runs as NT SERVICE\<service>, not LocalSystem: see the
// serviceAccountPrefix comment below for the rule and why the project may not
// be user-writable when the service is SYSTEM.

// The SCM's state and control values this package names, mirroring svc.State,
// svc.Accepted and svc.Cmd (the same numeric values the Windows API uses, which
// is why the adapter can convert with a cast). They are defined here so the
// manager and the service host compile and test on every OS.
const (
	stateStopped         serviceState = 1
	stateStartPending    serviceState = 2
	stateStopPending     serviceState = 3
	stateRunning         serviceState = 4
	stateContinuePending serviceState = 5
	statePausePending    serviceState = 6
	statePaused          serviceState = 7

	controlStop        serviceControl = 1
	controlInterrogate serviceControl = 4
	controlShutdown    serviceControl = 5

	acceptStop     serviceAccept = 1
	acceptShutdown serviceAccept = 4
)

// serviceAccountPrefix starts the virtual account a service of this target
// runs as: NT SERVICE\<service name>. It is not LocalSystem, and that is the
// point of the rule.
//
// The service's exe (bin/piperoom-setup.exe), its classpath (kafka/libs), its
// plugins and its worker file all sit under the project directory, which
// properties_write (T1) and the person's own account can change at any time. A
// machine service under LocalSystem there would let anyone who can write a
// project file run code as SYSTEM — the same hole this package refuses on
// Linux by never installing a system unit from a user-writable directory. The
// virtual account closes it: the SCM creates NT SERVICE\<service> and logs the
// service on as that identity, which has no password, no profile and no rights
// beyond what an administrator grants it, so replacing the exe gains nothing.
// mgr.Config exposes the account as ServiceStartName (go doc
// golang.org/x/sys/windows/svc/mgr.Config) and the virtual-account SID as
// SidType SERVICE_SID_TYPE_UNRESTRICTED.
//
// Consequence, which the definition records and the summary states: the
// project directory and the JDK must be readable by NT SERVICE\<service>. A
// directory under a user profile usually grants only that user, SYSTEM and
// Administrators, so a service installed from there starts, cannot read its
// worker file and reports the JVM's exit code. Row U8e documents the account
// and where to install for the operator.
const serviceAccountPrefix = `NT SERVICE\`

// serviceSIDTypeUnrestricted is windows.SERVICE_SID_TYPE_UNRESTRICTED: the
// service's virtual-account SID is written into its token (and its child
// processes' tokens) at logon, which is what makes NT SERVICE\<service> an
// identity the SCM accepts. It mirrors the Windows constant here, the way the
// state and control values above do, because this file compiles on every OS.
const serviceSIDTypeUnrestricted = 1

// serviceAccount is the account the service runs as.
func serviceAccount(service string) string { return serviceAccountPrefix + service }

// Timing of the SCM calls. The poll interval bounds how long a stop or start
// the manager asked for may take before the op reports it did not happen; the
// service host's own wait hint (servicehost.go) is the SCM's copy of it.
const (
	defaultPollInterval = 500 * time.Millisecond
	stateWaitTimeout    = 30 * time.Second
	// deleteRetries bounds the retries of CreateService after a service was
	// deleted: the SCM may still hold the name ("marked for deletion") until
	// every handle to it is closed.
	deleteRetries = 10
)

// Errors the SCM's answers map to, so callers branch with errors.Is on a
// sentinel rather than a Win32 code.
var (
	errServiceNotExist        = errors.New("host: the service does not exist")
	errServiceExists          = errors.New("host: the service already exists")
	errServiceMarkedForDelete = errors.New("host: the service is marked for deletion")
	errServiceNotActive       = errors.New("host: the service is not active")
)

// serviceState is one of the SCM's states.
type serviceState uint32

// String is the state's name in a summary or Data.
func (s serviceState) String() string {
	switch s {
	case stateStopped:
		return "stopped"
	case stateStartPending:
		return "start pending"
	case stateStopPending:
		return "stop pending"
	case stateRunning:
		return "running"
	case stateContinuePending:
		return "continue pending"
	case statePausePending:
		return "pause pending"
	case statePaused:
		return "paused"
	}
	return fmt.Sprintf("state %d", uint32(s))
}

// serviceAccept is the bit set of controls a service accepts (svc.Accepted).
type serviceAccept uint32

// serviceControl is one control request (svc.Cmd).
type serviceControl uint32

// serviceStatus mirrors svc.Status: what the SCM reports for one service and
// what a service reports back about itself.
type serviceStatus struct {
	State         serviceState
	Accepts       serviceAccept
	CheckPoint    uint32
	WaitHint      uint32 // milliseconds
	ProcessID     uint32
	Win32ExitCode uint32
	// ServiceSpecificExitCode is the code the handler returned when
	// Win32ExitCode is ERROR_SERVICE_SPECIFIC_ERROR, which is how the service
	// host reports the JVM's exit code.
	ServiceSpecificExitCode uint32
}

// win32ErrorServiceSpecific is windows.ERROR_SERVICE_SPECIFIC_ERROR: the
// Win32ExitCode svc.Run writes when the handler returns svcSpecificEC true,
// with the handler's code in ServiceSpecificExitCode.
const win32ErrorServiceSpecific = 1066

// exitCode is the code the service last exited with, from whichever field the
// SCM put it in: the service host's JVM code is service-specific, a start
// failure the SCM saw itself is a Win32 code.
func (st serviceStatus) exitCode() uint32 {
	if st.Win32ExitCode == win32ErrorServiceSpecific {
		return st.ServiceSpecificExitCode
	}
	return st.Win32ExitCode
}

// changeRequest mirrors svc.ChangeRequest: one control the SCM sent.
type changeRequest struct {
	Cmd           serviceControl
	CurrentStatus serviceStatus
}

// serviceConfig is the part of mgr.Config service_install sets: the display
// name and description come from the template, the start type from the target's
// semantics (the worker starts with the machine, like an enabled unit), and the
// account from the virtual-account rule above.
type serviceConfig struct {
	DisplayName string
	Description string
	Automatic   bool
	// Account is mgr.Config.ServiceStartName. It is never empty (never
	// LocalSystem): serviceAccount names the service's virtual account.
	Account string
	// ServiceSID asks for SERVICE_SID_TYPE_UNRESTRICTED, so the virtual
	// account's SID is in the service's token.
	ServiceSID bool
}

// serviceAPI is the service control manager behind the Windows manager and the
// service host: the mgr calls this target makes (Connect/Disconnect, CreateService,
// OpenService). The fake in the tests records every call.
type serviceAPI interface {
	// Close closes the connection to the SCM (mgr.Disconnect).
	Close() error
	// Create registers a service (mgr.CreateService). exe and args are passed
	// separately so the mgr package builds and escapes the command line.
	Create(name, exe string, args []string, cfg serviceConfig) (serviceHandle, error)
	// Open opens an existing service (mgr.OpenService).
	Open(name string) (serviceHandle, error)
}

// serviceHandle is one service the SCM knows: the mgr.Service methods this
// target calls.
type serviceHandle interface {
	// Start starts the service (Service.Start).
	Start() error
	// Query reports the service's state (Service.Query).
	Query() (serviceStatus, error)
	// Control sends a control request (Service.Control).
	Control(cmd serviceControl) (serviceStatus, error)
	// Delete marks the service for deletion (Service.Delete).
	Delete() error
	// Close releases the handle (Service.Close).
	Close() error
}

// windowsManager is the serviceManager of a Windows host.
type windowsManager struct {
	connect func() (serviceAPI, error)
	// self returns the running binary's path: the file install copies to
	// bin/piperoom-setup.exe, which is what the SCM runs.
	self func() (string, error)
	poll time.Duration
}

// newWindowsManager builds the manager over the platform's SCM binding. self is
// os.Executable in production; the tests pass a small file.
func newWindowsManager(connect func() (serviceAPI, error), self func() (string, error)) *windowsManager {
	return &windowsManager{connect: connect, self: self, poll: defaultPollInterval}
}

func (m *windowsManager) artifact(s serviceSpec) ([]byte, error) { return renderServiceDefinition(s) }

// open connects to the SCM. Every failure here is the "no elevated prompt"
// refusal in practice, so the error says so.
func (m *windowsManager) open() (serviceAPI, error) {
	api, err := m.connect()
	if err != nil {
		return nil, fmt.Errorf("%w: connecting to the service control manager (service_install and service_restart install and start a machine service, so they need an elevated prompt): %w", ErrServiceManager, err)
	}
	return api, nil
}

// status reports what the SCM says about name.
func (m *windowsManager) status(ctx context.Context, env ops.Env, name string) (ops.Result, error) {
	api, err := m.open()
	if err != nil {
		return ops.Result{}, err
	}
	defer api.Close()
	h, err := api.Open(name)
	if errors.Is(err, errServiceNotExist) {
		return ops.Result{
			Summary: name + " is not installed",
			Data: map[string]any{"unit": name, "service": name, "scope": "machine",
				"installed": false, "running": false, "this_project": false},
			Receipt: events.SetupReceipt{Kind: "", Target: name},
		}, nil
	}
	if err != nil {
		return ops.Result{}, fmt.Errorf("%w: opening %s: %w", ErrServiceManager, q(name), err)
	}
	defer h.Close()
	st, err := h.Query()
	if err != nil {
		return ops.Result{}, fmt.Errorf("%w: querying %s: %w", ErrServiceManager, q(name), err)
	}
	own := ownedByProject(env, name)
	code := st.exitCode()
	data := map[string]any{
		"unit": name, "service": name, "scope": "machine",
		"installed": true, "running": st.State == stateRunning,
		"state": st.State.String(), "accepts_stop": st.Accepts&acceptStop != 0,
		"process_id": st.ProcessID, "exit_code": code,
		"this_project": own,
	}
	summary := name + ": " + st.State.String()
	if st.State == stateStartPending || st.State == stateStopPending {
		summary += fmt.Sprintf(" (wait hint %s)", time.Duration(st.WaitHint)*time.Millisecond)
	}
	if code != 0 {
		summary += fmt.Sprintf(", last exit code %d", code)
	}
	if own {
		data["record"] = ServiceDir + "/" + name + ".txt"
	} else {
		// A service name is per machine, not per project: another project's
		// service_install may own it. Say so rather than report its worker as
		// this project's.
		summary += "; this project has no record of installing it"
	}
	return ops.Result{
		Summary: summary, Data: data,
		Receipt: events.SetupReceipt{Kind: "", Target: name},
	}, nil
}

// install registers the service, or replaces this project's earlier install of
// the same name. Anything named exe or argument comes from s, which the spec
// built from the template and the project directory.
//
// The order is the replace guarantee: the new exe copy and definition are
// written before the working service is stopped and deleted, and a failed
// create puts the previous definition back and re-creates the previous service,
// so a replacement that cannot finish leaves a service behind rather than none.
func (m *windowsManager) install(ctx context.Context, env ops.Env, s serviceSpec) (ops.Result, error) {
	def, err := m.artifact(s)
	if err != nil {
		return ops.Result{}, err
	}
	// The path rule first, so an unsafe record path writes nothing and calls
	// nothing.
	if _, err := env.Confine.Check(s.UnitFile); err != nil {
		return ops.Result{}, err
	}
	exeName := BinDir + "/" + RunnerExe
	if _, err := env.Confine.Check(exeName); err != nil {
		return ops.Result{}, err
	}
	// The SCM runs a copy of a binary that must answer run-service. The
	// umbrella piperoom.exe (`piperoom setup`) does not, so install refuses
	// before it writes anything (the service would fail with error 1053).
	runner, err := m.runnerExe()
	if err != nil {
		return ops.Result{}, err
	}
	api, err := m.open()
	if err != nil {
		return ops.Result{}, err
	}
	defer api.Close()

	previous, recordExisted := readDefinition(env, s.UnitFile)
	replaced := false
	h, err := api.Open(s.Unit)
	switch {
	case err == nil:
		// The name is machine-wide. Replacing a service this project did not
		// install would stop and delete someone else's worker.
		if !ownedByProject(env, s.Unit) {
			h.Close()
			return ops.Result{}, fmt.Errorf("%w: a service named %s is already installed and this project did not install it (no %s/%s.txt); pick another service name",
				ErrDetect, q(s.Unit), ServiceDir, s.Unit)
		}
		h.Close()
		replaced = true
	case !errors.Is(err, errServiceNotExist):
		return ops.Result{}, fmt.Errorf("%w: opening %s: %w", ErrServiceManager, q(s.Unit), err)
	}

	exeBytes, err := os.ReadFile(runner)
	if err != nil {
		return ops.Result{}, fmt.Errorf("%w: reading the piperoom-setup binary %s: %w", ErrDetect, q(runner), err)
	}
	exeExisted := requireFile(env, exeName, false) == nil
	cleanExe, exeChanged, err := writeDriverFile(env, exeName, exeBytes, 0o755, BinDir)
	if err != nil {
		return ops.Result{}, err
	}
	record, recordChanged, err := writeDriverFile(env, s.UnitFile, def, 0o600, ServiceDir)
	if err != nil {
		undoDriverFiles(env, exeName, exeExisted)
		return ops.Result{}, err
	}
	if replaced {
		if err := m.deleteExisting(ctx, api, s.Unit); err != nil {
			restoreDefinition(env, s.UnitFile, previous, recordExisted)
			undoDriverFiles(env, exeName, exeExisted)
			return ops.Result{}, err
		}
	}
	h, err = m.create(ctx, api, s)
	if err != nil {
		if replaced {
			// Put the service back: the person had a working worker before
			// this step, and the definition just read still names its exe and
			// arguments.
			if rerr := m.restore(ctx, api, s.Unit, previous); rerr != nil {
				err = fmt.Errorf("%w; restoring the previous service also failed: %v", err, rerr)
			}
		}
		restoreDefinition(env, s.UnitFile, previous, recordExisted)
		undoDriverFiles(env, exeName, exeExisted)
		return ops.Result{}, err
	}
	h.Close()

	account := serviceAccount(s.Unit)
	summary := fmt.Sprintf("installed %s (%s) as a Windows service: %s", s.Unit, s.Mode, cleanExe)
	if replaced {
		summary = fmt.Sprintf("replaced %s (%s): the previous install was stopped and removed first; now %s", s.Unit, s.Mode, cleanExe)
	}
	summary += "; start=automatic, account=" + account + " (the project directory and the JDK must be readable by it); service_restart starts it"
	return ops.Result{
		Summary: summary,
		Data: map[string]any{
			"unit": s.Unit, "service": s.Unit, "mode": s.Mode, "exe": cleanExe,
			"exe_changed": exeChanged, "args": s.Args, "java": s.Java,
			"record": record, "record_changed": recordChanged, "definition": string(def),
			"replaced": replaced, "start_type": "automatic", "account": account,
		},
		Receipt: events.SetupReceipt{Kind: "file", Target: record},
	}, nil
}

// create registers the service s describes.
func (m *windowsManager) create(ctx context.Context, api serviceAPI, s serviceSpec) (serviceHandle, error) {
	return m.createService(ctx, api, s.Unit, s.ExePath, s.Args, m.config(s))
}

// config is what the SCM is told about s: the template's display name,
// automatic start, and the service's own virtual account.
func (m *windowsManager) config(s serviceSpec) serviceConfig {
	return serviceConfig{
		DisplayName: serviceDisplayName(s),
		Description: "Piperoom Kafka Connect " + s.Mode + " worker for the project at " + s.ProjectDir,
		Automatic:   true,
		Account:     serviceAccount(s.Unit),
		ServiceSID:  true,
	}
}

// createService registers name with the SCM, retrying while the SCM still
// holds a name a delete marked for deletion (the retries are bounded and the
// wait is the caller's: a cancelled step stops retrying).
func (m *windowsManager) createService(ctx context.Context, api serviceAPI, name, exe string, args []string, cfg serviceConfig) (serviceHandle, error) {
	var last error
	for attempt := 0; attempt <= deleteRetries; attempt++ {
		h, err := api.Create(name, exe, args, cfg)
		if err == nil {
			return h, nil
		}
		last = err
		switch {
		case errors.Is(err, errServiceExists):
			// Another install created the name while this one was writing its
			// files; it will not go away by itself.
			return nil, fmt.Errorf("%w: creating %s: %w (another install created the name meanwhile; run service_status)",
				ErrServiceManager, q(name), err)
		case !errors.Is(err, errServiceMarkedForDelete):
			return nil, fmt.Errorf("%w: creating %s: %w", ErrServiceManager, q(name), err)
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: creating %s: %w", ErrServiceManager, q(name), ctx.Err())
		case <-time.After(m.poll):
		}
	}
	return nil, fmt.Errorf("%w: creating %s: %w", ErrServiceManager, q(name), last)
}

// runnerExe is the binary the SCM runs: the running piperoom-setup, or the
// piperoom-setup next to the running binary when this process is the umbrella
// (`piperoom setup`, U1-21). The SCM starts it with run-service, which only
// piperoom-setup answers; a copy of piperoom.exe would fail the start with
// error 1053 ("the service did not respond in time"), so it is refused before
// anything is written.
func (m *windowsManager) runnerExe() (string, error) {
	self, err := m.self()
	if err != nil {
		return "", fmt.Errorf("%w: locating the running piperoom-setup binary: %w", ErrDetect, err)
	}
	if isRunnerName(filepath.Base(self)) {
		return self, nil
	}
	sibling := filepath.Join(filepath.Dir(self), RunnerExe)
	if fi, serr := os.Stat(sibling); serr == nil && fi.Mode().IsRegular() {
		return sibling, nil
	}
	return "", fmt.Errorf("%w: the SCM runs a copy of this binary with %s, and %s cannot answer it (the service would fail with Windows error 1053, \"the service did not respond in time\"); run service_install through %s, or put it next to %s",
		ErrDetect, RunServiceSubcommand, q(filepath.Base(self)), RunnerExe, q(filepath.Base(self)))
}

// isRunnerName reports whether base is the setup runner's file name, the only
// binary that answers RunServiceSubcommand.
func isRunnerName(base string) bool {
	return strings.EqualFold(base, RunnerExe) || strings.EqualFold(base, "piperoom-setup")
}

// deleteExisting stops and deletes the service install is replacing, after the
// new definition is on disk.
func (m *windowsManager) deleteExisting(ctx context.Context, api serviceAPI, name string) error {
	h, err := api.Open(name)
	if errors.Is(err, errServiceNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: opening %s: %w", ErrServiceManager, q(name), err)
	}
	defer h.Close()
	return m.stopAndDelete(ctx, h, name)
}

// restore re-creates a service install replaced, from the definition it
// recorded before this install. The exe path is the same bin/ copy (now the
// new runner), and the recorded arguments still name this project, so the
// previous worker comes back as it was.
func (m *windowsManager) restore(ctx context.Context, api serviceAPI, name string, previous []byte) error {
	exe, args, display, err := parseDefinition(previous)
	if err != nil {
		return err
	}
	h, err := m.createService(ctx, api, name, exe, args, serviceConfig{
		DisplayName: display, Automatic: true,
		Account: serviceAccount(name), ServiceSID: true,
	})
	if err != nil {
		return err
	}
	return h.Close()
}

// readDefinition returns this project's recorded definition for name, and
// whether it existed; a read failure is treated as "no record" because the
// caller only uses it to restore after a failed replace.
func readDefinition(env ops.Env, name string) ([]byte, bool) {
	if env.Confine == nil {
		return nil, false
	}
	clean, err := env.Confine.Check(name)
	if err != nil {
		return nil, false
	}
	root, err := os.OpenRoot(env.Confine.Dir())
	if err != nil {
		return nil, false
	}
	defer root.Close()
	b, err := root.ReadFile(clean)
	if err != nil {
		return nil, false
	}
	return b, true
}

// restoreDefinition puts the recorded definition back after a failed install,
// best effort: the op already reports the failure that matters, and the record
// is not what runs.
func restoreDefinition(env ops.Env, name string, previous []byte, existed bool) {
	if !existed {
		_ = removeDriverFile(env, name)
		return
	}
	if _, _, err := writeDriverFile(env, name, previous, 0o600, ServiceDir); err != nil {
		return
	}
}

// parseDefinition reads the exe, arguments and display name of a definition
// record (renderServiceDefinition's output).
func parseDefinition(record []byte) (exe string, args []string, display string, err error) {
	for _, line := range strings.Split(string(record), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch key {
		case "exe":
			exe = value
		case "arg":
			args = append(args, value)
		case "display":
			display = value
		}
	}
	if exe == "" || display == "" {
		return "", nil, "", fmt.Errorf("%w: the recorded definition of the service to restore names no exe or display name", ErrDetect)
	}
	return exe, args, display, nil
}

// restart stops the service if it runs and starts it, then reports the state
// it reached.
func (m *windowsManager) restart(ctx context.Context, env ops.Env, name string) (ops.Result, error) {
	// Only a service this project installed: service_install wrote the
	// record, and a machine-wide name alone does not prove it is this
	// project's.
	if err := requireFile(env, ServiceDir+"/"+name+".txt", false); err != nil {
		return ops.Result{}, fmt.Errorf("%w; run service_install first", err)
	}
	api, err := m.open()
	if err != nil {
		return ops.Result{}, err
	}
	defer api.Close()
	h, err := api.Open(name)
	if errors.Is(err, errServiceNotExist) {
		return ops.Result{}, fmt.Errorf("%w: the service manager has no service named %s; run service_install first", ErrDetect, q(name))
	}
	if err != nil {
		return ops.Result{}, fmt.Errorf("%w: opening %s: %w", ErrServiceManager, q(name), err)
	}
	defer h.Close()
	before, err := h.Query()
	if err != nil {
		return ops.Result{}, fmt.Errorf("%w: querying %s: %w", ErrServiceManager, q(name), err)
	}
	summary := "started " + name
	if before.State != stateStopped {
		if _, err := h.Control(controlStop); err != nil && !errors.Is(err, errServiceNotActive) {
			return ops.Result{}, fmt.Errorf("%w: stopping %s: %w", ErrServiceManager, q(name), err)
		}
		if err := m.waitForState(ctx, h, name, stateStopped, 0); err != nil {
			return ops.Result{}, err
		}
		summary = "restarted " + name
	}
	if err := h.Start(); err != nil {
		return ops.Result{}, fmt.Errorf("%w: starting %s: %w", ErrServiceManager, q(name), err)
	}
	// A JVM that dies at once must not keep this op polling for 30 s: a Stopped
	// state while it waits for Running ends the wait with the exit code.
	if err := m.waitForState(ctx, h, name, stateRunning, stateStopped); err != nil {
		return ops.Result{}, err
	}
	after, _ := h.Query()
	summary += "; now " + after.State.String()
	return ops.Result{
		Summary: summary,
		Data: map[string]any{
			"unit": name, "service": name, "state": after.State.String(),
			"running": after.State == stateRunning, "was": before.State.String(),
			"process_id": after.ProcessID, "exit_code": after.exitCode(),
		},
		Receipt: events.SetupReceipt{Kind: "", Target: name},
	}, nil
}

// uninstall stops and deletes the service and removes the record. It is the
// rollback of install; no op exposes it yet (U1-14b's rollback path owns that),
// so the package's tests are its only caller for now.
func (m *windowsManager) uninstall(ctx context.Context, env ops.Env, name string) (ops.Result, error) {
	if err := requireFile(env, ServiceDir+"/"+name+".txt", false); err != nil {
		return ops.Result{}, fmt.Errorf("%w; nothing to uninstall", err)
	}
	api, err := m.open()
	if err != nil {
		return ops.Result{}, err
	}
	defer api.Close()
	h, err := api.Open(name)
	if errors.Is(err, errServiceNotExist) {
		_ = removeDriverFile(env, ServiceDir+"/"+name+".txt")
		return ops.Result{Summary: name + " was already gone from the service manager; removed its record",
			Data:    map[string]any{"unit": name, "installed": false},
			Receipt: events.SetupReceipt{Kind: "", Target: name}}, nil
	}
	if err != nil {
		return ops.Result{}, fmt.Errorf("%w: opening %s: %w", ErrServiceManager, q(name), err)
	}
	defer h.Close()
	if err := m.stopAndDelete(ctx, h, name); err != nil {
		return ops.Result{}, err
	}
	removed := true
	if err := removeDriverFile(env, ServiceDir+"/"+name+".txt"); err != nil {
		removed = false
	}
	return ops.Result{
		Summary: name + " uninstalled (stopped and deleted); the bin/" + RunnerExe + " copy stays for a reinstall",
		Data:    map[string]any{"unit": name, "installed": false, "record_removed": removed},
		Receipt: events.SetupReceipt{Kind: "", Target: name},
	}, nil
}

// stopAndDelete stops a running service and deletes it.
func (m *windowsManager) stopAndDelete(ctx context.Context, h serviceHandle, name string) error {
	st, err := h.Query()
	if err != nil {
		return fmt.Errorf("%w: querying %s: %w", ErrServiceManager, q(name), err)
	}
	if st.State != stateStopped {
		if _, err := h.Control(controlStop); err != nil && !errors.Is(err, errServiceNotActive) {
			return fmt.Errorf("%w: stopping %s: %w", ErrServiceManager, q(name), err)
		}
		if err := m.waitForState(ctx, h, name, stateStopped, 0); err != nil {
			return err
		}
	}
	if err := h.Delete(); err != nil && !errors.Is(err, errServiceNotExist) {
		return fmt.Errorf("%w: deleting %s: %w", ErrServiceManager, q(name), err)
	}
	return nil
}

// waitForState polls the SCM until the service reaches want. A non-zero abort
// state ends the wait early (a service that stopped again while a start was
// being awaited has failed, and polling cannot make it run).
func (m *windowsManager) waitForState(ctx context.Context, h serviceHandle, name string, want, abort serviceState) error {
	poll := m.poll
	if poll <= 0 {
		poll = defaultPollInterval
	}
	deadline := time.Now().Add(stateWaitTimeout)
	for {
		st, err := h.Query()
		if err != nil {
			return fmt.Errorf("%w: querying %s: %w", ErrServiceManager, q(name), err)
		}
		if st.State == want {
			return nil
		}
		if abort != 0 && st.State == abort {
			detail := fmt.Sprintf("%s stopped before reaching %s", q(name), want)
			if code := st.exitCode(); code != 0 {
				detail += fmt.Sprintf(" (last exit code %d)", code)
			}
			return fmt.Errorf("%w: %s", ErrServiceManager, detail)
		}
		if !time.Now().Add(poll).Before(deadline) {
			return fmt.Errorf("%w: %s did not reach %s within %s (it is %s)", ErrServiceManager, q(name), want, stateWaitTimeout, st.State)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: waiting for %s to reach %s: %w", ErrServiceManager, q(name), want, ctx.Err())
		case <-time.After(poll):
		}
	}
}

// ownedByProject reports whether this project recorded installing name.
func ownedByProject(env ops.Env, name string) bool {
	if env.Confine == nil {
		return false
	}
	return requireFile(env, ServiceDir+"/"+name+".txt", false) == nil
}

// undoDriverFiles removes the exe copy install wrote when it did not exist
// before, best effort: the op already failed and its error is what it reports.
func undoDriverFiles(env ops.Env, exeName string, existed bool) {
	if !existed {
		_ = removeDriverFile(env, exeName)
	}
}

// removeDriverFile removes one file the driver wrote under the project, through
// the project root.
func removeDriverFile(env ops.Env, name string) error {
	if env.Confine == nil {
		return errors.New("host: the project root is not open")
	}
	clean, err := env.Confine.Check(name)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(env.Confine.Dir())
	if err != nil {
		return fmt.Errorf("host: opening the project root: %w", err)
	}
	defer root.Close()
	return root.Remove(clean)
}

// serviceDisplayName is the SCM display name, from the template and the mode.
func serviceDisplayName(s serviceSpec) string {
	return "Piperoom Kafka Connect worker (" + s.Mode + ")"
}

// renderServiceDefinition is the artifact service_install records and the
// preview diffs: what the SCM is told, one item per line, so a changed argument
// shows as one changed line. It is not parsed back; the record proves this
// project installed the name, and service_status shows what the SCM reports
// now.
func renderServiceDefinition(s serviceSpec) ([]byte, error) {
	if !descriptionRE.MatchString(serviceDisplayName(s)) {
		return nil, fmt.Errorf("%w: display name %s", ErrParams, q(serviceDisplayName(s)))
	}
	if err := checkDefinitionValue(s.Unit); err != nil {
		return nil, err
	}
	for _, p := range []string{s.ExePath, s.ProjectDir, s.LogFile} {
		if err := CheckServicePath(p); err != nil {
			return nil, err
		}
	}
	for _, v := range append(append([]string(nil), s.Args...), s.Java...) {
		if err := checkDefinitionValue(v); err != nil {
			return nil, err
		}
	}
	var b strings.Builder
	b.WriteString("# Generated by piperoom setup (service_install, Windows); service_install rewrites this file.\n")
	line := func(key, value string) { b.WriteString(key + "=" + value + "\n") }
	line("service", s.Unit)
	line("display", serviceDisplayName(s))
	line("account", serviceAccount(s.Unit))
	line("start", "automatic")
	line("project", s.ProjectDir)
	line("exe", s.ExePath)
	for _, a := range s.Args {
		line("arg", a)
	}
	for _, a := range s.Java {
		line("java-arg", a)
	}
	line("log", s.LogFile)
	return []byte(b.String()), nil
}
