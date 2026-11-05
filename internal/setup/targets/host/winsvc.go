package host

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
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
// installed service is therefore this binary — the copy staged in the
// service's release under Program Files — started with the run-service
// subcommand; that process
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
// staged release only (specWindows, complete): no op param names an exe or an
// argument. CreateService builds the command line itself with
// syscall.EscapeArg from the exe path and the argument slice, so this package
// never builds a BinaryPathName string by hand.
//
// The service runs as NT SERVICE\<service>, not LocalSystem, its token keeps one
// privilege, and the directories it executes from are given a protected DACL at
// install: see serviceAccountPrefix below for the account, the ACLs and what
// they do and do not close.

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
// runs as: NT SERVICE\<service name>. It is not LocalSystem.
//
// A Windows service is a machine service, so everything it executes or reads
// as configuration must be something only administrators can change, or any
// process of the person's account could plant code that runs at the next
// (automatic) start under another identity. Four things make it so, and the
// install refuses rather than weakens any of them:
//
//   - Where it runs from: never the project. install stages a release into
//     %ProgramFiles%\Piperoom\<service>\releases\<digest> (servicehome.go):
//     the runner exe, the distribution's libs and config, the plugins and the
//     worker file, validated again and with its paths rewritten into the
//     release. The person's later edits under the project change nothing that
//     runs until the next approved, elevated service_install.
//   - The DACLs: the home and everything in it are owned by Administrators with
//     a protected DACL — Administrators and SYSTEM full control, the service
//     and Users read and execute, the service modify on logs\ and data\ only
//     (acl.go) — and install then checks the release, the home and every
//     directory above them (trustAPI) before it records the service.
//   - The JDK: it is not staged, so it must already be one only administrators
//     can change (a JDK under Program Files); one under the person's profile is
//     refused at preview, and the service checks the JDK and its release again
//     at every start.
//   - The account and token: the SCM logs the service on as NT SERVICE\<service>
//     (mgr.Config.ServiceStartName, SidType SERVICE_SID_TYPE_UNRESTRICTED so
//     the SID is in the token), whose token keeps SeChangeNotifyPrivilege and
//     nothing else (serviceRequiredPrivileges), so the SeImpersonatePrivilege
//     every service token otherwise carries — the public "potato" step from a
//     service account to SYSTEM — is gone.
//
// A virtual account authenticates to a domain as the machine account
// (DOMAIN\HOST$); row U8e documents that, the account and where to install.
const serviceAccountPrefix = `NT SERVICE\`

// serviceSIDTypeUnrestricted is windows.SERVICE_SID_TYPE_UNRESTRICTED: the
// service's virtual-account SID is written into its token (and its child
// processes' tokens) at logon, which is what makes NT SERVICE\<service> an
// identity the SCM accepts. It mirrors the Windows constant here, the way the
// state and control values above do, because this file compiles on every OS.
const serviceSIDTypeUnrestricted = 1

// serviceAccount is the account the service runs as.
func serviceAccount(service string) string { return serviceAccountPrefix + service }

// serviceRequiredPrivileges is the whole privilege set the service's token
// keeps (SERVICE_REQUIRED_PRIVILEGES_INFO, which the SCM applies when it logs
// the service on; winsvc_windows.go). SeChangeNotifyPrivilege is "bypass
// traverse checking", which every process needs to open a path through a
// directory it may not list. Everything else a service token
// otherwise gets is dropped, and SeImpersonatePrivilege and
// SeCreateGlobalPrivilege are the two that matter: "Impersonate a client after
// authentication" is granted to the SERVICE group by default, and a token that
// holds it can be raised to SYSTEM by the public potato/PrintSpoofer tools,
// which Microsoft does not treat as a vulnerability. The Connect JVM needs no
// privilege beyond reading its files and opening sockets.
var serviceRequiredPrivileges = []string{"SeChangeNotifyPrivilege"}

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
	// Privileges is the privilege set the token keeps
	// (SERVICE_CONFIG_REQUIRED_PRIVILEGES_INFO); empty leaves the SCM's
	// default set, which is what install must not do (serviceRequiredPrivileges).
	Privileges []string
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
	// self returns the running binary's path: the file install stages as the
	// release's bin\piperoom-setup.exe, which is what the SCM runs.
	self func() (string, error)
	// programFiles is %ProgramFiles% from the known-folder API (the service
	// home's parent).
	programFiles func() (string, error)
	// acl sets the DACLs of the service home, after the service exists (its
	// virtual account has no SID before that).
	acl aclAPI
	// trust checks that only administrators can change the JDK and the
	// staged release.
	trust trustAPI
	poll  time.Duration
}

// newWindowsManager builds the manager over the platform's bindings. self is
// os.Executable in production; the tests pass a small file and a temporary
// Program Files.
func newWindowsManager(connect func() (serviceAPI, error), self, programFiles func() (string, error), acl aclAPI, trust trustAPI) *windowsManager {
	return &windowsManager{connect: connect, self: self, programFiles: programFiles, acl: acl, trust: trust, poll: defaultPollInterval}
}

func (m *windowsManager) artifact(s serviceSpec) ([]byte, error) { return renderServiceDefinition(s) }

// home is the admin-only home of service name.
func (m *windowsManager) home(name string) (string, error) {
	pf, err := m.programFiles()
	if err != nil {
		return "", err
	}
	home := serviceHome(pf, name)
	if err := CheckServicePath(home); err != nil {
		return "", fmt.Errorf("the service home: %w", err)
	}
	return home, nil
}

// complete completes the project half of s (specWindows) with where the service
// runs from: the home under Program Files, the release the project's content
// makes (planRelease: its id is the content digest, so the preview names what
// install stages) and the exe, arguments and JVM argv in that release. The JDK
// is not staged, so it must already be one only administrators can change; a
// JDK under the person's profile is refused here, before anything is shown for
// approval.
func (m *windowsManager) complete(env ops.Env, s serviceSpec) (serviceSpec, error) {
	home, err := m.home(s.Unit)
	if err != nil {
		return serviceSpec{}, err
	}
	if err := m.trust.CheckTrusted(s.JavaHome, true); err != nil {
		return serviceSpec{}, fmt.Errorf("the JDK at %s: %w; the service runs java.exe from it, so it must be a JDK only administrators can change (one installed under Program Files)", q(s.JavaHome), err)
	}
	runner, err := m.runnerExe()
	if err != nil {
		return serviceSpec{}, err
	}
	plan, err := planRelease(env, runner, s.Mode, s.Class, home)
	if err != nil {
		return serviceSpec{}, err
	}
	release := releasePath(home, plan.id)
	java, err := javaArgv(release, home, s.JavaHome, s.Mode, nativeJoin(s.ProjectDir, DistDir, "config"))
	if err != nil {
		return serviceSpec{}, err
	}
	s.Home, s.Release, s.Digest, s.plan = home, release, plan.digest, &plan
	s.ExePath = nativeJoin(release, BinDir, RunnerExe)
	s.Args = []string{
		RunServiceSubcommand, "--release", release,
		"--name", strings.TrimPrefix(s.Unit, UnitPrefix), "--mode", s.Mode, "--java", s.JavaHome,
	}
	for _, v := range append([]string{s.ExePath}, s.Args...) {
		if err := CheckServiceToken(v); err != nil {
			return serviceSpec{}, err
		}
	}
	s.Java = java
	s.LogFile = nativeJoin(home, homeLogDir, ServiceLogName)
	s.Offsets = plan.offsetsFile(s.ProjectDir, home)
	return s, nil
}

// open connects to the SCM. Every failure here is the "no elevated prompt"
// refusal in practice, so the error says so.
func (m *windowsManager) open() (serviceAPI, error) {
	api, err := m.connect()
	if err != nil {
		return nil, fmt.Errorf("%w: connecting to the service control manager (service_install and service_restart install and start a machine service, so they need an elevated prompt): %w", ErrServiceManager, err)
	}
	return api, nil
}

// owned reports whether the service named name is this project's: its home
// holds a record service_install wrote for this project directory. The record
// is in the admin-only home, so a file under the project cannot claim a
// service.
func (m *windowsManager) owned(env ops.Env, name string) (serviceSpec, bool) {
	home, err := m.home(name)
	if err != nil {
		return serviceSpec{}, false
	}
	record, ok := readHomeRecord(home)
	if !ok {
		return serviceSpec{}, false
	}
	s, err := parseRecord(env, name, home, record)
	return s, err == nil
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
	rec, own := m.owned(env, name)
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
		data["record"] = nativeJoin(rec.Home, homeRecordName)
		data["release"] = rec.Release
		data["log"] = rec.LogFile
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

// installState is what install found before it changed anything, and what it
// changed: what a failure puts back (rollbackInstall).
type installState struct {
	previous      []byte      // the home's record before this install
	prevSpec      serviceSpec // previous, parsed; set when it was valid
	recordExisted bool
	markerBefore  []byte // the project's services/<name>.txt before this install
	markerExisted bool
	replaced      bool
	wasRunning    bool
	staged        string // the release directory this install staged
	created       bool
}

// install registers the service, or replaces this project's earlier install of
// the same name. Anything named exe or argument comes from s, which the spec
// and complete built from the template, the project's content and the home.
//
// The order is the replace guarantee. A running service is stopped first.
// Then the release is staged next to the previous one (never over it, unless
// its content is identical), the old service is deleted and the new one
// created, and only then is the previous release pruned. A failure at any step
// after the stop puts the previous service back, created from its validated
// record and started again, with its release untouched; so a replacement that
// cannot finish leaves the previous worker running rather than none.
func (m *windowsManager) install(ctx context.Context, env ops.Env, s serviceSpec) (ops.Result, error) {
	if s.plan == nil || s.Home == "" {
		return ops.Result{}, errors.New("host: the service definition was not resolved")
	}
	def, err := m.artifact(s)
	if err != nil {
		return ops.Result{}, err
	}
	// The path rule first, so an unsafe marker path writes nothing and calls
	// nothing.
	if _, err := env.Confine.Check(s.UnitFile); err != nil {
		return ops.Result{}, err
	}
	api, err := m.open()
	if err != nil {
		return ops.Result{}, err
	}
	defer api.Close()

	st := installState{}
	st.previous, st.recordExisted = readHomeRecord(s.Home)
	st.markerBefore, st.markerExisted = readDefinition(env, s.UnitFile)
	h, err := api.Open(s.Unit)
	switch {
	case err == nil:
		// The name is machine-wide. Replacing a service this project did not
		// install would stop and delete someone else's worker.
		prev, own := m.owned(env, s.Unit)
		if !own {
			h.Close()
			return ops.Result{}, fmt.Errorf("%w: a service named %s is already installed and this project did not install it (no record for this project in %s); pick another service name",
				ErrDetect, q(s.Unit), q(nativeJoin(s.Home, homeRecordName)))
		}
		st.prevSpec = prev
		before, qerr := h.Query()
		h.Close()
		if qerr != nil {
			return ops.Result{}, fmt.Errorf("%w: querying %s: %w", ErrServiceManager, q(s.Unit), qerr)
		}
		st.replaced = true
		st.wasRunning = before.State != stateStopped
	case !errors.Is(err, errServiceNotExist):
		return ops.Result{}, fmt.Errorf("%w: opening %s: %w", ErrServiceManager, q(s.Unit), err)
	}

	if st.wasRunning {
		if err := m.stopExisting(ctx, api, s.Unit); err != nil {
			return ops.Result{}, err
		}
	}
	fail := func(cause error) (ops.Result, error) {
		return ops.Result{}, m.rollbackInstall(ctx, api, env, s, st, cause)
	}

	release, err := s.plan.stage(env, s.Home)
	if err != nil {
		return fail(err)
	}
	st.staged = release
	if st.replaced {
		if err := m.deleteExisting(ctx, api, s.Unit); err != nil {
			return fail(err)
		}
	}
	h, err = m.create(ctx, api, s)
	if err != nil {
		return fail(err)
	}
	h.Close()
	st.created = true
	account := serviceAccount(s.Unit)
	// The service exists now, so its virtual account has a SID to grant. Then
	// the whole home is checked, not assumed: a service whose release another
	// account could change is removed rather than installed.
	if err := m.acl.Harden(account, homeACLTargets(s.Home)); err != nil {
		return fail(fmt.Errorf("%w: %w", ErrServiceManager, err))
	}
	if err := m.trust.CheckTrusted(s.Release, true); err != nil {
		return fail(fmt.Errorf("the staged release: %w", err))
	}
	if err := writeHomeRecord(s.Home, def); err != nil {
		return fail(err)
	}
	record, recordChanged, err := writeDriverFile(env, s.UnitFile, def, 0o600, ServiceDir)
	if err != nil {
		return fail(err)
	}
	restarted := false
	var startErr error
	if st.wasRunning {
		restarted, startErr = m.startExisting(ctx, api, s.Unit)
	}
	// The previous release is removed only now: until the new service exists,
	// a rollback recreates the previous one from it.
	pruneErr := pruneReleases(s.Home, nativeBase(s.Release))

	summary := fmt.Sprintf("installed %s (%s) as a Windows service from release %s under %s", s.Unit, s.Mode, nativeBase(s.Release), s.Home)
	if st.replaced {
		summary = fmt.Sprintf("replaced %s (%s); now release %s under %s", s.Unit, s.Mode, nativeBase(s.Release), s.Home)
		if st.wasRunning {
			summary += "; the running worker was stopped first"
			if restarted {
				summary += " and started again"
			} else if startErr != nil {
				summary += " and did not start again (" + startErr.Error() + "); service_restart retries"
			}
		}
	}
	summary += "; start=automatic, account=" + account + ", its token keeps " + strings.Join(serviceRequiredPrivileges, ",")
	summary += "; the release is Administrators-owned and read-only for the service, which writes only " + homeLogDir + " and " + homeDataDir
	summary += "; a change under the project (properties_write, plugin_install) takes effect at the next service_install"
	if pruneErr != nil {
		summary += "; an older release could not be removed yet (the next install retries)"
	}
	if !st.replaced {
		summary += "; service_restart starts it"
	}
	return ops.Result{
		Summary: summary,
		Data: map[string]any{
			"unit": s.Unit, "service": s.Unit, "mode": s.Mode, "exe": s.ExePath,
			"home": s.Home, "release": s.Release, "digest": s.Digest,
			"args": s.Args, "java": s.Java, "java_home": s.JavaHome,
			"record": record, "record_changed": recordChanged, "definition": string(def),
			"replaced": st.replaced, "was_running": st.wasRunning, "started": restarted,
			"start_type": "automatic", "account": account, "privileges": serviceRequiredPrivileges,
			"acl": homeACLTargets(s.Home),
		},
		Receipt: events.SetupReceipt{Kind: "file", Target: record},
	}, nil
}

// stopExisting stops a service and waits for it to reach Stopped.
func (m *windowsManager) stopExisting(ctx context.Context, api serviceAPI, name string) error {
	h, err := api.Open(name)
	if errors.Is(err, errServiceNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: opening %s: %w", ErrServiceManager, q(name), err)
	}
	defer h.Close()
	if _, err := h.Control(controlStop); err != nil && !errors.Is(err, errServiceNotActive) {
		return fmt.Errorf("%w: stopping %s: %w", ErrServiceManager, q(name), err)
	}
	return m.waitForState(ctx, h, name, stateStopped, 0)
}

// startExisting starts a service and waits for it to run, reporting a JVM that
// died at once with its exit code instead of polling for the whole timeout.
func (m *windowsManager) startExisting(ctx context.Context, api serviceAPI, name string) (bool, error) {
	h, err := api.Open(name)
	if err != nil {
		return false, fmt.Errorf("%w: opening %s: %w", ErrServiceManager, q(name), err)
	}
	defer h.Close()
	if err := h.Start(); err != nil {
		return false, fmt.Errorf("%w: starting %s: %w", ErrServiceManager, q(name), err)
	}
	if err := m.waitForState(ctx, h, name, stateRunning, stateStopped); err != nil {
		return false, err
	}
	return true, nil
}

// rollbackInstall puts back what install changed when it cannot finish: the
// service it created is deleted, the previous service is created from its
// validated record and started again if it was running, the home's record and
// the project's marker are restored, and the release this install staged is
// removed unless the previous service runs from it. Every step is attempted;
// the failures are appended to the cause the op reports.
func (m *windowsManager) rollbackInstall(ctx context.Context, api serviceAPI, env ops.Env, s serviceSpec, st installState, cause error) error {
	err := cause
	if st.created {
		if derr := m.deleteExisting(ctx, api, s.Unit); derr != nil {
			err = fmt.Errorf("%w; removing the new service also failed: %v", err, derr)
		}
	}
	if st.replaced {
		if rerr := m.restore(ctx, api, st.prevSpec); rerr != nil {
			err = fmt.Errorf("%w; restoring the previous service also failed: %v", err, rerr)
		} else if st.wasRunning {
			if _, serr := m.startExisting(ctx, api, s.Unit); serr != nil {
				err = fmt.Errorf("%w; the previous service was restored but did not start again: %v", err, serr)
			}
		}
	}
	var prevRecord []byte
	if st.recordExisted {
		prevRecord = st.previous
	}
	if werr := writeHomeRecord(s.Home, prevRecord); werr != nil {
		err = fmt.Errorf("%w; restoring the service record also failed: %v", err, werr)
	}
	restoreDefinition(env, s.UnitFile, st.markerBefore, st.markerExisted)
	if st.staged != "" && !samePath(st.staged, st.prevSpec.Release) {
		if rerr := removeRelease(s.Home, nativeBase(st.staged)); rerr != nil {
			err = fmt.Errorf("%w; removing the staged release also failed: %v", err, rerr)
		}
	}
	return err
}

// removeRelease removes one release directory of home.
func removeRelease(home, id string) error {
	if !releaseIDRE.MatchString(id) {
		return fmt.Errorf("%w: %s is not a release id", ErrParams, q(id))
	}
	hroot, err := os.OpenRoot(home)
	if err != nil {
		return err
	}
	defer hroot.Close()
	return hroot.RemoveAll(releasesDir + "/" + id)
}

// pollOrDefault is the poll interval, never zero.
func (m *windowsManager) pollOrDefault() time.Duration {
	if m.poll <= 0 {
		return defaultPollInterval
	}
	return m.poll
}

// create registers the service s describes.
func (m *windowsManager) create(ctx context.Context, api serviceAPI, s serviceSpec) (serviceHandle, error) {
	return m.createService(ctx, api, s.Unit, s.ExePath, s.Args, m.config(s))
}

// config is what the SCM is told about s: the template's display name,
// automatic start, the service's own virtual account and its reduced privilege
// set.
func (m *windowsManager) config(s serviceSpec) serviceConfig {
	return serviceConfig{
		DisplayName: serviceDisplayName(s),
		Description: "Piperoom Kafka Connect " + s.Mode + " worker for the project at " + s.ProjectDir,
		Automatic:   true,
		Account:     serviceAccount(s.Unit),
		ServiceSID:  true,
		Privileges:  serviceRequiredPrivileges,
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

// restore re-creates a service install replaced, from the definition the home
// recorded before this install (parsed and validated by owned).
func (m *windowsManager) restore(ctx context.Context, api serviceAPI, prev serviceSpec) error {
	if prev.Unit == "" || prev.ExePath == "" {
		return fmt.Errorf("%w: there is no valid record of the previous service to restore", ErrDetect)
	}
	h, err := m.createService(ctx, api, prev.Unit, prev.ExePath, prev.Args, m.config(prev))
	if err != nil {
		return err
	}
	return h.Close()
}

// readDefinition returns the project's marker record for name, and whether it
// existed; a read failure is treated as "no record" because the caller only
// uses it to put the marker back after a failed install.
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

// restoreDefinition puts the project's marker back after a failed install,
// best effort: the op already reports the failure that matters, and the marker
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

// parseRecord reads a definition record (renderServiceDefinition's output) back
// into the spec install wrote for it, and refuses one it did not write. The
// record install trusts is the one in the admin-only home, but it is still
// checked rather than believed, because the elevated install hands its exe and
// arguments to CreateService:
//
//   - the project must be this project directory, and the home this service's
//     home under Program Files;
//   - the exe must be the bin\piperoom-setup.exe of a release under that home,
//     and the arguments exactly run-service and this service's own flags;
//   - every value must pass the definition's own checks, and the record must
//     round-trip byte for byte through renderServiceDefinition, so an added,
//     removed or reordered line is refused too.
func parseRecord(env ops.Env, name, home string, record []byte) (serviceSpec, error) {
	refuse := func(why string) (serviceSpec, error) {
		return serviceSpec{}, fmt.Errorf("%w: the recorded definition of %s is not one service_install wrote (%s); run service_install to rewrite it",
			ErrDetect, q(name), why)
	}
	s := serviceSpec{Unit: name, UnitFile: ServiceDir + "/" + name + ".txt"}
	var account, start string
	for _, line := range strings.Split(string(record), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return refuse("a line without a key")
		}
		if err := checkDefinitionValue(value); err != nil {
			return refuse("a value a definition cannot carry")
		}
		switch key {
		case "service":
			if value != name {
				return refuse("it names another service")
			}
		case "display":
			// Compared through the round-trip: the display name is derived from
			// the mode.
		case "account":
			account = value
		case "start":
			start = value
		case "project":
			s.ProjectDir = value
		case "home":
			s.Home = value
		case "release":
			s.Release = value
		case "digest":
			s.Digest = value
		case "exe":
			s.ExePath = value
		case "arg":
			s.Args = append(s.Args, value)
		case "java-arg":
			s.Java = append(s.Java, value)
		case "log":
			s.LogFile = value
		default:
			return refuse("it holds an unknown key " + q(key))
		}
	}
	if env.Confine == nil {
		return serviceSpec{}, errors.New("host: the project root is not open")
	}
	dir := env.Confine.Dir()
	if s.ProjectDir != dir {
		return refuse("its project is not this project directory")
	}
	if s.Home != home {
		return refuse("its home is not this service's home under Program Files")
	}
	id := nativeBase(s.Release)
	if !releaseIDRE.MatchString(id) || s.Release != releasePath(home, id) {
		return refuse("its release is not a release under the service's home")
	}
	if len(s.Digest) != 64 || !strings.HasPrefix(s.Digest, id) || strings.Trim(s.Digest, "0123456789abcdef") != "" {
		return refuse("its digest is not the release's")
	}
	if s.ExePath != nativeJoin(s.Release, BinDir, RunnerExe) {
		return refuse("its exe is not the release's " + BinDir + "/" + RunnerExe)
	}
	if account != serviceAccount(name) || start != "automatic" {
		return refuse("its account or start type is not the one install sets")
	}
	// The arguments are exactly the ones complete builds: the subcommand, the
	// release, this service's name and mode, and the JDK home.
	args := s.Args
	if len(args) != 9 || args[0] != RunServiceSubcommand ||
		args[1] != "--release" || args[2] != s.Release ||
		args[3] != "--name" || args[5] != "--mode" || args[7] != "--java" {
		return refuse("its arguments are not " + RunServiceSubcommand + " with --release, --name, --mode and --java")
	}
	if _, ok := modeFiles[args[6]]; !ok {
		return refuse("its mode is neither " + ModeDistributed + " nor " + ModeStandalone)
	}
	if strings.HasPrefix(name, UnitPrefix) && args[4] != strings.TrimPrefix(name, UnitPrefix) {
		return refuse("its --name is not this service's")
	}
	for _, v := range append([]string{s.ExePath}, args...) {
		if err := CheckServiceToken(v); err != nil {
			return refuse("an argument is not a token a service definition can carry")
		}
	}
	if err := CheckServicePath(args[8]); err != nil {
		return refuse("its --java is not an absolute path")
	}
	if s.LogFile != nativeJoin(home, homeLogDir, ServiceLogName) {
		return refuse("its log is not the home's")
	}
	s.Mode = args[6]
	s.JavaHome = args[8]
	s.Worker = modeFiles[s.Mode][1]
	// The round-trip is the last check: the record must be exactly the bytes
	// install writes for the spec above, so a display name or a JVM argument
	// the record does not share with install is refused.
	rendered, err := renderServiceDefinition(s)
	if err != nil || !bytes.Equal(rendered, record) {
		return refuse("it is not the exact text service_install writes")
	}
	return s, nil
}

// restart stops the service if it runs and starts it, then reports the state
// it reached.
func (m *windowsManager) restart(ctx context.Context, env ops.Env, name string) (ops.Result, error) {
	// Only a service this project installed: service_install wrote the
	// home's record, and a machine-wide name alone does not prove it is this
	// project's.
	if _, own := m.owned(env, name); !own {
		return ops.Result{}, fmt.Errorf("%w: this project has no record of installing %s; run service_install first", ErrDetect, q(name))
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
	summary += "; now " + after.State.String() + " (from its staged release: a change under the project applies at the next service_install)"
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

// uninstall stops and deletes the service, then removes its releases, the
// home's record and the project's marker; the home's logs and data (a
// standalone worker's offsets) stay for a reinstall. It is the rollback of
// install; no op exposes it yet (U1-14b's rollback path owns that), so the
// package's tests are its only caller for now.
func (m *windowsManager) uninstall(ctx context.Context, env ops.Env, name string) (ops.Result, error) {
	rec, own := m.owned(env, name)
	if !own {
		return ops.Result{}, fmt.Errorf("%w: this project has no record of installing %s; nothing to uninstall", ErrDetect, q(name))
	}
	api, err := m.open()
	if err != nil {
		return ops.Result{}, err
	}
	defer api.Close()
	h, err := api.Open(name)
	switch {
	case errors.Is(err, errServiceNotExist):
	case err != nil:
		return ops.Result{}, fmt.Errorf("%w: opening %s: %w", ErrServiceManager, q(name), err)
	default:
		err := m.stopAndDelete(ctx, h, name)
		h.Close()
		if err != nil {
			return ops.Result{}, err
		}
	}
	var errs []error
	if err := pruneReleases(rec.Home, ""); err != nil {
		errs = append(errs, err)
	}
	if err := writeHomeRecord(rec.Home, nil); err != nil {
		errs = append(errs, err)
	}
	if err := removeDriverFile(env, rec.UnitFile); err != nil && !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, err)
	}
	summary := name + " uninstalled (stopped and deleted); its releases and record are removed, " + homeLogDir + " and " + homeDataDir + " under " + rec.Home + " stay"
	if err := errors.Join(errs...); err != nil {
		summary += "; some files could not be removed: " + clipText(err.Error(), maxQuoted)
	}
	return ops.Result{
		Summary: summary,
		Data:    map[string]any{"unit": name, "installed": false, "home": rec.Home, "record_removed": len(errs) == 0},
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
	for _, p := range []string{s.ExePath, s.ProjectDir, s.Home, s.Release, s.LogFile} {
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
	line("home", s.Home)
	line("release", s.Release)
	line("digest", s.Digest)
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
