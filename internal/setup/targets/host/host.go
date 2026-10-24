// Package host is the plain-host target of the setup runner (row U8a,
// docs/design/setup-agent.md §2.5 and §5): a Kafka Connect worker that runs
// from a Kafka distribution under the project directory as a service of the
// host's service manager. On Linux that is a user-level systemd unit
// (`systemctl --user`); on Windows it is a service of the service control
// manager (row U8b, file: winsvc.go), which cannot start java.exe directly:
// the installed service is a copy of piperoom-setup itself running its
// run-service subcommand (dispatched by setupcmd.Run, file: servicehost.go),
// and it supervises the Connect JVM.
//
// Ops: service_status, properties_read and journal_tail (T0, row U5a);
// properties_write (T1, the project directory only); service_install and
// service_restart (T2).
//
// The S1 rule of §2.5 for the service: service_install takes a service name and
// a worker mode, nothing else. ExecStart, WorkingDirectory and Environment (the
// systemd unit) and the exe, its arguments and the JVM argv (the Windows
// service) come from the driver's templates and from what it detects: the
// project directory, the distribution at kafka/ under it, the worker properties
// file the mode names, and the JDK the java on PATH resolves to. No op param
// reaches the rendered text. A detected path is checked against the shape of
// the artifact it goes into (pathrule.go): a systemd unit takes POSIX values of
// safe characters only (a space, %, $, a quote or a backslash is refused,
// because systemd would split or expand it), while a Windows service definition
// takes a drive-absolute path with a space or a parenthesis, which
// mgr.CreateService and the argv slice pass through unchanged.
//
// Why user scope only on Linux: a system unit file must be root-owned and
// outside every user-writable directory, or its owner can edit it into a root
// service. The confinement rule (§2.3) puts every file the runner writes under
// the project directory, so a system unit linked from there would be exactly
// that hole. A system-scope install needs a design ruling (a root-owned unit
// directory outside the project) and is not in this row. A Windows service
// always installs into the machine's service database and therefore needs an
// elevated prompt; for the same reason it never runs from the project. Install
// stages a release into an admin-only home under Program Files (servicehome.go:
// the runner, the distribution's libs and config, the plugins and a re-validated
// worker file), checks that only administrators can change the release, the
// home, the JDK and every directory above them, and points the service at that
// release; the service runs as its own virtual account (NT SERVICE\<service>,
// never LocalSystem) with a token that keeps one privilege (winsvc.go,
// serviceAccountPrefix). A change under the project takes effect at the next
// service_install. Row U8e documents the account and where to install.
//
// The unit file is written to systemd/<unit> under the project directory and
// linked into the user's unit search path by `systemctl --user enable
// <absolute path>`. On Windows the project holds services/<name>.txt, a marker
// of the definition install recorded (the record install trusts is the one in
// the home). The distribution, the plugin directory and the unit and services
// directories are registered as mount directories, so file_write and
// file_delete can never change what a service executes or loads.
//
// Wiring (one line outside this package, after ops.New(cfg), in place of
// compose.Register for a room whose SetupRequest.Target is "host"):
//
//	drv, err := host.Register(reg, host.Options{Class: class})
//
// compose.Register and host.Register are exclusive in one registry: both
// targets name their properties op properties_write (design §5), and a room
// has one target.
package host

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// Fixed project layout of a host target. Every path the unit or a worker file
// carries is one of these under the project directory; no op param names a
// path the unit executes.
const (
	// DistDir is where the Kafka distribution lives under the project
	// directory; ExecStart runs a script from its bin/.
	DistDir = "kafka"
	// PluginDir is the Connect plugin directory, the same name the Compose
	// target mounts (compose.PluginDir), so one plugin layout serves both.
	PluginDir = "connect-plugins"
	// UnitDir holds the rendered unit files.
	UnitDir = "systemd"
	// LogDir is the worker's LOG_DIR.
	LogDir = "logs"
	// OffsetsFile is a standalone worker's default offset file.
	OffsetsFile = "connect.offsets"
	// UnitPrefix starts every unit this driver names: an op can only ever
	// reach a piperoom unit, never another service on the host.
	UnitPrefix = "piperoom-"
	// DefaultService is the service name when the params name none.
	DefaultService = "connect"

	// BinDir holds, in a Windows service's release, the copy of
	// piperoom-setup.exe the service control manager runs (row U8b).
	BinDir = "bin"
	// ServiceDir holds the markers of the Windows service definitions
	// service_install records (the unit file's counterpart).
	ServiceDir = "services"
	// RunnerExe is that copy's fixed name.
	RunnerExe = "piperoom-setup.exe"
	// RunServiceSubcommand is the subcommand the installed service runs.
	// setupcmd.Run dispatches it before it parses the runner's flags, and
	// service_install refuses a binary that is not piperoom-setup, because
	// only that binary answers it (file: servicehost.go).
	RunServiceSubcommand = "run-service"

	hostTarget = "host"
	hostLayer  = "environment"
)

// Worker modes: the script ExecStart runs and the worker file it reads.
const (
	ModeDistributed = "distributed"
	ModeStandalone  = "standalone"
)

// Errors callers branch on with errors.Is.
var (
	// ErrParams: params do not decode, carry an unknown field or fail a bound.
	ErrParams = errors.New("host: invalid params")
	// ErrPath is the §2.3 path rule (ops.ErrPath).
	ErrPath = ops.ErrPath
	// ErrProperties: a properties file, key or value is refused.
	ErrProperties = errors.New("host: properties refused")
	// ErrSecretValue: a value looks like a credential. The value is in no
	// error, summary or Data.
	ErrSecretValue = errors.New("host: credential-looking value")
	// ErrDetect: a path the unit needs (the distribution, the worker file,
	// the JDK, the project directory) is missing or not safe to put in it.
	ErrDetect = errors.New("host: detection failed")
	// ErrUnsupported: this build has no service manager for the target's OS
	// (the Windows binding is behind a build tag; a test that forces
	// Options.GOOS="windows" elsewhere reaches this).
	ErrUnsupported = errors.New("host: service ops are not supported on this OS")
	// ErrServiceManager: a service control manager call failed or was refused
	// (no elevated prompt, the service already exists, it is marked for
	// deletion). The underlying error is wrapped too.
	ErrServiceManager = errors.New("host: service manager refused")
)

// Options configures a Driver.
type Options struct {
	// Class is the host target's class (protocol.ClassLocal, ClassShared,
	// ClassProduction); "" means local. Every class's worker gets a
	// loopback REST listener by default; a local worker's must bind
	// loopback, a raised class may name another one explicitly.
	Class string
	// GOOS is the platform the runner is on; "" means runtime.GOOS. Tests
	// pass another one to check the unsupported path.
	GOOS string
	// ResolvePath resolves the symlinks of the java found on PATH, so the
	// unit pins the JDK that java_version reported. nil means
	// filepath.EvalSymlinks; tests pass a fake.
	ResolvePath func(string) (string, error)
}

// Driver holds the target's options and the registry it registered with.
type Driver struct {
	reg     *ops.Registry
	class   string
	mgr     serviceManager
	goos    string
	resolve func(string) (string, error)
}

// Register adds the host ops to r and registers the directories the service
// executes or loads from as mount directories, so the generic file ops refuse
// them from the start.
func Register(r *ops.Registry, opts Options) (*Driver, error) {
	if r == nil {
		return nil, errors.New("host: a registry is required")
	}
	switch opts.Class {
	case "", protocol.ClassLocal, protocol.ClassShared, protocol.ClassProduction:
	default:
		return nil, fmt.Errorf("host: unknown target class %q", opts.Class)
	}
	d := &Driver{reg: r, class: opts.Class, goos: opts.GOOS, resolve: opts.ResolvePath}
	if d.goos == "" {
		d.goos = runtime.GOOS
	}
	if d.resolve == nil {
		d.resolve = filepath.EvalSymlinks
	}
	switch d.goos {
	case "linux":
		d.mgr = systemdUser{resolve: d.resolve}
	case "windows":
		d.mgr = newWindowsManager(defaultServiceAPI, os.Executable, defaultProgramFiles, defaultServiceACL(), defaultTrust())
	}
	for _, op := range []ops.Op{
		serviceStatusOp{d: d}, propertiesReadOp{d: d}, propertiesWriteOp{d: d},
		serviceInstallOp{d: d}, serviceRestartOp{d: d}, JournalTailOp{},
	} {
		if err := r.Register(op); err != nil {
			return nil, fmt.Errorf("host: registering %s: %w", op.Name(), err)
		}
	}
	dirs := []string{DistDir, PluginDir, UnitDir}
	if d.goos == "windows" {
		// install stages the distribution and the plugins into the service's
		// release and marks services/<name>.txt: none may become file_write's
		// target (the release itself is outside the project).
		dirs = []string{DistDir, PluginDir, ServiceDir}
	}
	for _, dir := range dirs {
		if err := r.RegisterMountDir(dir); err != nil {
			return nil, fmt.Errorf("host: registering %s as a mount directory: %w", dir, err)
		}
	}
	return d, nil
}

// classOrDefault is the target class, local when unset.
func (d *Driver) classOrDefault() string {
	if d.class == "" {
		return protocol.ClassLocal
	}
	return d.class
}

// manager returns the platform's service manager or ErrUnsupported.
func (d *Driver) manager() (serviceManager, error) {
	if d.mgr == nil {
		return nil, fmt.Errorf("%w (%s: this target has a systemd unit on Linux and a service control manager service on Windows)", ErrUnsupported, d.goos)
	}
	return d.mgr, nil
}

// maxQuoted bounds peer text quoted in an error or summary.
const maxQuoted = 120

// q quotes peer text for an error: valid UTF-8, no control characters, at
// most maxQuoted bytes, then strconv.Quote, so a refusal cannot carry a
// terminal escape or a forged line onto an approver's screen.
func q(s string) string { return strconv.Quote(clipText(s, maxQuoted)) }
