// Package server is the Debezium Server target of the setup runner (row U11a,
// docs/design/setup-agent.md §2.5 and §5): the application.properties builder
// for the sinks the knowledge index knows (Kinesis, Pub/Sub, Redis, HTTP and
// Kafka among them), the server as one Compose service with the pinned image
// of internal/setup/knowledge, the same server as a systemd unit rendered
// through host.RenderUnit, and server_status (T0), which reads either the
// Compose service or the user-level unit.
//
// Ops: server_properties_write (T1), server_compose_write (T1),
// server_unit_write (T2) and server_status (T0). The ops are named with the
// server_ prefix because a dbzserver room registers this driver next to the
// Compose driver (compose_write, compose_up, compose_ps, compose_down), whose
// op names are already taken.
//
// The S1 rule of §2.5 for the properties file: the sink keys are not a list
// in this package. server_properties_write resolves the sink in the SMT and
// sink index (internal/setup/knowledge, index/sink-server.yaml), refuses any
// key the index does not carry for that sink, refuses a missing key the index
// marks required, checks each value against the key's allowed values, and
// accepts a ${secret:name} reference only on a key the index marks secret.
// The source half is held to a per-connector allow-list (sourceKeyTables in
// properties.go): only the documented database and connector keys are
// accepted, a credential-named key must be a ${secret:name} reference, file
// paths may only name data/, hosts are checked like any other host (metadata
// addresses and names refused), and the transforms and predicates families are
// resolved against the same U3a index, so no SMT class or key outside the index
// reaches the embedded engine. The knowledge index does not carry connector
// keys yet; when a source driver lands (U9+), its ConnectorConfig replaces the
// fixed table.
//
// The S1 rule for the other two artifacts: the Compose file is rendered from
// a typed params set and passes compose.ValidateYAML before it is written (the
// image is the knowledge index's pinned debezium/server digest, the config
// directory is the one read-only bind, the offsets live in a named volume);
// the unit file is rendered by host.RenderUnit, whose template and safe-token
// checks row U8a already ships.
//
// Wiring (one line outside this package, after ops.New(cfg)):
//
//	drv, err := server.Register(reg, server.Options{Class: class, Mode: server.ModeCompose})
//
// For ModeCompose also register the Compose driver (compose.Register): this
// package renders and validates the stack, compose_up/compose_ps/compose_down
// start, read and stop it. For ModeService register only this driver; the unit
// is written under systemd/ and enabling it is a person's step (U11-R).
//
// External keys. Every Debezium Server key this package writes itself is cited
// in the code that writes it; the sink key sets come from the index entries,
// which cite the Maven Central sources jars of the 3.2.0.Final release of the
// pinned quay.io/debezium/server image. The pages read:
//
//   - https://debezium.io/documentation/reference/3.2/operations/debezium-server.html
//     (debezium.sink.type, debezium.source.*, the offset file, the formats,
//     the config/application.properties location and the MicroProfile
//     environment-variable source)
//   - https://raw.githubusercontent.com/debezium/docker-images/main/server/3.2/Dockerfile
//     (WORKDIR /debezium, config in /debezium/config, data in /debezium/data)
//   - https://raw.githubusercontent.com/debezium/debezium-server/3.2/debezium-server-dist/src/main/resources/distro/run.sh
//     (run.sh runs from the distribution directory)
//   - the connector pages under https://debezium.io/documentation/reference/3.2/connectors/
//     for the source connector classes in properties.go.
package server

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"piperoom/internal/setup/knowledge"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// Fixed project layout and names of this target.
const (
	// DistDir is the Debezium Server distribution under the project
	// directory. run.sh runs from its root (it lists its runner jar there and
	// sources ./jmx/enable_jmx.sh), so the unit's WorkingDirectory is
	// <project>/debezium and the Compose image's WORKDIR is /debezium.
	DistDir = "debezium"
	// ConfigDir is the distribution's config directory: where a Debezium
	// Server reads application.properties from (the Compose image mounts it at
	// /debezium/config).
	ConfigDir = "debezium/config"
	// ConfigFile is the one properties file this driver writes.
	ConfigFile = ConfigDir + "/application.properties"
	// DataDir holds the file offset store, a named volume in the Compose
	// render so a container re-creation keeps the offsets.
	DataDir = "debezium/data"
	// UnitDir holds the rendered unit file, as in the host target.
	UnitDir = "systemd"
	// DefaultService is the Compose service name and the unit name segment
	// when the params name none.
	DefaultService = "dbzserver"
	// UnitName is the unit file server_unit_write writes and server_status
	// reads in service mode: piperoom-<DefaultService>.service.
	UnitName = "piperoom-" + DefaultService + ".service"
	// ComposeFile is the file server_compose_write writes; the Compose
	// driver's compose_up/compose_ps read the same name.
	ComposeFile = "compose.yaml"
	// EnvFileName is the env file the Compose render interpolates; the Compose
	// driver's properties_write writes it.
	EnvFileName = ".env"
	// DefaultProject is the Compose project name when the params name none.
	DefaultProject = "piperoom-dbzserver"
	// ProjectPrefix starts every project name this driver accepts, as in the
	// Compose driver: a render can only ever name a piperoom stack.
	ProjectPrefix = "piperoom-"

	targetKind = "dbzserver"
	layerName  = "environment"
)

// Driver modes: the server as one Compose service, or as a user-level systemd
// unit on a plain host.
const (
	// ModeCompose runs the server as a container of the rendered stack.
	ModeCompose = "compose"
	// ModeService runs the server from a distribution under the project
	// directory through a systemd user unit.
	ModeService = "service"
)

// Errors callers branch on with errors.Is.
var (
	// ErrParams: params do not decode, carry an unknown field or fail a bound.
	ErrParams = errors.New("server: invalid params")
	// ErrPath is the §2.3 path rule (ops.ErrPath).
	ErrPath = ops.ErrPath
	// ErrProperties: the sink, a key, a value or the source is refused.
	ErrProperties = errors.New("server: properties refused")
	// ErrSecretValue: a credential arrived in plain text where only a
	// ${secret:name} reference is allowed. The value is in no error, summary
	// or Data.
	ErrSecretValue = errors.New("server: plain-text credential")
	// ErrDetect: a path the unit needs (the distribution's run.sh, the
	// properties file, the JDK, the project directory) is missing or not safe
	// to put in it.
	ErrDetect = errors.New("server: detection failed")
	// ErrUnsupported: the mode needs a service manager this OS does not have.
	ErrUnsupported = errors.New("server: this mode is not supported on this OS")
	// ErrStatus: the state read cannot run (no rendered stack, an unreadable
	// command output).
	ErrStatus = errors.New("server: the status read cannot run")
)

// Options configures a Driver.
type Options struct {
	// Class is the target class (protocol.ClassLocal, ClassShared,
	// ClassProduction); "" means local. A local target publishes every host
	// port on 127.0.0.1 and binds the server's config directory read-only.
	Class string
	// Mode is ModeCompose (default) or ModeService.
	Mode string
	// GOOS is the platform the runner is on; "" means runtime.GOOS. Tests
	// pass another one to check the unsupported path.
	GOOS string
	// ResolvePath resolves the symlinks of the java found on PATH, so the unit
	// pins the JDK that java_version reported. nil means
	// filepath.EvalSymlinks; tests pass a fake.
	ResolvePath func(string) (string, error)
	// Index overrides the SMT and sink index (tests). nil means the shipped
	// one, knowledge.LoadIndex().
	Index *knowledge.Index
}

// Driver holds what this target's ops share: the registry it registered with,
// the class and mode of the room's target, the knowledge index the sink
// allow-list comes from, and the pinned server image.
type Driver struct {
	reg   *ops.Registry
	class string
	mode  string
	ix    *knowledge.Index
	image knowledge.Image
	goos  string
	// resolve resolves symlinks for the unit's JAVA_HOME check.
	resolve func(string) (string, error)
}

// Register loads the knowledge index, resolves the pinned server image and
// adds this target's ops to r. The distribution and unit directories are
// registered as mount directories, so file_write refuses them from the start;
// this driver's own two writes (application.properties, the unit file) are the
// only ones that reach them.
func Register(r *ops.Registry, opts Options) (*Driver, error) {
	if r == nil {
		return nil, errors.New("server: a registry is required")
	}
	switch opts.Class {
	case "", protocol.ClassLocal, protocol.ClassShared, protocol.ClassProduction:
	default:
		return nil, fmt.Errorf("server: unknown target class %q", opts.Class)
	}
	mode := opts.Mode
	if mode == "" {
		mode = ModeCompose
	}
	if mode != ModeCompose && mode != ModeService {
		return nil, fmt.Errorf("server: unknown mode %q (compose or service)", opts.Mode)
	}
	ix := opts.Index
	if ix == nil {
		var err error
		ix, err = knowledge.LoadIndex()
		if err != nil {
			return nil, fmt.Errorf("server: loading the knowledge index: %w", err)
		}
	}
	image, err := knowledge.ImageByName("server")
	if err != nil {
		return nil, fmt.Errorf("server: resolving the pinned image: %w", err)
	}
	d := &Driver{reg: r, class: opts.Class, mode: mode, ix: ix, image: image, goos: opts.GOOS, resolve: opts.ResolvePath}
	if d.goos == "" {
		d.goos = runtime.GOOS
	}
	if d.resolve == nil {
		d.resolve = filepath.EvalSymlinks
	}
	for _, op := range []ops.Op{
		serverPropertiesWriteOp{d: d}, serverComposeWriteOp{d: d},
		serverUnitWriteOp{d: d}, serverStatusOp{d: d},
	} {
		if err := r.Register(op); err != nil {
			return nil, fmt.Errorf("server: registering %s: %w", op.Name(), err)
		}
	}
	for _, dir := range []string{DistDir, UnitDir} {
		if err := r.RegisterMountDir(dir); err != nil {
			return nil, fmt.Errorf("server: registering %s as a mount directory: %w", dir, err)
		}
	}
	return d, nil
}

// classOrDefault is the target class, local when unset.
func (d *Driver) classOrDefault() string {
	if d == nil || d.class == "" {
		return protocol.ClassLocal
	}
	return d.class
}

// loopbackOnly reports whether the class requires every published port to be
// bound to 127.0.0.1 (a local target does; a class the person raised does not).
func loopbackOnly(class string) bool {
	return class != protocol.ClassShared && class != protocol.ClassProduction
}

// maxQuoted bounds peer text quoted in an error or summary.
const maxQuoted = 120

// q quotes peer text for an error: valid UTF-8, no control characters, at
// most maxQuoted bytes, then strconv.Quote, so a refusal cannot carry a
// terminal escape or a forged line onto an approver's screen.
func q(s string) string { return strconv.Quote(clipText(s, maxQuoted)) }

// clipText is host or proposal text cut to max bytes with control characters
// replaced, on a rune boundary.
func clipText(s string, max int) string {
	s = strings.ToValidUTF8(s, "?")
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, s)
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && cut < len(s) && (s[cut]&0xC0) == 0x80 {
		cut--
	}
	return s[:cut] + "…"
}
