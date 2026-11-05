// Package compose is the Compose target of the setup runner (rows U1-3a and
// U1-3b, docs/design/setup-agent.md §2.5): the four service templates the
// driver renders with pinned images from internal/setup/knowledge, the
// validator every render and every Compose file the runner is pointed at must
// pass, and the catalogue ops of this target — compose_write and
// properties_write (T1); the reads docker_info, compose_ps, compose_config
// (masked), container_inspect (T0, env names only) and container_logs (T0,
// row U5a: the last 200 lines, capped and redacted); the stack writes
// compose_up, compose_restart and compose_down (T1, volumes always kept); and
// plugin_install (T1), which downloads one connector archive from an
// allow-listed host and verifies the digest the knowledge index pins.
//
// The S1 rule of §2.5 is what makes a Compose file T1: the model never writes
// one. compose_write takes a template name and typed params (templates, a
// project name, host ports), renders the YAML itself, and the result is
// validated before it is written. properties_write takes an allow-listed file
// kind and allow-listed keys, and refuses any credential in plain text: a
// credential arrives as a ${secret:name} reference, which the runner's secret
// store materialises later (U1-4), never as a value from a proposal.
//
// Wiring (one line, outside this package, after ops.New(cfg)):
//
//	drv, err := compose.Register(reg, compose.Options{Class: class, PluginHosts: setup.PluginHosts})
//
// where class is the class of the Compose target the room resolved and
// PluginHosts is config.ParseSetup(cfg).PluginHosts. Register adds the ops to
// reg and adds the plugin directory to reg's mount list, so file_write refuses
// to write into it; every later render registers the directories it mounts the
// same way.
//
// Every Docker command goes through ops.Env.Exec, the injectable command
// runner the catalogue already carries: unit tests replace it with a fake and
// no Docker is needed, and an op never builds a shell string.
//
// Files this package writes go through the U1-2 path rule (ops.Confiner, the
// design's §2.3 lexical check plus the Lstat walk) and then through an
// os.Root at the project directory, exactly as the file ops do: no write
// leaves the project root, and a symlink, junction or reparse point on the
// way is refused.
package compose

import (
	"errors"
	"fmt"
	"net/http"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// Template names accepted by compose_write. Each renders one service, except
// that the connect and registry templates require kafka-kraft in the same
// render so the stack is usable on its own.
const (
	TemplateKafka    = "kafka-kraft"
	TemplateConnect  = "connect"
	TemplateRegistry = "registry"
	TemplatePostgres = "source-postgres"
	// TemplateSinkPostgres is the dev warehouse (row U3c): a second Postgres,
	// service "warehouse", that a JDBC sink connector writes into.
	TemplateSinkPostgres = "sink-postgres"
)

// Fixed names and defaults of a rendered stack.
const (
	// DefaultProject is the Compose project name when the params name none.
	DefaultProject = "piperoom-setup"
	// ProjectPrefix starts every project name this driver renders or runs:
	// a compose file can only ever name a piperoom stack, never another
	// project on the host.
	ProjectPrefix = "piperoom-"
	// DefaultFile is the Compose file compose_write writes when the params
	// name none. compose_write accepts the other compose names too
	// (compose.yml, compose.override.yaml, docker-compose*.yml).
	DefaultFile = "compose.yaml"
	// EnvFileName is the one env file the pinned compose argv reads
	// (Argv). properties_write treats it as the "env" file kind.
	EnvFileName = ".env"
	// PluginDir is the project-relative Connect plugin directory every
	// connect render mounts. It is in the ops mount list from Register on,
	// so file_write can never write into it: plugin_install (U1-3b) is the
	// op that populates it.
	PluginDir = "connect-plugins"
	// PluginDirContainer is where PluginDir is mounted in the Connect
	// image: the Debezium images' KAFKA_CONNECT_PLUGINS_DIR.
	PluginDirContainer = "/kafka/connect"

	targetKind = "compose"
	layerName  = "environment"
)

// Options configures a Driver.
type Options struct {
	// Class is the class of the Compose target (protocol.ClassLocal,
	// ClassShared, ClassProduction); "" means local. A local stack publishes
	// every port on 127.0.0.1; a class the person raised publishes on the
	// interface the target exists for, and the validator accepts that form
	// only for a raised class.
	Class string
	// PluginHosts are the hosts plugin_install may download a connector
	// archive from (PIPEROOM_SETUP_PLUGIN_HOSTS; an entry is a host name, or
	// host:port). Empty means the documented default,
	// config.DefaultSetupPluginHosts; the wiring passes the parsed config.
	PluginHosts []string
	// PluginArtifact resolves a Debezium release and connector to the archive
	// URL, its pinned sha512 digest and the archive's one top-level directory.
	// nil resolves through internal/setup/knowledge; it is a seam for tests
	// that serve an archive from a local server.
	PluginArtifact PluginResolver
	// HTTPClient overrides the plugin download client (tests). nil builds the
	// guarded one: no proxy, no redirects, a timeout, and a dial hook that
	// refuses link-local and metadata addresses.
	HTTPClient *http.Client
	// GOOS and Getenv are the platform the runner thinks it is on, for the
	// Docker Desktop differences (socket path, host networking). nil means
	// runtime.GOOS and os.Getenv; tests pass fakes.
	GOOS   string
	Getenv func(string) string
}

// Driver renders templates and registers this package's ops.
type Driver struct {
	reg   *ops.Registry
	class string

	pluginHosts    []string
	pluginArtifact PluginResolver
	httpClient     *http.Client
	goos           string
	getenv         func(string) string
}

// Register adds the Compose read (T0) and write (T1) ops to r and returns the
// driver that renders for it. The mount list starts with the Connect plugin
// directory, so the U1-2 file ops refuse it before the first render;
// plugin_install is the one writer that fills it.
func Register(r *ops.Registry, opts Options) (*Driver, error) {
	if r == nil {
		return nil, errors.New("compose: a registry is required")
	}
	switch opts.Class {
	case "", protocol.ClassLocal, protocol.ClassShared, protocol.ClassProduction:
	default:
		return nil, fmt.Errorf("compose: unknown target class %q", opts.Class)
	}
	hosts, err := normalizePluginHosts(opts.PluginHosts)
	if err != nil {
		return nil, err
	}
	d := &Driver{
		reg:            r,
		class:          opts.Class,
		pluginHosts:    hosts,
		pluginArtifact: opts.PluginArtifact,
		httpClient:     opts.HTTPClient,
		goos:           opts.GOOS,
		getenv:         opts.Getenv,
	}
	writes := []ops.Op{
		composeWriteOp{d: d}, propertiesWriteOp{d: d},
		&stackWriteOp{d: d, name: "compose_up", sub: "up", flags: []string{"-d"},
			describe: "start the rendered Compose stack in the background"},
		&stackWriteOp{d: d, name: "compose_restart", sub: "restart", flags: nil,
			describe: "restart the services of the rendered Compose stack"},
		&stackWriteOp{d: d, name: "compose_down", sub: "down", flags: nil, down: true,
			describe: "stop and remove the rendered stack's containers; volumes are kept"},
		pluginInstallOp{d: d},
	}
	reads := []ops.Op{
		dockerInfoOp{d: d}, composePSOp{d: d}, composeConfigOp{d: d}, containerInspectOp{d: d},
		containerLogsOp{d: d},
	}
	for _, op := range append(reads, writes...) {
		if err := r.Register(op); err != nil {
			return nil, fmt.Errorf("compose: registering %s: %w", op.Name(), err)
		}
	}
	if err := r.RegisterMountDir(PluginDir); err != nil {
		return nil, fmt.Errorf("compose: registering the plugin directory: %w", err)
	}
	return d, nil
}

// class is the target class, defaulting to local.
func (d *Driver) classOrDefault() string {
	if d == nil || d.class == "" {
		return protocol.ClassLocal
	}
	return d.class
}

// loopbackOnly reports whether the class requires every published port to be
// bound to 127.0.0.1 (design §2.5: local targets do, a raised class does not).
func loopbackOnly(class string) bool {
	return class != protocol.ClassShared && class != protocol.ClassProduction
}
