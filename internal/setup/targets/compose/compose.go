// Package compose is the Compose target of the setup runner (row U1-3a,
// docs/design/setup-agent.md §2.5): the four service templates the driver
// renders with pinned images from internal/setup/knowledge, the validator
// every render and every Compose file the runner is pointed at must pass, and
// the two write ops of this row — compose_write and properties_write —
// registered into the U1-2 operation catalogue (internal/setup/ops).
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
//	drv, err := compose.Register(reg, compose.Options{Class: class})
//
// where class is the class of the Compose target the room resolved. Register
// adds compose_write and properties_write to reg and adds the plugin
// directory to reg's mount list, so file_write refuses to write into it; every
// later render registers the directories it mounts the same way.
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
)

// Fixed names and defaults of a rendered stack.
const (
	// DefaultProject is the Compose project name when the params name none.
	DefaultProject = "piperoom-setup"
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
}

// Driver renders templates and registers this package's ops.
type Driver struct {
	reg   *ops.Registry
	class string
}

// Register adds compose_write and properties_write to r and returns the
// driver that renders for it. The mount list starts with the Connect plugin
// directory, so the U1-2 file ops refuse it before the first render.
func Register(r *ops.Registry, opts Options) (*Driver, error) {
	if r == nil {
		return nil, errors.New("compose: a registry is required")
	}
	switch opts.Class {
	case "", protocol.ClassLocal, protocol.ClassShared, protocol.ClassProduction:
	default:
		return nil, fmt.Errorf("compose: unknown target class %q", opts.Class)
	}
	d := &Driver{reg: r, class: opts.Class}
	for _, op := range []ops.Op{composeWriteOp{d: d}, propertiesWriteOp{d: d}} {
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
