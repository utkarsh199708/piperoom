package setupcmd

import (
	"fmt"
	"strings"
	"sync"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/targets/compose"
	"piperoom/internal/setup/targets/host"
)

// targetRegistrar adds the target driver's ops to the runner's catalogue once
// the Welcome says what the room's target is (row U8f). compose.Register and
// host.Register are exclusive in one registry (both name properties_write and
// a room has one target), so exactly one of them runs, chosen by the target
// kinds the Welcome names, and only the first Welcome of the process registers
// anything: a reconnect re-sends the targets, and the catalogue it would
// register into already has the driver.
type targetRegistrar struct {
	mu          sync.Mutex
	done        bool
	cat         *ops.Registry
	pluginHosts []string
	// embedded is the --embedded flag (row U6b): the room's target is
	// embedded, which the Welcome cannot say yet (it carries no SetupRequest
	// target; the U6b PR's contract change request), so the person states it
	// on the runner's command line. It wins over the targets.
	embedded bool
}

// newTargetRegistrar wires cat as the catalogue the Welcome's target driver
// registers into. pluginHosts is PIPEROOM_SETUP_PLUGIN_HOSTS as parsed by
// parsePluginHosts; empty lets the compose driver fall back to its documented
// default.
func newTargetRegistrar(cat *ops.Registry, pluginHosts []string) *targetRegistrar {
	return &targetRegistrar{cat: cat, pluginHosts: pluginHosts}
}

// Apply registers the driver the targets name. It has the signature of
// runner.Options.RegisterTargets and is idempotent: the first Welcome decides,
// later calls (reconnects) change nothing.
func (t *targetRegistrar) Apply(targets []events.SetupTarget) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return nil
	}
	driver, class := chooseTargetDriver(targets)
	if t.embedded {
		driver = "embedded"
	}
	switch driver {
	case "embedded":
		if err := ops.RegisterEmbedded(t.cat); err != nil {
			return fmt.Errorf("setup: registering the embedded target: %w", err)
		}
	case "host":
		if _, err := host.Register(t.cat, host.Options{Class: class}); err != nil {
			return fmt.Errorf("setup: registering the host target: %w", err)
		}
	case "compose":
		if _, err := compose.Register(t.cat, compose.Options{Class: class, PluginHosts: t.pluginHosts}); err != nil {
			return fmt.Errorf("setup: registering the compose target: %w", err)
		}
	default:
		// A managed target (Strimzi, MSK, Confluent, Aiven): its writes run
		// on another runner shape (the in-cluster Job, the cloud ops); this
		// process keeps the generic file and env ops only.
	}
	t.done = true
	return nil
}

// managedKinds are the target kinds whose driver does not run in this
// process: a room with one of them gets neither local driver.
var managedKinds = map[string]bool{
	"kubernetes": true, "aws": true, "confluent": true, "aiven": true,
}

// chooseTargetDriver picks the local driver the targets name: "host" when a
// host target is named, "" when a managed target is, and "compose" otherwise
// (the quickstart default, also when the Welcome names no targets at all).
// The host driver's class is the highest class of the host targets; the
// compose stack is always rendered loopback-only (class local), because no
// Welcome target describes the stack the runner renders — raising the class
// there would let a rendered stack publish on a non-loopback interface.
func chooseTargetDriver(targets []events.SetupTarget) (driver, class string) {
	hostClass := ""
	hasHost, hasManaged := false, false
	for _, t := range targets {
		switch {
		case t.Kind == "host":
			hasHost = true
			if classRank(t.Class) > classRank(hostClass) {
				hostClass = t.Class
			}
		case managedKinds[t.Kind]:
			hasManaged = true
		}
	}
	switch {
	case hasHost:
		return "host", hostClass
	case hasManaged:
		return "", ""
	default:
		return "compose", ""
	}
}

// classRank orders classes local < shared < production; an unknown or empty
// class ranks as local (the runner's floors do the same).
func classRank(c string) int {
	switch c {
	case "production":
		return 2
	case "shared":
		return 1
	default:
		return 0
	}
}

// parsePluginHosts splits PIPEROOM_SETUP_PLUGIN_HOSTS the way the server's
// config.ParseSetup does: a comma list, entries trimmed, empty entries
// dropped. An empty result lets the compose driver use its documented
// default (config.DefaultSetupPluginHosts).
func parsePluginHosts(raw string) []string {
	var hosts []string
	for _, h := range strings.Split(raw, ",") {
		if h = strings.TrimSpace(h); h != "" {
			hosts = append(hosts, h)
		}
	}
	return hosts
}
