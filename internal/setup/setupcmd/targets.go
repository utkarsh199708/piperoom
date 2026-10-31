package setupcmd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/targets/compose"
	"piperoom/internal/setup/targets/host"
	"piperoom/internal/setup/targets/strimzi"
)

// targetRegistrar adds the target driver's ops to the runner's catalogue once
// the Welcome says what the room's target is (row U8f). compose.Register and
// host.Register are exclusive in one registry (both name properties_write and
// a room has one target), so exactly one of them runs, chosen by the target
// kinds the Welcome names, and only the first Welcome of the process registers
// anything: a reconnect re-sends the targets, and the catalogue it would
// register into already has the driver. A room whose target is a managed
// cloud (aws, confluent, aiven) gets that provider's driver instead
// (managed.go).
type targetRegistrar struct {
	mu   sync.Mutex
	done bool
	// err is the first Welcome's registration error, returned again on a
	// reconnect: a half-registered catalogue is never registered into twice.
	// An inner timeout (the AWS credential fetch's own deadline, while the
	// runner's ctx is alive) that left the catalogue untouched is retried
	// once inside Apply, because the runner treats any registration error
	// as terminal (runner.ErrRegisterTargets) and never calls Apply again
	// for that pairing. Only an error after the runner's own ctx ended is
	// not cached: the runner is stopping.
	err error
	// room is the room the drivers were registered for: they tag what they
	// create with it, so an Apply for another room is refused
	// (errOtherRoom) rather than answered with this room's drivers.
	room        string
	cat         *ops.Registry
	pluginHosts []string
	// prodPatterns are PIPEROOM_SETUP_PROD_PATTERNS: a managed target whose
	// account, environment, cluster or project matches one is production.
	prodPatterns []string
	// secrets opens the runner's secret store (runner.Runner.SecretStore):
	// the managed drivers materialise ${secret:} references and read the
	// Confluent and Aiven setup credentials from it. nil leaves them without
	// a store (their ops then refuse, naming the missing store).
	secrets storeOpener
	// clients builds the managed drivers' clients; the default is the real
	// ones (defaultManagedClients).
	clients managedClients
	// embedded is the --embedded flag (row U6b): the room's target is
	// embedded, which the Welcome cannot say yet (it carries no SetupRequest
	// target; the U6b PR's contract change request), so the person states it
	// on the runner's command line. It wins over the targets.
	embedded bool
	// strimzi is set in the in-cluster Job (--confirm-secret-prefix): a
	// kubernetes target then registers the Strimzi driver with these options.
	strimzi *strimzi.Options
}

// newTargetRegistrar wires cat as the catalogue the Welcome's target driver
// registers into. pluginHosts is PIPEROOM_SETUP_PLUGIN_HOSTS as parsed by
// parsePluginHosts; empty lets the compose driver fall back to its documented
// default.
func newTargetRegistrar(cat *ops.Registry, pluginHosts []string) *targetRegistrar {
	return &targetRegistrar{cat: cat, pluginHosts: pluginHosts, clients: defaultManagedClients()}
}

// Apply registers the driver the targets name. It has the signature of
// runner.Options.RegisterTargets and is idempotent: the first Welcome decides,
// later calls (reconnects) change nothing and return its error again. roomID
// tags what a managed driver creates; ctx bounds the AWS credential fetch.
//
// A registration is bound to its room: an Apply naming another room (a
// runner process re-paired with a second code) is refused with errOtherRoom,
// because the registered drivers tag and guard by the first room's id; the
// person restarts the runner to set up another room.
func (t *targetRegistrar) Apply(ctx context.Context, roomID string, targets []events.SetupTarget) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		if roomID != t.room {
			return fmt.Errorf("%w: this runner registered room %s's target drivers; restart it to pair with room %s", errOtherRoom, t.room, roomID)
		}
		return t.err
	}
	before := len(t.cat.Names())
	untouched := func() bool { return len(t.cat.Names()) == before }
	err := t.apply(ctx, roomID, targets)
	if err != nil && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) && untouched() {
		// One retry: a slow first credential refresh (SSO, IMDS) is the
		// usual cause, and the runner will not ask again.
		err = t.apply(ctx, roomID, targets)
	}
	if err != nil && ctx.Err() != nil && untouched() {
		// The runner is stopping; nothing registered, nothing to cache.
		return err
	}
	t.err, t.done, t.room = err, true, roomID
	return err
}

// errOtherRoom: Apply named a room other than the one the drivers were
// registered for.
var errOtherRoom = errors.New("setup: the runner is already set up for another room")

func (t *targetRegistrar) apply(ctx context.Context, roomID string, targets []events.SetupTarget) error {
	driver, class := chooseTargetDriver(targets)
	if t.embedded {
		driver = "embedded"
	}
	if driver == "" && t.strimzi != nil {
		if c, ok := kubernetesClass(targets); ok {
			driver, class = "kubernetes", c
		}
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
	case "kubernetes":
		o := *t.strimzi
		o.Class = class
		o.PluginHosts = t.pluginHosts
		if o.Secret == nil && t.secrets != nil {
			// k8s_secret_apply fills the database Secret from the Job's own
			// store (under the Job's /work/.piperoom/), the one secret_set
			// and the UI's secret frame write to.
			o.Secret = t.secrets.lookup
		}
		if _, err := strimzi.Register(t.cat, o); err != nil {
			return fmt.Errorf("setup: registering the Strimzi target: %w", err)
		}
	default:
		// A managed target. MSK, Confluent and Aiven register their cloud
		// ops here, with this machine's credentials; a kubernetes target
		// outside the in-cluster Job registers nothing (its writes run in
		// the Job), and this process keeps the generic file and env ops.
		if _, err := t.registerManaged(ctx, roomID, targets); err != nil {
			return err
		}
	}
	return nil
}

// managedKinds are the target kinds whose driver is not a local one: a room
// with one of them gets neither the compose nor the host driver, but the
// Strimzi driver in the in-cluster Job or a managed cloud driver
// (registerManaged).
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

// kubernetesClass is the class the Strimzi driver runs at, and whether the
// room has a kubernetes target at all: the highest class of every target the
// room names, not of the kubernetes targets only. The driver's production
// refusals (the canary topic, a connector delete, the Connect cluster and the
// credentials Secret) protect what the Kafka cluster and the source database
// hold, so a production kafka or postgres target makes the driver production
// even when the namespace's own target is shared. A local class stays ""
// (shared): a cluster is never local (strimzi.Register refuses it).
func kubernetesClass(targets []events.SetupTarget) (string, bool) {
	class, found := "", false
	for _, t := range targets {
		if t.Kind == "kubernetes" {
			found = true
		}
		if classRank(t.Class) > classRank(class) {
			class = t.Class
		}
	}
	if !found {
		return "", false
	}
	return class, true
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
func parsePluginHosts(raw string) []string { return splitList(raw) }

// parseProdPatterns splits PIPEROOM_SETUP_PROD_PATTERNS the same way the
// server's config.ParseSetup does, so the runner classes a managed target
// with the patterns the server classes it with.
func parseProdPatterns(raw string) []string { return splitList(raw) }

// splitList is a comma list, entries trimmed, empty entries dropped.
func splitList(raw string) []string {
	var out []string
	for _, h := range strings.Split(raw, ",") {
		if h = strings.TrimSpace(h); h != "" {
			out = append(out, h)
		}
	}
	return out
}
