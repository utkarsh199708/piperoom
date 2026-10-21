package owners

import (
	"fmt"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
)

// SetupSpec is the optional per-pipeline `setup:` block written by
// pipeline_register (docs/design/setup-agent.md §3.9): where the pipeline was
// set up from and who watches it. It is informational for the server; Load
// validates it so a typo in the written file stops the next start, not the
// room that wrote it.
type SetupSpec struct {
	// Target is the target kind the setup ran against, one of
	// events.SetupRequest.Target's values (compose, host, strimzi, msk,
	// confluent, aiven, dbzserver, embedded).
	Target string `yaml:"target"`
	// RunnerLabel is the label of the runner that executed the setup.
	RunnerLabel string `yaml:"runner_label"`
	// RoomID is the setup room's id.
	RoomID string `yaml:"room_id"`
	// WatchedBy is events.WatchedByServer or events.WatchedByLocal: who reads
	// the pipeline now that it is registered.
	WatchedBy string `yaml:"watched_by"`
}

// setupTargets are the target kinds a setup: block may name. They match the
// values events.SetupRequest.Target documents; refusing any other value keeps
// a mis-written file from loading silently.
var setupTargets = []string{"compose", "host", "strimzi", "msk", "confluent", "aiven", "dbzserver", "embedded"}

// UnmarshalYAML rejects unknown keys in the setup block, like WatchSpec: a
// typo such as `watched:` would otherwise silently drop who watches the
// pipeline.
func (s *SetupSpec) UnmarshalYAML(node *yaml.Node) error {
	if err := checkKeys(node, "setup", "target", "runner_label", "room_id", "watched_by"); err != nil {
		return err
	}
	type plain SetupSpec
	return node.Decode((*plain)(s))
}

// Setup returns the pipeline's setup: block, or false when it has none.
func (p Pipeline) Setup() (*SetupSpec, bool) {
	return p.SetupSpec, p.SetupSpec != nil
}

// validateSetup checks a pipeline's setup: block. The errors do not name the
// pipeline; the caller adds it. A pipeline with no block is fine.
func (p Pipeline) validateSetup() error {
	s := p.SetupSpec
	if s == nil {
		return nil
	}
	if strings.TrimSpace(s.Target) == "" {
		return fmt.Errorf("setup: target is required")
	}
	if !slices.Contains(setupTargets, s.Target) {
		return fmt.Errorf("setup: target %q: want one of %s", s.Target, strings.Join(setupTargets, ", "))
	}
	switch s.WatchedBy {
	case events.WatchedByServer, events.WatchedByLocal:
	default:
		return fmt.Errorf("setup: watched_by %q: want %q or %q", s.WatchedBy, events.WatchedByServer, events.WatchedByLocal)
	}
	return nil
}
