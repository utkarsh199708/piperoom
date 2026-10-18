// Package owners loads and validates the pipeline ownership config
// (dev/owners.yaml): which source tables, connectors, consumer groups and
// sink tables form each pipeline, and which user owns each layer.
package owners

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

// Pipeline is one named data pipeline.
type Pipeline struct {
	ID    string `yaml:"id"`
	Title string `yaml:"title"`
	// TopicPrefix is the Debezium topic prefix; pipeline topics are named
	// "<TopicPrefix>.<schema>.<table>".
	TopicPrefix    string   `yaml:"topic_prefix"`
	SourceTables   []string `yaml:"source_tables"`
	Connectors     []string `yaml:"connectors"`
	ConsumerGroups []string `yaml:"consumer_groups"`
	SinkTables     []string `yaml:"sink_tables"`
	SinkTSColumn   string   `yaml:"sink_ts_column"`
	// WatchSpec is the optional `watch:` block (see watch.go). Read the
	// validated values with Watch().
	WatchSpec *WatchSpec `yaml:"watch"`
	// ShadowSpec is the optional `shadow:` block (see shadow.go). Read it with
	// Shadow().
	ShadowSpec *ShadowSpec `yaml:"shadow"`
	// Owners is the optional per-pipeline `owners:` block. A layer listed here
	// replaces the top-level `layers:` entry for this pipeline.
	Owners map[string]LayerRights `yaml:"owners"`
	// AutoApprove is the optional `auto_approve:` list (tasks/P0-contracts.md
	// item 3): names of actions whose proposals this pipeline lets run after a
	// cancellable countdown with no human approval. Empty or absent = none, so a
	// pipeline opts in action by action and never by default. Load checks the
	// syntax; LoadWithActions and ValidateAutoApprove also check the names
	// against the action registry. Read it with AutoApproveList.
	AutoApprove []string `yaml:"auto_approve"`
	// RetentionDays is the optional `retention_days:` of the pipeline: closed
	// rooms of this pipeline are deleted after this many days; with
	// PIPEROOM_RETENTION_DAYS also set, the shorter wins
	// (docs/design/retention.md section 3). nil = not set. Load refuses a value
	// outside 1..MaxRetentionDays. Read it with Retention().
	RetentionDays *WholeDays `yaml:"retention_days"`
	// Tenant is the optional tenant (docs/design/tenancy.md); read it with
	// TenantName, which applies the default.
	Tenant string `yaml:"tenant"`
	// Connections is the optional `connections:` block (tenancy.go): where
	// this pipeline's DSNs, URLs and logins come from. Nil = the process-wide
	// settings.
	Connections Connections `yaml:"connections"`
	// SharedConnections allows a connection reference of this pipeline to be
	// used by another tenant too (CheckSharedRefs).
	SharedConnections bool `yaml:"shared_connections"`

	// watch is WatchSpec after validation, with defaults applied. Set by Load.
	watch *WatchConfig
}

// MaxRetentionDays bounds a pipeline's retention_days (100 years), the same
// bound as PIPEROOM_RETENTION_DAYS (store.MaxRetentionDays).
const MaxRetentionDays = 36500

// WholeDays is a number of days written as a YAML integer. yaml.v3 would
// otherwise turn 1.5 into 1 without a word; a retention policy must never be
// rounded silently.
type WholeDays int

// UnmarshalYAML accepts only a plain integer scalar.
func (d *WholeDays) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!int" {
		return fmt.Errorf("line %d: retention_days: want a whole number of days, got %q", node.Line, node.Value)
	}
	var n int
	if err := node.Decode(&n); err != nil {
		return fmt.Errorf("line %d: retention_days: want a whole number of days: %w", node.Line, err)
	}
	*d = WholeDays(n)
	return nil
}

// Retention returns the pipeline's retention_days, if set.
func (p Pipeline) Retention() (int, bool) {
	if p.RetentionDays == nil {
		return 0, false
	}
	return int(*p.RetentionDays), true
}

// Owners is the full ownership config.
//
// Layers maps a layer to its default approvers. Each value is a user id (the
// original form), or a list of `user:<id>` / `group:<name>` principals. A
// pipeline's own `owners:` block overrides a layer for that pipeline. Auditors
// may export (docs/design/auth.md sections 2.3 and 3.4). See rights.go for the
// functions that answer "may this session do X".
type Owners struct {
	Pipelines []Pipeline            `yaml:"pipelines"`
	Layers    map[string]Principals `yaml:"layers"`
	Auditors  Principals            `yaml:"auditors"`
	// Tenants is the optional tenants: block (tenancy.go). Present, it turns
	// the tenant filter on.
	Tenants map[string]Tenant `yaml:"tenants"`
}

// Load reads and validates the owners file at path. An `auto_approve:` entry
// is checked for syntax only, so the file loads before the action registry
// exists (the CLI tools do); a server that has the registry calls
// LoadWithActions, or ValidateAutoApprove after Load.
func Load(path string) (*Owners, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("owners: read %s: %w", path, err)
	}
	var o Owners
	// Unknown keys are refused. A pipeline `owner:` (for `owners:`) would
	// otherwise be dropped and the layer would fall back to the top-level
	// owners: a typo that widens who may approve (security audit 2026-10,
	// A-OWN-1).
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&o); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("owners: parse %s: %w", path, err)
	}
	// The decode above succeeded, so this one parses the same document; it
	// only finds keys that are present with a null value (tenancy.go).
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("owners: parse %s: %w", path, err)
	}
	if err := checkNullBlocks(&root); err != nil {
		return nil, fmt.Errorf("owners: validate %s: %w", path, err)
	}
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("owners: validate %s: %w", path, err)
	}
	return &o, nil
}

// firstDuplicate returns the first name that appears twice in names, or "".
func firstDuplicate(names []string) string {
	seen := make(map[string]bool, len(names))
	for _, n := range names {
		if seen[n] {
			return n
		}
		seen[n] = true
	}
	return ""
}

// Pipeline returns the pipeline with the given id.
func (o *Owners) Pipeline(id string) (Pipeline, bool) {
	for _, p := range o.Pipelines {
		if p.ID == id {
			return p, true
		}
	}
	return Pipeline{}, false
}

func (o *Owners) validate() error {
	if err := o.validateRights(); err != nil {
		return err
	}
	seen := map[string]bool{}
	for i := range o.Pipelines {
		p := o.Pipelines[i]
		if p.ID == "" {
			return fmt.Errorf("pipeline with empty id")
		}
		if seen[p.ID] {
			return fmt.Errorf("duplicate pipeline id %q", p.ID)
		}
		seen[p.ID] = true
		if p.TopicPrefix == "" {
			return fmt.Errorf("pipeline %q: topic_prefix is required", p.ID)
		}
		if len(p.SourceTables) == 0 {
			return fmt.Errorf("pipeline %q: no source tables", p.ID)
		}
		if len(p.Connectors) == 0 {
			return fmt.Errorf("pipeline %q: no connectors", p.ID)
		}
		for _, l := range []struct {
			name  string
			names []string
		}{
			{"source_tables", p.SourceTables}, {"connectors", p.Connectors},
			{"consumer_groups", p.ConsumerGroups}, {"sink_tables", p.SinkTables},
		} {
			// A repeated name becomes two graph nodes with one id.
			if d := firstDuplicate(l.names); d != "" {
				return fmt.Errorf("pipeline %q: %s lists %q twice", p.ID, l.name, d)
			}
		}
		if len(p.SinkTables) > 0 && p.SinkTSColumn == "" {
			return fmt.Errorf("pipeline %q: sink_ts_column is required when sink_tables is set", p.ID)
		}
		if d := p.RetentionDays; d != nil && (*d < 1 || *d > MaxRetentionDays) {
			return fmt.Errorf("pipeline %q: retention_days %d: want 1 to %d (leave it out to use PIPEROOM_RETENTION_DAYS)", p.ID, *d, MaxRetentionDays)
		}
		if err := checkAutoApproveSyntax(p.AutoApprove); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		w, err := resolveWatch(p.WatchSpec)
		if err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		if err := resolveDLQTopics(p, &w); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		o.Pipelines[i].watch = &w
	}
	if err := o.validateDLQ(); err != nil {
		return err
	}
	return o.validateTenancy()
}
