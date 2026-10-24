// Package owners loads and validates the pipeline ownership config
// (dev/owners.yaml): which source tables, connectors, consumer groups and
// sink tables form each pipeline, and which user owns each layer. A pipeline
// is of kind cdc (the default) or batch (DAGs, dbt models and sink tables);
// see kind.go.
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
	// Kind is the optional `kind:` (tasks/P0-contracts.md item 3): "cdc" or
	// empty for a Postgres -> Debezium -> Kafka pipeline, "batch" for an
	// orchestrated one (DAGs, dbt models, sink tables). Load refuses any other
	// value, and a field of the other kind. Read it with KindOrDefault and the
	// pipeline's layers with Layers.
	Kind string `yaml:"kind"`
	// SourceKind is the optional cdc `source_kind:` (Q8d, source_kind.go):
	// "postgres" or empty for a Postgres source, else "mysql", "sqlserver",
	// "mongodb" or "oracle". It
	// picks the source layer (Layers()[0]) and refuses the Postgres-only
	// watch fields off Postgres. Read it with SourceKindOrDefault.
	SourceKind string `yaml:"source_kind"`
	// TopicPrefix is the Debezium topic prefix; pipeline topics are named
	// "<TopicPrefix>.<schema>.<table>". A cdc field: a batch pipeline has none.
	TopicPrefix string `yaml:"topic_prefix"`
	// SourceTables, Connectors and ConsumerGroups are cdc fields too.
	SourceTables   []string `yaml:"source_tables"`
	Connectors     []string `yaml:"connectors"`
	ConsumerGroups []string `yaml:"consumer_groups"`
	// ReprocessTopic is the optional cdc `reprocess_topic:` (X2b): the Kafka
	// topic the recovery action kafka_replay_range copies a retained offset
	// range of one of the pipeline's CDC topics into, so a consumer re-reads
	// it there and the CDC topic itself never gets copies. Empty = none, and
	// the action refuses. Load refuses an invalid topic name, a CDC or DLQ
	// topic, and a topic two pipelines on one cluster declare (reprocess.go).
	ReprocessTopic string `yaml:"reprocess_topic"`
	// SinkTables and SinkTSColumn belong to both kinds.
	SinkTables   []string `yaml:"sink_tables"`
	SinkTSColumn string   `yaml:"sink_ts_column"`
	// Sinks is the optional cdc `sinks:` block (sinks.go): each sink table with
	// the warehouse it lives in and the writer that fills it, so two sinks on
	// one topic stay distinct. It replaces sink_tables:; the legacy
	// consumer_groups + sink_tables lists keep working and mean one sink per
	// table. Load merges the block into SinkTables and ConsumerGroups.
	Sinks []Sink `yaml:"sinks"`
	// sinksMerged records that validateSinks already merged Sinks into
	// SinkTables and ConsumerGroups, so a re-validation of the same pipeline
	// (LoadDir validates the merged set again) does not mistake the merged
	// SinkTables for a user-declared sink_tables: beside sinks:.
	sinksMerged bool
	// DAGs, DBTProject and Models are the batch fields (a cdc pipeline sets
	// none). DAGs are Airflow dag_ids, Models are dbt model names, DBTProject
	// is the dbt project's path or URL. A batch pipeline needs at least one
	// DAG or model. Load trims the entries.
	DAGs       []string `yaml:"dags"`
	DBTProject string   `yaml:"dbt_project"`
	Models     []string `yaml:"models"`
	// WatchSpec is the optional `watch:` block (see watch.go). Read the
	// validated values with Watch(). Refused on a batch pipeline in this wave:
	// every watcher signal reads Kafka or Connect.
	WatchSpec *WatchSpec `yaml:"watch"`
	// ShadowSpec is the optional `shadow:` block (see shadow.go). Read it with
	// Shadow(). Allowed on both kinds.
	ShadowSpec *ShadowSpec `yaml:"shadow"`
	// SetupSpec is the optional `setup:` block (see setup.go), written by
	// pipeline_register. Read it with Setup(). Allowed on both kinds.
	SetupSpec *SetupSpec `yaml:"setup"`
	// RecoverySpec is the optional cdc `recovery:` block (recovery.go, X2).
	// Read it with RecoveryMaxRows.
	RecoverySpec *RecoverySpec `yaml:"recovery"`
	// Owners is the optional per-pipeline `owners:` block. A layer listed here
	// replaces the top-level `layers:` entry for this pipeline. Load refuses a
	// layer that is not one of the pipeline's Layers().
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
	// Team is the optional `team:` (X5-0): the team that owns the pipeline,
	// for GET /api/fleet?team=. With a teams: list it must be one of its ids
	// serving the pipeline's tenant; without one it is a free label with the
	// teams[].id charset (teams.go).
	Team string `yaml:"team"`
	// Connections is the optional `connections:` block (tenancy.go): where
	// this pipeline's DSNs, URLs and logins come from. Nil = the process-wide
	// settings.
	Connections Connections `yaml:"connections"`
	// SharedConnections allows a connection reference of this pipeline to be
	// used by another tenant too (CheckSharedRefs).
	SharedConnections bool `yaml:"shared_connections"`
	// Staging is the optional cdc `staging:` block (X8, staging.go): a second
	// Connect and Schema Registry (and optionally Kafka) to try a fix on
	// before production. Nil = no staging. Load refuses one that names a
	// production endpoint.
	Staging *StagingSpec `yaml:"staging"`
	// RequireStaging is the optional `require_staging:` list (X8): action
	// names whose steps need a healthy stage before approval. It needs a
	// staging: block; wiring checks every name is a registered actions.Stager.
	RequireStaging []string `yaml:"require_staging"`
	// PII is the optional `pii:` block (X7c, pii.go): include and exclude
	// overrides of the column-name PII classifier. Read it with
	// PIIOverrideFor.
	PII *PIISpec `yaml:"pii"`
	// DeclaredRetention is the optional `declared_retention:` block (X7,
	// declared_retention.go). Nil = nothing declared.
	DeclaredRetention *DeclaredRetention `yaml:"declared_retention"`
	// Repos is the optional `repos:` block (Q7a, repos.go): the repositories
	// a pull-request fix may change, each with the globs it may touch and the
	// layer that approves each glob. Allowed on both kinds.
	Repos []Repo `yaml:"repos"`

	// watch is WatchSpec after validation, with defaults applied. Set by Load.
	watch *WatchConfig
	// pii is PII after validation. Set by Load.
	pii *piiRules
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
	// Teams is the optional teams: list (teams.go, X4): the teams that own
	// the services outside the pipelines, which a hypothesis may name.
	Teams []Team `yaml:"teams"`

	// path and dir are where LoadDir read the config from, so a Live built
	// over this Owners can re-read it on Reload. dir is "" when there is no
	// OWNERS_DIR. skips records the per-pipeline files the last
	// LoadDir/Reload skipped, for DirSkips.
	path  string
	dir   string
	skips []DirSkip

	// derived holds consumer groups the topology read from the pipeline's own
	// sink engines (derived.go). Load and LoadDir start it empty; a copied
	// Owners (Only) shares it.
	derived *derivedGroups

	// declaredGroups indexes every consumer_groups entry of the config by group
	// name. validate builds it and an Only copy shares it, so a builder that
	// sees one pipeline can still refuse a derived group that another pipeline
	// declares (derived.go). nil on a hand-built Owners.
	declaredGroups map[string]bool
}

// Load reads and validates the owners file at path. An `auto_approve:` entry
// is checked for syntax only, so the file loads before the action registry
// exists (the CLI tools do); a server that has the registry calls
// LoadWithActions, or ValidateAutoApprove after Load.
func Load(path string) (*Owners, error) {
	o, err := parse(path)
	if err != nil {
		return nil, err
	}
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("owners: validate %s: %w", path, err)
	}
	return o, nil
}

// parse reads and decodes the owners file at path: unknown keys and null blocks
// are refused, but the decoded config is not validated. LoadDir uses it for the
// per-pipeline OWNERS_DIR files, whose validation runs against the main file's
// top-level blocks rather than the file alone.
func parse(path string) (*Owners, error) {
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
	// Created here, not lazily at the first Build: two concurrent builds would
	// otherwise race to create it and one pipeline's set would be lost.
	o.derived = &derivedGroups{}
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
		// A pointer: validateKind trims the batch lists in place.
		p := &o.Pipelines[i]
		if p.ID == "" {
			return fmt.Errorf("pipeline with empty id")
		}
		if seen[p.ID] {
			return fmt.Errorf("duplicate pipeline id %q", p.ID)
		}
		seen[p.ID] = true
		if err := p.validateKind(); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		// validateSinks merges a sinks: block into SinkTables and
		// ConsumerGroups, so the checks below see the effective lists.
		if err := p.validateSinks(); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		if err := p.checkOwnerLayers(); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		if err := p.validateSetup(); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		// validateKind has already refused the cdc fields on a batch pipeline.
		if !p.isBatch() {
			if p.TopicPrefix == "" {
				return fmt.Errorf("pipeline %q: topic_prefix is required", p.ID)
			}
			if len(p.SourceTables) == 0 {
				return fmt.Errorf("pipeline %q: no source tables", p.ID)
			}
			if len(p.Connectors) == 0 {
				return fmt.Errorf("pipeline %q: no connectors", p.ID)
			}
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
		if err := p.validateReprocessTopic(); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		if err := p.validateRecovery(); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		if err := p.validatePII(); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		if err := p.validateDeclaredRetention(); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		if err := p.validateRepos(); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		if p.isBatch() {
			// No watch: block (validateKind) and nothing to watch.
			w := batchWatch()
			p.watch = &w
			continue
		}
		w, err := resolveWatch(p.WatchSpec)
		if err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		if err := resolveDLQTopics(*p, &w); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		if err := resolveReconcileTables(*p, &w); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		if p.hasNoSlot() {
			noSlotSignals(&w)
		}
		p.watch = &w
	}
	if err := o.validateDLQ(); err != nil {
		return err
	}
	if err := o.validateReprocessTopics(); err != nil {
		return err
	}
	if err := o.validateTenancy(); err != nil {
		return err
	}
	if err := o.validateStaging(); err != nil {
		return err
	}
	if err := o.validateTeams(); err != nil {
		return err
	}
	if err := o.validateRepoIDs(); err != nil {
		return err
	}
	o.indexConsumerGroups()
	return nil
}

// indexConsumerGroups records every declared consumer group across all
// pipelines. A derived group is checked against this set (derived.go); the
// whole config is kept, not the subset an Only view holds, because a
// per-pipeline stack's builder must still see another pipeline's declaration.
func (o *Owners) indexConsumerGroups() {
	idx := make(map[string]bool)
	for i := range o.Pipelines {
		for _, g := range o.Pipelines[i].ConsumerGroups {
			idx[g] = true
		}
	}
	o.declaredGroups = idx
}
