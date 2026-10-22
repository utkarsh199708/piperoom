package owners

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode"

	"piperoom/internal/events"
)

// This file holds the pipeline kind of owners.yaml (tasks/P4-pipeline-kinds-batch.md,
// sub-task P4a-1; schema in tasks/P0-contracts.md item 3):
//
//	Pipeline.Kind / KindOrDefault         "" or cdc = Postgres -> Debezium -> Kafka; batch = Airflow -> dbt -> warehouse
//	Pipeline.Layers                       the layers of the kind (events.LayersOf), so a caller never hard-codes six
//	Pipeline.DAGs / DBTProject / Models   the batch fields
//
// A pipeline has exactly one kind. Load refuses a field of the other kind
// rather than ignoring it: a `kind: batch` that kept a `connectors:` list, or
// a CDC pipeline that gained a `dags:` list because `kind:` was mistyped away,
// would give the agent and the scope rules a different pipeline than the file
// reads as.

var (
	// dagIDRE is Airflow's dag_id alphabet (letters, digits, '_', '.', '-'). It
	// has no '/', '?' or '#', so an id cannot add a path segment or a query to
	// a REST call. It does admit "." and "..", which a server may read as dot
	// segments; validateBatch refuses an id of only dots, and the Airflow
	// client's netsec.CheckPath refuses a dot segment in every request path.
	dagIDRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	// modelNameRE is a dbt model name: letters, digits and '_' only.
	modelNameRE = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	// modelUniqueIDRE is a dbt model unique_id: model.<package>.<name>, with an
	// optional version segment, each segment in the model alphabet (letters,
	// digits and '_'). The same shape owners.ModelName reads.
	modelUniqueIDRE = regexp.MustCompile(`^model\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$`)
)

// KindOrDefault returns the pipeline kind: events.PipelineKindCDC when the file
// names none (every file written before kinds existed), else Kind as written.
// Load refuses any value but "cdc" and "batch", so a loaded pipeline always
// answers one of those two; a Pipeline built in code with another Kind answers
// that value, and Layers then answers nil, which fails closed. A nil pipeline
// is the default kind.
func (p *Pipeline) KindOrDefault() string {
	if p == nil || p.Kind == "" {
		return events.PipelineKindCDC
	}
	return p.Kind
}

// Layers returns the layers of the pipeline's kind, upstream first
// (events.LayersOf): the six CDC layers, or orchestrator, transform and
// warehouse for batch. It is a new slice on every call, so the caller may keep
// or change it. A nil pipeline, or one with an unknown Kind, has no layers
// (nil): a caller must not read that as "every layer", and OwnsAll already
// reads an empty list as "not allowed".
func (p *Pipeline) Layers() []string {
	if p == nil {
		return nil
	}
	return events.LayersOf(p.KindOrDefault())
}

// ModelName returns the dbt model name that ref names: a model name as
// owners.yaml writes it ("fct_revenue"), or a model unique_id as
// clients.Lineage and clients.ModelResult report it
// ("model.analytics.fct_revenue", versioned "model.analytics.fct_revenue.v2").
// ok is false for anything else: another resource type ("source.", "seed.",
// "test."), or a name outside the model alphabet. The topology builder and
// the tool scope (tools.Scope) both compare a pipeline's models through it,
// so the map and the scope agree on which node is which model.
func ModelName(ref string) (string, bool) {
	name := ref
	if strings.Contains(ref, ".") {
		parts := strings.Split(ref, ".")
		// model.<package>.<name>[.<version>]
		if len(parts) < 3 || len(parts) > 4 || parts[0] != "model" || parts[1] == "" {
			return "", false
		}
		name = parts[2]
	}
	if !modelNameRE.MatchString(name) {
		return "", false
	}
	return name, true
}

// ModelsContain reports whether the pipeline's models list admits ref. models
// holds names as owners.yaml writes them: a bare name, or a unique_id
// "model.<package>.<name>[.<version>]". ref is a name in the same two forms
// (a tool input, or a unique_id from the manifest). A bare entry admits any
// package with that name (the match before unique_ids could be listed); a
// unique_id entry is pinned to its package, and its version when it names one,
// so the same-named model in another package is refused. The topology builder
// and tools.Scope both read a pipeline's models through ModelName, so the
// graph and the scope agree on which unique_id model belongs.
func ModelsContain(models []string, ref string) bool {
	rn, ok := ModelName(ref)
	if !ok {
		return false
	}
	for _, m := range models {
		mn, ok := ModelName(m)
		if !ok || mn != rn {
			continue
		}
		if m == mn { // a bare name admits any package with this name
			return true
		}
		if m == ref { // a unique_id admits only the exact id
			return true
		}
	}
	return false
}

// isBatch reports whether the pipeline is of kind batch.
func (p *Pipeline) isBatch() bool { return p.KindOrDefault() == events.PipelineKindBatch }

// validateKind checks the kind, refuses the fields of the other kind and, for
// a batch pipeline, cleans dags, models and dbt_project (trimmed) in place. The
// errors do not name the pipeline; the caller adds it.
//
// A `watch:` block is refused on a batch pipeline in this wave: every watcher
// signal reads Kafka, Connect or the replication slot, which a batch pipeline
// does not have, so a block would promise a check that never runs. A `shadow:`
// block is allowed: it only decides whether the agent may propose.
func (p *Pipeline) validateKind() error {
	switch p.Kind {
	case "", events.PipelineKindCDC:
		return p.refuseBatchFields()
	case events.PipelineKindBatch:
		return p.validateBatch()
	}
	return fmt.Errorf("kind %q: want %s or %s (leave it out for %s)",
		p.Kind, events.PipelineKindCDC, events.PipelineKindBatch, events.PipelineKindCDC)
}

// refuseBatchFields fails when a cdc pipeline sets a batch field. The hint says
// what to write when the kind was only implied.
func (p *Pipeline) refuseBatchFields() error {
	var field string
	switch {
	case len(p.DAGs) > 0:
		field = "dags"
	case p.DBTProject != "":
		field = "dbt_project"
	case len(p.Models) > 0:
		field = "models"
	default:
		return nil
	}
	hint := ""
	if p.Kind == "" {
		hint = fmt.Sprintf(" (the default kind; write kind: %s to use it)", events.PipelineKindBatch)
	}
	return fmt.Errorf("%s is a %s field, but the pipeline kind is %s%s",
		field, events.PipelineKindBatch, events.PipelineKindCDC, hint)
}

// validateBatch checks a batch pipeline: no CDC field and no watch block, at
// least one DAG or model, and clean names.
func (p *Pipeline) validateBatch() error {
	// The CDC fields in file order, so the error is the same for the same file.
	var field string
	switch {
	case p.TopicPrefix != "":
		field = "topic_prefix"
	case len(p.SourceTables) > 0:
		field = "source_tables"
	case len(p.Connectors) > 0:
		field = "connectors"
	case len(p.ConsumerGroups) > 0:
		field = "consumer_groups"
	case len(p.Sinks) > 0:
		field = "sinks"
	case p.ReprocessTopic != "":
		field = "reprocess_topic"
	}
	if field != "" {
		return fmt.Errorf("%s is a %s field, but the pipeline kind is %s",
			field, events.PipelineKindCDC, events.PipelineKindBatch)
	}
	if p.WatchSpec != nil {
		return fmt.Errorf("watch: not supported for kind %s in this wave (its signals read Kafka and Connect); remove the block",
			events.PipelineKindBatch)
	}
	dags, err := cleanNames("dags", p.DAGs, dagIDRE, "a DAG id (letters, digits, '_', '.' and '-')")
	if err != nil {
		return err
	}
	for _, d := range dags {
		// "." and ".." (and "...") match the alphabet but are a path segment
		// to a server that removes dot segments.
		if strings.Trim(d, ".") == "" {
			return fmt.Errorf("dags: %q is not a DAG id (an id of only dots is a path segment, not a name)", d)
		}
	}
	models, err := cleanModels(p.Models)
	if err != nil {
		return err
	}
	if len(dags) == 0 && len(models) == 0 {
		return fmt.Errorf("kind %s needs at least one of dags or models (nothing else tells the agent what to read)", events.PipelineKindBatch)
	}
	project := strings.TrimSpace(p.DBTProject)
	// The value is a path or URL for the dbt client; a control character in it
	// would reach logs and the agent's prompt as a line break. unicode.IsControl
	// covers the C0 and C1 ranges (DEL and NEL included); the Unicode line and
	// paragraph separators are not control characters but break a line too.
	for _, r := range project {
		if unicode.IsControl(r) || r == ' ' || r == ' ' {
			return fmt.Errorf("dbt_project %q: contains a control character", project)
		}
	}
	p.DAGs, p.Models, p.DBTProject = dags, models, project
	return nil
}

// checkOwnerLayers refuses an `owners:` entry for a layer the pipeline's kind
// does not have. validateRights checks the block against every known layer
// (events.Layers), so a batch pipeline with a kafka-connect entry, or a cdc
// pipeline with an orchestrator one, would load and never grant anything: the
// person who wrote it would believe a layer was covered. It runs after
// validateKind, so the kind is cdc or batch. The keys are sorted so the same
// file gives the same error.
func (p *Pipeline) checkOwnerLayers() error {
	layers := p.Layers()
	keys := make([]string, 0, len(p.Owners))
	for l := range p.Owners {
		keys = append(keys, l)
	}
	sort.Strings(keys)
	for _, l := range keys {
		if !slices.Contains(layers, l) {
			return fmt.Errorf("owners: layer %q is not a %s layer (want one of %s)", l, p.KindOrDefault(), strings.Join(layers, ", "))
		}
	}
	return nil
}

// cleanNames trims every entry of a name list and checks it: not empty, matches
// re, not repeated. It returns the trimmed list (nil for an absent list) and
// leaves its input alone, so a failed check changes nothing. A repeat is
// refused, not merged, as for the other lists: a name twice becomes two graph
// nodes with one id.
func cleanNames(field string, in []string, re *regexp.Regexp, what string) ([]string, error) {
	if in == nil {
		return nil, nil
	}
	out := make([]string, 0, len(in))
	for i, raw := range in {
		name := strings.TrimSpace(raw)
		switch {
		case name == "":
			return nil, fmt.Errorf("%s[%d]: empty entry", field, i)
		case !re.MatchString(name):
			return nil, fmt.Errorf("%s: %q is not %s", field, name, what)
		}
		out = append(out, name)
	}
	if d := firstDuplicate(out); d != "" {
		return nil, fmt.Errorf("%s lists %q twice", field, d)
	}
	return out, nil
}

// cleanModels trims every entry of the models list and checks it: not empty, a
// model name (letters, digits and '_') or a model unique_id
// (model.<package>.<name>[.<version>]), not repeated. It returns the trimmed
// list (nil for an absent list) and leaves its input alone, so a failed check
// changes nothing. A unique_id is pinned: the same name in another package is
// a different model and must be listed on its own.
func cleanModels(in []string) ([]string, error) {
	if in == nil {
		return nil, nil
	}
	out := make([]string, 0, len(in))
	for i, raw := range in {
		name := strings.TrimSpace(raw)
		switch {
		case name == "":
			return nil, fmt.Errorf("models[%d]: empty entry", i)
		case !modelNameRE.MatchString(name) && !modelUniqueIDRE.MatchString(name):
			return nil, fmt.Errorf("models: %q is not a model name (letters, digits and '_') or a model unique_id (model.<package>.<name>)", name)
		}
		out = append(out, name)
	}
	if d := firstDuplicate(out); d != "" {
		return nil, fmt.Errorf("models lists %q twice", d)
	}
	return out, nil
}

// batchWatch is the watcher configuration of a batch pipeline: every signal
// off. A batch pipeline has no `watch:` block (validateBatch), so without this
// it would get DefaultWatch, and the watcher would poll connectors, slots and
// consumer groups the pipeline does not have.
func batchWatch() WatchConfig {
	w := DefaultWatch()
	w.Enabled = false
	for _, s := range []*WatchSignal{
		&w.ConnectorDown, &w.SlotInactive, &w.SlotWAL.WatchSignal, &w.DLQGrowth.WatchSignal,
		&w.ConfigRisk, &w.ConsumerLag.WatchSignal, &w.GroupUnstable.WatchSignal, &w.OffsetBelowLogStart,
		&w.CountDrift, &w.RowDrift, &w.DupKeys, &w.OrderSkew,
	} {
		s.Enabled = false
	}
	return w
}
