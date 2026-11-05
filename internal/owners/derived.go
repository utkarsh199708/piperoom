package owners

import (
	"slices"
	"sync"
)

// Derived consumer groups.
//
// owners.yaml lists the consumer groups a pipeline declares. A warehouse sink
// can have a consumer of its own that owners.yaml does not name: a ClickHouse
// Kafka engine table consumes its topic with the group its CREATE statement
// sets (kafka_group_name). The topology builder reads that group from the
// warehouse at Build time and records it here, so the read tools (tools.Scope
// CheckReadGroup and FilterReadGroups) accept it (kafka_consumer_lag and
// kafka_list_groups then see the sink's consumer). It never widens the write
// guard: tools.Scope.CheckGroup, which actions and clients/kafka.Writer call,
// keeps accepting declared groups only.
//
// A group that any pipeline declares is not recorded (derivedGroups.set below):
// the DDL author controls kafka_group_name, so a malicious or accidental value
// that names another pipeline's group must not become this pipeline's group.
//
// The set is runtime state, not configuration: Load starts it empty, Reload
// publishes a new Owners whose set is empty until the next Build, and a Build
// replaces a pipeline's whole set. A group the engine no longer declares stops
// being allowed, so a stale entry can never widen scope. Deriving needs I/O
// (the warehouse), which is why this is not part of the config load.

// derivedGroups holds the derived consumer groups per pipeline id. The zero
// value is ready to use; Owners holds it through a pointer so copying an
// Owners (Only) keeps working and shares the set.
type derivedGroups struct {
	mu         sync.Mutex
	byPipeline map[string][]string
}

func (d *derivedGroups) set(pipelineID string, groups []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.byPipeline == nil {
		d.byPipeline = map[string][]string{}
	}
	if len(groups) == 0 {
		delete(d.byPipeline, pipelineID)
		return
	}
	clean := make([]string, 0, len(groups))
	for _, g := range groups {
		if g != "" && !slices.Contains(clean, g) {
			clean = append(clean, g)
		}
	}
	if len(clean) == 0 {
		delete(d.byPipeline, pipelineID)
		return
	}
	slices.Sort(clean)
	d.byPipeline[pipelineID] = clean
}

func (d *derivedGroups) get(pipelineID string) []string {
	if d == nil || pipelineID == "" {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.byPipeline[pipelineID])
}

// SetDerivedConsumerGroups replaces the consumer groups the topology derived
// from this pipeline's own sink engines. Only the topology builder calls it,
// with groups read from the pipeline's own objects: it must never carry a group
// from configuration or another pipeline. An empty list clears the set, so a
// failed or unsupported read leaves no group in scope.
//
// A group any pipeline declares is dropped: a warehouse DDL author controls
// kafka_group_name, so naming a declared group (another pipeline's, or this
// one's) must not register anything. declaredGroup checks the whole config, so
// an Only view applies the same rule.
func (o *Owners) SetDerivedConsumerGroups(pipelineID string, groups []string) {
	if o == nil || pipelineID == "" {
		return
	}
	if o.derived == nil {
		// A hand-built Owners: allocate so callers (tests) can still record a
		// set. Loaded configs get the store from parse.
		o.derived = &derivedGroups{}
	}
	kept := make([]string, 0, len(groups))
	for _, g := range groups {
		if g == "" || o.declaredGroup(g) {
			continue
		}
		kept = append(kept, g)
	}
	o.derived.set(pipelineID, kept)
}

// declaredGroup reports whether any pipeline in the config declares group. The
// index validate built is shared by Only copies; a hand-built Owners has none,
// so the pipelines it holds are scanned instead.
func (o *Owners) declaredGroup(group string) bool {
	if o.declaredGroups != nil {
		return o.declaredGroups[group]
	}
	for i := range o.Pipelines {
		if slices.Contains(o.Pipelines[i].ConsumerGroups, group) {
			return true
		}
	}
	return false
}

// DerivedConsumerGroups returns the consumer groups the topology derived from
// the pipeline's own sink engines, sorted, or nil.
func (o *Owners) DerivedConsumerGroups(pipelineID string) []string {
	if o == nil {
		return nil
	}
	return o.derived.get(pipelineID)
}
