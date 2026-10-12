// Package owners loads and validates the pipeline ownership config
// (dev/owners.yaml): which source tables, connectors, consumer groups and
// sink tables form each pipeline, and which user owns each layer.
package owners

import (
	"fmt"
	"os"
	"slices"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
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

	// watch is WatchSpec after validation, with defaults applied. Set by Load.
	watch *WatchConfig
}

// Owners is the full ownership config.
type Owners struct {
	Pipelines []Pipeline        `yaml:"pipelines"`
	Layers    map[string]string `yaml:"layers"`
}

// Load reads and validates the owners file at path.
func Load(path string) (*Owners, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("owners: read %s: %w", path, err)
	}
	var o Owners
	if err := yaml.Unmarshal(data, &o); err != nil {
		return nil, fmt.Errorf("owners: parse %s: %w", path, err)
	}
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("owners: validate %s: %w", path, err)
	}
	return &o, nil
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

// LayerOwner returns the user id that owns a layer, e.g. "kafka-connect" -> "arjun".
func (o *Owners) LayerOwner(layer string) (string, bool) {
	u, ok := o.Layers[layer]
	return u, ok
}

func (o *Owners) validate() error {
	for layer := range o.Layers {
		if !slices.Contains(events.Layers, layer) {
			return fmt.Errorf("unknown layer %q (known: %v)", layer, events.Layers)
		}
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
		if len(p.SinkTables) > 0 && p.SinkTSColumn == "" {
			return fmt.Errorf("pipeline %q: sink_ts_column is required when sink_tables is set", p.ID)
		}
		w, err := resolveWatch(p.WatchSpec)
		if err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
		o.Pipelines[i].watch = &w
	}
	return nil
}
