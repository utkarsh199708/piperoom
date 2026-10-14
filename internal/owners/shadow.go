package owners

import "gopkg.in/yaml.v3"

// ShadowSpec is the optional `shadow:` block of a pipeline
// (docs/design/shadow.md section 3.1). With enabled: true every alert room of
// the pipeline is a shadow room: the agent runs silently, proposes nothing and
// its output is revealed after the room closes.
type ShadowSpec struct {
	Enabled *bool `yaml:"enabled"`
}

// UnmarshalYAML rejects unknown keys in the shadow block, like WatchSpec: a
// typo such as `enable: true` must stop the start, not silently keep the
// pipeline live.
func (s *ShadowSpec) UnmarshalYAML(node *yaml.Node) error {
	if err := checkKeys(node, "shadow", "enabled"); err != nil {
		return err
	}
	type plain ShadowSpec // no methods, so Decode does not recurse into this one
	return node.Decode((*plain)(s))
}

// Shadow reports whether the pipeline opted in to shadow mode. No block, or
// enabled missing or false, means a live agent.
func (p Pipeline) Shadow() bool {
	return p.ShadowSpec != nil && p.ShadowSpec.Enabled != nil && *p.ShadowSpec.Enabled
}

// AnyShadow reports whether any pipeline has shadow mode on. The server uses it
// to refuse a store that cannot hold shadow runs.
func (o *Owners) AnyShadow() bool {
	if o == nil {
		return false
	}
	for _, p := range o.Pipelines {
		if p.Shadow() {
			return true
		}
	}
	return false
}
