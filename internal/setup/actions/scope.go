package setupactions

import (
	"bytes"
	"encoding/json"
	"net"
	"net/url"
	"slices"
	"strings"
	"unicode/utf8"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/setup/catalog"
	"piperoom/internal/setup/protocol"
)

// sourceKinds are the target kinds of a source database: an op reaching one
// needs a CDC pipeline, which is the only kind with a source.
var sourceKinds = map[string]bool{"postgres": true, owners.SourceKindMySQL: true}

// checkScope is CheckScope for one spec. A step whose params name nothing of
// the pipeline (a file under the runner's project directory, a compose
// service) is in scope: the runner confines it to the project directory and
// to the room's registered targets itself.
func checkScope(spec catalog.Spec, p owners.Pipeline, params json.RawMessage) error {
	name := spec.Name
	if strings.TrimSpace(p.ID) == "" {
		return scopeErr(name, p.ID, "the room has no pipeline to scope the step to")
	}
	if sourceKinds[spec.Target] && p.KindOrDefault() != events.PipelineKindCDC {
		return scopeErr(name, p.ID, "a source database step needs a CDC pipeline")
	}
	if len(spec.TableKeys) == 0 && len(spec.HostKeys) == 0 {
		return nil
	}
	obj, err := decodeObject(params)
	if err != nil {
		return scopeErr(name, p.ID, "params do not decode: %v", err)
	}
	for _, key := range spec.TableKeys {
		vals, ok := stringsAt(obj, key)
		if !ok {
			return scopeErr(name, p.ID, "%s is not a table name or a list of them", key)
		}
		for _, t := range vals {
			if !slices.Contains(p.SourceTables, t) {
				return scopeErr(name, p.ID, "table %s is not one of the pipeline's tables; extend the pipeline with a driver click first", clipName(t))
			}
		}
	}
	if len(spec.HostKeys) == 0 {
		return nil
	}
	hosts := pipelineHosts(p)
	for _, key := range spec.HostKeys {
		vals, ok := stringsAt(obj, key)
		if !ok {
			return scopeErr(name, p.ID, "%s is not a host", key)
		}
		for _, v := range vals {
			h := hostOf(v)
			if h == "" || !hosts[h] {
				return scopeErr(name, p.ID, "target %s is not one of the pipeline's hosts", clipName(v))
			}
		}
	}
	return nil
}

// decodeObject decodes params as an object, refusing keys the decoder would
// fold together (protocol.CheckParamKeys): which value a lookup sees must be
// the value the runner executes.
func decodeObject(params json.RawMessage) (map[string]any, error) {
	raw := bytes.TrimSpace(params)
	if len(raw) == 0 {
		raw = []byte(`{}`)
	}
	if err := protocol.CheckParamKeys(raw); err != nil {
		return nil, err
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// stringsAt returns the string or list of strings at key; an absent key is
// an empty list (the schema decides whether it is required).
func stringsAt(obj map[string]any, key string) ([]string, bool) {
	v, present := obj[key]
	if !present {
		return nil, true
	}
	switch x := v.(type) {
	case string:
		return []string{x}, true
	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			s, ok := item.(string)
			if !ok {
				return nil, false
			}
			out = append(out, s)
		}
		return out, true
	}
	return nil, false
}

// pipelineHosts is the set of hosts of p's literal connections: values (the
// provisional pipeline carries the person's target addresses there). An env:
// or file: reference names no host without I/O, so it adds none: a step
// cannot reach a host through a reference CheckScope could not read.
func pipelineHosts(p owners.Pipeline) map[string]bool {
	out := map[string]bool{}
	for _, ref := range p.Connections {
		for _, part := range strings.Split(ref.Value, ",") {
			if h := hostOf(part); h != "" {
				out[h] = true
			}
		}
	}
	return out
}

// hostOf extracts the lower-case host of a URL, host:port or bare host, and
// "" for anything else.
func hostOf(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if strings.Contains(s, "://") {
		u, err := url.Parse(s)
		if err != nil {
			return ""
		}
		return strings.ToLower(u.Hostname())
	}
	if h, _, err := net.SplitHostPort(s); err == nil {
		return strings.ToLower(h)
	}
	if strings.ContainsAny(s, "/@ ") {
		return ""
	}
	return strings.ToLower(strings.Trim(s, "[]"))
}

// clipName cuts a model-written name for an error message.
func clipName(s string) string {
	s = strings.ToValidUTF8(s, "?")
	if len(s) <= 64 {
		return s
	}
	cut := 64
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
