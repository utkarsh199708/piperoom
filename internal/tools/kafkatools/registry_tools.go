package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"piperoom/internal/tools"
)

// ---- schema_registry_subjects ----

type registrySubjects struct{ d Deps }

func (t *registrySubjects) Name() string     { return "schema_registry_subjects" }
func (t *registrySubjects) Kind() tools.Kind { return tools.KindRead }
func (t *registrySubjects) InputSchema() map[string]any {
	return objectSchema(map[string]any{"prefix": strProp("optional subject prefix filter")})
}
func (t *registrySubjects) Description() string {
	return "Lists schema registry subjects, optionally filtered by a prefix."
}

func (t *registrySubjects) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Prefix string `json:"prefix"`
	}
	if err := json.Unmarshal(input, &p); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	cctx, cancel := ctx()
	defer cancel()
	subjects, err := t.d.Registry.Subjects(cctx)
	if err != nil {
		return tools.Result{}, err
	}
	var out []string
	for _, s := range subjects {
		if p.Prefix == "" || strings.HasPrefix(s, p.Prefix) {
			out = append(out, s)
		}
	}
	return res(strings.Join(out, "\n"), out), nil
}

// ---- schema_registry_versions ----

type registryVersions struct{ d Deps }

func (t *registryVersions) Name() string     { return "schema_registry_versions" }
func (t *registryVersions) Kind() tools.Kind { return tools.KindRead }
func (t *registryVersions) InputSchema() map[string]any {
	return objectSchema(map[string]any{"subject": strProp("subject name")}, "subject")
}
func (t *registryVersions) Description() string {
	return "Lists a subject's versions, its latest schema id, and its compatibility level (subject or global)."
}

func (t *registryVersions) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Subject string `json:"subject"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Subject == "" {
		return tools.Result{}, fmt.Errorf("%w: subject is required", tools.ErrBadInput)
	}
	cctx, cancel := ctx()
	defer cancel()
	versions, err := t.d.Registry.Versions(cctx, p.Subject)
	if err != nil {
		return tools.Result{}, err
	}
	latest, err := t.d.Registry.Schema(cctx, p.Subject, -1)
	if err != nil {
		return tools.Result{}, err
	}
	level, subjectLevel, err := t.d.Registry.Compatibility(cctx, p.Subject)
	if err != nil {
		level, subjectLevel = "UNKNOWN", false
	}
	scope := "global"
	if subjectLevel {
		scope = "subject"
	}
	summary := fmt.Sprintf("%s: versions %v, latest id %d, compatibility %s (%s)",
		p.Subject, versions, latest.ID, level, scope)
	return res(summary, map[string]any{
		"subject": p.Subject, "versions": versions, "latest_id": latest.ID,
		"compatibility": level, "compatibility_scope": scope,
	}), nil
}

// ---- schema_registry_diff ----

type registryDiff struct{ d Deps }

func (t *registryDiff) Name() string     { return "schema_registry_diff" }
func (t *registryDiff) Kind() tools.Kind { return tools.KindRead }
func (t *registryDiff) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"subject":      strProp("subject name"),
		"from_version": map[string]any{"type": "integer"},
		"to_version":   map[string]any{"type": "integer"},
	}, "subject", "from_version")
}
func (t *registryDiff) Description() string {
	return "Field-level diff of two Avro record schema versions: added, removed and type-changed fields."
}

func (t *registryDiff) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Subject     string `json:"subject"`
		FromVersion int    `json:"from_version"`
		ToVersion   int    `json:"to_version"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Subject == "" {
		return tools.Result{}, fmt.Errorf("%w: subject and from_version are required", tools.ErrBadInput)
	}
	cctx, cancel := ctx()
	defer cancel()
	from, err := t.d.Registry.Schema(cctx, p.Subject, p.FromVersion)
	if err != nil {
		return tools.Result{}, err
	}
	toVersion := p.ToVersion
	if toVersion == 0 {
		latest, err := t.d.Registry.Schema(cctx, p.Subject, -1)
		if err != nil {
			return tools.Result{}, err
		}
		toVersion = latest.Version
	}
	to, err := t.d.Registry.Schema(cctx, p.Subject, toVersion)
	if err != nil {
		return tools.Result{}, err
	}

	fromFields, err := recordFields(from.Schema)
	if err != nil {
		return tools.Result{}, fmt.Errorf("parse from schema: %w", err)
	}
	toFields, err := recordFields(to.Schema)
	if err != nil {
		return tools.Result{}, fmt.Errorf("parse to schema: %w", err)
	}

	type change struct {
		Field string `json:"field"`
		Kind  string `json:"kind"` // added | removed | changed
		From  string `json:"from,omitempty"`
		To    string `json:"to,omitempty"`
	}
	var changes []change
	for name, toType := range toFields {
		fromType, ok := fromFields[name]
		switch {
		case !ok:
			changes = append(changes, change{Field: name, Kind: "added", To: toType})
		case fromType != toType:
			changes = append(changes, change{Field: name, Kind: "changed", From: fromType, To: toType})
		}
	}
	for name, fromType := range fromFields {
		if _, ok := toFields[name]; !ok {
			changes = append(changes, change{Field: name, Kind: "removed", From: fromType})
		}
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Field < changes[j].Field })

	var sb strings.Builder
	if len(changes) == 0 {
		sb.WriteString("no field changes")
	} else {
		for i, c := range changes {
			if i > 0 {
				sb.WriteString("\n")
			}
			switch c.Kind {
			case "added":
				fmt.Fprintf(&sb, "+ %s: %s", c.Field, c.To)
			case "removed":
				fmt.Fprintf(&sb, "- %s: %s", c.Field, c.From)
			case "changed":
				fmt.Fprintf(&sb, "%s: %s → %s", c.Field, c.From, c.To)
			}
		}
	}
	return res(sb.String(), map[string]any{
		"subject": p.Subject, "from_version": p.FromVersion, "to_version": toVersion, "changes": changes,
	}), nil
}

// recordFields flattens every record field in an Avro schema into name→canonical type.
func recordFields(schema string) (map[string]string, error) {
	var root any
	if err := json.Unmarshal([]byte(schema), &root); err != nil {
		return nil, err
	}
	out := map[string]string{}
	walkFields(root, out)
	return out, nil
}

func walkFields(node any, out map[string]string) {
	switch v := node.(type) {
	case map[string]any:
		if fields, ok := v["fields"].([]any); ok {
			for _, f := range fields {
				fm, ok := f.(map[string]any)
				if !ok {
					continue
				}
				if name, _ := fm["name"].(string); name != "" {
					if _, exists := out[name]; !exists {
						out[name] = canonType(fm["type"])
					}
				}
			}
		}
		for _, val := range v {
			walkFields(val, out)
		}
	case []any:
		for _, x := range v {
			walkFields(x, out)
		}
	}
}

// canonType renders an Avro type as a compact, comparable string.
func canonType(t any) string {
	switch v := t.(type) {
	case string:
		return v
	case map[string]any:
		vt, _ := v["type"].(string)
		if lt, ok := v["logicalType"].(string); ok {
			if vt == "bytes" && lt == "decimal" {
				return fmt.Sprintf("bytes(decimal %v,%v)", v["precision"], v["scale"])
			}
			return vt + "(" + lt + ")"
		}
		if name, ok := v["name"].(string); ok {
			if vt == "" {
				return "record:" + name
			}
			return vt + ":" + name
		}
		if vt != "" {
			return vt
		}
		return "record"
	case []any:
		parts := make([]string, len(v))
		for i, x := range v {
			parts[i] = canonType(x)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return fmt.Sprint(t)
}
