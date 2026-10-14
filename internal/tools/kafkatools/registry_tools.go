package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/hamba/avro/v2"

	"piperoom/internal/clients"
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
	return "Lists the pipeline's schema registry subjects, optionally filtered by a prefix."
}

func (t *registrySubjects) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Prefix string `json:"prefix"`
	}
	if err := json.Unmarshal(input, &p); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	if t.d.Registry == nil {
		return notConfigured(), nil
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	subjects, err := t.d.Registry.Subjects(cctx)
	if err != nil {
		return tools.Result{}, err
	}
	// Only subjects derived from this room's pipeline topics; the others are
	// only counted, before the prefix filter, so a prefix cannot probe for
	// other subjects' names through the count.
	all := len(subjects)
	subjects = sc.FilterSubjects(subjects)
	other := all - len(subjects)
	var out []string
	for _, s := range subjects {
		if p.Prefix == "" || strings.HasPrefix(s, p.Prefix) {
			out = append(out, s)
		}
	}
	var sb strings.Builder
	sb.WriteString(strings.Join(out, "\n"))
	appendLine(&sb, tools.NotShown(other, "subject", "subjects"))
	return res(sb.String(), out), nil
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

func (t *registryVersions) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Subject string `json:"subject"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Subject == "" {
		return tools.Result{}, fmt.Errorf("%w: subject is required", tools.ErrBadInput)
	}
	if t.d.Registry == nil {
		return notConfigured(), nil
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	if err := sc.CheckSubject(p.Subject); err != nil {
		return tools.Result{}, err
	}
	cctx, cancel := withTimeout(ctx)
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
		"from_version": map[string]any{"type": "integer", "description": "registered version to diff from (default latest)"},
		"to_version":   map[string]any{"type": "integer", "description": "registered version to diff to (default latest); not with schema"},
		"schema":       strProp("optional raw schema (same type as the subject's, e.g. Avro JSON); when set it is the 'to' side, diffed against from_version (default latest)"),
	}, "subject")
}
func (t *registryDiff) Description() string {
	return "Field-level diff of two schemas of a subject (added/removed/type-changed fields; Avro records and JSON Schema properties). Diff two registered versions, or a registered version (from) against a raw schema (to)."
}

func (t *registryDiff) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Subject     string `json:"subject"`
		FromVersion int    `json:"from_version"`
		ToVersion   int    `json:"to_version"`
		Schema      string `json:"schema"`
	}
	if err := json.Unmarshal(input, &p); err != nil {
		return tools.Result{}, fmt.Errorf("%w: invalid input (subject: string, from_version/to_version: integer, schema: string): %v", tools.ErrBadInput, err)
	}
	switch {
	case p.Subject == "":
		return tools.Result{}, fmt.Errorf("%w: subject is required", tools.ErrBadInput)
	case p.FromVersion < 0 || p.ToVersion < 0:
		return tools.Result{}, fmt.Errorf("%w: from_version and to_version must be positive (omit them for latest)", tools.ErrBadInput)
	case p.Schema != "" && p.ToVersion > 0:
		return tools.Result{}, fmt.Errorf("%w: set either to_version or schema, not both: schema is the 'to' side", tools.ErrBadInput)
	}
	if t.d.Registry == nil {
		return notConfigured(), nil
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	if err := sc.CheckSubject(p.Subject); err != nil {
		return tools.Result{}, err
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()

	from, fromType, err := t.schema(cctx, p.Subject, p.FromVersion)
	if err != nil {
		return tools.Result{}, err
	}
	data := map[string]any{"subject": p.Subject, "from_version": from.Version, "schema_type": fromType}

	var toSchema, toType string
	if p.Schema != "" {
		// A raw schema is checked against the subject's type: the registry
		// would accept it only as that type.
		toSchema, toType = p.Schema, fromType
		if err := validateRaw(toType, toSchema); err != nil {
			return tools.Result{}, err
		}
		data["to_version"] = 0 // 0 = the raw schema, not a registered version
	} else {
		to, typ, err := t.schema(cctx, p.Subject, p.ToVersion)
		if err != nil {
			return tools.Result{}, err
		}
		toSchema, toType = to.Schema, typ
		data["to_version"] = to.Version
	}

	if fromType != toType {
		data["supported"] = false
		return res(fmt.Sprintf("cannot diff a %s schema against a %s schema", fromType, toType), data), nil
	}
	var changes []fieldChange
	switch fromType {
	case schemaAvro:
		changes, err = diffFields(from.Schema, toSchema)
	case schemaJSON:
		changes, err = diffJSONSchema(from.Schema, toSchema)
	default:
		data["supported"] = false
		return res(fmt.Sprintf("diff not supported for %s schemas (subject %s); compare the versions' schemas with the registry directly", fromType, p.Subject), data), nil
	}
	if err != nil {
		return tools.Result{}, fmt.Errorf("kafkatools: schema_registry_diff %s: %w", p.Subject, err)
	}
	data["changes"] = changes
	return res(formatChanges(changes), data), nil
}

// Schema types as the registry names them; an empty schemaType means AVRO.
const (
	schemaAvro     = "AVRO"
	schemaJSON     = "JSON"
	schemaProtobuf = "PROTOBUF"
)

// typedRegistry is the local adapter for the registry's schemaType until
// clients.SchemaVersion carries it (contract change request): the real
// client implements it; for other clients the type is inferred from the text.
type typedRegistry interface {
	SchemaTyped(ctx context.Context, subject string, version int) (clients.SchemaVersion, string, error)
}

// schema fetches a version (0 = latest) and its schema type.
func (t *registryDiff) schema(ctx context.Context, subject string, version int) (clients.SchemaVersion, string, error) {
	if version <= 0 {
		version = -1
	}
	if tr, ok := t.d.Registry.(typedRegistry); ok {
		sv, typ, err := tr.SchemaTyped(ctx, subject, version)
		if err != nil {
			return clients.SchemaVersion{}, "", err
		}
		if typ == "" {
			typ = schemaAvro
		}
		return sv, strings.ToUpper(typ), nil
	}
	sv, err := t.d.Registry.Schema(ctx, subject, version)
	if err != nil {
		return clients.SchemaVersion{}, "", err
	}
	return sv, sniffSchemaType(sv.Schema), nil
}

// sniffSchemaType guesses a schema's type from its text: Protobuf is not
// JSON; JSON Schema is an object without Avro's "fields" that has
// "$schema" or "properties".
func sniffSchemaType(s string) string {
	t := strings.TrimSpace(s)
	if t == "" || (t[0] != '{' && t[0] != '[' && t[0] != '"') {
		return schemaProtobuf
	}
	var m map[string]any
	if json.Unmarshal([]byte(t), &m) != nil {
		return schemaAvro
	}
	if _, ok := m["fields"]; ok {
		return schemaAvro
	}
	_, hasSchema := m["$schema"]
	_, hasProps := m["properties"]
	if hasSchema || hasProps {
		return schemaJSON
	}
	return schemaAvro
}

// validateRaw rejects a raw schema that is not a valid schema of typ, so a
// typo reads as bad input instead of a misleading "no field changes".
func validateRaw(typ, schema string) error {
	switch typ {
	case schemaAvro:
		if _, err := avro.ParseWithCache(schema, "", &avro.SchemaCache{}); err != nil {
			return fmt.Errorf("%w: schema is not a valid Avro schema: %v", tools.ErrBadInput, err)
		}
		if _, err := recordFields(schema); err != nil {
			return fmt.Errorf("%w: schema is not valid JSON: %v", tools.ErrBadInput, err)
		}
	case schemaJSON:
		var m map[string]any
		if err := json.Unmarshal([]byte(schema), &m); err != nil {
			return fmt.Errorf("%w: schema is not a valid JSON Schema object: %v", tools.ErrBadInput, err)
		}
	}
	return nil
}

// fieldChange is one field-level difference between two schemas.
type fieldChange struct {
	Field string `json:"field"`
	Kind  string `json:"kind"` // added | removed | changed
	From  string `json:"from,omitempty"`
	To    string `json:"to,omitempty"`
}

// diffFields compares two Avro record schemas field by field.
func diffFields(fromSchema, toSchema string) ([]fieldChange, error) {
	fromFields, err := recordFields(fromSchema)
	if err != nil {
		return nil, err
	}
	toFields, err := recordFields(toSchema)
	if err != nil {
		return nil, err
	}
	var changes []fieldChange
	for name, toType := range toFields {
		fromType, ok := fromFields[name]
		switch {
		case !ok:
			changes = append(changes, fieldChange{Field: name, Kind: "added", To: toType})
		case fromType != toType:
			changes = append(changes, fieldChange{Field: name, Kind: "changed", From: fromType, To: toType})
		}
	}
	for name, fromType := range fromFields {
		if _, ok := toFields[name]; !ok {
			changes = append(changes, fieldChange{Field: name, Kind: "removed", From: fromType})
		}
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Field < changes[j].Field })
	return changes, nil
}

// formatChanges renders changes one per line, e.g. "amount: int → bytes(decimal 12,2)".
func formatChanges(changes []fieldChange) string {
	if len(changes) == 0 {
		return "no field changes"
	}
	var sb strings.Builder
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
	return sb.String()
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
		fields, hasFields := v["fields"].([]any)
		if !hasFields {
			for _, val := range v {
				walkFields(val, out)
			}
			return
		}
		recordName, _ := v["name"].(string)
		for _, f := range fields {
			fm, ok := f.(map[string]any)
			if !ok {
				continue
			}
			fname, _ := fm["name"].(string)
			if fname == "" {
				continue
			}
			key := fname
			if recordName != "" {
				key = recordName + "." + fname
			}
			if _, exists := out[key]; !exists {
				out[key] = canonType(fm["type"])
			}
			walkFields(fm["type"], out)
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

// notConfigured is a schema tool's answer when the deployment has no Schema
// Registry (SCHEMA_REGISTRY_URL empty or off): a finding for the agent, not
// an error to retry.
func notConfigured() tools.Result {
	return res("Schema Registry not configured (SCHEMA_REGISTRY_URL is empty or off)", map[string]any{"configured": false})
}
