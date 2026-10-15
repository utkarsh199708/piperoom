package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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

// maxSchemaVersion is the highest version Schema Registry can hold (a Java
// int). Versions are ids, not amounts: one past it is refused, not clamped.
const maxSchemaVersion = math.MaxInt32

type registryDiff struct{ d Deps }

func (t *registryDiff) Name() string     { return "schema_registry_diff" }
func (t *registryDiff) Kind() tools.Kind { return tools.KindRead }
func (t *registryDiff) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"subject":      strProp("subject name"),
		"from_version": map[string]any{"type": "integer", "description": "registered version to diff from (at most 2147483647); with schema, the registered side of the diff"},
		"to_version":   map[string]any{"type": "integer", "description": "registered version to diff to (default latest; at most 2147483647); with schema and no from_version, the registered side of the diff"},
		"schema":       strProp("optional raw candidate Avro schema JSON; when set, it is the to side: the registered version (from_version, else to_version, else latest) is diffed to it, so a field the candidate adds shows as added. Reported as to_version 0"),
	}, "subject")
}
func (t *registryDiff) Description() string {
	return "Field-level diff of two Avro record schemas (added/removed/type-changed). Diff either two registered versions, or a raw schema against a version."
}

func (t *registryDiff) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Subject     string `json:"subject"`
		FromVersion int    `json:"from_version"`
		ToVersion   int    `json:"to_version"`
		Schema      string `json:"schema"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Subject == "" {
		return tools.Result{}, fmt.Errorf("%w: subject is required", tools.ErrBadInput)
	}
	if p.FromVersion > maxSchemaVersion || p.ToVersion > maxSchemaVersion {
		return tools.Result{}, fmt.Errorf("%w: from_version and to_version must be at most %d", tools.ErrBadInput, maxSchemaVersion)
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

	toVersion := p.ToVersion
	if toVersion <= 0 {
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

	// The registered version is always the "from" side when a raw schema is
	// given: the raw schema is the candidate, so what it adds shows as added.
	fromSchema, toSchema := "", to.Schema
	fromVersion, outTo := p.FromVersion, toVersion
	if p.Schema != "" {
		// from_version names the registered side; else to_version, else latest.
		regVersion, reg := toVersion, to
		if p.FromVersion > 0 {
			regVersion = p.FromVersion
			if reg, err = t.d.Registry.Schema(cctx, p.Subject, regVersion); err != nil {
				return tools.Result{}, err
			}
		}
		fromSchema, toSchema = reg.Schema, p.Schema
		fromVersion, outTo = regVersion, 0 // 0 = raw schema, not a registered version
	} else {
		if p.FromVersion <= 0 {
			return tools.Result{}, fmt.Errorf("%w: from_version or schema is required", tools.ErrBadInput)
		}
		from, err := t.d.Registry.Schema(cctx, p.Subject, p.FromVersion)
		if err != nil {
			return tools.Result{}, err
		}
		fromSchema = from.Schema
	}

	changes, err := diffFields(fromSchema, toSchema)
	if err != nil {
		return tools.Result{}, fmt.Errorf("diff schemas: %w", err)
	}
	return res(formatChanges(changes), map[string]any{
		"subject": p.Subject, "from_version": fromVersion, "to_version": outTo, "changes": changes,
	}), nil
}

// fieldChange is one field-level difference between two Avro schemas.
type fieldChange struct {
	Field string `json:"field"`
	Kind  string `json:"kind"` // added | removed | changed
	From  string `json:"from,omitempty"`
	To    string `json:"to,omitempty"`
}

// diffFields compares two Avro record schemas field by field.
func diffFields(fromSchema, toSchema string) ([]fieldChange, error) {
	fromIdx, err := recordFields(fromSchema)
	if err != nil {
		return nil, err
	}
	toIdx, err := recordFields(toSchema)
	if err != nil {
		return nil, err
	}
	fromFields, toFields := fromIdx.fields, toIdx.fields
	changes := enumChanges(fromIdx.enums, toIdx.enums)
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

// schemaIndex is what a diff compares: every record field's canonical type,
// plus the symbols of every enum definition, keyed by full name.
type schemaIndex struct {
	fields map[string]string
	enums  map[string][]string // full enum name → symbols
}

// recordFields flattens every record field in an Avro schema into name→canonical type.
func recordFields(schema string) (schemaIndex, error) {
	var root any
	if err := json.Unmarshal([]byte(schema), &root); err != nil {
		return schemaIndex{}, err
	}
	idx := schemaIndex{fields: map[string]string{}, enums: map[string][]string{}}
	walkFields(root, "", idx)
	return idx, nil
}

// namedKind reports whether m defines a named Avro type (record, enum, fixed).
func namedKind(m map[string]any) (name string, ok bool) {
	name, _ = m["name"].(string)
	switch m["type"] {
	case "record", "enum", "fixed", "error":
		return name, name != ""
	}
	return "", false
}

// qualify returns the full name of a definition and the namespace its
// children inherit, following Avro: a dotted name carries its own namespace,
// else the namespace attribute, else the enclosing one.
func qualify(m map[string]any, name, enclosing string) (full, ns string) {
	if i := strings.LastIndex(name, "."); i >= 0 {
		return name, name[:i]
	}
	ns = enclosing
	if a, ok := m["namespace"].(string); ok {
		ns = a
	}
	if ns == "" {
		return name, ns
	}
	return ns + "." + name, ns
}

// shortName drops the namespace of a type name.
func shortName(n string) string { return n[strings.LastIndex(n, ".")+1:] }

func walkFields(node any, ns string, idx schemaIndex) {
	switch v := node.(type) {
	case map[string]any:
		if v["type"] == "enum" {
			if name, ok := namedKind(v); ok {
				full, _ := qualify(v, name, ns)
				if _, exists := idx.enums[full]; !exists {
					idx.enums[full] = []string{}
					for _, x := range anySlice(v["symbols"]) {
						idx.enums[full] = append(idx.enums[full], fmt.Sprint(x))
					}
				}
			}
			return
		}
		fields, hasFields := v["fields"].([]any)
		if !hasFields {
			for _, val := range v {
				walkFields(val, ns, idx)
			}
			return
		}
		recordName, _ := v["name"].(string)
		if recordName != "" {
			_, ns = qualify(v, recordName, ns)
		}
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
			if _, exists := idx.fields[key]; !exists {
				idx.fields[key] = canon(fm["type"], false)
			}
			walkFields(fm["type"], ns, idx)
		}
	case []any:
		for _, x := range v {
			walkFields(x, ns, idx)
		}
	}
}

var avroPrimitives = map[string]bool{"null": true, "boolean": true, "int": true, "long": true, "float": true, "double": true, "bytes": true, "string": true}

// structuralTypes are the "type" values that are not a by-name reference.
var structuralTypes = map[string]bool{"array": true, "map": true, "record": true, "enum": true, "fixed": true, "error": true}

// namedTypeName reports the short name when t names a type by definition
// ({"type":"record","name":...}), by bare reference ("com.x.Addr") or by an
// object-wrapped reference ({"type":"com.x.Addr"}); it is the one place that
// knows these spellings, so none can render differently from another.
func namedTypeName(t any) (string, bool) {
	switch v := t.(type) {
	case string:
		if avroPrimitives[v] {
			return "", false
		}
		return shortName(v), true
	case map[string]any:
		if name, ok := v["name"].(string); ok && name != "" {
			return shortName(name), true
		}
		if vt, ok := v["type"].(string); ok && !avroPrimitives[vt] && !structuralTypes[vt] {
			return shortName(vt), true
		}
	}
	return "", false
}

// canon renders an Avro type as a compact, comparable string. Enum symbols are
// compared separately (see enumChanges), so an enum prints as its name. Inside
// array items and map values (elem) a named type is written by its short name
// whether it is defined there, defined elsewhere or only referenced (a type
// from another subject cannot be resolved here), so moving a definition or
// writing a reference short or full is not a change. Outside them the text is
// unchanged (gap #493).
func canon(t any, elem bool) string {
	if elem {
		if n, ok := namedTypeName(t); ok {
			return n // every spelling of a named type: one helper, no form missed
		}
	}
	switch v := t.(type) {
	case string:
		return v
	case map[string]any:
		vt, _ := v["type"].(string)
		if _, nested := v["type"].(map[string]any); nested {
			return canon(v["type"], elem) // {"type":{"type":"array",...}}
		}
		if lt, ok := v["logicalType"].(string); ok {
			if vt == "bytes" && lt == "decimal" {
				return fmt.Sprintf("bytes(decimal %v,%v)", v["precision"], v["scale"])
			}
			return vt + "(" + lt + ")"
		}
		switch vt {
		case "array":
			return "array<" + canon(v["items"], true) + ">"
		case "map":
			return "map<" + canon(v["values"], true) + ">"
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
			parts[i] = canon(x, elem)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return fmt.Sprint(t)
}

// enumChanges reports enums defined on both sides whose symbols differ, as a
// compact delta: a long enum must not push the real cause out of the summary.
func enumChanges(from, to map[string][]string) []fieldChange {
	var out []fieldChange
	for name, fs := range from {
		ts, ok := to[name]
		if !ok {
			continue
		}
		rm, add := setDiff(fs, ts), setDiff(ts, fs)
		if len(rm) == 0 && len(add) == 0 {
			continue
		}
		var parts []string
		for _, x := range capList(rm) {
			parts = append(parts, "-"+x)
		}
		for _, x := range capList(add) {
			parts = append(parts, "+"+x)
		}
		out = append(out, fieldChange{Field: "enum " + name, Kind: "changed", From: "symbols", To: strings.Join(parts, " ")})
	}
	return out
}

// setDiff returns the elements of a that are not in b, in a's order.
func setDiff(a, b []string) []string {
	in := map[string]bool{}
	for _, x := range b {
		in[x] = true
	}
	var out []string
	for _, x := range a {
		if !in[x] {
			out = append(out, x)
		}
	}
	return out
}

const maxEnumSymbolsShown = 10

func capList(l []string) []string {
	if len(l) <= maxEnumSymbolsShown {
		return l
	}
	return append(l[:maxEnumSymbolsShown:maxEnumSymbolsShown], fmt.Sprintf("…(%d more)", len(l)-maxEnumSymbolsShown))
}

func anySlice(v any) []any {
	s, _ := v.([]any)
	return s
}

// notConfigured is a schema tool's answer when the deployment has no Schema
// Registry (SCHEMA_REGISTRY_URL empty or off): a finding for the agent, not
// an error to retry.
func notConfigured() tools.Result {
	return res("Schema Registry not configured (SCHEMA_REGISTRY_URL is empty or off)", map[string]any{"configured": false})
}
