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
	var fromRoot, toRoot any
	if err := json.Unmarshal([]byte(fromSchema), &fromRoot); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(toSchema), &toRoot); err != nil {
		return nil, err
	}
	fromFields, toFields := recordFields(fromRoot), recordFields(toRoot)
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
	changes = append(changes, diffEnumSymbols(schemaEnums(fromRoot), schemaEnums(toRoot))...)
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

// recordFields flattens every record field in a decoded Avro schema into name→canonical type.
func recordFields(root any) map[string]string {
	out := map[string]string{}
	walkFields(root, out)
	return out
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
		switch vt {
		case "array":
			if items, ok := v["items"]; ok {
				return "array<" + canonElem(items) + ">"
			}
		case "map":
			if values, ok := v["values"]; ok {
				return "map<" + canonElem(values) + ">"
			}
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

// avroPrimitives are the Avro type names that are never a named type.
var avroPrimitives = map[string]bool{
	"null": true, "boolean": true, "int": true, "long": true,
	"float": true, "double": true, "bytes": true, "string": true,
}

// avroNamedKinds are the "type" values that make an object a named type
// definition; any other object with a "name" property is not one.
var avroNamedKinds = map[string]bool{"record": true, "error": true, "enum": true, "fixed": true}

// namedTypeName reports the short name a type node names when it is a named
// type: a definition, a bare reference or an object-wrapped reference
// ({"type":"com.x.Addr"}, at any wrapping depth). Short names on purpose: a
// Schema Registry reference may be spelled short in one version and full in
// the next, and both must compare equal (a namespace-only change is gap #493).
func namedTypeName(t any) (string, bool) {
	switch v := t.(type) {
	case string:
		if v == "" || avroPrimitives[v] || v == "array" || v == "map" {
			return "", false
		}
		return shortName(v), true
	case map[string]any:
		switch vt := v["type"].(type) {
		case string:
			if avroNamedKinds[vt] {
				name, ok := v["name"].(string)
				if !ok || name == "" {
					return "", false
				}
				return shortName(name), true
			}
			return namedTypeName(vt)
		case map[string]any:
			return namedTypeName(vt)
		}
	}
	return "", false
}

// canonElem renders an array item or map value type. Unlike canonType it
// renders every spelling of one type the same way (definition, bare,
// full-name or object-wrapped reference; a nested {"type":{...}} wrapper),
// because inside a container a definition moves between fields freely. It is
// used only in item/value position: in field position canonType keeps main's
// rendering, which the diff corpus pins.
func canonElem(t any) string {
	if name, ok := namedTypeName(t); ok {
		// Named first: a fixed with a logicalType renders by name, as its references do.
		return name
	}
	switch v := t.(type) {
	case string:
		return v
	case map[string]any:
		switch vt := v["type"].(type) {
		case map[string]any, []any:
			return canonElem(vt)
		case string:
			switch vt {
			case "array":
				return "array<" + canonElem(v["items"]) + ">"
			case "map":
				return "map<" + canonElem(v["values"]) + ">"
			}
			// A primitive ignores any "name" property: it is not a named type.
			return vt + elemLogical(v, vt)
		}
		return "record"
	case []any:
		// Member order does not change which values a union in a container
		// accepts. A union directly inside a union is invalid Avro; flatten it.
		seen := map[string]bool{}
		var parts []string
		var add func(members []any)
		add = func(members []any) {
			for _, m := range members {
				if inner, ok := unionMembers(m); ok {
					add(inner)
					continue
				}
				if c := canonElem(m); !seen[c] {
					seen[c] = true
					parts = append(parts, c)
				}
			}
		}
		add(v)
		sort.Strings(parts)
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return fmt.Sprint(t)
}

// avroLogicalBase maps each logical type the Avro spec defines (on a
// primitive) to the only primitive it may annotate. decimal and duration on a
// fixed never reach here: a fixed renders by name.
var avroLogicalBase = map[string]string{
	"decimal": "bytes", "uuid": "string", "date": "int",
	"time-millis": "int", "time-micros": "long",
	"timestamp-millis": "long", "timestamp-micros": "long",
	"local-timestamp-millis": "long", "local-timestamp-micros": "long",
}

// elemLogical renders the logical-type suffix of a primitive in item/value
// position, e.g. "(uuid)" or "(decimal 10,2)". Readers ignore a logical type
// that is unknown, on the wrong primitive or with invalid decimal parameters,
// so those render as the bare primitive. A missing decimal scale is 0.
func elemLogical(v map[string]any, vt string) string {
	lt, _ := v["logicalType"].(string)
	if base, ok := avroLogicalBase[lt]; !ok || base != vt {
		return ""
	}
	if lt != "decimal" {
		return "(" + lt + ")"
	}
	precision, ok := avroInt(v["precision"])
	if !ok || precision <= 0 {
		return ""
	}
	scale := int64(0)
	if raw, present := v["scale"]; present {
		if scale, ok = avroInt(raw); !ok || scale < 0 || scale > precision {
			return ""
		}
	}
	return fmt.Sprintf("(decimal %d,%d)", precision, scale)
}

// avroInt reads a JSON number that holds a whole number.
func avroInt(x any) (int64, bool) {
	f, ok := x.(float64)
	if !ok || f != math.Trunc(f) || math.Abs(f) > 1<<53 {
		return 0, false
	}
	return int64(f), true
}

// unionMembers returns the members of a union given bare or object-wrapped
// at any depth ({"type":[...]}, {"type":{"type":[...]}}). Other properties on
// a wrapper are ignored: a union cannot carry any.
func unionMembers(t any) ([]any, bool) {
	for {
		switch v := t.(type) {
		case []any:
			return v, true
		case map[string]any:
			t = v["type"]
		default:
			return nil, false
		}
	}
}

// shortName drops the namespace from a dotted Avro name.
func shortName(name string) string {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// maxSymbolsShown caps each side of an enum symbol delta, so a long enum
// cannot push the real cause out of a capped summary.
const maxSymbolsShown = 10

// diffEnumSymbols compares the symbols of every enum defined in both schemas,
// keyed by full name. An enum defined on one side only is not reported: the
// field that holds it already has its own added/removed line. Reordering is
// not a change: readers resolve enum symbols by name.
func diffEnumSymbols(from, to map[string][]string) []fieldChange {
	var changes []fieldChange
	for full, toSyms := range to {
		fromSyms, ok := from[full]
		if !ok || fromSyms == nil || toSyms == nil {
			continue
		}
		delta := symbolDelta(fromSyms, toSyms)
		if delta == "" {
			continue
		}
		label := shortName(full)
		if !uniqueShort(label, from, to) {
			label = full
		}
		changes = append(changes, fieldChange{Field: "enum " + label, Kind: "changed", From: "symbols", To: delta})
	}
	return changes
}

// uniqueShort reports whether short names exactly one enum across both schemas.
func uniqueShort(short string, from, to map[string][]string) bool {
	full := ""
	for _, m := range []map[string][]string{from, to} {
		for f := range m {
			if shortName(f) != short {
				continue
			}
			if full != "" && full != f {
				return false
			}
			full = f
		}
	}
	return true
}

// symbolDelta renders removed and added symbols, e.g. "-B +C", or "" if the
// two lists hold the same symbols.
func symbolDelta(from, to []string) string {
	inFrom, inTo := map[string]bool{}, map[string]bool{}
	for _, s := range from {
		inFrom[s] = true
	}
	for _, s := range to {
		inTo[s] = true
	}
	var parts []string
	side := func(syms []string, other map[string]bool, sign string) {
		n := 0
		for _, s := range syms {
			if other[s] {
				continue
			}
			if n < maxSymbolsShown {
				parts = append(parts, sign+s)
			}
			n++
		}
		if n > maxSymbolsShown {
			parts = append(parts, fmt.Sprintf("(%d more %s)", n-maxSymbolsShown, sign))
		}
	}
	side(from, inTo, "-")
	side(to, inFrom, "+")
	return strings.Join(parts, " ")
}

// schemaEnums maps the full name of every enum defined in a decoded schema to
// its symbols. A full name defined twice with different symbols maps to nil
// (invalid Avro, so no symbol verdict is given for it).
func schemaEnums(root any) map[string][]string {
	out := map[string][]string{}
	collectEnums(root, "", out)
	return out
}

// fullName applies Avro's namespace rules: a dotted name is already full, else
// the "namespace" attribute, else the enclosing namespace.
func fullName(v map[string]any, name, enclosing string) string {
	if strings.Contains(name, ".") {
		return name
	}
	ns := enclosing
	if a, ok := v["namespace"].(string); ok {
		ns = a
	}
	if ns == "" {
		return name
	}
	return ns + "." + name
}

// namespaceOf is the namespace part of a full name.
func namespaceOf(full string) string {
	if i := strings.LastIndexByte(full, '.'); i >= 0 {
		return full[:i]
	}
	return ""
}

func collectEnums(node any, ns string, out map[string][]string) {
	switch v := node.(type) {
	case []any:
		for _, x := range v {
			collectEnums(x, ns, out)
		}
	case map[string]any:
		switch vt := v["type"].(type) {
		case map[string]any, []any:
			collectEnums(vt, ns, out)
		case string:
			name, _ := v["name"].(string)
			switch vt {
			case "record", "error":
				inner := ns
				if name != "" {
					inner = namespaceOf(fullName(v, name, ns))
				}
				fields, _ := v["fields"].([]any)
				for _, f := range fields {
					if fm, ok := f.(map[string]any); ok {
						collectEnums(fm["type"], inner, out)
					}
				}
			case "enum":
				if name == "" {
					return
				}
				var syms []string
				list, _ := v["symbols"].([]any)
				for _, s := range list {
					if str, ok := s.(string); ok {
						syms = append(syms, str)
					}
				}
				full := fullName(v, name, ns)
				if prev, seen := out[full]; seen {
					if prev == nil || symbolDelta(prev, syms) != "" || len(prev) != len(syms) {
						out[full] = nil
					}
					return
				}
				if syms == nil {
					syms = []string{}
				}
				out[full] = syms
			case "array":
				collectEnums(v["items"], ns, out)
			case "map":
				collectEnums(v["values"], ns, out)
			}
		}
	}
}

// notConfigured is a schema tool's answer when the deployment has no Schema
// Registry (SCHEMA_REGISTRY_URL empty or off): a finding for the agent, not
// an error to retry.
func notConfigured() tools.Result {
	return res("Schema Registry not configured (SCHEMA_REGISTRY_URL is empty or off)", map[string]any{"configured": false})
}
