package topology

import (
	"encoding/json"
	"slices"
	"strings"

	"piperoom/internal/clients"
)

// What a connector's transforms chain does to value field names (X7a,
// docs/design/lineage.md section 2), and the field list of a topic's latest
// Avro value schema. Names only: no config value other than the transform
// classes and their field lists is read, and no schema default or doc.

const (
	unwrapClass            = "io.debezium.transforms.ExtractNewRecordState"
	replaceFieldValueClass = "org.apache.kafka.connect.transforms.ReplaceField$Value"
	// debeziumJDBCSinkClass reads Debezium envelopes itself, so its
	// after.<f> -> <f> is proven without an unwrap transform.
	debeziumJDBCSinkClass = "io.debezium.connector.jdbc.JdbcSinkConnector"
	// envelopeAfter prefixes a table column inside a Debezium envelope.
	envelopeAfter = "after."
)

// fieldNeutral transforms change no value field name: they route records to
// other topics or touch the key only. Every other class Piperoom does not
// model makes the maps after it inferred.
var fieldNeutral = []string{
	regexRouterClass, logicalTableRouterType,
	"org.apache.kafka.connect.transforms.TimestampRouter",
	"org.apache.kafka.connect.transforms.ReplaceField$Key",
}

// smtStep is one modelled transform: an unwrap, or a ReplaceField$Value.
type smtStep struct {
	unwrap           bool
	renames          map[string]string
	exclude, include map[string]bool
}

// flowField is one upstream column on its way through a hop: from is its
// name in the hop's input dataset, name its name after the transforms so far.
type flowField struct {
	from, name       string
	renamed, dropped bool
}

// parseChain reads the transforms chain in order. modeled is false when the
// chain holds a transform whose effect on field names is unknown (another
// class, a predicate, a malformed renames list): the maps are then inferred.
func parseChain(cfg map[string]string) (steps []smtStep, modeled bool) {
	modeled = true
	for _, alias := range splitCSV(cfg["transforms"]) {
		pre := "transforms." + alias + "."
		class := strings.TrimSpace(cfg[pre+"type"])
		if slices.Contains(fieldNeutral, class) {
			continue
		}
		if strings.TrimSpace(cfg[pre+"predicate"]) != "" {
			modeled = false // applied to some records only
			continue
		}
		switch class {
		case unwrapClass:
			steps = append(steps, smtStep{unwrap: true})
		case replaceFieldValueClass:
			s, ok := replaceStep(cfg, pre)
			if !ok {
				modeled = false
				continue
			}
			steps = append(steps, s)
		default:
			modeled = false
		}
	}
	return steps, modeled
}

// replaceStep reads ReplaceField's lists; the older blacklist/whitelist names
// count when the new ones are unset. A rename without ':' is a config Connect
// refuses, so the step is not modelled.
func replaceStep(cfg map[string]string, pre string) (smtStep, bool) {
	list := func(key, old string) map[string]bool {
		v := cfg[pre+key]
		if strings.TrimSpace(v) == "" {
			v = cfg[pre+old]
		}
		set := map[string]bool{}
		for _, f := range splitCSV(v) {
			set[f] = true
		}
		return set
	}
	s := smtStep{renames: map[string]string{}, exclude: list("exclude", "blacklist"), include: list("include", "whitelist")}
	for _, r := range splitCSV(cfg[pre+"renames"]) {
		from, to, ok := strings.Cut(r, ":")
		from, to = strings.TrimSpace(from), strings.TrimSpace(to)
		if !ok || from == "" || to == "" {
			return smtStep{}, false
		}
		s.renames[from] = to
	}
	return s, true
}

// applyChain runs fields through steps, as Connect would: an unwrap takes
// after.<f> out of an envelope (a flat record passes unchanged); ReplaceField
// filters and renames top-level fields only, the filter on the name before
// the rename.
func applyChain(fields []flowField, steps []smtStep) []flowField {
	out := slices.Clone(fields)
	for _, s := range steps {
		for i := range out {
			f := &out[i]
			if f.dropped {
				continue
			}
			if s.unwrap {
				f.name = strings.TrimPrefix(f.name, envelopeAfter)
				continue
			}
			top, _, nested := strings.Cut(f.name, ".")
			if s.exclude[top] || len(s.include) > 0 && !s.include[top] {
				f.dropped = true
				continue
			}
			if to, ok := s.renames[f.name]; ok && !nested {
				f.name, f.renamed = to, true
			}
		}
	}
	return out
}

// sourceFlow is a source table's columns as its source connector writes them:
// inside a Debezium envelope (after.<column>), then through the connector's
// transforms. modeled is false for a connector that is not Debezium or whose
// chain is not fully modelled.
func sourceFlow(cols []string, cfg map[string]string) (fields []flowField, modeled bool) {
	deb := isDebezium(cfg)
	fields = make([]flowField, len(cols))
	for i, c := range cols {
		fields[i] = flowField{from: c, name: c}
		if deb {
			fields[i].name = envelopeAfter + c
		}
	}
	steps, modeled := parseChain(cfg)
	return applyChain(fields, steps), modeled && deb
}

// matchColumns maps the hop's upstream fields (of dataset input) to the
// downstream columns down by name: exact, then ignoring case (a warehouse
// may fold names), then without the envelope prefix (a writer that unwraps
// by itself; proven only when unwrapProven). Maps come in the downstream
// order, then the dropped fields in upstream order. inferred marks every map.
func matchColumns(input string, up []flowField, down []string, inferred, unwrapProven bool) []ColumnMap {
	exact, fold, leaf := map[string][]int{}, map[string][]int{}, map[string][]int{}
	for i, f := range up {
		if f.dropped {
			continue
		}
		exact[f.name] = append(exact[f.name], i)
		fold[strings.ToLower(f.name)] = append(fold[strings.ToLower(f.name)], i)
		l := strings.ToLower(strings.TrimPrefix(f.name, envelopeAfter))
		leaf[l] = append(leaf[l], i)
	}
	used := make([]bool, len(up))
	take := func(idx []int) int {
		for _, i := range idx {
			if !used[i] {
				used[i] = true
				return i
			}
		}
		return -1
	}
	mapping := EdgeMapping("")
	if inferred {
		mapping = MappingInferred
	}
	out := make([]ColumnMap, 0, len(down)+len(up))
	for _, d := range down {
		i := take(exact[d])
		if i < 0 {
			i = take(fold[strings.ToLower(d)])
		}
		m := mapping
		if i < 0 {
			if i = take(leaf[strings.ToLower(strings.TrimPrefix(d, envelopeAfter))]); i >= 0 && !unwrapProven {
				m = MappingInferred
			}
		}
		if i < 0 {
			out = append(out, ColumnMap{To: d, Transform: ColumnAdded, Mapping: mapping})
			continue
		}
		f := up[i]
		cm := ColumnMap{Input: input, From: f.from, To: d, Mapping: m}
		switch {
		case f.renamed:
			cm.Transform = ColumnRename
		case strings.HasPrefix(f.from, envelopeAfter) && !strings.HasPrefix(d, envelopeAfter):
			cm.Transform = ColumnUnwrap
		}
		out = append(out, cm)
	}
	for i, f := range up {
		if !used[i] {
			out = append(out, ColumnMap{Input: input, From: f.from, Transform: ColumnDropped, Mapping: mapping})
		}
	}
	return out
}

// avroValueFields returns the field paths of an Avro value schema: the
// fields of its top-level record, or, for a Debezium envelope (a record with
// an "after" record and an "op" field), after.<f> for each table column;
// envelope metadata (before, source, op, ts_ms, transaction) is not a column.
// ok is false for anything else (Protobuf, JSON Schema, a primitive). At most
// clients.MaxTableColumns fields.
func avroValueFields(schema string) (fields []string, ok bool) {
	var root any
	if err := json.Unmarshal([]byte(schema), &root); err != nil {
		return nil, false
	}
	named := map[string]map[string]any{}
	registerNamed(root, "", named, 0)
	rec := resolveRecord(root, named, 0)
	if rec == nil {
		return nil, false
	}
	top := recordFields(rec)
	var after map[string]any
	hasOp := false
	for _, f := range top {
		switch f.name {
		case "after":
			after = resolveRecord(f.typ, named, 0)
		case "op":
			hasOp = true
		}
	}
	prefix := ""
	if after != nil && hasOp {
		top, prefix = recordFields(after), envelopeAfter
	}
	for _, f := range top {
		if len(fields) == clients.MaxTableColumns {
			break
		}
		fields = append(fields, prefix+f.name)
	}
	return fields, true
}

type avroField struct {
	name string
	typ  any
}

func recordFields(rec map[string]any) []avroField {
	list, _ := rec["fields"].([]any)
	out := make([]avroField, 0, len(list))
	for _, f := range list {
		m, _ := f.(map[string]any)
		if name, _ := m["name"].(string); name != "" {
			out = append(out, avroField{name: name, typ: m["type"]})
		}
	}
	return out
}

// maxAvroDepth bounds the walk over a schema read from the registry.
const maxAvroDepth = 32

// registerNamed indexes every record of the schema by its full and its short
// name, so a later reference by name ("after": ["null", "Value"]) resolves.
func registerNamed(v any, ns string, named map[string]map[string]any, depth int) {
	if depth > maxAvroDepth {
		return
	}
	switch t := v.(type) {
	case []any:
		for _, b := range t {
			registerNamed(b, ns, named, depth+1)
		}
	case map[string]any:
		typ, _ := t["type"].(string)
		if name, _ := t["name"].(string); name != "" && (typ == "record" || typ == "error") {
			full := name
			if !strings.Contains(name, ".") {
				if n, _ := t["namespace"].(string); n != "" {
					ns = n
				}
				if ns != "" {
					full = ns + "." + name
				}
			} else {
				ns = name[:strings.LastIndexByte(name, '.')]
			}
			named[full] = t
			named[full[strings.LastIndexByte(full, '.')+1:]] = t
			for _, f := range recordFields(t) {
				registerNamed(f.typ, ns, named, depth+1)
			}
			return
		}
		for _, k := range []string{"type", "items", "values"} {
			if _, isString := t[k].(string); !isString && t[k] != nil {
				registerNamed(t[k], ns, named, depth+1)
			}
		}
	}
}

// resolveRecord returns the record v names: a record, a reference to one, a
// wrapper around one, or a union of null and one.
func resolveRecord(v any, named map[string]map[string]any, depth int) map[string]any {
	if depth > maxAvroDepth {
		return nil
	}
	switch t := v.(type) {
	case string:
		if r, ok := named[t]; ok {
			return r
		}
		return named[t[strings.LastIndexByte(t, '.')+1:]]
	case []any:
		var branch any
		for _, b := range t {
			if b == "null" {
				continue
			}
			if branch != nil {
				return nil // a union of two types is not one record
			}
			branch = b
		}
		return resolveRecord(branch, named, depth+1)
	case map[string]any:
		switch typ := t["type"].(type) {
		case string:
			if typ == "record" || typ == "error" {
				return t
			}
			if _, primitive := avroPrimitives[typ]; primitive {
				return nil
			}
			return resolveRecord(typ, named, depth+1)
		default:
			return resolveRecord(typ, named, depth+1)
		}
	}
	return nil
}

var avroPrimitives = map[string]struct{}{
	"null": {}, "boolean": {}, "int": {}, "long": {}, "float": {}, "double": {}, "bytes": {}, "string": {},
	"array": {}, "map": {}, "enum": {}, "fixed": {},
}
