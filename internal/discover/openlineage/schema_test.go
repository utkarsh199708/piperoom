package openlineage

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// A small JSON Schema (2020-12) checker for the keywords the vendored
// OpenLineage files use: $ref, allOf, anyOf, oneOf, not, type, enum,
// format (date-time, uri, uuid), minimum, required, properties,
// additionalProperties and items. No module is added for it (AGENTS.md
// rule 10); annotations (description, example, deprecated) are ignored.

type schemaSet struct {
	docs map[string]any // $id without fragment -> document
}

func loadSchemas(t *testing.T) *schemaSet {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("testdata", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no vendored schemas: %v", err)
	}
	s := &schemaSet{docs: map[string]any{}}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err := json.Unmarshal(b, &doc); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		id, _ := doc["$id"].(string)
		if id == "" {
			t.Fatalf("%s: no $id", f)
		}
		s.docs[id] = doc
	}
	return s
}

// resolve follows ref from the document base: "#/$defs/X" or an absolute
// "<id>#/$defs/X".
func (s *schemaSet) resolve(base, ref string) (string, any, error) {
	uri, ptr, _ := strings.Cut(ref, "#")
	if uri == "" {
		uri = base
	}
	node, ok := s.docs[uri]
	if !ok {
		return "", nil, fmt.Errorf("unknown schema %q (not vendored)", uri)
	}
	for _, part := range strings.Split(strings.TrimPrefix(ptr, "/"), "/") {
		if part == "" {
			continue
		}
		m, ok := node.(map[string]any)
		if !ok {
			return "", nil, fmt.Errorf("bad pointer %q", ref)
		}
		if node, ok = m[part]; !ok {
			return "", nil, fmt.Errorf("bad pointer %q", ref)
		}
	}
	return uri, node, nil
}

var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func (s *schemaSet) check(base string, schema, inst any, path string) error {
	if b, ok := schema.(bool); ok {
		if !b {
			return fmt.Errorf("%s: not allowed", path)
		}
		return nil
	}
	sc, _ := schema.(map[string]any)
	if ref, ok := sc["$ref"].(string); ok {
		nb, sub, err := s.resolve(base, ref)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if err := s.check(nb, sub, inst, path); err != nil {
			return err
		}
	}
	if typ, ok := sc["type"].(string); ok && !isType(typ, inst) {
		return fmt.Errorf("%s: want %s, got %T", path, typ, inst)
	}
	if enum, ok := sc["enum"].([]any); ok {
		found := false
		for _, v := range enum {
			found = found || v == inst
		}
		if !found {
			return fmt.Errorf("%s: %v not in %v", path, inst, enum)
		}
	}
	if str, ok := inst.(string); ok {
		switch sc["format"] {
		case "uri":
			if u, err := url.Parse(str); err != nil || u.Scheme == "" {
				return fmt.Errorf("%s: %q is not a URI", path, str)
			}
		case "uuid":
			if !uuidRE.MatchString(str) {
				return fmt.Errorf("%s: %q is not a UUID", path, str)
			}
		case "date-time":
			if _, err := time.Parse(time.RFC3339Nano, str); err != nil {
				return fmt.Errorf("%s: %q is not a date-time", path, str)
			}
		}
	}
	if min, ok := sc["minimum"].(float64); ok {
		if n, isNum := inst.(float64); isNum && n < min {
			return fmt.Errorf("%s: %v < %v", path, n, min)
		}
	}
	if obj, ok := inst.(map[string]any); ok {
		if req, ok := sc["required"].([]any); ok {
			for _, r := range req {
				if _, has := obj[r.(string)]; !has {
					return fmt.Errorf("%s: missing %q", path, r)
				}
			}
		}
		props, _ := sc["properties"].(map[string]any)
		for k, v := range obj {
			if p, ok := props[k]; ok {
				if err := s.check(base, p, v, path+"."+k); err != nil {
					return err
				}
			} else if ap, ok := sc["additionalProperties"]; ok {
				if err := s.check(base, ap, v, path+"."+k); err != nil {
					return err
				}
			}
		}
	}
	if arr, ok := inst.([]any); ok {
		if items, ok := sc["items"]; ok {
			for i, v := range arr {
				if err := s.check(base, items, v, fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return err
				}
			}
		}
	}
	if all, ok := sc["allOf"].([]any); ok {
		for _, sub := range all {
			if err := s.check(base, sub, inst, path); err != nil {
				return err
			}
		}
	}
	if anyOf, ok := sc["anyOf"].([]any); ok {
		var first error
		pass := false
		for _, sub := range anyOf {
			if err := s.check(base, sub, inst, path); err == nil {
				pass = true
			} else if first == nil {
				first = err
			}
		}
		if !pass {
			return fmt.Errorf("anyOf: %w", first)
		}
	}
	if oneOf, ok := sc["oneOf"].([]any); ok {
		n := 0
		var errs []string
		for _, sub := range oneOf {
			if err := s.check(base, sub, inst, path); err == nil {
				n++
			} else {
				errs = append(errs, err.Error())
			}
		}
		if n != 1 {
			return fmt.Errorf("%s: oneOf matched %d (%s)", path, n, strings.Join(errs, "; "))
		}
	}
	if not, ok := sc["not"]; ok {
		if s.check(base, not, inst, path) == nil {
			return fmt.Errorf("%s: matches a not", path)
		}
	}
	return nil
}

func isType(typ string, v any) bool {
	switch typ {
	case "object":
		_, ok := v.(map[string]any)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	case "string":
		_, ok := v.(string)
		return ok
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "integer":
		n, ok := v.(float64)
		return ok && n == math.Trunc(n)
	case "number":
		_, ok := v.(float64)
		return ok
	}
	return false
}

// facetFiles are the vendored facet schemas by the facets object they
// describe: each file's root schema names its facet key.
var facetFiles = map[string][]string{
	"job": {"https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json"},
	"run": {"urn:piperoom:spec:facets:1-0-0:PiperoomActionRunFacet.json"},
	"dataset": {
		"https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json",
		"https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json",
		"https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json",
		"https://openlineage.io/spec/facets/1-0-0/TagsDatasetFacet.json",
		"https://openlineage.io/spec/facets/1-0-1/OwnershipDatasetFacet.json",
	},
}

// validateEvent checks body against the event schema, every facet against
// its vendored facet schema, and that every _schemaURL names a vendored
// file (so the versions the emitter claims are the versions tested).
func (s *schemaSet) validateEvent(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var ev map[string]any
	if err := json.Unmarshal(body, &ev); err != nil {
		t.Fatalf("event is not JSON: %v", err)
	}
	const core = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
	if err := s.check(core, s.docs[core], ev, "$"); err != nil {
		t.Fatalf("event: %v\n%s", err, body)
	}
	facets := func(kind string, obj any, path string) {
		f, _ := obj.(map[string]any)
		fs, _ := f["facets"].(map[string]any)
		for _, id := range facetFiles[kind] {
			if err := s.check(id, s.docs[id], fs, path); err != nil {
				t.Fatalf("%s facets: %v", path, err)
			}
		}
		for name, v := range fs {
			u, _ := v.(map[string]any)["_schemaURL"].(string)
			id, ptr, _ := strings.Cut(u, "#")
			if _, _, err := s.resolve(id, "#"+ptr); err != nil {
				t.Fatalf("%s.%s: _schemaURL %q: %v", path, name, u, err)
			}
		}
	}
	facets("job", ev["job"], "$.job")
	facets("run", ev["run"], "$.run")
	for _, side := range []string{"inputs", "outputs"} {
		list, _ := ev[side].([]any)
		for i, d := range list {
			facets("dataset", d, fmt.Sprintf("$.%s[%d]", side, i))
		}
	}
	return ev
}

// The checker refuses what it must: a wrong enum, a missing runId, a bad
// UUID and a facet without _producer.
func TestSchemaCheckerRefuses(t *testing.T) {
	s := loadSchemas(t)
	ok := `{"eventType":"COMPLETE","eventTime":"2026-10-22T10:00:00Z","producer":"urn:piperoom:dev","schemaURL":"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent","run":{"runId":"0192f3a0-0000-7000-8000-000000000000"},"job":{"namespace":"n","name":"j"}}`
	const core = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
	var v any
	_ = json.Unmarshal([]byte(ok), &v)
	if err := s.check(core, s.docs[core], v, "$"); err != nil {
		t.Fatalf("valid event refused: %v", err)
	}
	for name, bad := range map[string]string{
		"enum":   strings.Replace(ok, "COMPLETE", "DONE", 1),
		"runId":  strings.Replace(ok, `"runId":"0192f3a0-0000-7000-8000-000000000000"`, `"x":1`, 1),
		"uuid":   strings.Replace(ok, "0192f3a0-0000", "zz", 1),
		"time":   strings.Replace(ok, "2026-10-22T10:00:00Z", "yesterday", 1),
		"facet":  strings.Replace(ok, `"name":"j"}`, `"name":"j","facets":{"jobType":{"_schemaURL":"https://x"}}}`, 1),
		"jobRun": strings.Replace(ok, `"job":{"namespace":"n","name":"j"}`, `"dataset":{}`, 1),
	} {
		var b any
		_ = json.Unmarshal([]byte(bad), &b)
		if s.check(core, s.docs[core], b, "$") == nil {
			t.Errorf("%s: invalid event accepted", name)
		}
	}
}
