package agent

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/tools"
)

// rawFix wraps one step's raw params JSON (a string, null, an object...) in a
// propose_fix input. paramsJSON is inserted as is, so a test can send shapes
// the model's strict schema would not allow.
func rawFix(action, paramsJSON string) json.RawMessage {
	return json.RawMessage(`{"title":"t","steps":[{"action":"` + action + `","params":` + paramsJSON + `,"description":"d"}],"risk":"low","rollback":"undo it"}`)
}

// quoted returns s as a JSON string literal.
func quoted(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestProposeFixParamsEdgeCases(t *testing.T) {
	cases := []struct {
		name   string
		params string // raw JSON for the params field
		want   string // fragment of the error
	}{
		{"duplicate key", quoted(`{"connector":"pg-orders-connector","connector":"billing"}`), `more than once`},
		{"duplicate key in a nested object", quoted(`{"x":{"a":1,"a":2}}`), `more than once`},
		{"case-variant key", quoted(`{"Connector":"pg-orders-connector"}`), `connector`},
		{"null params", `null`, `params is required`},
		{"empty string params", `""`, `params must be valid JSON`},
		{"double-encoded params", quoted(quoted(`{"connector":"pg-orders-connector"}`)), `params must be a JSON object`},
		{"trailing garbage", quoted(`{"connector":"pg-orders-connector"} trailing`), `valid JSON`},
		{"two objects", quoted(`{"connector":"pg-orders-connector"}{"connector":"billing"}`), `valid JSON`},
		{"truncated object", quoted(`{"connector":"pg-orders-connector"`), `valid JSON`},
		{"a number", `7`, `params must be a JSON object`},
		{"a bare array", `[]`, `params must be a JSON object`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, proposals := proposeRunner(t, realActions(t))
			res := r.runRoomTool(context.Background(), toolUse{id: "tu_1", name: "propose_fix", input: rawFix("connect_restart_connector", c.params)})
			if !res.isError {
				t.Fatalf("expected a tool error, got %q", res.content)
			}
			if !strings.Contains(res.content, c.want) {
				t.Errorf("error %q lacks %q", res.content, c.want)
			}
			if proposals.created.Title != "" {
				t.Fatalf("nothing may be stored, got %+v", proposals.created)
			}
		})
	}

	// A missing params field never reaches Run: the closed step schema needs it.
	t.Run("missing params", func(t *testing.T) {
		r, proposals := proposeRunner(t, realActions(t))
		raw := json.RawMessage(`{"title":"t","steps":[{"action":"connect_restart_connector","description":"d"}],"risk":"low","rollback":"undo it"}`)
		res := r.runRoomTool(context.Background(), toolUse{id: "tu_1", name: "propose_fix", input: raw})
		if !res.isError || !strings.Contains(res.content, `missing required field "params"`) {
			t.Fatalf("got %q", res.content)
		}
		if proposals.created.Title != "" {
			t.Fatal("nothing may be stored")
		}
	})
}

// A repeated key is allowed when it sits in different objects, and a key
// repeated across array elements is not a duplicate.
func TestDuplicateKeyOnlyWithinOneObject(t *testing.T) {
	for _, ok := range []string{
		`{"a":1,"b":{"a":2},"c":[{"a":3},{"a":4}]}`,
		`{"a":"a","b":"a"}`,
		`{}`,
	} {
		if k, dup := duplicateKey([]byte(ok)); dup {
			t.Errorf("%s: reported duplicate %q", ok, k)
		}
	}
	for _, bad := range []string{
		`{"a":1,"a":2}`,
		`{"a":{"b":1,"b":2}}`,
		`{"list":[{"k":1,"k":2}]}`,
		`{"a":[1,2],"a":3}`,
	} {
		if _, dup := duplicateKey([]byte(bad)); !dup {
			t.Errorf("%s: duplicate not reported", bad)
		}
	}
}

// Integers keep their exact digits: the stored text is the text the model
// sent, not a float64 round trip, and the schema check does not round either.
func TestProposeFixKeepsIntegerPrecision(t *testing.T) {
	reg := actions.NewRegistry()
	if err := reg.Register(&fakeAction{name: "set_offset", layer: "kafka-connect", params: map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"offset": map[string]any{"type": "integer"}},
		"additionalProperties": false,
		"required":             []string{"offset"},
	}}); err != nil {
		t.Fatal(err)
	}
	const big = `{"offset":9223372036854775807}` // max int64; float64 would round it
	r, proposals := proposeRunner(t, reg)
	res := r.runRoomTool(context.Background(), toolUse{id: "tu_1", name: "propose_fix", input: rawFix("set_offset", quoted(big))})
	if res.isError {
		t.Fatalf("refused: %s", res.content)
	}
	if got := string(proposals.created.Steps[0].Params); got != big {
		t.Fatalf("stored params = %s, want %s", got, big)
	}
	for _, bad := range []string{`{"offset":1.0}`, `{"offset":1e2}`, `{"offset":"9223372036854775807"}`} {
		r, proposals := proposeRunner(t, reg)
		res := r.runRoomTool(context.Background(), toolUse{id: "tu_2", name: "propose_fix", input: rawFix("set_offset", quoted(bad))})
		if !res.isError || proposals.created.Title != "" {
			t.Errorf("%s: want a refusal and nothing stored, got %q", bad, res.content)
		}
	}
}

// observingAction records the StepChecked calls it receives.
type observingAction struct {
	fakeAction
	checked []error
}

func (a *observingAction) StepChecked(_ json.RawMessage, err error) {
	a.checked = append(a.checked, err)
}

// A step refused before Validate is reported to an action that observes
// steps; a valid step, and a step refused by Validate itself, are not (Validate
// already sees those).
func TestProposeFixReportsEarlyRefusalsToObserver(t *testing.T) {
	reg := actions.NewRegistry()
	act := &observingAction{fakeAction: fakeAction{name: "do_it", layer: "kafka-connect", params: map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"n": map[string]any{"type": "integer"}},
		"additionalProperties": false,
		"required":             []string{"n"},
	}}}
	if err := reg.Register(act); err != nil {
		t.Fatal(err)
	}
	var _ StepObserver = act
	tool := NewProposeFixTool(reg, &fakeProposals{id: "p_1"})
	run := func(paramsJSON string) error {
		_, err := tool.Run(context.Background(), rawFix("do_it", paramsJSON))
		return err
	}
	if err := run(quoted(`{"n":"x"}`)); !errors.Is(err, tools.ErrBadInput) || len(act.checked) != 1 || act.checked[0] == nil {
		t.Fatalf("schema failure: err %v, observed %v", err, act.checked)
	}
	if err := run(quoted(`not json`)); err == nil || len(act.checked) != 2 {
		t.Fatalf("decode failure: err %v, observed %d", err, len(act.checked))
	}
	if err := run(quoted(`{"n":1}`)); err != nil || len(act.checked) != 2 {
		t.Fatalf("valid step: err %v, observed %d", err, len(act.checked))
	}
	// An unknown action has no action to tell.
	if _, err := tool.Run(context.Background(), rawFix("nope", `"{}"`)); err == nil || len(act.checked) != 2 {
		t.Fatalf("unknown action: err %v, observed %d", err, len(act.checked))
	}
}

// supportedSchemaKeywords are the JSON Schema keywords validateInput enforces
// (plus description, which it ignores on purpose). An action schema that uses
// any other keyword would look checked in its ParamsSchema but would not be:
// the agent no longer sends it to the API either, so nothing else would
// enforce it. Add support to validateInput before using a new keyword.
var supportedSchemaKeywords = map[string]bool{
	"type": true, "properties": true, "required": true, "additionalProperties": true,
	"items": true, "enum": true, "description": true,
}

func unsupportedKeywords(path string, schema map[string]any, out *[]string) {
	for kw, v := range schema {
		if !supportedSchemaKeywords[kw] {
			*out = append(*out, path+"."+kw)
		}
		switch kw {
		case "properties":
			props, _ := v.(map[string]any)
			for name, sub := range props {
				if m, ok := sub.(map[string]any); ok {
					unsupportedKeywords(path+".properties."+name, m, out)
				}
			}
		case "items":
			if m, ok := v.(map[string]any); ok {
				unsupportedKeywords(path+".items", m, out)
			}
		}
	}
}

func TestActionSchemasUseOnlySupportedKeywords(t *testing.T) {
	areg := actions.NewRegistry()
	if err := actions.Register(areg, actions.Deps{}); err != nil {
		t.Fatal(err)
	}
	for _, a := range areg.All() {
		schema := a.ParamsSchema()
		if schema == nil {
			t.Errorf("%s: no ParamsSchema; propose_fix could not check its params", a.Name())
			continue
		}
		if typ, _ := schema["type"].(string); typ != "object" {
			t.Errorf("%s: ParamsSchema type is %q, want object", a.Name(), typ)
		}
		var bad []string
		unsupportedKeywords(a.Name(), schema, &bad)
		sort.Strings(bad)
		if len(bad) > 0 {
			t.Errorf("%s: keywords validateInput does not enforce: %v", a.Name(), bad)
		}
	}
	// The guard itself catches what it should.
	var bad []string
	unsupportedKeywords("x", map[string]any{
		"type": "object",
		"properties": map[string]any{
			"n": map[string]any{"type": "integer", "minimum": 0},
			"s": map[string]any{"type": "string", "pattern": "^a"},
			"l": map[string]any{"type": "array", "items": map[string]any{"const": 1}},
		},
		"anyOf": []any{},
	}, &bad)
	sort.Strings(bad)
	if got := strings.Join(bad, ","); got != "x.anyOf,x.properties.l.items.const,x.properties.n.minimum,x.properties.s.pattern" {
		t.Fatalf("guard found %s", got)
	}
}
