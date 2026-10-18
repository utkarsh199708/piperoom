package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// stubConnect and stubRegistry answer only the reads that action Validate
// makes. The embedded nil interface makes any other call panic, which would
// show a Validate doing more than it should.
type stubConnect struct{ clients.ConnectClient }

func (stubConnect) ListConnectors(context.Context) ([]string, error) {
	return []string{"pg-orders-connector"}, nil
}
func (stubConnect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{State: "PAUSED"}, nil
}

type stubRegistry struct{ clients.SchemaRegistryClient }

func (stubRegistry) Subjects(context.Context) ([]string, error) {
	return []string{"pg.public.orders-value"}, nil
}

// realActions registers the six production write actions over stub clients.
func realActions(t *testing.T) *actions.Registry {
	t.Helper()
	reg := actions.NewRegistry()
	if err := actions.Register(reg, actions.Deps{Connect: stubConnect{}, Registry: stubRegistry{}}); err != nil {
		t.Fatal(err)
	}
	return reg
}

// proposeRunner builds a runner whose propose_fix runs over reg, and returns
// it with the fake proposal store so a test can see whether anything was kept.
func proposeRunner(t *testing.T, reg *actions.Registry) (*runner, *fakeProposals) {
	t.Helper()
	proposals := &fakeProposals{id: "p_1"}
	r, err := newRunner(Alert{}, Deps{
		Tools:     tools.NewRegistry(),
		Actions:   reg,
		Inbox:     &fakeInbox{},
		Sink:      &fakeSink{},
		Proposals: proposals,
		Logger:    discardLogger(),
	}, Options{})
	if err != nil {
		t.Fatalf("newRunner: %v", err)
	}
	r.toolList = buildToolList(r.deps.Tools, newRoomTools(r.deps, r.proposals))
	return r, proposals
}

// fixInput builds a propose_fix input the way the strict schema forces the
// model to send it: params as JSON text inside a string.
func fixInput(t *testing.T, action, paramsJSON string) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"title":    "fix",
		"steps":    []map[string]any{{"action": action, "params": paramsJSON, "description": "d"}},
		"risk":     "low",
		"rollback": "undo it",
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Every real action has a valid proposal: the strict wire form (params as
// JSON text) is accepted, stored as a params object and scoped to the action's
// layer, exactly as before.
func TestProposeFixAcceptsEveryRealAction(t *testing.T) {
	cases := []struct {
		name, action, params, layer string
	}{
		{"connect_restart_task", "connect_restart_task", `{"connector":"pg-orders-connector","task":0}`, "kafka-connect"},
		{"connect_restart_connector", "connect_restart_connector", `{"connector":"pg-orders-connector"}`, "kafka-connect"},
		{"connect_resume_connector", "connect_resume_connector", `{"connector":"pg-orders-connector"}`, "kafka-connect"},
		{"connect_pause_connector", "connect_pause_connector", `{"connector":"pg-orders-connector"}`, "kafka-connect"},
		{"connect_update_connector_config/one-key", "connect_update_connector_config", `{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":"string"}`, "kafka-connect"},
		{"connect_update_connector_config/n-key", "connect_update_connector_config", `{"changes":{"decimal.handling.mode":"string","tombstones.on.delete":"false"},"connector":"pg-orders-connector"}`, "kafka-connect"},
		{"schema_set_compatibility", "schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"BACKWARD"}`, "schema-registry"},
		{"schema_register", "schema_register", `{"subject":"pg.public.orders-value","schema":"\"string\""}`, "schema-registry"},
	}
	// Every registered action has at least one case, and no case names an
	// action the registry lacks.
	covered := map[string]bool{}
	for _, c := range cases {
		covered[c.action] = true
	}
	registered := map[string]bool{}
	for _, a := range realActions(t).All() {
		registered[a.Name()] = true
		if !covered[a.Name()] {
			t.Errorf("registry action %s has no case here: add it", a.Name())
		}
	}
	for a := range covered {
		if !registered[a] {
			t.Errorf("case action %s is not in the registry", a)
		}
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, proposals := proposeRunner(t, realActions(t))
			res := r.runRoomTool(context.Background(), toolUse{id: "tu_1", name: "propose_fix", input: fixInput(t, c.action, c.params)})
			if res.isError {
				t.Fatalf("valid proposal refused: %s", res.content)
			}
			st := proposals.created.Steps
			if len(st) != 1 || st[0].Action != c.action || st[0].Layer != c.layer {
				t.Fatalf("stored steps = %+v", st)
			}
			// Stored params are the object, not the JSON text in a string.
			if string(st[0].Params) != c.params {
				t.Fatalf("stored params = %s, want %s", st[0].Params, c.params)
			}
		})
	}
}

// An already-decoded params object (replays, other callers) is accepted too.
func TestProposeFixAcceptsParamsObject(t *testing.T) {
	r, proposals := proposeRunner(t, realActions(t))
	raw := json.RawMessage(`{"title":"t","steps":[{"action":"connect_restart_connector","params":{"connector":"pg-orders-connector"},"description":"d"}],"risk":"low","rollback":"none needed: restart is repeatable"}`)
	res := r.runRoomTool(context.Background(), toolUse{id: "tu_1", name: "propose_fix", input: raw})
	if res.isError {
		t.Fatalf("object params refused: %s", res.content)
	}
	if got := string(proposals.created.Steps[0].Params); got != `{"connector":"pg-orders-connector"}` {
		t.Fatalf("stored params = %s", got)
	}
}

// Bad proposals are refused with a tool error that names the problem, and
// nothing is stored.
func TestProposeFixRefusesBadSteps(t *testing.T) {
	cases := []struct {
		name, action, params string
		want                 []string // every fragment must appear in the error
	}{
		{"param has the wrong type", "connect_restart_task", `{"connector":"pg-orders-connector","task":"zero"}`, []string{"connect_restart_task", `"task"`, "expected integer"}},
		{"param is fractional", "connect_restart_task", `{"connector":"pg-orders-connector","task":1.5}`, []string{`"task"`, "expected integer"}},
		{"required param missing", "connect_restart_task", `{"connector":"pg-orders-connector"}`, []string{"connect_restart_task", `missing required field "task"`}},
		{"unknown param", "connect_restart_connector", `{"connector":"pg-orders-connector","force":true}`, []string{`unknown field "force"`}},
		{"unknown action", "drop_everything", `{}`, []string{`unknown action "drop_everything"`, "connect_restart_task", "schema_register"}},
		{"params is not JSON", "connect_restart_connector", `connector=pg-orders-connector`, []string{"params must be valid JSON"}},
		{"params is an array", "connect_restart_connector", `["pg-orders-connector"]`, []string{"params must be a JSON object"}},
		{"params is empty text", "connect_restart_connector", ``, []string{"params must"}},
		{"params fails the action Validate", "connect_update_connector_config", `{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":"nope"}`, []string{"not allowed"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, proposals := proposeRunner(t, realActions(t))
			res := r.runRoomTool(context.Background(), toolUse{id: "tu_1", name: "propose_fix", input: fixInput(t, c.action, c.params)})
			if !res.isError {
				t.Fatalf("expected a tool error, got %q", res.content)
			}
			for _, w := range c.want {
				if !strings.Contains(res.content, w) {
					t.Errorf("error %q does not mention %q", res.content, w)
				}
			}
			if len(proposals.created.Steps) != 0 || proposals.created.Title != "" {
				t.Fatalf("a refused proposal must store nothing, got %+v", proposals.created)
			}
		})
	}
}

// A param with an enum is checked against the enum, and a bad second step
// refuses the whole proposal, including the valid first step.
func TestProposeFixEnumAndLaterStep(t *testing.T) {
	reg := actions.NewRegistry()
	if err := reg.Register(&fakeAction{name: "set_mode", layer: "kafka-connect", params: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"mode": map[string]any{"type": "string", "enum": []string{"fast", "safe"}},
		},
		"additionalProperties": false,
		"required":             []string{"mode"},
	}}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ name, params string }{
		{"enum violation", `{"mode":"reckless"}`},
		{"enum wrong type", `{"mode":3}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, proposals := proposeRunner(t, reg)
			res := r.runRoomTool(context.Background(), toolUse{id: "tu_1", name: "propose_fix", input: fixInput(t, "set_mode", c.params)})
			if !res.isError || !strings.Contains(res.content, `"mode"`) {
				t.Fatalf("expected an error naming mode, got %q", res.content)
			}
			if proposals.created.Title != "" {
				t.Fatal("nothing may be stored")
			}
		})
	}

	r, proposals := proposeRunner(t, reg)
	raw := json.RawMessage(`{"title":"t","steps":[
		{"action":"set_mode","params":"{\"mode\":\"fast\"}","description":"ok"},
		{"action":"set_mode","params":"{}","description":"bad"}],"risk":"low","rollback":"set safe"}`)
	res := r.runRoomTool(context.Background(), toolUse{id: "tu_1", name: "propose_fix", input: raw})
	if !res.isError || !strings.Contains(res.content, "step 2") {
		t.Fatalf("expected an error naming step 2, got %q", res.content)
	}
	if proposals.created.Title != "" {
		t.Fatal("a proposal with one bad step must store nothing")
	}
}

// The schema check runs before the action's Validate, so a malformed step
// never reaches an action that may read customer systems.
func TestProposeFixSchemaCheckPrecedesValidate(t *testing.T) {
	reg := actions.NewRegistry()
	act := &countingAction{fakeAction: fakeAction{name: "do_it", layer: "kafka-connect", params: map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"n": map[string]any{"type": "integer"}},
		"additionalProperties": false,
		"required":             []string{"n"},
	}}}
	if err := reg.Register(act); err != nil {
		t.Fatal(err)
	}
	r, _ := proposeRunner(t, reg)
	r.runRoomTool(context.Background(), toolUse{id: "tu_1", name: "propose_fix", input: fixInput(t, "do_it", `{"n":"x"}`)})
	if act.validated != 0 {
		t.Fatalf("Validate ran %d times on params that fail the schema", act.validated)
	}
	r.runRoomTool(context.Background(), toolUse{id: "tu_2", name: "propose_fix", input: fixInput(t, "do_it", `{"n":1}`)})
	if act.validated != 1 {
		t.Fatalf("Validate ran %d times on valid params, want 1", act.validated)
	}
}

type countingAction struct {
	fakeAction
	validated int
}

func (a *countingAction) Validate(context.Context, json.RawMessage) error {
	a.validated++
	return nil
}

// The model sees each action's params in propose_fix's description, not in
// the strict schema.
func TestProposeFixDescriptionListsParams(t *testing.T) {
	tool := &proposeFixTool{actions: realActions(t)}
	d := tool.Description()
	for _, want := range []string{
		"connect_restart_task(connector:string [connector name], task:integer [task id]) [kafka-connect]",
		"connect_pause_connector(connector:string [connector name]) [kafka-connect]",
		"connect_update_connector_config(connector:string [connector name], changes?:{decimal.handling.mode?:precise|string, errors.deadletterqueue.context.headers.enable?:true, errors.log.enable?:true, time.precision.mode?:adaptive|adaptive_time_microseconds|connect, tombstones.on.delete?:false|true} [config keys to set, with their new values; all change in one request], key?:decimal.handling.mode|errors.deadletterqueue.context.headers.enable|errors.log.enable|time.precision.mode|tombstones.on.delete [old one-key form; use changes], value?:string [old one-key form; use changes]) [kafka-connect]",
		"schema_set_compatibility(subject:string [subject name], level:string [BACKWARD",
		"schema_register(subject:string [subject name], schema:string [Avro schema JSON string]) [schema-registry]",
	} {
		if !strings.Contains(d, want) {
			t.Errorf("description lacks %q:\n%s", want, d)
		}
	}
	if strings.Contains(d, `"additionalProperties"`) {
		t.Error("description still carries raw JSON schemas")
	}
	t.Logf("propose_fix description: %d bytes", len(d))
}

func TestParamsSignature(t *testing.T) {
	got := paramsSignature(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"b":    map[string]any{"type": "string"},
			"a":    map[string]any{"type": "integer"},
			"opt2": map[string]any{"type": "boolean"},
			"opt1": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"mode": map[string]any{"type": "string", "enum": []string{"x", "y"}},
		},
		"required": []string{"b", "a", "mode"},
	})
	want := "b:string, a:integer, mode:x|y, opt1?:array<string>, opt2?:boolean"
	if got != want {
		t.Fatalf("signature = %q, want %q", got, want)
	}
	if paramsSignature(nil) != "" {
		t.Fatal("nil schema must give an empty signature")
	}
}

// The strict propose_fix schema must not grow with the number of registered
// actions beyond the one enum entry each action adds. 10 fake actions with
// fat param schemas (enums, descriptions, nested objects) must cost only
// their names, which is what lets the next write action fit the byte budget.
func TestProposeFixStrictBytesDoNotGrowWithActions(t *testing.T) {
	reg, room := productionToolSet(t)
	base, baseN := strictSchemaBytes(t, buildToolList(reg, room))

	areg := actions.NewRegistry()
	if err := actions.Register(areg, actions.Deps{}); err != nil {
		t.Fatal(err)
	}
	extra := 0
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("fake_write_action_%02d", i)
		extra += len(name) + len(`"",`) // one enum entry: the quoted name and a comma
		if err := areg.Register(&fakeAction{name: name, layer: "kafka-connect", params: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"target":  map[string]any{"type": "string", "description": "a long description of the target that would cost real bytes if it were inlined into the strict schema"},
				"mode":    map[string]any{"type": "string", "enum": []string{"alpha", "beta", "gamma", "delta"}},
				"retries": map[string]any{"type": "integer"},
				"nested":  map[string]any{"type": "object", "properties": map[string]any{"k": map[string]any{"type": "string"}}, "required": []string{"k"}, "additionalProperties": false},
			},
			"additionalProperties": false,
			"required":             []string{"target", "mode"},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	grown, grownN := strictSchemaBytes(t, buildToolList(reg, newRoomTools(Deps{Actions: areg}, newProposalTracker())))
	if grownN != baseN {
		t.Fatalf("strict tool count changed from %d to %d", baseN, grownN)
	}
	t.Logf("strict bytes: %d with 6 actions, %d with 16 (+%d for 10 enum entries)", base, grown, grown-base)
	if grown-base > extra {
		t.Errorf("10 more actions grew the strict schemas by %d bytes, allowed %d (their enum entries only)", grown-base, extra)
	}
	if grown > maxStrictSchemaBytes {
		t.Errorf("strict schemas with 16 actions total %d bytes, budget is %d", grown, maxStrictSchemaBytes)
	}
}
