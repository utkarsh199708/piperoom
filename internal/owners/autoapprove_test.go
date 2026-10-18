package owners

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// pipelineYAML returns one pipeline block (indented for `pipelines:`) with the
// given extra lines appended, each already indented by four spaces.
func pipelineYAML(id, extra string) string {
	return "  - id: " + id + "\n    topic_prefix: " + id + "\n    source_tables: [public.t]\n    connectors: [c1]\n" + extra
}

const sixLayers = `layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
`

func TestAutoApproveSyntax(t *testing.T) {
	cases := []struct {
		name    string
		extra   string // lines under the pipeline
		want    []string
		wantErr string // substring; "" = must load
	}{
		{name: "absent", extra: "", want: []string{}},
		{name: "empty list", extra: "    auto_approve: []\n", want: []string{}},
		// null means "no list", which is no auto-approval: the safe reading,
		// unlike a null tenant or connections (checkNullBlocks).
		{name: "null", extra: "    auto_approve:\n", want: []string{}},
		{name: "one", extra: "    auto_approve: [connect_restart_task]\n", want: []string{"connect_restart_task"}},
		{
			name:  "two, order kept",
			extra: "    auto_approve: [connect_resume_connector, connect_restart_task]\n",
			want:  []string{"connect_resume_connector", "connect_restart_task"},
		},
		{name: "block list", extra: "    auto_approve:\n      - a1\n      - b_2\n", want: []string{"a1", "b_2"}},
		{name: "digits and underscores after the first letter", extra: "    auto_approve: [a_1_b2]\n", want: []string{"a_1_b2"}},

		{name: "duplicate", extra: "    auto_approve: [connect_restart_task, connect_restart_task]\n", wantErr: `auto_approve lists "connect_restart_task" twice`},
		{name: "duplicate not adjacent", extra: "    auto_approve: [a, b, a]\n", wantErr: `lists "a" twice`},
		{name: "uppercase", extra: "    auto_approve: [Connect_restart]\n", wantErr: `"Connect_restart" is not an action name`},
		{name: "hyphen", extra: "    auto_approve: [connect-restart]\n", wantErr: `"connect-restart" is not an action name`},
		{name: "leading digit", extra: "    auto_approve: [\"1restart\"]\n", wantErr: `"1restart" is not an action name`},
		{name: "leading underscore", extra: "    auto_approve: [_restart]\n", wantErr: `"_restart" is not an action name`},
		{name: "space", extra: "    auto_approve: [\"restart task\"]\n", wantErr: `"restart task" is not an action name`},
		{name: "trailing space", extra: "    auto_approve: [\"restart \"]\n", wantErr: `is not an action name`},
		{name: "newline", extra: "    auto_approve: [\"restart\\n\"]\n", wantErr: `is not an action name`},
		{name: "path-like", extra: "    auto_approve: [\"pipeline/*\"]\n", wantErr: `"pipeline/*" is not an action name`},
		{name: "wildcard", extra: "    auto_approve: [\"*\"]\n", wantErr: `"*" is not an action name`},
		{name: "non-ascii letter", extra: "    auto_approve: [\"réstart\"]\n", wantErr: `is not an action name`},
		{name: "empty string", extra: "    auto_approve: [\"\"]\n", wantErr: `"" is not an action name`},
		{name: "null item", extra: "    auto_approve: [null]\n", wantErr: "empty (null) entry"},
		{name: "null item among others", extra: "    auto_approve:\n      - connect_restart_task\n      -\n", wantErr: "empty (null) entry"},
		{name: "tilde item", extra: "    auto_approve: [~, connect_restart_task]\n", wantErr: "empty (null) entry"},
		{name: "scalar, not a list", extra: "    auto_approve: connect_restart_task\n", wantErr: "cannot unmarshal"},
		{name: "map, not a list", extra: "    auto_approve: {connect_restart_task: true}\n", wantErr: "cannot unmarshal"},
		{name: "typo of the key is still refused", extra: "    auto_approves: [connect_restart_task]\n", wantErr: "auto_approves"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o, err := Load(writeTemp(t, "pipelines:\n"+pipelineYAML("orders", tc.extra)+sixLayers))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("Load succeeded, want error containing %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			p, _ := o.Pipeline("orders")
			if got := p.AutoApproveList(); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("AutoApproveList() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// A syntax error names the pipeline it is in, not the first one.
func TestAutoApproveSyntaxErrorNamesThePipeline(t *testing.T) {
	doc := "pipelines:\n" + pipelineYAML("orders", "    auto_approve: [fine]\n") + pipelineYAML("payments", "    auto_approve: [BAD]\n") + sixLayers
	_, err := Load(writeTemp(t, doc))
	if err == nil || !strings.Contains(err.Error(), `pipeline "payments"`) || !strings.Contains(err.Error(), `"BAD"`) {
		t.Fatalf("err = %v, want one naming pipeline payments and entry BAD", err)
	}
}

// The existing null-block check must keep working when the new key sits next
// to it, and the new key must not make a null tenant or connections pass.
func TestAutoApproveDoesNotWeakenNullBlockChecks(t *testing.T) {
	for name, extra := range map[string]string{
		"null tenant":      "    auto_approve: [a]\n    tenant:\n",
		"null connections": "    auto_approve: [a]\n    connections:\n",
	} {
		_, err := Load(writeTemp(t, "pipelines:\n"+pipelineYAML("orders", extra)+sixLayers))
		if err == nil || !strings.Contains(err.Error(), "(null)") {
			t.Errorf("%s: err = %v, want a null-block error", name, err)
		}
	}
}

func TestLoadWithActions(t *testing.T) {
	registry := []string{"connect_restart_task", "connect_resume_connector", "pg_terminate_backend"}
	two := "pipelines:\n" +
		pipelineYAML("orders", "    auto_approve: [connect_restart_task, connect_resume_connector]\n") +
		pipelineYAML("payments", "    auto_approve: [connect_restart_taks]\n") + sixLayers
	cases := []struct {
		name    string
		doc     string
		actions []string
		wantErr []string // all must appear in the error; nil = must load
		loadOK  bool     // plain Load accepts the file (the entry is well formed, only unknown to the registry)
	}{
		{
			name:    "all entries known",
			doc:     "pipelines:\n" + pipelineYAML("orders", "    auto_approve: [connect_restart_task, connect_resume_connector]\n") + sixLayers,
			actions: registry,
		},
		{name: "empty list needs no registry", doc: "pipelines:\n" + pipelineYAML("orders", "    auto_approve: []\n") + sixLayers, actions: nil},
		{name: "absent needs no registry", doc: "pipelines:\n" + pipelineYAML("orders", "") + sixLayers, actions: nil},
		{
			name:    "unknown action names the pipeline and the entry",
			doc:     "pipelines:\n" + pipelineYAML("orders", "    auto_approve: [connect_restart_task, no_such_action]\n") + sixLayers,
			actions: registry,
			wantErr: []string{`pipeline "orders"`, `"no_such_action"`, "unknown action", "connect_restart_task, connect_resume_connector, pg_terminate_backend"},
			loadOK:  true,
		},
		{
			name:    "the unknown entry of the second pipeline is named",
			doc:     two,
			actions: registry,
			wantErr: []string{`pipeline "payments"`, `"connect_restart_taks"`},
			loadOK:  true,
		},
		{
			name:    "nil registry refuses every entry",
			doc:     "pipelines:\n" + pipelineYAML("orders", "    auto_approve: [connect_restart_task]\n") + sixLayers,
			actions: nil,
			wantErr: []string{`pipeline "orders"`, `"connect_restart_task"`, "known: none"},
			loadOK:  true,
		},
		{
			name:    "empty registry refuses every entry",
			doc:     "pipelines:\n" + pipelineYAML("orders", "    auto_approve: [connect_restart_task]\n") + sixLayers,
			actions: []string{},
			wantErr: []string{`"connect_restart_task"`},
			loadOK:  true,
		},
		{
			name:    "names are exact, not prefixes",
			doc:     "pipelines:\n" + pipelineYAML("orders", "    auto_approve: [connect_restart]\n") + sixLayers,
			actions: registry,
			wantErr: []string{`"connect_restart"`},
			loadOK:  true,
		},
		{
			name:    "syntax errors of Load still apply: duplicate",
			doc:     "pipelines:\n" + pipelineYAML("orders", "    auto_approve: [connect_restart_task, connect_restart_task]\n") + sixLayers,
			actions: registry,
			wantErr: []string{"twice"},
		},
		{
			name:    "syntax errors of Load still apply: bad name",
			doc:     "pipelines:\n" + pipelineYAML("orders", "    auto_approve: [Connect]\n") + sixLayers,
			actions: registry,
			wantErr: []string{"is not an action name"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTemp(t, tc.doc)
			o, err := LoadWithActions(path, tc.actions)
			if tc.wantErr == nil {
				if err != nil || o == nil {
					t.Fatalf("LoadWithActions = %v, %v", o, err)
				}
				return
			}
			if err == nil {
				t.Fatal("LoadWithActions succeeded, want an error")
			}
			if o != nil {
				t.Errorf("returned owners %v along with error", o)
			}
			for _, w := range tc.wantErr {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not contain %q", err, w)
				}
			}
			// Load must accept what only the registry check refuses: the
			// file has to load before the registry exists.
			if _, err := Load(path); (err == nil) != tc.loadOK {
				t.Errorf("plain Load err = %v, want accepted = %v", err, tc.loadOK)
			}
		})
	}
}

func TestLoadWithActionsMissingFile(t *testing.T) {
	if _, err := LoadWithActions(filepath.Join(t.TempDir(), "nope.yaml"), []string{"a"}); err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

func TestValidateAutoApprove(t *testing.T) {
	o, err := Load(writeTemp(t, "pipelines:\n"+pipelineYAML("orders", "    auto_approve: [connect_restart_task]\n")+sixLayers))
	if err != nil {
		t.Fatalf("Load must accept any well-formed name before the registry exists: %v", err)
	}
	if err := o.ValidateAutoApprove([]string{"connect_restart_task", "other"}); err != nil {
		t.Fatalf("known entry refused: %v", err)
	}
	err = o.ValidateAutoApprove([]string{"other"})
	if err == nil || !strings.Contains(err.Error(), `pipeline "orders"`) || !strings.Contains(err.Error(), `"connect_restart_task"`) {
		t.Fatalf("err = %v, want one naming pipeline orders and the entry", err)
	}
	if err := o.ValidateAutoApprove(nil); err == nil {
		t.Fatal("nil registry must refuse an entry")
	}
	var none *Owners
	if err := none.ValidateAutoApprove(nil); err != nil {
		t.Fatalf("nil Owners has nothing to refuse: %v", err)
	}
}

func TestAutoApproveListIsACopy(t *testing.T) {
	o, err := Load(writeTemp(t, "pipelines:\n"+pipelineYAML("orders", "    auto_approve: [a, b]\n")+sixLayers))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := o.Pipeline("orders")
	got := p.AutoApproveList()
	got[0] = "changed"
	again, _ := o.Pipeline("orders")
	if want := []string{"a", "b"}; !reflect.DeepEqual(again.AutoApproveList(), want) {
		t.Fatalf("loaded list changed through the returned slice: %v, want %v", again.AutoApproveList(), want)
	}
	if !reflect.DeepEqual(p.AutoApprove, []string{"a", "b"}) {
		t.Fatalf("field changed through the returned slice: %v", p.AutoApprove)
	}

	empty := Pipeline{}
	if l := empty.AutoApproveList(); l == nil || len(l) != 0 {
		t.Fatalf("empty list = %#v, want a non-nil empty slice", l)
	}
	var none *Pipeline
	if l := none.AutoApproveList(); l == nil || len(l) != 0 {
		t.Fatalf("nil pipeline list = %#v, want a non-nil empty slice", l)
	}
}

// ownershipDoc has a top-level default, a per-pipeline override, a layer
// nobody owns in the top-level block, and a second pipeline with no override.
const ownershipDoc = `pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
    owners:
      kafka-connect: { approvers: [group:dataeng-oncall], backup: [user:priya] }
      warehouse: { approvers: [user:meera] }
  - id: payments
    topic_prefix: pay
    source_tables: [public.payments]
    connectors: [c2]
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: [arjun, "group:kafka-team"]
  consumer: kabir
`

func TestIsOwned(t *testing.T) {
	o, err := Load(writeTemp(t, ownershipDoc))
	if err != nil {
		t.Fatal(err)
	}
	// Built by hand: Load refuses a pipeline entry without approvers, but the
	// type allows it and IsOwned must agree with CanApprove for it.
	hand := &Owners{
		Pipelines: []Pipeline{{
			ID: "hand",
			Owners: map[string]LayerRights{
				"consumer":        {Backup: Principals{{KindUser, "bea"}}},            // backup only
				"warehouse":       {},                                                 // present, no principals
				"kafka":           {Approvers: Principals{{KindGroup, "kafka-team"}}}, // group
				"postgres":        {Approvers: Principals{{KindUser, "pat"}}},         // replaces layers:
				"schema-registry": {Approvers: nil, Backup: Principals{{KindGroup, "sre"}}},
			},
		}},
		Layers: map[string]Principals{
			"warehouse": {{KindUser, "meera"}},
			"postgres":  {{KindUser, "priya"}},
			"consumer":  {{KindUser, "kabir"}},
		},
	}
	cases := []struct {
		name     string
		o        *Owners
		pipeline string
		layer    string
		want     bool
	}{
		{"top-level user", o, "payments", "postgres", true},
		{"top-level user, pipeline without an owners block", o, "orders", "postgres", true},
		{"top-level list with a group", o, "payments", "kafka", true},
		{"pipeline override replaces a top-level layer", o, "orders", "kafka-connect", true},
		{"pipeline override of a layer the top-level block lacks", o, "orders", "warehouse", true},
		{"unowned layer: no layers: entry and no override in this pipeline", o, "payments", "warehouse", false},
		{"pipeline with an owners block falls back to layers: for a layer it does not name", o, "orders", "schema-registry", true},
		{"unknown pipeline fails closed", o, "nope", "postgres", false},
		{"unknown pipeline, layer the override names", o, "nope", "warehouse", false},
		{"empty pipeline id reads layers: alone", o, "", "postgres", true},
		{"empty pipeline id, layer only a pipeline names", o, "", "warehouse", false},
		{"unknown layer", o, "orders", "bogus", false},
		{"empty layer", o, "orders", "", false},
		{"nil Owners", nil, "orders", "postgres", false},

		{"backup-only layer is owned", hand, "hand", "consumer", true},
		{"backup-only layer, group backup", hand, "hand", "schema-registry", true},
		{"pipeline entry with no principals is not owned, no fallback to layers:", hand, "hand", "warehouse", false},
		{"group approver", hand, "hand", "kafka", true},
		{"pipeline override of a top-level layer", hand, "hand", "postgres", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.o.IsOwned(tc.pipeline, tc.layer); got != tc.want {
				t.Errorf("IsOwned(%q, %q) = %v, want %v", tc.pipeline, tc.layer, got, tc.want)
			}
		})
	}

	// "Not owned" and "somebody can approve" must never disagree, or a layer
	// could be both fallback-approvable by the driver and owned by a backup.
	if _, ok := hand.CanApprove("hand", "consumer", "bea", nil); !ok {
		t.Error("CanApprove must let the backup of an IsOwned layer approve")
	}
	if _, ok := hand.CanApprove("hand", "warehouse", "meera", nil); ok {
		t.Error("an empty pipeline entry replaces layers:, so meera must not approve; IsOwned said false")
	}
}

func TestOwnsAll(t *testing.T) {
	o, err := Load(writeTemp(t, ownershipDoc))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		o        *Owners
		pipeline string
		layers   []string
		user     string
		groups   []string
		want     bool
	}{
		// user principals
		{"user owns one layer", o, "payments", []string{"postgres"}, "priya", nil, true},
		{"user owns two layers", o, "payments", []string{"postgres", "schema-registry"}, "priya", nil, true},
		{"layer order does not matter", o, "payments", []string{"schema-registry", "postgres"}, "priya", nil, true},
		{"a layer listed twice", o, "payments", []string{"postgres", "postgres"}, "priya", nil, true},
		{"other user owns the layer", o, "payments", []string{"postgres"}, "kabir", nil, false},

		// group principals
		{"group approver", o, "orders", []string{"kafka-connect"}, "zed", []string{"dataeng-oncall"}, true},
		{"group approver, wrong group", o, "orders", []string{"kafka-connect"}, "zed", []string{"other"}, false},
		{"group approver, no groups claim", o, "orders", []string{"kafka-connect"}, "zed", nil, false},
		{"group in a top-level list", o, "payments", []string{"kafka"}, "zed", []string{"kafka-team"}, true},

		// mixed: some layers by user, some by group, one by backup
		{"user and group", o, "payments", []string{"postgres", "kafka"}, "priya", []string{"kafka-team"}, true},
		{"user, group and backup", o, "orders", []string{"postgres", "kafka-connect", "kafka"}, "priya", []string{"kafka-team"}, true},
		{"backup counts like CanApprove", o, "orders", []string{"kafka-connect"}, "priya", nil, true},
		{"mixed with one layer missing", o, "payments", []string{"postgres", "kafka", "consumer"}, "priya", []string{"kafka-team"}, false},

		// missing layer
		{"missing the second layer", o, "payments", []string{"postgres", "consumer"}, "priya", nil, false},
		{"missing the first layer", o, "payments", []string{"consumer", "postgres"}, "priya", nil, false},
		{"pipeline override replaces the top-level owner", o, "orders", []string{"kafka-connect"}, "arjun", nil, false},
		{"top-level owner still owns it in the pipeline without an override", o, "payments", []string{"kafka-connect"}, "arjun", nil, true},
		{"layer nobody owns", o, "payments", []string{"postgres", "warehouse"}, "priya", nil, false},
		{"unknown layer name", o, "payments", []string{"postgres", "bogus"}, "priya", nil, false},
		{"unknown pipeline", o, "nope", []string{"postgres"}, "priya", nil, false},

		// empty layers
		{"nil layers", o, "payments", nil, "priya", nil, false},
		{"empty layers", o, "payments", []string{}, "priya", nil, false},

		// no identity
		{"empty user id", o, "payments", []string{"postgres"}, "", nil, false},
		{"nil Owners", nil, "payments", []string{"postgres"}, "priya", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.o.OwnsAll(tc.pipeline, tc.layers, tc.user, tc.groups)
			if got != tc.want {
				t.Errorf("OwnsAll(%q, %v, %q, %v) = %v, want %v", tc.pipeline, tc.layers, tc.user, tc.groups, got, tc.want)
			}
		})
	}
}

// The P2 done-when fixture: one person on every layer owns all of them; a
// person on five of six does not, and neither does anyone when one is unowned.
func TestOwnsAllSolo(t *testing.T) {
	const solo = `pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
layers:
  postgres: sam
  kafka-connect: sam
  schema-registry: sam
  kafka: sam
  consumer: sam
  warehouse: sam
`
	o, err := Load(writeTemp(t, solo))
	if err != nil {
		t.Fatal(err)
	}
	all := []string{"postgres", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse"}
	if !o.OwnsAll("orders", all, "sam", nil) {
		t.Error("sam owns every layer")
	}
	if o.OwnsAll("orders", all, "kim", nil) {
		t.Error("kim owns nothing")
	}
	for _, l := range all {
		if !o.IsOwned("orders", l) {
			t.Errorf("layer %s should be owned", l)
		}
	}

	without := strings.Replace(solo, "  warehouse: sam\n", "", 1)
	o, err = Load(writeTemp(t, without))
	if err != nil {
		t.Fatal(err)
	}
	if o.IsOwned("orders", "warehouse") {
		t.Error("warehouse has no entry, so it is unowned")
	}
	if o.OwnsAll("orders", all, "sam", nil) {
		t.Error("an unowned layer must not count as owned by sam")
	}
	if !o.OwnsAll("orders", all[:5], "sam", nil) {
		t.Error("sam still owns the five layers that have an owner")
	}
}

// dev/owners.yaml is A1's file. Whatever auto_approve entries it carries (none
// today) must load with the registry check, passing it its own names.
func TestDevOwnersLoadsWithActions(t *testing.T) {
	const path = "../../dev/owners.yaml"
	o, err := Load(path)
	if err != nil {
		t.Fatalf("Load(%s): %v", path, err)
	}
	var names []string
	for _, p := range o.Pipelines {
		names = append(names, p.AutoApproveList()...)
	}
	if _, err := LoadWithActions(path, names); err != nil {
		t.Fatalf("LoadWithActions(%s): %v", path, err)
	}
	if _, ok := o.Pipeline("orders"); !ok {
		t.Fatal("dev/owners.yaml lost the orders pipeline")
	}
	for _, l := range []string{"postgres", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse"} {
		if !o.IsOwned("orders", l) {
			t.Errorf("dev/owners.yaml: layer %s of orders is unowned", l)
		}
	}
}
