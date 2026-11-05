package owners

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

const pipelineHead = "pipelines:\n  - id: orders\n    topic_prefix: pg\n    source_tables: [public.orders]\n    connectors: [c1]\n"

// principalsYAML exercises every new form: string and list layers, a
// per-pipeline owners block, and auditors.
const principalsYAML = `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
    owners:
      kafka-connect: { approvers: [group:dataeng-oncall], backup: [user:priya] }
  - id: billing
    topic_prefix: bl
    source_tables: [public.invoices]
    connectors: [c2]
layers:
  postgres: priya
  kafka: [user:arjun, group:dataeng-oncall]
  kafka-connect: arjun
  warehouse: [meera]
auditors: [group:security, user:kabir]
`

func loadStr(t *testing.T, s string) *Owners {
	t.Helper()
	o, err := Load(writeTemp(t, s))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return o
}

func TestParsePrincipal(t *testing.T) {
	good := map[string]Principal{
		"priya":                  {KindUser, "priya"},
		"user:priya":             {KindUser, "priya"},
		" user: priya ":          {KindUser, "priya"},
		"group:dataeng-oncall":   {KindGroup, "dataeng-oncall"},
		"group:Data Eng On-Call": {KindGroup, "Data Eng On-Call"},
		"group:a:b":              {KindGroup, "a:b"},
	}
	for in, want := range good {
		got, err := ParsePrincipal(in)
		if err != nil || got != want {
			t.Errorf("ParsePrincipal(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	bad := map[string]string{
		"":            "empty principal",
		"grp:ops":     "bad prefix",
		"team:ops":    "bad prefix",
		"group:":      "empty group name",
		"user:":       "empty user name",
		"group:  ":    "empty group name",
		"user:a b":    "whitespace",
		"group:a,b":   "comma",
		"priya,arjun": "comma",
		"a b":         "whitespace",
		":x":          "bad prefix",
		"User:priya":  "bad prefix",
	}
	for in, sub := range bad {
		if _, err := ParsePrincipal(in); err == nil || !strings.Contains(err.Error(), sub) {
			t.Errorf("ParsePrincipal(%q) error = %v; want it to contain %q", in, err, sub)
		}
	}
}

func TestPrincipalsOldAndNewFormats(t *testing.T) {
	o := loadStr(t, principalsYAML)
	want := map[string]Principals{
		"postgres":      {{KindUser, "priya"}},
		"kafka":         {{KindUser, "arjun"}, {KindGroup, "dataeng-oncall"}},
		"kafka-connect": {{KindUser, "arjun"}},
		"warehouse":     {{KindUser, "meera"}},
	}
	if !reflect.DeepEqual(map[string]Principals(o.Layers), want) {
		t.Fatalf("layers = %v, want %v", o.Layers, want)
	}
	if !reflect.DeepEqual(o.Auditors, Principals{{KindGroup, "security"}, {KindUser, "kabir"}}) {
		t.Fatalf("auditors = %v", o.Auditors)
	}
	p, _ := o.Pipeline("orders")
	r := p.Owners["kafka-connect"]
	if !reflect.DeepEqual(r.Approvers, Principals{{KindGroup, "dataeng-oncall"}}) || !reflect.DeepEqual(r.Backup, Principals{{KindUser, "priya"}}) {
		t.Fatalf("orders owners = %+v", r)
	}
}

func TestExistingFilesStayValid(t *testing.T) {
	// dev/owners.staging.yaml (X8d) declares staging: and must load with
	// the X8a loader.
	for _, path := range []string{"../../dev/owners.yaml", "../../dev/owners.staging.yaml", "../../deploy/config/owners.example.yaml"} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		o, err := Load(path)
		if err != nil {
			t.Fatalf("Load(%s): %v", path, err)
		}
		if len(o.Layers) != 6 {
			t.Errorf("%s: %d layers, want 6", path, len(o.Layers))
		}
		for layer, ps := range o.Layers {
			if len(ps) != 1 || ps[0].Kind != KindUser {
				t.Errorf("%s: layer %s = %v, want one user", path, layer, ps)
			}
		}
		if len(o.Auditors) != 0 {
			t.Errorf("%s: unexpected auditors %v", path, o.Auditors)
		}
	}
	o, err := Load("../../dev/owners.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := o.LayerOwner("kafka"); !ok || got != "arjun" {
		t.Errorf("LayerOwner(kafka) = %q, %v", got, ok)
	}
}

func TestPrincipalValidationErrors(t *testing.T) {
	cases := []struct{ name, yaml, wantErr string }{
		{"unknown layer in layers", pipelineHead + "layers:\n  bogus: [user:x]\n", `unknown layer "bogus"`},
		{"unknown layer in owners", pipelineHead + "    owners:\n      bogus: {approvers: [x]}\nlayers: {}\n", `owners: unknown layer "bogus"`},
		{"bad prefix in layers", pipelineHead + "layers:\n  kafka: [grp:ops]\n", `bad prefix "grp"`},
		{"bad prefix scalar", pipelineHead + "layers:\n  kafka: team:ops\n", `bad prefix "team"`},
		{"empty group in layers", pipelineHead + "layers:\n  kafka: [user:a, 'group:']\n", "empty group name"},
		{"empty group in approvers", pipelineHead + "    owners:\n      kafka: {approvers: ['group: ']}\nlayers: {}\n", "empty group name"},
		{"empty group in backup", pipelineHead + "    owners:\n      kafka: {approvers: [a], backup: ['group:']}\nlayers: {}\n", "empty group name"},
		{"bad prefix in auditors", pipelineHead + "layers: {}\nauditors: [org:sec]\n", `bad prefix "org"`},
		{"empty group in auditors", pipelineHead + "layers: {}\nauditors: ['group:']\n", "empty group name"},
		{"empty layer list", pipelineHead + "layers:\n  kafka: []\n", `"kafka" has no owner`},
		{"layer with null owner", pipelineHead + "layers:\n  kafka:\n", `"kafka" has no owner`},
		{"owners entry without approvers", pipelineHead + "    owners:\n      kafka: {backup: [x]}\nlayers: {}\n", `layer "kafka" has no approvers`},
		{"null item in flow list", pipelineHead + "layers:\n  kafka: [~]\n", "null item"},
		{"null item among others", pipelineHead + "layers:\n  kafka: [user:a, null]\n", "null item"},
		{"null item in block list", pipelineHead + "layers:\n  kafka:\n    - user:a\n    -\n", "null item"},
		{"null item in auditors", pipelineHead + "layers: {}\nauditors: [~]\n", "null item"},
		{"null item in approvers", pipelineHead + "    owners:\n      kafka: {approvers: [~]}\nlayers: {}\n", "null item"},
		{"typo backups in owners", pipelineHead + "    owners:\n      kafka: {approvers: [a], backups: [b]}\nlayers: {}\n", `unknown field "backups"`},
		{"typo approver in owners", pipelineHead + "    owners:\n      kafka: {approver: [a]}\nlayers: {}\n", `unknown field "approver"`},
		{"principal is a map", pipelineHead + "layers:\n  kafka: [{a: b}]\n", "must be a string"},
		{"layer is a map", pipelineHead + "layers:\n  kafka: {a: b}\n", "want a principal or a list"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestCanApprove(t *testing.T) {
	o := loadStr(t, principalsYAML)
	cases := []struct {
		name     string
		pipeline string
		layer    string
		user     string
		groups   []string
		want     string
	}{
		{"old-form user", "orders", "postgres", "priya", nil, "user:priya"},
		{"old-form user, other pipeline", "billing", "postgres", "priya", nil, "user:priya"},
		{"not the owner", "orders", "postgres", "arjun", nil, ""},
		{"list: user", "orders", "kafka", "arjun", nil, "user:arjun"},
		{"list: group", "orders", "kafka", "someone", []string{"x", "dataeng-oncall"}, "group:dataeng-oncall"},
		{"list: user wins over group", "orders", "kafka", "arjun", []string{"dataeng-oncall"}, "user:arjun"},
		{"group but no groups claim", "orders", "kafka", "someone", nil, ""},
		{"wrong group", "orders", "kafka", "someone", []string{"security"}, ""},
		{"group name is exact", "orders", "kafka", "someone", []string{"DataEng-Oncall"}, ""},
		{"owners block overrides layers: group", "orders", "kafka-connect", "x", []string{"dataeng-oncall"}, "group:dataeng-oncall"},
		{"owners block overrides layers: old owner loses", "orders", "kafka-connect", "arjun", nil, ""},
		{"backup user", "orders", "kafka-connect", "priya", nil, "backup:user:priya"},
		{"no override in other pipeline", "billing", "kafka-connect", "arjun", nil, "user:arjun"},
		{"no override in other pipeline: backup has no right", "billing", "kafka-connect", "priya", nil, ""},
		{"unknown pipeline fails closed", "nope", "warehouse", "meera", nil, ""},
		{"renamed pipeline fails closed (old default owner too)", "orders-v2", "postgres", "priya", nil, ""},
		{"unknown pipeline: group and backup get nothing", "nope", "kafka", "x", []string{"dataeng-oncall"}, ""},
		{"empty pipeline id falls back to layers", "", "warehouse", "meera", nil, "user:meera"},
		{"auditor cannot approve", "orders", "warehouse", "kabir", []string{"security"}, ""},
		{"unknown layer", "orders", "bogus", "arjun", nil, ""},
		{"layer with no entry", "orders", "consumer", "arjun", nil, ""},
		{"empty user id", "orders", "postgres", "", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			grant, ok := o.CanApprove(tc.pipeline, tc.layer, tc.user, tc.groups)
			if ok != (tc.want != "") || grant != tc.want {
				t.Fatalf("CanApprove = %q, %v; want %q", grant, ok, tc.want)
			}
		})
	}
}

func TestCanApproveBackupGroupAndPrecedence(t *testing.T) {
	o := loadStr(t, pipelineHead+`
    owners:
      kafka: { approvers: [user:arjun], backup: [group:sre, user:arjun] }
layers: {}
`)
	if g, ok := o.CanApprove("orders", "kafka", "u1", []string{"sre"}); !ok || g != "backup:group:sre" {
		t.Errorf("backup group: %q, %v", g, ok)
	}
	// An approver match wins over a backup match.
	if g, ok := o.CanApprove("orders", "kafka", "arjun", []string{"sre"}); !ok || g != "user:arjun" {
		t.Errorf("approver beats backup: %q, %v", g, ok)
	}
}

func TestIsAuditor(t *testing.T) {
	o := loadStr(t, principalsYAML)
	if !o.IsAuditor("kabir", nil) {
		t.Error("kabir (user:) should be an auditor")
	}
	if !o.IsAuditor("anyone", []string{"security"}) {
		t.Error("group:security member should be an auditor")
	}
	if o.IsAuditor("anyone", nil) || o.IsAuditor("arjun", []string{"dataeng-oncall"}) || o.IsAuditor("", nil) {
		t.Error("non-auditor reported as auditor")
	}
}

func TestIsMemberPrincipalAndNamed(t *testing.T) {
	o := loadStr(t, principalsYAML)
	cases := []struct {
		name          string
		user          string
		groups        []string
		member, named bool
	}{
		{"layers user", "priya", nil, true, true},
		{"layers user in list", "arjun", nil, true, true},
		{"layers group", "x", []string{"dataeng-oncall"}, true, true},
		{"owners backup only", "priya", nil, true, true}, // also in layers; see backup-only case below
		{"auditor user only", "kabir", nil, false, true},
		{"auditor group only", "x", []string{"security"}, false, true},
		{"auditor group plus layer user", "meera", []string{"security"}, true, true},
		{"unknown", "stranger", []string{"other"}, false, false},
		{"empty id and no groups", "", nil, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := o.IsMemberPrincipal(tc.user, tc.groups); got != tc.member {
				t.Errorf("IsMemberPrincipal = %v, want %v", got, tc.member)
			}
			if got := o.Named(tc.user, tc.groups); got != tc.named {
				t.Errorf("Named = %v, want %v", got, tc.named)
			}
		})
	}

	// Backup-only, approver group-only inside an owners block, and nothing in layers.
	o2 := loadStr(t, pipelineHead+`
    owners:
      kafka: { approvers: [group:sre], backup: [user:bea] }
layers: {}
auditors: [user:aud]
`)
	for _, c := range []struct {
		user   string
		groups []string
		member bool
	}{
		{"bea", nil, true},
		{"x", []string{"sre"}, true},
		{"aud", nil, false},
	} {
		if got := o2.IsMemberPrincipal(c.user, c.groups); got != c.member {
			t.Errorf("owners-only IsMemberPrincipal(%s,%v) = %v, want %v", c.user, c.groups, got, c.member)
		}
		if !o2.Named(c.user, c.groups) {
			t.Errorf("owners-only Named(%s,%v) = false", c.user, c.groups)
		}
	}
	var nilOwners *Owners
	if nilOwners.Named("a", nil) || nilOwners.IsAuditor("a", nil) || nilOwners.IsMemberPrincipal("a", nil) {
		t.Error("nil Owners must name nobody")
	}
	if _, ok := nilOwners.CanApprove("p", "kafka", "a", nil); ok {
		t.Error("nil Owners must grant nothing")
	}
}

func TestLayersFor(t *testing.T) {
	o := loadStr(t, principalsYAML)
	cases := []struct {
		name   string
		user   string
		groups []string
		want   []string
	}{
		// Order follows events.Layers: postgres, kafka-connect, schema-registry, kafka, consumer, warehouse.
		{"old-form user", "priya", nil, []string{"postgres", "kafka-connect"}}, // kafka-connect: backup in orders
		{"user in list + layers", "arjun", nil, []string{"kafka-connect", "kafka"}},
		{"group", "x", []string{"dataeng-oncall"}, []string{"kafka-connect", "kafka"}},
		{"auditor only", "kabir", []string{"security"}, []string{}},
		{"stranger", "zed", nil, []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := o.LayersFor(tc.user, tc.groups)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("LayersFor = %v, want %v", got, tc.want)
			}
		})
	}
	var nilOwners *Owners
	if got := nilOwners.LayersFor("a", nil); got == nil || len(got) != 0 {
		t.Errorf("nil Owners LayersFor = %#v, want empty non-nil", got)
	}
}

func TestLayerOwnerDisplay(t *testing.T) {
	o := loadStr(t, principalsYAML)
	cases := map[string]string{
		"postgres": "priya",
		"kafka":    "arjun, group:dataeng-oncall",
	}
	for layer, want := range cases {
		if got, ok := o.LayerOwner(layer); !ok || got != want {
			t.Errorf("LayerOwner(%s) = %q, %v; want %q", layer, got, ok, want)
		}
	}
	if _, ok := o.LayerOwner("consumer"); ok {
		t.Error("layer without an owner should not resolve")
	}
	if got := (Principals{{KindUser, "a"}, {KindGroup, "g"}, {KindUser, "b"}}).Display(); got != "a, group:g, b" {
		t.Errorf("Display = %q", got)
	}
}

func TestNamedPrincipals(t *testing.T) {
	o := loadStr(t, principalsYAML)
	var got []string
	for _, p := range o.NamedPrincipals() {
		got = append(got, p.String())
	}
	want := []string{"group:dataeng-oncall", "group:security", "user:arjun", "user:kabir", "user:meera", "user:priya"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NamedPrincipals = %v, want %v", got, want)
	}
}
