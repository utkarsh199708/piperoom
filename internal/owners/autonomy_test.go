package owners

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// autonomyDoc returns one orders pipeline with extra lines (already indented
// by four spaces) and the six-layer block.
func autonomyDoc(extra string) string {
	return "pipelines:\n" + pipelineYAML("orders", extra) + sixLayers
}

func TestAutonomyBlockValid(t *testing.T) {
	registry := []string{
		"connect_resume_connector", "connect_restart_task", "kafka_replay_range",
		"pg_resnapshot_rows",
	}
	type ruleWant struct {
		after  int
		window time.Duration
		text   string
		tier   string
	}
	cases := []struct {
		name     string
		extra    string
		registry []string
		want     []string
		rules    map[string]ruleWant
	}{
		{
			name: "all tiers, both streak and window bounds",
			extra: "    autonomy:\n" +
				"      connect_resume_connector: {after_verified: 10, window: 30d, max_tier: T1}\n" +
				"      connect_restart_task: {after_verified: 3, window: 1d, max_tier: T0}\n" +
				"      kafka_replay_range: {after_verified: 100, window: 90d, max_tier: T2}\n",
			want: []string{"connect_restart_task", "connect_resume_connector", "kafka_replay_range"},
			rules: map[string]ruleWant{
				"connect_restart_task":     {3, 24 * time.Hour, "1d", "T0"},
				"connect_resume_connector": {10, 30 * 24 * time.Hour, "30d", "T1"},
				"kafka_replay_range":       {100, 90 * 24 * time.Hour, "90d", "T2"},
			},
		},
		{
			// The contract (docs/api.md, Earned autonomy) spells the window in
			// whole days as <n>d; the text is kept exactly as written.
			name: "whole-day windows inside the bounds",
			extra: "    autonomy:\n" +
				"      connect_resume_connector: {after_verified: 10, window: 2d}\n" +
				"      connect_restart_task: {after_verified: 4, window: 45d, max_tier: T0}\n" +
				"      kafka_replay_range: {after_verified: 5, window: 89d, max_tier: T2}\n" +
				"      pg_resnapshot_rows: {after_verified: 6, window: 7d, max_tier: T1}\n",
			want: []string{"connect_restart_task", "connect_resume_connector", "kafka_replay_range", "pg_resnapshot_rows"},
			rules: map[string]ruleWant{
				"connect_restart_task":     {4, 45 * 24 * time.Hour, "45d", "T0"},
				"connect_resume_connector": {10, 48 * time.Hour, "2d", "T1"},
				"kafka_replay_range":       {5, 89 * 24 * time.Hour, "89d", "T2"},
				"pg_resnapshot_rows":       {6, 7 * 24 * time.Hour, "7d", "T1"},
			},
		},
		{
			name:  "max_tier defaults to T1",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 10, window: 30d}\n",
			want:  []string{"connect_resume_connector"},
			rules: map[string]ruleWant{"connect_resume_connector": {10, 30 * 24 * time.Hour, "30d", "T1"}},
		},
		{
			name: "auto_approve on other actions is no conflict",
			extra: "    auto_approve: [connect_restart_task]\n" +
				"    autonomy:\n      connect_resume_connector: {after_verified: 10, window: 30d}\n",
			want:  []string{"connect_resume_connector"},
			rules: map[string]ruleWant{"connect_resume_connector": {10, 30 * 24 * time.Hour, "30d", "T1"}},
		},
		{
			// One anchored rule shared by two actions is plain YAML; the
			// decoder resolves aliases everywhere else in the file.
			name: "a rule shared through a YAML alias",
			extra: "    autonomy:\n" +
				"      connect_resume_connector: &shared {after_verified: 10, window: 30d, max_tier: T0}\n" +
				"      connect_restart_task: *shared\n",
			want: []string{"connect_restart_task", "connect_resume_connector"},
			rules: map[string]ruleWant{
				"connect_restart_task":     {10, 30 * 24 * time.Hour, "30d", "T0"},
				"connect_resume_connector": {10, 30 * 24 * time.Hour, "30d", "T0"},
			},
		},
		{name: "absent block", extra: "", want: []string{}},
		{name: "empty block", extra: "    autonomy: {}\n", want: []string{}},
		{name: "null block means no rules", extra: "    autonomy:\n", want: []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := tc.registry
			if reg == nil {
				reg = registry
			}
			o, err := LoadWithActions(writeTemp(t, autonomyDoc(tc.extra)), reg)
			if err != nil {
				t.Fatalf("LoadWithActions: %v", err)
			}
			p, ok := o.Pipeline("orders")
			if !ok {
				t.Fatal("pipeline orders not found")
			}
			if got := p.AutonomyActions(); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("AutonomyActions() = %#v, want %#v", got, tc.want)
			}
			for action, want := range tc.rules {
				got, ok := o.AutonomyRule("orders", action)
				if !ok {
					t.Fatalf("AutonomyRule(%q) = not found", action)
				}
				wantRule := AutonomyRule{
					Action:        action,
					AfterVerified: want.after,
					Window:        want.window,
					WindowText:    want.text,
					MaxTier:       want.tier,
				}
				if got != wantRule {
					t.Errorf("AutonomyRule(%q) = %+v, want %+v", action, got, wantRule)
				}
			}
		})
	}
}

// A whole block aliased from another pipeline is resolved by the decoder
// before the block's unmarshaler sees it.
func TestAutonomyBlockAlias(t *testing.T) {
	doc := "pipelines:\n" +
		pipelineYAML("orders", "    autonomy: &rules\n      connect_restart_task: {after_verified: 4, window: 3d}\n") +
		"  - id: refunds\n    topic_prefix: rf\n    source_tables: [public.r]\n    connectors: [c2]\n    autonomy: *rules\n" +
		sixLayers
	o, err := LoadWithActions(writeTemp(t, doc), []string{"connect_restart_task"})
	if err != nil {
		t.Fatalf("LoadWithActions: %v", err)
	}
	for _, id := range []string{"orders", "refunds"} {
		r, ok := o.AutonomyRule(id, "connect_restart_task")
		if !ok || r.AfterVerified != 4 || r.Window != 72*time.Hour || r.WindowText != "3d" || r.MaxTier != "T1" {
			t.Fatalf("AutonomyRule(%q) = %+v, %v", id, r, ok)
		}
	}
}

func TestAutonomyBlockOnBatchPipeline(t *testing.T) {
	const doc = `pipelines:
  - id: nightly
    kind: batch
    dags: [nightly_dag]
    autonomy:
      pg_resnapshot_rows: {after_verified: 5, window: 2d}
layers: {}
`
	o, err := LoadWithActions(writeTemp(t, doc), []string{"pg_resnapshot_rows"})
	if err != nil {
		t.Fatalf("LoadWithActions: %v", err)
	}
	if got, ok := o.AutonomyRule("nightly", "pg_resnapshot_rows"); !ok || got.Window != 48*time.Hour || got.MaxTier != "T1" {
		t.Fatalf("AutonomyRule = %+v, %v", got, ok)
	}
}

func TestAutonomyBlockRefusals(t *testing.T) {
	known := []string{"connect_resume_connector", "connect_restart_task"}
	// rule is one connect_resume_connector rule with the given fields.
	rule := func(fields string) string {
		return "    autonomy:\n      connect_resume_connector: {" + fields + "}\n"
	}
	cases := []struct {
		name    string
		extra   string
		actions []string
		want    []string // all must appear in the error
		loadOK  bool     // plain Load accepts the file; only the registry check refuses it
	}{
		{
			name:  "after_verified below the floor",
			extra: rule("after_verified: 2, window: 30d"),
			want:  []string{"autonomy.connect_resume_connector", "after_verified 2", "want 3 to 100"},
		},
		{
			name:  "after_verified above the ceiling",
			extra: rule("after_verified: 101, window: 30d"),
			want:  []string{"after_verified 101", "want 3 to 100"},
		},
		{
			name:  "after_verified missing",
			extra: rule("window: 30d"),
			want:  []string{"after_verified 0", "want 3 to 100"},
		},
		{
			name:  "after_verified null",
			extra: rule("after_verified: null, window: 30d"),
			want:  []string{"after_verified 0", "want 3 to 100"},
		},
		{
			name:  "after_verified negative",
			extra: rule("after_verified: -3, window: 30d"),
			want:  []string{"after_verified -3", "want 3 to 100"},
		},
		{
			name:  "after_verified is fractional",
			extra: rule("after_verified: 3.5, window: 30d"),
			want:  []string{"after_verified", "whole number", `"3.5"`},
		},
		{
			name:  "after_verified is a string",
			extra: rule(`after_verified: "10", window: 30d`),
			want:  []string{"after_verified", "whole number"},
		},
		{
			name:  "after_verified in exponent form",
			extra: rule("after_verified: 3e0, window: 30d"),
			want:  []string{"after_verified", "whole number", `"3e0"`},
		},
		{
			name:  "after_verified is a bool",
			extra: rule("after_verified: true, window: 30d"),
			want:  []string{"after_verified", "whole number"},
		},
		// yaml.v3 resolves each of these to !!int (and 010 to octal 8), so
		// the tag check alone would accept a spelling the author did not mean.
		{
			name:  "after_verified in hex",
			extra: rule("after_verified: 0x3, window: 30d"),
			want:  []string{"after_verified", "plain decimal", `"0x3"`},
		},
		{
			name:  "after_verified with a plus sign",
			extra: rule("after_verified: +3, window: 30d"),
			want:  []string{"after_verified", "plain decimal", `"+3"`},
		},
		{
			name:  "after_verified with a leading zero",
			extra: rule("after_verified: 03, window: 30d"),
			want:  []string{"after_verified", "plain decimal", `"03"`},
		},
		{
			name:  "after_verified in YAML 1.1 octal (010 is 8)",
			extra: rule("after_verified: 010, window: 30d"),
			want:  []string{"after_verified", "plain decimal", `"010"`},
		},
		{
			name:  "after_verified with digit separators",
			extra: rule("after_verified: 1_0, window: 30d"),
			want:  []string{"after_verified", "plain decimal", `"1_0"`},
		},
		{
			name:  "after_verified in YAML 1.2 octal",
			extra: rule("after_verified: 0o7, window: 30d"),
			want:  []string{"after_verified", "plain decimal", `"0o7"`},
		},
		{
			name:  "after_verified in binary",
			extra: rule("after_verified: 0b11, window: 30d"),
			want:  []string{"after_verified", "plain decimal", `"0b11"`},
		},
		{
			name:  "window zero days",
			extra: rule("after_verified: 10, window: 0d"),
			want:  []string{`window "0d"`, "want 1d to 90d"},
		},
		{
			name:  "window over 90 days",
			extra: rule("after_verified: 10, window: 91d"),
			want:  []string{`window "91d"`, "want 1d to 90d"},
		},
		// The contract writes the window in whole days as <n>d: no hours,
		// fractions, mixed units, signs, spaces or leading zeros.
		{
			name:  "window in hours",
			extra: rule("after_verified: 10, window: 720h"),
			want:  []string{`window "720h"`, "whole days"},
		},
		{
			name:  "window under one day in hours",
			extra: rule("after_verified: 10, window: 23h"),
			want:  []string{`window "23h"`, "want 1d to 90d"},
		},
		{
			name:  "window over 90 days in hours",
			extra: rule("after_verified: 10, window: 2161h"),
			want:  []string{`window "2161h"`, "want 1d to 90d"},
		},
		{
			name:  "window in mixed units",
			extra: rule("after_verified: 10, window: 2d12h"),
			want:  []string{`window "2d12h"`, "whole days"},
		},
		{
			name:  "window in fractional days",
			extra: rule("after_verified: 10, window: 1.5d"),
			want:  []string{`window "1.5d"`, "whole days"},
		},
		{
			name:  "window with a leading zero",
			extra: rule("after_verified: 10, window: 07d"),
			want:  []string{`window "07d"`, "whole days"},
		},
		{
			name:  "window with a plus sign",
			extra: rule("after_verified: 10, window: +1d"),
			want:  []string{`window "+1d"`, "whole days"},
		},
		{
			name:  "window negative",
			extra: rule("after_verified: 10, window: -1d"),
			want:  []string{`window "-1d"`, "whole days"},
		},
		{
			name:  "window with a space inside quotes",
			extra: rule(`after_verified: 10, window: " 30d"`),
			want:  []string{`window " 30d"`, "whole days"},
		},
		{
			name:  "window with an uppercase unit",
			extra: rule("after_verified: 10, window: 1D"),
			want:  []string{`window "1D"`, "whole days"},
		},
		{
			name:  "window in exponent form",
			extra: rule("after_verified: 10, window: 1e2d"),
			want:  []string{`window "1e2d"`, "whole days"},
		},
		{
			name:  "window too long for an int",
			extra: rule("after_verified: 10, window: 99999999999999999999d"),
			want:  []string{`window "99999999999999999999d"`, "want 1d to 90d"},
		},
		{
			name:  "window with two day units",
			extra: rule("after_verified: 10, window: 1d1d"),
			want:  []string{`window "1d1d"`, "whole days"},
		},
		{
			name:  "window unit only",
			extra: rule("after_verified: 10, window: d"),
			want:  []string{`window "d"`, "whole days"},
		},
		{
			name:  "window garbage",
			extra: rule("after_verified: 10, window: garbage"),
			want:  []string{`window "garbage"`, "whole days"},
		},
		{
			name:  "window empty",
			extra: rule(`after_verified: 10, window: ""`),
			want:  []string{`window ""`, "want 1d to 90d"},
		},
		{
			name:  "window missing",
			extra: rule("after_verified: 10"),
			want:  []string{`window ""`, "want 1d to 90d"},
		},
		{
			name:  "window is a bare number",
			extra: rule("after_verified: 10, window: 30"),
			want:  []string{`window "30"`, "whole days"},
		},
		{
			name:  "T3 is refused",
			extra: rule("after_verified: 10, window: 30d, max_tier: T3"),
			want:  []string{"max_tier", `"T3"`, "never earned"},
		},
		{
			name:  "unknown tier",
			extra: rule("after_verified: 10, window: 30d, max_tier: T4"),
			want:  []string{"max_tier", `"T4"`},
		},
		{
			name:  "tier is case-sensitive",
			extra: rule("after_verified: 10, window: 30d, max_tier: t1"),
			want:  []string{"max_tier", `"t1"`},
		},
		{
			name:  "tier is a bare number",
			extra: rule("after_verified: 10, window: 30d, max_tier: 1"),
			want:  []string{"max_tier", `"1"`},
		},
		{
			name:    "unknown action",
			extra:   "    autonomy:\n      no_such_action: {after_verified: 10, window: 30d}\n",
			actions: []string{"connect_resume_connector"},
			want:    []string{`pipeline "orders"`, `"no_such_action"`, "unknown action", "connect_resume_connector"},
			loadOK:  true,
		},
		{
			name: "an action in both lists",
			extra: "    auto_approve: [connect_resume_connector]\n" +
				"    autonomy:\n      connect_resume_connector: {after_verified: 10, window: 30d}\n",
			want: []string{`"connect_resume_connector"`, "also in auto_approve"},
		},
		{
			name:  "unknown sub-key",
			extra: rule("after_verifieds: 10, window: 30d"),
			want:  []string{"unknown field", "after_verifieds", "after_verified, window, max_tier"},
		},
		{
			// yaml.v3 refuses a repeated key when it decodes the rule.
			name:  "sub-key listed twice",
			extra: rule("after_verified: 10, window: 30d, window: 40d"),
			want:  []string{"autonomy.connect_resume_connector", "already defined"},
		},
		{
			name: "merge key inside a rule",
			extra: "    autonomy:\n" +
				"      connect_restart_task: &base {after_verified: 10, window: 30d}\n" +
				"      connect_resume_connector: {<<: *base, max_tier: T0}\n",
			want: []string{"autonomy.connect_resume_connector", "unknown field"},
		},
		{
			name: "merge key as an action",
			extra: "    autonomy:\n" +
				"      connect_restart_task: &base {after_verified: 10, window: 30d}\n" +
				"      <<: *base\n",
			want: []string{"merge key << is not supported"},
		},
		{
			name:  "rule is a scalar",
			extra: "    autonomy:\n      connect_resume_connector: 10\n",
			want:  []string{"want a mapping with after_verified, window and max_tier"},
		},
		{
			name:  "rule is a sequence",
			extra: "    autonomy:\n      connect_resume_connector: [10, 30d]\n",
			want:  []string{"want a mapping with after_verified, window and max_tier"},
		},
		{
			name:  "rule is null",
			extra: "    autonomy:\n      connect_resume_connector:\n",
			want:  []string{"rule is empty (null)"},
		},
		{
			name:  "action key is not an action name",
			extra: "    autonomy:\n      Bad.Key: {after_verified: 10, window: 30d}\n",
			want:  []string{`"Bad.Key" is not an action name`},
		},
		{
			name: "action listed twice",
			extra: "    autonomy:\n" +
				"      connect_resume_connector: {after_verified: 10, window: 30d}\n" +
				"      connect_resume_connector: {after_verified: 20, window: 30d}\n",
			want: []string{"autonomy.connect_resume_connector", "listed twice"},
		},
		{
			name:  "block is a list",
			extra: "    autonomy: [connect_resume_connector]\n",
			want:  []string{"want a mapping of action name to rule"},
		},
		{
			name:  "block key is not a scalar",
			extra: "    autonomy:\n      ? [a, b]\n      : {after_verified: 10, window: 30d}\n",
			want:  []string{"want an action name as the key"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actions := known
			if tc.actions != nil {
				actions = tc.actions
			}
			path := writeTemp(t, autonomyDoc(tc.extra))
			_, err := LoadWithActions(path, actions)
			if err == nil {
				t.Fatal("LoadWithActions succeeded, want an error")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not contain %q", err, w)
				}
			}
			_, loadErr := Load(path)
			if (loadErr == nil) != tc.loadOK {
				t.Errorf("plain Load err = %v, want accepted = %v", loadErr, tc.loadOK)
			}
		})
	}
}

func TestAutonomyAccessors(t *testing.T) {
	const doc = `pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.t]
    connectors: [c1]
    autonomy:
      connect_restart_task: {after_verified: 4, window: 3d, max_tier: T0}
      connect_resume_connector: {after_verified: 10, window: 30d}
layers: {}
`
	o, err := LoadWithActions(writeTemp(t, doc), []string{"connect_restart_task", "connect_resume_connector"})
	if err != nil {
		t.Fatalf("LoadWithActions: %v", err)
	}
	p, _ := o.Pipeline("orders")
	got := p.AutonomyActions()
	want := []string{"connect_restart_task", "connect_resume_connector"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AutonomyActions() = %#v, want %#v", got, want)
	}
	// The returned list is a copy: changing it must not change the config.
	got[0] = "changed"
	again, _ := o.Pipeline("orders")
	if !reflect.DeepEqual(again.AutonomyActions(), want) {
		t.Fatalf("AutonomyActions() changed through the returned slice: %#v", again.AutonomyActions())
	}

	if _, ok := o.AutonomyRule("orders", "no_such_action"); ok {
		t.Fatal("AutonomyRule found an action outside the block")
	}
	if _, ok := o.AutonomyRule("missing", "connect_restart_task"); ok {
		t.Fatal("AutonomyRule found a rule on an unknown pipeline")
	}
	var none *Owners
	if _, ok := none.AutonomyRule("orders", "connect_restart_task"); ok {
		t.Fatal("nil Owners returned a rule")
	}

	var empty Pipeline
	if list := empty.AutonomyActions(); list == nil || len(list) != 0 {
		t.Fatalf("empty pipeline list = %#v, want a non-nil empty slice", list)
	}
	var nilP *Pipeline
	if list := nilP.AutonomyActions(); list == nil || len(list) != 0 {
		t.Fatalf("nil pipeline list = %#v, want a non-nil empty slice", list)
	}
	if _, ok := nilP.AutonomyRule("connect_restart_task"); ok {
		t.Fatal("nil pipeline returned a rule")
	}
}

// KAN-463 TC5: a rule is per (pipeline, action). The same action on a
// pipeline of another tenant has its own rule, and a tenant's view (Only)
// holds only its own.
func TestAutonomyRulesArePerPipelineAcrossTenants(t *testing.T) {
	doc := twoTenants()
	doc = strings.Replace(doc, "    owners:\n      postgres: {approvers: [user:alice]",
		"    autonomy:\n      connect_restart_task: {after_verified: 5, window: 10d, max_tier: T0}\n"+
			"    owners:\n      postgres: {approvers: [user:alice]", 1)
	doc = strings.Replace(doc, "    owners:\n      kafka: {approvers: [user:bob]}",
		"    autonomy:\n      connect_restart_task: {after_verified: 50, window: 60d, max_tier: T2}\n"+
			"    owners:\n      kafka: {approvers: [user:bob]}", 1)
	if strings.Count(doc, "autonomy:") != 2 {
		t.Fatal("the two-tenant fixture changed; the autonomy blocks were not inserted")
	}
	o, err := LoadWithActions(writeTemp(t, doc), []string{"connect_restart_task"})
	if err != nil {
		t.Fatalf("LoadWithActions: %v", err)
	}
	pa, ok := o.AutonomyRule("pa", "connect_restart_task")
	if want := (AutonomyRule{Action: "connect_restart_task", AfterVerified: 5, Window: 10 * 24 * time.Hour, WindowText: "10d", MaxTier: "T0"}); !ok || pa != want {
		t.Fatalf("pa rule = %+v, %v; want %+v", pa, ok, want)
	}
	pb, ok := o.AutonomyRule("pb", "connect_restart_task")
	if want := (AutonomyRule{Action: "connect_restart_task", AfterVerified: 50, Window: 60 * 24 * time.Hour, WindowText: "60d", MaxTier: "T2"}); !ok || pb != want {
		t.Fatalf("pb rule = %+v, %v; want %+v", pb, ok, want)
	}
	view := o.Only("pa")
	if _, ok := view.AutonomyRule("pb", "connect_restart_task"); ok {
		t.Fatal("tenant a's view returned tenant b's rule")
	}
	if r, ok := view.AutonomyRule("pa", "connect_restart_task"); !ok || r != pa {
		t.Fatalf("tenant a's view rule = %+v, %v; want %+v", r, ok, pa)
	}
}

// KAN-463 TC3: every owners file in the repository predates the block, loads
// unchanged and declares no rule.
func TestExistingOwnersFilesLoadWithoutAutonomy(t *testing.T) {
	root := filepath.Join("..", "..")
	files, err := filepath.Glob(filepath.Join(root, "dev", "owners*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("glob dev/owners*.yaml: %v, %d files", err, len(files))
	}
	files = append(files,
		filepath.Join(root, "deploy", "config", "owners.example.yaml"),
		filepath.Join(root, "internal", "discover", "testdata", "draft.golden.yaml"),
		filepath.Join(root, "evals", "scenarios", "heldout", "fixtures", "owners.yaml"),
	)
	for _, f := range files {
		o, err := Load(f)
		if err != nil {
			t.Errorf("%s: %v", f, err)
			continue
		}
		for i := range o.Pipelines {
			p := &o.Pipelines[i]
			if len(p.Autonomy) != 0 || len(p.AutonomyActions()) != 0 {
				t.Errorf("%s: pipeline %q declares autonomy rules %v; none expected before AU1b", f, p.ID, p.AutonomyActions())
			}
		}
	}
}

func TestValidateAutonomyActions(t *testing.T) {
	o, err := Load(writeTemp(t, autonomyDoc(
		"    autonomy:\n      connect_resume_connector: {after_verified: 10, window: 30d}\n")))
	if err != nil {
		t.Fatalf("Load must accept a well-formed action before the registry exists: %v", err)
	}
	if err := o.ValidateAutonomyActions([]string{"connect_resume_connector", "other"}); err != nil {
		t.Fatalf("known action refused: %v", err)
	}
	err = o.ValidateAutonomyActions([]string{"other"})
	if err == nil || !strings.Contains(err.Error(), `pipeline "orders"`) ||
		!strings.Contains(err.Error(), `"connect_resume_connector"`) ||
		!strings.Contains(err.Error(), "unknown action") {
		t.Fatalf("err = %v, want one naming the pipeline and the action", err)
	}
	if err := o.ValidateAutonomyActions(nil); err == nil {
		t.Fatal("nil registry must refuse an action")
	}
	// A Deferred action (a pull-request fix) is never approved by a policy.
	if err := o.ValidateAutonomyActionsDeferred([]string{"connect_resume_connector"}, []string{"repo_open_pull_request"}); err != nil {
		t.Fatalf("a deferred action outside the block refused the file: %v", err)
	}
	err = o.ValidateAutonomyActionsDeferred([]string{"connect_resume_connector"}, []string{"connect_resume_connector"})
	if err == nil || !strings.Contains(err.Error(), "never approved by a policy") {
		t.Fatalf("err = %v, want a deferred refusal", err)
	}
	path := writeTemp(t, autonomyDoc(
		"    autonomy:\n      repo_open_pull_request: {after_verified: 10, window: 30d}\n"))
	if _, err := LoadWithDeferred(path, []string{"repo_open_pull_request"}, []string{"repo_open_pull_request"}); err == nil ||
		!strings.Contains(err.Error(), "never approved by a policy") {
		t.Fatalf("LoadWithDeferred err = %v, want a deferred refusal", err)
	}
	if _, err := LoadWithDeferred(path, []string{"repo_open_pull_request"}, []string{"other"}); err != nil {
		t.Fatalf("LoadWithDeferred refused a known, non-deferred action: %v", err)
	}
	var noRules *Owners
	if err := noRules.ValidateAutonomyActions(nil); err != nil {
		t.Fatalf("nil Owners has nothing to refuse: %v", err)
	}
	empty, err := Load(writeTemp(t, autonomyDoc("")))
	if err != nil {
		t.Fatal(err)
	}
	if err := empty.ValidateAutonomyActions(nil); err != nil {
		t.Fatalf("a pipeline without autonomy rules needs no registry: %v", err)
	}
	// A hand-built Owners (never through Load) is checked on the block as
	// written, not on validated rules it does not have.
	built := &Owners{Pipelines: []Pipeline{{ID: "orders", Autonomy: AutonomySpec{
		"connect_resume_connector": {AfterVerified: 10, WindowText: "30d"},
	}}}}
	if err := built.ValidateAutonomyActions([]string{"other"}); err == nil || !strings.Contains(err.Error(), `"connect_resume_connector"`) {
		t.Fatalf("hand-built block: err = %v, want the unknown action refused", err)
	}
	if err := built.ValidateAutonomyActions([]string{"connect_resume_connector"}); err != nil {
		t.Fatalf("hand-built block with a known action: %v", err)
	}
}

func TestParseAutonomyWindow(t *testing.T) {
	cases := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"30d", 30 * 24 * time.Hour, false},
		{"1d", 24 * time.Hour, false},
		{"2d", 48 * time.Hour, false},
		{"89d", 89 * 24 * time.Hour, false},
		{"90d", 90 * 24 * time.Hour, false},
		{"0d", 0, true},
		{"91d", 0, true},
		// The contract writes whole days as <n>d: no other unit or spelling.
		{"720h", 0, true},
		{"24h", 0, true},
		{"2d12h", 0, true},
		{"1.5d", 0, true},
		{"-1d", 0, true},
		{"+1d", 0, true},
		{"07d", 0, true},
		{"1h30m", 0, true},
		{"1e2d", 0, true},
		{"99999999999999999999d", 0, true},
		{"1d1d", 0, true},
		{" 30d", 0, true},
		{"30d ", 0, true},
		{"30", 0, true},
		{"d", 0, true},
		{"", 0, true},
		{"garbage", 0, true},
		{"1w", 0, true},
		{"1D", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseAutonomyWindow(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseAutonomyWindow(%q) = %v, want an error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseAutonomyWindow(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("parseAutonomyWindow(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
