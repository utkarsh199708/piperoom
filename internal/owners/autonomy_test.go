package owners

import (
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
			name: "Go durations at the bounds, hours and mixed units",
			extra: "    autonomy:\n" +
				"      connect_resume_connector: {after_verified: 10, window: 720h}\n" +
				"      connect_restart_task: {after_verified: 4, window: 24h, max_tier: T0}\n" +
				"      kafka_replay_range: {after_verified: 5, window: 2160h, max_tier: T2}\n" +
				"      pg_resnapshot_rows: {after_verified: 6, window: 2d12h, max_tier: T1}\n",
			want: []string{"connect_restart_task", "connect_resume_connector", "kafka_replay_range", "pg_resnapshot_rows"},
			rules: map[string]ruleWant{
				"connect_restart_task":     {4, 24 * time.Hour, "24h", "T0"},
				"connect_resume_connector": {10, 720 * time.Hour, "720h", "T1"},
				"kafka_replay_range":       {5, 2160 * time.Hour, "2160h", "T2"},
				"pg_resnapshot_rows":       {6, 60 * time.Hour, "2d12h", "T1"},
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
	cases := []struct {
		name    string
		extra   string
		actions []string
		want    []string // all must appear in the error
		loadOK  bool     // plain Load accepts the file; only the registry check refuses it
	}{
		{
			name:  "after_verified below the floor",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 2, window: 30d}\n",
			want:  []string{"autonomy.connect_resume_connector", "after_verified 2", "want 3 to 100"},
		},
		{
			name:  "after_verified above the ceiling",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 101, window: 30d}\n",
			want:  []string{"after_verified 101", "want 3 to 100"},
		},
		{
			name:  "after_verified missing",
			extra: "    autonomy:\n      connect_resume_connector: {window: 30d}\n",
			want:  []string{"after_verified 0", "want 3 to 100"},
		},
		{
			name:  "after_verified is fractional",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 3.5, window: 30d}\n",
			want:  []string{"after_verified", "whole number"},
		},
		{
			name:  "after_verified is a string",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: \"10\", window: 30d}\n",
			want:  []string{"after_verified", "whole number"},
		},
		{
			name:  "window zero days",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 10, window: 0d}\n",
			want:  []string{`window "0d"`, "want 1d to 90d"},
		},
		{
			name:  "window over 90 days",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 10, window: 91d}\n",
			want:  []string{`window "91d"`, "want 1d to 90d"},
		},
		{
			name:  "window under one day in hours",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 10, window: 23h}\n",
			want:  []string{`window "23h"`, "want 1d to 90d"},
		},
		{
			name:  "window over 90 days in hours",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 10, window: 2161h}\n",
			want:  []string{`window "2161h"`, "want 1d to 90d"},
		},
		{
			name:  "window garbage",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 10, window: garbage}\n",
			want:  []string{`window "garbage"`, "Go-style duration"},
		},
		{
			name:  "window empty",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 10, window: \"\"}\n",
			want:  []string{`window ""`, "want a duration between 1d and 90d"},
		},
		{
			name:  "window is a bare number",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 10, window: 30}\n",
			want:  []string{`window "30"`, "Go-style duration"},
		},
		{
			name:  "T3 is refused",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 10, window: 30d, max_tier: T3}\n",
			want:  []string{"max_tier", `"T3"`, "never earned"},
		},
		{
			name:  "unknown tier",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 10, window: 30d, max_tier: T4}\n",
			want:  []string{"max_tier", `"T4"`},
		},
		{
			name:  "tier is case-sensitive",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 10, window: 30d, max_tier: t1}\n",
			want:  []string{"max_tier", `"t1"`},
		},
		{
			name:  "tier is a bare number",
			extra: "    autonomy:\n      connect_resume_connector: {after_verified: 10, window: 30d, max_tier: 1}\n",
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
			extra: "    autonomy:\n      connect_resume_connector: {after_verifieds: 10, window: 30d}\n",
			want:  []string{"unknown field", "after_verifieds", "after_verified, window, max_tier"},
		},
		{
			name:  "rule is a scalar",
			extra: "    autonomy:\n      connect_resume_connector: 10\n",
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
}

func TestParseAutonomyWindow(t *testing.T) {
	cases := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"30d", 30 * 24 * time.Hour, false},
		{"720h", 720 * time.Hour, false},
		{"1d", 24 * time.Hour, false},
		{"90d", 90 * 24 * time.Hour, false},
		{"2d12h", 60 * time.Hour, false},
		{"1.5d", 36 * time.Hour, false},
		{"-1d", -24 * time.Hour, false},
		{"1h30m", 90 * time.Minute, false},
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
