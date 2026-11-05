package owners

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

// autonomyRuleDoc is one orders pipeline whose connect_restart_task rule has
// the given fields, or no block at all when fields is "".
func autonomyRuleDoc(fields string) string {
	if fields == "" {
		return autonomyDoc("")
	}
	return autonomyDoc("    autonomy:\n      connect_restart_task: {" + fields + "}\n")
}

func loadDoc(t *testing.T, doc string) *Owners {
	t.Helper()
	o, err := Load(writeTemp(t, doc))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return o
}

func TestAutonomyDiff(t *testing.T) {
	const base = "after_verified: 10, window: 30d, max_tier: T1"
	one := func(change string) []AutonomyChange {
		return []AutonomyChange{{Pipeline: "orders", Action: "connect_restart_task", Change: change}}
	}
	cases := []struct {
		name string
		prev string
		cur  string
		want []AutonomyChange
	}{
		{"unchanged", base, base, []AutonomyChange{}},
		{"more recoveries tightens", base, "after_verified: 20, window: 30d, max_tier: T1", one(AutonomyTightened)},
		{"fewer recoveries loosens", base, "after_verified: 5, window: 30d, max_tier: T1", one(AutonomyLoosened)},
		{"shorter window tightens", base, "after_verified: 10, window: 7d, max_tier: T1", one(AutonomyTightened)},
		{"longer window loosens", base, "after_verified: 10, window: 60d, max_tier: T1", one(AutonomyLoosened)},
		{"lower tier tightens", base, "after_verified: 10, window: 30d, max_tier: T0", one(AutonomyTightened)},
		{"higher tier loosens", base, "after_verified: 10, window: 30d, max_tier: T2", one(AutonomyLoosened)},
		{"the default tier is T1, so dropping it is no change", base, "after_verified: 10, window: 30d", []AutonomyChange{}},
		{"both directions at once is tightened", base, "after_verified: 20, window: 60d, max_tier: T2", one(AutonomyTightened)},
		{"rule removed", base, "", one(AutonomyRemoved)},
		{"rule added", "", base, one(AutonomyAdded)},
		{"no rules on either side", "", "", []AutonomyChange{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := AutonomyDiff(loadDoc(t, autonomyRuleDoc(tc.prev)), loadDoc(t, autonomyRuleDoc(tc.cur)))
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("AutonomyDiff = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestAutonomyDiffPipelinesAndNil(t *testing.T) {
	two := "pipelines:\n" +
		pipelineYAML("orders", "    autonomy:\n"+
			"      connect_restart_task: {after_verified: 10, window: 30d}\n"+
			"      kafka_replay_range: {after_verified: 10, window: 30d}\n") +
		"  - id: refunds\n    topic_prefix: rf\n    source_tables: [public.r]\n    connectors: [c2]\n" +
		"    autonomy:\n      connect_restart_task: {after_verified: 10, window: 30d}\n" +
		sixLayers
	one := autonomyDoc("    autonomy:\n      kafka_replay_range: {after_verified: 20, window: 30d}\n")
	prev, cur := loadDoc(t, two), loadDoc(t, one)
	// Sorted by pipeline then action; a pipeline that went away removes each
	// of its rules.
	want := []AutonomyChange{
		{Pipeline: "orders", Action: "connect_restart_task", Change: AutonomyRemoved},
		{Pipeline: "orders", Action: "kafka_replay_range", Change: AutonomyTightened},
		{Pipeline: "refunds", Action: "connect_restart_task", Change: AutonomyRemoved},
	}
	if got := AutonomyDiff(prev, cur); !reflect.DeepEqual(got, want) {
		t.Fatalf("AutonomyDiff = %+v, want %+v", got, want)
	}
	wantBack := []AutonomyChange{
		{Pipeline: "orders", Action: "connect_restart_task", Change: AutonomyAdded},
		{Pipeline: "orders", Action: "kafka_replay_range", Change: AutonomyLoosened},
		{Pipeline: "refunds", Action: "connect_restart_task", Change: AutonomyAdded},
	}
	if got := AutonomyDiff(cur, prev); !reflect.DeepEqual(got, wantBack) {
		t.Fatalf("reverse AutonomyDiff = %+v, want %+v", got, wantBack)
	}
	allAdded := []AutonomyChange{
		{Pipeline: "orders", Action: "connect_restart_task", Change: AutonomyAdded},
		{Pipeline: "orders", Action: "kafka_replay_range", Change: AutonomyAdded},
		{Pipeline: "refunds", Action: "connect_restart_task", Change: AutonomyAdded},
	}
	if got := AutonomyDiff(nil, prev); !reflect.DeepEqual(got, allAdded) {
		t.Fatalf("AutonomyDiff(nil, o) = %+v, want %+v", got, allAdded)
	}
	if got := AutonomyDiff(prev, nil); len(got) != 3 || got[0].Change != AutonomyRemoved || got[2].Change != AutonomyRemoved {
		t.Fatalf("AutonomyDiff(o, nil) = %+v, want three removals", got)
	}
	if got := AutonomyDiff(nil, nil); got == nil || len(got) != 0 {
		t.Fatalf("AutonomyDiff(nil, nil) = %#v, want an empty, non-nil slice", got)
	}
}

// KAN-463 TC4: Live.Reload applies a changed rule, and AutonomyDiff over the
// snapshots before and after reports a raised streak as tightened, a lowered
// one as loosened, a dropped block as removed and a new one as added.
func TestAutonomyReloadReportsChanges(t *testing.T) {
	refunds := func(after int) string {
		return "pipelines:\n  - id: refunds\n    topic_prefix: refunds\n    source_tables: [public.refunds]\n    connectors: [refunds-c]\n" +
			"    autonomy:\n      connect_restart_task: {after_verified: " + strconv.Itoa(after) + ", window: 30d}\n"
	}
	dir := writeDir(t, map[string]string{"refunds.yaml": refunds(10)})
	file := filepath.Join(dir, "refunds.yaml")
	live, err := NewLive(writeTemp(t, mainYAML), dir)
	if err != nil {
		t.Fatalf("NewLive: %v", err)
	}
	if r, ok := live.Load().AutonomyRule("refunds", "connect_restart_task"); !ok || r.AfterVerified != 10 {
		t.Fatalf("rule before any reload = %+v, %v", r, ok)
	}
	steps := []struct {
		name  string
		doc   string
		after int    // after_verified the reloaded snapshot must show; 0 = no rule
		want  string // the AutonomyDiff change; "" = none
	}{
		{"raised 10 to 20", refunds(20), 20, AutonomyTightened},
		{"unchanged", refunds(20), 20, ""},
		{"lowered 20 to 5", refunds(5), 5, AutonomyLoosened},
		{"block removed", dirPipeline("refunds"), 0, AutonomyRemoved},
		{"block added back", refunds(10), 10, AutonomyAdded},
	}
	for _, s := range steps {
		prev := live.Load()
		if err := os.WriteFile(file, []byte(s.doc), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := live.Reload(); err != nil {
			t.Fatalf("%s: Reload: %v", s.name, err)
		}
		cur := live.Load()
		r, ok := cur.AutonomyRule("refunds", "connect_restart_task")
		if ok != (s.after != 0) || r.AfterVerified != s.after {
			t.Fatalf("%s: AutonomyRule after reload = %+v, %v; want after_verified %d", s.name, r, ok, s.after)
		}
		got := AutonomyDiff(prev, cur)
		if s.want == "" {
			if len(got) != 0 {
				t.Fatalf("%s: AutonomyDiff = %+v, want none", s.name, got)
			}
			continue
		}
		want := []AutonomyChange{{Pipeline: "refunds", Action: "connect_restart_task", Change: s.want}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: AutonomyDiff = %+v, want %+v", s.name, got, want)
		}
	}
}
