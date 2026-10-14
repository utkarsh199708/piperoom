package loadtest

import (
	"reflect"
	"strings"
	"testing"
)

func TestAgentLimitAndCapCheck(t *testing.T) {
	limits := []struct{ per, replicas, global, want int }{
		{3, 1, 0, 3},
		{3, 3, 0, 9},
		{3, 3, 4, 4},
		{3, 3, 20, 9}, // a global cap above the sum changes nothing
		{10, 2, 10, 10},
	}
	for _, tc := range limits {
		if got := agentLimit(tc.per, tc.replicas, tc.global); got != tc.want {
			t.Errorf("agentLimit(%d, %d, %d) = %d, want %d", tc.per, tc.replicas, tc.global, got, tc.want)
		}
	}
	caps := []struct {
		inFlight, per, replicas, global int
		ok                              bool
	}{
		{3, 3, 1, 0, true},
		{4, 3, 1, 0, false}, // one run over the cap
		{6, 3, 2, 0, true},
		{7, 3, 2, 0, false},
		{4, 3, 3, 3, false}, // the global cap is lower than 3 x 3
		{3, 3, 3, 3, true},
	}
	for _, tc := range caps {
		c := agentCapCheck(tc.inFlight, tc.per, tc.replicas, tc.global)
		if c.OK != tc.ok {
			t.Errorf("agentCapCheck(%d, %d, %d, %d).OK = %v, want %v (%s)", tc.inFlight, tc.per, tc.replicas, tc.global, c.OK, tc.ok, c.Detail)
		}
	}
}

func TestLiteModelAndDupRunCheck(t *testing.T) {
	st := ModelStats{Requests: 9, MaxInFlight: 2, Rooms: map[string]*RoomModelStats{
		"r_ok":      {RunStarts: 1, MaxInFlight: 1},
		"r_restart": {RunStarts: 2, MaxInFlight: 1}, // started twice
		"r_overlap": {RunStarts: 1, MaxInFlight: 2}, // two calls at once
		"r_both":    {RunStarts: 3, MaxInFlight: 2}, // both
		"r_other":   {RunStarts: 5, MaxInFlight: 5}, // not one of the rooms asked about
		"r_zero":    {RunStarts: 0, MaxInFlight: 0}, // never ran
	}}
	l := liteModel(st, []string{"r_ok", "r_restart", "r_overlap", "r_both", "r_zero", "r_missing"})
	if l.DuplicateRuns != 2 || l.ConcurrentDup != 2 || l.Requests != 9 || l.MaxInFlight != 2 {
		t.Fatalf("liteModel = %+v", l)
	}
	if c := dupRunCheck(l); c.OK {
		t.Fatalf("duplicates must fail the check: %+v", c)
	}
	if c := dupRunCheck(liteModel(st, []string{"r_ok", "r_zero"})); !c.OK {
		t.Fatalf("clean rooms must pass: %+v", c)
	}
	if c := dupRunCheck(ModelStatsLite{ConcurrentDup: 1}); c.OK {
		t.Fatal("a concurrent duplicate alone must fail")
	}
}

func TestExecCountsAndCheck(t *testing.T) {
	total := map[string]int{"r_1": 1, "r_2": 2, "r_3": 1, "r_extra": 1}
	approved := map[string]bool{"r_1": true, "r_2": true, "r_3": true, "r_4": true}
	execs, dup, missing := execCounts(total, approved)
	if execs != 5 || dup != 1 || missing != 1 {
		t.Fatalf("execCounts = %d %d %d, want 5 1 1", execs, dup, missing)
	}
	good := ExecStats{Proposals: 4, Approved: 4, Executions: 4, ExecutedEvents: 4}
	if c := execCheck(good, 4); !c.OK {
		t.Fatalf("clean exec must pass: %s", c.Detail)
	}
	bad := map[string]ExecStats{
		"duplicate execution": {Proposals: 4, Approved: 4, Executions: 5, DuplicateExecutions: 1, ExecutedEvents: 4},
		"missing execution":   {Proposals: 4, Approved: 4, Executions: 3, MissingExecutions: 1, ExecutedEvents: 4},
		"duplicate event":     {Proposals: 4, Approved: 4, Executions: 4, ExecutedEvents: 5, DuplicateExecEvents: 1},
		"too few events":      {Proposals: 4, Approved: 4, Executions: 4, ExecutedEvents: 3},
		"not all approved":    {Proposals: 4, Approved: 3, Executions: 3, ExecutedEvents: 3},
		"approve error":       {Proposals: 4, Approved: 4, ApproveErr: 1, Executions: 4, ExecutedEvents: 4},
	}
	for name, e := range bad {
		if c := execCheck(e, 4); c.OK {
			t.Errorf("%s: check passed: %s", name, c.Detail)
		}
	}
}

func TestSeqAndLeakChecks(t *testing.T) {
	if !seqCheck(SubTotals{}).OK {
		t.Fatal("clean totals must pass")
	}
	for name, st := range map[string]SubTotals{
		"gap": {Gaps: 1}, "dup": {Dups: 1}, "missing": {MissingTail: 1}, "mismatch": {Mismatches: 1},
	} {
		if seqCheck(st).OK {
			t.Errorf("%s must fail", name)
		}
	}
	if !leakCheck(60, 60+goroutineSlack).OK || leakCheck(60, 61+goroutineSlack).OK {
		t.Fatal("leak check slack wrong")
	}
	c := labeled("subs=5: ", leakCheck(1, 1))
	if !strings.HasPrefix(c.Name, "subs=5: ") {
		t.Fatalf("label: %q", c.Name)
	}
}

func TestStaleContainers(t *testing.T) {
	ps := strings.Join([]string{
		"s2lt-pg-aaaa 100", // owner dead
		"s2lt-pg-bbbb 200", // owner alive
		"s2lt-pg-cccc",     // no owner label: not ours to judge
		"s2lt-pg-dddd x",   // unparsable owner
		"other-pg-eeee 100",
		"s2lt-it-pg-ffff 100", // another prefix
		"",
	}, "\n")
	alive := func(pid int) bool { return pid == 200 }
	got := staleContainers(ps, "s2lt", alive)
	if !reflect.DeepEqual(got, []string{"s2lt-pg-aaaa"}) {
		t.Fatalf("stale = %v", got)
	}
	if got := staleContainers(ps, "s2lt", func(int) bool { return true }); len(got) != 0 {
		t.Fatalf("live owners must keep their containers: %v", got)
	}
}

func TestProcessAlive(t *testing.T) {
	if processAlive(0) || processAlive(-1) {
		t.Fatal("non-positive pids are never alive")
	}
}

func TestWithQuery(t *testing.T) {
	got, err := withQuery("postgres://u:p@127.0.0.1:5432/lt_1?sslmode=disable", "pool_max_conns=20&application_name=x")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "pool_max_conns=20") || !strings.Contains(got, "sslmode=disable") || !strings.Contains(got, "application_name=x") ||
		!strings.HasPrefix(got, "postgres://u:p@127.0.0.1:5432/lt_1?") {
		t.Fatalf("withQuery = %q", got)
	}
	if got, _ := withQuery("postgres://h/db?sslmode=disable", "sslmode=require"); !strings.Contains(got, "sslmode=require") || strings.Contains(got, "disable") {
		t.Fatalf("a repeated key must be replaced: %q", got)
	}
	for _, bad := range []string{"%zz", "=1"} {
		if _, err := withQuery("postgres://h/db", bad); err == nil {
			t.Errorf("withQuery(%q) accepted", bad)
		}
	}
	if _, err := withQuery("postgres://h/%zz", "a=1"); err == nil {
		t.Error("a bad URL was accepted")
	}
}

func TestCheckModelEnv(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	ok := map[string]string{"ANTHROPIC_BASE_URL": "http://127.0.0.1:9/", "ANTHROPIC_API_KEY": FakeAPIKey}
	if err := checkModelEnv(env(ok)); err != nil {
		t.Fatal(err)
	}
	for name, m := range map[string]map[string]string{
		"no key":     {"ANTHROPIC_BASE_URL": "http://127.0.0.1:9/"},
		"real key":   {"ANTHROPIC_BASE_URL": "http://127.0.0.1:9/", "ANTHROPIC_API_KEY": "sk-ant-real"},
		"remote url": {"ANTHROPIC_BASE_URL": "https://api.anthropic.com", "ANTHROPIC_API_KEY": FakeAPIKey},
		"no url":     {"ANTHROPIC_API_KEY": FakeAPIKey},
	} {
		if err := checkModelEnv(env(m)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
