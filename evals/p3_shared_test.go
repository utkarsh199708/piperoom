package evals

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A scenario whose watch block owners.Load refuses is an error of
// LoadScenario, not a silent plain decode.
func TestLoadScenarioReportsOwnersError(t *testing.T) {
	b, err := os.ReadFile("scenarios/consumer-bad-deploy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	bad := strings.Replace(string(b), "written_by: order-sync", "written_by: not-a-group", 1)
	if bad == string(b) {
		t.Fatal("fixture no longer has the written_by line")
	}
	p := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(p, []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadScenario(p); err == nil || !strings.Contains(err.Error(), "owners") {
		t.Fatalf("LoadScenario = %v, want an owners error", err)
	}
}

// The P3 actions come after the seven, in production's order.
func TestLiveActionsOrderIsProductions(t *testing.T) {
	reg, err := LiveActions(Resources{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, a := range reg.All() {
		names = append(names, a.Name())
	}
	got := strings.Join(names, ",")
	for _, want := range []string{"kafka_reset_consumer_offsets", "dlq_replay", "connect_snapshot_table", "pg_drop_replication_slot", "pg_add_publication_table", "pg_snapshot_signal"} {
		if !strings.Contains(got, want) {
			t.Errorf("action %s missing from %s", want, got)
		}
	}
	if i, j := strings.Index(got, "connect_snapshot_table"), strings.Index(got, "pg_drop_replication_slot"); i > j {
		t.Errorf("snapshot after pg: %s", got)
	}
}

func TestWriteNotesShowActionAndTargetOnly(t *testing.T) {
	notes := writeNotes([]ReplayWrite{
		{Method: "Produce", Args: map[string]any{"topic": "t.dlq", "records": 3}},
		{Method: "UpdateConnectorConfigKeys", Args: map[string]any{"connector": "c", "changes": 2}},
	})
	if len(notes) != 2 || notes[0] != (WriteNote{Action: "Produce", Target: "t.dlq"}) || notes[1].Target != "c" {
		t.Fatalf("notes = %+v", notes)
	}
	var many []ReplayWrite
	for i := 0; i < 50; i++ {
		many = append(many, ReplayWrite{Method: "Resume"})
	}
	if got := len(writeNotes(many)); got != maxWriteNotes {
		t.Errorf("notes = %d, want cap %d", got, maxWriteNotes)
	}
}

func TestExecutedStepIsLoggedWithoutValues(t *testing.T) {
	wl := &writeLog{}
	x := &execScript{rp: &replayer{}, log: wl}
	x.note("connect_restart_task", []byte(`{"connector":"c","task":0,"password":"s3cret"}`))
	x.note("dlq_replay", []byte(`not json`))
	got := wl.snapshot()
	if len(got) != 2 || got[0].Args["connector"] != "c" || len(got[0].Args) != 1 || len(got[1].Args) != 0 {
		t.Fatalf("log = %+v", got)
	}
}

func TestReportListsReplayWrites(t *testing.T) {
	r := Report{Results: []RunResult{{Scenario: "s", Run: 1, Writes: []WriteNote{{Action: "connect_resume_connector", Target: "pg-orders-connector"}}}}}
	var b strings.Builder
	if err := r.WriteMarkdown(&b); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "Replay writes") || !strings.Contains(b.String(), "connect_resume_connector pg-orders-connector") {
		t.Errorf("markdown lacks the writes:\n%s", b.String())
	}
}
