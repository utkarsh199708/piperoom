package agent

import (
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/actions/pgactions"
)

// pg_snapshot_signal requires signal_table (P3b-4): the connector's
// signal.data.collection as read with connect_config. propose_fix's line for
// it carries the param's own description, so the model is told where to read
// the value without a word added to the pinned CDC system prompt.
func TestProposeFixDescriptionSignalTableHint(t *testing.T) {
	reg := actions.NewRegistry()
	if err := reg.Register(pgactions.NewSnapshotSignal(pgactions.SnapshotSignalDeps{})); err != nil {
		t.Fatal(err)
	}
	d := (&proposeFixTool{actions: reg}).Description()
	var line string
	for _, l := range strings.Split(d, "\n") {
		if strings.HasPrefix(l, "- pg_snapshot_signal(") {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("no pg_snapshot_signal line:\n%s", d)
	}
	for _, want := range []string{"signal_table:string [", "signal.data.collection", "connect_config", "[postgres]"} {
		if !strings.Contains(line, want) {
			t.Errorf("pg_snapshot_signal line lacks %q:\n%s", want, line)
		}
	}
	if strings.Contains(line, "signal_table?:") {
		t.Errorf("signal_table shown as optional:\n%s", line)
	}
}
