package plan

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStrimziLadderRungs(t *testing.T) {
	rungs, err := StrimziLadder("postgres", SinkNone)
	if err != nil {
		t.Fatal(err)
	}
	if len(rungs) != 8 {
		t.Fatalf("len = %d", len(rungs))
	}
	for i, want := range []string{RungConnector, RungTasks} {
		if rungs[i].Name != want || len(rungs[i].Reads) != 1 || rungs[i].Reads[0] != ReadK8sGet {
			t.Errorf("rung %d = %s %v", i+1, rungs[i].Name, rungs[i].Reads)
		}
	}
	if rungs[2].Name != RungCanaryTopic || rungs[6].NotApplicable == "" {
		t.Errorf("rungs 3 to 8 are not Ladder's: %+v", rungs[2:])
	}
	if _, err := StrimziLadder("cobol", SinkNone); err == nil {
		t.Error("unknown source accepted")
	}
}

func connectorObj(conds, state, tasks string) json.RawMessage {
	status := `{"conditions":[` + conds + `]`
	if state != "" {
		status += `,"connectorStatus":{"connector":{"state":"` + state + `","trace":"org.apache.kafka.connect.errors.ConnectException: boom\n\tat x"},"tasks":[` + tasks + `]}`
	}
	status += `}`
	return json.RawMessage(`{"kind":"KafkaConnector","status":` + status + `}`)
}

func TestStrimziPredicates(t *testing.T) {
	readyTrue := `{"type":"Ready","status":"True"}`
	cases := []struct {
		name          string
		data          json.RawMessage
		conn, tasks   bool
		connWhy, task string
	}{
		{"running", connectorObj(readyTrue, "RUNNING", `{"id":0,"state":"RUNNING"}`), true, true, "RUNNING", "all 1 tasks"},
		{"not ready", connectorObj(`{"type":"NotReady","status":"True","reason":"ConnectRestException","message":"Connector plugin not found"}`, "", ""), false, false, "plugin not found", "no tasks"},
		{"failed connector", connectorObj(readyTrue, "FAILED", `{"id":0,"state":"FAILED","trace":"java.lang.IllegalStateException: slot\nmore"}`), false, false, "ConnectException: boom", "task 0 is FAILED: java.lang.IllegalStateException: slot"},
		{"no status", json.RawMessage(`{"kind":"KafkaConnector"}`), false, false, "not reconciled", "not reconciled"},
		{"wrong kind", json.RawMessage(`{"kind":"KafkaConnect","status":{}}`), false, false, "did not return", "did not return"},
		{"no ready yet", connectorObj("", "", ""), false, false, "no Ready condition", "no tasks"},
	}
	for _, tc := range cases {
		ok, why := strimziConnectorRunning(tc.data)
		if ok != tc.conn || !strings.Contains(why, tc.connWhy) {
			t.Errorf("%s: connector = (%v, %q)", tc.name, ok, why)
		}
		if strings.Contains(why, "\tat x") {
			t.Errorf("%s: the stack leaked into the note: %q", tc.name, why)
		}
		ok, why = strimziTasksRunning(tc.data)
		if ok != tc.tasks || !strings.Contains(why, tc.task) {
			t.Errorf("%s: tasks = (%v, %q)", tc.name, ok, why)
		}
	}
}
