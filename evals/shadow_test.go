package evals

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/owners"
)

const shadowSample = "testdata/shadow-run.json"

// ordersPipeline is the dev stack's pipeline, spelled out so the test does
// not depend on dev/owners.yaml.
func lookupOrders(id string) (owners.Pipeline, bool) {
	if id != "orders" {
		return owners.Pipeline{}, false
	}
	return owners.Pipeline{
		ID: "orders", Title: "Orders CDC", TopicPrefix: "pg",
		SourceTables: []string{"public.orders"}, Connectors: []string{"pg-orders-connector"},
		ConsumerGroups: []string{"order-sync"}, SinkTables: []string{"analytics.orders"},
		SinkTSColumn: "updated_at",
	}, true
}

// sampleDoc returns the sample as a generic map for a test to damage.
func sampleDoc(t *testing.T) map[string]any {
	t.Helper()
	b, err := os.ReadFile(shadowSample)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func encode(t *testing.T, m map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func sub(m map[string]any, k string) map[string]any { return m[k].(map[string]any) }

func evList(m map[string]any) []any { return m["events"].([]any) }

func TestScenarioFromShadowSample(t *testing.T) {
	raw, err := os.ReadFile(shadowSample)
	if err != nil {
		t.Fatal(err)
	}
	res, err := ScenarioFromShadow(raw, lookupOrders, "")
	if err != nil {
		t.Fatalf("ScenarioFromShadow: %v", err)
	}
	if len(res.Warnings) != 0 {
		t.Errorf("a complete run warned: %v", res.Warnings)
	}

	// Round trip through the real loader and validator, from a file, as
	// `evalrun -check` reads it.
	path := filepath.Join(t.TempDir(), "shadow-room.yaml")
	if err := os.WriteFile(path, res.YAML, 0o600); err != nil {
		t.Fatal(err)
	}
	sc, err := LoadScenario(path)
	if err != nil {
		t.Fatalf("LoadScenario: %v", err)
	}
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateAll([]Scenario{sc}, defs); err != nil {
		t.Fatalf("ValidateAll: %v", err)
	}

	if sc.Name != "shadow-room_8f3a1c7e" {
		t.Errorf("name = %q", sc.Name)
	}
	if sc.Pipeline.ID != "orders" || sc.Pipeline.TopicPrefix != "pg" {
		t.Errorf("pipeline = %+v", sc.Pipeline)
	}
	if got := strings.Join(sc.Resources.Subjects, ","); got != "pg.public.orders-key,pg.public.orders-value" {
		t.Errorf("subjects = %s", got)
	}
	if sc.Alert.Title != "orders stale" || !strings.HasPrefix(sc.Alert.PipelineMap, "pipeline orders\n") {
		t.Errorf("alert = %+v", sc.Alert)
	}
	if sc.Expect.FirstFailingLayer != "kafka-connect" {
		t.Errorf("layer = %q", sc.Expect.FirstFailingLayer)
	}
	if len(sc.Expect.RootCause) != 1 || strings.Join(sc.Expect.RootCause[0], "|") != "amount|numeric|schema registry|409" {
		t.Errorf("root_cause = %v", sc.Expect.RootCause)
	}
	if sc.Expect.MaxToolCalls != 4+toolCallSlack {
		t.Errorf("max_tool_calls = %d", sc.Expect.MaxToolCalls)
	}
	if len(sc.Inputs) != 0 {
		t.Errorf("inputs = %v, a shadow run had none", sc.Inputs)
	}
	if !strings.Contains(sc.Description, "layer_correct=true") || !strings.Contains(sc.Description, "only the path this run took") {
		t.Errorf("description = %q", sc.Description)
	}

	// Recordings: exact summaries, why stripped, errors as errors, data kept.
	if len(sc.Recordings) != 4 {
		t.Fatalf("%d recordings, want 4", len(sc.Recordings))
	}
	byTool := map[string]Recording{}
	for _, r := range sc.Recordings {
		byTool[r.Tool] = r
	}
	cs := byTool["connect_status"]
	if len(cs.Input) != 1 || cs.Input["connector"] != "pg-orders-connector" {
		t.Errorf("connect_status input = %v (why must be stripped)", cs.Input)
	}
	want := evSummary(t, sampleDoc(t), "toolu_01")
	if cs.Summary != want {
		t.Errorf("connect_status summary changed in the round trip:\n got %q\nwant %q", cs.Summary, want)
	}
	if m, _ := cs.Data.(map[string]any); m == nil || m["state"] != "RUNNING" {
		t.Errorf("connect_status data = %#v", cs.Data)
	}
	if r := byTool["schema_registry_diff"]; r.Error == "" || r.Summary != "" {
		t.Errorf("failed call must become an error recording: %+v", r)
	}
	if r := byTool["pg_replication_slots"]; len(r.Input) != 0 {
		t.Errorf("empty input after stripping why = %v", r.Input)
	}
}

// evSummary reads the summary of one tool_result from the sample.
func evSummary(t *testing.T, m map[string]any, id string) string {
	t.Helper()
	for _, e := range evList(m) {
		ev := e.(map[string]any)
		p := sub(ev, "payload")
		if ev["type"] == "agent.tool_result" && p["tool_use_id"] == id {
			return p["summary"].(string)
		}
	}
	t.Fatalf("no tool_result %s", id)
	return ""
}

func TestScenarioFromShadowErrors(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(m map[string]any)
		raw    string // used instead of the sample when set
		want   error
		text   string // expected in the message
	}{
		{name: "unlabelled run", mutate: func(m map[string]any) { sub(m, "room")["labelled"] = nil },
			want: ErrShadowUnlabelled, text: "no ground truth"},
		{name: "label member missing", mutate: func(m map[string]any) { delete(sub(m, "room"), "labelled") },
			want: ErrShadowUnlabelled},
		{name: "label without terms", mutate: func(m map[string]any) { sub(sub(m, "room"), "labelled")["root_cause_terms"] = []any{"  ", ""} },
			want: ErrShadowUnlabelled, text: "root_cause_terms"},
		{name: "label without layer", mutate: func(m map[string]any) { sub(sub(m, "room"), "labelled")["actual_layer"] = "" },
			want: ErrShadowUnlabelled},
		{name: "unknown layer", mutate: func(m map[string]any) { sub(sub(m, "room"), "labelled")["actual_layer"] = "etl" },
			want: ErrShadowUnknownLayer, text: "etl"},
		{name: "layer in other case", mutate: func(m map[string]any) { sub(sub(m, "room"), "labelled")["actual_layer"] = "Kafka-Connect" },
			want: ErrShadowUnknownLayer},
		{name: "too many terms", mutate: func(m map[string]any) {
			var ts []any
			for i := 0; i < maxTerms+1; i++ {
				ts = append(ts, "term"+strings.Repeat("x", i))
			}
			sub(sub(m, "room"), "labelled")["root_cause_terms"] = ts
		}, want: ErrShadowMalformed},
		{name: "term too long", mutate: func(m map[string]any) {
			sub(sub(m, "room"), "labelled")["root_cause_terms"] = []any{strings.Repeat("a", maxTermLen+1)}
		}, want: ErrShadowMalformed},
		{name: "missing include=room", mutate: func(m map[string]any) { delete(m, "room") },
			want: ErrShadowNoRoom, text: "include=data,room"},
		{name: "missing include=data", mutate: func(m map[string]any) {
			for _, e := range evList(m) {
				delete(sub(e.(map[string]any), "payload"), "data")
			}
		}, want: ErrShadowNoData, text: "include=data,room"},
		{name: "run still running", mutate: func(m map[string]any) { sub(m, "run")["status"] = "running" },
			want: ErrShadowNotFinished},
		{name: "run queued", mutate: func(m map[string]any) { sub(m, "run")["status"] = "queued" },
			want: ErrShadowNotFinished},
		{name: "partial run with no completed call", mutate: func(m map[string]any) {
			sub(m, "run")["status"] = "interrupted"
			var keep []any
			for _, e := range evList(m) {
				if e.(map[string]any)["type"] != "agent.tool_result" {
					keep = append(keep, e)
				}
			}
			m["events"] = keep
		}, want: ErrShadowNothingToReplay},
		{name: "no events at all", mutate: func(m map[string]any) { m["events"] = []any{} },
			want: ErrShadowNothingToReplay},
		{name: "events missing", mutate: func(m map[string]any) { delete(m, "events") },
			want: ErrShadowMalformed},
		{name: "run missing", mutate: func(m map[string]any) { delete(m, "run") },
			want: ErrShadowMalformed},
		{name: "unknown status", mutate: func(m map[string]any) { sub(m, "run")["status"] = "exploded" },
			want: ErrShadowMalformed},
		{name: "created has no pipeline", mutate: func(m map[string]any) { sub(sub(m, "room"), "created")["pipeline_id"] = "" },
			want: ErrShadowMalformed},
		{name: "unknown pipeline", mutate: func(m map[string]any) { sub(sub(m, "room"), "created")["pipeline_id"] = "payments" },
			want: ErrShadowUnknownPipeline},
		{name: "tool_result for another tool", mutate: func(m map[string]any) {
			for _, e := range evList(m) {
				p := sub(e.(map[string]any), "payload")
				if p["tool_use_id"] == "toolu_01" && p["summary"] != nil {
					p["tool"] = "kafka_list_groups"
				}
			}
		}, want: ErrShadowMalformed},
		{name: "tool from a newer build", mutate: func(m map[string]any) {
			for _, e := range evList(m) {
				p := sub(e.(map[string]any), "payload")
				if p["tool_use_id"] == "toolu_01" {
					p["tool"] = "connect_teleport"
				}
			}
		}, want: ErrShadowInvalid, text: "connect_teleport"},
		{name: "input field the tool does not have", mutate: func(m map[string]any) {
			for _, e := range evList(m) {
				p := sub(e.(map[string]any), "payload")
				if p["tool_use_id"] == "toolu_01" && p["input"] != nil {
					sub(p, "input")["connector_name"] = "x"
				}
			}
		}, want: ErrShadowInvalid, text: "connector_name"},
		{name: "malformed JSON", raw: `{"run": {`, want: ErrShadowMalformed},
		{name: "not JSON at all", raw: `room_8f3a1c7e`, want: ErrShadowMalformed},
		{name: "empty file", raw: ``, want: ErrShadowMalformed},
		{name: "JSON array", raw: `[]`, want: ErrShadowMalformed},
		{name: "wrong field type", raw: `{"run": {"room_id": 7}, "events": []}`, want: ErrShadowMalformed},
		{name: "trailing document", raw: `{"run":{"room_id":"r"},"events":[]} {}`, want: ErrShadowMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(tc.raw)
			if tc.mutate != nil {
				m := sampleDoc(t)
				tc.mutate(m)
				raw = encode(t, m)
			}
			res, err := ScenarioFromShadow(raw, lookupOrders, "")
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if res.YAML != nil {
				t.Errorf("wrote a scenario despite the error")
			}
			if tc.text != "" && !strings.Contains(err.Error(), tc.text) {
				t.Errorf("message %q lacks %q", err, tc.text)
			}
		})
	}
}

func TestScenarioFromShadowBadName(t *testing.T) {
	raw, _ := os.ReadFile(shadowSample)
	if _, err := ScenarioFromShadow(raw, lookupOrders, "bad name\n- x"); err == nil {
		t.Fatal("accepted a name outside [A-Za-z0-9_-]")
	}
	res, err := ScenarioFromShadow(raw, lookupOrders, "mine")
	if err != nil || res.Scenario.Name != "mine" {
		t.Fatalf("name override: %v %q", err, res.Scenario.Name)
	}
}

// A run cut off before its last tool result still yields a scenario, with
// the label as ground truth, and says so.
func TestScenarioFromShadowPartial(t *testing.T) {
	for _, status := range []string{"interrupted", "budget", "error", "no_hypothesis"} {
		t.Run(status, func(t *testing.T) {
			m := sampleDoc(t)
			run := sub(m, "run")
			run["status"] = status
			run["final_hypothesis"] = ""
			// Drop the last result (toolu_04) and the hypothesis: cut off mid-call.
			var keep []any
			for _, e := range evList(m) {
				ev := e.(map[string]any)
				p := sub(ev, "payload")
				if ev["type"] == "agent.hypothesis" || (ev["type"] == "agent.tool_result" && p["tool_use_id"] == "toolu_04") {
					continue
				}
				keep = append(keep, e)
			}
			m["events"] = keep
			sub(m, "room")["graded"] = nil // never graded: still fine

			res, err := ScenarioFromShadow(encode(t, m), lookupOrders, "")
			if err != nil {
				t.Fatalf("partial run refused: %v", err)
			}
			if len(res.Scenario.Recordings) != 3 {
				t.Errorf("%d recordings, want the 3 completed calls", len(res.Scenario.Recordings))
			}
			if res.Scenario.Expect.MaxToolCalls != 4+toolCallSlack {
				t.Errorf("max_tool_calls = %d, the cut-off call still counts", res.Scenario.Expect.MaxToolCalls)
			}
			if !strings.Contains(res.Scenario.Description, "PARTIAL RUN") || !strings.Contains(res.Scenario.Description, "Not graded") {
				t.Errorf("description = %q", res.Scenario.Description)
			}
			joined := strings.Join(res.Warnings, "\n")
			if !strings.Contains(joined, "no result") || !strings.Contains(joined, "partial run (status "+status+")") {
				t.Errorf("warnings = %q", joined)
			}
		})
	}
}

// Everything in a shadow response is agent or tool output. Whatever it
// contains must come back as the same string, never as YAML structure.
func TestScenarioFromShadowHostileText(t *testing.T) {
	hostile := []string{
		"line one\n---\nname: pwned\n...",
		"!!python/object/apply:os.system ['id']",
		"*alias &anchor <<: {a: b}",
		"key: value # comment\n\ttabbed\r\ncrlf",
		"  leading and trailing spaces  \n\n\n",
		"quote ' and \" and \\ and ${HOME} and `id` and $(id)",
		"unicode   é \U0001F600 ‮ rtl",
		"- list item\n- another",
		"null", "true", "123", "~", "0x1F", "2026-10-11",
	}
	for _, h := range hostile {
		t.Run(h[:min(len(h), 12)], func(t *testing.T) {
			m := sampleDoc(t)
			for _, e := range evList(m) {
				ev := e.(map[string]any)
				p := sub(ev, "payload")
				if ev["type"] == "agent.tool_result" && p["tool_use_id"] == "toolu_01" {
					p["summary"] = h
					p["data"] = map[string]any{"state": h, "list": []any{h, 12345678901234567, 1.5}}
				}
			}
			sub(sub(m, "room"), "created")["title"] = h + "x"
			sub(m, "run")["pipeline_map"] = h
			sub(sub(m, "room"), "labelled")["root_cause_terms"] = []any{"amount", h + "t"}
			raw := encode(t, m)
			res, err := ScenarioFromShadow(raw, lookupOrders, "")
			if err != nil {
				if strings.TrimSpace(h) == "" {
					return
				}
				t.Fatal(err)
			}
			sc := res.Scenario
			var cs Recording
			for _, r := range sc.Recordings {
				if r.Tool == "connect_status" {
					cs = r
				}
			}
			if cs.Summary != h {
				t.Errorf("summary %q came back as %q", h, cs.Summary)
			}
			data := cs.Data.(map[string]any)
			if data["state"] != h {
				t.Errorf("data.state %q came back as %#v", h, data["state"])
			}
			list := data["list"].([]any)
			if list[1] != 12345678901234567 { // an offset must stay an integer
				t.Errorf("large integer came back as %#v", list[1])
			}
			if sc.Alert.Title != h+"x" || sc.Alert.PipelineMap != h {
				t.Errorf("alert text changed: %q / %q", sc.Alert.Title, sc.Alert.PipelineMap)
			}
			if sc.Name != "shadow-room_8f3a1c7e" || sc.Pipeline.ID != "orders" || len(sc.Recordings) != 4 {
				t.Errorf("hostile text changed the structure: name=%q pipeline=%q recordings=%d", sc.Name, sc.Pipeline.ID, len(sc.Recordings))
			}
		})
	}
}

func TestScenarioFromShadowCaps(t *testing.T) {
	m := sampleDoc(t)
	for _, e := range evList(m) {
		ev := e.(map[string]any)
		p := sub(ev, "payload")
		if ev["type"] != "agent.tool_result" {
			continue
		}
		switch p["tool_use_id"] {
		case "toolu_01":
			p["summary"] = strings.Repeat("é", 20000) // over MaxSummaryBytes
			p["data"] = map[string]any{"blob": strings.Repeat("x", maxRecordingData)}
		case "toolu_03":
			p["data"] = map[string]any{"ok": true}
		}
	}
	sub(m, "run")["pipeline_map"] = strings.Repeat("m", maxPipelineMapBytes*2)
	res, err := ScenarioFromShadow(encode(t, m), lookupOrders, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Scenario.Alert.PipelineMap) != maxPipelineMapBytes {
		t.Errorf("pipeline_map = %d bytes", len(res.Scenario.Alert.PipelineMap))
	}
	for _, r := range res.Scenario.Recordings {
		if r.Tool != "connect_status" {
			continue
		}
		if len(r.Summary) > 8000 || !r.Truncated {
			t.Errorf("summary %d bytes, truncated=%v", len(r.Summary), r.Truncated)
		}
		if r.Data != nil {
			t.Errorf("oversized data kept")
		}
	}
	if !strings.Contains(strings.Join(res.Warnings, "\n"), "data dropped") {
		t.Errorf("warnings = %v", res.Warnings)
	}

	// Input over the cap is refused before any parsing.
	if _, err := ScenarioFromShadow(make([]byte, MaxShadowInputBytes+1), lookupOrders, ""); !errors.Is(err, ErrShadowMalformed) {
		t.Errorf("oversized input: %v", err)
	}
}

// The room parts may be the whole event; repeated identical calls fold into
// the first result; runbook_lookup is left to the live tool.
func TestScenarioFromShadowShapes(t *testing.T) {
	m := sampleDoc(t)
	room := sub(m, "room")
	room["created"] = map[string]any{"type": "room.created", "payload": room["created"]}
	room["labelled"] = map[string]any{"type": "shadow.labelled", "payload": room["labelled"]}
	room["graded"] = map[string]any{"type": "shadow.graded", "payload": room["graded"]}

	evs := evList(m)
	n := len(evs)
	dupCall := map[string]any{"room_id": "r", "seq": n + 1, "type": "agent.tool_call", "shadow": true,
		"payload": map[string]any{"step_id": "s3", "tool_use_id": "toolu_09", "tool": "connect_status",
			"input": map[string]any{"connector": "pg-orders-connector", "why": "again"}}}
	dupRes := map[string]any{"room_id": "r", "seq": n + 2, "type": "agent.tool_result", "shadow": true,
		"payload": map[string]any{"step_id": "s3", "tool_use_id": "toolu_09", "tool": "connect_status", "summary": "different", "is_error": false}}
	rbCall := map[string]any{"room_id": "r", "seq": n + 3, "type": "agent.tool_call", "shadow": true,
		"payload": map[string]any{"step_id": "s3", "tool_use_id": "toolu_10", "tool": "runbook_lookup", "input": map[string]any{"query": "x"}}}
	rbRes := map[string]any{"room_id": "r", "seq": n + 4, "type": "agent.tool_result", "shadow": true,
		"payload": map[string]any{"step_id": "s3", "tool_use_id": "toolu_10", "tool": "runbook_lookup", "summary": "text", "is_error": false}}
	m["events"] = append(evs, dupCall, dupRes, rbCall, rbRes)

	res, err := ScenarioFromShadow(encode(t, m), lookupOrders, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Scenario.Recordings) != 4 {
		t.Errorf("%d recordings, want 4 (duplicate and runbook_lookup dropped)", len(res.Scenario.Recordings))
	}
	for _, r := range res.Scenario.Recordings {
		if r.Summary == "different" || r.Tool == "runbook_lookup" {
			t.Errorf("recording %+v should have been dropped", r)
		}
	}
	if res.Scenario.Expect.MaxToolCalls != 6+toolCallSlack {
		t.Errorf("max_tool_calls = %d, want observed calls (6) + slack", res.Scenario.Expect.MaxToolCalls)
	}
	if !strings.Contains(res.Scenario.Description, "layer_correct=true") {
		t.Errorf("grade lost: %q", res.Scenario.Description)
	}
	// An Event of the wrong type in a room part is refused.
	room["labelled"] = map[string]any{"type": "shadow.graded", "payload": map[string]any{}}
	if _, err := ScenarioFromShadow(encode(t, m), lookupOrders, ""); !errors.Is(err, ErrShadowMalformed) {
		t.Errorf("wrong event type: %v", err)
	}
}
