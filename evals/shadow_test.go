package evals

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
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
	// The only warning on a complete run is the one about production values in data.
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "production values") {
		t.Errorf("warnings = %q, want the data review warning only", res.Warnings)
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
	// The agent loop's "error: " prefix is stripped once; replay adds its own.
	if r := byTool["schema_registry_diff"]; r.Summary != "" || !strings.HasPrefix(r.Error, "schemareg: GET ") {
		t.Errorf("failed call must become an error recording without the error: prefix: %+v", r)
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
		{name: "label without terms", mutate: func(m map[string]any) { part(m, "labelled")["root_cause_terms"] = []any{"  ", ""} },
			want: ErrShadowUnlabelled, text: "root_cause_terms"},
		{name: "label without layer", mutate: func(m map[string]any) { part(m, "labelled")["actual_layer"] = "" },
			want: ErrShadowUnlabelled},
		{name: "unknown layer", mutate: func(m map[string]any) { part(m, "labelled")["actual_layer"] = "etl" },
			want: ErrShadowUnknownLayer, text: "etl"},
		{name: "layer in other case", mutate: func(m map[string]any) { part(m, "labelled")["actual_layer"] = "Kafka-Connect" },
			want: ErrShadowUnknownLayer},
		{name: "too many terms", mutate: func(m map[string]any) {
			var ts []any
			for i := 0; i < maxTerms+1; i++ {
				ts = append(ts, "term"+strings.Repeat("x", i))
			}
			part(m, "labelled")["root_cause_terms"] = ts
		}, want: ErrShadowMalformed},
		{name: "term too long", mutate: func(m map[string]any) {
			part(m, "labelled")["root_cause_terms"] = []any{strings.Repeat("a", maxTermLen+1)}
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
		{name: "created has no pipeline", mutate: func(m map[string]any) { part(m, "created")["pipeline_id"] = "" },
			want: ErrShadowMalformed},
		{name: "unknown pipeline", mutate: func(m map[string]any) { part(m, "created")["pipeline_id"] = "payments" },
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
			run["final_hypothesis"] = nil
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
			part(m, "created")["title"] = h + "x"
			sub(m, "run")["pipeline_map"] = h
			part(m, "labelled")["root_cause_terms"] = []any{"amount", h + "t"}
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

}

// Valid JSON padded past the byte cap: only the cap itself can refuse it.
func TestScenarioFromShadowInputCap(t *testing.T) {
	m := sampleDoc(t)
	m["pad"] = strings.Repeat("x", MaxShadowInputBytes)
	big := encode(t, m)
	if !json.Valid(big) || len(big) <= MaxShadowInputBytes {
		t.Fatalf("test input is not valid JSON over the cap (%d bytes)", len(big))
	}
	_, err := ScenarioFromShadow(big, lookupOrders, "")
	if !errors.Is(err, ErrShadowMalformed) || !strings.Contains(err.Error(), "exceeds the") {
		t.Fatalf("oversized input: %v", err)
	}
	// The same document under the cap loads, so the cap was what refused it.
	delete(m, "pad")
	if _, err := ScenarioFromShadow(encode(t, m), lookupOrders, ""); err != nil {
		t.Fatalf("unpadded: %v", err)
	}
}

// part returns the payload of room.<key>, which is a whole event.
func part(m map[string]any, key string) map[string]any {
	p := sub(m, "room")[key].(map[string]any)
	if pl, ok := p["payload"].(map[string]any); ok {
		return pl
	}
	return p
}

// The agent loop answers a call to an unknown tool, or with input the schema
// does not allow, with an error result without running anything. Those are
// skipped (and still count as calls); the same shape with a successful
// result is a hard error.
func TestScenarioFromShadowRejectedCalls(t *testing.T) {
	m := sampleDoc(t)
	evs := evList(m)
	n := len(evs)
	add := func(i int, tool string, input map[string]any, summary string) {
		id := "toolu_x" + string(rune('a'+i))
		evs = append(evs,
			map[string]any{"room_id": "r", "seq": n + 2*i + 1, "type": "agent.tool_call", "shadow": true,
				"payload": map[string]any{"step_id": "s3", "tool_use_id": id, "tool": tool, "input": input}},
			map[string]any{"room_id": "r", "seq": n + 2*i + 2, "type": "agent.tool_result", "shadow": true,
				"payload": map[string]any{"step_id": "s3", "tool_use_id": id, "tool": tool, "summary": summary, "is_error": true}})
	}
	add(0, "connect_teleport", map[string]any{"x": 1}, "error: unknown tool")
	add(1, "connect_status", map[string]any{"connector_name": "c", "why": "w"}, "error: invalid input: unknown property connector_name")
	m["events"] = evs
	res, err := ScenarioFromShadow(encode(t, m), lookupOrders, "")
	if err != nil {
		t.Fatalf("rejected calls killed the conversion: %v", err)
	}
	if len(res.Scenario.Recordings) != 4 {
		t.Errorf("%d recordings, want the 4 real ones", len(res.Scenario.Recordings))
	}
	if res.Scenario.Expect.MaxToolCalls != 6+toolCallSlack {
		t.Errorf("max_tool_calls = %d, rejected calls still count", res.Scenario.Expect.MaxToolCalls)
	}
	if !strings.Contains(strings.Join(res.Warnings, "\n"), "2 call(s) the agent loop rejected") {
		t.Errorf("warnings = %q", res.Warnings)
	}
}

// yaml.v3 writes a "<<" key unquoted, and the loader reads it as a merge key:
// data {"<<": {"injected": 1}, "a": 2} would come back with "injected" at the
// top. Data that does not survive is dropped; input that does not survive
// refuses the run.
func TestScenarioFromShadowMergeKey(t *testing.T) {
	cases := map[string]any{
		"top level": map[string]any{"<<": map[string]any{"injected": 1}, "a": 2},
		"nested":    map[string]any{"a": map[string]any{"b": []any{map[string]any{"<<": map[string]any{"injected": 1}}}}},
		"scalar":    map[string]any{"<<": "x"},
	}
	for name, data := range cases {
		t.Run("data "+name, func(t *testing.T) {
			m := sampleDoc(t)
			for _, e := range evList(m) {
				p := sub(e.(map[string]any), "payload")
				if p["tool_use_id"] == "toolu_01" && p["summary"] != nil {
					p["data"] = data
				}
			}
			res, err := ScenarioFromShadow(encode(t, m), lookupOrders, "")
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range res.Scenario.Recordings {
				if r.Tool == "connect_status" {
					if r.Data != nil {
						t.Errorf("data that reads back differently was kept: %#v", r.Data)
					}
					if r.Summary == "" {
						t.Error("summary lost with the data")
					}
				}
			}
			if !strings.Contains(strings.Join(res.Warnings, "\n"), `"<<" key`) {
				t.Errorf("no warning: %q", res.Warnings)
			}
		})
	}
	t.Run("input", func(t *testing.T) {
		m := sampleDoc(t)
		for _, e := range evList(m) {
			p := sub(e.(map[string]any), "payload")
			if p["tool_use_id"] == "toolu_01" && p["input"] != nil {
				p["input"] = map[string]any{"connector": map[string]any{"<<": map[string]any{"injected": 1}, "a": 2}}
			}
		}
		if _, err := ScenarioFromShadow(encode(t, m), lookupOrders, ""); !errors.Is(err, ErrShadowInvalid) {
			t.Fatalf("err = %v, want ErrShadowInvalid", err)
		}
	})
}

// A top-level free-choice input field is listed so the reviewer can drop it.
func TestScenarioFromShadowFreeChoiceWarning(t *testing.T) {
	m := sampleDoc(t)
	evs := evList(m)
	n := len(evs)
	m["events"] = append(evs,
		map[string]any{"room_id": "r", "seq": n + 1, "type": "agent.tool_call", "shadow": true,
			"payload": map[string]any{"step_id": "s3", "tool_use_id": "toolu_ddl", "tool": "pg_recent_ddl",
				"input": map[string]any{"since": "2026-10-11T09:00:00Z", "table": "public.orders", "why": "w"}}},
		map[string]any{"room_id": "r", "seq": n + 2, "type": "agent.tool_result", "shadow": true,
			"payload": map[string]any{"step_id": "s3", "tool_use_id": "toolu_ddl", "tool": "pg_recent_ddl",
				"summary": "ALTER TABLE public.orders\n", "is_error": false, "data": map[string]any{"n": 1}}})
	res, err := ScenarioFromShadow(encode(t, m), lookupOrders, "")
	if err != nil {
		t.Fatal(err)
	}
	var got string
	for _, w := range res.Warnings {
		if strings.Contains(w, "free choices") {
			got = w
		}
	}
	if !strings.Contains(got, "(pg_recent_ddl)") || !strings.Contains(got, "since") || strings.Contains(got, "table") {
		t.Errorf("free-choice warning = %q (warnings %q)", got, res.Warnings)
	}
	if w := freeChoiceWarnings([]outRecord{{Tool: "t", Input: map[string]any{"topic": "x", "limit": 5, "since": "y"}}}); len(w) != 1 ||
		!strings.Contains(w[0], "limit, since") {
		t.Errorf("warnings = %q", w)
	}
}

// The sample must be what the server sends: strict-decode it into the
// server's own type, so a renamed or retyped field fails here.
func TestShadowSampleMatchesServerType(t *testing.T) {
	raw, err := os.ReadFile(shadowSample)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var v room.ShadowView
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("sample is not a room.ShadowView: %v", err)
	}
	if v.Room == nil || v.Room.Created == nil || v.Room.Labelled == nil || v.Room.Graded == nil {
		t.Fatalf("room parts: %+v", v.Room)
	}
	var h events.AgentHypothesisPayload
	if err := json.Unmarshal(v.Run.FinalHypothesis, &h); err != nil || h.Text == "" {
		t.Fatalf("final_hypothesis is not a hypothesis payload: %v", err)
	}
}

// The room parts may be the whole event; repeated identical calls fold into
// the first result; runbook_lookup is left to the live tool.
func TestScenarioFromShadowShapes(t *testing.T) {
	m := sampleDoc(t)
	room := sub(m, "room")
	// The sample has whole events (the real shape); a bare payload also loads.
	for _, k := range []string{"created", "labelled", "graded"} {
		room[k] = room[k].(map[string]any)["payload"]
	}

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
