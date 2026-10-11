package evals

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/tools"
)

func TestReplayLookup(t *testing.T) {
	recs := []Recording{
		{Tool: "kafka_find_by_key", Summary: "wildcard"},
		{Tool: "kafka_find_by_key", Input: map[string]any{"topic": "t"}, Summary: "topic"},
		{Tool: "kafka_find_by_key", Input: map[string]any{"topic": "t", "key": "42"}, Summary: "topic+key"},
		{Tool: "kafka_find_by_key", Input: map[string]any{"topic": "t", "key": "42"}, Summary: "later duplicate"},
		{Tool: "schema_registry_diff", Input: map[string]any{"subject": "s", "to_version": 2}, Summary: "v2"},
		{Tool: "connect_status", Input: map[string]any{"connector": "c"}, Summary: "c"},
	}
	rp, err := newReplayer(recs)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, tool, input string
		want              string // "" = miss
	}{
		{"most specific wins", "kafka_find_by_key", `{"topic":"t","key":"42","since":"2026-10-11T10:00:00Z"}`, "topic+key"},
		{"extra call fields ignored", "kafka_find_by_key", `{"topic":"t","key":"7","since":"x"}`, "topic"},
		{"wildcard fallback", "kafka_find_by_key", `{"topic":"u","key":"7","since":"x"}`, "wildcard"},
		{"yaml int equals json number", "schema_registry_diff", `{"subject":"s","to_version":2}`, "v2"},
		{"value differs", "schema_registry_diff", `{"subject":"s","to_version":3}`, ""},
		{"field absent", "schema_registry_diff", `{"subject":"s"}`, ""},
		{"no wildcard means miss", "connect_status", `{"connector":"other"}`, ""},
		{"unknown tool", "pg_row", `{}`, ""},
		{"bad json", "connect_status", `{`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, ok := rp.lookup(tt.tool, json.RawMessage(tt.input))
			if tt.want == "" {
				if ok {
					t.Errorf("got %q, want miss", rec.Summary)
				}
				return
			}
			if !ok || rec.Summary != tt.want {
				t.Errorf("got %q (ok %v), want %q", rec.Summary, ok, tt.want)
			}
		})
	}
}

func TestReplayToolRun(t *testing.T) {
	rp, err := newReplayer([]Recording{
		{Tool: "connect_status", Input: map[string]any{"connector": "c"}, Summary: "ok", Data: map[string]any{"tasks": []any{map[string]any{"id": 0}}}},
		{Tool: "connect_status", Input: map[string]any{"connector": "down"}, Error: "connect: 503"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	reg, err := rp.registry(defs)
	if err != nil {
		t.Fatal(err)
	}
	if len(reg.All()) != len(defs) {
		t.Fatalf("registry has %d tools, want %d", len(reg.All()), len(defs))
	}
	tool, _ := reg.Get("connect_status")
	var live tools.Tool
	for _, d := range defs {
		if d.Name() == "connect_status" {
			live = d
		}
	}
	if tool.Description() != live.Description() || tool.Kind() != live.Kind() {
		t.Error("replay tool does not present the live definition")
	}

	res, err := tool.Run(context.Background(), json.RawMessage(`{"connector":"c"}`))
	if err != nil || res.Summary != "ok" {
		t.Fatalf("run = %+v, %v", res, err)
	}
	if b, _ := json.Marshal(res.Data); string(b) != `{"tasks":[{"id":0}]}` {
		t.Errorf("data = %s", b)
	}
	if _, err := tool.Run(context.Background(), json.RawMessage(`{"connector":"down"}`)); err == nil || err.Error() != "connect: 503" {
		t.Errorf("error recording: %v", err)
	}
	if _, err := tool.Run(context.Background(), json.RawMessage(`{"connector":"x"}`)); !errors.Is(err, errNoRecording) {
		t.Errorf("miss: %v", err)
	}
	if m := rp.Misses(); len(m) != 1 || m[0].Tool != "connect_status" || string(m[0].Input) != `{"connector":"x"}` {
		t.Errorf("misses = %+v", m)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tool.Run(ctx, json.RawMessage(`{"connector":"c"}`)); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled ctx: %v", err)
	}
}

func TestReplayRejectsUnmarshalableData(t *testing.T) {
	if _, err := newReplayer([]Recording{{Tool: "x", Summary: "s", Data: map[string]any{"f": func() {}}}}); err == nil {
		t.Fatal("want error for data that cannot become JSON")
	}
}

func TestLiveActionsValidateButNeverWrite(t *testing.T) {
	reg, err := LiveActions(Resources{Connectors: []string{"pg-orders-connector"}, Subjects: []string{"pg.public.orders-value"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	restart, _ := reg.Get("connect_restart_task")
	if err := restart.Validate(json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)); err != nil {
		t.Errorf("valid params refused: %v", err)
	}
	if err := restart.Validate(json.RawMessage(`{"connector":"nope","task":0}`)); err == nil {
		t.Error("unknown connector accepted")
	}
	if _, err := restart.Execute(ctx, json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)); err == nil {
		t.Error("restart executed in replay")
	}
	compat, _ := reg.Get("schema_set_compatibility")
	if err := compat.Validate(json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`)); err != nil {
		t.Errorf("valid params refused: %v", err)
	}
	if _, err := compat.Execute(ctx, json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`)); err == nil {
		t.Error("set compatibility executed in replay")
	}
}
