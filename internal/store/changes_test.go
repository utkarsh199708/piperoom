package store_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/store"
)

func baseChange() clients.PipelineChange {
	return clients.PipelineChange{
		PipelineID: "orders", Kind: clients.ChangeDDL, Object: "public.orders",
		At: time.Date(2026, 10, 14, 3, 4, 11, 123456789, time.FixedZone("x", 3600)), Source: "ddl_audit", Fingerprint: "fp1",
		Summary: "ALTER TABLE public.orders\nADD COLUMN x int\x00",
	}
}

func TestNormalizeChange(t *testing.T) {
	c, err := store.NormalizeChange(baseChange())
	if err != nil {
		t.Fatal(err)
	}
	if c.At.Location() != time.UTC || c.At.Nanosecond() != 123456000 {
		t.Errorf("At = %v, want UTC cut to microseconds", c.At)
	}
	if c.Summary != "ALTER TABLE public.orders ADD COLUMN x int" {
		t.Errorf("Summary = %q", c.Summary)
	}

	long := baseChange()
	long.Summary = strings.Repeat("é", 400)
	long.Before = json.RawMessage(`{"k":"` + strings.Repeat("a", store.MaxChangeJSONBytes) + `"}`)
	long.After = json.RawMessage(`not json`)
	c, err = store.NormalizeChange(long)
	if err != nil {
		t.Fatal(err)
	}
	if n := len([]rune(c.Summary)); n != store.MaxChangeSummaryRunes {
		t.Errorf("summary runes = %d", n)
	}
	if string(c.Before) != `{"truncated":true}` || string(c.After) != `{"truncated":true}` {
		t.Errorf("before/after = %s / %s", c.Before, c.After)
	}

	for name, mut := range map[string]func(*clients.PipelineChange){
		"kind":        func(c *clients.PipelineChange) { c.Kind = "topic_config" },
		"pipeline":    func(c *clients.PipelineChange) { c.PipelineID = "" },
		"object":      func(c *clients.PipelineChange) { c.Object = "" },
		"source":      func(c *clients.PipelineChange) { c.Source = "" },
		"fingerprint": func(c *clients.PipelineChange) { c.Fingerprint = "" },
		"at":          func(c *clients.PipelineChange) { c.At = time.Time{} },
	} {
		bad := baseChange()
		mut(&bad)
		if _, err := store.NormalizeChange(bad); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}
