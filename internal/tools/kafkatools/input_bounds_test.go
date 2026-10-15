package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

func TestFindBounds(t *testing.T) {
	n := func(v int) *int { return &v }
	epoch := time.Unix(0, 0).UTC()
	when := time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		maxScan   *int
		since     time.Time
		wantScan  int
		wantSince time.Time
		bad       bool
	}{
		{"absent is the default", nil, when, findDefaultMaxScan, when, false},
		{"one", n(1), when, 1, when, false},
		{"at the cap", n(findMaxScan), when, findMaxScan, when, false},
		{"over the cap is cut", n(findMaxScan + 1), when, findMaxScan, when, false},
		{"max int is cut", n(int(^uint(0) >> 1)), when, findMaxScan, when, false},
		{"zero is bad input", n(0), when, 0, when, true},
		{"negative is bad input", n(-5), when, 0, when, true},
		{"since at the epoch stays", nil, epoch, findDefaultMaxScan, epoch, false},
		{"since -1 ms ('latest' to Kafka) becomes the epoch", nil, epoch.Add(-time.Millisecond), findDefaultMaxScan, epoch, false},
		{"since year 1 becomes the epoch", nil, time.Time{}, findDefaultMaxScan, epoch, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan, since, err := findBounds(tt.maxScan, tt.since)
			if tt.bad {
				if !errors.Is(err, tools.ErrBadInput) {
					t.Fatalf("err = %v, want ErrBadInput", err)
				}
				return
			}
			if err != nil || scan != tt.wantScan || !since.Equal(tt.wantSince) {
				t.Fatalf("findBounds = %d, %s, %v; want %d, %s", scan, since, err, tt.wantScan, tt.wantSince)
			}
		})
	}
}

// TestFindByKeyClampsAtToolBoundary checks what reaches the client for the
// raw JSON the model can send.
func TestFindByKeyClampsAtToolBoundary(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantScan  int
		wantSince time.Time
		bad       bool
	}{
		{"default", `{"topic":"t","key":"k","since":"2026-10-14T03:00:00Z"}`, 5000, time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC), false},
		{"in range", `{"topic":"t","key":"k","since":"2026-10-14T03:00:00Z","max_scan":250}`, 250, time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC), false},
		{"huge", `{"topic":"t","key":"k","since":"2026-10-14T03:00:00Z","max_scan":9223372036854775807}`, findMaxScan, time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC), false},
		{"before the epoch", `{"topic":"t","key":"k","since":"1969-12-31T23:59:59.999Z"}`, 5000, time.Unix(0, 0), false},
		{"zero", `{"topic":"t","key":"k","since":"2026-10-14T03:00:00Z","max_scan":0}`, 0, time.Time{}, true},
		{"negative", `{"topic":"t","key":"k","since":"2026-10-14T03:00:00Z","max_scan":-1}`, 0, time.Time{}, true},
		{"fraction", `{"topic":"t","key":"k","since":"2026-10-14T03:00:00Z","max_scan":1.5}`, 0, time.Time{}, true},
		{"past int64", `{"topic":"t","key":"k","since":"2026-10-14T03:00:00Z","max_scan":1e100}`, 0, time.Time{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			var gotScan int
			var gotSince time.Time
			fk := &fakeKafka{findFn: func(_ context.Context, _, _ string, since time.Time, maxScan int) ([]clients.RecordRef, error) {
				called, gotScan, gotSince = true, maxScan, since
				return nil, nil
			}}
			reg := tools.NewRegistry()
			if err := Register(reg, Deps{Kafka: fk}); err != nil {
				t.Fatal(err)
			}
			tool, _ := reg.Get("kafka_find_by_key")
			_, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(tt.input))
			if tt.bad {
				if !errors.Is(err, tools.ErrBadInput) || called {
					t.Fatalf("err = %v, client called = %v; want ErrBadInput before any call", err, called)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if gotScan != tt.wantScan || !gotSince.Equal(tt.wantSince) {
				t.Fatalf("client got max_scan %d since %s, want %d since %s", gotScan, gotSince, tt.wantScan, tt.wantSince)
			}
		})
	}
}

// TestSchemasHaveNoRangeKeywords: strict tool use rejects numeric and length
// bounds with a 400, so every range lives in the description and in Run.
func TestSchemasHaveNoRangeKeywords(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{}); err != nil {
		t.Fatal(err)
	}
	for _, tl := range reg.All() {
		props, _ := tl.InputSchema()["properties"].(map[string]any)
		for name, p := range props {
			prop, _ := p.(map[string]any)
			for _, kw := range []string{"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "minLength", "maxLength"} {
				if _, ok := prop[kw]; ok {
					t.Errorf("%s.%s uses %s", tl.Name(), name, kw)
				}
			}
		}
	}
}

func TestSchemaRegistryDiffVersionBounds(t *testing.T) {
	tests := []struct {
		name  string
		input string
		bad   bool
	}{
		{"largest version", `{"subject":"s","from_version":2147483647,"to_version":2147483647}`, false},
		{"from_version past int32", `{"subject":"s","from_version":2147483648}`, true},
		{"to_version past int32", `{"subject":"s","from_version":1,"to_version":9223372036854775807}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			fr := &fakeRegistry{schemaFn: func(_ context.Context, s string, v int) (clients.SchemaVersion, error) {
				called = true
				return clients.SchemaVersion{Subject: s, Version: v, Schema: `{"type":"record","name":"V","fields":[]}`}, nil
			}}
			reg := tools.NewRegistry()
			if err := Register(reg, Deps{Registry: fr}); err != nil {
				t.Fatal(err)
			}
			tool, _ := reg.Get("schema_registry_diff")
			_, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(tt.input))
			if tt.bad {
				if !errors.Is(err, tools.ErrBadInput) || called {
					t.Fatalf("err = %v, registry called = %v; want ErrBadInput before any call", err, called)
				}
				return
			}
			if err != nil || !called {
				t.Fatalf("err = %v, registry called = %v", err, called)
			}
		})
	}
}
