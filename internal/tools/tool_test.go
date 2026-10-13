package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"piperoom/internal/owners"
)

// stubTool is the smallest Tool; only Name matters to the registry.
type stubTool struct{ name string }

func (s stubTool) Name() string              { return s.name }
func (stubTool) Description() string         { return "stub" }
func (stubTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (stubTool) Kind() Kind                  { return KindRead }
func (stubTool) Run(context.Context, json.RawMessage) (Result, error) {
	return Result{}, nil
}

func TestTruncate(t *testing.T) {
	const marker = "\n…[truncated]"
	tests := []struct {
		name        string
		in          string
		wantTrunc   bool
		wantLenMax  int  // upper bound on the output length
		wantSameOut bool // output must equal input
	}{
		{name: "empty", in: "", wantSameOut: true},
		{name: "short", in: "schema changed", wantSameOut: true},
		{name: "exactly at the limit", in: strings.Repeat("a", MaxSummaryBytes), wantSameOut: true},
		{name: "one byte over", in: strings.Repeat("a", MaxSummaryBytes+1), wantTrunc: true, wantLenMax: MaxSummaryBytes},
		{name: "far over", in: strings.Repeat("x", 3*MaxSummaryBytes), wantTrunc: true, wantLenMax: MaxSummaryBytes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, cut := Truncate(tt.in)
			if cut != tt.wantTrunc {
				t.Fatalf("truncated = %v, want %v", cut, tt.wantTrunc)
			}
			if tt.wantSameOut {
				if got != tt.in {
					t.Fatalf("output changed for input within the limit")
				}
				return
			}
			if len(got) > tt.wantLenMax {
				t.Fatalf("len = %d, want <= %d", len(got), tt.wantLenMax)
			}
			if !strings.HasSuffix(got, marker) {
				t.Fatalf("output does not end with the truncation marker: %q", got[len(got)-min(len(got), 30):])
			}
		})
	}
}

func TestTruncateNeverSplitsARune(t *testing.T) {
	// The cut point is MaxSummaryBytes-15 (the marker length). Place a
	// three-byte rune so that the cut lands on each of its continuation bytes.
	const euro = "€"
	cut := MaxSummaryBytes - len("\n…[truncated]")
	for offset := 0; offset < 3; offset++ {
		// Fill so the euro sign starts at cut-offset.
		prefix := strings.Repeat("a", cut-offset)
		s := prefix + strings.Repeat(euro, 2000)
		got, trunc := Truncate(s)
		if !trunc {
			t.Fatalf("offset %d: not truncated", offset)
		}
		if !utf8.ValidString(got) {
			t.Fatalf("offset %d: output is not valid UTF-8", offset)
		}
		if len(got) > MaxSummaryBytes {
			t.Fatalf("offset %d: len %d exceeds MaxSummaryBytes", offset, len(got))
		}
	}
}

func TestTruncateKeepsWholeRuneAtExactCut(t *testing.T) {
	// When the cut falls on a rune boundary nothing is backed off, so the
	// output is exactly the limit long.
	cut := MaxSummaryBytes - len("\n…[truncated]")
	got, trunc := Truncate(strings.Repeat("a", cut) + strings.Repeat("€", 10))
	if !trunc {
		t.Fatal("want truncation")
	}
	if len(got) != MaxSummaryBytes {
		t.Fatalf("len = %d, want exactly %d", len(got), MaxSummaryBytes)
	}
}

func TestRegistryRegisterAndGet(t *testing.T) {
	tests := []struct {
		name    string
		tools   []Tool
		wantErr string // substring of the error, empty for success
	}{
		{name: "single tool", tools: []Tool{stubTool{"connect_status"}}},
		{name: "two distinct tools", tools: []Tool{stubTool{"a"}, stubTool{"b"}}},
		{name: "empty name refused", tools: []Tool{stubTool{""}}, wantErr: "empty tool name"},
		{name: "duplicate refused", tools: []Tool{stubTool{"a"}, stubTool{"a"}}, wantErr: `duplicate tool "a"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry()
			var err error
			for _, tl := range tt.tools {
				if err = r.Register(tl); err != nil {
					break
				}
			}
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Register: unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Register error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestRegistryRejectedRegisterLeavesRegistryUnchanged(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(stubTool{"a"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(stubTool{"a"}); err == nil {
		t.Fatal("duplicate registered")
	}
	if err := r.Register(stubTool{""}); err == nil {
		t.Fatal("empty name registered")
	}
	all := r.All()
	if len(all) != 1 || all[0].Name() != "a" {
		t.Fatalf("registry changed after refused registrations: %v", names(all))
	}
}

func TestRegistryGet(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(stubTool{"connect_status"}); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		lookup string
		found  bool
	}{
		{name: "registered", lookup: "connect_status", found: true},
		{name: "unknown", lookup: "consumer_lag"},
		{name: "case sensitive", lookup: "Connect_Status"},
		{name: "empty", lookup: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tl, ok := r.Get(tt.lookup)
			if ok != tt.found {
				t.Fatalf("Get(%q) found = %v, want %v", tt.lookup, ok, tt.found)
			}
			if ok && tl.Name() != tt.lookup {
				t.Fatalf("Get(%q) returned %q", tt.lookup, tl.Name())
			}
		})
	}
}

func TestRegistryAllKeepsRegistrationOrder(t *testing.T) {
	// The tool list goes to the model in this order; a change breaks the
	// prompt cache, so the order must be exactly the registration order.
	want := []string{"zeta", "alpha", "mid", "beta"}
	r := NewRegistry()
	for _, n := range want {
		if err := r.Register(stubTool{n}); err != nil {
			t.Fatal(err)
		}
	}
	if got := names(r.All()); !slices.Equal(got, want) {
		t.Fatalf("All() = %v, want %v", got, want)
	}
}

func TestRegistryAllReturnsACopy(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(stubTool{"a"}); err != nil {
		t.Fatal(err)
	}
	all := r.All()
	all[0] = stubTool{"hijacked"}
	if got := names(r.All()); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("mutating All() changed the registry: %v", got)
	}
}

func TestRegistryConcurrentRegister(t *testing.T) {
	r := NewRegistry()
	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- r.Register(stubTool{fmt.Sprintf("t%02d", i)})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Register: %v", err)
		}
	}
	if got := len(r.All()); got != n {
		t.Fatalf("registered %d tools, want %d", got, n)
	}
}

func names(ts []Tool) []string {
	out := make([]string, 0, len(ts))
	for _, t := range ts {
		out = append(out, t.Name())
	}
	return out
}

func TestScopePipelineIDAndInScopeHelpers(t *testing.T) {
	p := owners.Pipeline{
		ID:           "orders",
		TopicPrefix:  "pg",
		SourceTables: []string{"public.orders"},
	}
	scoped := Scope{p: p}
	unscoped := Scope{unscoped: true}

	t.Run("PipelineID", func(t *testing.T) {
		tests := []struct {
			name  string
			scope Scope
			input string
			want  string
		}{
			{name: "scoped ignores input", scope: scoped, input: "other", want: "orders"},
			{name: "scoped with empty input", scope: scoped, input: "", want: "orders"},
			{name: "unscoped passes input through", scope: unscoped, input: "payments", want: "payments"},
		}
		for _, tt := range tests {
			if got := tt.scope.PipelineID(tt.input); got != tt.want {
				t.Errorf("%s: PipelineID(%q) = %q, want %q", tt.name, tt.input, got, tt.want)
			}
		}
	})

	t.Run("TopicInScope", func(t *testing.T) {
		tests := []struct {
			name  string
			scope Scope
			topic string
			want  bool
		}{
			{name: "pipeline topic", scope: scoped, topic: "pg.public.orders", want: true},
			{name: "other topic", scope: scoped, topic: "pg.public.payments"},
			{name: "bare table name is not a topic", scope: scoped, topic: "public.orders"},
			{name: "unscoped accepts anything", scope: unscoped, topic: "anything", want: true},
		}
		for _, tt := range tests {
			if got := tt.scope.TopicInScope(tt.topic); got != tt.want {
				t.Errorf("%s: TopicInScope(%q) = %v, want %v", tt.name, tt.topic, got, tt.want)
			}
		}
	})

	t.Run("SourceTableInScope", func(t *testing.T) {
		tests := []struct {
			name  string
			scope Scope
			table string
			want  bool
		}{
			{name: "pipeline table", scope: scoped, table: "public.orders", want: true},
			{name: "table of another schema", scope: scoped, table: "analytics.orders"},
			{name: "topic is not a table", scope: scoped, table: "pg.public.orders"},
			{name: "unscoped accepts anything", scope: unscoped, table: "anything", want: true},
		}
		for _, tt := range tests {
			if got := tt.scope.SourceTableInScope(tt.table); got != tt.want {
				t.Errorf("%s: SourceTableInScope(%q) = %v, want %v", tt.name, tt.table, got, tt.want)
			}
		}
	})

}
