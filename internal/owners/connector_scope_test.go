package owners

import (
	"slices"
	"testing"
)

// The pure parser: a listed table without metacharacters is literal; an entry
// with metacharacters is a pattern that still matches declared tables.
func TestParseConnectorInclude(t *testing.T) {
	c := ParseConnectorInclude(map[string]string{
		"table.include.list": "public.orders,public\\.devctl_.*, public.orders",
		"table.exclude.list": "public.devctl_signal",
	})
	if len(c.Literal) != 1 || c.Literal[0] != "public.orders" {
		t.Fatalf("literal = %v, want [public.orders]", c.Literal)
	}
	if len(c.Patterns) != 1 {
		t.Fatalf("patterns = %v", c.Patterns)
	}
	if !c.Includes("public.devctl_refunds") {
		t.Fatal("pattern should include devctl_refunds")
	}
	if c.Includes("public.devctl_signal") {
		t.Fatal("exclude list must win over a pattern")
	}
	if c.Includes("public.payments") {
		t.Fatal("unlisted table must not be included")
	}
	if got := c.OutsideDeclared([]string{"public.orders"}); len(got) != 0 {
		t.Fatalf("OutsideDeclared = %v, want none (only literal tables can be listed)", got)
	}
}

func TestParseConnectorIncludeTables(t *testing.T) {
	tests := []struct {
		name     string
		cfg      map[string]string
		literal  []string
		patterns []string
		invalid  []string
		bounded  bool
	}{
		{
			name:    "literal entries, duplicates dropped and spaces trimmed",
			cfg:     map[string]string{"table.include.list": " public.orders , public.customers,public.orders "},
			literal: []string{"public.orders", "public.customers"},
			bounded: true,
		},
		{
			name:     "pattern entry is not expanded",
			cfg:      map[string]string{"table.include.list": `public\.devctl_.*`},
			patterns: []string{`public\.devctl_.*`},
			bounded:  true,
		},
		{
			name:    "excluded literal is neither literal nor a gap",
			cfg:     map[string]string{"table.include.list": "public.orders,public.refunds", "table.exclude.list": "public.refunds"},
			literal: []string{"public.orders"},
			bounded: true,
		},
		{
			name:    "uncompilable entries are reported, not dropped silently",
			cfg:     map[string]string{"table.include.list": "public.orders,public.[", "table.exclude.list": "public.("},
			literal: []string{"public.orders"},
			// Exclude entries are read first, then include entries.
			invalid: []string{"public.(", "public.["},
			bounded: true,
		},
		{
			name:    "no include list is unbounded",
			cfg:     map[string]string{"table.exclude.list": "public.orders"},
			bounded: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ParseConnectorInclude(tt.cfg)
			if !slices.Equal(c.Literal, tt.literal) {
				t.Errorf("Literal = %v, want %v", c.Literal, tt.literal)
			}
			if !slices.Equal(c.Patterns, tt.patterns) {
				t.Errorf("Patterns = %v, want %v", c.Patterns, tt.patterns)
			}
			if !slices.Equal(c.Invalid, tt.invalid) {
				t.Errorf("Invalid = %v, want %v", c.Invalid, tt.invalid)
			}
			if c.Unbounded == tt.bounded {
				t.Errorf("Unbounded = %v, want %v", c.Unbounded, !tt.bounded)
			}
		})
	}
}

// The signal table is captured but is infrastructure, not a source table, so
// it is never literal and never a scope gap.
func TestConnectorIncludeSignalTableIsNotALiteral(t *testing.T) {
	c := ParseConnectorInclude(map[string]string{
		"table.include.list":     "public.orders,public.devctl_signal",
		"signal.data.collection": "public.devctl_signal",
	})
	if !slices.Equal(c.Literal, []string{"public.orders"}) {
		t.Fatalf("Literal = %v, want [public.orders]", c.Literal)
	}
	if !c.Includes("public.devctl_signal") {
		t.Fatal("the signal table is still captured by the connector")
	}
	if got := c.OutsideDeclared([]string{"public.orders"}); len(got) != 0 {
		t.Fatalf("OutsideDeclared = %v, want none", got)
	}
}

// A connector with no include list captures every table, and an excluded table
// is not captured even then.
func TestConnectorIncludeUnbounded(t *testing.T) {
	c := ParseConnectorInclude(map[string]string{"table.exclude.list": "public.secret"})
	if !c.Includes("public.anything") {
		t.Fatal("an absent include list captures every table")
	}
	if c.Includes("public.secret") {
		t.Fatal("an exclude entry must win over the unbounded default")
	}
	if got := c.OutsideDeclared([]string{"public.anything"}); len(got) != 0 {
		t.Fatalf("OutsideDeclared = %v, want none for an unbounded filter", got)
	}
}

// OutsideDeclared is sorted so findings and cache keys are stable.
func TestConnectorIncludeOutsideDeclaredSorted(t *testing.T) {
	c := ParseConnectorInclude(map[string]string{
		"table.include.list": "public.zeta,public.orders,public.alpha",
	})
	got := c.OutsideDeclared([]string{"public.orders"})
	if !slices.Equal(got, []string{"public.alpha", "public.zeta"}) {
		t.Fatalf("OutsideDeclared = %v, want [public.alpha public.zeta]", got)
	}
}
