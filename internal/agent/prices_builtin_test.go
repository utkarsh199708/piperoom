package agent

import (
	"math"
	"testing"

	"piperoom/internal/config"
)

// builtinWant is the list-price table of 2026-10 (QA G14), written out so a
// slip in the embedded file shows up as a failing row, not as a silently
// wrong estimate.
var builtinWant = map[string]ModelPrice{
	"claude-fable-5-1":  {Input: 10, Output: 50, CacheWrite: 12.5, CacheRead: 0.25},
	"claude-fable-5":    {Input: 10, Output: 50, CacheWrite: 12.5, CacheRead: 1.0},
	"claude-opus-5-5":   {Input: 4, Output: 20, CacheWrite: 5, CacheRead: 0.2},
	"claude-opus-5":     {Input: 5, Output: 25, CacheWrite: 6.25, CacheRead: 0.5},
	"claude-opus-4-8":   {Input: 5, Output: 25, CacheWrite: 6.25, CacheRead: 0.5},
	"claude-opus-4-7":   {Input: 5, Output: 25, CacheWrite: 6.25, CacheRead: 0.5},
	"claude-opus-4-6":   {Input: 5, Output: 25, CacheWrite: 6.25, CacheRead: 0.5},
	"claude-sonnet-5-5": {Input: 2, Output: 10, CacheWrite: 2.5, CacheRead: 0.2},
	"claude-sonnet-5":   {Input: 2, Output: 10, CacheWrite: 2.5, CacheRead: 0.2},
	"claude-sonnet-4-6": {Input: 3, Output: 15, CacheWrite: 3.75, CacheRead: 0.3},
	"claude-haiku-5-5":  {Input: 0.1, Output: 0.5, CacheWrite: 0.125, CacheRead: 0.01},
	"claude-haiku-4-5":  {Input: 1, Output: 5, CacheWrite: 1.25, CacheRead: 0.1},
}

func TestBuiltinPricesListEveryModel(t *testing.T) {
	got, err := LoadModelPrices(config.BuiltinModelPrices)
	if err != nil {
		t.Fatalf("LoadModelPrices(builtin): %v", err)
	}
	if len(got) != len(builtinWant) {
		t.Errorf("built-in table has %d models, want %d", len(got), len(builtinWant))
	}
	for model, want := range builtinWant {
		if !got.Has(model) {
			t.Errorf("%s is not priced", model)
			continue
		}
		if got[model] != want {
			t.Errorf("%s = %+v, want %+v", model, got[model], want)
		}
		// The 5-minute cache write is 1.25x the input price; a row that breaks
		// that is a typo, not a price change.
		if w := got[model].Input * 1.25; math.Abs(got[model].CacheWrite-w) > 1e-9 {
			t.Errorf("%s cache_write = %v, want 1.25 x input = %v", model, got[model].CacheWrite, w)
		}
	}
}

// The default model must be priced, or a cost cap would refuse to start on a
// fresh install (cmd/server needs every capped model in the table).
func TestBuiltinPricesCoverTheDefaultModel(t *testing.T) {
	got, err := LoadModelPrices(config.BuiltinModelPrices)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Has(defaultModel) {
		t.Fatalf("default model %q is not in the built-in table", defaultModel)
	}
}

func TestBuiltinPricesComputeCost(t *testing.T) {
	prices, err := LoadModelPrices(config.BuiltinModelPrices)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		model string
		in    callTokens
		want  float64
	}{
		// 100k x $4 + 50k x $5 + 1M x $0.20 + 10k x $20 per million = $1.05
		{"opus 5.5, all four counts", "claude-opus-5-5", callTokens{100_000, 50_000, 1_000_000, 10_000}, 1.05},
		// 83k tokens, the low end of a QA room: 80k in, 3k out on haiku 5.5.
		{"haiku 5.5, small room", "claude-haiku-5-5", callTokens{80_000, 0, 0, 3_000}, 0.0095},
		{"fable 5.1 cache read at 0.25", "claude-fable-5-1", callTokens{0, 0, 4_000_000, 0}, 1.0},
		{"nothing used", "claude-sonnet-5-5", callTokens{}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			usd, ok := prices.CostUSD(c.model, c.in.input, c.in.cacheCreation, c.in.cacheRead, c.in.output)
			if !ok {
				t.Fatalf("%s is not priced", c.model)
			}
			if math.Abs(usd-c.want) > 1e-9 {
				t.Fatalf("cost = %v, want %v", usd, c.want)
			}
		})
	}
	// An id the table does not name stays unpriced: omitted, never guessed.
	if _, ok := prices.CostUSD("us.anthropic.claude-opus-5-5-v1:0", 1, 1, 1, 1); ok {
		t.Fatal("a model id outside the table must not be priced")
	}
}

func TestBuiltinPricesAppearInUsageEvents(t *testing.T) {
	prices, err := LoadModelPrices(config.BuiltinModelPrices)
	if err != nil {
		t.Fatal(err)
	}
	p := usagePayload("claude-opus-5-5", "main", callTokens{input: 1_000_000}, 10, prices)
	if p.CostUSD == nil || math.Abs(*p.CostUSD-4) > 1e-9 {
		t.Fatalf("cost_usd = %v, want 4 for 1M input tokens", p.CostUSD)
	}
}

// Off means no cost, whichever way the caller spells it: the typed value ""
// that config.ParseCost produces, or the raw env value a caller that skipped
// ParseCost passes on.
func TestLoadModelPricesOff(t *testing.T) {
	for _, v := range []string{"", "off", "OFF", " Off "} {
		if p, err := LoadModelPrices(v); p != nil || err != nil {
			t.Errorf("LoadModelPrices(%q) = %v, %v; want nil, nil", v, p, err)
		}
	}
}

func TestParseCostAndLoadModelPricesAgree(t *testing.T) {
	cases := []struct {
		env       string
		wantTable bool
	}{
		{"", true},        // unset: the built-in table
		{"builtin", true}, // spelled out
		{"BUILTIN", true},
		{"off", false},
		{"Off", false},
	}
	for _, c := range cases {
		cost, err := config.ParseCost(config.Config{ModelPrices: c.env})
		if err != nil {
			t.Fatalf("ParseCost(%q): %v", c.env, err)
		}
		prices, err := LoadModelPrices(cost.PricesFile)
		if err != nil {
			t.Fatalf("LoadModelPrices(%q) for env %q: %v", cost.PricesFile, c.env, err)
		}
		if got := len(prices) > 0; got != c.wantTable {
			t.Errorf("env %q: table present = %v, want %v", c.env, got, c.wantTable)
		}
	}
}

// Each call hands out a table of its own, so a caller that edits its prices
// (a test, an eval) cannot change another caller's.
func TestBuiltinPricesAreNotShared(t *testing.T) {
	a, err := LoadModelPrices(config.BuiltinModelPrices)
	if err != nil {
		t.Fatal(err)
	}
	a["claude-opus-5-5"] = ModelPrice{}
	b, err := LoadModelPrices(config.BuiltinModelPrices)
	if err != nil {
		t.Fatal(err)
	}
	if b["claude-opus-5-5"] != builtinWant["claude-opus-5-5"] {
		t.Fatal("a second load saw the first caller's edit")
	}
}
