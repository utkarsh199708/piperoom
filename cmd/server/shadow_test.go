package main

import (
	"math"
	"testing"
	"time"

	"piperoom/internal/config"
)

func TestShadowAgentOptions(t *testing.T) {
	cfg := config.Config{Model: "m", Effort: "high"}
	for _, tc := range []struct {
		name string
		env  string
		want int64
	}{
		{"default", "", 500000},
		{"explicit", "1234", 1234},
		// 0 = no cap: never the agent's 0 (= the live Endpoint default).
		{"no cap", "0", math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := cfg
			c.ShadowTokenBudget = tc.env
			sh, err := config.ParseShadow(c)
			if err != nil {
				t.Fatal(err)
			}
			o := shadowAgentOptions(c, sh)
			if o.TokenBudget != tc.want {
				t.Fatalf("TokenBudget = %d, want %d", o.TokenBudget, tc.want)
			}
			if o.TokenBudget <= 0 {
				t.Fatal("a shadow run must never fall back to the live room budget")
			}
			if o.Model != "m" || o.Effort != "high" || o.MaxTurns != 20 {
				t.Fatalf("options = %+v", o)
			}
		})
	}
	sh, err := config.ParseShadow(config.Config{ShadowMaxTurns: "7", ShadowGrace: "30s"})
	if err != nil || shadowAgentOptions(cfg, sh).MaxTurns != 7 || sh.Grace != 30*time.Second {
		t.Fatalf("max turns / grace: %+v %v", sh, err)
	}
}
