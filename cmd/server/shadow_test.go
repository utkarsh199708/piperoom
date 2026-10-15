package main

import (
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
		// 0 passes through: the agent reads 0 as the room default, never unlimited.
		{"zero", "0", 0},
		{"500000", "500000", 500000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := cfg
			c.ShadowTokenBudget = tc.env
			sh, err := config.ParseShadow(c)
			if err != nil {
				t.Fatal(err)
			}
			o := shadowAgentOptions(c, sh, config.Limits{})
			if o.TokenBudget != tc.want {
				t.Fatalf("TokenBudget = %d, want %d", o.TokenBudget, tc.want)
			}
			if o.Model != "m" || o.Effort != "high" || o.MaxTurns != 20 {
				t.Fatalf("options = %+v", o)
			}
		})
	}
	sh, err := config.ParseShadow(config.Config{ShadowMaxTurns: "7", ShadowGrace: "30s"})
	if err != nil || shadowAgentOptions(cfg, sh, config.Limits{}).MaxTurns != 7 || sh.Grace != 30*time.Second {
		t.Fatalf("max turns / grace: %+v %v", sh, err)
	}
}

// PIPEROOM_EVENT_MAX_DATA_BYTES reaches the agent's tool data cap for live and
// shadow runs (#371): unset, the agent cuts Result.Data at 64 KiB whatever the
// room accepts.
func TestAgentOptionsToolDataCap(t *testing.T) {
	lim, err := config.ParseLimits(config.Config{EventMaxDataBytes: "256KiB"})
	if err != nil {
		t.Fatal(err)
	}
	sh, err := config.ParseShadow(config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Model: "m"}
	if got := agentOptions(cfg, lim).MaxToolDataBytes; got != 256<<10 {
		t.Fatalf("live MaxToolDataBytes = %d, want %d", got, 256<<10)
	}
	if got := shadowAgentOptions(cfg, sh, lim).MaxToolDataBytes; got != 256<<10 {
		t.Fatalf("shadow MaxToolDataBytes = %d, want %d", got, 256<<10)
	}
	if o := agentOptions(cfg, lim); o.Model != "m" || o.MaxTurns != 40 {
		t.Fatalf("options = %+v", o)
	}
}
