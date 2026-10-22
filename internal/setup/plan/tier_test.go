package plan

import (
	"errors"
	"testing"
)

// TestTierOf is the table over every (floor, class) pair: the class raises a
// write and a read stays a tool.
func TestTierOf(t *testing.T) {
	tests := []struct {
		floor, class string
		want         string
	}{
		{TierT0, ClassLocal, TierT0},
		{TierT0, ClassShared, TierT0},
		{TierT0, ClassProduction, TierT0},
		{TierT1, ClassLocal, TierT1},
		{TierT1, ClassShared, TierT2},
		{TierT1, ClassProduction, TierT3},
		{TierT2, ClassLocal, TierT2},
		{TierT2, ClassShared, TierT2},
		{TierT2, ClassProduction, TierT3},
		{TierT3, ClassLocal, TierT3},
		{TierT3, ClassShared, TierT3},
		{TierT3, ClassProduction, TierT3},
	}
	for _, tt := range tests {
		got, err := TierOf(tt.floor, tt.class)
		if err != nil {
			t.Errorf("TierOf(%q, %q): %v", tt.floor, tt.class, err)
			continue
		}
		if got != tt.want {
			t.Errorf("TierOf(%q, %q) = %q, want %q", tt.floor, tt.class, got, tt.want)
		}
	}
}

func TestTierOfRefusesUnknown(t *testing.T) {
	for _, tt := range []struct{ floor, class string }{
		{"", ClassLocal},
		{"T4", ClassLocal},
		{"t1", ClassLocal},
		{TierT1, ""},
		{TierT1, "prod"},
		{TierT1, "Local"},
	} {
		if got, err := TierOf(tt.floor, tt.class); !errors.Is(err, ErrTier) && !errors.Is(err, ErrClass) {
			t.Errorf("TierOf(%q, %q) = %q, %v; want ErrTier or ErrClass", tt.floor, tt.class, got, err)
		}
	}
}

func TestValidTier(t *testing.T) {
	for _, tier := range []string{TierT0, TierT1, TierT2, TierT3} {
		if !ValidTier(tier) {
			t.Errorf("ValidTier(%q) = false", tier)
		}
	}
	for _, tier := range []string{"", "T4", "t2", "T"} {
		if ValidTier(tier) {
			t.Errorf("ValidTier(%q) = true", tier)
		}
	}
}
