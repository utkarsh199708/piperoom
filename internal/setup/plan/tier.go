package plan

import (
	"errors"
	"fmt"

	"piperoom/internal/setup/protocol"
)

// Tiers of a step (design §3.5): the protocol's TierT0..TierT3.
const (
	TierT0 = protocol.TierT0
	TierT1 = protocol.TierT1
	TierT2 = protocol.TierT2
	TierT3 = protocol.TierT3
)

// ErrTier is returned for a tier string that is not TierT0..TierT3.
var ErrTier = errors.New("plan: unknown tier")

// TierRank orders tiers T0..T3, and reports false for a string that is not a
// tier.
func TierRank(tier string) (rank int, ok bool) {
	switch tier {
	case TierT0:
		return 0, true
	case TierT1:
		return 1, true
	case TierT2:
		return 2, true
	case TierT3:
		return 3, true
	}
	return 0, false
}

// ValidTier reports whether tier is one of the four tiers.
func ValidTier(tier string) bool {
	_, ok := TierRank(tier)
	return ok
}

// TierOf returns the tier a step runs at: the higher of the op's floor and the
// class of the endpoint it resolves to (local is T1, shared T2, production
// T3). The one exception is a read: a T0 op stays T0 on every class, because a
// T0 op is a tool and never a proposal (design §3.3, U1-6), so reading a
// production system needs no approval and the class only raises writes. An
// unknown floor or class is an error, never a silent T0.
func TierOf(floor, class string) (string, error) {
	if !ValidTier(floor) {
		return "", fmt.Errorf("%w: floor %q", ErrTier, floor)
	}
	rank, ok := ClassRank(class)
	if !ok {
		return "", fmt.Errorf("%w: class %q", ErrClass, class)
	}
	if floor == TierT0 {
		return TierT0, nil
	}
	classTier := [...]string{TierT1, TierT2, TierT3}[rank]
	if classRank, _ := TierRank(classTier); classRank > rankOf(floor) {
		return classTier, nil
	}
	return floor, nil
}

// rankOf is TierRank for a tier ValidTier already accepted.
func rankOf(tier string) int {
	rank, _ := TierRank(tier)
	return rank
}
