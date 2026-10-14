package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"piperoom/internal/room"
)

// parseMaxAgents reads PIPEROOM_MAX_AGENTS: empty or "0" means the default
// (room.DefaultMaxAgents); any other value must be plain decimal digits that
// make a whole number of at least 1. Signs ("+3", "-0", "-1") are refused, as
// are decimals and text. A bad value is a start-up error: a typo must not
// silently change the cap on model spend.
func parseMaxAgents(raw string) (int, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return room.DefaultMaxAgents, nil
	}
	for _, r := range v {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("server: PIPEROOM_MAX_AGENTS %q must be a whole number of 1 or more (0 or empty = %d)", raw, room.DefaultMaxAgents)
		}
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, fmt.Errorf("server: PIPEROOM_MAX_AGENTS %q is too large", raw)
		}
		return 0, fmt.Errorf("server: PIPEROOM_MAX_AGENTS %q is not a whole number: %w", raw, err)
	}
	if n == 0 {
		return room.DefaultMaxAgents, nil
	}
	return n, nil
}
