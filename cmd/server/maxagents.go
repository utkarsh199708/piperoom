package main

import (
	"fmt"
	"strconv"
	"strings"

	"piperoom/internal/room"
)

// parseMaxAgents reads PIPEROOM_MAX_AGENTS: empty or "0" means the default
// (room.DefaultMaxAgents); any other value must be a whole number of at least
// 1. A bad value is a start-up error: a typo must not silently lift the cap on
// model spend.
func parseMaxAgents(raw string) (int, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return room.DefaultMaxAgents, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("server: PIPEROOM_MAX_AGENTS %q is not a whole number", raw)
	}
	if n == 0 {
		return room.DefaultMaxAgents, nil
	}
	if n < 1 {
		return 0, fmt.Errorf("server: PIPEROOM_MAX_AGENTS %q must be 1 or more (0 or empty = %d)", raw, room.DefaultMaxAgents)
	}
	return n, nil
}
