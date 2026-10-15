//go:build race

package redact

// raceEnabled reports whether the race detector is on; it slows code by an
// amount too uneven for timing checks.
const raceEnabled = true
