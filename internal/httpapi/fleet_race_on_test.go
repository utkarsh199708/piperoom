//go:build race

package httpapi_test

// fleetTimingScale widens TC4's 50 ms bound under the race detector, which
// slows this code several times over.
const fleetTimingScale = 10
