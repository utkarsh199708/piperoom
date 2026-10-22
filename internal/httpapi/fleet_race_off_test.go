//go:build !race

package httpapi_test

// fleetTimingScale is 1 without the race detector: TC4's bound is 50 ms.
const fleetTimingScale = 1
