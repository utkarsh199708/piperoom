package watch

import (
	"math"
	"time"
)

// Rate windows and the least data that gives a rate (design §3, B2).
const (
	fastWindow = time.Hour
	slowWindow = 6 * time.Hour
	// minSamples and minSpan: a rate from fewer than 3 samples spanning 30
	// minutes would be noise, so the watcher says "unknown" instead.
	minSamples = 3
	minSpan    = 30 * time.Minute
)

// sample is one reading of the slot's retained WAL.
type sample struct {
	at       time.Time
	retained int64
}

// trimSamples drops samples older than the slow window.
func trimSamples(s []sample, now time.Time) []sample {
	cutoff := now.Add(-slowWindow)
	i := 0
	for i < len(s) && s[i].at.Before(cutoff) {
		i++
	}
	if i == 0 {
		return s
	}
	return append(s[:0:0], s[i:]...)
}

// slope is the least-squares growth of retained WAL in bytes per second over
// the samples no older than window. ok is false when there are fewer than 3
// samples or they span less than 30 minutes.
func slope(samples []sample, now time.Time, window time.Duration) (perSec float64, ok bool) {
	cutoff := now.Add(-window)
	i := 0
	for i < len(samples) && samples[i].at.Before(cutoff) {
		i++
	}
	pts := samples[i:]
	if len(pts) < minSamples || pts[len(pts)-1].at.Sub(pts[0].at) < minSpan {
		return 0, false
	}
	t0, y0 := pts[0].at, pts[0].retained
	var sx, sy, sxx, sxy float64
	for _, p := range pts {
		x := p.at.Sub(t0).Seconds()
		y := float64(p.retained - y0) // centred: keeps the sums small
		sx += x
		sy += y
		sxx += x * x
		sxy += x * y
	}
	n := float64(len(pts))
	den := n*sxx - sx*sx
	if den == 0 {
		return 0, false
	}
	return (n*sxy - sx*sy) / den, true
}

// ETA bases.
const (
	basisSafeWAL  = "safe_wal_size"
	basisHeadroom = "configured wal_headroom_bytes"
)

// eta is the estimate of how long until the slot is invalidated (basis
// safe_wal_size) or the configured headroom is used up.
type eta struct {
	Known bool
	D     time.Duration
	Basis string
	// Reason says why there is no ETA (Known false).
	Reason string
}

// computeETA divides the room left by the slow rate. safe_wal_size is exact
// (bytes writable before the slot goes "unreserved"), so it wins over the
// operator's headroom figure. slowOK false = still warming up.
func computeETA(slow float64, slowOK bool, retained int64, safe *int64, headroom int64) eta {
	if !slowOK {
		return eta{Reason: "no rate yet: needs 3 samples over 30 min"}
	}
	if slow <= 0 {
		return eta{Reason: "not growing"}
	}
	if safe != nil && *safe <= 0 {
		// Postgres reports a negative safe_wal_size once the slot is unreserved
		// but not yet lost: there is nothing left to estimate.
		return eta{Reason: "slot is already past max_slot_wal_keep_size"}
	}
	var room int64
	var basis string
	switch {
	case safe != nil:
		room, basis = *safe, basisSafeWAL
	case headroom > 0:
		room, basis = headroom-retained, basisHeadroom
	default:
		return eta{Reason: "no safe_wal_size and no wal_headroom_bytes set"}
	}
	if room < 0 {
		room = 0
	}
	secs := float64(room) / slow
	const maxETA = 10 * 365 * 24 * time.Hour
	d := maxETA
	if secs < maxETA.Seconds() && !math.IsInf(secs, 0) {
		d = time.Duration(secs * float64(time.Second))
	}
	return eta{Known: true, D: d, Basis: basis}
}
