package watch

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// phase is where one (pipeline, signal) pair is in its life.
type phase int

const (
	// phaseOK: not firing.
	phaseOK phase = iota
	// phaseFiring: the condition held for `for`.
	phaseFiring
	// phaseAcked: the driver closed the room while the signal still fires. It
	// goes back to phaseOK only when the signal clears, so a closed room is not
	// re-opened by the same incident.
	phaseAcked
)

func (p phase) String() string {
	switch p {
	case phaseFiring:
		return "firing"
	case phaseAcked:
		return "acked"
	}
	return "ok"
}

// verdict is what one tick says about one signal.
type verdict int

const (
	// vUnknown: the check failed. Timers are left alone.
	vUnknown verdict = iota
	// vBreach: the condition holds; it must hold for `for` before the signal fires.
	vBreach
	// vBreachNow: the condition holds and fires at once (wal_status lost).
	vBreachNow
	// vHold: between clear and fire. Neither timer runs.
	vHold
	// vClear: the condition is gone; it must stay gone for `for` to resolve.
	vClear
)

// reading is one tick's finding for one signal.
type reading struct {
	v   verdict
	err error
	sev string
	// label is the title fragment; with withDur the time since the signal
	// started is appended ("connector pg-orders FAILED 4m").
	label   string
	withDur bool
	// value and threshold are for the log line.
	value, threshold string
	// ev is this signal's evidence. Built fresh each tick and never changed
	// afterwards, so it can be read without the lock.
	ev map[string]any
	// targets name the objects a breach is about (connector, slot, topic or
	// group names). With the signal id they are the incident keys (QA G1).
	targets []string
}

// transition is what a step did to the state.
type transition int

const (
	tNone transition = iota
	tFired
	tResolved
)

// sigState is the memory of one (pipeline, signal) pair.
type sigState struct {
	phase phase
	// breachingSince is when the condition started to hold (zero = not holding).
	// While firing it keeps the start of the incident, for the "4m" in the title.
	breachingSince time.Time
	// clearingSince is when the value went below clear (zero = not clearing).
	clearingSince time.Time
	// etaSince is when the slot-invalidation estimate dropped below eta_sev1
	// (slot_wal only); sev1 needs it to hold for 2×for.
	etaSince time.Time
	// notified: a room or join already carries this signal.
	notified bool
	// ackSev is the severity when the driver closed the room; a rise above it
	// re-opens the incident (reviveWorse).
	ackSev string
	// last is the latest known (non-error) reading.
	last reading
	// errText is the latest error, "" after a good tick.
	errText string
	// targets are the objects the signal breached on since its breach began
	// (sorted, capped); see keys. Reset when the signal resolves.
	targets []string
	// firedAt is when the signal last fired; ackedAt when its room closed
	// while it still fired. escalated: it already fired EscalateAfter with no
	// room in this episode, so it re-opened (or tried to) once.
	firedAt, ackedAt time.Time
	// clearedFrom is when the signal last started to clear before it resolved;
	// it survives the resolve, so a new breach can tell a flap from a recovery
	// (Q6 U2).
	clearedFrom time.Time
	escalated   bool
	// escFree: the escalation could not have been what opens the room (the cap,
	// a failing open or a storm wait blocked it; no ack or cooldown to bypass).
	// The next room that carries the signal then gives the budget back, so a
	// close while it still fires re-opens once (QA Q2 F1).
	escFree bool
}

// maxSignalTargets caps the incident keys of one signal: a storm of a hundred
// connectors is still one connector_down.
const maxSignalTargets = 16

// addTargets merges the targets of a breach reading.
func (s *sigState) addTargets(ts []string) (grew bool) {
	for _, t := range ts {
		if t == "" || slices.Contains(s.targets, t) || len(s.targets) >= maxSignalTargets {
			continue
		}
		s.targets = append(s.targets, t)
		grew = true
	}
	slices.Sort(s.targets)
	return grew
}

// keys are the signal's incident keys: "connector_down:pg-orders" per target,
// or the bare signal id when the breach named no object.
func (s *sigState) keys(id string) []string {
	if len(s.targets) == 0 {
		return []string{id}
	}
	out := make([]string, 0, len(s.targets))
	for _, t := range s.targets {
		out = append(out, sigKey(id, t))
	}
	return out
}

// step feeds one reading into the state machine. All timing is wall-clock
// based, so the result does not depend on how often the loop runs: a breach for
// `for - 1s` does not fire, and an error tick in the middle changes nothing.
func (s *sigState) step(now time.Time, r reading, forD time.Duration) transition {
	if r.v == vUnknown {
		return tNone
	}
	s.last = r
	switch r.v {
	case vBreach, vBreachNow:
		s.clearingSince = time.Time{}
		grew := s.addTargets(r.targets)
		if s.phase == phaseAcked && grew {
			// A target the closed incident did not carry (a second connector,
			// group or DLQ topic) is a different incident: post it at once,
			// whatever escalation did before (QA Q4 R1). Its key is not in
			// the cooldown, so the open is not held back.
			s.phase, s.notified = phaseFiring, false
			return tNone
		}
		if s.phase != phaseOK {
			return tNone
		}
		if s.breachingSince.IsZero() {
			s.breachingSince = now
		}
		if r.v == vBreachNow || now.Sub(s.breachingSince) >= forD {
			s.phase = phaseFiring
			s.firedAt = now
			return tFired
		}
	case vHold:
		if s.phase == phaseOK {
			s.breachingSince, s.targets = time.Time{}, nil
		} else {
			s.clearingSince = time.Time{}
		}
	case vClear:
		if s.phase == phaseOK {
			s.breachingSince, s.targets = time.Time{}, nil
			return tNone
		}
		if s.clearingSince.IsZero() {
			s.clearingSince = now
		}
		if now.Sub(s.clearingSince) >= forD {
			s.phase = phaseOK
			s.clearedFrom = s.clearingSince
			s.breachingSince, s.clearingSince = time.Time{}, time.Time{}
			s.notified, s.ackSev = false, ""
			s.targets, s.firedAt, s.ackedAt, s.escalated, s.escFree = nil, time.Time{}, time.Time{}, false, false
			return tResolved
		}
	}
	return tNone
}

// ---- severity ----

// sevRank turns "sev2" into 2; unknown text ranks as the least severe.
func sevRank(s string) int {
	if strings.HasPrefix(s, "sev") {
		if n, err := strconv.Atoi(s[3:]); err == nil && n >= 1 {
			return n
		}
	}
	return 9
}

// moreSevere is the worse of two severities.
func moreSevere(a, b string) string {
	if sevRank(b) < sevRank(a) {
		return b
	}
	return a
}

// ---- text helpers ----

// firstLine is the first non-empty line of s with control characters removed,
// capped at max runes. A Kafka Connect trace starts with the exception class
// and message; the rest is stack frames that can hold file paths and values.
func firstLine(s string, max int) string {
	for _, line := range strings.Split(s, "\n") {
		line = clean(line, max)
		if line != "" {
			return line
		}
	}
	return ""
}

// clean removes control characters, trims space and caps the text at max runes.
func clean(s string, max int) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
	s = strings.TrimSpace(s)
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	return string([]rune(s)[:max])
}

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	h, m := int(d.Hours()), int(d.Minutes())%60
	if m == 0 {
		return fmt.Sprintf("%dh", h)
	}
	return fmt.Sprintf("%dh%dm", h, m)
}

func fmtBytes(n int64) string {
	const k = 1024.0
	f := float64(n)
	for _, u := range []string{"B", "KB", "MB", "GB", "TB"} {
		if f < k || u == "TB" {
			if u == "B" {
				return fmt.Sprintf("%d B", n)
			}
			return fmt.Sprintf("%.1f %s", f, u)
		}
		f /= k
	}
	return fmt.Sprintf("%d B", n)
}
