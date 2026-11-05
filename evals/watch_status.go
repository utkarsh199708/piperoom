package evals

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// WatchStatusSpec is the watcher's status row of the scenario's pipeline, as
// the room would read it through agent.Deps.Watch (the hypothesis guard's rule
// 1, docs/design/hypothesis-guard.md). The harness writes it to the run's
// in-memory store before the room opens; a scenario without the field gives
// the agent no watch reader, as before.
//
// Times are offsets from the start of the run ("0s", "-90s", "-20m"), because
// the guard compares them with the clock: an absolute time would go stale. An
// empty Signals is a fresh row that shows nothing firing (the pass path).
type WatchStatusSpec struct {
	// CheckedAt is when the watcher last ticked; empty means "0s" (just now).
	CheckedAt string            `yaml:"checked_at"`
	Signals   []WatchSignalSpec `yaml:"signals"`
}

// WatchSignalSpec is one clients.WatchSignal of the row.
type WatchSignalSpec struct {
	Signal   string `yaml:"signal"`
	State    string `yaml:"state"`    // firing | acked
	Severity string `yaml:"severity"` // sev1..sev4
	Node     string `yaml:"node"`
	// Since is when the signal started firing; required.
	Since string `yaml:"since"`
	// ClearingSince, when set, is when the condition went away while the
	// signal still counts as firing (the watcher waits out its `for`). Not
	// before Since.
	ClearingSince string `yaml:"clearing_since"`
}

// watchSignalNames are the signals the watcher can put in a status row.
var watchSignalNames = []string{
	owners.SignalConnectorDown, owners.SignalSlotInactive, owners.SignalSlotWAL, owners.SignalDLQGrowth,
	owners.SignalConfigRisk, owners.SignalConsumerLag, owners.SignalGroupUnstable, owners.SignalOffsetBelowLogStart,
	owners.SignalLongTx, events.SignalCountDrift, events.SignalRowDrift, events.SignalDupKeys, events.SignalOrderSkew,
	events.SignalForecast, events.SignalConfigDrift,
}

// offset parses a run-start offset: a Go duration, zero or negative ("the
// past"), since a status row describes what the watcher already saw.
func offset(field, s string) (time.Duration, error) {
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("%s %q: want a duration such as 0s, -90s or -20m", field, s)
	}
	if d > 0 {
		return 0, fmt.Errorf("%s %q is in the future of the run", field, s)
	}
	return d, nil
}

// validate checks the shape of the row; every error names its field.
func (w *WatchStatusSpec) validate() []error {
	if w == nil {
		return nil
	}
	var errs []error
	if w.CheckedAt != "" {
		if _, err := offset("watch_status.checked_at", w.CheckedAt); err != nil {
			errs = append(errs, err)
		}
	}
	if len(w.Signals) > clients.MaxWatchSignals {
		errs = append(errs, fmt.Errorf("watch_status.signals: %d signals, at most %d", len(w.Signals), clients.MaxWatchSignals))
	}
	seen := map[string]bool{}
	for i, g := range w.Signals {
		at := fmt.Sprintf("watch_status.signals[%d]", i)
		if !slices.Contains(watchSignalNames, g.Signal) {
			errs = append(errs, fmt.Errorf("%s.signal %q is not a watcher signal (want one of %s)", at, g.Signal, strings.Join(watchSignalNames, ", ")))
		}
		if seen[g.Signal] {
			errs = append(errs, fmt.Errorf("%s.signal %q is listed twice", at, g.Signal))
		}
		seen[g.Signal] = true
		if g.State != clients.WatchFiring && g.State != clients.WatchAcked {
			errs = append(errs, fmt.Errorf("%s.state %q: want %s or %s (a signal that is ok is not listed)", at, g.State, clients.WatchFiring, clients.WatchAcked))
		}
		switch g.Severity {
		case "sev1", "sev2", "sev3", "sev4":
		default:
			errs = append(errs, fmt.Errorf("%s.severity %q: want sev1, sev2, sev3 or sev4", at, g.Severity))
		}
		since, err := offset(at+".since", g.Since)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if g.ClearingSince != "" {
			cs, err := offset(at+".clearing_since", g.ClearingSince)
			switch {
			case err != nil:
				errs = append(errs, err)
			case cs < since:
				errs = append(errs, fmt.Errorf("%s.clearing_since %s is before since %s", at, g.ClearingSince, g.Since))
			}
		}
	}
	return errs
}

// status builds the row for pipelineID with every offset taken from start.
func (w *WatchStatusSpec) status(pipelineID string, start time.Time) (clients.WatchStatus, error) {
	if w == nil {
		return clients.WatchStatus{}, errors.New("evals: no watch_status")
	}
	checked := time.Duration(0)
	if w.CheckedAt != "" {
		d, err := offset("watch_status.checked_at", w.CheckedAt)
		if err != nil {
			return clients.WatchStatus{}, err
		}
		checked = d
	}
	st := clients.WatchStatus{PipelineID: pipelineID, CheckedAt: start.Add(checked), Signals: []clients.WatchSignal{}}
	for i, g := range w.Signals {
		since, err := offset(fmt.Sprintf("watch_status.signals[%d].since", i), g.Since)
		if err != nil {
			return clients.WatchStatus{}, err
		}
		sig := clients.WatchSignal{Signal: g.Signal, State: g.State, Severity: g.Severity, Node: g.Node, Since: start.Add(since)}
		if g.ClearingSince != "" {
			cs, err := offset(fmt.Sprintf("watch_status.signals[%d].clearing_since", i), g.ClearingSince)
			if err != nil {
				return clients.WatchStatus{}, err
			}
			t := start.Add(cs)
			sig.ClearingSince = &t
		}
		st.Signals = append(st.Signals, sig)
	}
	return st, nil
}

// IntRange is an expected count: a bare number means exactly that many, a
// {min, max} mapping a range with both ends included. Either end may be left
// out (min defaults to 0, max to no upper bound).
type IntRange struct {
	Min *int
	Max *int
}

// UnmarshalYAML accepts `2` or `{min: 1, max: 2}`.
func (r *IntRange) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.ScalarNode:
		var v int
		if err := n.Decode(&v); err != nil {
			return fmt.Errorf("line %d: want a whole number or {min, max}: %w", n.Line, err)
		}
		r.Min, r.Max = &v, &v
		return nil
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			if k := n.Content[i].Value; k != "min" && k != "max" {
				return fmt.Errorf("line %d: unknown field %q (want min or max)", n.Content[i].Line, k)
			}
		}
		var m struct {
			Min *int `yaml:"min"`
			Max *int `yaml:"max"`
		}
		if err := n.Decode(&m); err != nil {
			return err
		}
		if m.Min == nil && m.Max == nil {
			return fmt.Errorf("line %d: an empty range: give min, max or both", n.Line)
		}
		r.Min, r.Max = m.Min, m.Max
		return nil
	}
	return fmt.Errorf("line %d: want a whole number or {min, max}", n.Line)
}

// validate checks the bounds are not negative and not reversed.
func (r *IntRange) validate(field string) []error {
	if r == nil {
		return nil
	}
	var errs []error
	if r.Min != nil && *r.Min < 0 {
		errs = append(errs, fmt.Errorf("%s: min is negative", field))
	}
	if r.Max != nil && *r.Max < 0 {
		errs = append(errs, fmt.Errorf("%s: max is negative", field))
	}
	if r.Min != nil && r.Max != nil && *r.Min > *r.Max {
		errs = append(errs, fmt.Errorf("%s: min %d is above max %d", field, *r.Min, *r.Max))
	}
	return errs
}

// has reports whether n is inside the range.
func (r *IntRange) has(n int) bool {
	return (r.Min == nil || n >= *r.Min) && (r.Max == nil || n <= *r.Max)
}

// String renders the range for a failure line.
func (r *IntRange) String() string {
	switch {
	case r.Min != nil && r.Max != nil && *r.Min == *r.Max:
		return fmt.Sprint(*r.Min)
	case r.Min != nil && r.Max != nil:
		return fmt.Sprintf("%d to %d", *r.Min, *r.Max)
	case r.Min != nil:
		return fmt.Sprintf("at least %d", *r.Min)
	}
	return fmt.Sprintf("at most %d", *r.Max)
}

// guardRefusedMessage is the log message the hypothesis guard writes once per
// refusal it makes (internal/agent/guard.go, check): the call is returned to
// the model as an error and no room event records it, so a run counts its
// refusals from this record. It is written in enforce mode only; log mode
// writes "would refuse".
const guardRefusedMessage = "agent: hypothesis guard refused"

// guardCounter counts guard refusals in a run's log records.
type guardCounter struct{ n atomic.Int64 }

// countingHandler passes every record to next and counts the guard's refusals.
// It reports itself enabled for any level and forwards only what next wants:
// evalrun logs errors only, and a refusal is a warning.
type countingHandler struct {
	next slog.Handler
	c    *guardCounter
}

func (h countingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h countingHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == guardRefusedMessage {
		h.c.n.Add(1)
	}
	if !h.next.Enabled(ctx, r.Level) {
		return nil
	}
	return h.next.Handle(ctx, r)
}

func (h countingHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return countingHandler{next: h.next.WithAttrs(a), c: h.c}
}

func (h countingHandler) WithGroup(name string) slog.Handler {
	return countingHandler{next: h.next.WithGroup(name), c: h.c}
}
