package agent

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// Rule 1 of the hypothesis guard (H1b, docs/design/hypothesis-guard.md §A):
// no "healthy / nothing to fix" while the watcher still sees a symptom of the
// room's pipeline. In the long-transaction room the agent read
// pg_replication_slots twice, called the pipeline healthy, and the watcher
// kept firing slot_wal: the agent's own reading was the thing that was wrong,
// so the check asks something that does not depend on it, the watcher's
// status row (Deps.Watch, shared by every replica).
//
// The rule fails open: no reader, no row, a stale row or a store error pass,
// because Piperoom's own store being down must not stop an investigation and a
// room opened by an external alert may have no watcher at all.

// symptomSignals are the watcher signals that mean data is not flowing or not
// right now. The advisory ones (config_risk, forecast, config_drift) warn of a
// risk; a pipeline can be healthy while they fire, so they never block.
var symptomSignals = map[string]bool{
	owners.SignalConnectorDown:       true,
	owners.SignalSlotInactive:        true,
	owners.SignalSlotWAL:             true,
	owners.SignalLongTx:              true,
	owners.SignalDLQGrowth:           true,
	owners.SignalConsumerLag:         true,
	owners.SignalGroupUnstable:       true,
	owners.SignalOffsetBelowLogStart: true,
	events.SignalCountDrift:          true,
	events.SignalRowDrift:            true,
	events.SignalDupKeys:             true,
	events.SignalOrderSkew:           true,
}

// signalReadTools maps a watcher signal to the read tools that show it. The
// first-look table (triage_pass.go tableReads) picks its reads from the same
// tools; TestSignalReadToolsMatchFirstLook keeps the two in step. The drift
// rows are not in the first-look table.
var signalReadTools = map[string][]string{
	owners.SignalConnectorDown:       {"connect_status", "pg_recent_ddl"},
	owners.SignalSlotInactive:        {"connect_status", "pg_replication_slots"},
	owners.SignalSlotWAL:             {"pg_long_transactions", "pg_replication_slots"},
	owners.SignalLongTx:              {"pg_streaming_liveness", "pg_long_transactions"},
	owners.SignalDLQGrowth:           {"dlq_peek", "connect_status"},
	owners.SignalConsumerLag:         {"kafka_consumer_lag", "connect_status"},
	owners.SignalGroupUnstable:       {"kafka_consumer_lag", "connect_status"},
	owners.SignalOffsetBelowLogStart: {"kafka_topic_offsets", "kafka_topic_config"},
	owners.SignalConfigRisk:          {"connect_config", "connect_status"},
	events.SignalCountDrift:          {"reconcile", "warehouse_row"},
	events.SignalRowDrift:            {"reconcile", "warehouse_row"},
	events.SignalDupKeys:             {"kafka_dup_keys", "reconcile"},
	events.SignalOrderSkew:           {"kafka_find_by_key", "reconcile"},
}

const (
	// kindWatchCleared is the runner's own input kind for a recheck that saw
	// the refused symptom clear. Like kindApplyChecked it never comes from
	// the Inbox and never reaches the room; unlike it, the model reads it (it
	// is the reason for the turn), and it is not quiet: it wakes an idle
	// agent and starts a new episode.
	kindWatchCleared = "watch_cleared"

	// watchRowMaxAge is the agent's own ceiling on a row's age. The room
	// service's reader already drops rows older than 3 watcher intervals;
	// this keeps a reader without that filter from refusing on old news.
	watchRowMaxAge = 15 * time.Minute
	// symptomReadTimeout bounds one status read, inside the room tool's own
	// timeout.
	symptomReadTimeout = 5 * time.Second
	// maxSymptomSignalsListed keeps the refusal short; offeredTools caps the
	// reads it names.
	maxSymptomSignalsListed = 3
)

// watchRecheckAfter are the times after a refusal at which the runner reads
// the status row again (design §A "Loops and the race"): the watcher's next
// tick (60 s by default) marks a self-recovered signal clearing.
var watchRecheckAfter = []time.Duration{90 * time.Second, 3 * time.Minute, 6 * time.Minute}

// symptomGuard is rule 1's state for one run: the reader, and the recheck
// that wakes the agent when a refused symptom clears. Nil is off (no reader,
// setup mode, guard off).
type symptomGuard struct {
	watch    clients.WatchStatusReader
	pipeline string
	log      *slog.Logger
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error
	after    []time.Duration

	mu sync.Mutex
	wg sync.WaitGroup
	// base is the run's context, set by begin; a recheck runs under a child
	// of it, so it ends with the run. Nil before begin: no recheck starts.
	base    context.Context
	stopRun context.CancelFunc
	cur     *recheckRun // the running recheck, if any
	pending []HumanInput
	wake    chan struct{}
	warned  bool // the one store-error warning of the run was logged
}

// recheckRun is one running recheck; its identity tells it from a later one.
type recheckRun struct{ cancel context.CancelFunc }

func newSymptomGuard(w clients.WatchStatusReader, pipeline string, log *slog.Logger, now func() time.Time, sleep func(context.Context, time.Duration) error) *symptomGuard {
	if log == nil {
		log = slog.Default()
	}
	return &symptomGuard{watch: w, pipeline: pipeline, log: log, now: now, sleep: sleep, after: watchRecheckAfter, wake: make(chan struct{}, 1)}
}

// row reads the pipeline's status row: ok is false when there is none, it is
// stale, or the read failed (all pass).
func (s *symptomGuard) row(ctx context.Context) (clients.WatchStatus, bool) {
	rctx, cancel := context.WithTimeout(ctx, symptomReadTimeout)
	defer cancel()
	rows, err := s.watch.WatchStatuses(rctx, []string{s.pipeline})
	if err != nil {
		s.mu.Lock()
		first := !s.warned
		s.warned = true
		s.mu.Unlock()
		if first {
			s.log.Warn("agent: hypothesis guard: watch status unreadable; symptom rule passes", "pipeline", s.pipeline, "err", err)
		}
		return clients.WatchStatus{}, false
	}
	for _, r := range rows {
		if r.PipelineID != s.pipeline {
			continue
		}
		if s.now().Sub(r.CheckedAt) > watchRowMaxAge {
			return clients.WatchStatus{}, false
		}
		return r, true
	}
	return clients.WatchStatus{}, false
}

// blocking returns the row's symptom signals that still breach: firing or
// acked, and not clearing.
func blocking(row clients.WatchStatus) []clients.WatchSignal {
	var out []clients.WatchSignal
	for _, sig := range row.Signals {
		if !symptomSignals[sig.Signal] || sig.ClearingSince != nil {
			continue
		}
		if sig.State == clients.WatchFiring || sig.State == clients.WatchAcked {
			out = append(out, sig)
		}
	}
	return out
}

// check is rule 1 over one claim: nil passes. It reads the store only for a
// healthy claim.
func (s *symptomGuard) check(ctx context.Context, text string, offered func(string) bool) *guardFinding {
	if s == nil || !healthyClaim(text) {
		return nil
	}
	row, ok := s.row(ctx)
	if !ok {
		return nil
	}
	sigs := blocking(row)
	if len(sigs) == 0 {
		return nil
	}
	names := make([]string, 0, len(sigs))
	for _, sig := range sigs {
		names = append(names, sig.Signal)
	}
	return &guardFinding{
		rule:     guardRuleSymptom,
		refusal:  symptomRefusal(s.pipeline, row.CheckedAt, s.now(), sigs, offered),
		attrs:    []any{"signals", names},
		note:     fmt.Sprintf("[guard] healthy verdict accepted after %s refusals; the watcher still shows %s firing (last check %s)", countWord(guardMaxRefusals), andList(names), clockText(row.CheckedAt)),
		onRefuse: func() { s.arm(names) },
	}
}

// symptomRefusal is what the model reads: the signals, where, since when,
// how old the watcher's view is, and which reads would show the symptom.
func symptomRefusal(pipeline string, checked, now time.Time, sigs []clients.WatchSignal, offered func(string) bool) error {
	listed := sigs
	if len(listed) > maxSymptomSignalsListed {
		listed = listed[:maxSymptomSignalsListed]
	}
	parts := make([]string, 0, len(listed))
	var reads []string
	for _, sig := range listed {
		var where []string
		if sig.Node != "" {
			where = append(where, "node "+sanitizeLabel(cutBytes(sig.Node, 80)))
		}
		if sig.Severity != "" {
			where = append(where, sanitizeLabel(cutBytes(sig.Severity, 8)))
		}
		state := "firing"
		if sig.State == clients.WatchAcked {
			state = "acked and still breaching"
		}
		if !sig.Since.IsZero() {
			state += " since " + clockText(sig.Since)
		}
		where = append(where, state)
		parts = append(parts, fmt.Sprintf("%s (%s)", sig.Signal, strings.Join(where, ", ")))
		for _, t := range signalReadTools[sig.Signal] {
			if !contains(reads, t) {
				reads = append(reads, t)
			}
		}
	}
	if len(sigs) > len(listed) {
		parts = append(parts, fmt.Sprintf("%d more", len(sigs)-len(listed)))
	}
	reads = offeredTools(reads, offered)
	age := now.Sub(checked).Round(time.Second)
	if age < 0 {
		age = 0
	}
	fix := "Re-read what shows it"
	if len(reads) > 0 {
		fix = "Re-read " + andList(reads)
	}
	return fmt.Errorf("%w: hypothesis guard (%s): a healthy verdict is blocked: the watcher still sees %s on pipeline %s (last check %s, %s ago). Your reads do not explain why it still fires. %s and compare with the alert's numbers, or state what still fails and who owns it. If a newer read shows it recovered, say it is recovering and the watcher has not cleared yet.",
		tools.ErrBadInput, guardRuleSymptom, strings.Join(parts, "; "), sanitizeLabel(cutBytes(pipeline, 80)), clockText(checked), age, fix)
}

// begin binds rechecks to the run's context; stop ends them with the run.
func (s *symptomGuard) begin(ctx context.Context) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.base, s.stopRun = context.WithCancel(ctx)
}

// stop cancels a running recheck and waits for it.
func (s *symptomGuard) stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.stopRun != nil {
		s.stopRun()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

// arm starts the recheck after a refusal, unless one runs already. It reads
// the row at each of s.after and, the first time none of signals (nor any
// other symptom) still breaches, queues one watch_cleared input and wakes the
// agent. Three reads that still see the symptom end it without a turn.
func (s *symptomGuard) arm(signals []string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.base == nil || s.base.Err() != nil || s.cur != nil {
		return
	}
	ctx, cancel := context.WithCancel(s.base)
	run := &recheckRun{cancel: cancel}
	s.cur = run
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer cancel()
		in, ok := s.recheck(ctx, signals)
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.cur == run {
			s.cur = nil
		}
		if ctx.Err() != nil {
			return
		}
		if ok {
			s.pending = append(s.pending, in)
		}
		// A token without a pending input only ends the idle wait's watch
		// for this recheck: waitForInput drains nothing and waits on the
		// Inbox alone (a spurious wake is harmless there).
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}()
}

// recheck is the body of one armed recheck.
func (s *symptomGuard) recheck(ctx context.Context, signals []string) (HumanInput, bool) {
	var waited time.Duration
	for _, at := range s.after {
		if err := s.sleep(ctx, at-waited); err != nil {
			return HumanInput{}, false
		}
		waited = at
		row, ok := s.row(ctx)
		if !ok || len(blocking(row)) > 0 {
			continue
		}
		return HumanInput{Kind: kindWatchCleared, Text: clearedText(row, signals), At: s.now()}, true
	}
	return HumanInput{}, false
}

// clearedText is the runner's own words to the model: signal names and
// times only, nothing from a tool or a person.
func clearedText(row clients.WatchStatus, signals []string) string {
	clearing := map[string]time.Time{}
	for _, sig := range row.Signals {
		if sig.ClearingSince != nil {
			clearing[sig.Signal] = *sig.ClearingSince
		}
	}
	parts := make([]string, 0, len(signals))
	for _, name := range signals {
		if at, ok := clearing[name]; ok {
			parts = append(parts, name+" is clearing since "+clockText(at))
		} else {
			parts = append(parts, name+" is no longer firing")
		}
	}
	return fmt.Sprintf("[watch] %s (watcher check %s). The hypothesis guard refused your healthy verdict while it fired; read the status again and give your verdict.", strings.Join(parts, "; "), clockText(row.CheckedAt))
}

// settle drops a queued wake and stops a running recheck: a healthy verdict
// was accepted, so the wake has nothing left to ask.
func (s *symptomGuard) settle() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = nil
	if s.cur != nil {
		s.cur.cancel()
		s.cur = nil
	}
}

// take returns the queued watch_cleared inputs and forgets them.
func (s *symptomGuard) take() []HumanInput {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.pending
	s.pending = nil
	return out
}

// active reports whether a recheck runs or a wake waits to be taken, so the
// idle wait listens for it.
func (s *symptomGuard) active() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur != nil || len(s.pending) > 0
}

// wakeChan has a token when a recheck queued a wake; nil (never ready) for a
// nil guard.
func (s *symptomGuard) wakeChan() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.wake
}

// symptomCheck is rule 1 as a guard check func over text.
func (g *hypothesisGuard) symptomCheck(ctx context.Context, text string) func() *guardFinding {
	return func() *guardFinding {
		if g == nil {
			return nil
		}
		return g.symptom.check(ctx, text, g.offered)
	}
}

// healthyAccepted ends a recheck once a healthy verdict went in.
func (g *hypothesisGuard) healthyAccepted(text string) {
	if g == nil || g.symptom == nil || !healthyClaim(text) {
		return
	}
	g.symptom.settle()
}

// symptomWatch is the run's rule 1 state, nil when the rule is off.
func (g *hypothesisGuard) symptomWatch() *symptomGuard {
	if g == nil {
		return nil
	}
	return g.symptom
}

// clockText renders t as UTC "15:04:05Z".
func clockText(t time.Time) string { return t.UTC().Format("15:04:05") + "Z" }

// countWord spells a small count.
func countWord(n int) string {
	if w, ok := map[int]string{1: "one", 2: "two", 3: "three"}[n]; ok {
		return w
	}
	return fmt.Sprint(n)
}

// andList renders "a", "a and b", "a, b and c".
func andList(xs []string) string {
	if len(xs) <= 1 {
		return strings.Join(xs, "")
	}
	return strings.Join(xs[:len(xs)-1], ", ") + " and " + xs[len(xs)-1]
}
