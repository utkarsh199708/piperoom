package watch

import (
	"context"
	"slices"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// Watch status rows (X5a): at the end of each tick the watcher writes one
// clients.WatchStatus per pipeline it looked at, so the fleet view
// (GET /api/fleet) on any replica reads the store instead of this process.
// Signal names, states, severities, node ids and times only: never evidence
// values.

// buildStatusesLocked turns the state of each target into its status row.
// Caller holds w.mu.
func (w *Watcher) buildStatusesLocked(now time.Time, ts []target) []clients.WatchStatus {
	out := make([]clients.WatchStatus, 0, len(ts))
	for _, t := range ts {
		ps := w.pipes[t.p.ID]
		st := clients.WatchStatus{PipelineID: t.p.ID, CheckedAt: now, Signals: []clients.WatchSignal{}}
		if ps != nil {
			for _, id := range ps.ids() {
				ss := ps.sigs[id]
				if ss == nil || ss.phase == phaseOK {
					continue
				}
				st.Signals = append(st.Signals, statusSignal(now, id, ss))
			}
		}
		// Worst first, then oldest, so the cap keeps what matters.
		slices.SortStableFunc(st.Signals, func(a, b clients.WatchSignal) int {
			if d := sevRank(a.Severity) - sevRank(b.Severity); d != 0 {
				return d
			}
			return a.Since.Compare(b.Since)
		})
		if len(st.Signals) > clients.MaxWatchSignals {
			st.Signals = st.Signals[:clients.MaxWatchSignals]
		}
		out = append(out, st)
	}
	return out
}

// statusSignal is the status entry of one firing or acked state machine.
func statusSignal(now time.Time, id string, ss *sigState) clients.WatchSignal {
	name := signalName(id)
	state := clients.WatchFiring
	if ss.phase == phaseAcked {
		state = clients.WatchAcked
	}
	sev := ss.breachSev
	if sev == "" {
		sev = ss.last.sev
	}
	if r := sevRank(sev); r < 1 || r > 4 {
		sev = events.SignalSeverity(name)
		if sev == "" {
			sev = "sev4"
		}
	}
	since := ss.breachingSince
	if since.IsZero() {
		since = now
	}
	node := ss.last.node
	if node == "" {
		node = signalNode(id, ss.last.ev)
	}
	g := clients.WatchSignal{Signal: name, State: state, Severity: sev, Since: since, Node: node}
	// The clear timer runs while the latest good reading is clear; a breach or
	// a hold resets it and an unknown reading leaves it alone (sigState.step),
	// so this is the same notion the state machine resolves on.
	if !ss.clearingSince.IsZero() {
		c := ss.clearingSince
		g.ClearingSince = &c
	}
	if m := forecastMetric(id); m != "" {
		g.Metric = m
		if s, ok := ss.last.ev["eta"].(string); ok {
			if eta, err := time.Parse(time.RFC3339, s); err == nil {
				g.ETA = &eta
			}
		}
	}
	return g
}

// signalName is the public signal name of a state-machine id: the forecasts,
// drift checks and config_drift keep one id per metric, table or connector.
func signalName(id string) string {
	switch {
	case forecastMetric(id) != "":
		return events.SignalForecast
	case driftKind(id) != "":
		return driftKind(id)
	case configDriftConnector(id) != "":
		return events.SignalConfigDrift
	case id == retentionID:
		return owners.SignalConfigRisk
	}
	return id
}

// signalNode names the topology node a signal is about, as
// GET /api/pipelines/{pid}/topology names it, from the names the evidence
// already carries; "" when there is none (a slot or the database size has no
// node of its own).
func signalNode(id string, ev map[string]any) string {
	if c := configDriftConnector(id); c != "" {
		return "connect:" + c
	}
	str := func(m map[string]any, k string) string { s, _ := m[k].(string); return s }
	prefixed := func(prefix, name string) string {
		if name == "" {
			return ""
		}
		return prefix + name
	}
	// A drift id names its object: the source table (count_drift,
	// row_drift) or the CDC topic (dup_keys, order_skew).
	if k := driftKind(id); k != "" {
		_, object, _ := strings.Cut(id, ":")
		if k == events.SignalCountDrift || k == events.SignalRowDrift {
			return prefixed("pg:", object)
		}
		return prefixed("topic:", object)
	}
	if m := forecastMetric(id); m != "" {
		if m == clients.WatchMetricTopicOldestUnreadAge {
			return prefixed("topic:", str(ev, "object"))
		}
		return ""
	}
	switch id {
	case owners.SignalConnectorDown:
		if first := firstEntry(ev["connectors"]); first != nil {
			return prefixed("connect:", str(first, "name"))
		}
	case owners.SignalConfigRisk:
		if first := firstEntry(ev["risks"]); first != nil {
			return prefixed("connect:", str(first, "connector"))
		}
	case owners.SignalDLQGrowth:
		return prefixed("dlq:", str(ev, "topic"))
	case owners.SignalConsumerLag, owners.SignalGroupUnstable, owners.SignalOffsetBelowLogStart:
		return prefixed("group:", str(ev, "group"))
	}
	return ""
}

// firstEntry is the first object of an evidence list, built in process
// ([]map[string]any) or decoded from JSON ([]any).
func firstEntry(v any) map[string]any {
	switch l := v.(type) {
	case []map[string]any:
		if len(l) > 0 {
			return l[0]
		}
	case []any:
		if len(l) > 0 {
			m, _ := l[0].(map[string]any)
			return m
		}
	}
	return nil
}

// writeStatuses stores the tick's status rows, each write bounded by the
// check timeout. A failed write is logged and never stops the tick; the next
// tick replaces the row anyway.
func (w *Watcher) writeStatuses(ctx context.Context, rows []clients.WatchStatus) {
	if w.status == nil {
		return
	}
	failed := 0
	var firstErr error
	for _, st := range rows {
		if ctx.Err() != nil {
			return
		}
		sctx, cancel := context.WithTimeout(ctx, w.cfg.CheckTimeout)
		err := w.status.PutWatchStatus(sctx, st)
		cancel()
		if err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if failed > 0 && ctx.Err() == nil {
		w.log.Warn("watch: status rows not written", "failed", failed, "pipelines", len(rows), "err", firstErr)
	}
}
