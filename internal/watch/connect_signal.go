package watch

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// SignalConnectUnreachable is the connect_unreachable signal (QA S7): the
// Kafka Connect REST API failed every connector status read of the pipeline
// connectUnreachableTicks ticks in a row. Before it, a dead Connect worker
// only logged "check failed, signal state unchanged": connector_down cannot
// tell a failed connector from no answer, so it stays unknown. A 404 is an
// answer (connector_down reports it as missing), and no Connect client at all
// is a configuration gap, not an outage; neither counts. It follows
// connector_down's enabled, action and `for` (the clear time).
const SignalConnectUnreachable = "connect_unreachable"

// connectUnreachableTicks is how many failed ticks in a row fire it: one or
// two failures are a restart or a rebalance.
const connectUnreachableTicks = 3

// connectReading updates the pipeline's failure count from this tick and
// returns the connect_unreachable reading. ok is false when the tick says
// nothing (no connectors, no client, a connector not read). Caller holds w.mu.
func connectReading(now time.Time, t target, ps *pipeState, obs *observation) (reading, bool) {
	if len(t.connectors) == 0 || t.conn == nil {
		return reading{}, false
	}
	var firstErr error
	for _, name := range t.connectors {
		co := obs.conns[name]
		switch {
		case co == nil:
			return reading{}, false
		case co.statusErr == nil, errors.Is(co.statusErr, clients.ErrNotFound):
			ps.connFails, ps.connFailSince = 0, time.Time{}
			return reading{v: vClear, value: "answering", threshold: "answering"}, true
		case errors.Is(co.statusErr, errNoConnect):
			return reading{}, false
		case firstErr == nil:
			firstErr = co.statusErr
		}
	}
	if ps.connFails == 0 {
		ps.connFailSince = now
	}
	ps.connFails++
	kind := connectErrKind(firstErr)
	th := fmt.Sprintf("%d failed checks in a row", connectUnreachableTicks)
	if ps.connFails < connectUnreachableTicks {
		return reading{v: vHold, value: fmt.Sprintf("%d failed checks (%s)", ps.connFails, kind), threshold: th}, true
	}
	down := now.Sub(ps.connFailSince).Round(time.Second)
	return reading{
		v: vBreachNow, sev: "sev2",
		label:     fmt.Sprintf("Kafka Connect not answering for %s (%d checks, %s)", fmtDur(down), ps.connFails, kind),
		value:     fmt.Sprintf("%d failed checks (%s)", ps.connFails, kind),
		threshold: th,
		// The error text holds URLs and addresses: the evidence keeps its kind only.
		ev: map[string]any{
			"signal": SignalConnectUnreachable, "severity": "sev2", "failed_checks": ps.connFails,
			"error_kind": kind, "unreachable_since": ps.connFailSince.UTC().Format(time.RFC3339),
			"connectors_checked": len(t.connectors),
		},
	}, true
}

// connectErrKind sorts a Connect error into timeout, network or error (an
// HTTP error status or anything else).
func connectErrKind(err error) string {
	var ne net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.As(err, &ne):
		if ne.Timeout() {
			return "timeout"
		}
		return "network"
	}
	return "error"
}

// connectUnreachableCfg is the connect_unreachable setting: connector_down's.
func connectUnreachableCfg(cfg owners.WatchConfig) owners.WatchSignal { return cfg.ConnectorDown }
