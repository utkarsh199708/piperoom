package dbtools

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// Verdicts of pg_streaming_liveness. "streaming", "quiet" and
// "not_confirmed" are not faults: a quiet source has nothing to send, and a
// flat flush with a head−confirmed_flush lag that one worker offset flush
// could still explain cannot show a stall. Only "stuck" is a fault.
const (
	verdictStreaming = "streaming"
	verdictStuck     = "stuck"
	verdictQuiet     = "quiet"
	// verdictNotConfirmed means the source WAL head advanced while
	// confirmed_flush_lsn stayed flat, but the head−confirmed_flush lag is
	// still small enough that one worker offset flush could explain it
	// (Debezium confirms offsets only when Kafka Connect commits them), so
	// the window is too short to call the slot stuck. It is an explicit
	// unknown, never a fault.
	verdictNotConfirmed = "not_confirmed"
	// verdictUnknown means the reads did not carry enough to judge (a slot
	// without a comparable LSN, a slot that appeared or disappeared between the
	// reads). It is never reported as healthy.
	verdictUnknown = "unknown"
)

const (
	livenessDefaultWindowSeconds = 5
	// livenessMaxWindowSeconds keeps the two reads and the wait inside the
	// tool's 10 s deadline even when the source answers slowly.
	livenessMaxWindowSeconds = 8

	// workerOffsetFlushDefault is Kafka Connect's default WORKER
	// offset.flush.interval.ms (60 s). It is a worker property, unreadable
	// through the connector REST API, so the tool never reads it from a
	// connector config (a value put there by mistake is ignored by Connect
	// and must not be trusted here); it only scales the stuck-lag threshold.
	workerOffsetFlushDefault = 60 * time.Second

	// stuckLagFlushIntervals and stuckLagFloorBytes set the stuck threshold:
	// a flat confirmed_flush_lsn while the head moves is "stuck" only when
	// the head−confirmed_flush lag is growing and larger than
	// max(stuckLagFloorBytes,
	//     stuckLagFlushIntervals × observed write rate × workerOffsetFlushDefault).
	// A lag that one worker offset flush could explain, with generous margin,
	// never trips it, and the floor keeps quiet-ish sources from tripping on
	// noise.
	stuckLagFlushIntervals = 4
	stuckLagFloorBytes     = int64(16 << 20) // 16 MiB
)

// pgStreamingLivenessTool answers whether the source Postgres is actually
// streaming into Debezium, which connector/task status alone cannot show: a
// connector can be RUNNING while its slot stopped advancing (a long
// transaction, a publication that excludes the table, a broken walsender).
//
// It samples the pipeline's replication slot twice, window_seconds apart, and
// compares confirmed_flush_lsn with the server's WAL head. The head is
// reconstructed from the contract's ReplicationSlot fields
// (restart_lsn + retained_wal_bytes = pg_current_wal_lsn() at that read), so no
// JMX/Debezium-metrics endpoint is needed; the Debezium metrics tool is the
// other, optional source when DEBEZIUM_METRICS_URL is configured.
func pgStreamingLivenessTool(d Deps) tools.Tool {
	return &tool{
		name: "pg_streaming_liveness",
		desc: "Check whether the source Postgres is actually streaming to Debezium: sample the pipeline's replication slot confirmed_flush_lsn twice, window_seconds apart, and compare it with the source WAL head. " +
			"Returns verdict streaming (the slot advanced), stuck (confirmed_flush_lsn stayed flat while the source wrote AND the head-confirmed_flush lag is growing and larger than one Kafka Connect worker offset flush could explain: above max(16 MiB, 4 x the observed write rate x the 60 s worker offset.flush.interval.ms default)), not_confirmed (flat flush with a lag below that threshold: Debezium may simply not have been due to commit offsets yet, not a fault) or quiet (no source writes; not a fault). " +
			"A quiet reading with an inactive slot, or a stuck reading, is not healthy. Use it when a connector or its tasks are RUNNING but the sink is stale or no events arrive; debezium_metrics, when DEBEZIUM_METRICS_URL is set, is the connector-side view, and this check needs no JMX. " +
			"stuck also names the oldest open writer transaction: a long transaction, or a write outside the publication, holds the slot back without the connector being broken.",
		schema: objSchema(map[string]any{
			"pipeline_id":    strProp("Pipeline id from owners.yaml."),
			"window_seconds": intProp("Seconds between the two slot reads, 1-8. Default 5."),
		}, "pipeline_id"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				PipelineID    string `json:"pipeline_id"`
				WindowSeconds *int   `json:"window_seconds"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if input.PipelineID == "" {
				return tools.Result{}, fmt.Errorf("%w: pipeline_id is required", tools.ErrBadInput)
			}
			window, err := livenessWindow(input.WindowSeconds)
			if err != nil {
				return tools.Result{}, err
			}
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			p, err := d.resolve(sc, input.PipelineID)
			if err != nil {
				return tools.Result{}, err
			}
			if d.Src == nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_streaming_liveness: the source Postgres client is not configured, so streaming cannot be confirmed")
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()

			names, err := d.pipelineSlots(ctx, sc, p)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_streaming_liveness: %w", err)
			}
			if len(names) == 0 {
				return tools.Result{}, fmt.Errorf("dbtools: pg_streaming_liveness: verdict unknown: pipeline %q has no Debezium Postgres source connector, so it has no replication slot and streaming cannot be confirmed", p.ID)
			}

			// Read the connector configs before the wait, with the full tool
			// deadline left; they only NAME the measured connector. No worker
			// property (offset.flush.interval.ms) is read from them.
			slotsByConnector := d.connectorSlots(ctx, p)

			started := time.Now()
			before, _, err := d.namedSlots(ctx, names)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_streaming_liveness: %w", err)
			}
			if err := d.waitFor(ctx, window); err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_streaming_liveness: waiting %s for the second read: %w", topology.HumanAge(window), err)
			}
			after, _, err := d.namedSlots(ctx, names)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_streaming_liveness: %w", err)
			}
			elapsed := time.Since(started)

			beforeSlot, beforeOK := findSlot(names, before)
			afterSlot, afterOK := findSlot(names, after)
			s0, s1, ok := findSlotPair(names, before, after)
			measured := ""
			switch {
			case ok:
				measured = s0.Name
			case beforeOK:
				measured = beforeSlot.Name
			case afterOK:
				measured = afterSlot.Name
			}
			connector := measuredConnector(p, slotsByConnector, measured)

			var sl slotLiveness
			var verdict, note string
			switch {
			case ok:
				sl, verdict, note = evaluateSlot(s0, s1, window)
			case !beforeOK && !afterOK:
				return tools.Result{}, noSlotError(p, names)
			case beforeOK:
				sl = slotLivenessOf(beforeSlot)
				verdict = verdictUnknown
				note = fmt.Sprintf("slot %q disappeared during the %s check; it was dropped while the source was read", beforeSlot.Name, topology.HumanAge(window))
			default:
				sl = slotLivenessOf(afterSlot)
				verdict = verdictUnknown
				note = fmt.Sprintf("slot %q appeared during the %s check, so it has no earlier position to compare", afterSlot.Name, topology.HumanAge(window))
			}
			rep := livenessReport{
				Pipeline:      p.ID,
				Verdict:       verdict,
				Note:          note,
				Source:        "replication slot LSN (confirmed_flush_lsn vs restart_lsn + retained WAL)",
				WindowSeconds: int(window / time.Second),
				ElapsedMS:     elapsed.Milliseconds(),
				Slot:          sl,
			}
			rep.Connector = connector
			rep.ConnectorState, rep.ConnectorTasks, rep.ConnectorStatusError = d.connectorStatus(ctx, connector)
			if verdict == verdictStuck {
				d.explainStuck(ctx, sc, p, &rep)
			}

			summary, truncated := tools.Truncate(livenessSummary(rep))
			return tools.Result{Summary: summary, Data: rep, Truncated: truncated}, nil
		},
	}
}

// livenessWindow applies the window_seconds rules: missing is the default,
// a value outside 1..8 is refused rather than clamped, because the model would
// read the answer as covering the window it asked for.
func livenessWindow(v *int) (time.Duration, error) {
	if v == nil {
		return livenessDefaultWindowSeconds * time.Second, nil
	}
	if *v < 1 || *v > livenessMaxWindowSeconds {
		return 0, fmt.Errorf("%w: window_seconds must be between 1 and %d", tools.ErrBadInput, livenessMaxWindowSeconds)
	}
	return time.Duration(*v) * time.Second, nil
}

// pipelineSlots returns the replication slot names that belong to p, by the
// watcher's rule. Scoped calls use Scope.Slots so the rule stays in one place;
// unscoped calls (CLI/dev) apply the same rule to the resolved pipeline.
func (d Deps) pipelineSlots(ctx context.Context, sc tools.Scope, p owners.Pipeline) ([]string, error) {
	if !sc.Unscoped() {
		return sc.Slots(ctx, d.Connect)
	}
	if slot := p.Watch().Slot; slot != "" {
		return []string{slot}, nil
	}
	if len(p.Connectors) == 0 {
		return nil, nil
	}
	if d.Connect == nil {
		return nil, fmt.Errorf("pipeline %q: no Connect client to read the connector configs, and owners.yaml sets no watch.slot", p.ID)
	}
	var out []string
	for _, name := range p.Connectors {
		cfg, err := d.Connect.Config(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("pipeline %q: read connector %q config to find its slot (set watch.slot in owners.yaml to name it without Connect): %w", p.ID, name, err)
		}
		slot, err := owners.ConnectorSlot(cfg)
		if err != nil {
			return nil, fmt.Errorf("pipeline %q: connector %q has an invalid slot.name: %w", p.ID, name, err)
		}
		if slot != "" && !containsString(out, slot) {
			out = append(out, slot)
		}
	}
	return out, nil
}

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// findSlot returns the first of names present in slots. names comes from the
// owners config or the connector config, never from tool input.
func findSlot(names []string, slots []clients.ReplicationSlot) (clients.ReplicationSlot, bool) {
	for _, n := range names {
		for _, s := range slots {
			if s.Name == n {
				return s, true
			}
		}
	}
	return clients.ReplicationSlot{}, false
}

// findSlotPair returns the same slot at both reads, so two reads of a
// pipeline with several slots can never be compared to each other.
func findSlotPair(names []string, before, after []clients.ReplicationSlot) (clients.ReplicationSlot, clients.ReplicationSlot, bool) {
	for _, n := range names {
		b, ok := slotByName(n, before)
		if !ok {
			continue
		}
		a, ok := slotByName(n, after)
		if !ok {
			continue
		}
		return b, a, true
	}
	return clients.ReplicationSlot{}, clients.ReplicationSlot{}, false
}

func slotByName(name string, slots []clients.ReplicationSlot) (clients.ReplicationSlot, bool) {
	for _, s := range slots {
		if s.Name == name {
			return s, true
		}
	}
	return clients.ReplicationSlot{}, false
}

// noSlotError names the connector a configured slot should have come from, so
// the model reports "no slot" instead of "healthy".
func noSlotError(p owners.Pipeline, names []string) error {
	looked := make([]string, len(names))
	for i, n := range names {
		looked[i] = strconv.Quote(n)
	}
	switch len(p.Connectors) {
	case 0:
		return fmt.Errorf("dbtools: pg_streaming_liveness: verdict unknown: no slot for pipeline %q (no replication slot named %s in pg_replication_slots)", p.ID, strings.Join(looked, ", "))
	case 1:
		return fmt.Errorf("dbtools: pg_streaming_liveness: verdict unknown: no slot for connector %q (pipeline %q; no replication slot named %s in pg_replication_slots)",
			p.Connectors[0], p.ID, strings.Join(looked, ", "))
	default:
		return fmt.Errorf("dbtools: pg_streaming_liveness: verdict unknown: no slot for the connectors of pipeline %q (connectors %s; no replication slot named %s in pg_replication_slots)",
			p.ID, strings.Join(p.Connectors, ", "), strings.Join(looked, ", "))
	}
}

// waitFor blocks for dur or until ctx ends. Deps.wait lets tests replace the
// sleep.
func (d Deps) waitFor(ctx context.Context, dur time.Duration) error {
	if d.wait != nil {
		return d.wait(ctx, dur)
	}
	t := time.NewTimer(dur)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// slotLivenessOf is the report entry for one read, used when the two reads
// cannot be compared.
func slotLivenessOf(s clients.ReplicationSlot) slotLiveness {
	return slotLiveness{
		Name: s.Name, Active: s.Active, WALStatus: s.WALStatus,
		RetainedWALBytes:  s.RetainedWALBytes,
		ConfirmedFlushLSN: s.ConfirmedFlushLSN,
		Verdict:           verdictUnknown,
	}
}

// evaluateSlot compares one slot at two reads. The verdict is:
//   - streaming when confirmed_flush_lsn advanced;
//   - stuck when it did not move while the source WAL head advanced, AND the
//     head−confirmed_flush lag is old and growing: larger at the second read
//     than the first, and above a threshold that one worker offset flush
//     cannot explain (Debezium only confirms offsets when Kafka Connect
//     commits them, about every worker offset.flush.interval.ms — a worker
//     property this tool cannot read, so the threshold is scaled from
//     Connect's 60 s default; see stuckLagThreshold);
//   - not_confirmed when it did not move while the head advanced, but the lag
//     is below that bar: too early to call it a stall. A quiet capture set
//     with no heartbeat (only other tables or databases moving the head)
//     lands here, never at stuck;
//   - quiet when neither moved (the source had nothing to send);
//   - unknown, with a note, when the reads do not carry a comparable position.
//     A slot that cannot be judged is never reported as healthy.
func evaluateSlot(before, after clients.ReplicationSlot, window time.Duration) (slotLiveness, string, string) {
	sl := slotLivenessOf(before)
	sl.Active = after.Active
	sl.WALStatus = after.WALStatus
	sl.RetainedWALBytes = after.RetainedWALBytes
	sl.ConfirmedFlushLSNAfter = after.ConfirmedFlushLSN

	flush0, err := parseLSN(before.ConfirmedFlushLSN)
	if err != nil {
		return sl, verdictUnknown, fmt.Sprintf("slot %q: confirmed_flush_lsn: %v", before.Name, err)
	}
	flush1, err := parseLSN(after.ConfirmedFlushLSN)
	if err != nil {
		return sl, verdictUnknown, fmt.Sprintf("slot %q: confirmed_flush_lsn: %v", after.Name, err)
	}
	if flush1 < flush0 {
		return sl, verdictUnknown, fmt.Sprintf("slot %q: confirmed_flush_lsn went backwards (%s -> %s)", before.Name, before.ConfirmedFlushLSN, after.ConfirmedFlushLSN)
	}
	sl.ConfirmedFlushAdvanceBytes = int64(flush1 - flush0)
	if flush1 > flush0 {
		sl.Verdict = verdictStreaming
		return sl, verdictStreaming, ""
	}

	// confirmed_flush_lsn is flat. The server's WAL head is
	// restart_lsn + retained WAL in the same slot row, so a growing head means
	// the source wrote while the slot did not consume.
	head0, err := slotWALHead(before)
	if err != nil {
		return sl, verdictUnknown, fmt.Sprintf("slot %q: %v", before.Name, err)
	}
	head1, err := slotWALHead(after)
	if err != nil {
		return sl, verdictUnknown, fmt.Sprintf("slot %q: %v", after.Name, err)
	}
	if head1 < head0 {
		return sl, verdictUnknown, fmt.Sprintf("slot %q: the source WAL head went backwards (%d -> %d bytes)", before.Name, head0, head1)
	}
	sl.SourceWALAdvanceBytes = int64(head1 - head0)
	if head1 > head0 {
		lag0 := lagBytes(head0, flush0)
		lag1 := lagBytes(head1, flush1)
		threshold := stuckLagThreshold(sl.SourceWALAdvanceBytes, window)
		sl.FlushLagBytes = lag0
		sl.FlushLagBytesAfter = lag1
		sl.StuckLagThresholdBytes = threshold
		if lag1 > lag0 && lag1 > threshold {
			sl.Verdict = verdictStuck
			note := fmt.Sprintf("the head-confirmed_flush lag grew from %s to %s in %s and is above the stuck threshold of %s (max of %s and %d x the observed write rate x the 60 s Kafka Connect worker offset-flush default), which one worker offset flush cannot explain",
				topology.HumanBytes(lag0), topology.HumanBytes(lag1), topology.HumanAge(window),
				topology.HumanBytes(threshold), topology.HumanBytes(stuckLagFloorBytes), stuckLagFlushIntervals)
			return sl, verdictStuck, note
		}
		sl.Verdict = verdictNotConfirmed
		note := fmt.Sprintf("confirmed_flush_lsn did not move in %s while the source wrote %d bytes, but the head-confirmed_flush lag (%s) is below the stuck threshold of %s (max of %s and %d x the observed write rate x the 60 s Kafka Connect worker offset-flush default, a worker property this tool cannot read), so one worker offset flush could still explain it: the window is too short to call the slot stuck",
			topology.HumanAge(window), sl.SourceWALAdvanceBytes, topology.HumanBytes(lag1),
			topology.HumanBytes(threshold), topology.HumanBytes(stuckLagFloorBytes), stuckLagFlushIntervals)
		return sl, verdictNotConfirmed, note
	}
	sl.Verdict = verdictQuiet
	return sl, verdictQuiet, ""
}

// lagBytes is the signed distance from confirmed_flush_lsn to the WAL head.
func lagBytes(head, flush uint64) int64 {
	if head >= flush {
		return int64(head - flush)
	}
	return -int64(flush - head)
}

// stuckLagThreshold is the smallest head−confirmed_flush lag that one worker
// offset flush cannot explain: stuckLagFlushIntervals flush intervals' worth
// of WAL at the write rate observed over the window, with stuckLagFloorBytes
// as a floor so quiet-ish sources do not trip on noise.
// offset.flush.interval.ms is a Kafka Connect WORKER property, unreadable
// through the connector REST API, so Connect's 60 s worker default is the
// scale (workerOffsetFlushDefault); a worker flushing more often only makes
// the threshold more conservative.
func stuckLagThreshold(headAdvance int64, window time.Duration) int64 {
	if window <= 0 {
		window = time.Second
	}
	rate := float64(headAdvance) / window.Seconds()
	t := int64(float64(stuckLagFlushIntervals) * rate * workerOffsetFlushDefault.Seconds())
	if t < stuckLagFloorBytes {
		return stuckLagFloorBytes
	}
	return t
}

// slotWALHead reconstructs the server's WAL position from a slot row:
// pg_wal_lsn_diff(now(), restart_lsn) is the retained WAL size, so
// restart_lsn + retained = the head at that read.
func slotWALHead(s clients.ReplicationSlot) (uint64, error) {
	restart, err := parseLSN(s.RestartLSN)
	if err != nil {
		return 0, fmt.Errorf("restart_lsn: %w", err)
	}
	if s.RetainedWALBytes < 0 {
		return 0, fmt.Errorf("retained WAL size is negative (%d)", s.RetainedWALBytes)
	}
	return restart + uint64(s.RetainedWALBytes), nil
}

// parseLSN parses PostgreSQL's text LSN ("X/Y", two 32-bit hex parts) into a
// byte position. An empty value (a physical slot, or a slot that never
// confirmed a position) is an error: nothing can be compared.
func parseLSN(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	hiStr, loStr, ok := strings.Cut(s, "/")
	if !ok || hiStr == "" || loStr == "" || len(hiStr) > 8 || len(loStr) > 8 {
		return 0, fmt.Errorf("%q is not a PostgreSQL LSN (want X/Y in hex)", cutShort(s, 40))
	}
	hi, err := strconv.ParseUint(hiStr, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("%q is not a PostgreSQL LSN: %w", cutShort(s, 40), err)
	}
	lo, err := strconv.ParseUint(loStr, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("%q is not a PostgreSQL LSN: %w", cutShort(s, 40), err)
	}
	return hi<<32 | lo, nil
}

// connectorSlots maps each of the pipeline's connectors to the slot its
// config names. It is read before the two slot reads, with the full tool
// deadline left, and only to NAME the measured connector in the report. No
// worker property is read from the connector config: offset.flush.interval.ms
// lives in the WORKER config, which the connector REST API does not expose,
// and a copy mistakenly set on the connector is ignored by Connect. A
// connector whose config cannot be read is simply absent from the map.
func (d Deps) connectorSlots(ctx context.Context, p owners.Pipeline) map[string]string {
	if d.Connect == nil || len(p.Connectors) == 0 {
		return nil
	}
	out := make(map[string]string, len(p.Connectors))
	for _, c := range p.Connectors {
		cfg, err := d.Connect.Config(ctx, c)
		if err != nil {
			continue
		}
		slot, err := owners.ConnectorSlot(cfg)
		if err != nil {
			continue
		}
		out[c] = slot
	}
	return out
}

// measuredConnector names the connector whose config names the measured slot;
// otherwise the pipeline's first connector.
func measuredConnector(p owners.Pipeline, slotsByConnector map[string]string, slot string) string {
	if len(p.Connectors) == 0 {
		return ""
	}
	if slot != "" {
		for _, c := range p.Connectors {
			if slotsByConnector[c] == slot {
				return c
			}
		}
	}
	return p.Connectors[0]
}

// connectorStatus reads connector's state. The liveness verdict comes from
// Postgres, so a Connect failure is reported as a note, not as a failed check;
// it never hides a reading.
func (d Deps) connectorStatus(ctx context.Context, connector string) (state string, tasks []string, note string) {
	if connector == "" {
		return "", nil, ""
	}
	if d.Connect == nil {
		return "", nil, "Connect is not configured, so the connector state is unknown"
	}
	st, err := d.Connect.Status(ctx, connector)
	if err != nil {
		return "", nil, "connector status unavailable: " + cutShort(oneLine(err.Error()), 200)
	}
	state = st.State
	for _, t := range st.Tasks {
		tasks = append(tasks, fmt.Sprintf("%d:%s", t.ID, t.State))
	}
	return state, tasks, ""
}

// oneLine collapses whitespace so an error line cannot add lines to a summary.
func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

// explainStuck adds the oldest open writer transaction, which holds a slot's
// confirmed_flush_lsn back without the connector being broken (a long
// transaction, or a writer outside the publication's tables). Scoped calls
// show only transactions on the pipeline's tables plus the anonymised oldest
// writer outside them, like pg_long_transactions.
func (d Deps) explainStuck(ctx context.Context, sc tools.Scope, p owners.Pipeline, rep *livenessReport) {
	if sc.Unscoped() {
		txns, err := d.Src.LongTransactions(ctx, 0)
		if err != nil {
			rep.TransactionNote = "transaction detail unavailable: " + cutShort(oneLine(err.Error()), 200)
			return
		}
		if t, ok := oldestWriter(txns); ok {
			lt := toLongTxn(t)
			rep.OldestTransaction = &lt
			return
		}
		rep.TransactionNote = "no open writer transaction was found; the slot is held back by something else"
		return
	}

	l, ok := d.Src.(tableTxnLister)
	if !ok {
		rep.TransactionNote = "the source client cannot filter sessions by table, so the oldest open transaction is not shown"
		return
	}
	tt, err := l.LongTransactionsOnTables(ctx, 0, p.SourceTables)
	if err != nil {
		rep.TransactionNote = "transaction detail unavailable: " + cutShort(oneLine(err.Error()), 200)
		return
	}
	if t, ok := oldestWriter(tt.Transactions); ok {
		lt := toLongTxn(t)
		rep.OldestTransaction = &lt
		return
	}
	if len(tt.OldestOtherWriters) > 0 {
		ow := anonWriter(tt.OldestOtherWriters[0])
		rep.OldestOtherWriter = &ow
		return
	}
	rep.TransactionNote = "no open writer transaction was found; the slot is held back by something else"
}

// oldestWriter returns the oldest transaction that holds an xid (only a
// writer holds the WAL horizon). The client lists oldest first.
func oldestWriter(txns []clients.Transaction) (clients.Transaction, bool) {
	for _, t := range txns {
		if t.HasXID {
			return t, true
		}
	}
	return clients.Transaction{}, false
}

// toLongTxn maps a client transaction onto the allow-listed tool shape (no
// query text, statement verb only).
func toLongTxn(t clients.Transaction) longTxn {
	return longTxn{
		PID: t.PID, User: t.Username, ApplicationName: t.ApplicationName, State: t.State,
		Age: topology.HumanAge(t.Age), AgeSeconds: int64(t.Age / time.Second),
		WaitEvent: t.WaitEvent, Verb: statementVerb(t.Query), HasXID: t.HasXID,
	}
}

// livenessReport is Data of pg_streaming_liveness.
type livenessReport struct {
	Pipeline string `json:"pipeline"`
	Verdict  string `json:"verdict"`
	// Note explains a stuck verdict (the lag and the threshold it crossed),
	// a not_confirmed verdict (why the reading is too early to judge), or why
	// a verdict is unknown.
	Note string `json:"note,omitempty"`
	// Source says which signal produced the verdict, so the model does not
	// claim Debezium metrics were read.
	Source        string       `json:"source"`
	WindowSeconds int          `json:"window_seconds"`
	ElapsedMS     int64        `json:"elapsed_ms"`
	Slot          slotLiveness `json:"slot"`
	// Connector is the connector whose slot was measured (the pipeline's first
	// connector when no config names the slot); its state is what makes
	// "RUNNING but stuck" visible.
	Connector            string   `json:"connector,omitempty"`
	ConnectorState       string   `json:"connector_state,omitempty"`
	ConnectorTasks       []string `json:"connector_tasks,omitempty"`
	ConnectorStatusError string   `json:"connector_status_error,omitempty"`
	// OldestTransaction explains a stuck verdict on this pipeline's tables;
	// OldestOtherWriter is an anonymised writer outside them (a writer on any
	// table holds back every logical slot).
	OldestTransaction *longTxn     `json:"oldest_transaction,omitempty"`
	OldestOtherWriter *otherWriter `json:"oldest_other_writer,omitempty"`
	TransactionNote   string       `json:"transaction_note,omitempty"`
}

// slotLiveness is one slot's measurement.
type slotLiveness struct {
	Name             string `json:"name"`
	Active           bool   `json:"active"`
	WALStatus        string `json:"wal_status,omitempty"`
	RetainedWALBytes int64  `json:"retained_wal_bytes"`
	// ConfirmedFlushLSN is the first read, ConfirmedFlushLSNAfter the second.
	ConfirmedFlushLSN          string `json:"confirmed_flush_lsn"`
	ConfirmedFlushLSNAfter     string `json:"confirmed_flush_lsn_after"`
	ConfirmedFlushAdvanceBytes int64  `json:"confirmed_flush_advance_bytes"`
	// SourceWALAdvanceBytes is how far the server's WAL head moved between the
	// reads (0 when nothing was written).
	SourceWALAdvanceBytes int64 `json:"source_wal_advance_bytes"`
	// FlushLagBytes / FlushLagBytesAfter are the head−confirmed_flush lag at
	// each read, and StuckLagThresholdBytes the lag above which a flat flush
	// with a moving head is "stuck":
	// max(16 MiB, 4 x observed write rate x the 60 s Kafka Connect worker
	// offset-flush default). Set only when the flush was flat while the head
	// moved (the two verdicts the threshold decides between).
	FlushLagBytes          int64  `json:"flush_lag_bytes,omitempty"`
	FlushLagBytesAfter     int64  `json:"flush_lag_bytes_after,omitempty"`
	StuckLagThresholdBytes int64  `json:"stuck_lag_threshold_bytes,omitempty"`
	Verdict                string `json:"verdict"`
}

// livenessSummary renders the verdict, the slot measurement, the connector
// state and (for stuck) the transaction that may explain it. Data keeps the
// full detail.
func livenessSummary(rep livenessReport) string {
	sl := rep.Slot
	window := topology.HumanAge(time.Duration(rep.WindowSeconds) * time.Second)
	var sb strings.Builder
	switch rep.Verdict {
	case verdictStreaming:
		fmt.Fprintf(&sb, "streaming: slot %q confirmed_flush_lsn advanced %d bytes in %s (%s -> %s)",
			sl.Name, sl.ConfirmedFlushAdvanceBytes, window, sl.ConfirmedFlushLSN, sl.ConfirmedFlushLSNAfter)
	case verdictStuck:
		fmt.Fprintf(&sb, "stuck: confirmed_flush_lsn did not move while the source wrote %d bytes in %s (slot %q, active=%t, wal_status=%s, retains %s)",
			sl.SourceWALAdvanceBytes, window, sl.Name, sl.Active, orDash(sl.WALStatus), topology.HumanBytes(sl.RetainedWALBytes))
	case verdictQuiet:
		fmt.Fprintf(&sb, "quiet: no source writes in %s (slot %q, confirmed_flush_lsn %s unchanged, active=%t); not a stall",
			window, sl.Name, sl.ConfirmedFlushLSN, sl.Active)
	case verdictNotConfirmed:
		fmt.Fprintf(&sb, "not_confirmed: %s (slot %q, active=%t, retains %s); not a fault",
			rep.Note, sl.Name, sl.Active, topology.HumanBytes(sl.RetainedWALBytes))
	case verdictUnknown:
		fmt.Fprintf(&sb, "unknown: cannot tell whether the source is streaming: %s (slot %q, active=%t)",
			rep.Note, sl.Name, sl.Active)
	}
	if rep.Connector != "" {
		if rep.ConnectorState != "" {
			fmt.Fprintf(&sb, "; connector %s state %s", rep.Connector, rep.ConnectorState)
		} else {
			fmt.Fprintf(&sb, "; connector %s state unknown", rep.Connector)
		}
	}
	if !sl.Active {
		sb.WriteString("; the slot is INACTIVE, so no walsender is streaming it")
	}
	sb.WriteString("\n")
	if rep.ConnectorStatusError != "" {
		fmt.Fprintf(&sb, "%s\n", rep.ConnectorStatusError)
	}
	if rep.Verdict == verdictStuck {
		// The note states the lag and the threshold that made this stuck.
		if rep.Note != "" {
			fmt.Fprintf(&sb, "%s\n", rep.Note)
		}
		switch {
		case rep.OldestTransaction != nil:
			t := rep.OldestTransaction
			fmt.Fprintf(&sb, "oldest open writer: pid=%d state=%s age=%s verb=%s (a long transaction holds confirmed_flush_lsn back; the connector may not be broken)\n",
				t.PID, orDash(t.State), t.Age, orDash(t.Verb))
		case rep.OldestOtherWriter != nil:
			w := rep.OldestOtherWriter
			fmt.Fprintf(&sb, "oldest open writer outside this pipeline's tables: pid=%d same_database=%t state=%s age=%s xid_age=%d (it holds back every logical slot's WAL horizon)\n",
				w.PID, w.SameDatabase, orDash(w.State), w.Age, w.XIDAge)
		case rep.TransactionNote != "":
			fmt.Fprintf(&sb, "%s\n", rep.TransactionNote)
		}
	}
	return sb.String()
}
