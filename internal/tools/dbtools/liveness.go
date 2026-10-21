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

// Verdicts of pg_streaming_liveness. "streaming" and "quiet" are healthy
// states (a quiet source has nothing to send); only "stuck" is a fault.
const (
	verdictStreaming = "streaming"
	verdictStuck     = "stuck"
	verdictQuiet     = "quiet"
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
			"Returns verdict streaming (the slot advanced), stuck (confirmed_flush_lsn did not move while the source wrote) or quiet (no source writes; not a fault). " +
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

			started := time.Now()
			before, _, err := d.namedSlots(ctx, names)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_streaming_liveness: %w", err)
			}
			if err := d.wait(ctx, window); err != nil {
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
			var sl slotLiveness
			var verdict, note string
			switch {
			case ok:
				sl, verdict, note = evaluateSlot(s0, s1)
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
			rep.Connector, rep.ConnectorState, rep.ConnectorTasks, rep.ConnectorStatusError = d.connectorStatus(ctx, p)
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

// wait blocks for d or until ctx ends. Deps.Wait lets tests replace the sleep.
func (d Deps) wait(ctx context.Context, dur time.Duration) error {
	if d.Wait != nil {
		return d.Wait(ctx, dur)
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
//   - stuck when it did not move while the source WAL head advanced;
//   - quiet when neither moved (the source had nothing to send);
//   - unknown, with a note, when the reads do not carry a comparable position.
//     A slot that cannot be judged is never reported as healthy.
func evaluateSlot(before, after clients.ReplicationSlot) (slotLiveness, string, string) {
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
		sl.Verdict = verdictStuck
		return sl, verdictStuck, ""
	}
	sl.Verdict = verdictQuiet
	return sl, verdictQuiet, ""
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

// connectorStatus reads the pipeline's first connector's state. The liveness
// verdict comes from Postgres, so a Connect failure is reported as a note, not
// as a failed check; it never hides a reading.
func (d Deps) connectorStatus(ctx context.Context, p owners.Pipeline) (connector, state string, tasks []string, note string) {
	if len(p.Connectors) == 0 {
		return "", "", nil, ""
	}
	connector = p.Connectors[0]
	if d.Connect == nil {
		return connector, "", nil, "Connect is not configured, so the connector state is unknown"
	}
	st, err := d.Connect.Status(ctx, connector)
	if err != nil {
		return connector, "", nil, "connector status unavailable: " + cutShort(oneLine(err.Error()), 200)
	}
	state = st.State
	for _, t := range st.Tasks {
		tasks = append(tasks, fmt.Sprintf("%d:%s", t.ID, t.State))
	}
	return connector, state, tasks, ""
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
	// Note says why a verdict is unknown.
	Note string `json:"note,omitempty"`
	// Source says which signal produced the verdict, so the model does not
	// claim Debezium metrics were read.
	Source        string       `json:"source"`
	WindowSeconds int          `json:"window_seconds"`
	ElapsedMS     int64        `json:"elapsed_ms"`
	Slot          slotLiveness `json:"slot"`
	// Connector is the pipeline's first connector; its state is what makes
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
	SourceWALAdvanceBytes int64  `json:"source_wal_advance_bytes"`
	Verdict               string `json:"verdict"`
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
