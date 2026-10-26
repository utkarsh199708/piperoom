package watch

import (
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// A MySQL pipeline (owners.yaml source_kind: mysql, Q8d) has no replication
// slot and no WAL: the source answers ErrNotSupported for ReplicationSlots
// (docs/design/mysql-source.md §9). The watcher therefore never reads slots
// for it, so slot_inactive, slot_wal, long_tx and the slot WAL forecast are off. The
// connector, lag and DLQ signals are source-neutral and stay on (Q8p).
// owners.Load already turns the slot signals off for such a pipeline; this
// also covers a config built in code, and says so once in the log.

// A SQL Server, MongoDB or Oracle pipeline (KAN-529) has no slot either, so
// the check is positive: only a Postgres pipeline reads slots (KAN-541).

// hasNoSlot reports whether p is a cdc pipeline on a source other than
// Postgres: it has no replication slot, so the watcher never reads one. A
// batch pipeline has no source database and is not one (as before).
func hasNoSlot(p owners.Pipeline) bool {
	return p.KindOrDefault() == events.PipelineKindCDC && p.SourceKindOrDefault() != owners.SourceKindPostgres
}

// slotSignalsOff turns the slot signals off in cfg for the non-Postgres pipeline id,
// with one INFO line per pipeline for the watcher's lifetime.
func (w *Watcher) slotSignalsOff(id, kind string, cfg *owners.WatchConfig) {
	cfg.SlotInactive.Enabled = false
	cfg.SlotWAL.Enabled = false
	cfg.LongTx.Enabled = false
	w.slotOffMu.Lock()
	logged := w.slotOff[id]
	w.slotOff[id] = true
	w.slotOffMu.Unlock()
	if !logged {
		w.log.Info("watch: slot signals off for this pipeline", "mode", string(w.cfg.Mode), "pipeline", id,
			"signals", owners.SignalSlotInactive+","+owners.SignalSlotWAL+","+owners.SignalLongTx, "reason", "source_kind "+kind+" has no replication slot")
	}
}
