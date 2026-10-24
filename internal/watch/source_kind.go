package watch

import "piperoom/internal/owners"

// A MySQL pipeline (owners.yaml source_kind: mysql, Q8d) has no replication
// slot and no WAL: the source answers ErrNotSupported for ReplicationSlots
// (docs/design/mysql-source.md §9). The watcher therefore never reads slots
// for it, so slot_inactive, slot_wal and the slot WAL forecast are off. The
// connector, lag and DLQ signals are source-neutral and stay on (Q8p).
// owners.Load already turns the slot signals off for such a pipeline; this
// also covers a config built in code, and says so once in the log.

// slotSignalsOff turns the slot signals off in cfg for the MySQL pipeline id,
// with one INFO line per pipeline for the watcher's lifetime.
func (w *Watcher) slotSignalsOff(id string, cfg *owners.WatchConfig) {
	cfg.SlotInactive.Enabled = false
	cfg.SlotWAL.Enabled = false
	w.slotOffMu.Lock()
	logged := w.slotOff[id]
	w.slotOff[id] = true
	w.slotOffMu.Unlock()
	if !logged {
		w.log.Info("watch: slot signals off for this pipeline", "mode", string(w.cfg.Mode), "pipeline", id,
			"signals", owners.SignalSlotInactive+","+owners.SignalSlotWAL, "reason", "source_kind mysql has no replication slot")
	}
}
