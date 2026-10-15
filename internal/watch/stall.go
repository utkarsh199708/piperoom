package watch

import (
	"fmt"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// SignalSlotStall is the slot_stall signal (QA G18): a replication slot whose
// restart_lsn and confirmed_flush_lsn have not moved for stallAfter while the
// WAL it holds keeps growing. A quiet captured table on a busy server does
// this: Debezium has nothing to confirm, the slot sits still for days, and
// slot_wal fires only at 1 GiB. A connector that reads RUNNING but loops on a
// decoding error (QA G17) stalls the slot the same way.
//
// owners.yaml has no slot_stall block (internal/owners rejects unknown signal
// names), so its settings are the code defaults below and it follows the
// slot_wal block: on when slot_wal is on, with slot_wal's action.
const SignalSlotStall = "slot_stall"

// Slot-stall defaults: conservative, so a slot that confirms now and then, or
// an idle server, never fires.
const (
	// stallAfter: neither LSN moved for this long. The reading holds it, so
	// the signal itself has no `for` and clears on the first tick after a move.
	stallAfter = 30 * time.Minute
	// stallMinRetained: the slot holds at least this much WAL.
	stallMinRetained int64 = 64 << 20
	// stallMinGrowth: the retained WAL grew at least this much since the slot
	// last moved (an idle server writes almost nothing, so nothing is at risk).
	stallMinGrowth int64 = 1 << 20
)

// stallMem is the slot_stall memory of one pipeline's slot.
type stallMem struct {
	slot               string
	restart, confirmed string
	// since is when the LSNs were last seen to move, or first seen.
	since time.Time
	// base is the retained WAL at since.
	base int64
}

// slotStallCfg is the slot_stall setting of a pipeline (see SignalSlotStall).
func slotStallCfg(cfg owners.WatchConfig) owners.WatchSignal {
	return owners.WatchSignal{Enabled: cfg.SlotWAL.Enabled, Action: cfg.SlotWAL.Action}
}

// slotStallReading updates m with this tick's row and decides the verdict:
// clear while the slot moved within stallAfter, breach once it has sat still
// that long, is active and holds at least stallMinRetained that grew by
// stallMinGrowth, hold otherwise (an inactive slot is slot_inactive's; a
// slot that stopped growing neither fires nor clears). A slot with no
// restart_lsn (lost, or never reserved) is clear: slot_wal reports a lost
// slot. sev2 when the connector sends heartbeats (the slot should move, so
// Debezium is not confirming), else sev3 (an idle table is the likely cause).
// The evidence holds numbers and the slot name only, never an LSN.
func slotStallReading(now time.Time, slot string, m *stallMem, row clients.ReplicationSlot, hb heartbeat) reading {
	th := fmt.Sprintf("restart_lsn moves within %s, or under %s retained", fmtDur(stallAfter), fmtBytes(stallMinRetained))
	if m.slot != slot || row.RestartLSN == "" {
		*m = stallMem{slot: slot}
	}
	if row.RestartLSN == "" {
		return reading{v: vClear, value: "no restart_lsn", threshold: th}
	}
	if m.since.IsZero() || row.RestartLSN != m.restart || row.ConfirmedFlushLSN != m.confirmed {
		m.restart, m.confirmed, m.since, m.base = row.RestartLSN, row.ConfirmedFlushLSN, now, row.RetainedWALBytes
	}
	stalled := now.Sub(m.since)
	growth := row.RetainedWALBytes - m.base
	if stalled < stallAfter {
		return reading{v: vClear, value: "moved " + fmtDur(stalled.Round(time.Second)) + " ago", threshold: th}
	}
	sev := "sev3"
	if hb.known && hb.configured {
		sev = "sev2"
	}
	r := reading{
		sev:       sev,
		label:     fmt.Sprintf("slot %s stalled %s, %s retained (+%s)", slot, fmtDur(stalled.Round(time.Minute)), fmtBytes(row.RetainedWALBytes), fmtBytes(max(growth, 0))),
		value:     fmt.Sprintf("still %s, %s retained, +%s", fmtDur(stalled.Round(time.Second)), fmtBytes(row.RetainedWALBytes), fmtBytes(max(growth, 0))),
		threshold: th,
		targets:   []string{slot},
	}
	switch {
	case row.Active && row.RetainedWALBytes >= stallMinRetained && growth >= stallMinGrowth:
		r.v = vBreach
	default:
		r.v = vHold
	}
	ev := map[string]any{
		"signal": SignalSlotStall, "severity": sev, "slot": slot, "active": row.Active,
		"retained_bytes": row.RetainedWALBytes, "retained_growth_bytes": max(growth, 0),
		"stalled_seconds": int64(stalled.Seconds()), "stalled_since": m.since.UTC().Format(time.RFC3339),
		"stall_after_seconds": int64(stallAfter.Seconds()), "fire_bytes": stallMinRetained,
	}
	if hb.known {
		ev["heartbeat_configured"] = hb.configured
	}
	r.ev = ev
	return r
}
