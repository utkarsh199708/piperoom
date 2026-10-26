package watch

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// long_tx (H1): an open transaction on the source holds the slot's WAL
// horizon. Debezium still streams and confirms the commits of other
// transactions, so the connector, the consumer and the warehouse all read
// healthy; the only symptom is in pg_replication_slots: restart_lsn stops
// moving while the WAL head advances and retained WAL grows (live test
// 2026-10-25, dev/scenarios.md §4). slot_wal would only see it at 1 GiB.
//
// It reads the slot row the slot signals already read each tick, no other
// I/O. The WAL head is restart_lsn + retained_wal_bytes (retained is
// pg_current_wal_lsn() - restart_lsn in the same query).
//
// The condition holds when, while restart_lsn stays at one value:
//   - the slot is active (an inactive slot is slot_inactive's finding),
//   - the WAL head moved forward in at least ticks ticks,
//   - retained WAL grew by at least min_growth since the value was first seen
//     (a quiet source never qualifies), and
//   - confirmed_flush_lsn moved: the connector is confirming, so it is not a
//     stopped reader or an idle connector without heartbeats (both hold
//     restart_lsn too, and slot_inactive / slot_wal cover them).
//
// A tick in which the WAL head did not move (a quiet moment) neither counts
// nor resets: restart_lsn is still held. A failed read changes nothing.
// restart_lsn moving starts a new episode and reads clear; the state machine
// then resolves after `for`, like every other signal.

// longTxSev: the pipeline still delivers (data flows), but the WAL the source
// keeps grows without bound until the transaction ends; slot_wal raises the
// severity once the size or the ETA says so.
const longTxSev = "sev3"

// longTxMem is the memory of one held restart_lsn: the first and the latest
// sample taken while the slot's restart_lsn stayed at one value.
type longTxMem struct {
	slot    string
	restart uint64
	firstAt time.Time

	firstHead, lastHead           uint64
	firstRetained, lastRetained   int64
	firstConfirmed, lastConfirmed uint64
	// advanced counts the ticks in which the WAL head moved forward.
	advanced int
}

// longTxReading steps the long_tx memory with this tick's slot row and
// returns the reading. ss is the long_tx state (its last reading is kept
// while the slot is inactive). Caller holds w.mu.
func longTxReading(now time.Time, t target, ps *pipeState, ss *sigState, row clients.ReplicationSlot) reading {
	lt := t.cfg.LongTx
	threshold := fmt.Sprintf("restart_lsn held %d ticks, WAL +%s, connector confirming", lt.Ticks, fmtBytes(lt.MinGrowthBytes))
	if !row.Active {
		// Nobody reads the slot, so restart_lsn cannot move: that is
		// slot_inactive's finding, not a held horizon. Neither timer runs and
		// the memory waits for the reader to come back.
		r := ss.last
		r.v, r.value, r.threshold = vHold, "active=false", threshold
		return r
	}
	clear := reading{v: vClear, value: "restart_lsn moving", threshold: threshold}
	if row.RestartLSN == "" {
		// NULL restart_lsn: a lost or never-used slot holds no horizon.
		ps.ltx = nil
		return clear
	}
	restart, err := parseLSN(row.RestartLSN)
	if err != nil {
		return reading{v: vUnknown, err: fmt.Errorf("watch: pipeline %s: slot %s restart_lsn: %w", t.p.ID, ps.slot, err)}
	}
	var confirmed uint64
	if row.ConfirmedFlushLSN != "" {
		if confirmed, err = parseLSN(row.ConfirmedFlushLSN); err != nil {
			return reading{v: vUnknown, err: fmt.Errorf("watch: pipeline %s: slot %s confirmed_flush_lsn: %w", t.p.ID, ps.slot, err)}
		}
	}
	head := restart + uint64(max(row.RetainedWALBytes, 0))

	m := ps.ltx
	// A new restart_lsn (it moved), another slot, or positions that went
	// backwards (the slot was re-created) start a new episode.
	if m == nil || m.slot != ps.slot || m.restart != restart || head < m.lastHead || confirmed < m.lastConfirmed {
		ps.ltx = &longTxMem{
			slot: ps.slot, restart: restart, firstAt: now,
			firstHead: head, lastHead: head,
			firstRetained: row.RetainedWALBytes, lastRetained: row.RetainedWALBytes,
			firstConfirmed: confirmed, lastConfirmed: confirmed,
		}
		return clear
	}
	if head > m.lastHead {
		m.advanced++
	}
	m.lastHead, m.lastRetained, m.lastConfirmed = head, row.RetainedWALBytes, confirmed
	growth := m.lastRetained - m.firstRetained
	heldFor := now.Sub(m.firstAt)
	if m.advanced < lt.Ticks || growth < lt.MinGrowthBytes || m.lastConfirmed <= m.firstConfirmed {
		clear.value = fmt.Sprintf("restart_lsn %s held %d ticks, WAL +%s", fmtLSN(restart), m.advanced, fmtBytes(growth))
		return clear
	}
	return reading{
		v:         vBreach,
		sev:       longTxSev,
		label:     fmt.Sprintf("slot %s restart_lsn %s held, WAL +%s", ps.slot, fmtLSN(restart), fmtBytes(growth)),
		withDur:   true,
		value:     fmt.Sprintf("restart_lsn %s held %s, WAL +%s", fmtLSN(restart), fmtDur(heldFor.Round(time.Second)), fmtBytes(growth)),
		threshold: threshold,
		ev: map[string]any{
			"signal": owners.SignalLongTx, "severity": longTxSev, "slot": ps.slot, "active": true,
			"restart_lsn":    fmtLSN(restart),
			"wal_head_first": fmtLSN(m.firstHead), "wal_head_last": fmtLSN(m.lastHead),
			"confirmed_flush_first": fmtLSN(m.firstConfirmed), "confirmed_flush_last": fmtLSN(m.lastConfirmed),
			"retained_bytes": m.lastRetained, "retained_first_bytes": m.firstRetained,
			"retained_growth_bytes": growth,
			"ticks":                 m.advanced,
			"held_for_seconds":      int64(heldFor.Seconds()),
			"note": "restart_lsn is held while the connector keeps confirming: an open transaction on the source " +
				"(pg_stat_activity with backend_xid set, oldest xact_start) holds the WAL horizon",
		},
	}
}

// parseLSN reads a Postgres pg_lsn text ("16/B374D848": two hex numbers of at
// most 32 bits each).
func parseLSN(s string) (uint64, error) {
	hi, lo, ok := strings.Cut(strings.TrimSpace(s), "/")
	if !ok || hi == "" || lo == "" {
		return 0, fmt.Errorf("lsn %q: want X/Y in hex", clean(s, 40))
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("lsn %q: want X/Y in hex", clean(s, 40))
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("lsn %q: want X/Y in hex", clean(s, 40))
	}
	return h<<32 | l, nil
}

// fmtLSN writes an LSN the way Postgres does ("0/2528450").
func fmtLSN(n uint64) string { return fmt.Sprintf("%X/%X", n>>32, n&0xFFFFFFFF) }
