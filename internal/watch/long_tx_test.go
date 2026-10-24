package watch

import (
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// tick15 is the dev stack's watch interval (PIPEROOM_WATCH_INTERVAL=15s).
const tick15 = 15 * time.Second

// restartHeld is the restart_lsn of the 2026-10-25 live test (0/2528450).
const restartHeld = uint64(0x2528450)

// pgSlot models one Postgres slot the way pg_replication_slots reports it:
// the WAL head advances with writes, confirmed_flush_lsn follows the head
// while the connector confirms, and restart_lsn moves only when told.
type pgSlot struct {
	h          *harness
	name       string
	head       uint64
	restart    uint64
	confirmed  uint64
	active     bool
	confirming bool
}

func newPGSlot(h *harness, pipe string) *pgSlot {
	s := &pgSlot{h: h, name: "debezium_" + pipe, head: restartHeld + 464<<10, restart: restartHeld,
		confirmed: restartHeld + 464<<10, active: true, confirming: true}
	s.publish()
	return s
}

// write advances the WAL head by n bytes (n = 0: a quiet tick).
func (s *pgSlot) write(n uint64) {
	s.head += n
	if s.confirming {
		s.confirmed = s.head
	}
	s.publish()
}

// release moves restart_lsn up to the confirmed position (the transaction
// ended, or a healthy flush).
func (s *pgSlot) release() {
	s.restart = s.confirmed
	s.publish()
}

func (s *pgSlot) setActive(a bool) {
	s.active = a
	s.publish()
}

func (s *pgSlot) publish() {
	s.h.src.update(s.name, func(r *clients.ReplicationSlot) {
		r.Active = s.active
		r.RestartLSN = fmtLSN(s.restart)
		r.ConfirmedFlushLSN = fmtLSN(s.confirmed)
		r.RetainedWALBytes = int64(s.head - s.restart)
	})
}

// heldSteps runs n ticks of tick15 apart, writing perTick bytes before each.
func (s *pgSlot) heldSteps(n int, perTick uint64) {
	s.h.t.Helper()
	for range n {
		s.write(perTick)
		s.h.step(tick15)
	}
}

func longTxRooms(t *testing.T, h *harness) []openRec {
	t.Helper()
	var out []openRec
	for _, r := range h.rooms.created() {
		if strings.Contains(string(r.alert.Raw), `"signal":"long_tx"`) {
			out = append(out, r)
		}
	}
	return out
}

// With the defaults (ticks 3, min_growth 64 KiB, for 5m) and a 15 s interval,
// the condition holds on the third tick that sees the head advance (45 s
// after the held value was first seen) and fires 5 min later.
func TestLongTxFiresAfterHeldTicks(t *testing.T) {
	h := newHarness(t, opts{})
	s := newPGSlot(h, "orders")
	h.step(0) // first sample: restart_lsn first seen
	s.heldSteps(2, 32<<10)
	if r := h.w.pipes["orders"].sigs[owners.SignalLongTx].last; r.v != vClear {
		t.Fatalf("after 2 advancing ticks the reading is %v, want clear (ticks 3)", r.v)
	}
	s.heldSteps(1, 32<<10) // 3 ticks, +96 KiB: breaching from here (t = 45 s)
	if r := h.w.pipes["orders"].sigs[owners.SignalLongTx].last; r.v != vBreach {
		t.Fatalf("after 3 advancing ticks the reading is %v, want breach", r.v)
	}
	s.heldSteps(19, 32<<10) // t = 330 s: 4m45s of breach
	if h.phase("orders", owners.SignalLongTx) != phaseOK || len(longTxRooms(t, h)) != 0 {
		t.Fatalf("fired before for (5m)\n%s", h.logs)
	}
	s.heldSteps(1, 32<<10) // t = 345 s: 5 min of breach
	if h.phase("orders", owners.SignalLongTx) != phaseFiring {
		t.Fatalf("did not fire after for\n%s", h.logs)
	}
	rooms := longTxRooms(t, h)
	if len(rooms) != 1 {
		t.Fatalf("long_tx rooms = %d, want 1", len(rooms))
	}
	a := rooms[0].alert
	if !strings.Contains(a.Title, "restart_lsn 0/2528450 held") {
		t.Fatalf("title %q does not name the held restart_lsn", a.Title)
	}
	ev := signalEv(t, evidenceOf(t, a.Raw), owners.SignalLongTx)
	wantStr := map[string]string{
		"severity": "sev3", "slot": "debezium_orders", "restart_lsn": "0/2528450",
		"wal_head_first": fmtLSN(restartHeld + 464<<10), "wal_head_last": fmtLSN(restartHeld + 464<<10 + 23*32<<10),
	}
	for k, v := range wantStr {
		if ev[k] != v {
			t.Errorf("evidence %s = %v, want %s", k, ev[k], v)
		}
	}
	wantNum := map[string]float64{
		"retained_first_bytes": 464 << 10, "retained_bytes": 464<<10 + 23*32<<10,
		"retained_growth_bytes": 23 * 32 << 10, "ticks": 23, "held_for_seconds": 345,
	}
	for k, v := range wantNum {
		if ev[k] != v {
			t.Errorf("evidence %s = %v, want %v", k, ev[k], v)
		}
	}
	if ev["active"] != true {
		t.Errorf("evidence active = %v", ev["active"])
	}
	// The other slot signals stay quiet: this is long_tx's finding alone.
	if h.phase("orders", owners.SignalSlotInactive) != phaseOK || h.phase("orders", owners.SignalSlotWAL) != phaseOK {
		t.Fatal("slot_inactive or slot_wal fired on a held horizon")
	}
}

// A held restart_lsn on a source that writes nothing (or a few bytes) is an
// idle database, not a held horizon.
func TestLongTxQuietSourceDoesNotFire(t *testing.T) {
	for _, tc := range []struct {
		name    string
		perTick uint64
	}{
		{"no WAL at all", 0},
		{"a trickle under min_growth", 100}, // 120 ticks: 12 KB < 64 KiB
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, opts{})
			s := newPGSlot(h, "orders")
			h.step(0)
			s.heldSteps(120, tc.perTick) // 30 min
			if h.phase("orders", owners.SignalLongTx) != phaseOK || len(longTxRooms(t, h)) != 0 {
				t.Fatalf("long_tx fired on a quiet source\n%s", h.logs)
			}
		})
	}
}

// restart_lsn that keeps moving is a healthy slot, including Debezium's
// pattern of confirming only on each offset flush (every 60 s: restart_lsn
// sits still for 4 ticks while the WAL grows).
func TestLongTxRestartMovesDoesNotFire(t *testing.T) {
	for _, every := range []int{1, 4, 8} {
		h := newHarness(t, opts{})
		s := newPGSlot(h, "orders")
		s.confirming = false // confirmed moves with each flush, as Debezium does
		h.step(0)
		for i := 1; i <= 120; i++ {
			s.write(64 << 10)
			if i%every == 0 {
				s.confirmed = s.head
				s.release()
			}
			h.step(tick15)
		}
		if h.phase("orders", owners.SignalLongTx) != phaseOK {
			t.Fatalf("restart_lsn moving every %d ticks: long_tx fired\n%s", every, h.logs)
		}
	}
}

func TestLongTxClearsWhenRestartMoves(t *testing.T) {
	h := newHarness(t, opts{})
	s := newPGSlot(h, "orders")
	h.step(0)
	s.heldSteps(23, 32<<10)
	if h.phase("orders", owners.SignalLongTx) != phaseFiring {
		t.Fatalf("did not fire\n%s", h.logs)
	}
	// The transaction ends: restart_lsn catches up and moves on from then.
	s.release()
	h.step(tick15) // clearing from here
	for range 19 { // 4m45s of clear
		s.write(32 << 10)
		s.release()
		h.step(tick15)
	}
	if h.phase("orders", owners.SignalLongTx) != phaseFiring {
		t.Fatal("resolved before for")
	}
	s.write(32 << 10)
	s.release()
	h.step(tick15) // 5 min of clear
	if h.phase("orders", owners.SignalLongTx) != phaseOK {
		t.Fatalf("did not resolve 5 min after restart_lsn moved\n%s", h.logs)
	}
	if len(h.rooms.resolved()) != 1 {
		t.Fatalf("resolved alerts = %d, want 1", len(h.rooms.resolved()))
	}
}

// One quiet tick inside a held episode neither resets the timer nor clears.
func TestLongTxQuietTickDoesNotFlap(t *testing.T) {
	h := newHarness(t, opts{})
	s := newPGSlot(h, "orders")
	h.step(0)
	s.heldSteps(3, 32<<10) // breaching from t = 45 s
	s.heldSteps(1, 0)      // a quiet tick: the head did not move
	if r := h.w.pipes["orders"].sigs[owners.SignalLongTx].last; r.v != vBreach {
		t.Fatalf("a quiet tick read %v, want breach (restart_lsn is still held)", r.v)
	}
	s.heldSteps(19, 32<<10) // t = 345 s
	if h.phase("orders", owners.SignalLongTx) != phaseFiring {
		t.Fatalf("a quiet tick delayed the signal\n%s", h.logs)
	}
	s.heldSteps(1, 0)
	if h.phase("orders", owners.SignalLongTx) != phaseFiring {
		t.Fatal("a quiet tick while firing started a clear")
	}
	if c := h.w.pipes["orders"].sigs[owners.SignalLongTx].clearingSince; !c.IsZero() {
		t.Fatalf("clearingSince = %v after a quiet tick, want zero", c)
	}
}

// An inactive slot holds restart_lsn because nobody reads it: that is
// slot_inactive, never long_tx.
func TestLongTxInactiveSlotIsNotLongTx(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", watch: "    watch:\n      signals:\n        long_tx: {for: 1m}\n"}}})
	s := newPGSlot(h, "orders")
	s.setActive(false)
	h.step(0)
	s.heldSteps(40, 64<<10) // 10 min, WAL growing, confirmed moving
	if h.phase("orders", owners.SignalSlotInactive) != phaseFiring {
		t.Fatalf("slot_inactive did not fire\n%s", h.logs)
	}
	if h.phase("orders", owners.SignalLongTx) != phaseOK {
		t.Fatal("long_tx fired on an inactive slot")
	}
	ev := evidenceOf(t, h.rooms.created()[0].alert.Raw)
	if strings.Contains(string(h.rooms.created()[0].alert.Raw), `"long_tx"`) {
		t.Fatalf("long_tx is in the evidence of an inactive slot: %v", ev)
	}
}

// A connector that confirms nothing (stopped streaming, or no heartbeat while
// only unwatched tables are written) also holds restart_lsn. That is not an
// open transaction; slot_wal covers its growth.
func TestLongTxNotConfirmingDoesNotFire(t *testing.T) {
	h := newHarness(t, opts{})
	s := newPGSlot(h, "orders")
	s.confirming = false
	h.step(0)
	s.heldSteps(60, 64<<10) // 15 min
	if h.phase("orders", owners.SignalLongTx) != phaseOK {
		t.Fatalf("long_tx fired while confirmed_flush_lsn stood still\n%s", h.logs)
	}
}

func TestLongTxReadErrorStaysUnknown(t *testing.T) {
	t.Run("a failing source opens no room", func(t *testing.T) {
		h := newHarness(t, opts{})
		newPGSlot(h, "orders")
		h.src.mu.Lock()
		h.src.err = errors.New("pq: connection refused")
		h.src.mu.Unlock()
		h.steps(40, tick15)
		if h.phase("orders", owners.SignalLongTx) != phaseOK || len(h.rooms.created()) != 0 {
			t.Fatalf("a failing source changed long_tx or opened a room\n%s", h.logs)
		}
		if !strings.Contains(h.logs.String(), "signal=long_tx err=") {
			t.Fatalf("no check-failed line for long_tx\n%s", h.logs)
		}
	})
	t.Run("an error tick in a held episode changes nothing", func(t *testing.T) {
		h := newHarness(t, opts{})
		s := newPGSlot(h, "orders")
		h.step(0)
		s.heldSteps(10, 32<<10) // breaching since t = 45 s
		h.src.mu.Lock()
		h.src.err = errors.New("pq: timeout")
		h.src.mu.Unlock()
		s.heldSteps(2, 32<<10) // unknown
		h.src.mu.Lock()
		h.src.err = nil
		h.src.mu.Unlock()
		s.heldSteps(10, 32<<10) // t = 330 s
		if h.phase("orders", owners.SignalLongTx) != phaseOK {
			t.Fatal("fired early")
		}
		s.heldSteps(1, 32<<10) // t = 345 s: on time
		if h.phase("orders", owners.SignalLongTx) != phaseFiring {
			t.Fatalf("an error tick delayed or reset long_tx\n%s", h.logs)
		}
	})
	t.Run("an unreadable restart_lsn is unknown", func(t *testing.T) {
		h := newHarness(t, opts{})
		h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.RestartLSN, r.ConfirmedFlushLSN = "garbage", "0/1" })
		h.steps(40, tick15)
		if h.phase("orders", owners.SignalLongTx) != phaseOK || len(h.rooms.created()) != 0 {
			t.Fatal("an unreadable restart_lsn fired")
		}
		if !strings.Contains(h.logs.String(), "restart_lsn: lsn") {
			t.Fatalf("no check-failed line naming restart_lsn\n%s", h.logs)
		}
	})
}

func TestLongTxConfigOffNeverFires(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", watch: "    watch:\n      signals:\n        long_tx: {enabled: false}\n"}}})
	s := newPGSlot(h, "orders")
	h.step(0)
	s.heldSteps(60, 64<<10)
	if h.phase("orders", owners.SignalLongTx) != phaseOK || len(h.rooms.created()) != 0 {
		t.Fatal("long_tx fired while off")
	}
	if h.w.pipes["orders"].ltx != nil {
		t.Fatal("long_tx keeps memory while off")
	}
}

// Action log: the signal fires (and logs) but opens no room.
func TestLongTxActionLog(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", watch: "    watch:\n      signals:\n        long_tx: {action: log, for: 1m}\n"}}})
	s := newPGSlot(h, "orders")
	h.step(0)
	s.heldSteps(8, 32<<10)
	if h.phase("orders", owners.SignalLongTx) != phaseFiring {
		t.Fatalf("did not fire\n%s", h.logs)
	}
	if len(h.rooms.created()) != 0 {
		t.Fatal("action log opened a room")
	}
	if !strings.Contains(h.logs.String(), `msg="watch: signal fired"`) || !strings.Contains(h.logs.String(), "signal=long_tx") {
		t.Fatalf("no fired line\n%s", h.logs)
	}
}

func TestParseLSN(t *testing.T) {
	tests := []struct {
		in   string
		want uint64
		bad  bool
	}{
		{"0/2528450", 0x2528450, false},
		{"16/B374D848", 0x16<<32 | 0xB374D848, false},
		{"FFFFFFFF/FFFFFFFF", 1<<64 - 1, false},
		{" 0/1 ", 1, false},
		{"0/0", 0, false},
		{"", 0, true},
		{"2528450", 0, true},
		{"0/", 0, true},
		{"/1", 0, true},
		{"0/XYZ", 0, true},
		{"1FFFFFFFF/0", 0, true},
		{"0/-1", 0, true},
	}
	for _, tc := range tests {
		got, err := parseLSN(tc.in)
		if tc.bad {
			if err == nil {
				t.Errorf("parseLSN(%q) = %d, want an error", tc.in, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("parseLSN(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
		if strings.TrimSpace(tc.in) != fmtLSN(got) {
			t.Errorf("fmtLSN(%d) = %q, want %q", got, fmtLSN(got), strings.TrimSpace(tc.in))
		}
	}
}
