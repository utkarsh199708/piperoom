package watch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
)

// The secret and the plain value the tests put in configs: neither may reach
// a log line, a title or the evidence.
const (
	driftSecretOld = "s3cr3t-OLD-pw"
	driftSecretNew = "s3cr3t-NEW-pw"
	driftValueOld  = "value-OLD-precise"
	driftValueNew  = "value-NEW-double"
)

var driftOrders = configDriftID("pg-orders")

// editCfg changes pg-<pipe>'s config in place.
func (h *harness) editCfg(pipe string, f func(m map[string]string)) {
	h.conn.mu.Lock()
	defer h.conn.mu.Unlock()
	m := h.conn.cfg["pg-"+pipe]
	f(m)
}

// driftHarness is a harness whose orders connector carries a plain value and
// a password, so a change of either can be checked for leaks.
func driftHarness(t *testing.T, o opts) *harness {
	t.Helper()
	h := newHarness(t, o)
	for _, p := range o.pipes {
		h.editCfg(p.id, func(m map[string]string) {
			m["decimal.handling.mode"] = driftValueOld
			m["database.password"] = driftSecretOld
		})
	}
	if len(o.pipes) == 0 {
		h.editCfg("orders", func(m map[string]string) {
			m["decimal.handling.mode"] = driftValueOld
			m["database.password"] = driftSecretOld
		})
	}
	return h
}

// noValues fails when any config value shows in the logs, the room titles,
// the evidence or a post.
func noValues(t *testing.T, h *harness) {
	t.Helper()
	var all strings.Builder
	all.WriteString(h.logs.String())
	h.rooms.mu.Lock()
	for _, o := range h.rooms.opens {
		all.WriteString(o.alert.Title)
		all.Write(o.alert.Raw)
	}
	for _, p := range h.rooms.posts {
		all.WriteString(p.text)
	}
	h.rooms.mu.Unlock()
	for _, v := range []string{driftSecretOld, driftSecretNew, driftValueOld, driftValueNew, "OLD", "NEW"} {
		if strings.Contains(all.String(), v) {
			t.Errorf("a config value %q leaked into logs, titles or evidence", v)
		}
	}
}

// TC1: the same config with its keys in another order has the same hash and
// never fires.
func TestConfigDriftKeyOrderIsNoChange(t *testing.T) {
	a := map[string]string{}
	b := map[string]string{}
	keys := []string{"connector.class", "slot.name", "decimal.handling.mode", "tasks.max", "database.password"}
	for i, k := range keys {
		a[k] = fmt.Sprint(i)
	}
	for i := len(keys) - 1; i >= 0; i-- {
		b[keys[i]] = fmt.Sprint(i)
	}
	da, db := actions.DigestConfig(a), actions.DigestConfig(b)
	if da.Hash != db.Hash || len(actions.ChangedConfigKeys(da, db)) != 0 {
		t.Fatalf("shuffled keys: hash %s vs %s, changed %v", da.Hash, db.Hash, actions.ChangedConfigKeys(da, db))
	}

	h := driftHarness(t, opts{})
	h.steps(5, time.Minute)
	// Rebuild the same config with another insertion order.
	h.conn.mu.Lock()
	old := h.conn.cfg["pg-orders"]
	re := map[string]string{}
	for _, k := range []string{"database.password", "slot.name", "decimal.handling.mode", "heartbeat.interval.ms", "connector.class"} {
		re[k] = old[k]
	}
	h.conn.cfg["pg-orders"] = re
	h.conn.mu.Unlock()
	h.steps(5, time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseOK {
		t.Fatalf("config_drift = %s after a key-order change, want ok", got)
	}
	if n := len(h.rooms.created()); n != 0 {
		t.Fatalf("%d rooms opened, want none", n)
	}
}

// TC2 + E1: one changed key fires after the one-minute `for`, sev3, with the
// key's name only; no value reaches the evidence, the title or the logs.
func TestConfigDriftOneKeyChanged(t *testing.T) {
	h := driftHarness(t, opts{})
	h.step(time.Minute) // baseline
	before := actions.DigestConfig(h.conn.cfg["pg-orders"]).Hash
	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })

	h.step(time.Minute) // breach starts
	if got := h.phase("orders", driftOrders); got != phaseOK {
		t.Fatalf("config_drift fired before its for window: %s", got)
	}
	h.step(time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseFiring {
		t.Fatalf("config_drift = %s after the for window, want firing\n%s", got, h.logs)
	}
	cr := h.rooms.created()
	if len(cr) != 1 {
		t.Fatalf("%d rooms opened, want 1", len(cr))
	}
	a := cr[0].alert
	if a.Severity != "sev3" || !strings.Contains(a.Title, "connector pg-orders config changed outside an approved fix (decimal.handling.mode)") {
		t.Fatalf("room: severity %q title %q", a.Severity, a.Title)
	}
	ev := signalEv(t, evidenceOf(t, a.Raw), events.SignalConfigDrift)
	keys, _ := ev["changed_keys"].([]any)
	if len(keys) != 1 || keys[0] != "decimal.handling.mode" {
		t.Fatalf("changed_keys = %v, want [decimal.handling.mode]", ev["changed_keys"])
	}
	if ev["severity"] != "sev3" || ev["connector"] != "pg-orders" || ev["old_hash"] != before || ev["new_hash"] == before {
		t.Fatalf("evidence = %v (old hash %s)", ev, before)
	}
	if _, ok := ev["keys_omitted"]; ok {
		t.Fatalf("keys_omitted present with one key: %v", ev)
	}
	noValues(t, h)
}

// TC3: a changed password is reported as <secret key changed>; the hashes
// are equal (no secret value enters them) and nothing leaks.
func TestConfigDriftSecretKeyMasked(t *testing.T) {
	h := driftHarness(t, opts{})
	h.step(time.Minute)
	h.editCfg("orders", func(m map[string]string) { m["database.password"] = driftSecretNew })
	h.steps(2, time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseFiring {
		t.Fatalf("config_drift = %s, want firing", got)
	}
	ev := h.openedEv(t, events.SignalConfigDrift)
	keys, _ := ev["changed_keys"].([]any)
	if len(keys) != 1 || keys[0] != events.SecretKeyChanged {
		t.Fatalf("changed_keys = %v, want [%s]", ev["changed_keys"], events.SecretKeyChanged)
	}
	if ev["old_hash"] != ev["new_hash"] {
		t.Fatalf("a secret-only change moved the public hash: %v", ev)
	}
	if strings.Contains(h.rooms.created()[0].alert.Title, "database.password") {
		t.Fatalf("title names the secret key: %q", h.rooms.created()[0].alert.Title)
	}
	noValues(t, h)
}

// TC4: a change made by an approved action never fires: the action.executed
// hook makes the next read the baseline. A later hand edit still fires.
func TestConfigDriftApprovedActionMovesBaseline(t *testing.T) {
	h := driftHarness(t, opts{})
	h.step(time.Minute)
	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })
	h.w.ApproveConnectorChange("orders", "pg-orders")
	h.steps(4, time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseOK {
		t.Fatalf("config_drift = %s after an approved change, want ok", got)
	}
	h.editCfg("orders", func(m map[string]string) { m["tasks.max"] = "4" })
	h.steps(2, time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseFiring {
		t.Fatalf("config_drift = %s after a later hand edit, want firing", got)
	}
	keys, _ := h.openedEv(t, events.SignalConfigDrift)["changed_keys"].([]any)
	if len(keys) != 1 || keys[0] != "tasks.max" {
		t.Fatalf("changed_keys = %v, want only the hand edit", keys)
	}
}

// TC4, the race: the tick reads the fixed config before the action.executed
// hook arrives. The breach is still inside its `for` when the hook moves the
// baseline, so nothing fires.
func TestConfigDriftApprovedActionRaceDoesNotFire(t *testing.T) {
	h := driftHarness(t, opts{})
	h.step(time.Minute)
	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })
	h.step(time.Minute) // reads the new config: breach starts
	h.w.ApproveConnectorChange("orders", "pg-orders")
	h.steps(4, time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseOK {
		t.Fatalf("config_drift = %s, want ok", got)
	}
	if n := len(h.rooms.created()); n != 0 {
		t.Fatalf("%d rooms opened, want none", n)
	}
}

// A read that started before the hook's time is not judged; the first read
// after it becomes the baseline.
func TestConfigDriftReadingWaitsForAFreshRead(t *testing.T) {
	ps := &pipeState{sigs: map[string]*sigState{}}
	b := ps.driftBase("c")
	b.approved, b.ok = actions.DigestConfig(map[string]string{"k": "1"}), true
	b.rebaseAt = t0
	changed := &observation{conns: map[string]*connObs{"c": {cfgTried: true, cfg: map[string]string{"k": "2"}}}}

	changed.at = t0.Add(-time.Second)
	if r := configDriftReading("c", ps, changed); r.v != vHold || b.approved.Hash != actions.DigestConfig(map[string]string{"k": "1"}).Hash {
		t.Fatalf("a read from before the action: verdict %v, baseline moved = %v", r.v, b.rebaseAt.IsZero())
	}
	changed.at = t0
	if r := configDriftReading("c", ps, changed); r.v != vClear || !b.rebaseAt.IsZero() {
		t.Fatalf("the first fresh read: verdict %v, rebase pending = %v", r.v, !b.rebaseAt.IsZero())
	}
	if r := configDriftReading("c", ps, changed); r.v != vClear {
		t.Fatalf("after the rebase the same config is %v, want clear", r.v)
	}
	// An accept while a tick was reading: the older read is not judged.
	b.setAt = t0.Add(time.Minute)
	changed.conns["c"].cfg = map[string]string{"k": "3"}
	if r := configDriftReading("c", ps, changed); r.v != vHold {
		t.Fatalf("a read from before the accept: verdict %v, want hold", r.v)
	}
}

// TC5 (watcher side): accept-config makes the current config the baseline;
// the next poll is quiet and the firing signal resolves. The hashes are the
// redacted configs' public ids.
func TestConfigDriftAcceptConfig(t *testing.T) {
	h := driftHarness(t, opts{})
	h.step(time.Minute)
	base := actions.DigestConfig(h.conn.cfg["pg-orders"]).Hash
	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })
	h.steps(2, time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseFiring {
		t.Fatalf("config_drift = %s, want firing", got)
	}
	oldH, newH, err := h.w.AcceptConfig(context.Background(), "orders", "pg-orders")
	if err != nil {
		t.Fatal(err)
	}
	want := actions.ConfigHash(actions.RedactConfig(h.conn.cfg["pg-orders"]))
	if oldH != base || newH != want || oldH == newH {
		t.Fatalf("AcceptConfig = %s, %s; want %s, %s", oldH, newH, base, want)
	}
	posts := len(h.rooms.posts)
	h.step(time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseFiring {
		t.Fatalf("config_drift = %s right after the accept, want still firing until its for window", got)
	}
	h.step(time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseOK {
		t.Fatalf("config_drift = %s after the accept, want ok", got)
	}
	if len(h.rooms.resolved()) != 1 {
		t.Fatalf("resolved alerts = %d, want 1", len(h.rooms.resolved()))
	}
	if n := len(h.rooms.postsContaining("config changed")); n != 0 || len(h.rooms.posts) != posts {
		t.Fatalf("posts after the accept: %d new, %d about drift", len(h.rooms.posts)-posts, n)
	}
	h.steps(5, time.Minute)
	if n := len(h.rooms.created()); n != 1 {
		t.Fatalf("rooms opened = %d, want the one from before the accept", n)
	}
	noValues(t, h)
}

// TC5/TC6 (watcher side): a failed Connect read answers ErrConnectUnavailable
// and leaves the baseline alone; an unknown connector is ErrNotWatched.
func TestConfigDriftAcceptConfigErrors(t *testing.T) {
	h := driftHarness(t, opts{})
	h.step(time.Minute)
	base := actions.DigestConfig(h.conn.cfg["pg-orders"]).Hash
	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })
	h.conn.mu.Lock()
	h.conn.cfgErr["pg-orders"] = errors.New("dial tcp: connection refused")
	h.conn.mu.Unlock()
	if _, _, err := h.w.AcceptConfig(context.Background(), "orders", "pg-orders"); !errors.Is(err, ErrConnectUnavailable) {
		t.Fatalf("AcceptConfig with Connect down: %v, want ErrConnectUnavailable", err)
	}
	h.w.mu.Lock()
	got := h.w.pipes["orders"].drift["pg-orders"].approved.Hash
	h.w.mu.Unlock()
	if got != base {
		t.Fatalf("baseline moved on a failed accept: %s, want %s", got, base)
	}
	for _, c := range [][2]string{{"orders", "pg-other"}, {"nope", "pg-orders"}} {
		if _, _, err := h.w.AcceptConfig(context.Background(), c[0], c[1]); !errors.Is(err, ErrNotWatched) {
			t.Fatalf("AcceptConfig(%s, %s): %v, want ErrNotWatched", c[0], c[1], err)
		}
	}
	// No Connect client at all.
	h.deps.Connect = nil
	w, err := New(h.deps)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.AcceptConfig(context.Background(), "orders", "pg-orders"); !errors.Is(err, ErrConnectUnavailable) {
		t.Fatalf("AcceptConfig with no Connect client: %v", err)
	}
}

// TC6 + E3: Connect unreachable: no signal, no baseline change, one warning.
// When it answers again, the change made meanwhile still fires against the
// old baseline.
func TestConfigDriftConnectUnreachable(t *testing.T) {
	h := driftHarness(t, opts{})
	h.step(time.Minute)
	base := actions.DigestConfig(h.conn.cfg["pg-orders"]).Hash
	h.conn.mu.Lock()
	h.conn.cfgErr["pg-orders"] = errors.New("connect: GET /connectors/pg-orders/config: 500")
	h.conn.mu.Unlock()
	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })
	h.steps(5, time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseOK {
		t.Fatalf("config_drift = %s while Connect fails, want ok", got)
	}
	if n := h.logs.count(`level=WARN msg="watch: check failed, signal state unchanged" mode=rooms pipeline=orders signal=` + driftOrders); n != 1 {
		t.Fatalf("%d warnings for one failing check, want 1\n%s", n, h.logs)
	}
	h.w.mu.Lock()
	got := h.w.pipes["orders"].drift["pg-orders"].approved.Hash
	h.w.mu.Unlock()
	if got != base {
		t.Fatalf("baseline moved while Connect failed")
	}
	h.conn.mu.Lock()
	delete(h.conn.cfgErr, "pg-orders")
	h.conn.mu.Unlock()
	h.steps(2, time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseFiring {
		t.Fatalf("config_drift = %s once Connect answers, want firing", got)
	}
}

// E2: reverting the change by hand resolves the signal after its `for`.
func TestConfigDriftResolvesOnRevert(t *testing.T) {
	h := driftHarness(t, opts{})
	h.step(time.Minute)
	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })
	h.steps(2, time.Minute)
	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueOld })
	h.step(time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseFiring {
		t.Fatalf("config_drift = %s one tick after the revert, want firing until for", got)
	}
	h.step(time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseOK {
		t.Fatalf("config_drift = %s after the revert, want ok", got)
	}
}

// E4 + E5: the first read is the baseline (a change made before the start
// never fires), and a restart starts from the current config again.
func TestConfigDriftBaselineAtStart(t *testing.T) {
	h := driftHarness(t, opts{})
	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })
	h.steps(5, time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseOK || len(h.rooms.created()) != 0 {
		t.Fatalf("config_drift = %s, rooms %d on a quiet start", got, len(h.rooms.created()))
	}
	// Drift fires, then the process restarts: the new watcher takes the
	// current config as its baseline.
	h.editCfg("orders", func(m map[string]string) { m["tasks.max"] = "2" })
	h.steps(2, time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseFiring {
		t.Fatalf("config_drift = %s, want firing", got)
	}
	w, err := New(h.deps)
	if err != nil {
		t.Fatal(err)
	}
	h.w = w
	h.steps(5, time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseOK {
		t.Fatalf("config_drift after a restart = %s, want ok (baseline resets at start)", got)
	}
}

// E5: a closed room acks the drift; the same change opens no second room.
func TestConfigDriftAckedNoSecondRoom(t *testing.T) {
	h := driftHarness(t, opts{})
	h.step(time.Minute)
	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })
	h.steps(2, time.Minute)
	cr := h.rooms.created()
	if len(cr) != 1 {
		t.Fatalf("rooms = %d, want 1", len(cr))
	}
	h.rooms.closeRoom(cr[0].roomID, h.clk.Now())
	h.w.OnRoomClosed(cr[0].roomID, "orders", DedupKey("orders"))
	h.steps(10, time.Minute)
	if got := h.phase("orders", driftOrders); got != phaseAcked {
		t.Fatalf("config_drift = %s after the close, want acked", got)
	}
	if n := len(h.rooms.created()); n != 1 {
		t.Fatalf("rooms = %d after the close, want 1", n)
	}
}

// E6: two pipelines each get their own entry; E7: past 50 changed keys the
// rest is counted in keys_omitted.
func TestConfigDriftPerPipelineAndKeyCap(t *testing.T) {
	h := driftHarness(t, opts{pipes: []pipeSpec{{id: "orders"}, {id: "payments"}}})
	h.step(time.Minute)
	h.editCfg("orders", func(m map[string]string) {
		for i := range 60 {
			m[fmt.Sprintf("transforms.t%02d.type", i)] = driftValueNew
		}
	})
	h.editCfg("payments", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })
	h.steps(2, time.Minute)
	for pipe, want := range map[string]int{"orders": events.MaxChangedKeys, "payments": 1} {
		var ev map[string]any
		for _, o := range h.rooms.created() {
			if o.alert.PipelineID == pipe {
				ev = signalEv(t, evidenceOf(t, o.alert.Raw), events.SignalConfigDrift)
			}
		}
		if ev == nil {
			t.Fatalf("no room for %s", pipe)
		}
		keys, _ := ev["changed_keys"].([]any)
		if len(keys) != want || ev["connector"] != "pg-"+pipe {
			t.Fatalf("%s: %d keys, connector %v; want %d", pipe, len(keys), ev["connector"], want)
		}
		if pipe == "orders" && num(t, ev, "keys_omitted") != 10 {
			t.Fatalf("keys_omitted = %v, want 10", ev["keys_omitted"])
		}
	}
	noValues(t, h)
}

// A connector that leaves the pipeline stops its drift and drops its
// baseline.
func TestConfigDriftConnectorLeavesPipeline(t *testing.T) {
	h := driftHarness(t, opts{})
	h.step(time.Minute)
	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })
	h.steps(2, time.Minute)
	h.w.mu.Lock()
	ps := h.w.pipes["orders"]
	// As if owners.yaml no longer listed it: a state for a gone connector.
	ps.sigs[configDriftID("pg-gone")] = &sigState{phase: phaseFiring, last: reading{sev: "sev3"}}
	ps.driftBase("pg-gone").ok = true
	h.w.mu.Unlock()
	h.steps(3, time.Minute)
	h.w.mu.Lock()
	_, sig := ps.sigs[configDriftID("pg-gone")]
	_, base := ps.drift["pg-gone"]
	h.w.mu.Unlock()
	if sig || base {
		t.Fatalf("gone connector kept state: signal %v baseline %v", sig, base)
	}
	if got := h.phase("orders", driftOrders); got != phaseFiring {
		t.Fatalf("the pipeline's own connector = %s, want still firing", got)
	}
}

// The hook is a no-op for an unknown pipeline or connector and before the
// first read.
func TestApproveConnectorChangeNoState(t *testing.T) {
	h := driftHarness(t, opts{})
	h.w.ApproveConnectorChange("orders", "pg-orders") // before any read
	h.w.ApproveConnectorChange("nope", "pg-orders")
	h.w.ApproveConnectorChange("", "")
	h.step(time.Minute)
	h.w.mu.Lock()
	b := h.w.pipes["orders"].drift["pg-orders"]
	_, other := h.w.pipes["nope"]
	h.w.mu.Unlock()
	if b == nil || !b.ok || !b.rebaseAt.IsZero() || other {
		t.Fatalf("baseline %+v, state for an unknown pipeline %v", b, other)
	}
}
