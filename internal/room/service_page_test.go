package room_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// pagedEvents returns the owner.paged events of a room.
func pagedEvents(t *testing.T, svc *room.Service, roomID string) []events.Event {
	t.Helper()
	var out []events.Event
	for _, e := range roomEvents(t, svc, roomID) {
		if e.Type == events.OwnerPaged {
			out = append(out, e)
		}
	}
	return out
}

func pagedPayload(t *testing.T, e events.Event) events.OwnerPagedPayload {
	t.Helper()
	p, err := events.Decode[events.OwnerPagedPayload](e)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPageOwnersValidation(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, _, _ := newTestService(t, pg)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	priya := mustUser(t, svc, "priya")

	for _, tc := range []struct {
		name, layer, reason, proposal string
	}{
		{"unknown layer", "network", "look", ""},
		{"empty layer", "", "look", ""},
		{"empty reason", "consumer", "", ""},
		{"blank reason", "consumer", "  \n ", ""},
		{"reason too long", "consumer", strings.Repeat("é", 301), ""},
		{"NUL in reason", "consumer", "a\x00b", ""},
		{"invalid UTF-8", "consumer", "a\xffb", ""},
		{"proposal not in the room", "consumer", "look", "p_missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.PageOwners(ctx, priya, roomID, tc.layer, tc.reason, tc.proposal)
			wantCode(t, err, "validation")
		})
	}
	if n := len(pagedEvents(t, svc, roomID)); n != 0 {
		t.Fatalf("refused pages wrote %d events", n)
	}
	// 300 characters is allowed.
	if _, err := svc.PageOwners(ctx, priya, roomID, "consumer", strings.Repeat("é", 300), ""); err != nil {
		t.Fatalf("300 characters: %v", err)
	}
	_, err := svc.PageOwners(ctx, priya, "r_missing", "consumer", "look", "")
	wantCode(t, err, "not_found")

	// An open proposal of the room may be named.
	pid, err := svc.CreateProposal(ctx, roomID, pgPayload(`{"table":"public.orders"}`), events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := svc.PageOwners(ctx, priya, roomID, "postgres", "approve please", pid)
	if err != nil {
		t.Fatalf("page with a proposal: %v", err)
	}
	if p := pagedPayload(t, ev); p.ProposalID != pid || p.Auto || ev.Actor.ID != "priya" {
		t.Fatalf("event = %+v %+v", ev, p)
	}

	if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "arjun"), roomID, "done"); err != nil {
		t.Fatal(err)
	}
	_, err = svc.PageOwners(ctx, priya, roomID, "kafka", "look", "")
	wantCode(t, err, "room_closed")
}

func TestPageOwnersRateLimitAndSnapshot(t *testing.T) {
	svc, _, clk := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	priya, kabir := mustUser(t, svc, "priya"), mustUser(t, svc, "kabir")

	ev, err := svc.PageOwners(ctx, priya, roomID, "consumer", "lag keeps growing", "")
	if err != nil {
		t.Fatal(err)
	}
	p := pagedPayload(t, ev)
	// testOwners has no layers: the users.yaml layers (dev mode) decide.
	if p.Layer != "consumer" || len(p.Owners) != 1 || p.Owners[0] != "kabir" || p.Reason != "lag keeps growing" {
		t.Fatalf("payload = %+v", p)
	}

	clk.Advance(3 * time.Minute)
	_, err = svc.PageOwners(ctx, kabir, roomID, "consumer", "again", "")
	wantCode(t, err, "already_paged")
	if !strings.Contains(err.Error(), "paged 3 min ago") {
		t.Fatalf("message = %q", err.Error())
	}
	// Another layer is not limited.
	if _, err := svc.PageOwners(ctx, kabir, roomID, "warehouse", "rows missing", ""); err != nil {
		t.Fatal(err)
	}
	clk.Advance(7 * time.Minute)
	if _, err := svc.PageOwners(ctx, kabir, roomID, "consumer", "still lagging", ""); err != nil {
		t.Fatalf("after the window: %v", err)
	}

	// Snapshot: the newest page per layer, in seq order; GET /api/rooms rows too.
	snap, err := svc.GetRoom(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Pages) != 2 || snap.Pages[0].Layer != "warehouse" || snap.Pages[1].Layer != "consumer" ||
		snap.Pages[1].Reason != "still lagging" || snap.Pages[1].By.ID != "kabir" || snap.Pages[0].Owners[0] != "meera" {
		t.Fatalf("pages = %+v", snap.Pages)
	}
	rows, err := svc.ListRooms(ctx)
	if err != nil || len(rows) != 1 || len(rows[0].Pages) != 2 {
		t.Fatalf("list rows = %+v, %v", rows, err)
	}
	b, _ := json.Marshal(snap)
	if !strings.Contains(string(b), `"pages":[{"layer":"warehouse","owners":["meera"],"reason":"rows missing","proposal_id":"","by":{"kind":"human","id":"kabir","name":"Kabir","verified":true},"auto":false,"at":`) {
		t.Fatalf("snapshot JSON: %s", b)
	}

	// A closed room shows none.
	if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "arjun"), roomID, "done"); err != nil {
		t.Fatal(err)
	}
	snap, _ = svc.GetRoom(ctx, roomID)
	if snap.Pages == nil || len(snap.Pages) != 0 {
		t.Fatalf("closed room pages = %#v, want []", snap.Pages)
	}
	// A new room starts with [] (never null).
	other := createRoom(t, svc, "arjun")
	if other.Pages == nil {
		t.Fatal("new room pages is nil")
	}
}

func TestPageOwnersLease(t *testing.T) {
	svc, _, clk := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	before, _ := svc.GetRoom(ctx, roomID)

	// A non-driver's page does not renew the driver lease.
	clk.Advance(10 * time.Minute)
	if _, err := svc.PageOwners(ctx, mustUser(t, svc, "priya"), roomID, "postgres", "slot", ""); err != nil {
		t.Fatal(err)
	}
	snap, _ := svc.GetRoom(ctx, roomID)
	if !snap.Driver.LeaseUntil.Equal(before.Driver.LeaseUntil) {
		t.Fatalf("non-driver page moved the lease: %v -> %v", before.Driver.LeaseUntil, snap.Driver.LeaseUntil)
	}
	// The driver's does.
	if _, err := svc.PageOwners(ctx, mustUser(t, svc, "arjun"), roomID, "schema-registry", "who owns the subject?", ""); err != nil {
		t.Fatal(err)
	}
	snap, _ = svc.GetRoom(ctx, roomID)
	if !snap.Driver.LeaseUntil.After(before.Driver.LeaseUntil) {
		t.Fatalf("driver page did not renew the lease: %v", snap.Driver.LeaseUntil)
	}
}

// agentRig is a service whose agent runs record their Deps and wait.
type agentRig struct {
	svc   *room.Service
	clk   *clock
	mu    sync.Mutex
	deps  []agent.Deps
	ended chan struct{}
}

func newAgentRig(t *testing.T, run func(ctx context.Context, d agent.Deps) error) *agentRig {
	t.Helper()
	r := &agentRig{clk: &clock{t: time.Date(2026, 10, 15, 4, 0, 0, 0, time.UTC)}, ended: make(chan struct{}, 8)}
	r.svc = room.New(room.Deps{
		Store: memstore.NewWithClock(r.clk.Now), Users: room.DefaultUsers(), Owners: testOwners(), Now: r.clk.Now,
		WriteHandoff: fakeWrite,
		Run: func(ctx context.Context, _ agent.Alert, d agent.Deps, _ agent.Options) error {
			r.mu.Lock()
			r.deps = append(r.deps, d)
			r.mu.Unlock()
			defer func() { r.ended <- struct{}{} }()
			return run(ctx, d)
		},
	})
	t.Cleanup(r.svc.Close)
	return r
}

func (r *agentRig) waitDeps(t *testing.T, n int) agent.Deps {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.deps) >= n
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.deps[n-1]
}

func (r *agentRig) runs() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.deps)
}

func blockRun(ctx context.Context, _ agent.Deps) error {
	<-ctx.Done()
	return nil
}

func TestAgentPager(t *testing.T) {
	r := newAgentRig(t, blockRun)
	ctx := context.Background()
	snap, _, err := r.svc.OpenAlertRoom(ctx, alertin.Alert{
		Source: alertin.Alertmanager, Status: alertin.StatusFiring, Severity: "sev2", PipelineID: "orders",
		Title: "lag", DedupKey: "am:lag",
	})
	if err != nil {
		t.Fatal(err)
	}
	pager := r.waitDeps(t, 1).Pager
	if pager == nil {
		t.Fatal("the agent of a live room has no Pager")
	}

	_, err = pager.Page(ctx, "networking", "x", "")
	wantCode(t, err, "validation")
	_, err = pager.Page(ctx, "consumer", " \n ", "")
	wantCode(t, err, "validation")

	reason := "Kabir: consumer order-sync\nlag is 40k\x00 and rising " + strings.Repeat("x", 400)
	owners, err := pager.Page(ctx, "consumer", reason, "")
	if err != nil {
		t.Fatalf("page: %v", err)
	}
	if len(owners) != 1 || owners[0] != "kabir" {
		t.Fatalf("owners = %v", owners)
	}
	evs := pagedEvents(t, r.svc, snap.ID)
	if len(evs) != 1 || evs[0].Actor != events.AgentActor {
		t.Fatalf("events = %+v", evs)
	}
	p := pagedPayload(t, evs[0])
	if strings.ContainsAny(p.Reason, "\n\x00") || !strings.HasPrefix(p.Reason, "Kabir: consumer order-sync lag is 40k� and rising") ||
		len([]rune(p.Reason)) != room.MaxPageReasonChars || !strings.HasSuffix(p.Reason, "…") {
		t.Fatalf("reason not cleaned and clipped: %q (%d)", p.Reason, len([]rune(p.Reason)))
	}

	r.clk.Advance(2 * time.Minute)
	_, err = pager.Page(ctx, "consumer", "again", "")
	wantCode(t, err, "already_paged")
	if !strings.Contains(err.Error(), "paged 2 min ago") {
		t.Fatalf("message = %q", err.Error())
	}
	_, err = pager.Page(ctx, "consumer", "x", "p_missing")
	wantCode(t, err, "validation")
}

func TestShadowRoomAgentHasNoPager(t *testing.T) {
	var mu sync.Mutex
	var got []agent.Deps
	e := newShadowEnv(t, shadowCfg{fn: func(ctx context.Context, _ int, d agent.Deps) error {
		mu.Lock()
		got = append(got, d)
		mu.Unlock()
		<-ctx.Done()
		return nil
	}})
	snap := e.open(t, "k1")
	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 1
	})
	mu.Lock()
	pager := got[0].Pager
	mu.Unlock()
	if pager != nil {
		t.Fatal("a shadow run got a Pager")
	}
	// A new incident does not start a second shadow run either.
	if err := e.svc.PostIncidentJSON(context.Background(), snap.ID, "watcher: new signal", json.RawMessage(`{"source":"watcher"}`)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	n := len(got)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("shadow runs = %d, want 1", n)
	}
}

// ---- new incidents (G1) ----

func incidentRaw(signal string) json.RawMessage {
	return json.RawMessage(`{"source":"watcher","pipeline":"orders","severity":"sev2","reason":"new signal","signals":[{"signal":"` + signal + `","severity":"sev2"}]}`)
}

// waitReturns reports whether in.Wait returns within d.
func waitReturns(in agent.Inbox, d time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return in.Wait(ctx) == nil
}

func TestNewIncidentWakesIdleAgent(t *testing.T) {
	r := newAgentRig(t, blockRun)
	ctx := context.Background()
	snap, _, err := r.svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil {
		t.Fatal(err)
	}
	in := r.waitDeps(t, 1).Inbox
	_, _ = in.Drain(ctx)

	// A repeat of a known signal is quiet: it never wakes the agent.
	if err := r.svc.PostRoomEvidence(ctx, snap.ID, "watcher: still firing", incidentRaw("connector_down")); err != nil {
		t.Fatal(err)
	}
	if waitReturns(in, 50*time.Millisecond) {
		t.Fatal("a quiet alert update woke the agent")
	}
	// A new incident does.
	if err := r.svc.PostIncidentJSON(ctx, snap.ID, "watcher: orders: consumer lag (new signal)", incidentRaw("consumer_lag")); err != nil {
		t.Fatal(err)
	}
	if !waitReturns(in, 2*time.Second) {
		t.Fatal("a new incident did not wake the agent")
	}
	items, err := in.Drain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var quiet, incident int
	for _, it := range items {
		switch {
		case it.Kind == "alert_update" && it.NewIncident:
			incident++
			if it.Actor.Kind != events.ActorSystem || !strings.Contains(it.Text, "consumer_lag") {
				t.Errorf("incident input = %+v", it)
			}
		case it.Kind == "alert_update":
			quiet++
		}
	}
	if incident != 1 || quiet != 1 {
		t.Fatalf("inputs = %+v; want the quiet update and one new incident", items)
	}
	if n := r.runs(); n != 1 {
		t.Fatalf("runs = %d, want 1: a running agent is woken, not restarted", n)
	}
	// The note is in the room for the people.
	if texts := systemTexts(t, r.svc, snap.ID); !strings.Contains(strings.Join(texts, "\n"), "consumer lag (new signal)") {
		t.Fatalf("notes = %v", texts)
	}
}

func TestNewIncidentRestartsAgentAfterRestart(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	ctx := context.Background()
	snap, _, err := svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return h.runs.Load() == 1 })
	svc.Close()

	// After a restart the room has no agent run: a quiet update starts none,
	// a new incident starts one (through the slots, as a steer would) and the
	// run reads the incident first.
	svc = h.build()
	if err := svc.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if err := svc.PostRoomEvidence(ctx, snap.ID, "watcher: still firing", incidentRaw("connector_down")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if n := h.runs.Load(); n != 1 {
		t.Fatalf("a quiet update started an agent: runs = %d", n)
	}
	if err := svc.PostIncidentJSON(ctx, snap.ID, "watcher: new signal", incidentRaw("consumer_lag")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return h.runs.Load() == 2 })
	items, err := h.inbox(1).Drain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, it := range items {
		found = found || (it.Kind == "alert_update" && it.NewIncident)
	}
	if !found {
		t.Fatalf("restarted run's inbox = %+v, want the new incident", items)
	}
	// A second incident while the run is going wakes it; never a second run.
	if err := svc.PostIncidentJSON(ctx, snap.ID, "watcher: another signal", incidentRaw("slot_wal")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if n := h.runs.Load(); n != 2 {
		t.Fatalf("runs = %d, want 2", n)
	}
}

func TestNewIncidentRestartsFinishedRun(t *testing.T) {
	// The first run ends on its own at once; later runs wait.
	var calls int
	var mu sync.Mutex
	r := newAgentRig(t, func(ctx context.Context, d agent.Deps) error {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			return nil
		}
		<-ctx.Done()
		return nil
	})
	ctx := context.Background()
	snap, _, err := r.svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil {
		t.Fatal(err)
	}
	r.waitDeps(t, 1)
	select {
	case <-r.ended:
	case <-time.After(5 * time.Second):
		t.Fatal("first run did not end")
	}
	// The supervisor marks the run finished right after Run returns.
	waitFor(t, 5*time.Second, func() bool {
		if err := r.svc.PostIncidentJSON(ctx, snap.ID, "watcher: new signal", incidentRaw("consumer_lag")); err != nil {
			t.Fatal(err)
		}
		return r.runs() == 2
	})
	in := r.waitDeps(t, 2).Inbox
	items, _ := in.Drain(ctx)
	if len(items) == 0 || !items[len(items)-1].NewIncident {
		t.Fatalf("new run inbox = %+v", items)
	}
	time.Sleep(30 * time.Millisecond)
	if n := r.runs(); n != 2 {
		t.Fatalf("runs = %d, want 2", n)
	}
}

func TestPostIncidentErrors(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	ctx := context.Background()
	snap, _, err := svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil {
		t.Fatal(err)
	}
	wantCode(t, svc.PostIncidentJSON(ctx, "r_missing", "x", nil), "not_found")
	wantCode(t, svc.PostIncidentJSON(ctx, snap.ID, "  ", nil), "validation")
	h.closeRoom(svc, snap.ID)
	wantCode(t, svc.PostIncidentJSON(ctx, snap.ID, "x", nil), "room_closed")
}

// ---- OpenRoomInfo (G1) ----

func TestOpenRoomInfo(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	ctx := context.Background()
	if _, ok := svc.OpenRoomInfo("orders"); ok {
		t.Fatal("no rooms: want ok false")
	}
	if _, ok := svc.OpenRoomInfo("bad id!"); ok {
		t.Fatal("bad pipeline id matched")
	}
	opened := h.clock.Now()
	w, _, err := svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil {
		t.Fatal(err)
	}
	info, ok := svc.OpenRoomInfo("orders")
	if !ok || info.RoomID != w.ID || info.AlertSource != "watcher" || !info.OpenedAt.Equal(opened) || !info.LastEventAt.Equal(opened) {
		t.Fatalf("info = %+v %v", info, ok)
	}

	// A comment is activity.
	h.clock.Advance(30 * time.Minute)
	if _, err := svc.AddComment(ctx, mustUser(t, svc, "priya"), w.ID, "", "looking"); err != nil {
		t.Fatal(err)
	}
	commented := h.clock.Now()
	info, _ = svc.OpenRoomInfo("orders")
	if !info.LastEventAt.Equal(commented) || !info.OpenedAt.Equal(opened) {
		t.Fatalf("after a comment: %+v", info)
	}

	// A restart writes the restart notice and an idle status: not activity.
	h.clock.Advance(3 * time.Hour)
	svc.Close()
	svc = h.build()
	if err := svc.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if texts := systemTexts(t, svc, w.ID); !strings.Contains(strings.Join(texts, "\n"), "server restarted") {
		t.Fatalf("no restart notice: %v", texts)
	}
	info, _ = svc.OpenRoomInfo("orders")
	if !info.LastEventAt.Equal(commented) {
		t.Fatalf("after a restart LastEventAt = %v, want the comment's %v", info.LastEventAt, commented)
	}
	// A watcher note is activity.
	if err := svc.PostRoomEvidence(ctx, w.ID, "watcher: still firing", nil); err != nil {
		t.Fatal(err)
	}
	if info, _ = svc.OpenRoomInfo("orders"); !info.LastEventAt.Equal(h.clock.Now()) {
		t.Fatalf("after a note: %+v", info)
	}

	// The newest open room is described; a person's room has no alert source.
	h.clock.Advance(time.Minute)
	manual, err := svc.CreateRoom(ctx, mustUser(t, svc, "arjun"), "test", "sev3", "orders")
	if err != nil {
		t.Fatal(err)
	}
	info, _ = svc.OpenRoomInfo("orders")
	if info.RoomID != manual.ID || info.AlertSource != "" {
		t.Fatalf("manual room info = %+v", info)
	}
	h.closeRoom(svc, manual.ID)
	h.closeRoom(svc, w.ID)
	if _, ok := svc.OpenRoomInfo("orders"); ok {
		t.Fatal("all closed: want ok false")
	}
}
