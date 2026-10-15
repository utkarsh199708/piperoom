package room

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/auth"
	"piperoom/internal/events"
	"piperoom/internal/notify"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/watch"
)

var pageT0 = time.Date(2026, 10, 15, 4, 0, 0, 0, time.UTC)

// pageRig is one Service with a hand-fed auto-page ticker; tick runs one
// check and returns when it has finished.
type pageRig struct {
	t     *testing.T
	svc   *Service
	ticks chan time.Time
	done  chan struct{}
	stops chan struct{}
	made  atomic.Int32
}

func newPageRig(t *testing.T, st store.Store, c *testClock, after time.Duration, n PageNotifier) *pageRig {
	t.Helper()
	r := &pageRig{t: t, ticks: make(chan time.Time), done: make(chan struct{}), stops: make(chan struct{}, 1)}
	r.svc = New(Deps{
		Store: st, Users: DefaultUsers(), Owners: &owners.Owners{Pipelines: []owners.Pipeline{
			{ID: "orders", SourceTables: []string{"public.orders"}, ConsumerGroups: []string{"order-sync"}},
		}},
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		Now: c.Now,
		Paging: Paging{UndrivenAfter: after, Notifier: n, Ticker: func(d time.Duration) (<-chan time.Time, func()) {
			if d != AutoPageCheckEvery {
				t.Errorf("ticker period %v, want %v", d, AutoPageCheckEvery)
			}
			r.made.Add(1)
			return r.ticks, func() { r.stops <- struct{}{} }
		}},
	})
	r.svc.paging.tickDone = func() { r.done <- struct{}{} }
	t.Cleanup(r.svc.Close)
	return r
}

func (r *pageRig) load() {
	r.t.Helper()
	if err := r.svc.Load(context.Background()); err != nil {
		r.t.Fatalf("load: %v", err)
	}
}

func (r *pageRig) tick() {
	r.t.Helper()
	r.ticks <- time.Time{}
	<-r.done
}

// openWatcherRoom opens a watcher room whose evidence lists signals in order.
func openWatcherRoom(t *testing.T, svc *Service, signals ...string) string {
	t.Helper()
	var sigs []map[string]any
	for _, s := range signals {
		sigs = append(sigs, map[string]any{"signal": s, "severity": "sev2"})
	}
	raw, _ := json.Marshal(map[string]any{"source": "watcher", "pipeline": "orders", "severity": "sev2", "reason": "opened", "signals": sigs})
	snap, _, err := svc.OpenAlertRoom(context.Background(), alertin.Alert{
		Source: alertin.Watcher, Status: alertin.StatusFiring, Severity: "sev2", PipelineID: "orders",
		Title: "orders: watcher", DedupKey: "watcher:orders", Raw: raw,
	})
	if err != nil {
		t.Fatal(err)
	}
	return snap.ID
}

func autoPages(t *testing.T, st store.Store, roomID string) []events.OwnerPagedPayload {
	t.Helper()
	evs, err := st.Since(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var out []events.OwnerPagedPayload
	for _, e := range evs {
		if e.Type != events.OwnerPaged {
			continue
		}
		p, err := events.Decode[events.OwnerPagedPayload](e)
		if err != nil {
			t.Fatal(err)
		}
		if p.Auto {
			if e.Actor != systemActor {
				t.Fatalf("auto page actor = %+v, want the system actor", e.Actor)
			}
			out = append(out, p)
		}
	}
	return out
}

func TestAutoPageTimingOnceAndRestart(t *testing.T) {
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	r := newPageRig(t, ms, c, 5*time.Minute, nil)
	r.load()
	roomID := openWatcherRoom(t, r.svc, owners.SignalConnectorDown, owners.SignalConsumerLag)

	c.Advance(4*time.Minute + 59*time.Second)
	r.tick()
	if n := len(autoPages(t, ms, roomID)); n != 0 {
		t.Fatalf("before PIPEROOM_PAGE_UNDRIVEN_AFTER: %d pages", n)
	}
	c.Advance(time.Second)
	r.tick()
	got := autoPages(t, ms, roomID)
	if len(got) != 1 {
		t.Fatalf("auto pages = %d, want 1", len(got))
	}
	// The first signal's layer; DefaultUsers gives kafka-connect to arjun.
	if got[0].Layer != "kafka-connect" || !reflect.DeepEqual(got[0].Owners, []string{"arjun"}) ||
		!strings.Contains(got[0].Reason, "connector_down") || !strings.Contains(got[0].Reason, "5 min") {
		t.Fatalf("page = %+v", got[0])
	}
	snap := mustRoom(t, r.svc, roomID)
	if len(snap.Pages) != 1 || !snap.Pages[0].Auto || snap.Pages[0].By != systemActor {
		t.Fatalf("snapshot pages = %+v", snap.Pages)
	}

	// Once per room: later checks, past the 10-minute window, write nothing.
	c.Advance(20 * time.Minute)
	r.tick()
	if n := len(autoPages(t, ms, roomID)); n != 1 {
		t.Fatalf("after another check: %d pages, want 1", n)
	}
	// A restart reads the mark from the log.
	r.svc.Close()
	r2 := newPageRig(t, ms, c, 5*time.Minute, nil)
	r2.load()
	r2.tick()
	if n := len(autoPages(t, ms, roomID)); n != 1 {
		t.Fatalf("after a restart: %d pages, want 1", n)
	}
}

func TestAutoPageSkips(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	r := newPageRig(t, ms, c, 5*time.Minute, nil)
	r.load()

	// A room someone drives is never auto-paged.
	driven := openWatcherRoom(t, r.svc, owners.SignalSlotWAL)
	if _, err := r.svc.GrantControl(ctx, leaseUser(t, r.svc, "priya"), driven, "priya"); err != nil {
		t.Fatal(err)
	}
	// A person's room is not an alert room.
	manual, err := r.svc.CreateRoom(ctx, leaseUser(t, r.svc, "arjun"), "by hand", "sev3", "orders")
	if err != nil {
		t.Fatal(err)
	}
	// An Alertmanager room has no watcher signal; the triage pass names the layer.
	am, _, err := r.svc.OpenAlertRoom(ctx, alertin.Alert{
		Source: alertin.Alertmanager, Status: alertin.StatusFiring, Severity: "sev2", PipelineID: "orders",
		Title: "lag", DedupKey: "am:lag",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Triage is in before the check: the auto page names its layer (no fallback).
	if err := r.svc.emitAs(ctx, am.ID, events.AgentActor, events.AgentTriage, events.AgentTriagePayload{
		Status: "ok", Headline: "consumer stuck", SuspectedLayer: "consumer", EvidenceRefs: []string{},
	}); err != nil {
		t.Fatal(err)
	}
	c.Advance(6 * time.Minute)
	r.tick()
	for _, id := range []string{driven, manual.ID} {
		if n := len(autoPages(t, ms, id)); n != 0 {
			t.Fatalf("room %s: %d auto pages, want 0", id, n)
		}
	}
	got := autoPages(t, ms, am.ID)
	if len(got) != 1 || got[0].Layer != "consumer" || !reflect.DeepEqual(got[0].Owners, []string{"kabir"}) || !strings.Contains(got[0].Reason, "first look") {
		t.Fatalf("triage auto page = %+v", got)
	}
}

func TestAutoPageWaitsForAPageOfTheLayer(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	r := newPageRig(t, ms, c, 5*time.Minute, nil)
	r.load()
	roomID := openWatcherRoom(t, r.svc, owners.SignalConnectorDown)
	c.Advance(2 * time.Minute)
	if _, err := r.svc.PageOwners(ctx, leaseUser(t, r.svc, "kabir"), roomID, "kafka-connect", "connector is down", ""); err != nil {
		t.Fatal(err)
	}
	c.Advance(3 * time.Minute)
	r.tick()
	if n := len(autoPages(t, ms, roomID)); n != 0 {
		t.Fatalf("auto page inside the window of a person's page: %d", n)
	}
	c.Advance(6 * time.Minute) // 9 min after the person's page
	r.tick()
	if n := len(autoPages(t, ms, roomID)); n != 0 {
		t.Fatalf("auto page inside the window: %d", n)
	}
	c.Advance(time.Minute)
	r.tick()
	if n := len(autoPages(t, ms, roomID)); n != 1 {
		t.Fatalf("after the window: %d auto pages, want 1", n)
	}
}

func TestAutoPageOffStartsNoLoop(t *testing.T) {
	c := &testClock{t: pageT0}
	r := newPageRig(t, memstore.NewWithClock(c.Now), c, 0, nil)
	r.load()
	if n := r.made.Load(); n != 0 {
		t.Fatalf("PIPEROOM_PAGE_UNDRIVEN_AFTER=0 made %d tickers", n)
	}
}

func TestAutoPageLoopStopsOnClose(t *testing.T) {
	c := &testClock{t: pageT0}
	r := newPageRig(t, memstore.NewWithClock(c.Now), c, time.Minute, nil)
	r.load()
	r.tick()
	r.svc.Close()
	select {
	case <-r.stops:
	default:
		t.Fatal("Close returned but the auto-page loop had not stopped its ticker")
	}
}

func TestAutoPageReplicas(t *testing.T) {
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	setup := newPageRig(t, ms, c, 5*time.Minute, nil)
	elsewhere := openWatcherRoom(t, setup.svc, owners.SignalConnectorDown)
	// A second watcher room needs its own key.
	raw := json.RawMessage(`{"source":"watcher","signals":[{"signal":"consumer_lag"}]}`)
	snap, _, err := setup.svc.OpenAlertRoom(context.Background(), alertin.Alert{
		Source: alertin.Watcher, Status: alertin.StatusFiring, Severity: "sev2", PipelineID: "orders",
		Title: "orders: lag", DedupKey: "watcher:orders-2", Raw: raw,
	})
	if err != nil {
		t.Fatal(err)
	}
	unheld := snap.ID
	setup.svc.Close()

	fc := &fakeCoord{Store: ms, leases: map[string]store.RoomLease{elsewhere: {RoomID: elsewhere, Owner: "replica-b"}}}
	svc := New(Deps{Store: fc, Users: DefaultUsers(), Now: c.Now, ReplicaID: "replica-a", Paging: Paging{UndrivenAfter: 5 * time.Minute}})
	t.Cleanup(svc.Close)
	mustRoom(t, svc, elsewhere)
	mustRoom(t, svc, unheld)
	c.Advance(6 * time.Minute)
	ctx := context.Background()
	svc.checkAutoPages(ctx)
	if n := len(autoPages(t, ms, elsewhere)); n != 0 {
		t.Fatalf("room held by another replica: %d pages", n)
	}
	if got := autoPages(t, ms, unheld); len(got) != 1 || got[0].Layer != "consumer" {
		t.Fatalf("unheld room: %+v", got)
	}

	// Another replica paged first: this one folds it on the conflict of its
	// checked append and writes nothing.
	svc2 := New(Deps{Store: fc, Users: DefaultUsers(), Now: c.Now, ReplicaID: "replica-c", Paging: Paging{UndrivenAfter: 5 * time.Minute}})
	t.Cleanup(svc2.Close)
	mustRoom(t, svc2, elsewhere)
	fc.leases = map[string]store.RoomLease{}
	if _, err := ms.Append(ctx, elsewhere, events.OwnerPaged, systemActor, events.OwnerPagedPayload{
		Layer: "kafka-connect", Owners: []string{"arjun"}, Reason: "auto", Auto: true,
	}); err != nil {
		t.Fatal(err)
	}
	svc2.autoPage(ctx, elsewhere) // its cache is one event behind until the sync
	if n := len(autoPages(t, ms, elsewhere)); n != 1 {
		t.Fatalf("after another replica's page: %d pages, want 1", n)
	}
}

func TestFirstWatcherSignalAndLayer(t *testing.T) {
	svc := New(Deps{Store: memstore.New(), Owners: &owners.Owners{Pipelines: []owners.Pipeline{
		{ID: "orders", ConsumerGroups: []string{"order-sync", "audit"}},
	}}})
	t.Cleanup(svc.Close)
	for _, tc := range []struct {
		name, raw, signal, layer string
	}{
		{"compose body", `{"source":"watcher","signals":[{"signal":"slot_wal"},{"signal":"connector_down"}]}`, "slot_wal", "postgres"},
		{"unknown first signal is skipped", `{"signals":[{"signal":"future_thing"},{"signal":"group_unstable"}]}`, "group_unstable", "consumer"},
		{"top-level signal", `{"signal":"connector_down"}`, "connector_down", "kafka-connect"},
		{"dlq by consumer groups", `{"signals":[{"signal":"dlq_growth","writers":["order-sync","audit"]}]}`, "dlq_growth", "consumer"},
		{"dlq with a connector writer", `{"signals":[{"signal":"dlq_growth","writers":["order-sync","pg-sink"]}]}`, "dlq_growth", "kafka-connect"},
		{"dlq with odd writers", `{"signals":[{"signal":"dlq_growth","writers":"x"}]}`, "dlq_growth", "kafka-connect"},
		{"config risk", `{"signals":[{"signal":"config_risk"}]}`, "config_risk", "kafka-connect"},
		{"offsets", `{"signals":[{"signal":"offset_below_log_start"}]}`, "offset_below_log_start", "consumer"},
		{"slot stall (QA F22)", `{"source":"watcher","signals":[{"signal":"slot_stall","layer":"postgres"}]}`, "slot_stall", "postgres"},
		{"evidence layer of a signal this table does not know", `{"signals":[{"signal":"future_thing","layer":"warehouse"}]}`, "future_thing", "warehouse"},
		{"evidence layer that is not a layer", `{"signals":[{"signal":"future_thing","layer":"moon"},{"signal":"slot_wal"}]}`, "slot_wal", "postgres"},
		{"no signals", `{"source":"watcher","signals":[]}`, "", ""},
		{"not JSON", `not json`, "", ""},
		{"empty", ``, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sig, writers, evLayer := firstWatcherSignal(json.RawMessage(tc.raw))
			if sig != tc.signal {
				t.Fatalf("signal = %q, want %q", sig, tc.signal)
			}
			if sig == "" {
				return
			}
			if l := svc.signalLayer("orders", sig, writers, evLayer); l != tc.layer {
				t.Fatalf("layer = %q, want %q", l, tc.layer)
			}
		})
	}
}

// TestSignalLayersMatchWatcher: every signal the watcher can open a room on
// has a layer here, the same one watch.SignalLayer gives (QA F22: slot_stall
// was missing, so an undriven slot_stall room was never auto-paged).
func TestSignalLayersMatchWatcher(t *testing.T) {
	for _, sig := range []string{owners.SignalConnectorDown, owners.SignalSlotInactive, owners.SignalSlotWAL,
		owners.SignalDLQGrowth, owners.SignalConfigRisk, owners.SignalConsumerLag, owners.SignalGroupUnstable,
		owners.SignalOffsetBelowLogStart, watch.SignalSlotStall} {
		want := watch.SignalLayer(sig)
		if want == "" {
			t.Fatalf("watch.SignalLayer(%q) is empty", sig)
		}
		if got := signalLayers[sig]; got != want {
			t.Errorf("signalLayers[%q] = %q, want %q (watch.SignalLayer)", sig, got, want)
		}
	}
}

const pageOwnersYAML = `pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
    owners:
      consumer: { approvers: [user:kabir, group:consumers], backup: [user:meera, group:night] }
  - id: billing
    topic_prefix: pb
    source_tables: [public.bills]
    connectors: [c2]
layers:
  postgres: priya
  kafka: [user:arjun, group:dataeng-oncall]
  consumer: user:arjun
`

func pageOwnersService(t *testing.T, sessions auth.Store, n PageNotifier) *Service {
	t.Helper()
	path := t.TempDir() + "/owners.yaml"
	if err := os.WriteFile(path, []byte(pageOwnersYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	svc := New(Deps{
		Store: memstore.New(), Owners: own, Sessions: sessions, SessionIdle: time.Hour,
		Users: mustUsers([]User{
			{ID: "arjun", Name: "Arjun", Email: "arjun@example.com"},
			{ID: "priya", Name: "Priya"},
			{ID: "kabir", Name: "Kabir", Email: "kabir@example.com"},
			{ID: "meera", Name: "Meera", Layers: []string{"warehouse"}},
		}),
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		Paging: Paging{Notifier: n},
	})
	t.Cleanup(svc.Close)
	return svc
}

func addSession(t *testing.T, sessions *auth.MemStore, user, name, email string, groups ...string) {
	t.Helper()
	now := time.Now().UTC()
	_, hash, _ := auth.NewToken()
	if err := sessions.Create(context.Background(), auth.Session{ID: "s_" + user, TokenHash: hash, UserID: user, Name: name, Email: email,
		Groups: groups, Method: auth.MethodOIDC, CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
}

func TestPageOwnersResolution(t *testing.T) {
	sessions := auth.NewMemStore()
	addSession(t, sessions, "u_night", "Nia", "", "night")
	addSession(t, sessions, "u_cons", "Cole", "cole@example.com", "consumers")
	addSession(t, sessions, "u_kafka", "Kai", "", "dataeng-oncall")
	addSession(t, sessions, "u_other", "Oz", "", "unrelated")
	svc := pageOwnersService(t, sessions, nil)
	ctx := context.Background()
	for _, tc := range []struct {
		pipeline, layer string
		want            []string
	}{
		// Pipeline owners: approvers (file users, then group members), then backups.
		// Group principals follow the people found in them: the server
		// cannot list a group's members (QA F6).
		{"orders", "consumer", []string{"kabir", "u_cons", "group:consumers", "meera", "u_night", "group:night"}},
		// No pipeline block: layers:.
		{"billing", "consumer", []string{"arjun"}},
		{"orders", "kafka", []string{"arjun", "u_kafka", "group:dataeng-oncall"}},
		// No owners.yaml entry: the users.yaml layers (dev mode) decide.
		{"orders", "warehouse", []string{"meera"}},
		// Nobody: [] (never nil).
		{"orders", "schema-registry", []string{}},
		// A pipeline owners.yaml does not know has nobody (fail closed).
		{"nope", "consumer", []string{}},
	} {
		got := svc.pageOwners(ctx, tc.pipeline, tc.layer)
		if got == nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s/%s: owners = %#v, want %#v", tc.pipeline, tc.layer, got, tc.want)
		}
	}
	// No session store: users.yaml and owners.yaml only.
	plain := pageOwnersService(t, nil, nil)
	if got := plain.pageOwners(ctx, "orders", "consumer"); !reflect.DeepEqual(got, []string{"kabir", "group:consumers", "meera", "group:night"}) {
		t.Errorf("without sessions: %v", got)
	}
	// The payload of a page with nobody to page carries [], not null.
	snap, err := svc.CreateRoom(ctx, User{ID: "priya", Name: "Priya"}, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	ev, err := svc.PageOwners(ctx, User{ID: "priya", Name: "Priya"}, snap.ID, "schema-registry", "who owns it?", "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ev.Payload), `"owners":[]`) {
		t.Fatalf("payload = %s", ev.Payload)
	}
}

// recNotifier records deliveries; block, when set, holds Deliver until its
// context ends.
type recNotifier struct {
	mu     sync.Mutex
	got    []notify.OwnerPaged
	errs   []error
	calls  atomic.Int32
	block  bool
	called chan struct{}
}

func newRecNotifier(block bool) *recNotifier {
	return &recNotifier{block: block, called: make(chan struct{}, 32)}
}

func (n *recNotifier) Deliver(ctx context.Context, p notify.OwnerPaged) error {
	n.calls.Add(1)
	n.called <- struct{}{}
	if n.block {
		<-ctx.Done()
	}
	n.mu.Lock()
	n.got = append(n.got, p)
	n.errs = append(n.errs, ctx.Err())
	n.mu.Unlock()
	return ctx.Err()
}

func TestPageNotifierBody(t *testing.T) {
	sessions := auth.NewMemStore()
	addSession(t, sessions, "u_cons", "Cole", "cole@example.com", "consumers")
	n := newRecNotifier(false)
	svc := pageOwnersService(t, sessions, n)
	ctx := context.Background()
	snap, err := svc.CreateRoom(ctx, User{ID: "priya", Name: "Priya"}, "orders lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	ev, err := svc.PageOwners(ctx, User{ID: "priya", Name: "Priya"}, snap.ID, "consumer", "lag keeps growing", "")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-n.called:
	case <-time.After(5 * time.Second):
		t.Fatal("notifier never called")
	}
	svc.Close() // waits for the delivery goroutine
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.got) != 1 {
		t.Fatalf("deliveries = %d", len(n.got))
	}
	got := n.got[0]
	want := notify.OwnerPaged{
		RoomID: snap.ID, RoomTitle: "orders lag", PipelineID: "orders", Layer: "consumer",
		Owners: []notify.Owner{
			{ID: "kabir", Name: "Kabir", Email: "kabir@example.com"},
			{ID: "u_cons", Name: "Cole", Email: "cole@example.com"},
			{ID: "group:consumers", Name: "group:consumers"},
			{ID: "meera", Name: "Meera"},
			{ID: "group:night", Name: "group:night"},
		},
		Reason: "lag keeps growing", At: ev.At, Seq: ev.Seq,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("body = %+v\nwant  %+v", got, want)
	}
}

func TestPageDeliveryBoundedAndStopsAtClose(t *testing.T) {
	n := newRecNotifier(true)
	svc := pageOwnersService(t, nil, n)
	ctx := context.Background()
	u := User{ID: "priya", Name: "Priya"}
	var rooms []string
	for i := 0; i < 2; i++ {
		snap, err := svc.CreateRoom(ctx, u, "t", "sev2", "orders")
		if err != nil {
			t.Fatal(err)
		}
		rooms = append(rooms, snap.ID)
	}
	pages := 0
	for _, id := range rooms {
		for _, layer := range events.Layers {
			if pages == maxPageDeliveries+1 {
				break
			}
			if _, err := svc.PageOwners(ctx, u, id, layer, "look", ""); err != nil {
				t.Fatal(err)
			}
			pages++
		}
	}
	for i := 0; i < maxPageDeliveries; i++ {
		select {
		case <-n.called:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d deliveries started", i)
		}
	}
	time.Sleep(30 * time.Millisecond)
	if c := n.calls.Load(); c != maxPageDeliveries {
		t.Fatalf("deliveries started = %d, want %d (the page past the cap is not sent)", c, maxPageDeliveries)
	}
	done := make(chan struct{})
	go func() { svc.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not end the deliveries in flight")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, err := range n.errs {
		if err == nil {
			t.Fatalf("delivery %d saw a live context after Close", i)
		}
	}
}

// loadOwners parses an owners.yaml body for a test.
func loadOwners(t *testing.T, body string) *owners.Owners {
	t.Helper()
	path := t.TempDir() + "/owners.yaml"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return own
}

// QA F6: a layer owned only by groups, with nobody of them signed in. The
// server cannot list a group's members, so the page names the groups as
// principals: the event, the snapshot and the webhook carry them, and the
// room's one automatic page reaches someone a relay can route to.
func TestGroupOwnedLayerPagesTheGroup(t *testing.T) {
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	n := newRecNotifier(false)
	r := newPageRig(t, ms, c, 5*time.Minute, n)
	r.svc.owners = loadOwners(t, `pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
    consumer_groups: [order-sync]
    owners:
      consumer: { approvers: [group:data-eng], backup: [group:night] }
`)
	r.load()
	roomID := openWatcherRoom(t, r.svc, owners.SignalConsumerLag)
	c.Advance(5 * time.Minute)
	r.tick()
	got := autoPages(t, ms, roomID)
	want := []string{"group:data-eng", "group:night"}
	if len(got) != 1 || got[0].Layer != "consumer" || !reflect.DeepEqual(got[0].Owners, want) {
		t.Fatalf("auto pages = %+v, want one consumer page to %v", got, want)
	}
	if snap := mustRoom(t, r.svc, roomID); len(snap.Pages) != 1 || !reflect.DeepEqual(snap.Pages[0].Owners, want) {
		t.Fatalf("snapshot pages = %+v", snap.Pages)
	}
	select {
	case <-n.called:
	case <-time.After(5 * time.Second):
		t.Fatal("notifier never called")
	}
	r.svc.Close()
	n.mu.Lock()
	defer n.mu.Unlock()
	wantOwners := []notify.Owner{{ID: "group:data-eng", Name: "group:data-eng"}, {ID: "group:night", Name: "group:night"}}
	if len(n.got) != 1 || !reflect.DeepEqual(n.got[0].Owners, wantOwners) {
		t.Fatalf("webhook owners = %+v, want %+v", n.got, wantOwners)
	}
}

// QA F6: an automatic page that would reach nobody is not written, so it is
// not spent: the next check pages once someone can be paged.
func TestAutoPageToNobodyIsNotSpent(t *testing.T) {
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	r := newPageRig(t, ms, c, 5*time.Minute, nil)
	r.svc.users = mustUsers([]User{{ID: "arjun", Name: "Arjun"}})
	r.load()
	roomID := openWatcherRoom(t, r.svc, owners.SignalConsumerLag)
	c.Advance(5 * time.Minute)
	r.tick()
	if got := autoPages(t, ms, roomID); len(got) != 0 {
		t.Fatalf("auto pages to nobody = %+v, want none", got)
	}
	r.svc.users = DefaultUsers() // kabir owns consumer
	c.Advance(time.Minute)
	r.tick()
	got := autoPages(t, ms, roomID)
	if len(got) != 1 || !reflect.DeepEqual(got[0].Owners, []string{"kabir"}) {
		t.Fatalf("auto pages = %+v, want one to kabir", got)
	}
}

func openAMRoom(t *testing.T, svc *Service, key string, raw string) string {
	t.Helper()
	snap, _, err := svc.OpenAlertRoom(context.Background(), alertin.Alert{
		Source: alertin.Alertmanager, Status: alertin.StatusFiring, Severity: "sev2", PipelineID: "orders",
		Title: "lag " + key, DedupKey: "am:" + key, Raw: json.RawMessage(raw),
	})
	if err != nil {
		t.Fatal(err)
	}
	return snap.ID
}

// S4: no watcher signal and no triage layer: the alert's layer label, else the
// first layer of the pipeline that has someone to page.
func TestAutoPageFallbackLayer(t *testing.T) {
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	r := newPageRig(t, ms, c, 5*time.Minute, nil)
	r.load()
	labelled := openAMRoom(t, r.svc, "a", `{"commonLabels":{"layer":"warehouse"},"alerts":[]}`)
	badLabel := openAMRoom(t, r.svc, "b", `{"alerts":[{"labels":{"layer":"nonsense"}}]}`)
	plain := openAMRoom(t, r.svc, "c", ``)
	c.Advance(6 * time.Minute)
	r.tick()
	if got := autoPages(t, ms, labelled); len(got) != 1 || got[0].Layer != "warehouse" {
		t.Fatalf("label fallback = %+v", got)
	}
	for _, id := range []string{badLabel, plain} {
		got := autoPages(t, ms, id)
		if len(got) != 1 || !slices.Contains(events.Layers, got[0].Layer) || len(got[0].Owners) == 0 {
			t.Fatalf("owner-order fallback = %+v", got)
		}
	}
	r.tick()
	if n := len(autoPages(t, ms, plain)); n != 1 {
		t.Fatalf("fallback page repeated: %d", n)
	}
}

// S4: nobody to page at all logs one WARN and writes no page.
func TestAutoPageFallbackNobody(t *testing.T) {
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	var buf syncBuf
	r := newPageRig(t, ms, c, 5*time.Minute, nil)
	r.svc.log = slog.New(slog.NewTextHandler(&buf, nil))
	r.svc.users = mustUsers(nil)
	r.load()
	id := openAMRoom(t, r.svc, "n", ``)
	c.Advance(6 * time.Minute)
	r.tick()
	r.tick()
	if n := len(autoPages(t, ms, id)); n != 0 {
		t.Fatalf("pages = %d, want 0", n)
	}
	if got := buf.String(); strings.Count(got, "level=WARN") != 1 || !strings.Contains(got, id) {
		t.Fatalf("log = %q, want one WARN naming the room", got)
	}
}

type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }
