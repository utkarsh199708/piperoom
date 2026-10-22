package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// fakePager records the pages it was asked to post. err fails every post;
// started and gate, when set, hold a post open so a test can act meanwhile.
type fakePager struct {
	mu      sync.Mutex
	pages   []room.TeamPage
	err     error
	started chan struct{}
	gate    chan struct{}
}

func (f *fakePager) PageTeam(ctx context.Context, p room.TeamPage) error {
	if f.started != nil {
		f.started <- struct{}{}
	}
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.pages = append(f.pages, p)
	return nil
}

func (f *fakePager) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.pages) }

// teamOwners has two tenants: orders (tenant a) sees checkout and billing,
// never tenant b's ledger team.
func teamOwners() *owners.Owners {
	return &owners.Owners{
		Tenants: map[string]owners.Tenant{
			"a": {Members: owners.Principals{{Kind: owners.KindUser, Name: "arjun"}, {Kind: owners.KindUser, Name: "priya"}}},
			"b": {Members: owners.Principals{{Kind: owners.KindUser, Name: "kabir"}}},
		},
		Teams: []owners.Team{
			{ID: "checkout", SlackChannel: "C0123ABCD", Owns: []string{"checkout-api"}, Tenant: "a"},
			{ID: "billing", SlackChannel: "C0456EFGH", Owns: []string{"billing-api"}, Tenant: "a"},
			{ID: "ledger", SlackChannel: "C0789IJKL", Owns: []string{"ledger-api"}, Tenant: "b"},
		},
		Pipelines: []owners.Pipeline{{ID: "orders", Tenant: "a", SourceTables: []string{"public.orders"}}},
	}
}

type pageRig struct {
	svc   *room.Service
	st    *memstore.Store
	clk   *clock
	pager *fakePager
	mu    sync.Mutex
	sink  agent.Sink
}

// newPageRig builds a service paging through pager; a nil pager is the Slack
// bridge turned off.
func newPageRig(t *testing.T, pager *fakePager) *pageRig {
	t.Helper()
	r := &pageRig{pager: pager}
	r.clk = &clock{t: time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)}
	r.st = memstore.NewWithClock(r.clk.Now)
	run := func(ctx context.Context, _ agent.Alert, d agent.Deps, _ agent.Options) error {
		r.mu.Lock()
		r.sink = d.Sink
		r.mu.Unlock()
		<-ctx.Done()
		return nil
	}
	deps := room.Deps{Store: r.st, Users: room.DefaultUsers(), Owners: teamOwners(), Run: run, WriteHandoff: fakeWrite, Now: r.clk.Now}
	if pager != nil {
		deps.TeamPager = pager
	}
	r.svc = room.New(deps)
	t.Cleanup(r.svc.Close)
	return r
}

func (r *pageRig) emit(t *testing.T, typ events.Type, payload any) error {
	t.Helper()
	waitFor(t, 2*time.Second, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.sink != nil })
	r.mu.Lock()
	sink := r.sink
	r.mu.Unlock()
	return sink.Emit(context.Background(), typ, payload)
}

func (r *pageRig) mustEmit(t *testing.T, typ events.Type, payload any) {
	t.Helper()
	if err := r.emit(t, typ, payload); err != nil {
		t.Fatal(err)
	}
}

func (r *pageRig) teamPaged(t *testing.T, roomID string) []events.Event {
	t.Helper()
	var out []events.Event
	for _, e := range allEvents(t, r.st, roomID) {
		if e.Type == events.RoomTeamPaged {
			out = append(out, e)
		}
	}
	return out
}

func upstreamHyp(team string, changeID int64, steps ...string) events.AgentHypothesisPayload {
	return events.AgentHypothesisPayload{Text: "checkout-api deploy dropped <@U999> column", BasedOnSteps: steps,
		Confidence: "high", Scope: events.ScopeUpstreamApp, Team: team, ChangeID: changeID}
}

// TC1 + TC2: the driver pages; the event carries the team, the hypothesis seq
// and its change; non-drivers and unknown teams are refused; the same team
// again within the hour is 429, another team is not, and after the hour it is
// paged again.
func TestPageTeamFlow(t *testing.T) {
	pager := &fakePager{}
	r := newPageRig(t, pager)
	ctx := context.Background()
	roomID := createRoom(t, r.svc, "arjun").ID
	arjun, priya := mustUser(t, r.svc, "arjun"), mustUser(t, r.svc, "priya")

	_, err := r.svc.PageTeam(ctx, arjun, roomID, "checkout")
	wantCode(t, err, "no_hypothesis")

	for _, id := range []string{"s_a", "s_b", "s_c"} {
		r.mustEmit(t, events.AgentStepStart, events.AgentStepStartPayload{StepID: id, Title: id})
	}
	after, _ := json.Marshal(clients.ExternalChange{URL: "https://ci.example/run/7", Service: "checkout-api"})
	if _, err := r.st.RecordChange(ctx, clients.PipelineChange{PipelineID: "orders", Kind: clients.ChangeDeploy, Object: "checkout-api",
		At: r.clk.Now().Add(-time.Hour), Source: "webhook", Fingerprint: "f1", Summary: "deploy 812", After: after}); err != nil {
		t.Fatal(err)
	}
	rows, _ := r.st.ListChanges(ctx, "orders", time.Time{}, time.Time{}, nil, 10)
	changeID := rows[0].ID
	r.mustEmit(t, events.AgentHypothesis, upstreamHyp("checkout", changeID, "s_c", "s_a", "s_gone", "s_a"))

	_, err = r.svc.PageTeam(ctx, priya, roomID, "checkout")
	wantCode(t, err, "not_driver")
	for _, team := range []string{"", "ghost", "CHECKOUT"} {
		_, err = r.svc.PageTeam(ctx, arjun, roomID, team)
		wantCode(t, err, "validation")
	}
	if pager.count() != 0 || len(r.teamPaged(t, roomID)) != 0 {
		t.Fatal("a refused page must post and write nothing")
	}

	ev, err := r.svc.PageTeam(ctx, arjun, roomID, "checkout")
	if err != nil {
		t.Fatalf("driver page: %v", err)
	}
	snap, _ := r.svc.GetRoom(ctx, roomID)
	p, _ := events.Decode[events.TeamPagedPayload](ev)
	if ev.Type != events.RoomTeamPaged || ev.Actor.ID != "arjun" || ev.Actor.Kind != events.ActorHuman ||
		p.Team != "checkout" || p.HypothesisSeq != snap.Hypothesis.Seq || p.ChangeID != changeID {
		t.Fatalf("event = %+v payload %+v (hypothesis seq %d)", ev, p, snap.Hypothesis.Seq)
	}
	if raw := string(ev.Payload); strings.Contains(raw, "deploy") || strings.Contains(raw, "C0123ABCD") {
		t.Fatalf("the event must carry ids only: %s", raw)
	}
	got := pager.pages[0]
	if got.Channel != "C0123ABCD" || got.Team != "checkout" || got.PipelineID != "orders" || got.RoomID != roomID ||
		got.Scope != events.ScopeUpstreamApp || got.Hypothesis != upstreamHyp("", 0).Text || got.By.ID != "arjun" ||
		!slices.Equal(got.Steps, []int{1, 3}) || got.Change == nil || got.Change.ID != changeID || got.Change.Summary != "deploy 812" {
		t.Fatalf("page = %+v (change %+v)", got, got.Change)
	}
	if h := snap.Hypothesis; h.Scope != events.ScopeUpstreamApp || h.Team != "checkout" || h.ChangeID != changeID {
		t.Fatalf("snapshot hypothesis = %+v", h)
	}
	if len(snap.TeamPages) != 1 || snap.TeamPages[0].Team != "checkout" || snap.TeamPages[0].By.ID != "arjun" ||
		snap.TeamPages[0].HypothesisSeq != p.HypothesisSeq || !snap.TeamPages[0].At.Equal(ev.At) {
		t.Fatalf("team_pages = %+v", snap.TeamPages)
	}

	r.clk.Advance(59 * time.Minute)
	_, err = r.svc.PageTeam(ctx, arjun, roomID, "checkout")
	wantCode(t, err, "rate_limited")
	var re *room.Error
	if errors.As(err, &re); re.Status != 429 || !strings.Contains(re.Msg, "59 min ago") || !strings.Contains(re.Msg, "11:00 UTC") {
		t.Fatalf("429 must say when: %d %q", re.Status, re.Msg)
	}
	if _, err := r.svc.PageTeam(ctx, arjun, roomID, "billing"); err != nil {
		t.Fatalf("another team is its own limit: %v", err)
	}
	r.clk.Advance(time.Minute)
	if _, err := r.svc.PageTeam(ctx, arjun, roomID, "checkout"); err != nil {
		t.Fatalf("after the hour: %v", err)
	}
	if pager.count() != 3 || len(r.teamPaged(t, roomID)) != 3 {
		t.Fatalf("pages %d, events %d, want 3 each", pager.count(), len(r.teamPaged(t, roomID)))
	}
}

// A hypothesis whose scope is pipeline, unknown or missing has no team to page.
func TestPageTeamNotUpstream(t *testing.T) {
	pager := &fakePager{}
	r := newPageRig(t, pager)
	ctx := context.Background()
	roomID := createRoom(t, r.svc, "arjun").ID
	arjun := mustUser(t, r.svc, "arjun")
	for _, h := range []events.AgentHypothesisPayload{
		{Text: "slot", Confidence: "low"},
		{Text: "slot", Confidence: "low", Scope: events.ScopePipeline},
		{Text: "slot", Confidence: "low", Scope: "made-up"},
	} {
		r.mustEmit(t, events.AgentHypothesis, h)
		_, err := r.svc.PageTeam(ctx, arjun, roomID, "checkout")
		wantCode(t, err, "not_upstream")
	}
	// A vendor scope pages; the cited change may be absent.
	r.mustEmit(t, events.AgentHypothesis, events.AgentHypothesisPayload{Text: "msk incident", Confidence: "medium", Scope: events.ScopeVendor, Team: "checkout"})
	if _, err := r.svc.PageTeam(ctx, arjun, roomID, "checkout"); err != nil {
		t.Fatal(err)
	}
	if pager.count() != 1 || pager.pages[0].Change != nil || pager.pages[0].Steps != nil {
		t.Fatalf("pages = %+v", pager.pages)
	}
}

// TC4: with the Slack bridge off the route answers 409 slack_off and writes
// nothing.
func TestPageTeamSlackOff(t *testing.T) {
	r := newPageRig(t, nil)
	ctx := context.Background()
	roomID := createRoom(t, r.svc, "arjun").ID
	r.mustEmit(t, events.AgentHypothesis, upstreamHyp("checkout", 0))
	before := len(allEvents(t, r.st, roomID))
	_, err := r.svc.PageTeam(ctx, mustUser(t, r.svc, "arjun"), roomID, "checkout")
	wantCode(t, err, "slack_off")
	var re *room.Error
	if errors.As(err, &re); re.Status != 409 || !strings.Contains(re.Msg, "SLACK_BOT_TOKEN") {
		t.Fatalf("slack_off = %d %q", re.Status, re.Msg)
	}
	if n := len(allEvents(t, r.st, roomID)); n != before {
		t.Fatalf("slack_off wrote %d events", n-before)
	}
}

// E3: a team of another tenant reads as unknown (400) and is never posted to,
// even when the agent named it.
func TestPageTeamOtherTenantTeam(t *testing.T) {
	pager := &fakePager{}
	r := newPageRig(t, pager)
	roomID := createRoom(t, r.svc, "arjun").ID
	r.mustEmit(t, events.AgentHypothesis, upstreamHyp("ledger", 0))
	_, err := r.svc.PageTeam(context.Background(), mustUser(t, r.svc, "arjun"), roomID, "ledger")
	wantCode(t, err, "validation")
	var re *room.Error
	if errors.As(err, &re); strings.Contains(re.Msg, "ledger") {
		t.Fatalf("the message must not echo the id: %q", re.Msg)
	}
	if pager.count() != 0 || len(r.teamPaged(t, roomID)) != 0 {
		t.Fatal("another tenant's team was paged")
	}
}

// E6: Slack refusing the page is 503 slack_unavailable with its code; nothing
// is recorded, so the driver can retry at once.
func TestPageTeamSlackFailureWritesNothing(t *testing.T) {
	pager := &fakePager{err: &room.TeamPageError{Code: "not_in_channel", Err: errors.New("slack error not_in_channel")}}
	r := newPageRig(t, pager)
	ctx := context.Background()
	roomID := createRoom(t, r.svc, "arjun").ID
	arjun := mustUser(t, r.svc, "arjun")
	r.mustEmit(t, events.AgentHypothesis, upstreamHyp("checkout", 0))
	_, err := r.svc.PageTeam(ctx, arjun, roomID, "checkout")
	wantCode(t, err, "slack_unavailable")
	var re *room.Error
	if errors.As(err, &re); re.Status != 503 || !strings.Contains(re.Msg, "not_in_channel") {
		t.Fatalf("503 = %d %q", re.Status, re.Msg)
	}
	// A code that is not a plain word is never shown.
	pager.mu.Lock()
	pager.err = &room.TeamPageError{Code: "<@U1> hi", Err: errors.New("x")}
	pager.mu.Unlock()
	_, err = r.svc.PageTeam(ctx, arjun, roomID, "checkout")
	if errors.As(err, &re); !strings.Contains(re.Msg, "(error)") {
		t.Fatalf("odd code shown: %q", re.Msg)
	}
	if len(r.teamPaged(t, roomID)) != 0 {
		t.Fatal("a failed page was recorded")
	}
	pager.mu.Lock()
	pager.err = nil
	pager.mu.Unlock()
	if _, err := r.svc.PageTeam(ctx, arjun, roomID, "checkout"); err != nil {
		t.Fatalf("retry after a failure: %v", err)
	}
}

func TestPageTeamClosedRoom(t *testing.T) {
	r := newPageRig(t, &fakePager{})
	ctx := context.Background()
	roomID := createRoom(t, r.svc, "arjun").ID
	arjun := mustUser(t, r.svc, "arjun")
	r.mustEmit(t, events.AgentHypothesis, upstreamHyp("checkout", 0))
	if _, err := r.svc.CloseRoom(ctx, arjun, roomID, "fixed upstream"); err != nil {
		t.Fatal(err)
	}
	_, err := r.svc.PageTeam(ctx, arjun, roomID, "checkout")
	wantCode(t, err, "room_closed")
	_, err = r.svc.PageTeam(ctx, arjun, "r_nope", "checkout")
	wantCode(t, err, "not_found")
}

// E5: a second click while the first page is being posted is refused, so one
// page goes out and one event is written.
func TestPageTeamDoubleClickPostsOnce(t *testing.T) {
	pager := &fakePager{started: make(chan struct{}, 2), gate: make(chan struct{})}
	r := newPageRig(t, pager)
	ctx := context.Background()
	roomID := createRoom(t, r.svc, "arjun").ID
	arjun := mustUser(t, r.svc, "arjun")
	r.mustEmit(t, events.AgentHypothesis, upstreamHyp("checkout", 0))
	done := make(chan error, 1)
	go func() {
		_, err := r.svc.PageTeam(ctx, arjun, roomID, "checkout")
		done <- err
	}()
	<-pager.started
	_, err := r.svc.PageTeam(ctx, arjun, roomID, "checkout")
	wantCode(t, err, "rate_limited")
	// The room is not locked while Slack is posted to.
	if _, err := r.svc.AddComment(ctx, arjun, roomID, "", "waiting for checkout"); err != nil {
		t.Fatalf("comment during a page: %v", err)
	}
	close(pager.gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if pager.count() != 1 || len(r.teamPaged(t, roomID)) != 1 {
		t.Fatalf("pages %d events %d, want 1 each", pager.count(), len(r.teamPaged(t, roomID)))
	}
}

// The agent can never write room.team_paged through its Sink.
func TestAgentSinkRefusesTeamPaged(t *testing.T) {
	r := newPageRig(t, &fakePager{})
	roomID := createRoom(t, r.svc, "arjun").ID
	err := r.emit(t, events.RoomTeamPaged, events.TeamPagedPayload{Team: "checkout", HypothesisSeq: 1})
	if err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("sink err = %v, want a refusal", err)
	}
	if len(r.teamPaged(t, roomID)) != 0 {
		t.Fatal("the agent wrote room.team_paged")
	}
}

// The snapshot's hypothesis reads scope "unknown", team "" and change_id 0
// for a payload written before X4; team_pages is [] and never null.
func TestSnapshotHypothesisScopeDefaults(t *testing.T) {
	snap := room.NewSnapshot()
	snap = room.Apply(snap, events.Event{Seq: 1, Type: events.AgentHypothesis, Payload: json.RawMessage(`{"text":"x","based_on_steps":[],"confidence":"low","confirmed":false}`)})
	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"scope":"unknown"`, `"team":""`, `"change_id":0`, `"team_pages":[]`} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("snapshot lacks %s: %s", want, raw)
		}
	}
	next := room.Apply(snap, events.Event{Seq: 2, Type: events.RoomTeamPaged, Payload: json.RawMessage(`{"team":"checkout","hypothesis_seq":1}`)})
	if len(snap.TeamPages) != 0 || len(next.TeamPages) != 1 || len(next.Clone().TeamPages) != 1 {
		t.Fatalf("Apply must not change its input: %v / %v", snap.TeamPages, next.TeamPages)
	}
}
