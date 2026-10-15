package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/notify"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/watch"
)

// Paging (Q0, QA G2): owner.paged asks the owners of one layer to look at the
// room. Three writers share one path (page): a member (PageOwners, POST
// /api/rooms/{id}/page), the agent (roomPager, its page_owner tool) and the
// server itself, once, when a watcher or alert room still has no driver
// Paging.UndrivenAfter after it opened (the auto-page loop below). The owners
// are resolved here, never chosen by the caller, and a layer is paged at most
// once per pageWindow. Once the event is written, the notifier (the webhook of
// internal/notify) gets it on a goroutine of the service; the web app and the
// Slack bridge read the event itself.

const (
	// pageWindow is the minimum gap between two pages of one layer of a room.
	pageWindow = 10 * time.Minute
	// MaxPageReasonChars caps the reason of a page (docs/api.md: 1–300).
	MaxPageReasonChars = 300
	// AutoPageCheckEvery is how often the service looks for undriven alert
	// rooms. A page up to one period late is fine.
	AutoPageCheckEvery = 15 * time.Second
	// autoPageCheckTimeout bounds one pass of the check.
	autoPageCheckTimeout = 30 * time.Second
	// maxPageDeliveries caps webhook deliveries in flight; a page past it
	// waits in a queue of maxPageQueue (QA R3).
	maxPageDeliveries = 8
	// maxPageQueue bounds the pages waiting for a delivery slot: an alert
	// storm of a few hundred rooms still fits. A page past it is not
	// delivered by webhook (it is still in the room and in Slack) and is
	// logged as a WARN.
	maxPageQueue = 512
	// pageDeliveryTimeout bounds one delivery: the contact lookups, two
	// attempts of notify.DefaultTimeout and the pause between them.
	pageDeliveryTimeout = 30 * time.Second
)

// PageNotifier delivers an owner.paged outside the app; notify.Webhook
// implements it. Deliver runs on a goroutine of the service with a context
// that ends at Close, and must honour it.
type PageNotifier interface {
	Deliver(ctx context.Context, p notify.OwnerPaged) error
}

// Paging configures owner.paged (Deps.Paging).
type Paging struct {
	// Notifier gets every owner.paged this replica writes, after the append.
	// Nil: the page stays in-app (and Slack).
	Notifier PageNotifier
	// UndrivenAfter is PIPEROOM_PAGE_UNDRIVEN_AFTER: a room opened by an alert
	// or the watcher that still has no driver this long after it opened gets
	// one owner.paged with auto set. 0 = never.
	UndrivenAfter time.Duration
	// Ticker, when set, replaces the ticker of the auto-page check, like
	// Deps.LeaseTicker (tests feed it by hand). Nil uses time.NewTicker.
	Ticker func(d time.Duration) (<-chan time.Time, func())
}

// pagingState is the service's paging runtime.
type pagingState struct {
	cfg Paging
	// queue holds written pages waiting for one of maxPageDeliveries workers.
	queue   chan pageJob
	workers sync.Once // the delivery workers start with the first page
	once    sync.Once // the auto-page loop starts once
	// tickDone, when set (tests only), runs after each check of the loop.
	tickDone func()
	// noOwners marks rooms whose automatic page found nobody to page, so the
	// warning is written once per room. Guarded by mu, a leaf lock.
	mu       sync.Mutex
	noOwners map[string]bool
}

// firstNoOwners marks roomID's automatic page as having found nobody and
// reports whether it is the first time, so the warning is written once.
func (p *pagingState) firstNoOwners(roomID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	first := !p.noOwners[roomID]
	p.noOwners[roomID] = true
	return first
}

func newPagingState(p Paging) *pagingState {
	return &pagingState{cfg: p, queue: make(chan pageJob, maxPageQueue), noOwners: map[string]bool{}}
}

// Page is one entry of the snapshot's pages (docs/api.md, Q-wave snapshot
// fields): the newest owner.paged of one layer.
type Page struct {
	Layer      string       `json:"layer"`
	Owners     []string     `json:"owners"`
	Reason     string       `json:"reason"`
	ProposalID string       `json:"proposal_id"`
	By         events.Actor `json:"by"`
	Auto       bool         `json:"auto"`
	At         time.Time    `json:"at"`
}

// alreadyPaged is 409 already_paged; msg says how long ago (Q0).
func alreadyPaged(msg string) *Error {
	return &Error{Status: 409, Code: "already_paged", Msg: msg}
}

// errNobodyToPage ends an automatic page that would name nobody: it is not
// written, so the room's one automatic page is not spent on it (QA F6).
var errNobodyToPage = errors.New("room: nobody to page")

// errPageNotDue ends an automatic page whose room stopped being due between
// the check and the write (a driver arrived, another replica paged).
var errPageNotDue = errors.New("room: automatic page no longer due")

// ---- the fold ----

// roomFacts are folded from the log for the watcher and the paging code (Q1);
// unexported, so they are not part of the snapshot JSON.
type roomFacts struct {
	// alertSource is room.created.alert_source ("" for a room a person opened).
	alertSource string
	// alertSignal is the first watcher signal in room.created.alert with a
	// known layer (signalLayers, or the "layer" its evidence carries),
	// alertWriters its DLQ writers and alertLayer that evidence layer.
	alertSignal  string
	alertWriters []string
	alertLayer   string
	// alertLabelLayer is a valid `layer` label of the alert (labelLayer), the
	// auto page's fallback when no signal or triage layer exists.
	alertLabelLayer string
	// lastActivity is the time of the newest event that is room activity
	// (countsAsActivity).
	lastActivity time.Time
	// autoPaged is set by an owner.paged with auto: the server's own page is
	// written once per room, and the log says whether it was.
	autoPaged bool
	// signalKeys are the sorted incident keys of the watcher evidence in
	// room.created.alert and in the system's notes (RoomSignalKeys).
	signalKeys []string
}

// foldRoomFacts is Apply's Q1 step: the snapshot's pages and the room facts.
// Like Apply it never mutates what the input snapshot shares.
func foldRoomFacts(snap *RoomSnapshot, e events.Event) {
	switch e.Type {
	case events.RoomCreated:
		if p, ok := decode[events.RoomCreatedPayload](e); ok {
			snap.facts.alertSource = p.AlertSource
			snap.facts.alertLabelLayer = labelLayer(p.Alert)
			if p.AlertSource == string(alertin.Watcher) {
				snap.facts.alertSignal, snap.facts.alertWriters, snap.facts.alertLayer = firstWatcherSignal(p.Alert)
				snap.facts.signalKeys = mergeKeys(snap.facts.signalKeys, watch.EvidenceKeys(p.Alert))
			}
		}
	case events.AgentMessage:
		// Only the service writes system notes, so people cannot add keys.
		if e.Actor.Kind == events.ActorSystem {
			if p, ok := decode[incidentNotePayload](e); ok {
				snap.facts.signalKeys = mergeKeys(snap.facts.signalKeys, p.SignalKeys)
			}
		}
	case events.OwnerPaged:
		p, ok := decode[events.OwnerPagedPayload](e)
		if !ok {
			break
		}
		if p.Auto {
			snap.facts.autoPaged = true
		}
		if snap.Status == "open" {
			snap.Pages = withPage(snap.Pages, Page{
				Layer: p.Layer, Owners: append([]string{}, p.Owners...), Reason: p.Reason,
				ProposalID: p.ProposalID, By: e.Actor, Auto: p.Auto, At: e.At,
			})
		}
	case events.RoomClosed:
		snap.Pages = []Page{}
	}
	if countsAsActivity(e) {
		snap.facts.lastActivity = e.At
	}
}

// mergeKeys returns the sorted union of a and b in a new slice (Apply is pure).
func mergeKeys(a, b []string) []string {
	if len(b) == 0 {
		return a
	}
	out := slices.Concat(a, b)
	slices.Sort(out)
	return slices.Compact(out)
}

// withPage returns pages with pg as the newest page of its layer, at the end
// (the seq order), in a new slice.
func withPage(pages []Page, pg Page) []Page {
	out := make([]Page, 0, len(pages)+1)
	for _, old := range pages {
		if old.Layer != pg.Layer {
			out = append(out, old)
		}
	}
	return append(out, pg)
}

func clonePages(in []Page) []Page {
	out := make([]Page, len(in))
	for i, p := range in {
		out[i] = p
		out[i].Owners = append([]string{}, p.Owners...)
	}
	return out
}

// pageOf returns the newest page of layer in an open room.
func (s RoomSnapshot) pageOf(layer string) (Page, bool) {
	for _, p := range s.Pages {
		if p.Layer == layer {
			return p, true
		}
	}
	return Page{}, false
}

// signalLayers is the layer a watcher signal points at: the layer whose owner
// can act on it first. dlq_growth is the writer's layer (docs/design/dlq.md
// §3.3): kafka-connect unless every writer is a consumer group (signalLayer).
var signalLayers = map[string]string{
	owners.SignalConnectorDown:       "kafka-connect",
	owners.SignalSlotInactive:        "postgres",
	owners.SignalSlotWAL:             "postgres",
	owners.SignalDLQGrowth:           "kafka-connect",
	owners.SignalConfigRisk:          "kafka-connect",
	owners.SignalConsumerLag:         "consumer",
	owners.SignalGroupUnstable:       "consumer",
	owners.SignalOffsetBelowLogStart: "consumer",
	signalSlotStall:                  "postgres",
}

// signalSlotStall is watch.SignalSlotStall (QA G18), named here so the room
// package does not import the watcher; a test checks every watcher signal
// has the layer watch.SignalLayer gives it.
const signalSlotStall = "slot_stall"

// firstWatcherSignal reads the watcher evidence of room.created.alert
// (internal/watch compose: {"source":"watcher","signals":[{"signal":...,
// "layer":...}]}) and returns its first signal with a known layer, that
// signal's DLQ writers and the layer its evidence names ("" when none or not
// one of events.Layers). A signal this package has no table entry for still
// counts when its evidence names a layer, so a new watcher signal pages
// without a room-side change (QA F22). An older body with one top-level
// "signal" is read too.
func firstWatcherSignal(raw json.RawMessage) (string, []string, string) {
	var body struct {
		Signal  string `json:"signal"`
		Signals []struct {
			Signal   string          `json:"signal"`
			Layer    string          `json:"layer"`
			Writers  json.RawMessage `json:"writers"`
			Evidence struct {
				Layer string `json:"layer"`
			} `json:"evidence"`
		} `json:"signals"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &body) != nil {
		return "", nil, ""
	}
	for _, sg := range body.Signals {
		evLayer := sg.Layer
		if evLayer == "" {
			evLayer = sg.Evidence.Layer
		}
		if !slices.Contains(events.Layers, evLayer) {
			evLayer = ""
		}
		if _, ok := signalLayers[sg.Signal]; ok || (sg.Signal != "" && evLayer != "") {
			var writers []string
			_ = json.Unmarshal(sg.Writers, &writers) // absent or odd: no writers
			return sg.Signal, writers, evLayer
		}
	}
	if _, ok := signalLayers[body.Signal]; ok {
		return body.Signal, nil, ""
	}
	return "", nil, ""
}

// labelLayer is the valid layer named by a `layer` label of a raw alert
// (Alertmanager commonLabels, groupLabels, alerts[].labels, or a top-level
// labels map), "" if there is none.
func labelLayer(raw json.RawMessage) string {
	var body struct {
		Labels       map[string]string `json:"labels"`
		CommonLabels map[string]string `json:"commonLabels"`
		GroupLabels  map[string]string `json:"groupLabels"`
		Alerts       []struct {
			Labels map[string]string `json:"labels"`
		} `json:"alerts"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &body) != nil {
		return ""
	}
	sets := []map[string]string{body.Labels, body.CommonLabels, body.GroupLabels}
	for _, a := range body.Alerts {
		sets = append(sets, a.Labels)
	}
	for _, m := range sets {
		if l := m["layer"]; slices.Contains(events.Layers, l) {
			return l
		}
	}
	return ""
}

// fallbackLayer picks the layer to page when neither the watcher nor triage
// named one: the alert's layer label if someone owns it, else the first layer of events.Layers
// (the pipeline's flow order; owners.yaml keeps layers in a map) that has
// someone to page. "" when nobody can be paged.
func (s *Service) fallbackLayer(ctx context.Context, pipelineID, label string) string {
	// A label nobody owns would end in errNobodyToPage on every check, so it
	// is skipped like an empty one (QA R4).
	if label != "" && len(s.pageOwners(ctx, pipelineID, label)) > 0 {
		return label
	}
	for _, l := range events.Layers {
		if len(s.pageOwners(ctx, pipelineID, l)) > 0 {
			return l
		}
	}
	return ""
}

// signalLayer is the layer of a watcher signal in pipelineID; evLayer is the
// layer the signal's evidence named, used when this table has none.
func (s *Service) signalLayer(pipelineID, signal string, writers []string, evLayer string) string {
	layer, known := signalLayers[signal]
	if !known {
		return evLayer
	}
	if signal != owners.SignalDLQGrowth || len(writers) == 0 || s.owners == nil {
		return layer
	}
	p, ok := s.owners.Pipeline(pipelineID)
	if !ok {
		return layer
	}
	for _, w := range writers {
		if !slices.Contains(p.ConsumerGroups, w) {
			return layer
		}
	}
	return "consumer"
}

// ---- writing a page ----

// pageReq is one owner.paged to write.
type pageReq struct {
	actor events.Actor
	// user is the person paging, whose driver lease the page renews; nil for
	// the agent and the system.
	user       *User
	layer      string
	reason     string
	proposalID string
	auto       bool
	// needOwners refuses the page (errNobodyToPage) when it would name
	// nobody; the automatic page sets it.
	needOwners bool
	// check adds the writer's own condition under the room's lock: the agent run's
	// fence, or that an automatic page is still due. Nil for a person.
	check func(st *roomState) error
}

// PageOwners serves POST /api/rooms/{id}/page: user asks the owners of layer
// to look at the room. Any member may page (the HTTP layer refuses viewers);
// the room must be open; layer is one of events.Layers; reason is 1–300
// characters of typed text; proposalID, when set, is an open proposal of the
// room; one page per room and layer per 10 minutes (409 already_paged). The
// owners are resolved by the server. It renews the driver lease only when
// user is the driver.
func (s *Service) PageOwners(ctx context.Context, user User, roomID, layer, reason, proposalID string) (events.Event, error) {
	if err := rejectUnstorable(layer, reason, proposalID); err != nil {
		return events.Event{}, err
	}
	if !slices.Contains(events.Layers, layer) {
		return events.Event{}, validation("layer must be one of " + strings.Join(events.Layers, ", "))
	}
	if strings.TrimSpace(reason) == "" {
		return events.Event{}, validation("reason is required")
	}
	if err := checkLen("reason", reason, MaxPageReasonChars); err != nil {
		return events.Event{}, err
	}
	e, _, err := s.page(ctx, roomID, pageReq{actor: s.humanActor(user), user: &user, layer: layer, reason: reason, proposalID: proposalID})
	return e, err
}

// roomPager is the agent's agent.Pager for one run, fenced like its Sink: a
// run that lost the room's lease writes nothing.
type roomPager struct {
	s      *Service
	roomID string
	run    uint64
}

var _ agent.Pager = roomPager{}

// Page implements agent.Pager. The reason is model text: it is cleaned like
// other agent text, made one line and cut to 300 characters rather than
// refused. Errors are *Error values whose message is safe to show the model.
func (p roomPager) Page(ctx context.Context, layer, reason, proposalID string) ([]string, error) {
	layer = strings.TrimSpace(sanitizeAgentText(layer))
	if !slices.Contains(events.Layers, layer) {
		return nil, validation(fmt.Sprintf("unknown layer %q: use one of %s", clipRunes(layer, 40), strings.Join(events.Layers, ", ")))
	}
	reason = agentPageReason(reason)
	if reason == "" {
		return nil, validation("reason is required")
	}
	_, owners, err := p.s.page(ctx, p.roomID, pageReq{
		actor: events.AgentActor, layer: layer, reason: reason,
		proposalID: strings.TrimSpace(sanitizeAgentText(proposalID)),
		check:      func(st *roomState) error { return p.s.fenceLocked(p.roomID, st, p.run) },
	})
	return owners, err
}

// agentPageReason cleans a model's page reason: storable, one line, at most
// MaxPageReasonChars.
func agentPageReason(s string) string {
	return clipRunes(strings.Join(strings.Fields(sanitizeAgentText(s)), " "), MaxPageReasonChars)
}

// clipRunes cuts s to at most n characters, the last one "…" when cut.
func clipRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n-1]) + "…"
}

// page writes one owner.paged and returns it with the owners paged. The owners
// are resolved between two holds of the room's lock, so no store or session
// read runs under it; everything is checked again before the append (a checked
// append with a shared store), so a page the first check let through can still
// be refused.
func (s *Service) page(ctx context.Context, roomID string, r pageReq) (events.Event, []string, error) {
	if !slices.Contains(events.Layers, r.layer) {
		return events.Event{}, nil, validation("layer must be one of " + strings.Join(events.Layers, ", "))
	}
	// Shared store: fold what other replicas wrote, reading outside the lock.
	s.syncRoom(ctx, roomID)
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return events.Event{}, nil, err
	}
	err = s.pageCheckLocked(st, r)
	pipelineID := st.snap.PipelineID
	s.unlockRoom(st)
	if err != nil {
		return events.Event{}, nil, err
	}

	ownerIDs := s.pageOwners(ctx, pipelineID, r.layer)
	if r.needOwners && len(ownerIDs) == 0 {
		return events.Event{}, nil, errNobodyToPage
	}

	if st, err = s.lockRoom(ctx, roomID); err != nil {
		return events.Event{}, nil, err
	}
	defer s.unlockRoom(st)
	payload := events.OwnerPagedPayload{Layer: r.layer, Owners: ownerIDs, Reason: r.reason, ProposalID: r.proposalID, Auto: r.auto}
	e, err := s.emitCheckedLocked(ctx, st, roomID, func() error { return s.pageCheckLocked(st, r) }, events.OwnerPaged, r.actor, payload)
	if err != nil {
		return events.Event{}, nil, err
	}
	if r.user != nil && s.isDriver(st, r.user.ID) {
		s.touchDriverLease(ctx, st, roomID)
	}
	s.notifyPageLocked(st, e, payload)
	return e, ownerIDs, nil
}

// pageCheckLocked is every rule of a page but the layer name. Caller holds
// st's lock.
func (s *Service) pageCheckLocked(st *roomState, r pageReq) error {
	if !s.roomOpen(st) {
		return roomClosed("room is closed")
	}
	if r.check != nil {
		if err := r.check(st); err != nil {
			return err
		}
	}
	if r.proposalID != "" && !slices.ContainsFunc(st.snap.OpenProposals, func(p Proposal) bool { return p.ProposalID == r.proposalID }) {
		return validation("proposal_id is not an open proposal of this room")
	}
	if last, ok := st.snap.pageOf(r.layer); ok {
		// A page stamped later than now (another replica's clock) counts as
		// just paged.
		if ago := s.now().Sub(last.At); ago < pageWindow {
			return alreadyPaged(fmt.Sprintf("paged %d min ago", int(max(ago, 0)/time.Minute)))
		}
	}
	return nil
}

// pageOwners resolves who to page for a layer of a pipeline: the people the
// approve path accepts for it (authorizeLayer, the not_layer_owner rule),
// approvers first, then backups, each once, never nil. Named owners.yaml users
// come first in file order; then users.yaml users and members with a live
// session whose groups (or, in dev mode, users.yaml layers) grant the layer;
// then the layer's `group:` principals themselves. The server has no IdP
// directory, so a group's members are known only while they have a live
// session: naming the group lets a webhook relay route the page to the group
// when none of them is signed in (QA F6). The solo user's blanket grant does
// not count.
func (s *Service) pageOwners(ctx context.Context, pipelineID, layer string) []string {
	out := []string{}
	seen := map[string]bool{}
	add := func(id string) {
		if id != "" && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	addGroups := func(ps owners.Principals) {
		for _, p := range ps {
			if p.Kind == owners.KindGroup {
				add(p.String())
			}
		}
	}
	approvers, backups := s.layerRights(pipelineID, layer)
	cands := s.pageCandidates(ctx)
	var candBackups []string
	for _, id := range approvers.Users() {
		add(id)
	}
	for _, u := range cands {
		grant, err := s.authorizeLayer(pipelineID, u, layer, "", false)
		switch {
		case err != nil || grant == "solo":
		case strings.HasPrefix(grant, owners.GrantBackupPrefix):
			candBackups = append(candBackups, u.ID)
		default:
			add(u.ID)
		}
	}
	addGroups(approvers)
	for _, id := range backups.Users() {
		add(id)
	}
	for _, id := range candBackups {
		add(id)
	}
	addGroups(backups)
	return out
}

// isGroupOwner reports whether an owners entry of a page is a group
// principal ("group:data-eng"), not a user id.
func isGroupOwner(id string) bool {
	return strings.HasPrefix(id, owners.KindGroup+":")
}

// layerRights is the owners.yaml approvers and backups of a layer of a
// pipeline, the rule owners.CanApprove applies: a pipeline owners: entry
// replaces the layers: entry, and a pipeline id the file does not know has
// nobody (fail closed).
func (s *Service) layerRights(pipelineID, layer string) (approvers, backups owners.Principals) {
	if s.owners == nil {
		return nil, nil
	}
	if pipelineID == "" {
		return s.owners.Layers[layer], nil
	}
	p, ok := s.owners.Pipeline(pipelineID)
	if !ok {
		return nil, nil
	}
	if r, over := p.Owners[layer]; over {
		return r.Approvers, r.Backup
	}
	return s.owners.Layers[layer], nil
}

// pageCandidates are the people authorizeLayer is asked about: every
// users.yaml user, then members with a live session (their IdP groups can
// grant a layer), as GET /api/people lists them. A session read that fails
// leaves users.yaml only.
func (s *Service) pageCandidates(ctx context.Context) []User {
	out := append([]User{}, s.users.All()...)
	if s.sessions == nil {
		return out
	}
	live, err := s.sessions.LiveSessions(ctx, s.now(), s.sessionIdle, maxPeople)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn("room: page: list live sessions failed; paging users.yaml owners only", "err", err)
		}
		return out
	}
	for _, sess := range live {
		if _, inFile := s.users.Get(sess.UserID); inFile {
			continue
		}
		if role, ok := s.RoleFor(sess.UserID, sess.Groups); !ok || role != RoleMember {
			continue
		}
		out = append(out, User{ID: sess.UserID, Name: sess.Name, Groups: sess.Groups, GroupsMissing: sess.GroupsMissing})
	}
	return out
}

// ---- delivery ----

// pageJob is one written page waiting for a webhook delivery.
type pageJob struct {
	body notify.OwnerPaged
	ids  []string
}

// notifyPageLocked queues a written page for the notifier, so the page never
// waits on the webhook. maxPageDeliveries workers of the service (s.wg, s.ctx)
// deliver from the queue; Close cancels them and waits. A page is dropped only
// when maxPageQueue pages already wait, and that is logged as a WARN. Delivery
// is best effort: the page event is what is written once (the auto page is
// spent by that event, not by a delivery), and a queued page is lost if the
// process stops before it is sent. Caller holds st's lock.
func (s *Service) notifyPageLocked(st *roomState, e events.Event, p events.OwnerPagedPayload) {
	n := s.paging.cfg.Notifier
	if n == nil || s.isClosed() {
		return
	}
	s.paging.workers.Do(func() {
		// addWorkers fails only once Close has begun: then no worker starts
		// and nothing queued is delivered, as with a close mid-delivery.
		if !s.addWorkers(maxPageDeliveries) {
			return
		}
		for i := 0; i < maxPageDeliveries; i++ {
			go s.pageWorker(n, s.paging.queue)
		}
	})
	job := pageJob{
		body: notify.OwnerPaged{
			RoomID: e.RoomID, RoomTitle: st.snap.Title, PipelineID: st.snap.PipelineID, Layer: p.Layer,
			Reason: p.Reason, ProposalID: p.ProposalID, Auto: p.Auto, At: e.At, Seq: e.Seq,
		},
		ids: append([]string(nil), p.Owners...),
	}
	select {
	case s.paging.queue <- job:
	default:
		s.log.Warn("room: page not sent to the webhook: the delivery queue is full", "room", e.RoomID, "layer", p.Layer, "seq", e.Seq)
	}
}

// pageWorker delivers queued pages until the service closes.
func (s *Service) pageWorker(n PageNotifier, queue <-chan pageJob) {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case job := <-queue:
			ctx, cancel := context.WithTimeout(s.ctx, pageDeliveryTimeout)
			job.body.Owners = s.pageContacts(ctx, job.ids)
			// The notifier logs a failure itself (status code only); a failed
			// delivery never fails the page.
			_ = n.Deliver(ctx, job.body)
			cancel()
		}
	}
}

// pageContacts names the paged people for the webhook: users.yaml first,
// then the IdP (the newest live session: its shown name and verified email),
// else the id alone. Runs outside every lock.
func (s *Service) pageContacts(ctx context.Context, ids []string) []notify.Owner {
	out := make([]notify.Owner, 0, len(ids))
	for _, id := range ids {
		o := notify.Owner{ID: id, Name: id}
		if isGroupOwner(id) {
			// A group: the relay routes on the id; nobody to look up.
		} else if u, ok := s.users.Get(id); ok {
			if u.Name != "" {
				o.Name = u.Name
			}
			o.Email = u.Email
		} else if s.sessions != nil {
			if sess, err := s.sessions.LiveByUserID(ctx, id, s.now(), s.sessionIdle); err == nil {
				name, _ := s.ShownName(sess.UserID, sess.Name, sess.Email)
				if name != "" {
					o.Name = name
				}
				o.Email = sess.Email
			}
		}
		out = append(out, o)
	}
	return out
}

// ---- the automatic page ----

// startAutoPage starts the auto-page loop once, after Load, when
// Paging.UndrivenAfter is set. It stops when the service lifetime ends.
func (s *Service) startAutoPage() {
	if s.paging.cfg.UndrivenAfter <= 0 {
		return
	}
	s.paging.once.Do(func() {
		if !s.addWorker() {
			return
		}
		newTicker := s.paging.cfg.Ticker
		if newTicker == nil {
			newTicker = func(d time.Duration) (<-chan time.Time, func()) {
				t := time.NewTicker(d)
				return t.C, t.Stop
			}
		}
		tick, stop := newTicker(AutoPageCheckEvery)
		go func() {
			defer s.wg.Done()
			defer stop()
			for {
				select {
				case <-s.ctx.Done():
					return
				case <-tick:
				}
				ctx, cancel := context.WithTimeout(s.ctx, autoPageCheckTimeout)
				s.checkAutoPages(ctx)
				cancel()
				if s.paging.tickDone != nil {
					s.paging.tickDone()
				}
			}
		}()
	})
}

// checkAutoPages pages every due room. Like the lease expiry check
// (lease_expiry.go) with a shared store: a room whose agent lease another
// replica holds is left to that replica, a room nobody holds is written by
// whichever replica gets there first, and the checked append lets one page
// land: the others fold it and find the room no longer due.
func (s *Service) checkAutoPages(ctx context.Context) {
	if s.isClosed() {
		return
	}
	// Judged on what each room published; only a room that looks due is
	// locked, and a room busy in a slow call waits for the next check.
	var due []string
	for id, p := range s.publishedRooms() {
		if _, _, ok := s.autoPageDue(p); ok {
			due = append(due, id)
		}
	}
	if len(due) == 0 {
		return
	}
	sort.Strings(due)
	var held map[string]store.RoomLease
	if s.coord != nil {
		cctx, cancel := context.WithTimeout(ctx, coordTimeout)
		leases, err := s.coord.RoomLeases(cctx)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				s.log.Warn("room: auto page: list agent leases failed; retry at the next check", "err", err)
			}
			return
		}
		held = leases
	}
	for _, id := range due {
		if ctx.Err() != nil {
			return
		}
		if l, ok := held[id]; ok && l.Owner != s.replica {
			continue
		}
		// Shared store: fold what other replicas wrote (a driver, a page)
		// before judging the room again; a room busy in a slow sync waits.
		if s.trySyncRoom(ctx, id) {
			continue
		}
		s.autoPage(ctx, id)
	}
}

// autoPage writes the automatic page of one room if it is still due. A room
// whose lock is busy is left for the next check.
func (s *Service) autoPage(ctx context.Context, roomID string) {
	st, _ := s.tryLockCachedRoom(roomID)
	if st == nil {
		return
	}
	layer, reason, ok := s.autoPageDueLocked(roomID, st)
	var pipelineID, label string
	if ok {
		pipelineID, label = st.snap.PipelineID, st.snap.facts.alertLabelLayer
	}
	s.unlockRoom(st)
	if !ok {
		return
	}
	fallback := layer == ""
	if fallback {
		if layer = s.fallbackLayer(ctx, pipelineID, label); layer == "" {
			if s.paging.firstNoOwners(roomID) {
				s.log.Warn("room: auto page skipped: no suspected layer and nobody to page for any layer; retry at the next check", "room", roomID)
			}
			return
		}
		reason = fmt.Sprintf("nobody has taken the driver role %s after this room opened; no layer is suspected, so the owners of %s are paged", fmtWait(s.paging.cfg.UndrivenAfter), layer)
	}
	_, ownerIDs, err := s.page(ctx, roomID, pageReq{
		actor: systemActor, layer: layer, reason: reason, auto: true, needOwners: true,
		check: func(st *roomState) error {
			if l, _, ok := s.autoPageDueLocked(roomID, st); !ok || (l != "" && l != layer) {
				return errPageNotDue
			}
			return nil
		},
	})
	var re *Error
	switch {
	case err == nil:
		s.log.Info("room: paged the owners of an undriven room", "room", roomID, "layer", layer, "owners", len(ownerIDs))
	case errors.Is(err, errPageNotDue):
	case errors.Is(err, errNobodyToPage):
		if s.paging.firstNoOwners(roomID) {
			s.log.Warn("room: auto page skipped: nobody to page for the layer; retry at the next check", "room", roomID, "layer", layer)
		}
	case errors.As(err, &re) && re.Code == "already_paged":
		// Someone paged the layer moments ago; once the window has passed and
		// the room is still undriven, the server pages.
		s.log.Debug("room: auto page waits: the layer was paged recently", "room", roomID, "layer", layer)
	case errors.As(err, &re) && re.Code == codeRoomClosed:
	default:
		if ctx.Err() == nil {
			s.log.Warn("room: auto page failed; retry at the next check", "room", roomID, "layer", layer, "err", err)
		}
	}
}

// autoPageDueLocked reports whether the room is due its automatic page, with
// the layer and the reason: open, opened by an alert or the watcher, no
// driver Paging.UndrivenAfter after it opened, not auto-paged yet (from the
// log, so a restart does not repeat it), and a suspected layer: the watcher's
// first signal, else the triage pass's suspected_layer. With neither the layer
// is "" and autoPage falls back. Caller holds st's lock.
func (s *Service) autoPageDueLocked(roomID string, st *roomState) (layer, reason string, ok bool) {
	if st == nil || st.deleting || !s.roomOpen(st) {
		return "", "", false
	}
	return s.autoPageDue(&st.snap)
}

// autoPageDue is autoPageDueLocked on a snapshot: the room's own, under its
// lock, or a published one (checkAutoPages), which only picks the rooms to
// look at again under the lock.
func (s *Service) autoPageDue(snap *RoomSnapshot) (layer, reason string, ok bool) {
	after := s.paging.cfg.UndrivenAfter
	if after <= 0 || snap.Status != "open" || snap.Driver != nil ||
		snap.facts.alertSource == "" || snap.facts.autoPaged || snap.LogCap == LogCapFull {
		return "", "", false
	}
	if s.now().Sub(snap.OpenedAt) < after {
		return "", "", false
	}
	waited := fmtWait(after)
	if sig := snap.facts.alertSignal; sig != "" {
		layer = s.signalLayer(snap.PipelineID, sig, snap.facts.alertWriters, snap.facts.alertLayer)
		reason = fmt.Sprintf("nobody has taken the driver role %s after this room opened; the watcher's first signal (%s) points at %s", waited, sig, layer)
	} else if t := snap.Triage; t != nil && slices.Contains(events.Layers, t.SuspectedLayer) {
		layer = t.SuspectedLayer
		reason = fmt.Sprintf("nobody has taken the driver role %s after this room opened; the first look suspects %s", waited, layer)
	}
	// layer "" is due too: autoPage then falls back (fallbackLayer).
	return layer, reason, true
}

// fmtWait renders the undriven wait for the page reason ("5 min").
func fmtWait(d time.Duration) string {
	if d >= time.Minute && d%time.Minute == 0 {
		return fmt.Sprintf("%d min", int(d/time.Minute))
	}
	return d.String()
}
