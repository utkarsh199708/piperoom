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
	// is not delivered by webhook (it is still in the room and in Slack).
	maxPageDeliveries = 8
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
	cfg  Paging
	sem  chan struct{} // webhook deliveries in flight
	once sync.Once     // the auto-page loop starts once
	// tickDone, when set (tests only), runs after each check of the loop.
	tickDone func()
	// noLayer marks rooms whose missing suspected layer was logged, so the
	// debug line is written once per room. Guarded by Service.mu.
	noLayer map[string]bool
}

func newPagingState(p Paging) *pagingState {
	return &pagingState{cfg: p, sem: make(chan struct{}, maxPageDeliveries), noLayer: map[string]bool{}}
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
	// known layer (signalLayers), and alertWriters its DLQ writers.
	alertSignal  string
	alertWriters []string
	// lastActivity is the time of the newest event that is room activity
	// (countsAsActivity).
	lastActivity time.Time
	// autoPaged is set by an owner.paged with auto: the server's own page is
	// written once per room, and the log says whether it was.
	autoPaged bool
}

// foldRoomFacts is Apply's Q1 step: the snapshot's pages and the room facts.
// Like Apply it never mutates what the input snapshot shares.
func foldRoomFacts(snap *RoomSnapshot, e events.Event) {
	switch e.Type {
	case events.RoomCreated:
		if p, ok := decode[events.RoomCreatedPayload](e); ok {
			snap.facts.alertSource = p.AlertSource
			if p.AlertSource == string(alertin.Watcher) {
				snap.facts.alertSignal, snap.facts.alertWriters = firstWatcherSignal(p.Alert)
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
}

// firstWatcherSignal reads the watcher evidence of room.created.alert
// (internal/watch compose: {"source":"watcher","signals":[{"signal":...}]})
// and returns its first signal with a known layer, and that signal's DLQ
// writers. An older body with one top-level "signal" is read too.
func firstWatcherSignal(raw json.RawMessage) (string, []string) {
	var body struct {
		Signal  string `json:"signal"`
		Signals []struct {
			Signal  string          `json:"signal"`
			Writers json.RawMessage `json:"writers"`
		} `json:"signals"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &body) != nil {
		return "", nil
	}
	for _, sg := range body.Signals {
		if _, ok := signalLayers[sg.Signal]; ok {
			var writers []string
			_ = json.Unmarshal(sg.Writers, &writers) // absent or odd: no writers
			return sg.Signal, writers
		}
	}
	if _, ok := signalLayers[body.Signal]; ok {
		return body.Signal, nil
	}
	return "", nil
}

// signalLayer is the layer of a watcher signal in pipelineID.
func (s *Service) signalLayer(pipelineID, signal string, writers []string) string {
	layer := signalLayers[signal]
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
	// check adds the writer's own condition under s.mu: the agent run's
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
// are resolved between two holds of s.mu, so no store or session read runs
// under it; everything is checked again before the append (a checked append
// with a shared store), so a page the first check let through can still be
// refused.
func (s *Service) page(ctx context.Context, roomID string, r pageReq) (events.Event, []string, error) {
	if !slices.Contains(events.Layers, r.layer) {
		return events.Event{}, nil, validation("layer must be one of " + strings.Join(events.Layers, ", "))
	}
	// Shared store: fold what other replicas wrote, reading outside s.mu.
	s.syncRoom(ctx, roomID)
	s.mu.Lock()
	st, err := s.getStateLocked(ctx, roomID)
	if err == nil {
		err = s.pageCheckLocked(st, r)
	}
	if err != nil {
		s.mu.Unlock()
		return events.Event{}, nil, err
	}
	pipelineID := st.snap.PipelineID
	s.mu.Unlock()

	ownerIDs := s.pageOwners(ctx, pipelineID, r.layer)

	s.mu.Lock()
	defer s.mu.Unlock()
	if st, err = s.getStateLocked(ctx, roomID); err != nil {
		return events.Event{}, nil, err
	}
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
// s.mu.
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

// pageOwners resolves the user ids to page for a layer of a pipeline: the
// people the approve path accepts for it (authorizeLayer, the not_layer_owner
// rule), approvers first, then backups, each id once, never nil. Named
// owners.yaml users come first in file order; then users.yaml users and
// members with a live session whose groups (or, in dev mode, users.yaml
// layers) grant the layer. The solo user's blanket grant does not count.
func (s *Service) pageOwners(ctx context.Context, pipelineID, layer string) []string {
	out := []string{}
	seen := map[string]bool{}
	add := func(id string) {
		if id != "" && !seen[id] {
			seen[id] = true
			out = append(out, id)
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
	for _, id := range backups.Users() {
		add(id)
	}
	for _, id := range candBackups {
		add(id)
	}
	return out
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

// notifyPageLocked hands a written page to the notifier on a goroutine of the
// service (s.wg, s.ctx), so the page never waits on the webhook and Close
// waits for, and cancels, deliveries in flight. At most maxPageDeliveries run
// at once; a page past that is logged and not delivered by webhook. Caller
// holds s.mu.
func (s *Service) notifyPageLocked(st *roomState, e events.Event, p events.OwnerPagedPayload) {
	n := s.paging.cfg.Notifier
	if n == nil || s.closed {
		return
	}
	select {
	case s.paging.sem <- struct{}{}:
	default:
		s.log.Warn("room: page not sent to the webhook: too many deliveries in flight", "room", e.RoomID, "layer", p.Layer, "seq", e.Seq)
		return
	}
	body := notify.OwnerPaged{
		RoomID: e.RoomID, RoomTitle: st.snap.Title, PipelineID: st.snap.PipelineID, Layer: p.Layer,
		Reason: p.Reason, ProposalID: p.ProposalID, Auto: p.Auto, At: e.At, Seq: e.Seq,
	}
	ids := append([]string(nil), p.Owners...)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { <-s.paging.sem }()
		ctx, cancel := context.WithTimeout(s.ctx, pageDeliveryTimeout)
		defer cancel()
		body.Owners = s.pageContacts(ctx, ids)
		// The notifier logs a failure itself (status code only); a failed
		// delivery never fails the page.
		_ = n.Deliver(ctx, body)
	}()
}

// pageContacts names the paged people for the webhook: users.yaml first,
// then the IdP (the newest live session: its shown name and verified email),
// else the id alone. Runs outside s.mu.
func (s *Service) pageContacts(ctx context.Context, ids []string) []notify.Owner {
	out := make([]notify.Owner, 0, len(ids))
	for _, id := range ids {
		o := notify.Owner{ID: id, Name: id}
		if u, ok := s.users.Get(id); ok {
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
		s.mu.Lock()
		closed := s.closed
		if !closed {
			s.wg.Add(1)
		}
		s.mu.Unlock()
		if closed {
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
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	var due []string
	for id, st := range s.rooms {
		if _, _, ok := s.autoPageDueLocked(id, st); ok {
			due = append(due, id)
		}
	}
	s.mu.Unlock()
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
		s.autoPage(ctx, id)
	}
}

// autoPage writes the automatic page of one room if it is still due.
func (s *Service) autoPage(ctx context.Context, roomID string) {
	s.syncRoom(ctx, roomID)
	s.mu.Lock()
	st := s.rooms[roomID]
	layer, reason, ok := s.autoPageDueLocked(roomID, st)
	s.mu.Unlock()
	if !ok {
		return
	}
	_, ownerIDs, err := s.page(ctx, roomID, pageReq{
		actor: systemActor, layer: layer, reason: reason, auto: true,
		check: func(st *roomState) error {
			if l, _, ok := s.autoPageDueLocked(roomID, st); !ok || l != layer {
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
// first signal, else the triage pass's suspected_layer. A room with neither is
// skipped (one debug line). Caller holds s.mu.
func (s *Service) autoPageDueLocked(roomID string, st *roomState) (layer, reason string, ok bool) {
	after := s.paging.cfg.UndrivenAfter
	if after <= 0 || st == nil || st.deleting || !s.roomOpen(st) || st.snap.Driver != nil ||
		st.snap.facts.alertSource == "" || st.snap.facts.autoPaged || st.snap.LogCap == LogCapFull {
		return "", "", false
	}
	if s.now().Sub(st.snap.OpenedAt) < after {
		return "", "", false
	}
	waited := fmtWait(after)
	if sig := st.snap.facts.alertSignal; sig != "" {
		layer = s.signalLayer(st.snap.PipelineID, sig, st.snap.facts.alertWriters)
		reason = fmt.Sprintf("nobody has taken the driver role %s after this room opened; the watcher's first signal (%s) points at %s", waited, sig, layer)
	} else if t := st.snap.Triage; t != nil && slices.Contains(events.Layers, t.SuspectedLayer) {
		layer = t.SuspectedLayer
		reason = fmt.Sprintf("nobody has taken the driver role %s after this room opened; the first look suspects %s", waited, layer)
	}
	if layer == "" {
		if !s.paging.noLayer[roomID] {
			s.paging.noLayer[roomID] = true
			s.log.Debug("room: auto page skipped: no suspected layer yet", "room", roomID)
		}
		return "", "", false
	}
	return layer, reason, true
}

// fmtWait renders the undriven wait for the page reason ("5 min").
func fmtWait(d time.Duration) string {
	if d >= time.Minute && d%time.Minute == 0 {
		return fmt.Sprintf("%d min", int(d/time.Minute))
	}
	return d.String()
}
