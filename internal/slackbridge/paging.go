package slackbridge

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"piperoom/internal/events"
)

// Paging mentions people in a thread reply so Slack notifies them: the layer
// owners whose approval a proposal needs, and the engineer a hand-off is for.
//
// Injection rule: a Slack mention (<@U…>) is built ONLY from a user ID that
// was given in PagingOptions (owner/config data) and passed slackUserID in
// New. Event text (layer names, actor IDs, titles, reasons) is used as a
// lookup KEY at most, never copied into a mention; everything else from an
// event goes through clip, which escapes &, < and > so <@U…>, <!here>,
// <!channel>, <!everyone>, <!subteam^…> and <url|label> render as plain text
// (https://docs.slack.dev/messaging/formatting-message-text#escaping).
// Delivery: limits and de-duplication are recorded only AFTER Slack accepted
// the page, so a failed post does not use up a user's quota. A failed page is
// logged by the bridge and NOT retried: the event's LastSeq still advances, so
// that one page is dropped (the room link and the proposal itself are in the
// thread's other messages).
//
// Explicit <@U…> markup still works inside a verbatim:true text object
// (https://docs.slack.dev/reference/block-kit/composition-objects/text-object).

// The pages are: approval needed (proposal.created, to the required layers'
// owners), hand-off (handoff.created, to Users[to.id]) and no active driver
// (control.lease_expired, to Users[driver.id] plus NoDriver) and nudge
// (approval.nudged, to the owners of the nudged layer only).

// slackUserID is the shape of a Slack member ID. It also keeps `|`, `>` and
// spaces out of a configured ID, so a config typo cannot break the markup.
var slackUserID = regexp.MustCompile(`^[UW][A-Z0-9]{8,20}$`)

const (
	maxPagedUsers   = 20 // per message
	maxFallbackPage = 6  // mentions repeated in the notification text
	maxSeenPages    = 4096
	// maxTrackedRooms caps the per-room rate-limit maps. A room evicted by the
	// cap loses its gap and hourly count, which only ever lets one more page
	// through; the per-user hourly cap still holds.
	maxTrackedRooms = 4096
	// pruneEvery spaces the sweeps for idle entries, so record stays cheap.
	pruneEvery = time.Minute
	// gapSlack lets a page through slightly before the gap is over: the gap is
	// stamped after the Slack post, which can trail the event by a second or more,
	// and the server allows a re-nudge exactly 10 minutes after the last one.
	gapSlack = 2 * time.Second
)

// PagingLimits bounds how often people are paged. Zero fields take defaults.
type PagingLimits struct {
	UserRoomInterval time.Duration // min gap between pages of one user in one room (default 10m)
	UserPerHour      int           // pages of one user across all rooms (default 6)
	RoomPerHour      int           // user-pages in one room (default 20)
	// NudgeRoomPerHour caps nudge pages of one user in one room (default 2), inside
	// the UserPerHour nudge budget, so one room cannot use up the owner's budget for others.
	NudgeRoomPerHour int
}

// PagingOptions is the allow-list. Paging is off while both maps are empty.
// IDs come from owner/config data, never from events.
type PagingOptions struct {
	Layers map[string][]string // required layer name -> Slack user IDs of its owners
	Users  map[string]string   // Piperoom user ID (hand-off "to", lapsed driver) -> Slack user ID
	// NoDriver are the on-call Slack IDs paged, besides the lapsed driver, when
	// a driver lease expires and the room has nobody steering.
	NoDriver []string
	Limits   PagingLimits
}

type pager struct {
	layers map[string][]string
	users  map[string]string
	noDrv  []string
	lim    PagingLimits
	now    func() time.Time

	mu       sync.Mutex
	last     map[[2]string]time.Time // room, user
	userHits map[string][]time.Time
	// nudgeHits counts person-triggered nudge pages per user, apart from userHits,
	// so a room member's nudges cannot use up an owner's budget for agent pages.
	nudgeHits map[string][]time.Time
	// nudgeRoom counts the same pages per (room, user); bounded like roomHits.
	nudgeRoom map[[2]string][]time.Time
	roomHits  map[string][]time.Time
	seen      map[pageKey]struct{}
	seenQ     []pageKey // insertion order of seen, oldest first
	lastSwp   time.Time
}

// newPager validates o. It returns nil when nothing is allow-listed.
func newPager(o PagingOptions) (*pager, error) {
	if len(o.Layers) == 0 && len(o.Users) == 0 && len(o.NoDriver) == 0 {
		return nil, nil
	}
	p := &pager{
		layers: map[string][]string{}, users: map[string]string{}, lim: o.Limits, now: time.Now,
		last: map[[2]string]time.Time{}, userHits: map[string][]time.Time{},
		nudgeHits: map[string][]time.Time{}, nudgeRoom: map[[2]string][]time.Time{},
		roomHits: map[string][]time.Time{}, seen: map[pageKey]struct{}{},
	}
	if p.lim.UserRoomInterval <= 0 {
		p.lim.UserRoomInterval = 10 * time.Minute
	}
	if p.lim.UserPerHour <= 0 {
		p.lim.UserPerHour = 6
	}
	if p.lim.NudgeRoomPerHour <= 0 {
		p.lim.NudgeRoomPerHour = 2
	}
	if p.lim.RoomPerHour <= 0 {
		p.lim.RoomPerHour = 20
	}
	for layer, ids := range o.Layers {
		if layer == "" {
			return nil, fmt.Errorf("slackbridge: paging: empty layer name")
		}
		for _, id := range ids {
			if !slackUserID.MatchString(id) {
				return nil, fmt.Errorf("slackbridge: paging: layer %q has an invalid Slack user ID", layer)
			}
		}
		p.layers[layer] = append([]string(nil), ids...)
	}
	for _, id := range o.NoDriver {
		if !slackUserID.MatchString(id) {
			return nil, fmt.Errorf("slackbridge: paging: a no-driver on-call entry is not a valid Slack user ID")
		}
	}
	p.noDrv = append([]string(nil), o.NoDriver...)
	for user, id := range o.Users {
		if user == "" || !slackUserID.MatchString(id) {
			return nil, fmt.Errorf("slackbridge: paging: user %q has an empty key or an invalid Slack user ID", user)
		}
		p.users[user] = id
	}
	return p, nil
}

// pageKey names one page: who, in which room, for which reason.
type pageKey struct{ room, user, reason string }

// isNudge reports whether k is a person-triggered nudge page. The server already
// limits nudges to one per (proposal, layer) every 10 minutes, so they skip the
// per-user-per-room gap and have their own hourly bucket.
func (k pageKey) isNudge() bool { return strings.HasPrefix(k.reason, "nudge:") }

// check reports whether key may be paged now, counting the pages already
// planned in the same message. It records nothing; call record once Slack
// accepted the post. The first rule that fails wins: a repeated reason is
// never paged twice, then the per-user-per-room gap, then the hourly caps.
func (p *pager) check(key pageKey, planned []pageKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, dup := p.seen[key]; dup {
		return false
	}
	now := p.now()
	if key.isNudge() {
		var planned1, plannedRoom int
		for _, k := range planned {
			if k.isNudge() && k.user == key.user {
				planned1++
				if k.room == key.room {
					plannedRoom++
				}
			}
		}
		rk := [2]string{key.room, key.user}
		return len(recent(p.nudgeHits[key.user], now))+planned1 < p.lim.UserPerHour &&
			len(recent(p.nudgeRoom[rk], now))+plannedRoom < p.lim.NudgeRoomPerHour
	}
	if t, ok := p.last[[2]string{key.room, key.user}]; ok && now.Sub(t)+gapSlack < p.lim.UserRoomInterval {
		return false
	}
	var userPlanned, roomPlanned int
	for _, k := range planned {
		if k.isNudge() {
			continue
		}
		if k.user == key.user {
			userPlanned++
		}
		if k.room == key.room {
			roomPlanned++
		}
	}
	if len(recent(p.userHits[key.user], now))+userPlanned >= p.lim.UserPerHour ||
		len(recent(p.roomHits[key.room], now))+roomPlanned >= p.lim.RoomPerHour {
		return false
	}
	return true
}

// record books pages that Slack accepted. It also drops state that can no
// longer change a decision (see prune) and enforces the caps.
func (p *pager) record(keys []pageKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	for _, k := range keys {
		if _, dup := p.seen[k]; !dup {
			p.seen[k] = struct{}{}
			p.seenQ = append(p.seenQ, k)
		}
		if k.isNudge() {
			p.nudgeHits[k.user] = append(recent(p.nudgeHits[k.user], now), now)
			rk := [2]string{k.room, k.user}
			p.nudgeRoom[rk] = append(recent(p.nudgeRoom[rk], now), now)
			continue
		}
		p.last[[2]string{k.room, k.user}] = now
		p.userHits[k.user] = append(recent(p.userHits[k.user], now), now)
		p.roomHits[k.room] = append(recent(p.roomHits[k.room], now), now)
	}
	p.prune(now)
}

// prune drops entries that no longer block anything: a gap older than
// UserRoomInterval and hits older than an hour. Then it evicts the oldest
// entries over the caps. Callers hold p.mu.
func (p *pager) prune(now time.Time) {
	if now.Sub(p.lastSwp) >= pruneEvery || now.Before(p.lastSwp) {
		p.lastSwp = now
		for k, t := range p.last {
			if now.Sub(t) >= p.lim.UserRoomInterval {
				delete(p.last, k)
			}
		}
		for room, hits := range p.roomHits {
			if hits = recent(hits, now); len(hits) == 0 {
				delete(p.roomHits, room)
			} else {
				p.roomHits[room] = hits
			}
		}
		for rk, hits := range p.nudgeRoom {
			if hits = recent(hits, now); len(hits) == 0 {
				delete(p.nudgeRoom, rk)
			} else {
				p.nudgeRoom[rk] = hits
			}
		}
		for user, hits := range p.nudgeHits {
			if hits = recent(hits, now); len(hits) == 0 {
				delete(p.nudgeHits, user)
			} else {
				p.nudgeHits[user] = hits
			}
		}
		for user, hits := range p.userHits {
			if hits = recent(hits, now); len(hits) == 0 {
				delete(p.userHits, user)
			} else {
				p.userHits[user] = hits
			}
		}
	}
	if len(p.last) > maxTrackedRooms {
		evictOldest(p.last, maxTrackedRooms*3/4, func(t time.Time) time.Time { return t })
	}
	if len(p.roomHits) > maxTrackedRooms {
		evictOldest(p.roomHits, maxTrackedRooms*3/4, func(ts []time.Time) time.Time { return ts[len(ts)-1] })
	}
	if len(p.nudgeRoom) > maxTrackedRooms {
		evictOldest(p.nudgeRoom, maxTrackedRooms*3/4, func(ts []time.Time) time.Time { return ts[len(ts)-1] })
	}
	if len(p.seenQ) > maxSeenPages {
		drop := len(p.seenQ) - maxSeenPages
		for _, k := range p.seenQ[:drop] {
			delete(p.seen, k)
		}
		p.seenQ = append([]pageKey(nil), p.seenQ[drop:]...)
	}
}

// evictOldest deletes entries of m until keep remain, oldest activity first.
// It trims below the cap so the sort does not run on every record.
func evictOldest[K comparable, V any](m map[K]V, keep int, at func(V) time.Time) {
	type kt struct {
		k K
		t time.Time
	}
	all := make([]kt, 0, len(m))
	for k, v := range m {
		all = append(all, kt{k, at(v)})
	}
	slices.SortFunc(all, func(a, b kt) int { return a.t.Compare(b.t) })
	for _, e := range all[:len(all)-keep] {
		delete(m, e.k)
	}
}

// forgetRoom drops everything kept for a room that closed. A deleted room never
// reaches the bridge (room.deleted is not in the log); prune ages it out.
func (p *pager) forgetRoom(room string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k := range p.last {
		if k[0] == room {
			delete(p.last, k)
		}
	}
	delete(p.roomHits, room)
	q := p.seenQ[:0]
	for _, k := range p.seenQ {
		if k.room == room {
			delete(p.seen, k)
			continue
		}
		q = append(q, k)
	}
	p.seenQ = q
}

func recent(ts []time.Time, now time.Time) []time.Time {
	i := 0
	for i < len(ts) && now.Sub(ts[i]) >= time.Hour {
		i++
	}
	return append([]time.Time(nil), ts[i:]...)
}

func mention(id string) string { return "<@" + id + ">" }

// message builds the page for e and the keys to record once it is posted;
// keys is empty when nobody is to be paged.
func (p *pager) message(r *renderer, e events.Event) (m message, keys []pageKey, err error) {
	switch e.Type {
	case events.ProposalCreated:
		pl, err := events.Decode[events.ProposalCreatedPayload](e)
		if err != nil {
			return m, nil, err
		}
		var lines, fb []string
		picked := map[string]bool{}
		for i, layer := range pl.RequiredLayers {
			if i == maxLayers {
				break
			}
			var ms []string
			for _, id := range p.layers[layer] { // layer is only a map key
				k := pageKey{e.RoomID, id, "proposal:" + pl.ProposalID}
				if picked[id] || len(picked) >= maxPagedUsers || !p.check(k, keys) {
					continue
				}
				picked[id] = true
				keys = append(keys, k)
				ms = append(ms, mention(id))
			}
			if len(ms) > 0 {
				lines = append(lines, inlineCode(layer, 60)+": "+strings.Join(ms, " "))
				fb = append(fb, ms...)
			}
		}
		if len(lines) == 0 {
			return m, nil, nil
		}
		text := ":bell: *Approval needed* for " + clip(pl.Title, 200) + " (" + inlineCode(pl.ProposalID, 100) + ")\n" +
			strings.Join(lines, "\n")
		return r.pageMessage(e.RoomID, "Approval needed: ", fb, pl.Title, text, "Approve or reject in Piperoom"), keys, nil

	case events.HandoffCreated:
		pl, err := events.Decode[events.HandoffCreatedPayload](e)
		if err != nil {
			return m, nil, err
		}
		id, listed := p.users[pl.To.ID] // To.ID is only a map key
		k := pageKey{e.RoomID, id, "handoff:" + strconv.FormatInt(e.Seq, 10)}
		if !listed || !p.check(k, nil) {
			return m, nil, nil
		}
		text := ":handshake: " + mention(id) + " " + clip(actorName(e.Actor), 100) + " is handing this room over to you"
		return r.pageMessage(e.RoomID, "Hand-off to you: ", []string{mention(id)}, actorName(e.Actor), text, "Accept the hand-off in Piperoom"), []pageKey{k}, nil

	case events.ApprovalNudged:
		pl, err := events.Decode[events.ApprovalNudgedPayload](e)
		if err != nil {
			return m, nil, err
		}
		// Only the nudged layer's owners are paged; the layer is a map key.
		// The key is the event seq: one nudge event pages once, even when the
		// stream is redelivered, and the re-nudge the server allows every 10
		// minutes is a new event that pages again. check adds the nudge hourly cap;
		// the per-room gap does not apply, or a nudge soon after the proposal page
		// would page nobody.
		var ms []string
		picked := map[string]bool{}
		for _, id := range p.layers[pl.Layer] {
			k := pageKey{e.RoomID, id, "nudge:" + strconv.FormatInt(e.Seq, 10)}
			if picked[id] || len(ms) >= maxPagedUsers || !p.check(k, keys) {
				continue
			}
			picked[id] = true
			keys = append(keys, k)
			ms = append(ms, mention(id))
		}
		if len(keys) == 0 {
			return m, nil, nil
		}
		text := ":bell: " + strings.Join(ms, " ") + " " + clip(actorName(e.Actor), 100) + " asked for your approval of " +
			inlineCode(pl.ProposalID, 100) + " (layer " + inlineCode(pl.Layer, 60) + ")"
		return r.pageMessage(e.RoomID, "Your approval is needed: ", ms, actorName(e.Actor), text, "Approve or reject in Piperoom"), keys, nil

	case events.ControlLeaseExpired:
		pl, err := events.Decode[events.ControlLeaseExpiredPayload](e)
		if err != nil {
			return m, nil, err
		}
		// Driver.ID is only a map key. One page per lease: the event is
		// written once per (room, lease_until), and the key repeats that.
		reason := "lease:" + pl.LeaseUntil.UTC().Format(time.RFC3339)
		ids := append([]string(nil), p.noDrv...)
		if id, listed := p.users[pl.Driver.ID]; listed {
			ids = append([]string{id}, ids...)
		}
		var ms []string
		picked := map[string]bool{}
		for _, id := range ids {
			k := pageKey{e.RoomID, id, reason}
			if picked[id] || len(picked) >= maxPagedUsers || !p.check(k, keys) {
				continue
			}
			picked[id] = true
			keys = append(keys, k)
			ms = append(ms, mention(id))
		}
		if len(keys) == 0 {
			return m, nil, nil
		}
		text := ":rotating_light: *Room has no active driver* " + strings.Join(ms, " ") + "\nThe lease of " +
			clip(actorName(pl.Driver), 100) + " ran out at " + pl.LeaseUntil.UTC().Format(time.RFC3339) + "."
		return r.pageMessage(e.RoomID, "No active driver: ", ms, actorName(pl.Driver), text, "Take control in Piperoom"), keys, nil
	}
	return m, nil, nil
}

// pageMessage wraps trusted+escaped text. The fallback text carries the
// mentions too, because Slack builds notifications from it.
func (r *renderer) pageMessage(roomID, prefix string, mentions []string, untrusted, text, footer string) message {
	if len(mentions) > maxFallbackPage {
		mentions = mentions[:maxFallbackPage]
	}
	head := strings.Join(mentions, " ") + " " + prefix
	foot := append([]textObj{md(footer)}, r.linkElement(roomID)...)
	return message{
		Text:   head + clip(untrusted, max(fallbackBytes-len(head), 20)),
		Blocks: []block{section(text), {Type: "context", Elements: foot}},
	}
}
