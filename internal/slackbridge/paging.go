package slackbridge

import (
	"fmt"
	"regexp"
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
// (control.lease_expired, to Users[driver.id] plus NoDriver).

// slackUserID is the shape of a Slack member ID. It also keeps `|`, `>` and
// spaces out of a configured ID, so a config typo cannot break the markup.
var slackUserID = regexp.MustCompile(`^[UW][A-Z0-9]{8,20}$`)

const (
	maxPagedUsers   = 20 // per message
	maxFallbackPage = 6  // mentions repeated in the notification text
	maxSeenPages    = 4096
)

// PagingLimits bounds how often people are paged. Zero fields take defaults.
type PagingLimits struct {
	UserRoomInterval time.Duration // min gap between pages of one user in one room (default 10m)
	UserPerHour      int           // pages of one user across all rooms (default 6)
	RoomPerHour      int           // user-pages in one room (default 20)
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
	roomHits map[string][]time.Time
	seen     map[pageKey]struct{}
}

// newPager validates o. It returns nil when nothing is allow-listed.
func newPager(o PagingOptions) (*pager, error) {
	if len(o.Layers) == 0 && len(o.Users) == 0 && len(o.NoDriver) == 0 {
		return nil, nil
	}
	p := &pager{
		layers: map[string][]string{}, users: map[string]string{}, lim: o.Limits, now: time.Now,
		last: map[[2]string]time.Time{}, userHits: map[string][]time.Time{},
		roomHits: map[string][]time.Time{}, seen: map[pageKey]struct{}{},
	}
	if p.lim.UserRoomInterval <= 0 {
		p.lim.UserRoomInterval = 10 * time.Minute
	}
	if p.lim.UserPerHour <= 0 {
		p.lim.UserPerHour = 6
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
	if t, ok := p.last[[2]string{key.room, key.user}]; ok && now.Sub(t) < p.lim.UserRoomInterval {
		return false
	}
	var userPlanned, roomPlanned int
	for _, k := range planned {
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

// record books pages that Slack accepted.
func (p *pager) record(keys []pageKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	for _, k := range keys {
		if len(p.seen) >= maxSeenPages {
			clear(p.seen) // the rate limits still hold
		}
		p.seen[k] = struct{}{}
		p.last[[2]string{k.room, k.user}] = now
		p.userHits[k.user] = append(recent(p.userHits[k.user], now), now)
		p.roomHits[k.room] = append(recent(p.roomHits[k.room], now), now)
	}
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
