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
// Explicit <@U…> markup still works inside a verbatim:true text object
// (https://docs.slack.dev/reference/block-kit/composition-objects/text-object).

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
	Users  map[string]string   // Piperoom user ID (hand-off "to") -> Slack user ID
	Limits PagingLimits
}

type pager struct {
	layers map[string][]string
	users  map[string]string
	lim    PagingLimits
	now    func() time.Time

	mu       sync.Mutex
	last     map[[2]string]time.Time // room, user
	userHits map[string][]time.Time
	roomHits map[string][]time.Time
	seen     map[string]struct{}
}

// newPager validates o. It returns nil when nothing is allow-listed.
func newPager(o PagingOptions) (*pager, error) {
	if len(o.Layers) == 0 && len(o.Users) == 0 {
		return nil, nil
	}
	p := &pager{
		layers: map[string][]string{}, users: map[string]string{}, lim: o.Limits, now: time.Now,
		last: map[[2]string]time.Time{}, userHits: map[string][]time.Time{},
		roomHits: map[string][]time.Time{}, seen: map[string]struct{}{},
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
	for user, id := range o.Users {
		if user == "" || !slackUserID.MatchString(id) {
			return nil, fmt.Errorf("slackbridge: paging: user %q has an empty key or an invalid Slack user ID", user)
		}
		p.users[user] = id
	}
	return p, nil
}

// allow decides whether to page user in room for reason, and records the page
// when it says yes. The first rule that fails wins: a repeated reason is
// never paged twice, then the per-user-per-room gap, then the hourly caps.
func (p *pager) allow(room, user, reason string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := room + "\x00" + user + "\x00" + reason
	if _, dup := p.seen[key]; dup {
		return false
	}
	now := p.now()
	if t, ok := p.last[[2]string{room, user}]; ok && now.Sub(t) < p.lim.UserRoomInterval {
		return false
	}
	uh, rh := recent(p.userHits[user], now), recent(p.roomHits[room], now)
	if len(uh) >= p.lim.UserPerHour || len(rh) >= p.lim.RoomPerHour {
		p.userHits[user], p.roomHits[room] = uh, rh
		return false
	}
	if len(p.seen) >= maxSeenPages {
		clear(p.seen) // the rate limits above still hold
	}
	p.seen[key] = struct{}{}
	p.last[[2]string{room, user}] = now
	p.userHits[user] = append(uh, now)
	p.roomHits[room] = append(rh, now)
	return true
}

func recent(ts []time.Time, now time.Time) []time.Time {
	i := 0
	for i < len(ts) && now.Sub(ts[i]) >= time.Hour {
		i++
	}
	return append([]time.Time(nil), ts[i:]...)
}

func mention(id string) string { return "<@" + id + ">" }

// message builds the page for e. ok is false when nobody is to be paged.
func (p *pager) message(r *renderer, e events.Event) (m message, ok bool, err error) {
	switch e.Type {
	case events.ProposalCreated:
		pl, err := events.Decode[events.ProposalCreatedPayload](e)
		if err != nil {
			return m, false, err
		}
		var lines, fb []string
		picked := map[string]bool{}
		for i, layer := range pl.RequiredLayers {
			if i == maxLayers {
				break
			}
			var ms []string
			for _, id := range p.layers[layer] { // layer is only a map key
				if picked[id] || len(picked) >= maxPagedUsers || !p.allow(e.RoomID, id, "proposal:"+pl.ProposalID) {
					continue
				}
				picked[id] = true
				ms = append(ms, mention(id))
			}
			if len(ms) > 0 {
				lines = append(lines, inlineCode(layer, 60)+": "+strings.Join(ms, " "))
				fb = append(fb, ms...)
			}
		}
		if len(lines) == 0 {
			return m, false, nil
		}
		text := ":bell: *Approval needed* for " + clip(pl.Title, 200) + " (" + inlineCode(pl.ProposalID, 100) + ")\n" +
			strings.Join(lines, "\n")
		return r.pageMessage(e.RoomID, "Approval needed: ", fb, pl.Title, text, "Approve or reject in Piperoom"), true, nil

	case events.HandoffCreated:
		pl, err := events.Decode[events.HandoffCreatedPayload](e)
		if err != nil {
			return m, false, err
		}
		id, listed := p.users[pl.To.ID] // To.ID is only a map key
		if !listed || !p.allow(e.RoomID, id, "handoff:"+strconv.FormatInt(e.Seq, 10)) {
			return m, false, nil
		}
		text := ":handshake: " + mention(id) + " " + clip(actorName(e.Actor), 100) + " is handing this room over to you"
		return r.pageMessage(e.RoomID, "Hand-off to you: ", []string{mention(id)}, actorName(e.Actor), text, "Accept the hand-off in Piperoom"), true, nil
	}
	return m, false, nil
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
