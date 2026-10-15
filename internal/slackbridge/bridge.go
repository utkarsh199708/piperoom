// Package slackbridge mirrors each incident room into one Slack thread.
//
// The bridge is outbound only: nothing typed in Slack reaches a room, and
// approvals happen only in Piperoom. Use a PRIVATE channel: Slack reaches more
// people than the room does, and the thread repeats proposals and hand-offs.
// A crash between a Slack post and saving the thread state can repeat one
// message after a restart.
//
// Paging (see paging.go) mentions only Slack user IDs from PagingOptions, which
// come from owner/config data, never from event text. A restart forgets the
// in-memory page limits, so one page can repeat after a crash.
//
// Slack API references: chat.postMessage (arguments, errors)
// https://docs.slack.dev/reference/methods/chat.postMessage ; rate limits
// https://docs.slack.dev/apis/web-api/rate-limits ; Block Kit text limits
// https://docs.slack.dev/reference/block-kit/composition-objects/text-object .
package slackbridge

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/events"
)

// ErrDisabled means no Slack settings are configured; callers skip the bridge.
var ErrDisabled = errors.New("slackbridge: disabled (no token and no channel)")

// ErrAlreadyRunning means Run is already mirroring that room.
var ErrAlreadyRunning = errors.New("slackbridge: room is already being mirrored")

const defaultAPIBase = "https://slack.com/api"

// Options configures a Bridge. Settings arrive here, not from config.Load.
type Options struct {
	Token      netsec.Secret // bot token with the chat:write scope
	ChannelID  string        // a channel ID such as C0123, never a #name
	PublicURL  string        // base URL of the web UI; empty = no room links
	APIBaseURL string        // tests only; default https://slack.com/api
	HTTPClient *http.Client  // optional; redirects are never followed
	Threads    Threads       // default NewMemThreads()
	Logger     *slog.Logger  // default discards
	Paging     PagingOptions // who may be mentioned; empty = no paging
}

// Bridge mirrors room events to Slack.
type Bridge struct {
	store     events.Store
	client    *client
	channel   string
	publicURL string
	threads   Threads
	log       *slog.Logger
	pager     *pager // nil = paging off

	mu      sync.Mutex
	running map[string]bool
}

// New validates o and builds a Bridge.
func New(store events.Store, o Options) (*Bridge, error) {
	if o.Token == "" && o.ChannelID == "" {
		return nil, ErrDisabled
	}
	if o.Token == "" || o.ChannelID == "" {
		return nil, errors.New("slackbridge: token and channel id must be set together")
	}
	if strings.HasPrefix(o.ChannelID, "#") || strings.ContainsAny(o.ChannelID, " \t\n") {
		return nil, errors.New("slackbridge: channel must be a channel ID (like C0123ABC), not a name")
	}
	pub := strings.TrimRight(o.PublicURL, "/")
	if pub != "" {
		u, err := url.Parse(pub)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || strings.ContainsAny(pub, "<>| \t\n") {
			return nil, errors.New("slackbridge: public url must be an absolute http(s) URL")
		}
	}
	hc := &http.Client{Timeout: attemptTimeout}
	if o.HTTPClient != nil {
		cp := *o.HTTPClient
		hc = &cp
	}
	// Never follow redirects: the bearer header must not reach another host.
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	base := strings.TrimRight(o.APIBaseURL, "/")
	if base == "" {
		base = defaultAPIBase
	}
	if o.Threads == nil {
		o.Threads = NewMemThreads()
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	pg, err := newPager(o.Paging)
	if err != nil {
		return nil, err
	}
	return &Bridge{
		pager: pg, store: store, client: newClient(hc, base, o.Token), channel: o.ChannelID, publicURL: pub,
		threads: o.Threads, log: o.Logger, running: map[string]bool{},
	}, nil
}

// fatalCodes stop one room's mirror: retrying cannot help until someone fixes
// the token, scope or channel membership.
var fatalCodes = map[string]bool{
	"invalid_auth": true, "not_authed": true, "token_revoked": true, "token_expired": true,
	"account_inactive": true, "missing_scope": true, "channel_not_found": true,
	"not_in_channel": true, "is_archived": true,
}

// Run mirrors one room until it closes, ctx ends or Slack reports a fatal
// error. Call it only for open rooms: for a room whose close was already
// mirrored it waits for new events until ctx ends.
func (b *Bridge) Run(ctx context.Context, roomID string) error {
	b.mu.Lock()
	if b.running[roomID] {
		b.mu.Unlock()
		return ErrAlreadyRunning
	}
	b.running[roomID] = true
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.running, roomID)
		b.mu.Unlock()
	}()

	ctx, cancel := context.WithCancel(ctx) // ends the subscription when we return
	defer cancel()

	th, _, err := b.threads.Get(ctx, roomID)
	if err != nil {
		return fmt.Errorf("slackbridge: load thread for room %s: %w", roomID, err)
	}
	ch, err := b.store.Subscribe(ctx, roomID, th.LastSeq)
	if err != nil {
		return fmt.Errorf("slackbridge: subscribe to room %s: %w", roomID, err)
	}
	r := newRenderer(b.publicURL)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e, ok := <-ch:
			if !ok {
				if err := ctx.Err(); err != nil {
					return err
				}
				return fmt.Errorf("slackbridge: subscription for room %s closed unexpectedly", roomID)
			}
			if !Important(e.Type) || e.Seq <= th.LastSeq {
				continue
			}
			if e.Type == events.RoomCreated && th.TS != "" {
				continue
			}
			next, err := b.mirror(ctx, r, e, th)
			if err != nil {
				return fmt.Errorf("slackbridge: room %s: %w", roomID, err)
			}
			th = next
			if e.Type == events.RoomClosed {
				if b.pager != nil {
					b.pager.forgetRoom(roomID) // a closed room pages no more
				}
				return nil
			}
		}
	}
}

// mirror posts one event and saves progress. It returns an error only for
// fatal Slack failures; other failures are logged and the event is skipped.
func (b *Bridge) mirror(ctx context.Context, r *renderer, e events.Event, th Thread) (Thread, error) {
	if th.TS == "" && e.Type != events.RoomCreated {
		res, err := b.send(ctx, e, r.minimalParent(e.RoomID), b.channel, "")
		if err != nil {
			if isFatal(err) || ctx.Err() != nil {
				return th, cmp.Or(ctx.Err(), err)
			}
		} else {
			th.Channel, th.TS = res.Channel, res.TS
			// Save the parent now: if ctx ends before the reply below is
			// posted, a restart must reuse this parent, not post another.
			b.put(ctx, e, th)
		}
	}
	var (
		msg message
		ok  bool
		err error
	)
	if e.Type == events.RoomCreated {
		msg, err = r.parent(e)
		ok = err == nil
	} else {
		msg, ok, err = r.reply(e)
	}
	if err != nil {
		b.log.Warn("slack render failed", "room_id", e.RoomID, "seq", e.Seq, "type", string(e.Type), "err", err)
	}
	if ok && (e.Type == events.RoomCreated || th.TS != "") {
		channel, thread := b.channel, ""
		if e.Type != events.RoomCreated {
			// A thread stays where it started even if the config changed since.
			channel, thread = cmp.Or(th.Channel, b.channel), th.TS
		}
		res, err := b.send(ctx, e, msg, channel, thread)
		switch {
		case err == nil && e.Type == events.RoomCreated:
			th.Channel, th.TS = res.Channel, res.TS
		case err != nil && isFatal(err):
			return th, err
		case err != nil && ctx.Err() != nil:
			// Shutdown, not a Slack verdict: keep LastSeq so a restart re-posts it.
			return th, ctx.Err()
		}
		if err == nil && b.pager != nil {
			if err := b.page(ctx, r, e, th); err != nil {
				return th, err
			}
		}
	}
	th.LastSeq = e.Seq
	b.put(ctx, e, th)
	return th, nil
}

// page posts the allow-listed mentions for e as a second reply in the thread.
// Only fatal Slack errors and shutdown are returned; a failed page is logged
// (by send) and dropped, never retried.
func (b *Bridge) page(ctx context.Context, r *renderer, e events.Event, th Thread) error {
	m, keys, err := b.pager.message(r, e)
	if err != nil {
		b.log.Warn("slack page render failed", "room_id", e.RoomID, "seq", e.Seq, "type", string(e.Type), "err", err)
		return nil
	}
	if len(keys) == 0 {
		return nil
	}
	if _, err := b.send(ctx, e, m, cmp.Or(th.Channel, b.channel), th.TS); err != nil {
		if isFatal(err) {
			return err
		}
		// Not recorded, so the quota is untouched; the page itself is not retried.
		return ctx.Err()
	}
	b.pager.record(keys)
	return nil
}

// put saves thread state. A failure is logged, not returned: the Slack post
// already happened, and a restart then repeats at most this one message.
func (b *Bridge) put(ctx context.Context, e events.Event, th Thread) {
	if err := b.threads.Put(ctx, e.RoomID, th); err != nil {
		b.log.Warn("slack thread state not saved", "room_id", e.RoomID, "seq", e.Seq, "err", err)
	}
}

// send posts m to channel. Log lines hold only ids and the Slack code, never text or the token.
func (b *Bridge) send(ctx context.Context, e events.Event, m message, channel, threadTS string) (postResult, error) {
	m.Channel, m.ThreadTS = channel, threadTS
	res, err := b.client.post(ctx, m)
	if err != nil {
		code := errCode(err)
		if isFatal(err) {
			b.log.Error("slack mirror stopped", "room_id", e.RoomID, "seq", e.Seq, "type", string(e.Type), "code", code)
		} else {
			b.log.Warn("slack post failed", "room_id", e.RoomID, "seq", e.Seq, "type", string(e.Type), "code", code)
		}
		return res, err
	}
	if res.Channel == "" {
		res.Channel = channel
	}
	return res, nil
}

// errCode names a failure for logs without quoting any message text.
func errCode(err error) string {
	var ae *APIError
	var he *httpError
	switch {
	case errors.As(err, &ae):
		return ae.Code
	case errors.Is(err, ErrRateLimited):
		return "rate_limited"
	case errors.As(err, &he):
		return fmt.Sprintf("http_%d", he.Status)
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	return "transport"
}

func isFatal(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && fatalCodes[ae.Code]
}
