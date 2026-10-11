package slackbridge

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"piperoom/internal/events"
)

const (
	pageLimit      = 500
	maxInboundKeys = 1000
)

// Bridge mirrors room events into Slack threads.
type Bridge struct {
	store   events.Store
	poster  Poster
	threads ThreadStore
	channel string
	roomURL string // base URL of the web UI, "" to omit links
	log     *slog.Logger

	mu      sync.Mutex
	inbound map[string]int // comments that came from Slack; not echoed back
}

// New builds a Bridge posting into channel.
func New(store events.Store, poster Poster, threads ThreadStore, channel, roomURL string, log *slog.Logger) *Bridge {
	return &Bridge{store: store, poster: poster, threads: threads, channel: channel,
		roomURL: roomURL, log: log, inbound: map[string]int{}}
}

func inboundKey(roomID, actorID, text string) string {
	return roomID + "\x00" + actorID + "\x00" + text
}

// markInbound records a comment about to be written from Slack so the mirror
// skips it; otherwise the reply would be posted into its own thread again.
// It returns a func that undoes the mark if the comment write fails.
func (b *Bridge) markInbound(roomID, actorID, text string) (undo func()) {
	k := inboundKey(roomID, actorID, text)
	b.mu.Lock()
	if len(b.inbound) >= maxInboundKeys {
		b.inbound = map[string]int{}
	}
	b.inbound[k]++
	b.mu.Unlock()
	return func() { b.consumeInbound(k) }
}

func (b *Bridge) consumeInbound(k string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inbound[k] == 0 {
		return false
	}
	if b.inbound[k]--; b.inbound[k] == 0 {
		delete(b.inbound, k)
	}
	return true
}

// Run mirrors one room until ctx ends or the room closes. Mirroring is best
// effort: a failed Slack post is logged and skipped so Slack trouble never
// blocks the room.
func (b *Bridge) Run(ctx context.Context, roomID string) error {
	th, _ := b.threads.Get(roomID)
	ch, err := b.store.Subscribe(ctx, roomID, th.LastSeq)
	if err != nil {
		return fmt.Errorf("slackbridge: subscribe room %s: %w", roomID, err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e, ok := <-ch:
			if !ok {
				return ctx.Err()
			}
			th = b.handle(ctx, e, th)
			b.threads.Put(roomID, th)
			if e.Type == events.RoomClosed {
				return nil
			}
		}
	}
}

func (b *Bridge) handle(ctx context.Context, e events.Event, th Thread) Thread {
	th.LastSeq = e.Seq
	if e.Type == events.CommentAdded && b.consumeInbound(inboundKey(e.RoomID, e.Actor.ID, commentText(e))) {
		return th
	}
	text, ok := Format(e, b.roomURL)
	if !ok {
		return th
	}
	// Replies before a root exists (room.created missed) become the root.
	ts, err := b.poster.Post(ctx, b.channel, th.TS, text)
	if err != nil {
		b.log.Warn("slack post failed", "room", e.RoomID, "seq", e.Seq, "type", string(e.Type), "err", err)
		return th
	}
	if th.TS == "" {
		th.TS = ts
	}
	return th
}

func commentText(e events.Event) string {
	p, err := events.Decode[events.CommentAddedPayload](e)
	if err != nil {
		return ""
	}
	return p.Text
}
