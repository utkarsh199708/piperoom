// Package pump is the per-subscriber delivery loop shared by the Postgres and
// in-memory stores. Keeping it in one place is what makes the two stores behave
// identically: a subscriber is "read everything after my cursor, whenever
// something might have changed", so a merged, lost or early wake-up can never
// skip or repeat an event.
package pump

import (
	"context"
	"log/slog"
	"time"

	"piperoom/internal/events"
)

// Defaults for the knobs in Config.
const (
	// DefaultBuffer is the subscriber channel capacity.
	DefaultBuffer = 256
	// DefaultSlowAfter is how long a full channel may block delivery before the
	// subscriber is dropped. The client resubscribes with its last seen seq.
	DefaultSlowAfter = 5 * time.Second
	// DefaultPage is how many events one read asks for.
	DefaultPage = 500
)

// ReadFunc returns events with Seq > after, in order, at most limit. It must
// return a non-nil error only for a failed read, never for "nothing new".
type ReadFunc func(ctx context.Context, after int64, limit int) ([]events.Event, error)

// Config tunes one subscriber.
type Config struct {
	// Read fetches the next page after the subscriber's cursor. Required.
	Read ReadFunc
	// Wake signals "something may have been appended". Sends must be
	// non-blocking on a channel with capacity >= 1 so signals coalesce.
	Wake <-chan struct{}
	// Poll, when > 0, re-reads this often even without a wake-up, covering a
	// lost notification.
	Poll time.Duration
	// Buffer is the output channel capacity (DefaultBuffer when <= 0).
	Buffer int
	// SlowAfter is how long a full output channel may stay blocked
	// (DefaultSlowAfter when <= 0).
	SlowAfter time.Duration
	// Page is the read size (DefaultPage when <= 0).
	Page int
	// Log receives read failures and slow-consumer drops. Required.
	Log *slog.Logger
	// OnExit, when set, runs once when the goroutine ends, before the output
	// channel is closed, so a consumer that sees the close can rely on the
	// subscriber already being unregistered.
	OnExit func()
}

// Start launches the delivery goroutine and returns its output channel.
//
// first holds the events the caller already read after `after` (pass nil if it
// has not read yet; a non-nil empty slice means "read, nothing there"). The
// goroutine sends them, then keeps reading after its cursor each time Wake
// fires or Poll ticks. It closes the channel, and exits, when ctx ends or when
// the consumer is too slow.
func Start(ctx context.Context, cfg Config, after int64, first []events.Event) <-chan events.Event {
	if cfg.Buffer <= 0 {
		cfg.Buffer = DefaultBuffer
	}
	if cfg.SlowAfter <= 0 {
		cfg.SlowAfter = DefaultSlowAfter
	}
	if cfg.Page <= 0 {
		cfg.Page = DefaultPage
	}
	out := make(chan events.Event, cfg.Buffer)
	go run(ctx, cfg, after, first, out)
	return out
}

type sendResult int

const (
	sent sendResult = iota
	canceled
	tooSlow
)

func run(ctx context.Context, cfg Config, cursor int64, first []events.Event, out chan events.Event) {
	// Deferred in this order so OnExit runs before the close (LIFO).
	defer close(out)
	if cfg.OnExit != nil {
		defer cfg.OnExit()
	}

	var tick <-chan time.Time
	if cfg.Poll > 0 {
		t := time.NewTicker(cfg.Poll)
		defer t.Stop()
		tick = t.C
	}

	pending, havePending := first, first != nil
	failing := false
	for {
		// Drain every page that is available now; only then go back to waiting.
		for {
			if !havePending {
				batch, err := cfg.Read(ctx, cursor, cfg.Page)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					// Retried on the next wake-up or poll. Log only the first
					// failure of a streak so an outage is not a log flood.
					if !failing {
						cfg.Log.Warn("store: subscriber read failed, will retry", "after_seq", cursor, "error", err)
						failing = true
					}
					break
				}
				if failing {
					cfg.Log.Info("store: subscriber read recovered", "after_seq", cursor)
					failing = false
				}
				pending = batch
			}
			havePending = false

			for _, ev := range pending {
				// Defensive: a reader must never hand us anything at or below the
				// cursor, but if it does, dropping it keeps "exactly once".
				if ev.Seq <= cursor {
					continue
				}
				if ctx.Err() != nil {
					return
				}
				switch send(ctx, out, ev, cfg.SlowAfter) {
				case canceled:
					return
				case tooSlow:
					cfg.Log.Warn("store: dropping slow subscriber, it must resubscribe", "room_id", ev.RoomID, "last_sent_seq", cursor)
					return
				}
				cursor = ev.Seq
			}
			full := len(pending) >= cfg.Page
			pending = nil
			if !full {
				break
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-cfg.Wake:
		case <-tick:
		}
	}
}

// send delivers one event. The fast path never allocates a timer; the timer
// only exists while the consumer is stalled.
func send(ctx context.Context, out chan<- events.Event, ev events.Event, slowAfter time.Duration) sendResult {
	select {
	case out <- ev:
		return sent
	default:
	}
	timer := time.NewTimer(slowAfter)
	defer timer.Stop()
	select {
	case out <- ev:
		return sent
	case <-ctx.Done():
		return canceled
	case <-timer.C:
		return tooSlow
	}
}
