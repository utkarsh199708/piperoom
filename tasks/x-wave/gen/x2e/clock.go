//go:build xwavegen

package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"piperoom/internal/watch"
)

// evalClock is a watch.Clock that moves only when the harness advances it.
type evalClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*evalTimer
}

type evalTimer struct {
	c     *evalClock
	ch    chan time.Time
	at    time.Time
	armed bool
}

func newEvalClock(t time.Time) *evalClock { return &evalClock{now: t} }

func (c *evalClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *evalClock) NewTimer(d time.Duration) watch.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &evalTimer{c: c, ch: make(chan time.Time, 1), at: c.now.Add(d), armed: true}
	c.timers = append(c.timers, t)
	c.fireLocked()
	return t
}

func (c *evalClock) fireLocked() {
	for _, t := range c.timers {
		if t.armed && !t.at.After(c.now) {
			t.armed = false
			select {
			case t.ch <- c.now:
			default:
			}
		}
	}
}

func (c *evalClock) advanceTo(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if at.After(c.now) {
		c.now = at
	}
	c.fireLocked()
}

// armedAt is the deadline of an armed timer that has not fired.
func (c *evalClock) armedAt() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.timers {
		if t.armed {
			return t.at, true
		}
	}
	return time.Time{}, false
}

// waitArmed waits until a timer is armed (the tick is over) and returns its
// deadline; a zero time when the room opened or the watcher stopped.
func (c *evalClock) waitArmed(ctx context.Context, opened <-chan struct{}, done <-chan error) (time.Time, error) {
	for {
		select {
		case <-opened:
			return time.Time{}, nil
		case err := <-done:
			if err != nil {
				return time.Time{}, fmt.Errorf("evals: watcher: %w", err)
			}
			return time.Time{}, errors.New("evals: the watcher stopped before it opened a room")
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		default:
		}
		if at, ok := c.armedAt(); ok {
			return at, nil
		}
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (t *evalTimer) C() <-chan time.Time { return t.ch }

func (t *evalTimer) Reset(d time.Duration) {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	t.at, t.armed = t.c.now.Add(d), true
	t.c.fireLocked()
}

func (t *evalTimer) Stop() {
	t.c.mu.Lock()
	t.armed = false
	t.c.mu.Unlock()
}
