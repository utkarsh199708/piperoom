package main

import (
	"bufio"
	"context"
	"os"
	"sync"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

// stdinInbox turns stdin lines into driver steers. A background goroutine
// reads stdin so Drain never blocks and Wait blocks until a line arrives.
// When stdin reaches EOF the inbox reports agent.ErrInboxClosed from Wait once
// every pending line has been drained, so the run finishes its work and stops
// cleanly instead of being cancelled mid-turn.
type stdinInbox struct {
	mu     sync.Mutex
	lines  []agent.HumanInput
	notify chan struct{}
	eof    bool
}

func newStdinInbox() *stdinInbox {
	in := &stdinInbox{notify: make(chan struct{}, 1)}
	go in.read()
	return in
}

func (in *stdinInbox) read() {
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		in.mu.Lock()
		in.lines = append(in.lines, agent.HumanInput{
			Actor: events.Actor{Kind: events.ActorHuman, ID: "driver", Name: "driver"},
			Kind:  "steer",
			Text:  line,
		})
		in.mu.Unlock()
		in.signal()
	}
	// stdin closed: no more input will arrive. Wake any waiter so it can see the
	// EOF state and stop.
	in.mu.Lock()
	in.eof = true
	in.mu.Unlock()
	in.signal()
}

func (in *stdinInbox) signal() {
	select {
	case in.notify <- struct{}{}:
	default:
	}
}

// Drain returns every pending steer without blocking and consumes any stale
// notify token, so a token left over from an earlier signal cannot make a later
// Wait return with nothing to drain.
func (in *stdinInbox) Drain(_ context.Context) ([]agent.HumanInput, error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if len(in.lines) == 0 {
		return nil, nil
	}
	out := in.lines
	in.lines = nil
	select {
	case <-in.notify:
	default:
	}
	return out, nil
}

// Wait blocks until a steer is pending or ctx ends. Once stdin has reached EOF
// and every pending line is drained it returns agent.ErrInboxClosed.
func (in *stdinInbox) Wait(ctx context.Context) error {
	for {
		in.mu.Lock()
		pending := len(in.lines) > 0
		eof := in.eof
		in.mu.Unlock()
		if pending {
			return nil
		}
		if eof {
			return agent.ErrInboxClosed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-in.notify:
			// Re-check lines and EOF; a stale token wakes us without input.
		}
	}
}
