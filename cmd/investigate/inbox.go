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
// When stdin reaches EOF, onEOF is called once so the caller can end the run.
type stdinInbox struct {
	mu     sync.Mutex
	lines  []agent.HumanInput
	notify chan struct{}
	onEOF  func()
	once   sync.Once
}

func newStdinInbox(onEOF func()) *stdinInbox {
	in := &stdinInbox{notify: make(chan struct{}, 1), onEOF: onEOF}
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
	// stdin closed: stop the run cleanly.
	in.once.Do(func() {
		if in.onEOF != nil {
			in.onEOF()
		}
	})
}

func (in *stdinInbox) signal() {
	select {
	case in.notify <- struct{}{}:
	default:
	}
}

// Drain returns every pending steer without blocking.
func (in *stdinInbox) Drain(_ context.Context) ([]agent.HumanInput, error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if len(in.lines) == 0 {
		return nil, nil
	}
	out := in.lines
	in.lines = nil
	return out, nil
}

// Wait blocks until a steer is pending or ctx ends.
func (in *stdinInbox) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-in.notify:
		return nil
	}
}
