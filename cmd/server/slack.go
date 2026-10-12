package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/slackbridge"
)

// mirrorStopTimeout bounds how long shutdown waits for the Slack mirrors.
const mirrorStopTimeout = 5 * time.Second

// slackSettings are the bridge settings read from the environment.
type slackSettings struct {
	Token     netsec.Secret
	ChannelID string
	PublicURL string
	apiBase   string // tests only: the fake Slack API
}

// slackEnv reads the Slack settings. They are not in internal/config yet
// (contract change request in the PR). The token is a netsec.Secret from the
// moment it is read, so it prints as [redacted] anywhere it ends up.
func slackEnv() slackSettings {
	env := func(k string) string { return strings.TrimSpace(os.Getenv(k)) }
	return slackSettings{
		Token:     netsec.Secret(env("SLACK_BOT_TOKEN")),
		ChannelID: env("SLACK_CHANNEL_ID"),
		PublicURL: env("PIPEROOM_PUBLIC_URL"),
	}
}

// mirrors runs one slackbridge.Run per open room and stops them all at
// shutdown. A nil *mirrors is a disabled bridge: every method is a no-op.
type mirrors struct {
	bridge *slackbridge.Bridge
	log    *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex // orders closed against wg.Add
	closed bool
	wg     sync.WaitGroup
}

// newMirrors builds the Slack bridge. With neither token nor channel set it
// returns nil, nil (bridge off, server unchanged); a half or wrong
// configuration is an error so it never fails silently. ctx bounds every
// mirror; stop must run before the store closes.
func newMirrors(ctx context.Context, st events.Store, threads slackbridge.Threads, s slackSettings, log *slog.Logger) (*mirrors, error) {
	if s.Token == "" && s.ChannelID == "" {
		log.Info("slack bridge disabled")
		return nil, nil
	}
	b, err := slackbridge.New(st, slackbridge.Options{
		Token: s.Token, ChannelID: s.ChannelID, PublicURL: s.PublicURL,
		APIBaseURL: s.apiBase, Threads: threads, Logger: log,
	})
	if errors.Is(err, slackbridge.ErrDisabled) {
		log.Info("slack bridge disabled")
		return nil, nil
	}
	if err != nil {
		// slackbridge.New errors name the bad setting, never its value.
		return nil, fmt.Errorf("server: slack bridge: %w", err)
	}
	mctx, cancel := context.WithCancel(ctx)
	log.Info("slack bridge enabled", "channel", s.ChannelID)
	return &mirrors{bridge: b, log: log, ctx: mctx, cancel: cancel}, nil
}

// onRoomOpened is the room.Deps.OnRoomOpened hook, or nil when the bridge is
// off. It only starts a goroutine, so it never blocks the room service.
func (m *mirrors) onRoomOpened() func(context.Context, string) {
	if m == nil {
		return nil
	}
	return func(_ context.Context, roomID string) { m.start(roomID) }
}

// startOpen mirrors every room that is already open, e.g. after a restart.
func (m *mirrors) startOpen(ctx context.Context, svc *room.Service) error {
	if m == nil {
		return nil
	}
	rooms, err := svc.ListRooms(ctx)
	if err != nil {
		return fmt.Errorf("server: list rooms for slack: %w", err)
	}
	for _, r := range rooms {
		if r.Status == "open" {
			m.start(r.ID)
		}
	}
	return nil
}

// start mirrors one room until it closes or shutdown begins.
func (m *mirrors) start(roomID string) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.wg.Add(1)
	m.mu.Unlock()
	go func() {
		defer m.wg.Done()
		err := m.bridge.Run(m.ctx, roomID)
		switch {
		case err == nil:
		case errors.Is(err, slackbridge.ErrAlreadyRunning):
			m.log.Debug("slack mirror already running", "room_id", roomID)
		case m.ctx.Err() != nil:
			m.log.Debug("slack mirror stopped for shutdown", "room_id", roomID)
		default:
			// The bridge already logged the Slack code; the error text could
			// carry store details, so only the room id is logged here.
			m.log.Warn("slack mirror stopped", "room_id", roomID)
		}
	}()
}

// stop cancels every mirror and waits up to timeout for them to return.
func (m *mirrors) stop(timeout time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	m.cancel()
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		m.log.Warn("slack mirrors did not stop in time", "timeout", timeout)
	}
}
