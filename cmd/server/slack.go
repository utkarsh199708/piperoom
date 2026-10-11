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

// mirrorStopTimeout bounds how long shutdown waits for Slack mirrors.
const mirrorStopTimeout = 5 * time.Second

// slackSettings holds the bridge settings read from the environment.
type slackSettings struct {
	Token     netsec.Secret
	ChannelID string
	PublicURL string
	apiBase   string // tests only: the fake Slack API
}

// slackEnv reads the Slack settings. These names are not in internal/config
// yet (contract change request in the PR). The token is a netsec.Secret from
// the moment it is read, so it never prints.
func slackEnv() slackSettings {
	env := func(k string) string { return strings.TrimSpace(os.Getenv(k)) }
	return slackSettings{
		Token:     netsec.Secret(env("SLACK_BOT_TOKEN")),
		ChannelID: env("SLACK_CHANNEL_ID"),
		PublicURL: env("PIPEROOM_PUBLIC_URL"),
	}
}

// mirrors runs one slackbridge.Run per open room and stops them all at shutdown.
type mirrors struct {
	bridge *slackbridge.Bridge
	log    *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex // guards closed vs wg.Add
	closed bool
	wg     sync.WaitGroup
}

// newMirrors builds the Slack bridge. It returns nil, nil when Slack is not
// configured; a half or wrong configuration is an error so it is never silent.
// ctx is the process lifetime; stop must be called before the store closes.
func newMirrors(ctx context.Context, st events.Store, threads slackbridge.Threads, s slackSettings, logger *slog.Logger) (*mirrors, error) {
	if s.Token == "" && s.ChannelID == "" {
		logger.Info("slack bridge disabled")
		return nil, nil
	}
	b, err := slackbridge.New(st, slackbridge.Options{
		Token: s.Token, ChannelID: s.ChannelID, PublicURL: s.PublicURL,
		APIBaseURL: s.apiBase, Threads: threads, Logger: logger,
	})
	if errors.Is(err, slackbridge.ErrDisabled) {
		logger.Info("slack bridge disabled")
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("server: slack bridge: %w", err)
	}
	mctx, cancel := context.WithCancel(ctx)
	logger.Info("slack bridge enabled", "channel", s.ChannelID)
	return &mirrors{bridge: b, log: logger, ctx: mctx, cancel: cancel}, nil
}

// onRoomOpened is the room.Config hook: it only starts a goroutine, so it never blocks.
func (m *mirrors) onRoomOpened(_ context.Context, roomID string) { m.start(roomID) }

// startOpen mirrors every room that is already open, e.g. after a restart.
func (m *mirrors) startOpen(ctx context.Context, svc *room.Service) error {
	rooms, err := svc.ListRooms(ctx)
	if err != nil {
		return fmt.Errorf("server: list rooms for slack: %w", err)
	}
	for _, r := range rooms {
		if r.Status == room.StatusOpen {
			m.start(r.ID)
		}
	}
	return nil
}

// start runs the bridge for one room until it closes or shutdown.
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
			// Bridge errors carry only ids and Slack error codes, never the token or message text.
			m.log.Warn("slack mirror stopped", "room_id", roomID, "err", err)
		}
	}()
}

// stop cancels every mirror and waits up to timeout for them to return.
func (m *mirrors) stop(timeout time.Duration) {
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
