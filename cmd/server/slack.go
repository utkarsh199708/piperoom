package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"strings"
	"sync"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/slackbridge"
)

// mirrorStopTimeout bounds how long shutdown waits for the Slack mirrors.
const mirrorStopTimeout = 5 * time.Second

// slackSettings are the bridge settings taken from config.
type slackSettings struct {
	Token     netsec.Secret
	ChannelID string
	PublicURL string
	apiBase   string // tests only: the fake Slack API
}

// slackFromConfig builds the bridge settings from config.Load(). config
// keeps the token a plain string; it becomes a netsec.Secret here, at wiring
// time, so it prints as [redacted] anywhere it ends up from now on.
func slackFromConfig(cfg config.Config) slackSettings {
	return slackSettings{
		Token:     netsec.Secret(strings.TrimSpace(cfg.SlackBotToken)),
		ChannelID: strings.TrimSpace(cfg.SlackChannelID),
		PublicURL: strings.TrimSpace(cfg.PublicURL),
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
	// lead is the context mirrors start under: ctx while this replica leads
	// "slack" (always, until elect is called), nil while another replica does.
	lead context.Context
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
	return &mirrors{bridge: b, log: log, ctx: mctx, cancel: cancel, lead: mctx}, nil
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

// start mirrors one room until it closes, shutdown begins, or this replica
// stops leading "slack". On a replica that does not lead it does nothing: the
// leader's rescan picks the room up.
func (m *mirrors) start(roomID string) {
	m.mu.Lock()
	if m.closed || m.lead == nil {
		m.mu.Unlock()
		return
	}
	ctx := m.lead
	m.wg.Add(1)
	m.mu.Unlock()
	go func() {
		defer m.wg.Done()
		// The leadership term must not outlive shutdown either.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		stopOnShutdown := context.AfterFunc(m.ctx, cancel)
		defer stopOnShutdown()
		err := m.bridge.Run(ctx, roomID)
		switch {
		case err == nil:
		case errors.Is(err, slackbridge.ErrAlreadyRunning):
			m.log.Debug("slack mirror already running", "room_id", roomID)
		case ctx.Err() != nil:
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

// teamPager is room.Deps.TeamPager (X4d): nil while the bridge is off, so
// page-team answers 409 slack_off. Every replica pages, not only the leader:
// the page is the driver's request, not part of the mirror.
func (m *mirrors) teamPager() room.TeamPager {
	if m == nil {
		return nil
	}
	return bridgePager{b: m.bridge}
}

// bridgePager turns a room.TeamPage into the bridge's typed page. Only the
// cited change's URL and service come out of its after JSON, and only for the
// external kinds that carry a clients.ExternalChange.
type bridgePager struct{ b *slackbridge.Bridge }

func (p bridgePager) PageTeam(ctx context.Context, pg room.TeamPage) error {
	tp := slackbridge.TeamPage{
		RoomID: pg.RoomID, PipelineID: pg.PipelineID, Channel: pg.Channel, Team: pg.Team, By: pg.By,
		Scope: pg.Scope, Hypothesis: pg.Hypothesis, Confidence: pg.Confidence, Steps: pg.Steps, ChangeID: pg.ChangeID,
	}
	if c := pg.Change; c != nil {
		tc := &slackbridge.TeamPageChange{Kind: c.Kind, Summary: c.Summary, At: c.At}
		var x clients.ExternalChange
		if clients.IsExternalChange(c.Kind) && json.Unmarshal(c.After, &x) == nil {
			tc.URL, tc.Service = x.URL, x.Service
		}
		tp.Change = tc
	}
	if err := p.b.PageTeam(ctx, tp); err != nil {
		return &room.TeamPageError{Code: slackbridge.ErrorCode(err), Err: err}
	}
	return nil
}
