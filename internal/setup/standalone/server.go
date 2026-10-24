package standalone

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/agent/llm"
	"piperoom/internal/auth"
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/quickstart"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// shutdownTimeout bounds the graceful stop of one server lifetime on a
// restart or a signal: WebSocket clients have a moment to see the close, and
// the process is never stuck on one that does not read.
const shutdownTimeout = 10 * time.Second

// loadTimeout bounds the room cache load at start. The in-memory store starts
// empty, so this only guards a hung store.
const loadTimeout = 30 * time.Second

// Server serves one lifetime of the standalone server: the in-memory room
// store, the room service, the HTTP and WebSocket API with dev login on
// loopback and the setup routes mounted, and the URL printed to Stdout as
// soon as the listener is bound, before the serve loop blocks.
type Server struct {
	// Log is the logger; nil is slog.Default.
	Log *slog.Logger
	// Events receives every room event (the OnRoomOpened subscription). Nil
	// logs none.
	Events *EventLog
	// Agent replaces the room service's agent runner; nil is the picker's
	// endpoint (modelHost, model.go).
	Agent agent.Runner
}

// Serve is the quickstart.ServeFunc of a standalone start: it builds the
// server on the settings quickstart resolved (owners, users, the forced
// config) and serves until ctx ends.
func (s *Server) Serve(parent context.Context, qs quickstart.Settings) error {
	// A Serve failure cancels local, so the shutdown goroutine below ends with
	// this lifetime and not with the process's context.
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	log := s.Log
	if log == nil {
		log = slog.Default()
	}
	// quickstart forced dev login and loopback, so ParseSettings only reads
	// the session lengths here; a bad PIPEROOM_SESSION_* still refuses.
	authSettings, err := auth.ParseSettings(qs.Config)
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	setupCfg, err := config.ParseSetup(qs.Config)
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	// The picker's price table and endpoint (V7, KAN-525). The stored key from
	// .piperoom/model.env reaches the adapters through the key overlay
	// (quickstart.Keys), never the process environment; a model that does not
	// build is the no model state, not a refused start.
	prices, err := agent.LoadModelPrices(qs.Config.ModelPrices)
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	st := memstore.New()
	// The hand-off's agent.usage events go to the room service once it exists
	// (usage.set below), so the room log and the ledger agree on the tokens.
	usage := &usageRelay{log: log}
	host := &modelHost{
		startKeys: quickstart.NewKeys(nil, qs.ModelEnv),
		ledger:    standaloneSpendLedger{st: st},
		usage:     usage,
	}
	host.wire(ctx, log, qs.Config, prices)
	run := s.Agent
	var write agent.HandoffWriter
	if run == nil {
		// No injected runner: rooms and hand-offs use the picker's endpoint,
		// so the stored key is the one their model calls read.
		run = host.Run
		write = host.WriteHandoff
	}
	sessions := auth.NewMemStore()
	deps := room.Deps{
		Store:        st,
		Users:        qs.Users,
		Owners:       qs.Owners,
		Run:          run,
		WriteHandoff: write,
		Options:      agent.Options{Model: qs.Config.Model, Effort: qs.Config.Effort, MaxTurns: 40},
		Sessions:     sessions,
		SessionLive:  authSettings.Liveness(),
		// Write mode is on (quickstart refused "off"); nothing here may record
		// an approved fix without running it.
		WriteModeOff: false,
		Setup:        setupCfg,
		// The listener is loopback (quickstart.CheckAddr and CheckListener), so
		// a loopback setup target is this machine: the denylist allows it.
		SetupLocal: true,
		SoloUser:   qs.Config.SoloUser,
		Logger:     log,
		// A closed room's token count is not kept on the endpoint.
		OnRoomClosed: func(_ context.Context, roomID, _, _ string) { host.Forget(roomID) },
	}
	if s.Events != nil {
		deps.OnRoomOpened = s.Events.Hook(st, log)
	}
	svc := room.New(deps)
	// Set before the listener opens: no hand-off can run before it.
	usage.set(svc)
	loadCtx, cancelLoad := context.WithTimeout(ctx, loadTimeout)
	loadErr := svc.Load(loadCtx)
	cancelLoad()
	if loadErr != nil {
		// The store is empty and in memory, so a failed load is a wiring bug,
		// not a slow database: refuse the start instead of serving rooms with
		// no cache behind them.
		svc.Close()
		return fmt.Errorf("setup: load the room cache: %w", loadErr)
	}
	opts := []httpapi.Option{
		httpapi.WithAuth(authSettings),
		httpapi.WithSessions(sessions),
		httpapi.WithLoginStates(auth.NewMemLoginStates()),
		httpapi.WithIdentities(auth.NewMemIdentities()),
		httpapi.WithSetup(qs.SetupEnabled),
	}
	// The V7 models API: GET always, PUT on loopback for the solo user. The
	// picker's Build rebuilds an endpoint from the same key overlay this
	// server's start endpoint used, so a PUT's key never enters the
	// environment either.
	supported, err := llm.SupportedModels()
	if err != nil {
		svc.Close()
		return fmt.Errorf("setup: %w", err)
	}
	soloUser := qs.Config.SoloUser
	if soloUser == "" && qs.Users != nil {
		// quickstart.Settings names the solo user only when --solo applied;
		// a one-user directory is the same person (cmd/server's rule).
		if all := qs.Users.All(); len(all) == 1 {
			soloUser = all[0].ID
		}
	}
	opts = append(opts, httpapi.WithModels(quickstart.NewModels(quickstart.ModelsOptions{
		Mode:       qs.ModelMode(),
		ProjectDir: qs.ProjectDir,
		Config:     qs.Config,
		Stored:     qs.ModelEnv,
		Ready:      host.ready(),
		SoloUser:   soloUser,
		Build:      host.build,
		Panel:      httpapi.PanelInput{Prices: prices, Supported: supported},
	})))
	if qs.Config.WebDir != "" {
		// Fail at start, not at the first page load, when the built UI is
		// missing from the path the operator configured (cmd/server's rule).
		if _, err := os.Stat(filepath.Join(qs.Config.WebDir, "index.html")); err != nil {
			svc.Close()
			return fmt.Errorf("setup: PIPEROOM_WEB_DIR %s has no index.html (run `npm run build` in web/): %w", qs.Config.WebDir, err)
		}
		opts = append(opts, httpapi.WithWebDir(qs.Config.WebDir))
	}
	handler := httpapi.New(svc, st, log, opts...)
	server := &http.Server{
		Addr:              qs.Config.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	ln, err := net.Listen("tcp", qs.Config.HTTPAddr)
	if err != nil {
		handler.Close()
		return fmt.Errorf("setup: listen on %s: %w", qs.Config.HTTPAddr, err)
	}
	// CheckAddr refused a non-loopback name before the write; this refuses a
	// "localhost" that resolved off loopback.
	if err := quickstart.CheckListener(ln.Addr()); err != nil {
		_ = ln.Close()
		handler.Close()
		return fmt.Errorf("setup: %w", err)
	}
	log.Info("setup: standalone server listening", "addr", ln.Addr().String(), "owners", qs.Config.OwnersFile,
		"write_mode", true, "setup", qs.SetupEnabled)
	// The URL goes out before Serve blocks, so `piperoom-setup --standalone`
	// prints it as soon as the port is open.
	fmt.Fprintln(qs.Stdout, quickstart.URL(ln.Addr()))

	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		stopCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		// Shutdown waits for ordinary requests; WebSocket connections are
		// hijacked, so the handler closes them (and the room service) too.
		_ = server.Shutdown(stopCtx)
		handler.Close()
	}()
	serveErr := server.Serve(ln)
	// A serve error ends the lifetime; cancel first so the shutdown goroutine
	// closes the handler and done follows.
	cancel()
	<-done
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("setup: serve on %s: %w", ln.Addr().String(), serveErr)
	}
	return nil
}
