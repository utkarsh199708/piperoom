// Command server runs the Piperoom room service: HTTP API, WebSocket, the
// agent supervisor and the approved-fix executor.
//
//	go run ./cmd/server --demo            scripted agent, in-memory log, no stack needed
//	go run ./cmd/server                   real agent and stack from the environment (see docs/pilot/setup.md)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
	"piperoom/internal/wiring"
	"piperoom/internal/ws"
)

func main() {
	var (
		demo      = flag.Bool("demo", false, "scripted agent, in-memory log and built-in users; no stack, database or API key needed")
		demoDelay = flag.Duration("demo-delay", 1500*time.Millisecond, "pause between the demo agent's visible steps")
		addr      = flag.String("addr", "", "listen address (default PIPEROOM_HTTP_ADDR, loopback)")
		insecure  = flag.Bool("insecure-listen", false, "allow a non-loopback listen address although there is no real login yet (security F1)")
	)
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(*demo, *demoDelay, *addr, *insecure, logger); err != nil {
		logger.Error("server failed", "err", err)
		os.Exit(1)
	}
}

func run(demo bool, demoDelay time.Duration, addrFlag string, insecure bool, logger *slog.Logger) error {
	cfg := config.Load()
	addr := cfg.HTTPAddr
	if addrFlag != "" {
		addr = addrFlag
	}
	if err := checkListenAddr(addr, insecure); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	own, err := owners.Load(cfg.OwnersFile)
	if err != nil {
		return err
	}
	rc := room.Config{Owners: own, Logger: logger, AgentOptions: agent.Options{Model: cfg.Model, Effort: cfg.Effort}}
	var st store.Store
	var verifier alertin.Config
	// ready stays nil in demo mode: the in-memory store is always ready.
	var ready httpapi.Pinger

	if demo {
		logger.Info("demo mode: scripted agent, in-memory log, built-in users")
		st = memstore.New()
		rc.Users = room.DemoUsers()
		areg, err := demoActionRegistry()
		if err != nil {
			return err
		}
		rc.Actions = areg
		rc.Topology = demoTopology{own}
		rc.Runner = demoRunner(demoDelay)
		rc.Handoff = demoHandoff
		rc.ExecTimeout = 10 * time.Second
	} else {
		pg, err := store.NewPG(ctx, cfg.RoomDBURL, logger)
		if err != nil {
			return err
		}
		defer pg.Close()
		st, ready = pg, pg
		if rc.Users, err = room.LoadUsers(cfg.UsersFile); err != nil {
			return err
		}
		w, err := wiring.Open(ctx, cfg)
		if err != nil {
			return err
		}
		defer w.Close()
		rc.Tools, rc.Actions = tools.NewRegistry(), actions.NewRegistry()
		if err := wiring.RegisterTools(rc.Tools, rc.Actions, w); err != nil {
			return err
		}
		var b topology.Builder = w.Builder
		rc.Topology = b
		rc.Runner = agent.Run
		rc.Handoff = agent.WriteHandoff
	}
	rc.Store = st
	// Webhook sources stay disabled (401) until their secret is set in the environment.
	verifier = webhookConfig(own)

	svc, err := room.New(rc)
	if err != nil {
		return err
	}
	defer svc.Shutdown()
	if err := svc.Start(ctx); err != nil {
		return err
	}

	wsh := ws.New(ws.Config{Subscribe: st.Subscribe, Authenticate: svc.Authenticate, RoomExists: svc.RoomExists, Logger: logger})
	api := httpapi.New(httpapi.Config{
		Service: svc, Verifier: verifier, WS: wsh, Logger: logger,
		Ready: ready, OnNotReady: newReadyLogger(logger, readyLogEvery, time.Now).notReady,
	})
	srv := &http.Server{
		Addr: addr, Handler: api,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		// No WriteTimeout: hand-offs may run for minutes and WebSockets are
		// long-lived. The handlers carry their own deadlines.
		IdleTimeout: 2 * time.Minute,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("listening", "addr", addr, "demo", demo)

	select {
	case err := <-errc:
		return fmt.Errorf("server: listen: %w", err)
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// http.Server.Shutdown does not close hijacked WebSocket connections.
	wsErr := wsh.Shutdown(sctx)
	if err := srv.Shutdown(sctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server: shutdown: %w", err)
	}
	if wsErr != nil {
		return fmt.Errorf("server: close websockets: %w", wsErr)
	}
	return nil
}

// checkListenAddr refuses a non-loopback bind unless the operator opted in:
// the API trusts the X-User header (security F1), so reaching it means being
// able to approve fixes as any owner.
func checkListenAddr(addr string, insecure bool) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("server: bad listen address %q: %w", addr, err)
	}
	if ip := net.ParseIP(host); host == "localhost" || (ip != nil && ip.IsLoopback()) {
		return nil
	}
	if insecure {
		slog.Warn("listening on a non-loopback address without real login: anyone who can reach it can approve fixes as any owner", "addr", addr)
		return nil
	}
	return fmt.Errorf("server: %q is not a loopback address; there is no real login yet (security F1). Use a tunnel or an authenticating proxy, or pass --insecure-listen", addr)
}

// webhookConfig reads the alert-webhook secrets from the environment. A source
// with no secret stays disabled. These names are not in internal/config yet
// (contract change request in the PR).
func webhookConfig(o *owners.Owners) alertin.Config {
	env := func(k string) string { return strings.TrimSpace(os.Getenv(k)) }
	return alertin.Config{
		AlertmanagerToken: env("PIPEROOM_ALERTMANAGER_TOKEN"),
		GrafanaSecret:     env("PIPEROOM_GRAFANA_SECRET"),
		PagerDutySecret:   env("PIPEROOM_PAGERDUTY_SECRET"),
		DatadogToken:      env("PIPEROOM_DATADOG_TOKEN"),
		GenericSecret:     env("PIPEROOM_GENERIC_SECRET"),
		Owners:            o,
		DefaultPipeline:   env("PIPEROOM_DEFAULT_PIPELINE"),
	}
}
