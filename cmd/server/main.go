// Command server runs the Piperoom room service. With --demo it uses a
// scripted fake agent (and fake topology/actions) so the UI can be built
// against it with no stack; without --demo it opens the real stack clients
// (source Postgres, warehouse, Connect, Schema Registry, Kafka) via openStack,
// registers the real tools/actions and runs the real agent (agent.Run).
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/room/demo"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

func main() {
	demoMode := flag.Bool("demo", false, "run a scripted fake agent (no real tools/agent)")
	flag.Parse()

	cfg := config.Load()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	users, err := room.LoadUsers(cfg.UsersFile)
	if err != nil {
		log.Warn("server: users file unavailable, using demo users", "path", cfg.UsersFile, "err", err)
		users = room.DefaultUsers()
	}

	// --demo is fully self-contained (in-memory store + scripted agent) so A6
	// can build the UI against it with no database or migrations. The real
	// mode uses Postgres and requires the migrations applied via goose.
	var st store.Store
	var closeStore func()
	if *demoMode {
		st = memstore.New()
	} else {
		pg, err := store.New(ctx, cfg.RoomDBURL, log)
		if err != nil {
			log.Error("server: cannot connect to room store", "err", err)
			os.Exit(1)
		}
		st = pg
		closeStore = pg.Close
		defer closeStore()
	}

	var (
		acts      *actions.Registry
		toolsReg  = tools.NewRegistry()
		topo      topology.Builder
		ownersCfg *owners.Owners
		run       = agent.Run
		write     = agent.WriteHandoff
	)
	if *demoMode {
		acts = demo.Actions()
		topo = demo.Builder{}
		run = demo.Run
		write = demo.WriteHandoff
	} else {
		stack, err := openStack(ctx, cfg)
		if err != nil {
			log.Error("server: cannot open the data stack", "err", err)
			os.Exit(1)
		}
		defer stack.Close()
		acts = stack.Actions
		toolsReg = stack.Tools
		topo = stack.Builder
		ownersCfg = stack.Owners
	}

	svc := room.New(room.Deps{
		Store:        st,
		Actions:      acts,
		Tools:        toolsReg,
		Topology:     topo,
		Users:        users,
		Owners:       ownersCfg,
		Run:          run,
		WriteHandoff: write,
		Options: agent.Options{
			Model:    cfg.Model,
			Effort:   cfg.Effort,
			MaxTurns: 40,
		},
		Logger: log,
	})

	loadCtx, cancelLoad := context.WithTimeout(ctx, 30*time.Second)
	if err := svc.Load(loadCtx); err != nil {
		log.Warn("server: cache load failed", "err", err)
	}
	cancelLoad()

	handler := httpapi.New(svc, st, log)
	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Shutdown waits for non-hijacked requests; WS connections are
		// hijacked, so close them (and the room service) explicitly.
		_ = server.Shutdown(shutdownCtx)
		handler.Close()
	}()

	mode := "real"
	if *demoMode {
		mode = "demo"
	}
	log.Info("server: listening", "addr", cfg.HTTPAddr, "mode", mode)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server: failed", "err", err)
		os.Exit(1)
	}
}
