// Command server runs the Piperoom room service. With --demo it uses a
// scripted fake agent (and fake topology/actions) so the UI can be built
// against it with no stack; without --demo it applies the migrations, opens
// the real stack clients via wiring.Open, registers the real tools/actions
// and runs the real agent (agent.Run).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

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
	"piperoom/internal/wiring"
	"piperoom/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server: failed", "err", err)
		os.Exit(1)
	}
}

// run is main's body so every cleanup defer runs on every exit path,
// including the listen-error path.
func run() error {
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
	// mode uses Postgres: migrations first (goose on migrations.FS), then the
	// store.
	var st store.Store
	if *demoMode {
		st = memstore.New()
	} else {
		if err := migrateRoomDB(ctx, cfg.RoomDBURL, log); err != nil {
			return err
		}
		pg, err := store.New(ctx, cfg.RoomDBURL, log)
		if err != nil {
			return fmt.Errorf("server: connect room store: %w", err)
		}
		st = pg
		defer pg.Close()
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
		w, err := wiring.Open(ctx, cfg)
		if err != nil {
			return fmt.Errorf("server: open the data stack: %w", err)
		}
		defer w.Close()
		acts = actions.NewRegistry()
		if err := wiring.RegisterTools(toolsReg, acts, w); err != nil {
			return fmt.Errorf("server: register tools and actions: %w", err)
		}
		topo = w.Builder
		ownersCfg = w.Owners
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

	// done closes only after Shutdown AND handler.Close (WS connections, the
	// room service and every goroutine it owns) have returned, so main can
	// wait for them before its defers close the store and the stack.
	done := make(chan struct{})
	go func() {
		defer close(done)
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
	serveErr := server.ListenAndServe()
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		// There was no signal; trigger the same shutdown path, then report.
		log.Error("server: listen failed", "err", serveErr)
		stop()
	}
	<-done
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("server: listen: %w", serveErr)
	}
	return nil
}

// migrateRoomDB applies the embedded goose migrations before the store opens,
// so a fresh database works with no manual `goose up`.
func migrateRoomDB(ctx context.Context, dbURL string, log *slog.Logger) error {
	cc, err := pgx.ParseConfig(dbURL)
	if err != nil {
		return fmt.Errorf("server: parse db url: %w", err)
	}
	db := stdlib.OpenDB(*cc)
	defer db.Close()

	mctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(mctx); err != nil {
		return fmt.Errorf("server: ping for migrations: %w", err)
	}
	n, err := migrations.Up(mctx, db)
	if err != nil {
		return err
	}
	log.Info("server: migrations applied", "count", n)
	return nil
}
