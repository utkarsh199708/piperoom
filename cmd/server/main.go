// Command server runs the Piperoom room service. With --demo it uses a
// scripted fake agent and fake topology over the REAL actions running on
// in-memory clients, so the UI can be built against it with no stack; without
// --demo it applies the migrations, opens the real stack clients via
// wiring.OpenStacks (one client set per pipeline with its own connections,
// docs/design/tenancy.md), registers the real tools/actions and runs the real
// agent (agent.Run). Owners are loaded and cross-checked against the users
// file in both modes.
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
	"path/filepath"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/auth"
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/room/demo"
	"piperoom/internal/slackbridge"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
	"piperoom/internal/tools/similarincidents"
	"piperoom/internal/topology"
	"piperoom/internal/version"
	"piperoom/internal/watch"
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
	allowRemote := flag.Bool("allow-remote", false, "DEPRECATED alias of PIPEROOM_DEV_ALLOW_NONLOOPBACK=1: allow dev auth on a non-loopback PIPEROOM_HTTP_ADDR")
	probe := flag.Bool("healthcheck", false, "probe GET /healthz on PIPEROOM_HTTP_ADDR and exit 0 if healthy (for the container HEALTHCHECK)")
	showVersion := flag.Bool("version", false, "print the build version and exit")
	var down migrateDownOpts
	flag.IntVar(&down.n, "migrate-down", 0, "roll back the N most recent migrations of the room database and exit; a dry run unless -migrate-down-apply")
	flag.BoolVar(&down.apply, "migrate-down-apply", false, "with -migrate-down: really roll back (needs -migrate-down-confirm)")
	flag.StringVar(&down.confirm, "migrate-down-confirm", "", "with -migrate-down-apply: the database's current version, as the dry run printed it")
	flag.BoolVar(&down.dropEvents, "migrate-down-drop-events", false, "with -migrate-down-apply: allow a rollback that deletes room_events data")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.String())
		return nil
	}
	cfg := config.Load()
	if *probe {
		return healthcheck(context.Background(), cfg.HTTPAddr)
	}
	if down.n != 0 || down.apply || down.confirm != "" || down.dropEvents {
		if down.n == 0 {
			return errDownStrayFlag
		}
		if *demoMode {
			return errors.New("server: -migrate-down needs the room database; it cannot run with -demo")
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return migrateDownMain(ctx, cfg.RoomDBURL, down, os.Stdout)
	}
	// A bad PIPEROOM_LOG_LEVEL or _FORMAT stops the start before anything is
	// logged in a format the collector would not parse.
	log, err := newLogger(cfg, os.Stderr)
	if err != nil {
		return err
	}
	slog.SetDefault(log)
	log.Info("server: starting", "version", version.String())

	// The agent cap is cost control: parse it first so a bad value stops the
	// server before it connects to anything.
	maxAgents, err := parseMaxAgents(cfg.MaxAgents)
	if err != nil {
		return err
	}
	maxAgentsGlobal, err := parseMaxAgentsGlobal(cfg.MaxAgentsGlobal)
	if err != nil {
		return err
	}
	resumeOnTakeover, err := parseResumeOnTakeover(cfg.ResumeOnTakeover)
	if err != nil {
		return err
	}
	// The name of this process in room leases, executor claims and leader
	// logs (docs/design/replicas.md section 2).
	replica := replicaID(cfg.ReplicaID)
	log.Info("server: agent cap", "max_agents_per_replica", maxAgents, "max_agents_global", maxAgentsGlobal, "replica", replica, "resume_on_takeover", resumeOnTakeover)

	// A bad PIPEROOM_WATCH_* value stops the server too; the error names the
	// variable. --demo forces the watcher off.
	watchCfg, err := watchConfig(cfg, *demoMode, log)
	if err != nil {
		return err
	}
	// The watcher's open-room cap is its own (F6), not the agent pool's: a
	// room beyond PIPEROOM_MAX_AGENTS queues for an agent but still gets its
	// first look.
	watchMaxOpenRooms, err := config.ParseWatchMaxOpenRooms(cfg)
	if err != nil {
		return fmt.Errorf("server: %w", err)
	}

	// A bad PIPEROOM_POSTMORTEM_MODEL stops the server.
	pmMode, err := config.ParsePostmortemModel(cfg)
	if err != nil {
		return fmt.Errorf("server: %w", err)
	}

	// Shadow mode numbers (docs/design/shadow.md 5.3): a bad
	// PIPEROOM_SHADOW_* value stops the server like a bad watcher value. Which
	// pipelines are shadow comes from owners.yaml.
	shadowCfg, err := config.ParseShadow(cfg)
	if err != nil {
		return fmt.Errorf("server: %w", err)
	}

	// Spend limits and the price table (docs/design/cost.md): a bad value
	// stops the server before it connects to anything.
	spend, err := spendSetup(cfg)
	if err != nil {
		return err
	}
	spend.logSettings(log)

	// Login settings (docs/design/auth.md): a bad PIPEROOM_AUTH_MODE or oidc
	// config refuses start, and so does a reachable-from-outside listen
	// address in dev mode without PIPEROOM_DEV_ALLOW_NONLOOPBACK=1 (security
	// F1). Both run before anything else starts.
	authSettings, err := authSetup(cfg, *allowRemote, *demoMode, log)
	if err != nil {
		return err
	}
	log.Info("server: auth", "mode", authSettings.Mode, "session_idle", authSettings.Idle.String(), "session_max", authSettings.Max.String())

	// PIPEROOM_PUBLIC_URL becomes an allowed WebSocket origin, so a malformed
	// value (a wildcard, say) must stop the start rather than widen access.
	publicOrigin, err := httpapi.ParsePublicURL(cfg.PublicURL)
	if err != nil {
		return fmt.Errorf("server: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Real mode refuses to start without a usable users file: it is the
	// directory of who may approve and drive (LEAD-ARCH round 4, item 7).
	// Demo mode stays self-contained for the users file only, falling back to
	// the dev directory so the UI can be built with no config.
	users, err := room.LoadUsers(cfg.UsersFile)
	if err != nil {
		if !*demoMode {
			return fmt.Errorf("server: load users file %s: %w", cfg.UsersFile, err)
		}
		log.Warn("server: users file unavailable, using demo users", "path", cfg.UsersFile, "err", err)
		users = room.DefaultUsers()
	}
	// Owners are required in both modes: every action's scope check runs
	// against them, so a room can only ever write inside its own pipeline.
	// Users and owners must agree (a layer owner in owners.yaml must exist in
	// users.yaml and actually own that layer), otherwise start is refused.
	own, err := owners.Load(cfg.OwnersFile)
	if err != nil {
		return fmt.Errorf("server: load owners file %s: %w", cfg.OwnersFile, err)
	}
	if err := room.ValidateUsers(users, own); err != nil {
		return fmt.Errorf("server: users/owners config: %w", err)
	}
	// Tenancy (docs/design/tenancy.md): PIPEROOM_TENANCY_MODE must agree with
	// the owners file, and the chosen model is logged.
	slackCfg := slackFromConfig(cfg)
	if _, err := tenancySetup(cfg, own, slackCfg.Token != "" && slackCfg.ChannelID != "", log); err != nil {
		return err
	}
	// Retention and room limits (docs/design/retention.md): a bad value stops
	// the server before it connects to anything; a typo must never silently
	// keep (or delete) incident data.
	limits, retention, err := limitsSetup(cfg, own, log)
	if err != nil {
		return err
	}
	if own.AnyShadow() {
		log.Info("server: shadow mode", "pipelines", shadowPipelines(own), "grace", shadowCfg.Grace.String(),
			"token_budget", shadowCfg.TokenBudget, "max_turns", shadowCfg.MaxTurns)
	}
	// Solo mode: refuse to start when the solo user is unknown or owners.yaml
	// names anyone else (design section 5.3).
	soloUser, err := soloSetup(cfg, users, own, log)
	if err != nil {
		return err
	}
	// The OIDC relying party (nil in dev mode). Discovery runs in the
	// background and is retried, so an IdP that is down now does not stop the
	// start; new logins answer 503 until it is up.
	oidcRP, err := oidcSetup(cfg, authSettings, publicOrigin, log)
	if err != nil {
		return err
	}
	if oidcRP != nil {
		oidcRP.Start(ctx)
	}

	// Signed alert routes (POST /api/alerts/{source}). Built before anything
	// is opened so a bad PAGERDUTY_SERVICES refuses start at once. Only the
	// names of the enabled sources are logged, never a secret.
	alerts, err := alertConfig(cfg, own)
	if err != nil {
		return err
	}
	log.Info("server: alert sources", "enabled", enabledAlertSources(alerts))

	// --demo is fully self-contained (in-memory store + scripted agent) so A6
	// can build the UI against it with no database or migrations. The real
	// mode uses Postgres: migrations first (goose on migrations.FS), then the
	// store.
	var (
		st      store.Store
		ready   httpapi.Pinger      // nil in demo mode: the in-memory store is always ready
		threads slackbridge.Threads = slackbridge.NewMemThreads()
		// elector runs the leader-only loops; the in-memory store is one
		// process, so in demo mode it is always the leader.
		elector store.Elector = store.SoloElector{}
		// Login sessions: in memory in demo mode, the sessions table otherwise.
		sessions auth.Store = auth.NewMemStore()
		// The pre-login rows and the iss+sub bindings of OIDC login.
		loginStates auth.LoginStateStore = auth.NewMemLoginStates()
		identities  auth.IdentityStore   = auth.NewMemIdentities()
	)
	if *demoMode {
		st = memstore.New()
	} else {
		if err := migrateRoomDB(ctx, cfg.RoomDBURL, log); err != nil {
			return err
		}
		// store.New pings the pool; bound it so a hung database cannot block
		// start-up forever (nit 10: store.New had no deadline).
		pgCtx, cancelPG := context.WithTimeout(ctx, 30*time.Second)
		pg, err := store.New(pgCtx, cfg.RoomDBURL, log)
		cancelPG()
		if err != nil {
			return fmt.Errorf("server: connect room store: %w", err)
		}
		st = pg
		ready = pg
		elector = pg.Elector(replica)
		threads = pg.SlackThreads()
		sessions = pg.Sessions()
		loginStates = pg.LoginStates()
		identities = pg.Identities()
		defer pg.Close()
	}
	// The per-room log caps are enforced by the store, in the insert's
	// transaction (docs/design/retention.md section 5.2).
	storeLimits(st, limits)

	// Slack mirroring is on only when SLACK_BOT_TOKEN/SLACK_CHANNEL_ID are set,
	// in both modes; a wrong setting refuses start. The deferred stop runs
	// before the store's Close (defers are LIFO), so no mirror outlives it.
	slack, err := newMirrors(ctx, st, threads, slackCfg, log)
	if err != nil {
		return err
	}
	defer slack.stop(mirrorStopTimeout)

	var (
		acts     *actions.Registry
		toolsReg = tools.NewRegistry()
		topo     topology.Builder
		run      = agent.Run
		write    = agent.WriteHandoff
		// writePM is room.Deps.WritePostmortem; nil gives the events-only
		// draft (PIPEROOM_POSTMORTEM_MODEL=off, or no writer wired yet).
		writePM agent.PostmortemWriter
		// triage is the first look (F6); nil in --demo, which has no model.
		triage        agent.Triager
		triageTimeout time.Duration
		stacks        *wiring.Stacks // nil in demo mode
		// stackFor is room.Deps.StackFor: nil in demo mode, where every
		// room uses acts and toolsReg.
		stackFor func(string) room.PipelineStack
		// closedHook is room.Deps.OnRoomClosed: it forgets the room's token
		// count and tells the watcher, which is set once it exists.
		closedHook = &roomClosedHook{}
		// deletedHook is room.Deps.OnRoomDeleted: it closes a deleted room's
		// WebSockets once the HTTP handler (and its gateway) exists.
		deletedHook = &roomDeletedHook{}
		// usage hands the hand-off's agent.usage events to the room service
		// once it exists.
		usage = &usageRelay{log: log}
	)
	// Both stores keep the spend tables; the in-memory one only for --demo.
	spendStore, ok := st.(store.SpendStore)
	if !ok {
		return errors.New("server: the room store keeps no spend tables")
	}
	if *demoMode {
		// The real actions over the demo's in-memory clients (LEAD-ARCH round
		// 4, item 3): the proposal path -- scope check, approval, executor --
		// is identical to production.
		acts = actions.NewRegistry()
		if err := actions.Register(acts, demo.FakeActionDeps()); err != nil {
			return fmt.Errorf("server: register demo actions: %w", err)
		}
		topo = demo.Builder{}
		run = demo.Run
		write = demo.WriteHandoff
		if pmMode == config.PostmortemModelAuto {
			writePM = demo.WritePostmortem
		}
	} else {
		// The model endpoint first: a wrong provider, region, project or token
		// budget should stop the server before it opens any connection.
		// One AWS config for the process, shared by Bedrock and MSK IAM.
		awsCfg, err := wiring.LoadAWS(ctx, cfg)
		if err != nil {
			return fmt.Errorf("server: %w", err)
		}
		ep, err := wiring.NewAgentEndpoint(ctx, cfg, log, awsCfg)
		if err != nil {
			return fmt.Errorf("server: %w", err)
		}
		// Spend limits are checked against the room database, so a restart
		// does not reset them.
		if err := ep.SetSpend(spend.options(spendStore, usage)); err != nil {
			return fmt.Errorf("server: %w", err)
		}
		run = ep.Run
		write = ep.WriteHandoff
		// With PIPEROOM_TRIAGE_PASS=off the pass still writes agent.triage
		// "failed"/"off" (no model call), so the UI never waits for one.
		triage = ep.Triage
		_, _, triageTimeout = ep.FirstLook()
		closedHook.setForget(ep)

		// One client set per pipeline with connections: in owners.yaml, plus
		// the process-wide set when a pipeline uses it (docs/design/tenancy.md
		// section 5). Each room, the topology and the watcher reach only the
		// set of their own pipeline.
		sets, err := wiring.OpenStacks(ctx, cfg, awsCfg, own, wiring.StackOptions{
			// similar_incidents reads past closed rooms from the room store;
			// each set's Owners holds only its own pipelines.
			Prepare: func(w *wiring.Wired) { w.Rooms = similarincidents.FromStore(st) },
		})
		if err != nil {
			return fmt.Errorf("server: open the data stack: %w", err)
		}
		defer sets.Close()
		logStacks(log, sets)
		stacks = sets
		stackFor = roomStacks(sets)
		topo = sets
	}

	svc := newRoomService(room.Deps{
		Store:             st,
		Actions:           acts,
		Tools:             toolsReg,
		StackFor:          stackFor,
		Topology:          topo,
		Users:             users,
		Owners:            own,
		Run:               run,
		WriteHandoff:      write,
		WritePostmortem:   writePM,
		Triage:            triage,
		TriagePassTimeout: triageTimeout,
		Options: agent.Options{
			Model:    cfg.Model,
			Effort:   cfg.Effort,
			MaxTurns: 40,
		},
		ShadowOptions:    shadowAgentOptions(cfg, shadowCfg),
		ShadowGrace:      shadowCfg.Grace,
		Logger:           log,
		MaxAgents:        maxAgents,
		ReplicaID:        replica,
		MaxAgentsGlobal:  maxAgentsGlobal,
		ResumeOnTakeover: resumeOnTakeover,
		Sessions:         sessions,
		SessionIdle:      authSettings.Idle,
		ViewerGroups:     authSettings.ViewerGroups,
		SoloUser:         soloUser,
		Limits:           limits,
		OnRoomOpened:     slack.onRoomOpened(),
		OnRoomClosed:     closedHook.onClosed,
		OnRoomDeleted:    deletedHook.onDeleted,
	}, usage)

	loadCtx, cancelLoad := context.WithTimeout(ctx, 30*time.Second)
	loadErr := svc.Load(loadCtx)
	cancelLoad()
	if loadErr != nil {
		if *demoMode {
			// Demo uses an in-memory store; a failed load is unexpected but not
			// worth refusing to start.
			log.Warn("server: cache load failed", "err", loadErr)
		} else {
			// Real mode must fail: a broken cache means rooms reached later are
			// rebuilt lazily without their agent (review nit 9).
			return fmt.Errorf("server: load room cache: %w", loadErr)
		}
	}

	// The Slack mirror, retention, auth prune and watcher run on one replica
	// only: the leader of each loop (docs/design/replicas.md section 3).
	stopSlack := slack.elect(ctx, elector, svc)
	defer stopSlack()

	// Retention starts after Load so the cache is built before any room can
	// disappear. The deferred stop runs before the store closes (LIFO).
	stopRetention := startElectedRetention(ctx, elector, svc, retention, retentionInterval, retentionRunTimeout, time.Now, log)
	defer stopRetention()
	// The open-room limit closes rooms through the service, so it too starts
	// after Load and stops before the service closes (LIFO defers).
	// With several replicas it runs on the elected leader only.
	stopOpenLimit := startElectedOpenLimit(ctx, elector, svc, limits.MaxOpenDays, openLimitInterval, openLimitRunTimeout, log)
	defer stopOpenLimit()
	// Rooms from before migration 00012 get their byte_size counted in the
	// background; their byte cap applies once that is done. Every replica
	// runs it: each one reloads its own usage cache afterwards, and the
	// backfill only fills rows still NULL, so a second pass finds nothing.
	var backfiller byteSizeBackfiller
	if pg, ok := st.(*store.PG); ok {
		backfiller = pg
	}
	stopBackfill := startByteSizeBackfill(ctx, backfiller, svc.ReloadUsage, log)
	defer stopBackfill()
	// Dead sessions and login states are pruned whatever PIPEROOM_RETENTION_DAYS
	// says: the job above is off by default, this one is always on.
	sessionPruner, loginStatePruner := authPruners(sessions, loginStates)
	stopAuthPrune := startElectedAuthPrune(ctx, elector, sessionPruner, loginStatePruner, retentionInterval, retentionRunTimeout, time.Now, log)
	defer stopAuthPrune()
	// The change log keeps PIPEROOM_CHANGES_RETENTION_DAYS (default 30) of rows.
	changesDays, err := config.ParseChangesRetentionDays(cfg)
	if err != nil {
		return err
	}
	changesPurge, _ := st.(changesPurger)
	stopChangesRetention := startElectedChangesRetention(ctx, elector, changesPurge, changesDays, retentionInterval, retentionRunTimeout, time.Now, log)
	defer stopChangesRetention()

	httpOpts := []httpapi.Option{httpapi.WithReady(ready), httpapi.WithAlertIngest(alerts), httpapi.WithPublicOrigin(publicOrigin), httpapi.WithAuth(authSettings), httpapi.WithSessions(sessions),
		httpapi.WithLoginStates(loginStates), httpapi.WithIdentities(identities),
		httpapi.WithSpend(httpapi.Spend{
			Store:                spendStore,
			RoomTokens:           spend.roomLimit,
			PipelineDailyTokens:  spend.cost.PipelineDailyTokens,
			PipelineDailyCostUSD: spend.cost.PipelineDailyCostUSD,
		})}
	if oidcRP != nil {
		httpOpts = append(httpOpts, httpapi.WithOIDC(oidcRP))
	}
	if cfg.WebDir != "" {
		// Fail at start, not at the first page load, when the image or the
		// mount has no built UI.
		if err := checkWebDir(cfg.WebDir); err != nil {
			return err
		}
		httpOpts = append(httpOpts, httpapi.WithWebDir(cfg.WebDir))
		log.Info("server: serving the web UI", "dir", cfg.WebDir)
	}
	handler := httpapi.New(svc, st, log, httpOpts...)
	// Retention and other replicas' deletes close sockets from here on; none
	// is open before the handler exists.
	deletedHook.set(handler.CloseRoomSockets)
	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// The watcher starts after Load (so it never sees a half-built room cache)
	// and after every check that can still refuse the start (web dir, handler),
	// just before the server listens, so a start that is about to fail never
	// runs a tick. Shutdown stops it before the room service closes (the done
	// goroutine below); the defer covers the early returns after this point.
	watchDeps := watch.Deps{
		Owners:       own,
		Rooms:        watchRooms{svc: svc},
		Config:       watchCfg,
		MaxOpenRooms: watchMaxOpenRooms,
	}
	if stacks != nil {
		watchDeps.ClientsFor = watchClients(stacks)
	}
	stopWatch, err := startElectedWatcher(ctx, elector, watchDeps, closedHook, log)
	if err != nil {
		handler.Close()
		return err
	}
	defer stopWatch()

	// done closes only after Shutdown AND handler.Close (WS connections, the
	// room service and every goroutine it owns) have returned, so main can
	// wait for them before its defers close the store and the stack.
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		shutdownServer(stopWatch, server, handler)
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

// checkWebDir verifies that dir holds a built web UI (an index.html).
func checkWebDir(dir string) error {
	if _, err := os.Stat(filepath.Join(dir, "index.html")); err != nil {
		return fmt.Errorf("server: PIPEROOM_WEB_DIR %s has no index.html (run `npm run build` in web/): %w", dir, err)
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
	// Replicas that start together take turns (docs/design/replicas.md
	// section 7): the first applies the migrations, the rest find none.
	var n int
	if err := withMigrationLock(mctx, db, func() error {
		var err error
		n, err = migrations.Up(mctx, db)
		return err
	}); err != nil {
		return err
	}
	log.Info("server: migrations applied", "count", n)
	return nil
}
