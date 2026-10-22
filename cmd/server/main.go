// Command server runs the Piperoom room service. With --demo it uses a
// scripted fake agent and fake topology over the REAL actions running on
// in-memory clients, so the UI can be built against it with no stack; without
// --demo it applies the migrations, opens the real stack clients via
// wiring.OpenStacks (one client set per pipeline with its own connections,
// docs/design/tenancy.md), registers the real tools/actions and runs the real
// agent (agent.Run). With --quickstart it runs the real agent and tools like
// the full server, but on the in-memory store, with dev login on loopback
// only, write mode off and a one-user users file written for the run
// (quickstart.go). Owners are loaded and cross-checked against the users file
// in every mode.
//
// PIPEROOM_FAKE_AGENT=scripted replaces every model path (the agent run, the
// hand-off and post-mortem writers, triage) with the scripted story of
// internal/room/demo while keeping the real path around it: Postgres store and
// migrations, tools, actions, topology, watcher, HTTP and WebSocket API. It
// needs no model call and no ANTHROPIC_API_KEY. Because a scripted
// investigation is not real, it is accepted only with dev login, write mode
// off and no Slack token; the documented demo start is
//
//	PIPEROOM_FAKE_AGENT=scripted PIPEROOM_AUTH_MODE=dev \
//	PIPEROOM_DEV_ACK=dev-login-has-no-password PIPEROOM_WRITE_MODE=off \
//	PIPEROOM_WATCH_MODE=rooms go run ./cmd/server
//
// Any other value, or a scripted request that would reach real users or run
// fixes, stops the start through config.ParseFakeAgent. --demo and
// --quickstart ignore the variable (--demo already runs the scripted agent and
// forces write mode on).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"piperoom/internal/actions"
	"piperoom/internal/adminconfig"
	"piperoom/internal/agent"
	"piperoom/internal/auth"
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/quickstart"
	"piperoom/internal/room"
	"piperoom/internal/room/demo"
	"piperoom/internal/slackbridge"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
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

// startDeps are what run reaches outside the process besides the room store:
// the data stack and stdout. Tests replace the stack opener so the whole
// --quickstart start runs with no stack.
type startDeps struct {
	openStacks func(ctx context.Context, cfg config.Config, awsCfg *aws.Config, own *owners.Owners, opts wiring.StackOptions) (*wiring.Stacks, error)
	stdout     io.Writer
}

// run is main's body so every cleanup defer runs on every exit path,
// including the listen-error path.
func run() error {
	return runWith(startDeps{openStacks: wiring.OpenStacks, stdout: os.Stdout})
}

func runWith(d startDeps) error {
	demoMode := flag.Bool("demo", false, "run a scripted fake agent (no real tools/agent)")
	quickstartFlag := flag.Bool("quickstart", false, "read-only quickstart: the real agent and tools, in-memory room store, dev login on loopback only, write mode off, a one-user users file for this run; prints the URL to open")
	solo := flag.Bool("solo", false, "with --demo: give the demo presenter the solo ceremony (sole driver, approve-all); with --quickstart: give the sole owner from the owners file the solo ceremony (sole driver, approve-all)")
	ownersPath := flag.String("owners", "", "with --quickstart: the owners file (default ./"+quickstart.DefaultOwnersFile+"); without --quickstart, set OWNERS_FILE instead")
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
	if err := checkStartFlags(*demoMode, *quickstartFlag, *solo, *allowRemote, *ownersPath); err != nil {
		return err
	}
	if down.n != 0 || down.apply || down.confirm != "" || down.dropEvents {
		if down.n == 0 {
			return errDownStrayFlag
		}
		if *demoMode {
			return errors.New("server: -migrate-down needs the room database; it cannot run with -demo")
		}
		if *quickstartFlag {
			return errors.New("server: -migrate-down needs the room database; it cannot run with --quickstart")
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The quickstart runs through internal/quickstart: the package forces the
	// settings it owns (loopback, dev login, write mode off) and writes the
	// run's users file, then calls serve with the composition every mode uses.
	if *quickstartFlag {
		return runQuickstart(ctx, d, cfg, *ownersPath, *solo, log)
	}

	// The settings every mode validates first: a bad value refuses the start
	// before the users or owners are read.
	m := serveMode{demo: *demoMode, allowRemote: *allowRemote, solo: *solo, log: log, stdout: d.stdout}
	s, err := parseServer(cfg, m)
	if err != nil {
		return err
	}

	// Real mode refuses to start without a usable users file: it is the
	// directory of who may approve and drive (LEAD-ARCH round 4, item 7).
	// Demo mode stays self-contained for the users file only, falling back to
	// the dev directory so the UI can be built with no config.
	var users *room.Users
	users, err = room.LoadUsers(cfg.UsersFile)
	if err != nil {
		if !*demoMode {
			return fmt.Errorf("server: load users file %s: %w", cfg.UsersFile, err)
		}
		log.Warn("server: users file unavailable, using demo users", "path", cfg.UsersFile, "err", err)
		users = room.DefaultUsers()
	}
	// Owners are required in every mode: every action's scope check runs
	// against them, so a room can only ever write inside its own pipeline.
	// Users and owners must agree (a layer owner in owners.yaml must exist in
	// users.yaml and actually own that layer), otherwise start is refused.
	own, err := owners.Load(cfg.OwnersFile)
	if err != nil {
		return fmt.Errorf("server: load owners file %s: %w", cfg.OwnersFile, err)
	}
	// --solo resolves the solo user from the flags. The quickstart resolves it
	// in internal/quickstart, before that package writes the users file.
	if err := applySoloFlag(&cfg, own, *demoMode, false, *solo, log); err != nil {
		return err
	}
	if err := room.ValidateUsers(users, own); err != nil {
		return fmt.Errorf("server: users/owners config: %w", err)
	}
	return serve(ctx, d, cfg, own, users, m, s)
}

// serveMode is what serve reads of the flags. The quickstart start reaches
// serve through internal/quickstart with quickstart set and the rest zero.
type serveMode struct {
	demo        bool
	quickstart  bool
	allowRemote bool
	solo        bool
	log         *slog.Logger
	// stdout receives the quickstart URL once the listener is bound.
	stdout io.Writer
}

// serverSettings are the parsed settings the server needs. The real and demo
// starts read them before the users and owners, so a bad value refuses the
// start first, as it always did; the quickstart reads them after
// internal/quickstart has forced its config.
type serverSettings struct {
	replica           string
	maxAgents         int
	maxAgentsGlobal   int
	resumeOnTakeover  bool
	watchCfg          config.Watch
	watchMaxOpenRooms int
	watchForecast     config.WatchForecast
	pmMode            string
	shadowCfg         config.Shadow
	writeOn           bool
	fakeAgent         bool
	autoDelay         time.Duration
	setupCfg          config.Setup
	spend             spendSettings
	authSettings      auth.Settings
	publicOrigin      httpapi.PublicOrigin
}

// parseServer reads and validates the settings serve needs, before anything
// connects.
func parseServer(cfg config.Config, m serveMode) (serverSettings, error) {
	log := m.log
	var s serverSettings
	var err error

	// The agent cap is cost control: parse it first so a bad value stops the
	// server before it connects to anything.
	if s.maxAgents, err = parseMaxAgents(cfg.MaxAgents); err != nil {
		return serverSettings{}, err
	}
	if s.maxAgentsGlobal, err = parseMaxAgentsGlobal(cfg.MaxAgentsGlobal); err != nil {
		return serverSettings{}, err
	}
	if s.resumeOnTakeover, err = parseResumeOnTakeover(cfg.ResumeOnTakeover); err != nil {
		return serverSettings{}, err
	}
	// The name of this process in room leases, executor claims and leader
	// logs (docs/design/replicas.md section 2).
	s.replica = replicaID(cfg.ReplicaID)
	log.Info("server: agent cap", "max_agents_per_replica", s.maxAgents, "max_agents_global", s.maxAgentsGlobal, "replica", s.replica, "resume_on_takeover", s.resumeOnTakeover)

	// A bad PIPEROOM_WATCH_* value stops the server too; the error names the
	// variable. --demo forces the watcher off.
	if s.watchCfg, err = watchConfig(cfg, m.demo, log); err != nil {
		return serverSettings{}, err
	}
	// The watcher's open-room cap is its own (F6), not the agent pool's: a
	// room beyond PIPEROOM_MAX_AGENTS queues for an agent but still gets its
	// first look.
	if s.watchMaxOpenRooms, err = config.ParseWatchMaxOpenRooms(cfg); err != nil {
		return serverSettings{}, fmt.Errorf("server: %w", err)
	}
	// X3: a bad PIPEROOM_WATCH_FORECAST_HORIZON stops the start like any
	// other watcher value.
	if s.watchForecast, err = config.ParseWatchForecast(cfg); err != nil {
		return serverSettings{}, fmt.Errorf("server: %w", err)
	}

	// A bad PIPEROOM_POSTMORTEM_MODEL stops the server.
	if s.pmMode, err = config.ParsePostmortemModel(cfg); err != nil {
		return serverSettings{}, fmt.Errorf("server: %w", err)
	}

	// Shadow mode numbers (docs/design/shadow.md 5.3): a bad
	// PIPEROOM_SHADOW_* value stops the server like a bad watcher value. Which
	// pipelines are shadow comes from owners.yaml.
	if s.shadowCfg, err = config.ParseShadow(cfg); err != nil {
		return serverSettings{}, fmt.Errorf("server: %w", err)
	}

	// Write mode (P0 item 1): a bad PIPEROOM_WRITE_MODE stops the server
	// before it connects to anything; off (the default) records approvals
	// and runs nothing; --demo forces on. The quickstart's option is already
	// written into cfg by internal/quickstart.
	if s.writeOn, err = writeModeSetup(cfg, m.demo, log); err != nil {
		return serverSettings{}, err
	}
	// The scripted agent on a real stack (PIPEROOM_FAKE_AGENT=scripted) is
	// read only outside --demo and --quickstart. A bad value, or a scripted
	// request that would reach real users or run fixes, refuses the start
	// before anything connects (config.ParseFakeAgent).
	if s.fakeAgent, err = fakeAgentSetup(cfg, m.demo, m.quickstart, log); err != nil {
		return serverSettings{}, err
	}
	// Auto-approve delay (P0 item 2): a bad value stops the server here; the
	// owners.yaml lists are checked once the action registries exist.
	if s.autoDelay, err = autoApproveDelay(cfg); err != nil {
		return serverSettings{}, err
	}

	// Setup mode (docs/design/setup-agent.md): PIPEROOM_SETUP_ENABLED=on mounts
	// the runner endpoint (GET /api/runners/ws); ParseSetup refuses it while
	// write mode is off, since setup executes writes through the runner.
	if s.setupCfg, err = config.ParseSetup(cfg); err != nil {
		return serverSettings{}, fmt.Errorf("server: %w", err)
	}

	// Spend limits and the price table (docs/design/cost.md): a bad value
	// stops the server before it connects to anything.
	if s.spend, err = spendSetup(cfg); err != nil {
		return serverSettings{}, err
	}
	s.spend.logSettings(log)

	// Login settings (docs/design/auth.md): a bad PIPEROOM_AUTH_MODE or oidc
	// config refuses start, and so does a reachable-from-outside listen
	// address in dev mode without PIPEROOM_DEV_ALLOW_NONLOOPBACK=1 (security
	// F1). Both run before anything else starts.
	if s.authSettings, err = authSetup(cfg, m.allowRemote, m.demo, log); err != nil {
		return serverSettings{}, err
	}
	log.Info("server: auth", "mode", s.authSettings.Mode, "session_idle", s.authSettings.Idle.String(), "session_max", s.authSettings.Max.String())

	// PIPEROOM_PUBLIC_URL becomes an allowed WebSocket origin, so a malformed
	// value (a wildcard, say) must stop the start rather than widen access.
	if s.publicOrigin, err = httpapi.ParsePublicURL(cfg.PublicURL); err != nil {
		return serverSettings{}, fmt.Errorf("server: %w", err)
	}
	return s, nil
}

// serve is the server from the users and owners on: the real, demo and
// quickstart starts share it, so they cannot drift apart.
func serve(ctx context.Context, d startDeps, cfg config.Config, own *owners.Owners, users *room.Users, m serveMode, s serverSettings) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	log := m.log
	// memStore: the in-memory room store, which --demo and --quickstart use.
	memStore := m.demo || m.quickstart

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
		log.Info("server: shadow mode", "pipelines", shadowPipelines(own), "grace", s.shadowCfg.Grace.String(),
			"token_budget", s.shadowCfg.TokenBudget, "max_turns", s.shadowCfg.MaxTurns)
	}
	// Solo mode: refuse to start when the solo user is unknown or owners.yaml
	// names anyone else (design section 5.3). The solo user was resolved from
	// the flags above (--demo -> the presenter, --quickstart -> the sole owner,
	// otherwise PIPEROOM_SOLO_USER); only --demo relaxes the single-owner rule,
	// so --quickstart runs the real check against the owners file.
	soloUser, err := soloSetup(cfg, users, own, m.demo, log)
	if err != nil {
		return err
	}
	if m.demo && m.solo && soloUser != "" && s.authSettings.DevNonLoopback {
		log.Warn("server: --demo --solo on a non-loopback address: anyone who can reach this port can drive as the solo user", "addr", cfg.HTTPAddr)
	}
	// The OIDC relying party (nil in dev mode). Discovery runs in the
	// background and is retried, so an IdP that is down now does not stop the
	// start; new logins answer 503 until it is up.
	oidcRP, err := oidcSetup(cfg, s.authSettings, s.publicOrigin, log)
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
	// can build the UI against it with no database or migrations;
	// --quickstart takes only its in-memory store. The real mode uses
	// Postgres: migrations first (goose on migrations.FS), then the store.
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
	if memStore {
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
		elector = pg.Elector(s.replica)
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
		// demoCtl is the scripted demo's presenter controls (KAN-398); nil
		// outside --demo, where the control routes do not exist.
		demoCtl *demo.Controls
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
	if m.demo {
		// The real actions over the demo's in-memory clients (LEAD-ARCH round
		// 4, item 3): the proposal path -- scope check, approval, executor --
		// is identical to production.
		acts = actions.NewRegistry()
		fakes := demo.NewFakes()
		if err := actions.Register(acts, fakes.Actions()); err != nil {
			return fmt.Errorf("server: register demo actions: %w", err)
		}
		topo = demo.Builder{}
		// The presenter controls: skip, pause, resume and reset, over the
		// same fake clients the actions write through (KAN-398).
		demoCtl = demo.NewControls(fakes)
		run = demoCtl.Run
		write = demo.WriteHandoff
		if s.pmMode == config.PostmortemModelAuto {
			writePM = demo.WritePostmortem
		}
	} else {
		w, err := wireAgent(ctx, d.openStacks, wiring.NewAgentEndpoint, wireAgentInput{
			cfg:        cfg,
			log:        log,
			own:        own,
			st:         st,
			spendStore: spendStore,
			spend:      s.spend,
			usage:      usage,
			hook:       closedHook,
			fakeAgent:  s.fakeAgent,
			pmMode:     s.pmMode,
			writeOn:    s.writeOn,
		})
		if err != nil {
			return err
		}
		defer w.stacks.Close()
		logStacks(log, w.stacks)
		run = w.run
		write = w.write
		writePM = w.writePM
		triage = w.triage
		triageTimeout = w.triageTimeout
		stacks = w.stacks
		stackFor = w.stackFor
		topo = w.topo
	}

	// The auto-approve lists name actions of the proposal registry: acts in
	// --demo, every client set's actions otherwise (registered whatever the
	// write mode; only their clients change).
	regs := []*actions.Registry{acts}
	if stacks != nil {
		for _, s := range stacks.All() {
			regs = append(regs, s.Actions)
		}
	}
	if err := checkAutoApprove(own, regs, s.autoDelay, log); err != nil {
		return err
	}

	// GET /api/actions lists every action once, across every client set, in
	// stack order (process-wide first); room.AllActions deduplicates by name.
	var actionCatalogue func() []actions.Action
	if stacks != nil {
		actionCatalogue = func() []actions.Action {
			var out []actions.Action
			for _, s := range stacks.All() {
				out = append(out, s.Actions.All()...)
			}
			return out
		}
	}

	svc := newRoomService(room.Deps{
		Store:             st,
		Actions:           acts,
		Tools:             toolsReg,
		StackFor:          stackFor,
		ActionCatalogue:   actionCatalogue,
		Topology:          topo,
		Users:             users,
		Owners:            own,
		Run:               run,
		WriteHandoff:      write,
		WritePostmortem:   writePM,
		Triage:            triage,
		TriagePassTimeout: triageTimeout,
		Options:           agentOptions(cfg, limits),
		ShadowOptions:     shadowAgentOptions(cfg, s.shadowCfg, limits),
		ShadowGrace:       s.shadowCfg.Grace,
		Logger:            log,
		MaxAgents:         s.maxAgents,
		ReplicaID:         s.replica,
		MaxAgentsGlobal:   s.maxAgentsGlobal,
		ResumeOnTakeover:  s.resumeOnTakeover,
		Sessions:          sessions,
		SoloUser:          soloUser,
		WriteModeOff:      !s.writeOn,
		AutoApproveDelay:  s.autoDelay,
		Limits:            limits,
		OnRoomOpened:      slack.onRoomOpened(),
		OnRoomClosed:      closedHook.onClosed,
		OnActionExecuted:  closedHook.onActionExecuted,
		OnRoomDeleted:     deletedHook.onDeleted,
	}, s.authSettings, usage)

	loadCtx, cancelLoad := context.WithTimeout(ctx, 30*time.Second)
	loadErr := svc.Load(loadCtx)
	cancelLoad()
	if loadErr != nil {
		if memStore {
			// Demo and quickstart use an in-memory store; a failed load is
			// unexpected but not worth refusing to start.
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
	// Watch samples (X3a) are kept 7 days, whatever else is configured.
	sampleStore, _ := st.(store.SampleStore)
	var samplesPurge samplesPurger
	if sampleStore != nil {
		samplesPurge = sampleStore
	}
	stopSamplesRetention := startElectedSamplesRetention(ctx, elector, samplesPurge, retentionInterval, retentionRunTimeout, time.Now, log)
	defer stopSamplesRetention()

	httpOpts := []httpapi.Option{httpapi.WithReady(ready), httpapi.WithAlertIngest(alerts), httpapi.WithPublicOrigin(s.publicOrigin), httpapi.WithAuth(s.authSettings), httpapi.WithSessions(sessions),
		httpapi.WithLoginStates(loginStates), httpapi.WithIdentities(identities),
		httpapi.WithSpend(httpapi.Spend{
			Store:                spendStore,
			RoomTokens:           s.spend.roomLimit,
			PipelineDailyTokens:  s.spend.cost.PipelineDailyTokens,
			PipelineDailyCostUSD: s.spend.cost.PipelineDailyCostUSD,
		})}
	httpOpts = append(httpOpts, httpapi.WithChangesWebhook(changeWebhook(cfg, own, st, log)))
	if s.watchCfg.Mode != config.WatchOff {
		// config_drift accept-config (X3b): the current term's watcher.
		httpOpts = append(httpOpts, httpapi.WithConfigAccept(closedHook))
	}
	if oidcRP != nil {
		httpOpts = append(httpOpts, httpapi.WithOIDC(oidcRP))
	}
	if sampleStore != nil {
		httpOpts = append(httpOpts, httpapi.WithSamples(sampleStore))
	}
	if s.setupCfg.Enabled {
		httpOpts = append(httpOpts, httpapi.WithSetup(true))
	}
	if demoCtl != nil {
		// --demo only: mounts POST /api/rooms/{id}/demo/{skip|pause|resume|reset}
		// and marks /healthz with "demo": true for the web control strip.
		httpOpts = append(httpOpts, httpapi.WithDemoControls(demoCtl))
	}
	// Admin settings and connection tests (docs/api.md): real mode only.
	// --demo has no real connections to show or test, and its users may be
	// the built-in directory rather than a users file.
	if !m.demo {
		adm, err := adminconfig.New(cfg, own, users, cfg.OwnersFile, cfg.UsersFile)
		if err != nil {
			return fmt.Errorf("server: admin settings: %w", err)
		}
		httpOpts = append(httpOpts, httpapi.WithAdmin(adm))
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
		Config:       s.watchCfg,
		MaxOpenRooms: s.watchMaxOpenRooms,
		Forecast:     s.watchForecast,
	}
	if sampleStore != nil {
		watchDeps.Samples = sampleStore
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
	switch {
	case m.demo:
		mode = "demo"
	case m.quickstart:
		mode = "quickstart"
	}
	// Listen before Serve so the log and the quickstart URL carry the bound
	// address (a :0 port is the real one there).
	serveErr := listenAndServe(server, m.quickstart, m.stdout, log, mode)
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		// There was no signal; trigger the same shutdown path, then report.
		log.Error("server: listen failed", "err", serveErr)
		cancel()
	}
	<-done
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveError(serveErr)
	}
	return nil
}

// serveError adds the one "server:" prefix to a listenAndServe error, whose
// own messages carry none ("listen on ...", "listen: --quickstart: ...",
// "serve: ...").
func serveError(err error) error {
	return fmt.Errorf("server: %w", err)
}

// listenAndServe binds the server's address and serves until Shutdown. With
// quickstart it refuses a listener that is not on loopback and prints the URL
// to open once to out; everything else goes to the log.
func listenAndServe(server *http.Server, quickstartMode bool, out io.Writer, log *slog.Logger, mode string) error {
	addr := server.Addr
	if addr == "" {
		addr = ":http" // what http.Server.ListenAndServe uses
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	if quickstartMode {
		if err := quickstart.CheckListener(ln.Addr()); err != nil {
			_ = ln.Close()
			return err
		}
	}
	log.Info("server: listening", "addr", ln.Addr().String(), "mode", mode)
	if quickstartMode {
		fmt.Fprintln(out, quickstart.URL(ln.Addr()))
	}
	if err := server.Serve(ln); err != nil {
		return fmt.Errorf("serve: %w", err)
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
