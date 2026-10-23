// Package standalone runs the Piperoom server in-process for
// `piperoom-setup --standalone` (docs/design/setup-agent.md section 2.7): the
// quickstart's loopback-only checks with write mode and setup on, the
// in-memory room store, dev login, every room event appended to
// setup-log.jsonl, and --watch, which restarts the server when the setup room
// writes owners.yaml.
//
// The start itself is internal/quickstart's (A5): the package forces the
// settings standalone may not change (loopback, dev login, write mode, setup),
// loads the owners file, writes the one-user users file and then calls
// Server.Serve, the standalone server composition. The HTTP API, the room
// service and the event log are the real ones; the agent is the package's
// default (a later row wires the setup-mode stack into it).
package standalone

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/discover"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/quickstart"
	"piperoom/internal/setup/runner"
)

const (
	// DefaultAddr is where the standalone server listens when neither
	// PIPEROOM_HTTP_ADDR nor Options.Addr names an address: loopback only.
	DefaultAddr = "127.0.0.1:8090"
	// DefaultWatchInterval is the --watch polling period. Polling (not a
	// filesystem notification) is the one mechanism that behaves the same on
	// the three operating systems and over network drives.
	DefaultWatchInterval = 1 * time.Second
	// MinWatchInterval is the shortest period --watch accepts: a faster poll
	// only costs CPU on a file nobody writes every millisecond.
	MinWatchInterval = 10 * time.Millisecond
	// fallbackOwner is the user id when neither the OS nor the environment
	// gives one the server would accept.
	fallbackOwner = "piperoom"
)

// Options is one standalone start.
type Options struct {
	// Config is the environment, read with config.Load by the caller. The
	// package changes a copy: the address, the secrets directory.
	Config config.Config
	// ProjectDir is --project: the directory owners.yaml, setup-log.jsonl and
	// .piperoom/ live in. "" is the working directory.
	ProjectDir string
	// OwnersPath is the owners file to start on; "" is
	// <project>/owners.yaml.
	OwnersPath string
	// Addr overrides Config.HTTPAddr; "" keeps it, and an empty address
	// becomes DefaultAddr.
	Addr string
	// Watch restarts the server whenever the owners file is written
	// (--watch).
	Watch bool
	// WatchInterval is the --watch polling period; 0 is DefaultWatchInterval.
	WatchInterval time.Duration
	// Stdout receives the URL once the listener is bound; nil discards it.
	Stdout io.Writer
	// Log is the logger; nil is slog.Default.
	Log *slog.Logger
	// Agent replaces the room service's agent runner (tests); nil is the real
	// agent (room.New's default).
	Agent agent.Runner
}

// Run starts the standalone server and serves until ctx ends. With Watch it
// restarts the server whenever the owners file is written; every restart
// reloads the owners file, so a pipeline the setup room registered is watched
// without stopping the process.
func Run(ctx context.Context, opts Options) error {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	stdout := opts.Stdout
	if stdout == nil {
		stdout = io.Discard
	}
	// Flag refusals come before prepare, which writes the secrets directory,
	// the .gitignore entries and a bootstrap owners file.
	interval := opts.WatchInterval
	if interval <= 0 {
		interval = DefaultWatchInterval
	}
	if opts.Watch && interval < MinWatchInterval {
		return fmt.Errorf("setup: --watch-interval %s: want at least %s", interval, MinWatchInterval)
	}
	p, err := prepare(opts)
	if err != nil {
		return err
	}
	logEvents, err := OpenEventLog(filepath.Join(p.project, LogFileName))
	if err != nil {
		return err
	}
	defer func() {
		if err := logEvents.Close(); err != nil {
			log.Warn("setup: close the setup event log", "err", err)
		}
	}()
	srv := &Server{Log: log, Events: logEvents, Agent: opts.Agent}
	// runOnce is one server lifetime. quickstart prepares the loopback,
	// dev-login and write-mode/setup settings and the users file on every
	// start, so a restart re-reads the owners file the setup room wrote.
	runOnce := func(ctx context.Context) error {
		own, err := owners.Load(p.owners)
		if err != nil {
			return fmt.Errorf("setup: load owners file %s: %w", p.owners, err)
		}
		// The sole-owner case gets the solo ceremony (one person approves
		// every layer of their own laptop); a registered file naming several
		// people starts without it.
		_, ok := quickstart.SoleUser(own)
		return quickstart.Run(ctx, quickstart.Options{
			Config:       p.cfg,
			WriteMode:    true,
			SetupEnabled: true,
			OwnersPath:   p.owners,
			Solo:         ok,
			Stdout:       stdout,
			Log:          log,
			Serve:        srv.Serve,
		})
	}
	if !opts.Watch {
		return runOnce(ctx)
	}
	// The pre-restart check: a write that does not load (a half-finished
	// save, a typo) must not stop the serving process; the running server
	// keeps its loaded file and the watch waits for the next write.
	check := func() error {
		_, err := owners.Load(p.owners)
		return err
	}
	return watchRestart(ctx, p.owners, interval, log, runOnce, check)
}

// paths are a start's resolved locations.
type paths struct {
	cfg     config.Config
	project string
	owners  string
}

// prepare resolves the start's locations and makes the two directories and
// the file the server needs, before anything serves: the setup secrets
// directory (PIPEROOM_SECRETS_DIR, defaulting under the project) and, on a
// first start from an empty directory, a bootstrap owners file naming only
// layers. Refusals happen before any of that is written.
func prepare(opts Options) (paths, error) {
	project := strings.TrimSpace(opts.ProjectDir)
	if project == "" {
		project = "."
	}
	abs, err := filepath.Abs(project)
	if err != nil {
		return paths{}, fmt.Errorf("setup: resolve --project %q: %w", project, err)
	}
	info, err := os.Stat(abs)
	if err != nil {
		return paths{}, fmt.Errorf("setup: --project %s: %w", abs, err)
	}
	if !info.IsDir() {
		return paths{}, fmt.Errorf("setup: --project %s is not a directory", abs)
	}
	cfg := opts.Config
	addr := strings.TrimSpace(opts.Addr)
	if addr == "" {
		addr = strings.TrimSpace(cfg.HTTPAddr)
	}
	if addr == "" {
		addr = DefaultAddr
	}
	cfg.HTTPAddr = addr
	// The loopback refusal runs first: a non-loopback address must not leave
	// an owners file or a secrets directory behind.
	if err := quickstart.CheckAddr(addr); err != nil {
		return paths{}, fmt.Errorf("setup: %w", err)
	}
	// Setup executes writes through the runner, so both switches are on here
	// and quickstart.Prepare refuses anything that disagrees.
	cfg.WriteMode = "on"
	cfg.SetupEnabled = "on"
	// The event log carries every room event, so the project's .gitignore
	// names it before the first line is written.
	gitignore := []string{LogFileName}
	if strings.TrimSpace(cfg.SecretsDir) == "" {
		// The pipeline's read credentials land here (U1-5d); the design keeps
		// them next to the project, on the machine that runs the runner.
		cfg.SecretsDir = filepath.Join(abs, ".piperoom", "secrets")
		// The directory is where the secret store lives, so the project's
		// .gitignore must name it even before a runner creates the store
		// (U1-4's rule, the same helper).
		gitignore = append([]string{".piperoom/"}, gitignore...)
	}
	if err := runner.EnsureGitignore(abs, gitignore...); err != nil {
		return paths{}, err
	}
	if err := os.MkdirAll(cfg.SecretsDir, 0o700); err != nil {
		return paths{}, fmt.Errorf("setup: create PIPEROOM_SECRETS_DIR %s: %w", cfg.SecretsDir, err)
	}
	ownersPath := strings.TrimSpace(opts.OwnersPath)
	if ownersPath == "" {
		ownersPath = filepath.Join(abs, quickstart.DefaultOwnersFile)
	}
	if ownersPath, err = filepath.Abs(ownersPath); err != nil {
		return paths{}, fmt.Errorf("setup: resolve the owners file %q: %w", ownersPath, err)
	}
	if err := bootstrapOwners(ownersPath); err != nil {
		return paths{}, err
	}
	return paths{cfg: cfg, project: abs, owners: ownersPath}, nil
}

// bootstrapOwners writes a first owners file when the project has none: no
// pipeline yet, every layer owned by one local user, so the server starts and
// the setup room (which writes the real file at registration) has somewhere to
// write. An existing file is never touched.
func bootstrapOwners(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("setup: owners file %s: %w", path, err)
	}
	var b strings.Builder
	b.WriteString("# Written by `piperoom-setup --standalone`: no pipeline yet.\n")
	b.WriteString("# The setup room writes the live pipeline here when it registers; --watch\n")
	b.WriteString("# restarts the server when it does.\n")
	b.WriteString("layers:\n")
	id := osUserID()
	for _, l := range events.LayersOf(events.PipelineKindCDC) {
		fmt.Fprintf(&b, "  %s: %s\n", l, id)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			// Another start of the same project won the race.
			return nil
		}
		return fmt.Errorf("setup: write the bootstrap owners file %s: %w", path, err)
	}
	if _, err := io.WriteString(f, b.String()); err != nil {
		_ = f.Close()
		return fmt.Errorf("setup: write the bootstrap owners file %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("setup: write the bootstrap owners file %s: %w", path, err)
	}
	return nil
}

// osUserID is the id that owns every layer of a bootstrap owners file: the OS
// user name, then $USER, then "piperoom", skipping any candidate the server
// would refuse as a users.yaml id. On Windows the OS name is DOMAIN\name; only
// the name is kept. discover's -owner default follows the same rule, so a
// later `discover -write-owners` names the same person.
func osUserID() string {
	var candidates []string
	if u, err := user.Current(); err == nil {
		candidates = append(candidates, u.Username)
	}
	candidates = append(candidates, os.Getenv("USER"), os.Getenv("USERNAME"))
	for _, c := range candidates {
		if i := strings.LastIndexAny(c, `\/`); i >= 0 {
			c = c[i+1:]
		}
		if c = strings.TrimSpace(c); c != "" && discover.ValidateOwner(c) == nil {
			return c
		}
	}
	return fallbackOwner
}
