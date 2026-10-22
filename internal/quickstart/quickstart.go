// Package quickstart runs the quickstart server: the real agent and the real
// tools on the pipelines of an owners file, with the in-memory room store, dev
// login on loopback only and a one-user users file written for the run
// (docs/pilot/quickstart.md).
//
// cmd/server --quickstart is this package with WriteMode off: read-only, so an
// approved fix is recorded and never runs. WriteMode on, and SetupEnabled with
// it, is the standalone server of docs/design/setup-agent.md section 2.7,
// where the setup runner runs on the same machine. The server composition
// itself (store, tools, actions, agent, HTTP handler) stays with the caller in
// Options.Serve, so the quickstart and the full server share one start path
// instead of two copies of it.
package quickstart

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"piperoom/internal/auth"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

const (
	// DefaultOwnersFile is the --owners default: the file
	// `discover -write-owners owners.yaml` writes in the same directory.
	DefaultOwnersFile = "owners.yaml"
	// UsersFileName is the one-user users file's name inside the run's temp
	// directory.
	UsersFileName = "users.yaml"
	// TempDirPrefix prefixes the temp directory the one-user users file lives
	// in; Run removes it when the server stops.
	TempDirPrefix = "piperoom-quickstart-"

	// defaultUsersFile is what config.Load gives USERS_FILE when it is unset
	// (a test pins the two together). Run writes its own users file only then:
	// a USERS_FILE the operator set is always used.
	defaultUsersFile = "dev/users.yaml"
)

// The sentinel errors carry or are wrapped with cmd/server's user-facing
// "server:" prefix so `cmd/server --quickstart` prints exactly what it printed
// before the package existed.
var (
	// ErrWriteMode refuses a read-only quickstart when PIPEROOM_WRITE_MODE=on.
	// The quickstart is read-only by default (in-memory store, dev login): an
	// approved fix that really runs belongs on the full server with its room
	// database.
	ErrWriteMode = errors.New("--quickstart is read-only, so PIPEROOM_WRITE_MODE=on cannot be used with it: unset it, or run the full server without --quickstart (docs/pilot/setup.md)")
	// ErrNonLoopback reports a quickstart address another host can reach. Dev
	// login has no password, so the quickstart serves this machine only,
	// whatever PIPEROOM_DEV_ALLOW_NONLOOPBACK says.
	ErrNonLoopback = errors.New("listen address is not loopback")
	// ErrSetupNeedsWriteMode refuses SetupEnabled while WriteMode is off: setup
	// executes writes through the runner, so the two cannot disagree.
	ErrSetupNeedsWriteMode = errors.New("config: PIPEROOM_SETUP_ENABLED=on requires PIPEROOM_WRITE_MODE=on (setup executes writes through the runner)")
	// ErrSoloNeedsOneUser refuses --solo on an owners file that does not name
	// exactly one user.
	ErrSoloNeedsOneUser = errors.New("server: --solo needs an owners file that names one user")

	errNoServer = errors.New("quickstart: Run needs Options.Serve: the server composition is the caller's")
)

// Options is one quickstart start. The zero value plus a loopback Config is
// cmd/server --quickstart: write mode off, setup off, ./owners.yaml.
type Options struct {
	// Config is config.Load's view of the environment. Prepare forces the
	// quickstart's values into a copy; the caller's Config is not changed.
	Config config.Config
	// WriteMode is the write mode this start runs with: false is read-only
	// (cmd/server --quickstart), true allows approved fixes to run
	// (standalone mode, docs/design/setup-agent.md section 2.7).
	WriteMode bool
	// SetupEnabled mounts the setup runner endpoint (GET /api/runners/ws).
	// It needs WriteMode: setup is all writes, so the pair off/on is refused
	// before anything starts.
	SetupEnabled bool
	// OwnersPath is --owners; "" means DefaultOwnersFile in the working
	// directory.
	OwnersPath string
	// Solo gives the owners file's sole user the solo ceremony (--solo). It is
	// refused unless the owners file names exactly one user.
	Solo bool
	// TempDir is where the one-user users file is written; "" is the OS temp
	// directory. Run removes it when the server stops.
	TempDir string
	// Stdout receives the URL once the listener is bound; nil discards it.
	Stdout io.Writer
	// Log is the logger; nil is slog.Default.
	Log *slog.Logger
	// Serve serves the server with the resolved Settings until ctx ends. It is
	// required: the package owns everything that makes a start a quickstart,
	// not the server composition.
	Serve ServeFunc
}

// Settings is a quickstart start resolved against the environment: the config
// with the quickstart's forced values, the owners file, the users the run
// serves, the two options and where the URL goes.
type Settings struct {
	Config       config.Config
	Owners       *owners.Owners
	Users        *room.Users
	WriteMode    bool
	SetupEnabled bool
	Stdout       io.Writer
}

// ServeFunc serves the quickstart server with the resolved settings until ctx
// ends. A non-nil error stops the process with that error.
type ServeFunc func(ctx context.Context, s Settings) error

// Run resolves opts, writes the run's users file and serves through opts.Serve
// until ctx ends or the server fails. The temp users file is removed on every
// exit path.
func Run(ctx context.Context, opts Options) error {
	if opts.Serve == nil {
		return errNoServer
	}
	s, cleanup, err := Prepare(opts)
	if err != nil {
		return err
	}
	defer cleanup()
	return opts.Serve(ctx, s)
}

// Prepare resolves opts without serving: it forces the quickstart's settings
// (loopback, dev login, the write mode, setup), loads the owners file, applies
// --solo and writes the run's users file. The returned cleanup removes the
// temp directory and is safe to call more than once.
func Prepare(opts Options) (Settings, func(), error) {
	noop := func() {}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	cfg := opts.Config
	if err := CheckAddr(cfg.HTTPAddr); err != nil {
		return Settings{}, noop, err
	}
	on, err := config.ParseWriteMode(cfg)
	if err != nil {
		return Settings{}, noop, fmt.Errorf("server: %w", err)
	}
	if on && !opts.WriteMode {
		return Settings{}, noop, fmt.Errorf("server: %w", ErrWriteMode)
	}
	cfg.WriteMode = "off"
	if opts.WriteMode {
		cfg.WriteMode = "on"
	}
	if opts.SetupEnabled {
		if !opts.WriteMode {
			return Settings{}, noop, fmt.Errorf("server: %w", ErrSetupNeedsWriteMode)
		}
		cfg.SetupEnabled = "on"
		// Name the missing or malformed variable now, before anything serves.
		if _, err := config.ParseSetup(cfg); err != nil {
			return Settings{}, noop, fmt.Errorf("server: %w", err)
		}
	} else {
		// The option wins, but a typo, or setup requested while write mode is
		// off, must still refuse the start exactly as it did before the
		// package existed.
		if _, err := config.ParseSetup(cfg); err != nil {
			return Settings{}, noop, fmt.Errorf("server: %w", err)
		}
		cfg.SetupEnabled = "off"
	}
	if mode := strings.ToLower(strings.TrimSpace(cfg.AuthMode)); mode != "" && mode != auth.ModeDev {
		log.Warn("server: --quickstart forces PIPEROOM_AUTH_MODE=dev (loopback only)", "configured", mode)
	}
	cfg.AuthMode = auth.ModeDev
	// The address check above already refused anything off loopback; clearing
	// the switch makes authSetup refuse it too.
	cfg.DevAllowNonLoopback = ""
	ownersPath := opts.OwnersPath
	if ownersPath == "" {
		ownersPath = DefaultOwnersFile
	}
	cfg.OwnersFile = ownersPath
	own, err := owners.Load(cfg.OwnersFile)
	if err != nil {
		return Settings{}, noop, fmt.Errorf("server: load owners file %s: %w", cfg.OwnersFile, err)
	}
	if opts.Solo {
		if got := strings.TrimSpace(cfg.SoloUser); got != "" {
			log.Warn("server: --solo and PIPEROOM_SOLO_USER are both set; --solo wins", "solo_user", got)
		}
		id, ok := SoleUser(own)
		if !ok {
			return Settings{}, noop, ErrSoloNeedsOneUser
		}
		cfg.SoloUser = id
	}
	users, cleanup, err := usersFor(cfg, own, opts.TempDir, log)
	if err != nil {
		return Settings{}, noop, err
	}
	if err := room.ValidateUsers(users, own); err != nil {
		cleanup()
		return Settings{}, noop, fmt.Errorf("server: users/owners config: %w", err)
	}
	if opts.WriteMode {
		log.Info("server: quickstart: in-memory room store, dev login on loopback, write mode on", "owners_file", cfg.OwnersFile, "setup", opts.SetupEnabled)
	} else {
		log.Info("server: quickstart: in-memory room store, dev login on loopback, write mode off", "owners_file", cfg.OwnersFile)
	}
	if cfg.WebDir == "" {
		log.Info("server: quickstart: PIPEROOM_WEB_DIR is unset, so the printed URL serves the API only; run `npm run build` in web/ and set PIPEROOM_WEB_DIR=web/dist for the UI")
	}
	stdout := opts.Stdout
	if stdout == nil {
		stdout = io.Discard
	}
	return Settings{
		Config:       cfg,
		Owners:       own,
		Users:        users,
		WriteMode:    opts.WriteMode,
		SetupEnabled: opts.SetupEnabled,
		Stdout:       stdout,
	}, cleanup, nil
}

// CheckAddr refuses a listen address another host can reach. The error wraps
// ErrNonLoopback unless the address is malformed.
func CheckAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("server: --quickstart: parse PIPEROOM_HTTP_ADDR %q: %w", addr, err)
	}
	if !IsLoopbackHost(host) {
		return fmt.Errorf("server: --quickstart listens on loopback only (dev login has no password) and PIPEROOM_HTTP_ADDR=%q is not loopback: use 127.0.0.1:<port>, or run the full server without --quickstart: %w", addr, ErrNonLoopback)
	}
	return nil
}

// CheckListener checks the address the listener really bound: a "localhost"
// that resolves off loopback must not slip past CheckAddr. The message carries
// no "server:" prefix; the caller adds it.
func CheckListener(a net.Addr) error {
	tcp, ok := a.(*net.TCPAddr)
	if !ok || !tcp.IP.IsLoopback() {
		return fmt.Errorf("listen: --quickstart: the listener bound %s, which is not loopback: %w", a, ErrNonLoopback)
	}
	return nil
}

// URL is the address to open, built from the bound listener so a :0 port
// prints the real one.
func URL(a net.Addr) string {
	return "http://" + a.String() + "/"
}

// IsLoopbackHost reports whether host names the loopback interface. An empty
// host (":8090") listens on every interface, so it is not loopback.
func IsLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// SoleUser returns the user id when the owners file names exactly one
// principal and it is a user. Only then can a one-user users file agree with
// it: room.ValidateUsers checks both files in both directions.
func SoleUser(own *owners.Owners) (string, bool) {
	ps := own.NamedPrincipals()
	if len(ps) != 1 || ps[0].Kind != owners.KindUser {
		return "", false
	}
	return ps[0].Name, true
}

// ownedLayers lists the layers of the owners file's layers: block, in
// events.Layers order and then any other name sorted, so the users file lists
// exactly what the owners file assigns.
func ownedLayers(own *owners.Owners) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, l := range events.Layers {
		if _, ok := own.Layers[l]; ok {
			out = append(out, l)
			seen[l] = true
		}
	}
	var rest []string
	for l := range own.Layers {
		if !seen[l] {
			rest = append(rest, l)
		}
	}
	sort.Strings(rest)
	return append(out, rest...)
}

// usersDoc is the users.yaml shape the quickstart writes.
type usersDoc struct {
	Users []usersEntry `yaml:"users"`
}

type usersEntry struct {
	ID     string   `yaml:"id"`
	Name   string   `yaml:"name"`
	Layers []string `yaml:"layers,flow"`
}

// writeUsers writes a one-user users.yaml for id, owning layers, into a new
// private directory under tmpRoot ("" is the OS temp directory). The returned
// cleanup removes the directory; it is safe to call more than once.
func writeUsers(tmpRoot, id string, layers []string, log *slog.Logger) (string, func(), error) {
	data, err := yaml.Marshal(usersDoc{Users: []usersEntry{{ID: id, Name: id, Layers: layers}}})
	if err != nil {
		return "", nil, fmt.Errorf("server: --quickstart: encode users file: %w", err)
	}
	dir, err := os.MkdirTemp(tmpRoot, TempDirPrefix)
	if err != nil {
		return "", nil, fmt.Errorf("server: --quickstart: make temp dir: %w", err)
	}
	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			log.Warn("server: --quickstart: remove temp users file", "dir", dir, "err", err)
		}
	}
	path := filepath.Join(dir, UsersFileName)
	body := append([]byte("# Written by `server --quickstart` for this run only; removed when the server stops.\n"), data...)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("server: --quickstart: write users file: %w", err)
	}
	return path, cleanup, nil
}

// usersFor returns the users directory of a quickstart start and a cleanup to
// run when the server stops.
//
//   - USERS_FILE set: that file, as in a normal start.
//   - Otherwise, an owners file that names one user (what `discover
//     -write-owners` writes, for the OS user by default): a temp users.yaml
//     where that user owns every layer of the owners file.
//   - Otherwise (several owners, or groups): the default users file, with an
//     error that says to set USERS_FILE when it does not load.
func usersFor(cfg config.Config, own *owners.Owners, tmpRoot string, log *slog.Logger) (*room.Users, func(), error) {
	noop := func() {}
	if cfg.UsersFile != defaultUsersFile {
		users, err := room.LoadUsers(cfg.UsersFile)
		if err != nil {
			return nil, noop, fmt.Errorf("server: load users file %s: %w", cfg.UsersFile, err)
		}
		log.Info("server: --quickstart uses USERS_FILE", "path", cfg.UsersFile)
		return users, noop, nil
	}
	id, ok := SoleUser(own)
	if !ok {
		users, err := room.LoadUsers(cfg.UsersFile)
		if err != nil {
			return nil, noop, fmt.Errorf("server: --quickstart writes a users file only when the owners file names one user, and %s names %s; set USERS_FILE to a users file that lists them: %w",
				cfg.OwnersFile, principalList(own), err)
		}
		log.Info("server: --quickstart: the owners file names several principals, using the default users file", "path", cfg.UsersFile)
		return users, noop, nil
	}
	layers := ownedLayers(own)
	path, cleanup, err := writeUsers(tmpRoot, id, layers, log)
	if err != nil {
		return nil, noop, err
	}
	users, err := room.LoadUsers(path)
	if err != nil {
		cleanup()
		return nil, noop, fmt.Errorf("server: --quickstart: users file for %q, the owner named by %s: %w", id, cfg.OwnersFile, err)
	}
	log.Info("server: --quickstart wrote a one-user users file", "path", path, "user", id, "layers", layers)
	return users, cleanup, nil
}

// principalList names the owners file's principals for an error message.
func principalList(own *owners.Owners) string {
	ps := own.NamedPrincipals()
	if len(ps) == 0 {
		return "no one"
	}
	names := make([]string, len(ps))
	for i, p := range ps {
		names[i] = p.String()
	}
	return strings.Join(names, ", ")
}
