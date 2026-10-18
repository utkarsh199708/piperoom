package main

// --quickstart (tasks/P1-quickstart-readonly.md, P1a): the real agent and the
// real read-only tools on your pipelines, with the in-memory room store, dev
// login on loopback only, write mode off, and a one-user users file written
// for this run, so `discover -write-owners` plus this flag is a whole setup.

import (
	"errors"
	"fmt"
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
	// quickstartDefaultOwners is the --owners default: the file
	// `discover -write-owners owners.yaml` writes in the same directory.
	quickstartDefaultOwners = "owners.yaml"
	// configDefaultUsersFile is what config.Load gives USERS_FILE when it is
	// unset (a test pins the two together). --quickstart writes its own users
	// file only then: a USERS_FILE the operator set is always used.
	configDefaultUsersFile = "dev/users.yaml"
	// quickstartUsersName is the file name inside the run's temp directory.
	quickstartUsersName = "users.yaml"
)

// errQuickstartWriteMode refuses PIPEROOM_WRITE_MODE=on with --quickstart. The
// quickstart is read-only by design (in-memory store, dev login): an approved
// fix that really runs belongs on the full server with its room database.
var errQuickstartWriteMode = errors.New("server: --quickstart is read-only, so PIPEROOM_WRITE_MODE=on cannot be used with it: unset it, or run the full server without --quickstart (docs/pilot/setup.md)")

// checkStartFlags refuses flag combinations that cannot mean what they say,
// before anything is read or opened.
func checkStartFlags(demo, quickstart, allowRemote bool, ownersPath string) error {
	switch {
	case quickstart && demo:
		return errors.New("server: --quickstart and --demo cannot be combined: --quickstart runs the real agent on your pipelines, --demo a scripted one on fake data")
	case quickstart && allowRemote:
		return errors.New("server: --quickstart listens on loopback only; --allow-remote cannot be combined with it")
	case ownersPath != "" && !quickstart:
		return errors.New("server: --owners needs --quickstart; without it, set OWNERS_FILE")
	}
	return nil
}

// checkQuickstartAddr refuses a listen address another host can reach. Dev
// login has no password, so the quickstart serves this machine only, whatever
// PIPEROOM_DEV_ALLOW_NONLOOPBACK says. The error wraps errNonLoopback.
func checkQuickstartAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("server: --quickstart: parse PIPEROOM_HTTP_ADDR %q: %w", addr, err)
	}
	if !isLoopbackHost(host) {
		return fmt.Errorf("server: --quickstart listens on loopback only (dev login has no password) and PIPEROOM_HTTP_ADDR=%q is not loopback: use 127.0.0.1:<port>, or run the full server without --quickstart: %w", addr, errNonLoopback)
	}
	return nil
}

// checkQuickstartListener checks the address the listener really bound: a
// "localhost" that resolves off loopback must not slip past the check above.
func checkQuickstartListener(a net.Addr) error {
	tcp, ok := a.(*net.TCPAddr)
	if !ok || !tcp.IP.IsLoopback() {
		return fmt.Errorf("server: --quickstart: the listener bound %s, which is not loopback: %w", a, errNonLoopback)
	}
	return nil
}

// quickstartConfig turns cfg into the quickstart's settings: loopback only,
// dev login, write mode off, the owners file from --owners (default
// ./owners.yaml). It runs before any other check reads cfg, so the auth and
// write-mode set-up see the forced values.
func quickstartConfig(cfg config.Config, ownersPath string, log *slog.Logger) (config.Config, error) {
	if err := checkQuickstartAddr(cfg.HTTPAddr); err != nil {
		return cfg, err
	}
	on, err := config.ParseWriteMode(cfg)
	if err != nil {
		return cfg, fmt.Errorf("server: %w", err)
	}
	if on {
		return cfg, errQuickstartWriteMode
	}
	if mode := strings.ToLower(strings.TrimSpace(cfg.AuthMode)); mode != "" && mode != auth.ModeDev {
		log.Warn("server: --quickstart forces PIPEROOM_AUTH_MODE=dev (loopback only)", "configured", mode)
	}
	cfg.AuthMode = auth.ModeDev
	// The address check above already refused anything off loopback; clearing
	// the switch makes authSetup refuse it too.
	cfg.DevAllowNonLoopback = ""
	if ownersPath == "" {
		ownersPath = quickstartDefaultOwners
	}
	cfg.OwnersFile = ownersPath
	return cfg, nil
}

// soleUser returns the user id when the owners file names exactly one
// principal and it is a user. Only then can a one-user users file agree with
// it: room.ValidateUsers checks both files in both directions.
func soleUser(own *owners.Owners) (string, bool) {
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

// quickstartUsersDoc is the users.yaml shape the quickstart writes.
type quickstartUsersDoc struct {
	Users []quickstartUser `yaml:"users"`
}

type quickstartUser struct {
	ID     string   `yaml:"id"`
	Name   string   `yaml:"name"`
	Layers []string `yaml:"layers,flow"`
}

// writeQuickstartUsers writes a one-user users.yaml for id, owning layers,
// into a new private directory under tmpRoot ("" is the OS temp directory).
// The returned cleanup removes the directory; it is safe to call more than
// once.
func writeQuickstartUsers(tmpRoot, id string, layers []string, log *slog.Logger) (string, func(), error) {
	data, err := yaml.Marshal(quickstartUsersDoc{Users: []quickstartUser{{ID: id, Name: id, Layers: layers}}})
	if err != nil {
		return "", nil, fmt.Errorf("server: --quickstart: encode users file: %w", err)
	}
	dir, err := os.MkdirTemp(tmpRoot, "piperoom-quickstart-")
	if err != nil {
		return "", nil, fmt.Errorf("server: --quickstart: make temp dir: %w", err)
	}
	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			log.Warn("server: --quickstart: remove temp users file", "dir", dir, "err", err)
		}
	}
	path := filepath.Join(dir, quickstartUsersName)
	body := append([]byte("# Written by `server --quickstart` for this run only; removed when the server stops.\n"), data...)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("server: --quickstart: write users file: %w", err)
	}
	return path, cleanup, nil
}

// quickstartUsers returns the users directory of a --quickstart start and a
// cleanup to run when the server stops.
//
//   - USERS_FILE set: that file, as in a normal start.
//   - Otherwise, an owners file that names one user (what `discover
//     -write-owners` writes, for the OS user by default): a temp users.yaml
//     where that user owns every layer of the owners file.
//   - Otherwise (several owners, or groups): the default users file, with an
//     error that says to set USERS_FILE when it does not load.
func quickstartUsers(cfg config.Config, own *owners.Owners, tmpRoot string, log *slog.Logger) (*room.Users, func(), error) {
	noop := func() {}
	if cfg.UsersFile != configDefaultUsersFile {
		users, err := room.LoadUsers(cfg.UsersFile)
		if err != nil {
			return nil, noop, fmt.Errorf("server: load users file %s: %w", cfg.UsersFile, err)
		}
		log.Info("server: --quickstart uses USERS_FILE", "path", cfg.UsersFile)
		return users, noop, nil
	}
	id, ok := soleUser(own)
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
	path, cleanup, err := writeQuickstartUsers(tmpRoot, id, layers, log)
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

// quickstartURL is the address to open, built from the bound listener so a
// :0 port prints the real one.
func quickstartURL(a net.Addr) string {
	return "http://" + a.String() + "/"
}
