package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

const (
	shadowService   = "piperoom-shadow"
	shadowProfile   = "shadow"
	shadowOwnersRel = "dev/owners.shadow.yaml"
	shadowURL       = "http://127.0.0.1:8090"
	apiKeyEnv       = "ANTHROPIC_API_KEY"
	devAckEnv       = "PIPEROOM_DEV_ACK"

	// Watcher timings of the compose service (dev/docker-compose.yml), reused for host mode.
	devWatchInterval = "15s"
	devWatchCooldown = "5m"
)

const shadowUsage = `usage: devctl shadow up [flags] | down | status | env [--shell sh|powershell]

  up      build and start the Piperoom server in shadow mode on the dev stack (needs ` + apiKeyEnv + `)
  down    stop that server (room data stays in the piperoom database)
  status  is the server up, and which pipelines are shadow
  env     print the settings to run the server on the host instead: go run ./cmd/server

flags (up, env): --grace <duration>  --token-budget <n>  --max-turns <n>  --watch-interval <duration>  --watch-cooldown <duration>
up and down also take --owner <tag> (the shared stack lock, like devctl up)`

// shadowOpts are the optional numbers a person may override. An empty field means
// "leave the default": compose and the server then apply their own.
type shadowOpts struct {
	Grace         string // PIPEROOM_SHADOW_GRACE
	TokenBudget   string // PIPEROOM_SHADOW_TOKEN_BUDGET
	MaxTurns      string // PIPEROOM_SHADOW_MAX_TURNS
	WatchInterval string // PIPEROOM_WATCH_INTERVAL
	WatchCooldown string // PIPEROOM_WATCH_COOLDOWN
	Shell         string // env only: "sh" or "powershell"
}

// parseShadowFlags reads the flags of `shadow up` and `shadow env`. It validates every
// value with the server's own parsers, so a typo fails here with the env var's name
// instead of after a two-minute image build.
func parseShadowFlags(name string, args []string) (shadowOpts, error) {
	var o shadowOpts
	fs := flag.NewFlagSet("shadow "+name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&o.Grace, "grace", "", "")
	fs.StringVar(&o.TokenBudget, "token-budget", "", "")
	fs.StringVar(&o.MaxTurns, "max-turns", "", "")
	fs.StringVar(&o.WatchInterval, "watch-interval", "", "")
	fs.StringVar(&o.WatchCooldown, "watch-cooldown", "", "")
	fs.StringVar(&o.Shell, "shell", defaultShell(runtime.GOOS), "")
	if err := fs.Parse(args); err != nil {
		return shadowOpts{}, fmt.Errorf("devctl: shadow %s: %w\n%s", name, err, shadowUsage)
	}
	if fs.NArg() != 0 {
		return shadowOpts{}, fmt.Errorf("devctl: shadow %s: unexpected argument %q\n%s", name, fs.Arg(0), shadowUsage)
	}
	if o.Shell != "sh" && o.Shell != "powershell" {
		return shadowOpts{}, fmt.Errorf("devctl: shadow %s: --shell %q: want sh or powershell", name, o.Shell)
	}
	if err := o.validate(); err != nil {
		return shadowOpts{}, err
	}
	return o, nil
}

func defaultShell(goos string) string {
	if goos == "windows" {
		return "powershell"
	}
	return "sh"
}

// validate runs the same parsers the server runs at start (config.ParseShadow,
// config.ParseWatch) over the overrides.
func (o shadowOpts) validate() error {
	if _, err := config.ParseShadow(config.Config{
		ShadowGrace: o.Grace, ShadowTokenBudget: o.TokenBudget, ShadowMaxTurns: o.MaxTurns,
	}); err != nil {
		return fmt.Errorf("devctl: %w", err)
	}
	if _, err := config.ParseWatch(config.Config{
		WatchMode: string(config.WatchRooms), WatchInterval: o.WatchInterval, WatchCooldown: o.WatchCooldown,
	}); err != nil {
		return fmt.Errorf("devctl: %w", err)
	}
	return nil
}

// envPairs is the set overrides as env var -> value.
func (o shadowOpts) envPairs() map[string]string {
	m := map[string]string{}
	for k, v := range map[string]string{
		"PIPEROOM_SHADOW_GRACE":        o.Grace,
		"PIPEROOM_SHADOW_TOKEN_BUDGET": o.TokenBudget,
		"PIPEROOM_SHADOW_MAX_TURNS":    o.MaxTurns,
		"PIPEROOM_WATCH_INTERVAL":      o.WatchInterval,
		"PIPEROOM_WATCH_COOLDOWN":      o.WatchCooldown,
	} {
		if v = strings.TrimSpace(v); v != "" {
			m[k] = v
		}
	}
	return m
}

// hostEnvLines is what to set to run `go run ./cmd/server` on the host against the
// dev stack: the shadow owners file, watcher rooms, and the overrides. The dev
// stack's addresses are the config defaults, so they are not repeated. The API key is
// never printed: it must already be in the environment.
func hostEnvLines(root string, o shadowOpts) []string {
	pairs := o.envPairs()
	// The same watcher timings the compose service uses; the server's own defaults
	// (60s, 60m) would make a first room slow and a second round open none for an hour.
	for k, v := range map[string]string{"PIPEROOM_WATCH_INTERVAL": devWatchInterval, "PIPEROOM_WATCH_COOLDOWN": devWatchCooldown} {
		if _, set := pairs[k]; !set {
			pairs[k] = v
		}
	}
	pairs["OWNERS_FILE"] = filepath.Join(root, filepath.FromSlash(shadowOwnersRel))
	pairs["PIPEROOM_WATCH_MODE"] = string(config.WatchRooms)
	keys := make([]string, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	lines := make([]string, 0, len(keys))
	for _, k := range keys {
		if o.Shell == "powershell" {
			lines = append(lines, fmt.Sprintf("$env:%s = %s", k, psQuote(pairs[k])))
		} else {
			lines = append(lines, fmt.Sprintf("export %s=%s", k, shQuote(pairs[k])))
		}
	}
	return lines
}

func shQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// psQuote single-quotes s for PowerShell, which treats the typographic single quotes
// (U+2018, U+2019, U+201A, U+201B) as quote characters too; each is doubled.
func psQuote(s string) string {
	var b strings.Builder
	b.WriteByte('\'')
	for _, r := range s {
		switch r {
		case '\'', '\u2018', '\u2019', '\u201a', '\u201b':
			b.WriteRune(r)
		}
		b.WriteRune(r)
	}
	b.WriteByte('\'')
	return b.String()
}

// requireAPIKey fails when the agent has no key. Only the presence is checked; the
// value is never read into a message.
func requireAPIKey(getenv func(string) string) error {
	if strings.TrimSpace(getenv(apiKeyEnv)) == "" {
		return fmt.Errorf("devctl: %s is not set: shadow mode runs the real agent, so export your key first (devctl never prints or stores it)", apiKeyEnv)
	}
	return nil
}

// requireDevAck fails unless PIPEROOM_DEV_ACK holds the exact acknowledgement sentence.
// The container listens on 0.0.0.0 in dev auth mode (login has no password), which the
// server refuses to do without it; checking here turns a restart loop into one clear
// message. devctl never sets the value for you: stating it is the point.
func requireDevAck(getenv func(string) string) error {
	given, err := config.ParseDevAck(config.Config{DevAck: getenv(devAckEnv)})
	if err != nil {
		return fmt.Errorf("devctl: %w", err)
	}
	if !given {
		return fmt.Errorf("devctl: %s is not set: the shadow server uses dev login, which has no password. If that is fine on this machine, export %s=%s and run again (the container is published on 127.0.0.1 only)", devAckEnv, devAckEnv, config.DevAckValue)
	}
	return nil
}

// shadowPipelines loads the shadow owners file and returns its shadow pipeline ids.
// An error here means `shadow up` would start a server that never opens a shadow room.
func shadowPipelines(path string) ([]string, error) {
	own, err := owners.Load(path)
	if err != nil {
		return nil, fmt.Errorf("devctl: %w", err)
	}
	var ids []string
	for _, p := range own.Pipelines {
		if p.Shadow() {
			ids = append(ids, p.ID)
		}
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("devctl: %s has no pipeline with shadow: {enabled: true}", path)
	}
	return ids, nil
}

// shadowComposeArgs is the docker compose argument list for the shadow server.
func shadowComposeArgs(verb ...string) []string {
	return append([]string{"--profile", shadowProfile}, verb...)
}

// shadowCmd implements `devctl shadow`. See dev/shadow.md.
func shadowCmd(ctx context.Context, st *stack, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New(shadowUsage)
	}
	sub, rest := args[0], args[1:]
	ownersPath := filepath.Join(st.root, filepath.FromSlash(shadowOwnersRel))
	switch sub {
	case "up":
		owner, rest, err := splitOwner(rest)
		if err != nil {
			return err
		}
		o, err := parseShadowFlags(sub, rest)
		if err != nil {
			return err
		}
		ids, err := shadowPipelines(ownersPath)
		if err != nil {
			return err
		}
		if err := requireAPIKey(os.Getenv); err != nil {
			return err
		}
		if err := requireDevAck(os.Getenv); err != nil {
			return err
		}
		if err := requireLock(owner); err != nil {
			return err
		}
		if err := st.compose(ctx, o.envPairs(), shadowComposeArgs("up", "-d", "--build", "--wait", shadowService)...); err != nil {
			return err
		}
		fmt.Fprintf(out, "shadow server is up at %s (shadow pipelines: %s)\n", shadowURL, strings.Join(ids, ", "))
		fmt.Fprintln(out, "next: open it, choose a user, then `devctl break connector-down --owner <tag>`; the watcher opens a shadow room within a few watcher intervals (the dev settings use a 30s `for:` and a 15s interval). See dev/shadow.md.")
		return nil
	case "down":
		owner, rest, err := splitOwner(rest)
		if err != nil {
			return err
		}
		if len(rest) != 0 {
			return errors.New("usage: devctl shadow down")
		}
		if err := requireLock(owner); err != nil {
			return err
		}
		return st.compose(ctx, nil, shadowComposeArgs("rm", "-sf", shadowService)...)
	case "status":
		if len(rest) != 0 {
			return errors.New("usage: devctl shadow status")
		}
		ids, err := shadowPipelines(ownersPath)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "shadow pipelines in %s: %s\n", shadowOwnersRel, strings.Join(ids, ", "))
		if _, err := st.do(ctx, http.MethodGet, shadowURL+"/readyz", nil, http.StatusOK); err != nil {
			fmt.Fprintf(out, "server %s: not ready (%v)\n", shadowURL, err)
			return errUnhealthy
		}
		fmt.Fprintf(out, "server %s: ready\n", shadowURL)
		return nil
	case "env":
		o, err := parseShadowFlags(sub, rest)
		if err != nil {
			return err
		}
		if _, err := shadowPipelines(ownersPath); err != nil {
			return err
		}
		fmt.Fprintf(out, "# then, from the repo root, with %s already set: go run ./cmd/server\n", apiKeyEnv)
		for _, l := range hostEnvLines(st.root, o) {
			fmt.Fprintln(out, l)
		}
		return nil
	}
	return fmt.Errorf("devctl: shadow: unknown subcommand %q\n%s", sub, shadowUsage)
}
