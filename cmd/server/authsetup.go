package main

import (
	"log/slog"
	"strings"

	"piperoom/internal/auth"
	"piperoom/internal/config"
)

// devNonLoopbackWarning is the loud warning for dev auth on a reachable
// address (docs/design/auth.md §5.1). httpapi logs the same text on every dev
// login.
const devNonLoopbackWarning = "dev auth on a non-loopback address: anyone who can reach this port can act as any user"

// authSetup parses and checks the login settings before anything else starts.
//
//   - PIPEROOM_AUTH_MODE must be dev or oidc; oidc needs an https
//     PIPEROOM_PUBLIC_URL, an issuer, a client id and a secret (unless
//     PIPEROOM_OIDC_PUBLIC_CLIENT=1). Validation only: the login is PR 3.
//   - dev mode refuses a non-loopback PIPEROOM_HTTP_ADDR unless
//     PIPEROOM_DEV_ALLOW_NONLOOPBACK=1 or the deprecated --allow-remote flag
//     is given; both start with a loud warning.
//   - oidc mode ignores both switches, with a warning: OIDC is the guard there.
//   - --demo forces dev.
func authSetup(cfg config.Config, allowRemoteFlag, demo bool, log *slog.Logger) (auth.Settings, error) {
	if demo && strings.ToLower(strings.TrimSpace(cfg.AuthMode)) == auth.ModeOIDC {
		log.Warn("server: --demo forces PIPEROOM_AUTH_MODE=dev")
		cfg.AuthMode = auth.ModeDev
	}
	settings, err := auth.ParseSettings(cfg)
	if err != nil {
		return auth.Settings{}, err
	}

	envSwitch := strings.TrimSpace(cfg.DevAllowNonLoopback) == "1"
	if allowRemoteFlag {
		log.Warn("server: --allow-remote is deprecated, set PIPEROOM_DEV_ALLOW_NONLOOPBACK=1")
	}

	if settings.IsOIDC() {
		if envSwitch || allowRemoteFlag {
			log.Warn("server: PIPEROOM_DEV_ALLOW_NONLOOPBACK and --allow-remote are ignored in oidc mode")
		}
		return settings, nil
	}

	remote, err := checkListenAddr(cfg.HTTPAddr, envSwitch || allowRemoteFlag)
	if err != nil {
		return auth.Settings{}, err
	}
	if remote {
		log.Warn("server: "+devNonLoopbackWarning, "addr", cfg.HTTPAddr)
		settings.DevNonLoopback = true
	}
	return settings, nil
}
