package main

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"piperoom/internal/auth"
	"piperoom/internal/config"
)

// devNonLoopbackWarning is the loud warning for dev auth on a reachable
// address (docs/design/auth.md §5.1). httpapi logs the same text on every dev
// login.
const devNonLoopbackWarning = "dev auth on a non-loopback address: anyone who can reach this port can act as any user"

// errDevAckMissing refuses dev auth off loopback without the written
// acknowledgement (config.ParseDevAck).
var errDevAckMissing = errors.New("server: dev auth on a non-loopback address also needs PIPEROOM_DEV_ACK=" + config.DevAckValue + " (dev login has no password); set it in your own deployment, or use PIPEROOM_AUTH_MODE=oidc")

// authSetup parses and checks the login settings before anything else starts.
//
//   - PIPEROOM_AUTH_MODE must be dev or oidc; oidc needs an https
//     PIPEROOM_PUBLIC_URL, an issuer, a client id and a secret (unless
//     PIPEROOM_OIDC_PUBLIC_CLIENT=1). The login itself is built by oidcSetup.
//   - dev mode refuses a non-loopback PIPEROOM_HTTP_ADDR unless
//     PIPEROOM_DEV_ALLOW_NONLOOPBACK=1 or the deprecated --allow-remote flag
//     is given; both start with a loud warning.
//   - outside --demo, a dev server that does listen off loopback also needs
//     PIPEROOM_DEV_ACK=config.DevAckValue: the image sets only the switch, so
//     the operator states in their own deployment that dev login has no
//     password. Any other non-empty value refuses the start.
//   - oidc mode ignores both switches and the acknowledgement, with a
//     warning: OIDC is the guard there.
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
		if envSwitch || allowRemoteFlag || strings.TrimSpace(cfg.DevAck) != "" {
			log.Warn("server: PIPEROOM_DEV_ALLOW_NONLOOPBACK, PIPEROOM_DEV_ACK and --allow-remote are ignored in oidc mode")
		}
		return settings, nil
	}
	ack, err := config.ParseDevAck(cfg)
	if err != nil {
		return auth.Settings{}, fmt.Errorf("server: %w", err)
	}

	remote, err := checkListenAddr(cfg.HTTPAddr, envSwitch || allowRemoteFlag)
	if err != nil {
		return auth.Settings{}, err
	}
	if remote && !demo && !ack {
		return auth.Settings{}, errDevAckMissing
	}
	if remote {
		log.Warn("server: "+devNonLoopbackWarning, "addr", cfg.HTTPAddr)
		settings.DevNonLoopback = true
	}
	return settings, nil
}
