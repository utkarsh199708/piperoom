package main

import (
	"fmt"
	"log/slog"
	"strings"

	"piperoom/internal/auth"
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

// oidcSetup builds the OIDC relying party for oidc mode (nil in dev mode). It
// does no network I/O: discovery is started by the caller and retried in the
// background, so an IdP that is down at start-up is not fatal (design section
// 7). The redirect URI is the PIPEROOM_PUBLIC_URL origin plus /auth/callback.
// The client secret goes into the config and nowhere else; auth.OIDCConfig
// redacts it when printed.
func oidcSetup(cfg config.Config, settings auth.Settings, public httpapi.PublicOrigin, log *slog.Logger) (*auth.OIDC, error) {
	if !settings.IsOIDC() {
		return nil, nil
	}
	if public.Host == "" {
		return nil, fmt.Errorf("auth: PIPEROOM_AUTH_MODE=oidc needs PIPEROOM_PUBLIC_URL")
	}
	scopes := strings.Fields(cfg.OIDCScopes)
	if len(scopes) == 0 {
		scopes = strings.Fields(config.DefaultOIDCScopes)
	}
	o, err := auth.NewOIDC(auth.OIDCConfig{
		Issuer:       strings.TrimSpace(cfg.OIDCIssuer),
		ClientID:     strings.TrimSpace(cfg.OIDCClientID),
		ClientSecret: cfg.OIDCClientSecret,
		RedirectURL:  public.Scheme + "://" + public.Host + "/auth/callback",
		Scopes:       scopes,
		GroupsClaim:  strings.TrimSpace(cfg.OIDCGroupsClaim),
	}, log)
	if err != nil {
		return nil, err
	}
	if cfg.OIDCClientSecret == "" {
		log.Warn("server: oidc public client: no client secret, PKCE only")
	}
	if strings.TrimSpace(cfg.OIDCGroupsClaim) == "" {
		log.Info("server: PIPEROOM_OIDC_GROUPS_CLAIM is empty: groups give no rights")
	}
	return o, nil
}

// demoPresenterID is the demo's presenter and driver (docs/demo/schema-change.md
// roles, and the first user of room.DefaultUsers). --solo gives this user the
// solo ceremony.
const demoPresenterID = "arjun"

// soloSetup runs the solo-mode start check (design section 5.3). It returns the
// trimmed solo user ("" when solo mode is off). A violation refuses the start;
// a pass logs a warning, because solo mode drops the second pair of eyes.
// soloFlag is the --solo flag: it gives the demo presenter the solo ceremony
// even though owners.yaml names other principals (a try-it convenience, not the
// production single-owner rule), so it only checks that the user is known.
func soloSetup(cfg config.Config, users *room.Users, own *owners.Owners, soloFlag bool, log *slog.Logger) (string, error) {
	solo := strings.TrimSpace(cfg.SoloUser)
	if solo == "" {
		return "", nil
	}
	if soloFlag {
		if err := checkDemoSoloUser(solo, users); err != nil {
			return "", fmt.Errorf("server: %w", err)
		}
	} else if err := room.CheckSoloUser(solo, users, own); err != nil {
		return "", fmt.Errorf("server: %w", err)
	}
	log.Warn("server: SOLO MODE: one user may approve every layer, with a typed confirm; use it only for a single owner", "user", solo)
	return solo, nil
}

// checkDemoSoloUser is the --solo (demo and quickstart) check: the presenter
// must be a known user. owners.yaml may name other principals (the demo's
// owners file names four owners), because --solo exists to show the solo
// ceremony on the try-it track, not to relax the production single-owner rule.
func checkDemoSoloUser(solo string, users *room.Users) error {
	if users == nil {
		return fmt.Errorf("solo mode: --solo user %q but there is no users file", solo)
	}
	if _, ok := users.Get(solo); !ok {
		return fmt.Errorf("solo mode: --solo user %q is not in the users file", solo)
	}
	return nil
}
