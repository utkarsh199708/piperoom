package main

import (
	"fmt"
	"log/slog"

	"piperoom/internal/actions/fixpr"
	"piperoom/internal/clients/forge"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

// The wiring of the setup room's pipeline_register step (U1-14, KAN-618):
// room.Deps.ReloadOwners, OnOwnersReloaded, OwnersNow and SetupForge. Without
// them pipeline_register refuses every registration.

// registerWiring is what cmd/server hands the room service and the fleet for
// pipeline registration.
type registerWiring struct {
	// own is the owners set the server starts with: the OWNERS_FILE pipelines
	// plus every OWNERS_DIR file when a Live holder is in use.
	own *owners.Owners
	// live is the reloadable holder; nil when OWNERS_DIR is unset or setup is
	// off (nothing can register then).
	live *owners.Live
	// forge is the repos: registration path; nil without FORGE_KIND.
	forge *room.SetupForge
}

// ownersNow is the current owners config, read on every request.
func (w registerWiring) ownersNow() *owners.Owners {
	if w.live != nil {
		return w.live.Load()
	}
	return w.own
}

// setupRegisterWiring builds the registration wiring. With setup on and
// OWNERS_DIR set, the owners are loaded through an owners.Live (the main file
// plus the per-pipeline files a registration writes); otherwise own is used
// as given. The forge path is built whenever setup is on and FORGE_KIND is
// set; its writer exists only with write mode on (setup needs it anyway).
func setupRegisterWiring(cfg config.Config, own *owners.Owners, setup config.Setup, writeOn bool) (registerWiring, error) {
	w := registerWiring{own: own}
	if !setup.Enabled {
		return w, nil
	}
	if setup.OwnersDir != "" {
		live, err := owners.NewLive(cfg.OwnersFile, setup.OwnersDir)
		if err != nil {
			return registerWiring{}, fmt.Errorf("server: load owners %s with OWNERS_DIR %s: %w", cfg.OwnersFile, setup.OwnersDir, err)
		}
		w.live, w.own = live, live.Load()
	}
	sf, err := setupForge(cfg, writeOn)
	if err != nil {
		return registerWiring{}, err
	}
	w.forge = sf
	return w, nil
}

// apply sets the registration fields of d.
func (w registerWiring) apply(d room.Deps, log *slog.Logger) room.Deps {
	d.OwnersNow = w.ownersNow
	d.SetupForge = w.forge
	if w.live != nil {
		d.ReloadOwners = w.live.Reload
		// The watcher and the per-pipeline client stacks are built once at
		// start: a registered pipeline shows in the fleet at once and is
		// watched after the next restart (KAN-697).
		d.OnOwnersReloaded = func() {
			log.Info("server: owners reloaded after a registration; the new pipeline is watched after the next restart")
		}
	}
	return d
}

// setupForge builds the repos: registration path: the PR-opening action over
// the GitHub App and its reader. FORGE_KIND empty: nil (a registration with
// repos: then refuses, as the room says).
func setupForge(cfg config.Config, writeOn bool) (*room.SetupForge, error) {
	f, err := config.ParseForge(cfg)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	if f.Kind == config.ForgeKindOff {
		return nil, nil
	}
	key, err := forge.LoadPrivateKeyFile(f.PrivateKeyFile, slog.Default())
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	c, err := forge.New(forge.Config{APIURL: f.APIURL, AppID: f.AppID, InstallationID: f.InstallationID, Key: key})
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	d := fixpr.Deps{Reader: c, WebHost: c.WebHost(), PublicURL: cfg.PublicURL}
	if writeOn {
		w, err := forge.NewWriter(c)
		if err != nil {
			return nil, fmt.Errorf("server: forge writer: %w", err)
		}
		d.Writer = w
	}
	return &room.SetupForge{PR: fixpr.New(d), Reader: c}, nil
}
