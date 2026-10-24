package main

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// autoApproveDelay reads PIPEROOM_AUTO_APPROVE_DELAY (config.ParseAutoApproveDelay,
// P0 item 2) before anything is opened: a typo stops the start.
func autoApproveDelay(cfg config.Config) (time.Duration, error) {
	d, err := config.ParseAutoApproveDelay(cfg)
	if err != nil {
		return 0, fmt.Errorf("server: %w", err)
	}
	return d, nil
}

// checkAutoApprove re-validates every owners.yaml auto_approve: entry once the
// action registries exist (tasks/P2-solo-ceremony.md P2b): each entry must
// name an action of the proposal registry, the one the agent proposes from,
// which holds every action whether or not write mode is on (with write mode
// off its actions only have read-only clients; nothing is removed), and no
// entry may be a schema_* action, which the policy does not cover in this
// wave. A typo such as connect_restart_taks fails here instead of becoming a
// policy that never matches. An entry naming an actions.Deferred action is
// refused too (Q7a). With the policy off (delay 0) the lists are
// still checked, so turning it on later cannot surface an old typo.
func checkAutoApprove(own *owners.Owners, regs []*actions.Registry, delay time.Duration, log *slog.Logger) error {
	var names, deferred []string
	for _, r := range regs {
		if r == nil {
			continue
		}
		for _, a := range r.All() {
			names = append(names, a.Name())
			if _, ok := a.(actions.Deferred); ok {
				deferred = append(deferred, a.Name())
			}
		}
	}
	// A Deferred action (a fix pull request) is never approved by a policy:
	// this load-time wall and the room's schedule check are the two walls of
	// docs/design/fix-pr.md §7, so one bug is not enough.
	if err := own.ValidateAutoApproveDeferred(names, deferred); err != nil {
		return fmt.Errorf("server: %w", err)
	}
	var on []string
	for i := range own.Pipelines {
		p := &own.Pipelines[i]
		for _, a := range p.AutoApproveList() {
			if strings.HasPrefix(a, "schema_") {
				return fmt.Errorf("server: owners: pipeline %q: auto_approve: %q is a schema change, which the policy never approves; approve it by hand", p.ID, a)
			}
		}
		if len(p.AutoApprove) > 0 {
			on = append(on, p.ID)
		}
	}
	switch {
	case len(on) == 0:
	case delay <= 0:
		log.Info("server: auto-approve policy off (PIPEROOM_AUTO_APPROVE_DELAY=0); owners.yaml auto_approve: lists are ignored", "pipelines", on)
	default:
		log.Info("server: auto-approve policy on", "pipelines", on, "delay", delay.String())
	}
	return nil
}
