package main

// Tenancy wiring (docs/design/tenancy.md): the PIPEROOM_TENANCY_MODE check,
// the start-up log of the chosen model, and the adapters that hand each room
// and each watched pipeline the client set of its own pipeline.

import (
	"fmt"
	"log/slog"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/watch"
	"piperoom/internal/wiring"
)

// tenancySetup checks PIPEROOM_TENANCY_MODE against owners.yaml and logs the
// mode. A mismatch refuses the start: an operator who set teams must never get
// a server that shows every room to everyone because the tenants: block was
// lost. slackOn adds the warning that the one Slack channel sees every tenant.
func tenancySetup(cfg config.Config, own *owners.Owners, slackOn bool, log *slog.Logger) (string, error) {
	want, err := config.ParseTenancyMode(cfg)
	if err != nil {
		return "", fmt.Errorf("server: %w", err)
	}
	on := own.TenancyOn()
	switch {
	case want == config.TenancyTeams && !on:
		return "", fmt.Errorf("server: PIPEROOM_TENANCY_MODE=teams but the owners file has no tenants: block (docs/design/tenancy.md)")
	case want == config.TenancySingle && on:
		return "", fmt.Errorf("server: PIPEROOM_TENANCY_MODE=single but the owners file has a tenants: block; remove it or set teams")
	}
	if !on {
		log.Info("server: tenancy", "mode", config.TenancySingle, "configured", want, "filter", "off: every signed-in user sees every room")
		return config.TenancySingle, nil
	}
	log.Info("server: tenancy", "mode", config.TenancyTeams, "configured", want, "tenants", own.TenantNames(),
		"filter", "on: a user sees only the rooms of their tenants")
	for _, p := range own.Pipelines {
		if layers := own.InheritedLayers(p.ID); len(layers) > 0 {
			log.Warn("server: tenancy: the top-level layers: owners of these layers own them in this pipeline too, so they are in its tenant; give the pipeline its own owners: block to keep tenants apart",
				"pipeline", p.ID, "tenant", p.TenantName(), "layers", layers)
		}
	}
	if slackOn {
		log.Warn("server: tenancy: the Slack mirror posts every tenant's rooms to one channel (SLACK_CHANNEL_ID)")
	}
	return config.TenancyTeams, nil
}

// logStacks logs which pipelines use their own connections and which the
// process-wide settings. Names only: never a value.
func logStacks(log *slog.Logger, st *wiring.Stacks) {
	var own, proc []string
	for _, s := range st.All() {
		if s.Name == wiring.ProcessWideStack {
			proc = append(proc, s.Pipelines...)
		} else {
			own = append(own, s.Pipelines...)
		}
	}
	log.Info("server: client sets", "sets", len(st.All()), "own_connections", own, "process_wide", proc)
}

// stackFinder is the part of *wiring.Stacks the adapters use.
type stackFinder interface {
	For(pipelineID string) *wiring.Stack
}

// roomStacks adapts the client sets to room.Deps.StackFor.
func roomStacks(st stackFinder) func(pipelineID string) room.PipelineStack {
	return func(pipelineID string) room.PipelineStack {
		s := st.For(pipelineID)
		return room.PipelineStack{Tools: s.Tools, Actions: s.Actions}
	}
}

// watchClients adapts the client sets to watch.Deps.ClientsFor. The empty set
// has no clients, so its pipelines read as unknown.
func watchClients(st stackFinder) func(pipelineID string) watch.PipelineClients {
	return func(pipelineID string) watch.PipelineClients {
		s := st.For(pipelineID)
		pc := watch.PipelineClients{Set: s.Name}
		if s.Clients != nil {
			pc.Source, pc.Connect = s.Clients.Source, s.Clients.Connect
			// dlq_growth needs DLQ offsets; a Kafka client without them leaves
			// the signal off for this set (watch.Deps.Kafka).
			if r, ok := s.Clients.Kafka.(clients.DLQReader); ok {
				pc.Kafka = r
			}
			// The consumer-group signals read GroupLag from the same client;
			// without this they stay unknown ("no consumer-group lag reader").
			if l, ok := s.Clients.Kafka.(watch.LagReader); ok {
				pc.Lag = l
			}
			// The change feed (F8, Q5 T2): subject versions from the set's
			// registry and DDL audit rows from its source. Without them
			// pipeline_changes never gets a schema or DDL row.
			if s.Clients.Registry != nil {
				pc.Registry = s.Clients.Registry
			}
			if d, ok := s.Clients.Source.(watch.DDLReader); ok {
				pc.DDL = d
			}
		}
		return pc
	}
}

// wireWatchChanges turns the watcher's change feed on when the room store can
// hold pipeline_changes rows (Q5 T2): without it what_changed reads an empty
// log and says "no recorded change", which misleads.
func wireWatchChanges(d *watch.Deps, st any) {
	if w, ok := st.(clients.ChangeWriter); ok {
		d.Changes = w
	}
}
