package main

import (
	"log/slog"

	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/watch"
)

// checkWatchLag logs an ERROR for every pipeline that has a consumer-group
// signal on and consumer_groups declared, but whose client set has no lag
// reader. The watcher itself only logs one INFO-level line for the whole
// process and the signals then stay unknown for ever, which looks like "all
// groups healthy". It does not refuse the start: a set without a Kafka client
// is a valid setup for the other signals. It returns the pipelines it named.
func checkWatchLag(log *slog.Logger, mode config.WatchMode, own *owners.Owners, clientsFor func(string) watch.PipelineClients) []string {
	if mode == config.WatchOff || own == nil || clientsFor == nil {
		return nil
	}
	var bad []string
	for _, p := range own.Pipelines {
		cfg := p.Watch()
		if !cfg.Enabled || len(p.ConsumerGroups) == 0 {
			continue
		}
		if !(cfg.ConsumerLag.Enabled || cfg.GroupUnstable.Enabled || cfg.OffsetBelowLogStart.Enabled) {
			continue
		}
		if clientsFor(p.ID).Lag != nil {
			continue
		}
		bad = append(bad, p.ID)
		log.Error("server: watch: no consumer-group lag reader for this pipeline; consumer_lag, group_unstable and offset_below_log_start can never fire",
			"pipeline", p.ID, "groups", len(p.ConsumerGroups), "reason", "the pipeline's Kafka client has no Kafka client")
	}
	return bad
}
