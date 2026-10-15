package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"piperoom/internal/topology"
)

// pipelineMapTimeout bounds the live topology read, like the room's.
const pipelineMapTimeout = 10 * time.Second

// pipelineMapFor renders the pipeline's live dependency graph for the model's
// first prompt, as the room does. The fixed orders map is only the fallback for
// fake tools or a pipeline whose graph cannot be built: it names the dev
// stack's components, which are wrong for any other pipeline, and it has none
// of the from:/depends on: links the prompt tells the model to walk.
func pipelineMapFor(ctx context.Context, topo topology.Builder, pipeline string, log *slog.Logger) string {
	if topo != nil {
		tctx, cancel := context.WithTimeout(ctx, pipelineMapTimeout)
		defer cancel()
		g, err := topo.Build(tctx, pipeline)
		if err == nil {
			if m := topo.Render(g); m != "" {
				return m
			}
		} else {
			log.Warn("investigate: topology build failed; using the static map", "pipeline", pipeline, "err", err)
		}
	}
	return fmt.Sprintf(`pipeline %s
postgres: public.orders  ->  connect: pg-orders-connector  ->  topic: pg.public.orders  ->  consumer: order-sync  ->  warehouse: analytics.orders`, pipeline)
}
