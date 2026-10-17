package connprobe

// Placeholders so the package builds while the probes are written. Each probe
// moves to its own file (postgres.go, warehouse.go, kafka.go, connect.go,
// registry.go) and its stub here is deleted in the same change.

import "context"

func notImplemented(k Kind) Result {
	return Result{Kind: k, Checks: []Check{{ID: "probe", Status: StatusSkip, Detail: "probe not implemented yet"}}}
}

func probeSourceDB(_ context.Context, t Target, _ *Guard) Result  { return notImplemented(t.Kind) }
func probeWarehouse(_ context.Context, t Target, _ *Guard) Result { return notImplemented(t.Kind) }
func probeKafka(_ context.Context, t Target, _ *Guard) Result     { return notImplemented(t.Kind) }
func probeConnect(_ context.Context, t Target, _ *Guard) Result   { return notImplemented(t.Kind) }
func probeSchemaRegistry(_ context.Context, t Target, _ *Guard) Result {
	return notImplemented(t.Kind)
}
