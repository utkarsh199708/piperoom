package connprobe

import "context"

// Placeholder until the probe is written; the probe's own file replaces this
// file in the same change.
func probeSchemaRegistry(_ context.Context, t Target, _ *Guard) Result { return notImplemented(t.Kind) }
