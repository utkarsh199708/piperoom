//go:build integration

package store_test

import (
	"testing"
	"time"

	"piperoom/internal/store"
	"piperoom/internal/store/storetest"
)

// The shared store checks memstore runs in its unit tests
// (internal/store/memstore/conformance_test.go), against Postgres.
func TestSinceConformance(t *testing.T) {
	s, _ := requireStore(t)
	storetest.RunSince(t, s)
}

// A deleted room ends its subscriptions at the room feed's next read; short
// polls make that read come within the test's time.
func TestSubscribeEndsOnDelete(t *testing.T) {
	_, _, schema := requireStoreSchema(t)
	s := openReplica(t, schema, store.Options{PollInterval: 50 * time.Millisecond, SafetyPoll: 100 * time.Millisecond})
	storetest.RunSubscribeEndsOnDelete(t, s, 10*time.Second)
}
