package memstore

import (
	"testing"
	"time"

	"piperoom/internal/store/storetest"
)

// The shared store checks; *store.PG runs the same ones under the
// integration tag (internal/store/conformance_integration_test.go).
func TestSinceConformance(t *testing.T) { storetest.RunSince(t, New()) }

func TestSubscribeEndsOnDelete(t *testing.T) {
	storetest.RunSubscribeEndsOnDelete(t, New(), 5*time.Second)
}
