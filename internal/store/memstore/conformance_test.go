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

func TestClientIDUniqueConformance(t *testing.T) { storetest.RunClientIDUnique(t, New()) }

// The watch sample history (X3a TC1); Postgres runs it under the integration tag.
func TestSamplesConformance(t *testing.T) { storetest.RunSamples(t, New(), "orders") }
