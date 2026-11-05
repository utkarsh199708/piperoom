//go:build integration

package store_test

import (
	"testing"

	"piperoom/internal/store/storetest"
	"piperoom/internal/testenv"
)

// The watch_status table of migration 00019 and the fleet change pages over
// pipeline_changes against Postgres (X5a): the same checks memstore runs, in a
// fresh schema.
func TestFleetConformance(t *testing.T) {
	s, _, _ := requireStoreSchema(t)
	storetest.RunFleet(t, s, testenv.Name("fleet"))
}
