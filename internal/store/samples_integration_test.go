//go:build integration

package store_test

import (
	"testing"

	"piperoom/internal/store/storetest"
	"piperoom/internal/testenv"
)

// The watch sample history of migration 00018 against Postgres (X3a TC1): the
// same checks memstore runs, in a fresh schema, plus the retention purge.
func TestSamplesConformance(t *testing.T) {
	s, _, _ := requireStoreSchema(t)
	storetest.RunSamples(t, s, testenv.Name("pipe"))
}
