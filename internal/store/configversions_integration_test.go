//go:build integration

package store_test

import (
	"testing"

	"piperoom/internal/store/storetest"
	"piperoom/internal/testenv"
)

// The connector config history of migration 00020 against Postgres (X5c TC1,
// TC3): the same checks memstore runs, in a fresh schema, plus the retention
// purge.
func TestConfigVersionsConformance(t *testing.T) {
	s, _, _ := requireStoreSchema(t)
	storetest.RunConfigVersions(t, s, testenv.Name("pipe"))
}
