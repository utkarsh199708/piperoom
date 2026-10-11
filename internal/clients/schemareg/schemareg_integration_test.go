//go:build integration

package schemareg

import (
	"context"
	"net/http"
	"testing"
	"time"

	"piperoom/internal/testenv"
)

// Registers two versions under a temp subject and diffs them. The temp subject
// persists in the dev registry (the contract has no subject delete).
func TestIntegrationRegisterAndVersions(t *testing.T) {
	cfg := testenv.Require(t)
	c := New(cfg.SchemaRegistryURL, &http.Client{Timeout: 5 * time.Second})
	subject := testenv.Name("it-schemareg")

	// Register under NONE so the two test schemas can differ freely.
	if err := c.SetCompatibility(context.Background(), subject, "NONE"); err != nil {
		t.Fatalf("SetCompatibility: %v", err)
	}

	v1 := `{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}`
	if _, err := c.Register(context.Background(), subject, v1); err != nil {
		t.Fatalf("register v1: %v", err)
	}
	v2 := `{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"extra","type":"string"}]}`
	if _, err := c.Register(context.Background(), subject, v2); err != nil {
		t.Fatalf("register v2: %v", err)
	}

	versions, err := c.Versions(context.Background(), subject)
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("want 2 versions, got %v", versions)
	}
	latest, err := c.Schema(context.Background(), subject, -1)
	if err != nil {
		t.Fatalf("Schema latest: %v", err)
	}
	t.Logf("subject %s: versions %v, latest v%d id %d", subject, versions, latest.Version, latest.ID)
}
