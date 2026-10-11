//go:build integration

package schemareg

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"piperoom/internal/testenv"
)

// deleteSubject removes a test subject (soft then permanent). The client
// interface has no delete method, so this is a test-only raw HTTP helper.
func deleteSubject(t *testing.T, baseURL, subject string) {
	t.Helper()
	for _, perm := range []string{"", "?permanent=true"} {
		u := baseURL + "/subjects/" + url.PathEscape(subject) + perm
		req, err := http.NewRequest(http.MethodDelete, u, nil)
		if err != nil {
			t.Fatalf("delete request: %v", err)
		}
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			t.Fatalf("delete %s: %v", u, err)
		}
		resp.Body.Close()
	}
}

// Registers two versions under a temp subject and diffs them, then cleans up.
func TestIntegrationRegisterAndVersions(t *testing.T) {
	cfg := testenv.Require(t)
	c := New(cfg.SchemaRegistryURL, &http.Client{Timeout: 5 * time.Second})
	subject := testenv.Name("it-schemareg")
	t.Cleanup(func() { deleteSubject(t, cfg.SchemaRegistryURL, subject) })

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
