//go:build manual

package aiven

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/setup/targets/managed/replay"
)

// TestManualSandbox records the read cassettes on the Aiven sandbox
// project (docs/design/setup-managed.md §6) into PIPEROOM_AIVEN_SANDBOX_OUT
// for a lead to compare with the hand-written ones; it confirms the
// response shapes this package reads defensively (available-connectors,
// connector status). The token comes from the shell's environment.
//
//	PIPEROOM_AIVEN_SANDBOX_TOKEN=... PIPEROOM_AIVEN_SANDBOX_PROJECT=... \
//	PIPEROOM_AIVEN_SANDBOX_SERVICE=... PIPEROOM_AIVEN_SANDBOX_OUT=/tmp/aiven-cassettes \
//	  go test -tags manual -run TestManualSandbox ./internal/clients/aiven/
func TestManualSandbox(t *testing.T) {
	token, project := os.Getenv("PIPEROOM_AIVEN_SANDBOX_TOKEN"), os.Getenv("PIPEROOM_AIVEN_SANDBOX_PROJECT")
	service, out := os.Getenv("PIPEROOM_AIVEN_SANDBOX_SERVICE"), os.Getenv("PIPEROOM_AIVEN_SANDBOX_OUT")
	if token == "" || project == "" || service == "" || out == "" {
		t.Skip("set PIPEROOM_AIVEN_SANDBOX_TOKEN, _PROJECT, _SERVICE and _OUT")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	scrub := replay.Scrubber{Names: map[string]string{project: "project-0", service: "connect-0", token: replay.Redacted}}
	rec := replay.NewRecorder(http.DefaultTransport, scrub, replay.ProviderAiven, "live_reads", "sandbox run "+time.Now().UTC().Format("2006-01-02"), time.Now())
	c, err := New(Config{Token: netsec.Secret(token), Project: project, Service: service, Transport: rec})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Service(ctx); err != nil {
		t.Errorf("Service: %v", err)
	}
	if _, err := c.Plugins(ctx); err != nil {
		t.Errorf("Plugins: %v", err)
	}
	names, err := c.ListConnectors(ctx)
	if err != nil {
		t.Errorf("ListConnectors: %v", err)
	}
	if len(names) > 0 {
		if _, err := c.Status(ctx, names[0]); err != nil {
			t.Errorf("Status: %v", err)
		}
	}
	if err := replay.Save(rec.Cassette(), filepath.Join(out, "live_reads.json")); err != nil {
		t.Fatal(err)
	}
}
