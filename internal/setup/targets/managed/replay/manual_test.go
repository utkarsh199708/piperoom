//go:build manual

package replay

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestManualRecordSmoke records one GET of PIPEROOM_REPLAY_URL through the
// recorder and writes the scrubbed cassette to PIPEROOM_REPLAY_OUT (a
// directory). It is the harness's own smoke test against a live endpoint:
// the provider re-recordings live next to each client
// (internal/clients/{mskconnect,confluentcloud,aiven}/manual_test.go).
//
//	PIPEROOM_REPLAY_URL=https://... PIPEROOM_REPLAY_OUT=/tmp/cassettes \
//	  go test -tags manual -run TestManualRecordSmoke ./internal/setup/targets/managed/replay/
func TestManualRecordSmoke(t *testing.T) {
	target := os.Getenv("PIPEROOM_REPLAY_URL")
	out := os.Getenv("PIPEROOM_REPLAY_OUT")
	if target == "" || out == "" {
		t.Skip("set PIPEROOM_REPLAY_URL and PIPEROOM_REPLAY_OUT")
	}
	rec := NewRecorder(http.DefaultTransport, Scrubber{}, ProviderTest, "smoke", target, time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Transport: rec, Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	path := filepath.Join(out, "smoke.json")
	if err := Save(rec.Cassette(), path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
	t.Logf("wrote %s", path)
}
