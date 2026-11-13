//go:build manual

package confluentcloud

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

// TestManualSandbox records the read cassettes on the Confluent Cloud
// sandbox (docs/design/setup-managed.md §6) into PIPEROOM_CCLOUD_SANDBOX_OUT
// for a lead to compare with the hand-written ones. The setup key is read
// from the environment of the shell that runs the test, never from a file in
// the repository.
//
//	PIPEROOM_CCLOUD_SANDBOX_KEY=... PIPEROOM_CCLOUD_SANDBOX_SECRET=... \
//	PIPEROOM_CCLOUD_SANDBOX_ENV=env-... PIPEROOM_CCLOUD_SANDBOX_CLUSTER=lkc-... \
//	PIPEROOM_CCLOUD_SANDBOX_OUT=/tmp/ccloud-cassettes \
//	  go test -tags manual -run TestManualSandbox ./internal/clients/confluentcloud/
func TestManualSandbox(t *testing.T) {
	key, secret := os.Getenv("PIPEROOM_CCLOUD_SANDBOX_KEY"), os.Getenv("PIPEROOM_CCLOUD_SANDBOX_SECRET")
	env, cluster, out := os.Getenv("PIPEROOM_CCLOUD_SANDBOX_ENV"), os.Getenv("PIPEROOM_CCLOUD_SANDBOX_CLUSTER"), os.Getenv("PIPEROOM_CCLOUD_SANDBOX_OUT")
	if key == "" || secret == "" || env == "" || cluster == "" || out == "" {
		t.Skip("set PIPEROOM_CCLOUD_SANDBOX_KEY, _SECRET, _ENV, _CLUSTER and _OUT")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	scrub := replay.Scrubber{Names: map[string]string{key: replay.Redacted}}
	record := func(name string, fn func(c *Client)) {
		rec := replay.NewRecorder(http.DefaultTransport, scrub, replay.ProviderConfluent, name, "sandbox run "+time.Now().UTC().Format("2006-01-02"), time.Now())
		c, err := New(Config{APIKey: key, APISecret: netsec.Secret(secret), Environment: env, Cluster: cluster, Transport: rec})
		if err != nil {
			t.Fatal(err)
		}
		fn(c)
		if err := replay.Save(rec.Cassette(), filepath.Join(out, name+".json")); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	record("live_plugins", func(c *Client) {
		if _, err := c.Plugins(ctx); err != nil {
			t.Errorf("Plugins: %v", err)
		}
	})
	record("live_list_and_cluster", func(c *Client) {
		if _, err := c.ListConnectors(ctx); err != nil {
			t.Errorf("ListConnectors: %v", err)
		}
		if _, err := c.Cluster(ctx); err != nil {
			t.Errorf("Cluster: %v", err)
		}
	})
}
