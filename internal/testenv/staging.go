//go:build integration

package testenv

import (
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"piperoom/internal/config"
)

// RequireStaging is Require plus the staging profile: it returns the dev config
// and where staging Connect and Schema Registry answer, and skips the test when
// either is not reachable, so a run without `devctl up --staging` skips instead
// of failing (the staging tests of X8b use it).
func RequireStaging(t *testing.T) (config.Config, Staging) {
	t.Helper()
	cfg := Require(t)
	stg := StagingFromEnv(os.Getenv)
	for name, raw := range map[string]string{"connect-staging": stg.ConnectURL, "schema-registry-staging": stg.SchemaRegistryURL} {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" {
			t.Fatalf("testenv: bad staging URL for %s: %q", name, raw)
		}
		conn, err := net.DialTimeout("tcp", u.Host, 2*time.Second)
		if err != nil {
			t.Skipf("testenv: staging profile not reachable: %s at %s (run `go run ./cmd/devctl up --staging`): %v", name, u.Host, err)
		}
		_ = conn.Close()
	}
	return cfg, stg
}
