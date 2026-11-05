//go:build integration

package sqlserver

import (
	"context"
	"net/url"
	"os"
	"testing"
)

// mssqlDSNEnv is the env var the dev compose profile "mssql" (U4c) will set for
// the integration tests. Until U4c lands it is unset and these tests skip; the
// skip is stated in the PR.
const mssqlDSNEnv = "PIPEROOM_MSSQL_DSN"

func mssqlDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(mssqlDSNEnv)
	if dsn == "" {
		t.Skipf("sqlserver integration: %s is not set (the dev compose profile \"mssql\", U4c, has not landed yet)", mssqlDSNEnv)
	}
	return dsn
}

// hostPort returns only host:port of a go-mssqldb URL so a failing test never
// prints the embedded password.
func hostPort(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return "(not a URL)"
	}
	return u.Host
}

func openClient(t *testing.T) *Client {
	t.Helper()
	dsn := mssqlDSN(t)
	c, err := New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("sqlserver unreachable (%s): %v", hostPort(dsn), err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestServerInfoIntegration(t *testing.T) {
	c := openClient(t)
	info, err := c.ServerInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Version == "" {
		t.Fatal("empty version")
	}
}

func TestAgentStateIntegration(t *testing.T) {
	c := openClient(t)
	if _, err := c.AgentState(context.Background()); err != nil {
		t.Fatal(err)
	}
}
