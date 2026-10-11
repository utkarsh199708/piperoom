//go:build integration

package connect

import (
	"context"
	"net/http"
	"testing"
	"time"

	"piperoom/internal/testenv"
)

// Reads the real connector from the dev stack; fails if unreachable.
func TestIntegrationStatus(t *testing.T) {
	cfg := testenv.Require(t)
	c := New(cfg.ConnectURL, &http.Client{Timeout: 5 * time.Second})

	names, err := c.ListConnectors(context.Background())
	if err != nil {
		t.Fatalf("ListConnectors: %v", err)
	}
	found := false
	for _, n := range names {
		if n == "pg-orders-connector" {
			found = true
		}
	}
	if !found {
		t.Fatalf("pg-orders-connector missing from %v", names)
	}

	st, err := c.Status(context.Background(), "pg-orders-connector")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	t.Logf("status: %s (%s) %s, %d tasks", st.Name, st.Type, st.State, len(st.Tasks))
	if len(st.Tasks) == 0 {
		t.Fatalf("no tasks: %+v", st)
	}
}
