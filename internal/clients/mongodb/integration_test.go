//go:build integration

package mongodb

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestMongoIntegration runs against a one-member replica set (the dev compose
// "mongo" profile, U7c). It skips unless PIPEROOM_IT_MONGODB_URI names it, so
// the whole integration suite stays green without the profile. It is
// read-only: it never creates or drops a collection.
func TestMongoIntegration(t *testing.T) {
	uri := os.Getenv("PIPEROOM_IT_MONGODB_URI")
	if uri == "" {
		t.Skip("set PIPEROOM_IT_MONGODB_URI to run MongoDB integration tests (dev compose profile mongo, U7c)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c, err := New(ctx, uri)
	if err != nil {
		t.Fatalf("connect: %v", redactErr(err, uri))
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	version, err := c.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if version == "" {
		t.Fatal("Version returned an empty string")
	}

	rs, err := c.ReplicaSetStatus(ctx)
	if err != nil {
		t.Fatalf("ReplicaSetStatus: %v", err)
	}
	if !rs.IsReplicaSet || rs.SetName == "" {
		t.Fatalf("expected a replica set, got %+v", rs)
	}

	cs, err := c.ChangeStreamSupport(ctx)
	if err != nil {
		t.Fatalf("ChangeStreamSupport: %v", err)
	}
	if !cs.Supported {
		t.Fatalf("expected change streams to be supported, got %+v", cs)
	}

	roles, err := c.Roles(ctx)
	if err != nil {
		t.Fatalf("Roles: %v", err)
	}
	if len(roles) == 0 {
		t.Fatal("expected at least one authenticated role")
	}

	settings, err := c.Settings(ctx)
	if err != nil {
		t.Fatalf("Settings: %v", err)
	}
	seen := make(map[string]bool, len(settings))
	for _, s := range settings {
		seen[s.Name] = true
	}
	if !seen["version"] || !seen["replica_set_state"] || !seen["change_streams"] {
		t.Fatalf("Settings missing a name: %+v", settings)
	}
}
