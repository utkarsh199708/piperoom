//go:build integration

package mongodb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// TestSetupWriterIntegration runs the setup writer against the one-member
// replica set of the dev compose "mongo" profile (U7c, dev/mongo/README.md) as
// a user that may create users, roles and collections: the profile's root
// user, not the read-only piperoom_cdc of PIPEROOM_IT_MONGODB_URI. It skips
// unless PIPEROOM_IT_MONGODB_WRITE_URI names it, e.g.
// mongodb://piperoom_admin:piperoom-mongo-admin@127.0.0.1:27017/?authSource=admin&directConnection=true
// (dev-only credentials), so the suite stays green while the full-stack job
// does not start the profile. Every name carries an it_<random> prefix, and
// cleanup drops the user, its role and the canary collection.
func TestSetupWriterIntegration(t *testing.T) {
	uri := os.Getenv("PIPEROOM_IT_MONGODB_WRITE_URI")
	if uri == "" {
		t.Skip("set PIPEROOM_IT_MONGODB_WRITE_URI (the mongo dev profile's root user, U7c) to run the MongoDB setup writer integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	prefix := "it_" + hex.EncodeToString(b)
	user := prefix + "_dbz"
	canary := "inventory." + prefix + "_canary"

	store := &memStore{}
	w, err := NewSetupWriter(ctx, true, uri, WithSecretStore(store))
	if err != nil {
		t.Fatalf("NewSetupWriter: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer ccancel()
		for _, c := range []struct {
			db  string
			cmd bson.D
		}{
			{"admin", bson.D{{Key: "dropUser", Value: user}}},
			{"admin", bson.D{{Key: "dropRole", Value: user + "_cdc"}}},
			{"inventory", bson.D{{Key: "drop", Value: prefix + "_canary"}}},
		} {
			if _, err := w.run.runCommand(cctx, c.db, c.cmd); err != nil {
				t.Logf("cleanup %s: %v", c.cmd[0].Key, err)
			}
		}
		_ = w.Close(cctx)
	})

	sctx := tools.WithPipeline(ctx, owners.Pipeline{ID: prefix, SourceTables: []string{canary}})

	// The canary collection exists afterwards whether or not it did before.
	for i := 0; i < 2; i++ {
		if err := w.CreateCanaryTable(sctx, canary); err != nil {
			t.Fatalf("CreateCanaryTable (call %d): %v", i+1, err)
		}
	}
	nb := make([]byte, 16)
	if _, err := rand.Read(nb); err != nil {
		t.Fatal(err)
	}
	nonce := hex.EncodeToString(nb)
	if err := w.InsertCanary(sctx, canary, nonce); err != nil {
		t.Fatalf("InsertCanary: %v", err)
	}
	found, err := w.run.runCommand(ctx, "inventory", bson.D{
		{Key: "find", Value: prefix + "_canary"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: nonce}}},
	})
	if err != nil {
		t.Fatalf("find canary: %v", err)
	}
	cur, _ := asMap(found["cursor"])
	batch, _ := asSlice(cur["firstBatch"])
	if len(batch) != 1 {
		t.Fatalf("found %d canary documents with the nonce, want 1", len(batch))
	}

	name, err := w.CreateReadRole(sctx, user, []string{canary})
	if err != nil {
		t.Fatalf("CreateReadRole: %v", err)
	}
	if name != readPasswordSecretName || store.value == "" {
		t.Fatalf("CreateReadRole returned %q, store holds %q", name, store.name)
	}
	if _, err := w.CreateReadRole(sctx, user, []string{canary}); !errors.Is(err, errUserExists) {
		t.Fatalf("second CreateReadRole: err = %v, want errUserExists", err)
	}

	// The read client sees what the new user may do.
	c, err := New(ctx, uri)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	ri, err := c.RoleInfo(ctx, "admin."+user, []string{canary, "inventory.orders"})
	if err != nil {
		t.Fatalf("RoleInfo: %v", err)
	}
	if !ri.Exists || !ri.Replication || !ri.Selectable[canary] {
		t.Fatalf("RoleInfo = %+v, want an existing user with oplog read and the canary readable", ri)
	}
	if ri.Selectable["inventory.orders"] {
		t.Fatal("the read user can read a collection it was not granted")
	}

	for _, err := range []error{
		w.CreatePublication(sctx, "pub", []string{canary}),
		func() error { _, err := w.AlterSystem(sctx, "wal_level", "logical"); return err }(),
	} {
		if err == nil {
			t.Fatal("a not-supported method succeeded")
		}
	}
}
