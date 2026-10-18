//go:build integration

package wiring

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/config"
	"piperoom/internal/testenv"
)

// TestOpenWriteModeRealClients opens a client set with the real constructors.
// Off: no writer. On: the Kafka writer (it dials only when used, so Kafka need
// not be up) and, with SOURCE_DB_WRITE_URL naming a role of its own, the
// source writer, which connects and checks the role can write. The read URL
// as the write URL is refused before anything opens.
//
// It needs only the source Postgres of the dev stack (SOURCE_DB_URL, with a
// role that may CREATE ROLE). It creates and drops its own role (it_writer_*).
func TestOpenWriteModeRealClients(t *testing.T) {
	base := config.Load()
	admin := base.SourceDBURL
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Skipf("source postgres not reachable at %s: %v", hostOf(admin), err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	role := testenv.Name("writer")
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	pw := hex.EncodeToString(b) // hex only: safe inside the quoted literal below
	if _, err := conn.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" LOGIN PASSWORD '"+pw+"'"); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		if _, err := conn.Exec(cctx, "DROP ROLE IF EXISTS "+pgx.Identifier{role}.Sanitize()); err != nil {
			t.Logf("drop role %s: %v", role, err)
		}
	})
	u, err := url.Parse(admin)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, pw)

	ownersFile := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(ownersFile, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := base
	cfg.OwnersFile = ownersFile
	cfg.WarehouseKind = WarehousePostgres
	cfg.WarehouseDBURL = admin
	cfg.SourceDBWriteURL = u.String()

	open := func(mode string) *Wired {
		t.Helper()
		c := cfg
		c.WriteMode = mode
		w, err := Open(ctx, c, nil)
		if err != nil {
			t.Fatalf("Open(write mode %q): %v", mode, err)
		}
		t.Cleanup(w.Close)
		return w
	}

	if w := open("off"); w.write || w.writers != (writeClients{}) {
		t.Fatalf("write mode off: write=%v writers=%+v", w.write, w.writers)
	}
	w := open("on")
	if !w.write || w.writers.kafka == nil || w.writers.source == nil {
		t.Fatalf("write mode on: write=%v writers=%+v, want both writers", w.write, w.writers)
	}
	w.Close() // releases the writer's session, so the role can be dropped

	c := cfg
	c.WriteMode, c.SourceDBWriteURL = "on", admin
	if _, err := Open(ctx, c, nil); !errors.Is(err, errSameSourceRole) {
		t.Fatalf("Open with SOURCE_DB_WRITE_URL = SOURCE_DB_URL: err = %v, want errSameSourceRole", err)
	}
}
