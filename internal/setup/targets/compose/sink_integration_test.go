//go:build integration

package compose

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// TestComposeSourceAndSinkCanaryRow is row U3c's integration test: source and
// sink on one Compose project — the source Postgres, Kafka, Connect with the
// Postgres source and the JDBC sink installed by plugin_install from their
// pinned digests, and the sink-postgres dev warehouse — and the canary row
// inserted into the source arrives in the warehouse (the ladder's rung 7,
// sink_row). It skips without a Docker daemon.
func TestComposeSourceAndSinkCanaryRow(t *testing.T) {
	requireDockerDaemon(t)

	dir := t.TempDir()
	project := integrationProjectName(t)
	ports := map[string]int{"kafka": freePort(t), "connect": freePort(t), "postgres": freePort(t), "warehouse": freePort(t)}

	confine, err := ops.OpenConfiner(dir)
	if err != nil {
		t.Fatalf("OpenConfiner: %v", err)
	}
	t.Cleanup(func() { _ = confine.Close() })
	env := ops.Env{ProjectDir: confine.Dir(), Confine: confine, MountDirs: []string{PluginDir}, Now: time.Now, Exec: integrationRun}
	reg, err := ops.New(ops.Config{ProjectDir: dir, Exec: integrationRun})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if _, err := Register(reg, Options{Class: protocol.ClassLocal, PluginHosts: []string{"repo1.maven.org"}}); err != nil {
		t.Fatalf("compose.Register: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	execOp := func(name string, params any, timeout time.Duration) {
		t.Helper()
		op, ok := reg.Get(name)
		if !ok {
			t.Fatalf("%s is not registered", name)
		}
		raw, _ := json.Marshal(params)
		opCtx, stop := context.WithTimeout(ctx, timeout)
		defer stop()
		if res, err := op.Execute(opCtx, raw, env); err != nil {
			t.Fatalf("%s: %v (summary %q)", name, err, res.Summary)
		}
	}

	execOp("compose_write", WriteParams{
		Templates: []string{TemplateKafka, TemplatePostgres, TemplateConnect, TemplateSinkPostgres},
		Project:   project,
		Ports:     ports,
	}, time.Minute)
	// properties_write refuses a plain-text credential by design (U1-4's store
	// materialises ${secret:name} references), so the test writes the .env the
	// store would: throwaway credentials of this test's own stack, never a
	// real secret (internal/testenv/setupstack does the same).
	envFile := "POSTGRES_USER=piperoom\nPOSTGRES_PASSWORD=it-source-only\nPOSTGRES_DB=orders\n" +
		"WAREHOUSE_USER=warehouse\nWAREHOUSE_PASSWORD=it-warehouse-only\nWAREHOUSE_DB=analytics\n"
	if err := os.WriteFile(filepath.Join(dir, EnvFileName), []byte(envFile), 0o600); err != nil {
		t.Fatal(err)
	}
	execOp("plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"}, 8*time.Minute)
	execOp("plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: SinkConnectorJDBC}, 8*time.Minute)

	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 3*time.Minute)
		defer stop()
		if op, ok := reg.Get("compose_down"); ok {
			raw, _ := json.Marshal(StackParams{})
			_, _ = op.Execute(cleanupCtx, raw, env)
		}
		removeProjectVolumes(cleanupCtx, project)
	})
	execOp("compose_up", StackParams{}, 12*time.Minute)

	connect := fmt.Sprintf("http://127.0.0.1:%d", ports["connect"])
	diag := func() string { return stackDiagnostics(ctx, dir) }
	waitForConnectorPlugin(t, ctx, connect+"/connector-plugins", "io.debezium.connector.postgresql.PostgresConnector", diag)
	waitForConnectorPlugin(t, ctx, connect+"/connector-plugins", "io.debezium.connector.jdbc.JdbcSinkConnector", diag)

	src := pgConnect(t, ctx, fmt.Sprintf("postgres://piperoom:it-source-only@127.0.0.1:%d/orders?sslmode=disable", ports["postgres"]))
	if _, err := src.Exec(ctx, "CREATE TABLE IF NOT EXISTS public.piperoom_canary (nonce text PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now())"); err != nil {
		t.Fatalf("creating the canary table: %v", err)
	}

	postConnector(t, ctx, connect, "it-source", map[string]string{
		"connector.class":             "io.debezium.connector.postgresql.PostgresConnector",
		"database.hostname":           "postgres",
		"database.port":               "5432",
		"database.user":               "piperoom",
		"database.password":           "it-source-only",
		"database.dbname":             "orders",
		"topic.prefix":                "it",
		"plugin.name":                 "pgoutput",
		"publication.autocreate.mode": "filtered",
		"table.include.list":          "public.piperoom_canary",
	})
	postConnector(t, ctx, connect, "it-sink", map[string]string{
		"connector.class":     "io.debezium.connector.jdbc.JdbcSinkConnector",
		"topics":              "it.public.piperoom_canary",
		"connection.url":      "jdbc:postgresql://warehouse:5432/analytics",
		"connection.username": "warehouse",
		"connection.password": "it-warehouse-only",
		"insert.mode":         "upsert",
		"primary.key.mode":    "record_key",
		"schema.evolution":    "basic",
	})

	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	nonce := hex.EncodeToString(b)
	if _, err := src.Exec(ctx, "INSERT INTO public.piperoom_canary (nonce) VALUES ($1)", nonce); err != nil {
		t.Fatalf("inserting the canary: %v", err)
	}

	wh := pgConnect(t, ctx, fmt.Sprintf("postgres://warehouse:it-warehouse-only@127.0.0.1:%d/analytics?sslmode=disable", ports["warehouse"]))
	deadline := time.Now().Add(8 * time.Minute)
	for {
		// The JDBC sink names the table from the topic; find it by suffix
		// rather than pin a naming rule of one release.
		var table string
		err := wh.QueryRow(ctx, "SELECT table_schema || '.' || quote_ident(table_name) FROM information_schema.tables WHERE table_name LIKE '%piperoom_canary' ORDER BY table_name LIMIT 1").Scan(&table)
		if err == nil {
			var n int
			if err := wh.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE nonce = $1", nonce).Scan(&n); err == nil && n == 1 {
				return // rung 7: the canary row is in the sink
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the canary row never reached the warehouse\n%s", diag())
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(3 * time.Second):
		}
	}
}

// pgConnect connects with retries: the container's healthcheck passes before
// the published port always accepts.
func pgConnect(t *testing.T, ctx context.Context, url string) *pgx.Conn {
	t.Helper()
	var lastErr error
	for i := 0; i < 30; i++ {
		c, err := pgx.Connect(ctx, url)
		if err == nil {
			t.Cleanup(func() { _ = c.Close(context.Background()) })
			return c
		}
		lastErr = err
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("connecting to Postgres: %v", lastErr)
	return nil
}

// postConnector registers one connector and fails the test on a non-2xx.
func postConnector(t *testing.T, ctx context.Context, connect, name string, config map[string]string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"name": name, "config": config})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, connect+"/connectors", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("registering %s: %v", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		t.Fatalf("registering %s: %s: %s", name, resp.Status, strings.TrimSpace(string(msg)))
	}
}
