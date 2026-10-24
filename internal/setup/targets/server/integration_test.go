//go:build integration

package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/compose"
)

// TestServerHTTP SinkIntegration is row U11a's real run: an empty project
// directory becomes a rendered stack of the pinned Debezium Server image and
// the pinned Postgres image, the server's application.properties is built from
// the knowledge index for the HTTP sink, and a canary row inserted before the
// server starts reaches a receiver on this machine as one HTTP event.
//
// The receiver is a httptest server on this host, not a cloud sink. It is
// reached from the container the one way a container can reach the host: the
// Docker bridge gateway on Linux (a host address, bound here directly), and
// host.docker.internal on Docker Desktop. When neither is available the test
// skips with the reason, which is why it never needs an entry in
// .github/integration-skip-allow.txt while the CI job has a working Docker.
func TestServerHTTPSinkIntegration(t *testing.T) {
	requireDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	dir := t.TempDir()
	project := integrationProjectName(t)
	nonce := integrationNonce(t)
	prefix := "it_" + nonce[:12]

	// The receiver: bound on a host address a container can dial, with the
	// URL host the sink config carries.
	receiver := startReceiver(t)
	sinkURL := receiver.url + "/ingest"

	// One registry, two drivers: this package renders and validates the stack,
	// the Compose driver starts and stops it (its compose_up/compose_ps read
	// the same compose.yaml and .env).
	reg, err := ops.New(ops.Config{ProjectDir: dir, Exec: integrationRun})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	env := integrationEnv(t, dir)
	if _, err := Register(reg, Options{Class: protocol.ClassLocal, Mode: ModeCompose}); err != nil {
		t.Fatalf("server.Register: %v", err)
	}
	if _, err := compose.Register(reg, compose.Options{}); err != nil {
		t.Fatalf("compose.Register: %v", err)
	}

	execOp := func(name string, params any, timeout time.Duration) ops.Result {
		t.Helper()
		op, ok := reg.Get(name)
		if !ok {
			t.Fatalf("%s is not registered", name)
		}
		raw, err := json.Marshal(params)
		if err != nil {
			t.Fatalf("marshalling %s params: %v", name, err)
		}
		opCtx, stop := context.WithTimeout(ctx, timeout)
		defer stop()
		res, err := op.Execute(opCtx, raw, env)
		if err != nil {
			t.Fatalf("%s: %v (summary %q)", name, err, res.Summary)
		}
		return res
	}

	// The .env the Compose render interpolates. Written directly, not through
	// properties_write: that op refuses a plain-text credential by design, and
	// the runner's materialisation point (U1-4) is what fills this file in a
	// real run. The value is a test credential for a loopback-only stack.
	if err := os.WriteFile(dir+"/.env", []byte("POSTGRES_USER=piperoom\nPOSTGRES_PASSWORD=piperoom-it-canary\nPOSTGRES_DB=orders\n"), 0o600); err != nil {
		t.Fatalf("writing .env: %v", err)
	}

	execOp("server_compose_write", ComposeParams{Project: project}, time.Minute)
	execOp("server_properties_write", PropertiesParams{
		Sink: "http",
		SinkKeys: map[string]string{
			"debezium.sink.http.url":        sinkURL,
			"debezium.sink.http.timeout.ms": "10000",
		},
		Source: "postgres",
		SourceKeys: map[string]string{
			"database.hostname":           "postgres",
			"database.port":               "5432",
			"database.user":               "piperoom",
			"database.dbname":             "orders",
			"table.include.list":          "public.canary",
			"plugin.name":                 "pgoutput",
			"topic.prefix":                prefix,
			"slot.name":                   prefix,
			"publication.name":            prefix,
			"publication.autocreate.mode": "filtered",
		},
	}, time.Minute)

	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 3*time.Minute)
		defer stop()
		if op, ok := reg.Get("compose_down"); ok {
			raw, _ := json.Marshal(compose.StackParams{})
			_, _ = op.Execute(cleanupCtx, raw, env)
		}
		removeProjectVolumes(cleanupCtx, project)
	})

	// Start the source alone, create the canary table and insert the nonce, so
	// Debezium's initial snapshot carries the row and its publication can be
	// created for an existing table.
	execOp("compose_up", compose.StackParams{Services: []string{SourceService}}, 6*time.Minute)
	waitForPostgres(t, ctx, dir)

	sql := fmt.Sprintf("create table public.canary (id int primary key, note text); insert into public.canary (id, note) values (1, '%s');", nonce)
	if out, code := dockerComposeRun(t, ctx, dir, "exec", "-T", SourceService, "psql", "-U", "piperoom", "-d", "orders", "-v", "ON_ERROR_STOP=1", "-c", sql); code != 0 {
		t.Fatalf("creating the canary row (exit %d):\n%s", code, out)
	}

	execOp("compose_up", compose.StackParams{}, 6*time.Minute)

	// The sink's event for the canary row: the nonce anywhere in the body,
	// which is the JSON event the HTTP sink posts.
	select {
	case body := <-receiver.events:
		if !bytes.Contains(body, []byte(nonce)) {
			t.Fatalf("the first event does not carry the nonce %s:\n%.2000s", nonce, body)
		}
	case <-time.After(6 * time.Minute):
		t.Fatalf("no event reached %s within 6 minutes; the stack:\n%s", sinkURL, stackDiagnostics(ctx, dir))
	case <-ctx.Done():
		t.Fatalf("giving up waiting for the sink: %v\n%s", ctx.Err(), stackDiagnostics(ctx, dir))
	}

	// server_status reads the running service.
	res := execOp("server_status", StatusParams{}, time.Minute)
	raw, err := json.Marshal(res.Data)
	if err != nil {
		t.Fatalf("status Data: %v", err)
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatalf("status Data: %v", err)
	}
	if data["running"] != true {
		t.Errorf("server_status = %q, data %v; want the server running", res.Summary, data)
	}
}

// receiver is the loopback HTTP sink target: a httptest server on a host
// address a container can reach, one channel of request bodies.
type receiver struct {
	url    string
	events chan []byte
}

func startReceiver(t *testing.T) *receiver {
	t.Helper()
	events := make(chan []byte, 64)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusOK)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		select {
		case events <- body:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	host, listener := receiverListener(t)
	_ = srv.Listener.Close()
	srv.Listener = listener
	srv.Start()
	t.Cleanup(srv.Close)
	port := listener.Addr().(*net.TCPAddr).Port
	return &receiver{url: "http://" + net.JoinHostPort(host, strconv.Itoa(port)), events: events}
}

// receiverListener picks an interface a container can dial: the Docker bridge
// gateway on Linux (the host owns that address, so the test binds it), or
// every interface on Docker Desktop, where host.docker.internal is the name
// the engine resolves to the host.
func receiverListener(t *testing.T) (string, net.Listener) {
	t.Helper()
	if runtime.GOOS == "linux" {
		if gw := dockerBridgeGateway(t); gw != "" {
			if l, err := net.Listen("tcp", net.JoinHostPort(gw, "0")); err == nil {
				return gw, l
			}
		}
	}
	if _, err := net.LookupHost("host.docker.internal"); err != nil {
		t.Skipf("no address a container can use to reach this test: the docker bridge gateway is not bindable and host.docker.internal does not resolve (%v); the CI job has one both ways", err)
	}
	l, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Skipf("cannot bind a receiver that containers can reach: %v", err)
	}
	return "host.docker.internal", l
}

// dockerBridgeGateway is the default bridge's host address, "" when docker
// cannot report it.
func dockerBridgeGateway(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, code := docker(ctx, "network", "inspect", "bridge",
		"--format", "{{(index .IPAM.Config 0).Gateway}}")
	if code != 0 {
		return ""
	}
	gw := strings.TrimSpace(out)
	if net.ParseIP(gw) == nil {
		return ""
	}
	return gw
}

// integrationEnv opens the project for the ops the way the runner does.
func integrationEnv(t *testing.T, dir string) ops.Env {
	t.Helper()
	confine, err := ops.OpenConfiner(dir)
	if err != nil {
		t.Fatalf("OpenConfiner: %v", err)
	}
	t.Cleanup(func() { _ = confine.Close() })
	return ops.Env{ProjectDir: confine.Dir(), Confine: confine, MountDirs: []string{DistDir, UnitDir},
		Now: time.Now, Exec: integrationRun}
}

// integrationRun is the real command runner with ops.runCommand's contract: a
// non-zero exit is a code, not an error.
func integrationRun(ctx context.Context, name string, args ...string) ([]byte, int, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.CombinedOutput()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return out, exitErr.ExitCode(), nil
		}
		return out, -1, err
	}
	return out, 0, nil
}

// requireDocker skips the test when Docker is not installed or its daemon is
// unreachable.
func requireDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker is not on PATH: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out, code := docker(ctx, "info", "--format", "{{.ServerVersion}}"); code != 0 {
		t.Skipf("the docker daemon is not reachable (exit %d):\n%s", code, out)
	}
}

// docker runs one docker argv in its own process.
func docker(ctx context.Context, args ...string) (string, int) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.CombinedOutput()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return string(out), exitErr.ExitCode()
		}
		return string(out), -1
	}
	return string(out), 0
}

// dockerComposeRun runs `docker compose ... <args>` in the project directory.
func dockerComposeRun(t *testing.T, ctx context.Context, dir string, args ...string) (string, int) {
	t.Helper()
	argv := append([]string{"compose", "--project-directory", dir, "-f", dir + "/" + ComposeFile, "--env-file", dir + "/" + EnvFileName}, args...)
	return docker(ctx, argv...)
}

// waitForPostgres polls pg_isready inside the source container.
func waitForPostgres(t *testing.T, ctx context.Context, dir string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		if out, code := dockerComposeRun(t, ctx, dir, "exec", "-T", SourceService, "pg_isready", "-U", "piperoom", "-d", "orders"); code == 0 {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("postgres never became ready:\n%s\n%s", out, stackDiagnostics(ctx, dir))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("giving up waiting for postgres: %v", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

// stackDiagnostics is what a failed wait prints: the containers' state and the
// server's last log lines.
func stackDiagnostics(ctx context.Context, dir string) string {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var b strings.Builder
	out, code := docker(ctx, "compose", "--project-directory", dir, "-f", dir+"/"+ComposeFile, "ps")
	fmt.Fprintf(&b, "docker compose ps (exit %d):\n%s\n", code, out)
	out, code = docker(ctx, "compose", "--project-directory", dir, "-f", dir+"/"+ComposeFile, "logs", "--tail", "100", DefaultService)
	fmt.Fprintf(&b, "docker compose logs %s (exit %d, tail):\n%s\n", DefaultService, code, out)
	return b.String()
}

// removeProjectVolumes removes the named volume compose_down keeps.
func removeProjectVolumes(ctx context.Context, project string) {
	out, code := docker(ctx, "volume", "ls", "--filter", "label=com.docker.compose.project="+project, "--quiet")
	if code != 0 {
		return
	}
	for _, id := range strings.Fields(out) {
		_, _ = docker(ctx, "volume", "rm", "-f", id)
	}
}

// integrationProjectName is a unique compose project name for this run.
func integrationProjectName(t *testing.T) string {
	t.Helper()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("generating a project name: %v", err)
	}
	return "piperoom-it-" + hex.EncodeToString(b[:])
}

// integrationNonce is the canary value the row waits for.
func integrationNonce(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("generating a nonce: %v", err)
	}
	return hex.EncodeToString(b[:])
}
