//go:build integration

package setupstack

// Setup brings up the setup driver's own stack for a test: the Compose
// project the Compose target renders (kafka-kraft, connect, source-postgres)
// in a temporary directory, with the Postgres connector plugin installed
// from its pinned archive, and tears it down when the test ends. It reuses
// the driver's ops (compose_write, plugin_install, compose_up, compose_down)
// rather than a launcher of its own, so a test exercises the same code the
// setup agent runs.
//
// The stack is independent of the dev stack (its own project name, its own
// free ports and volumes), so tests using it neither take the shared stack
// lock nor disturb a pipeline someone is watching. It skips when no Docker
// daemon is reachable, like every Compose integration test.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/targets/compose"
	"piperoom/internal/testenv"
)

// SetupStack is one rendered and running setup stack.
type SetupStack struct {
	// Dir is the project directory the templates and the .env live in.
	Dir string
	// Project is the Compose project name (unique per test).
	Project string
	// Ports are the published host ports: kafka, connect, postgres.
	Ports map[string]int
	// ConnectURL is the worker's REST base on 127.0.0.1.
	ConnectURL string
	// PostgresURL is the source database's DSN from the host.
	PostgresURL string
	// Env is what an op runs with: the project root, its confiner and the
	// real docker runner.
	Env ops.Env
	// Registry holds the Compose ops the test may run (compose_ps,
	// container_inspect, compose_up, compose_down).
	Registry *ops.Registry
}

// setupPorts are the services the stack publishes; the defaults the render
// would use are moved to free ports so a setup stack never collides with the
// dev stack or a second test.
var setupServices = []string{"kafka", "connect", "postgres"}

// setupStartTimeout bounds the render, the plugin download and the start.
const setupStartTimeout = 12 * time.Minute

// Setup renders and starts the setup stack for t and registers its cleanup.
// The test skips, never fails, when Docker is not available.
func Setup(t *testing.T) *SetupStack {
	t.Helper()
	requireDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), setupStartTimeout)
	defer cancel()

	dir := t.TempDir()
	conf, err := ops.OpenConfiner(dir)
	if err != nil {
		t.Fatalf("testenv: open project dir: %v", err)
	}
	t.Cleanup(func() { _ = conf.Close() })

	st := &SetupStack{
		Dir:     dir,
		Project: setupProjectName(t),
		Ports:   map[string]int{},
		Env:     ops.Env{ProjectDir: dir, Confine: conf, Exec: setupRun, Now: time.Now},
	}
	for _, svc := range setupServices {
		st.Ports[svc] = freeLoopbackPort(t)
	}
	st.ConnectURL = fmt.Sprintf("http://127.0.0.1:%d", st.Ports["connect"])
	st.PostgresURL = fmt.Sprintf("postgres://piperoom:piperoom-dev@127.0.0.1:%d/orders?sslmode=disable", st.Ports["postgres"])

	reg, err := ops.New(ops.Config{ProjectDir: dir, Exec: setupRun})
	if err != nil {
		t.Fatalf("testenv: ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if _, err := compose.Register(reg, compose.Options{}); err != nil {
		t.Fatalf("testenv: compose.Register: %v", err)
	}
	st.Registry = reg

	st.exec(t, ctx, "compose_write", compose.WriteParams{
		Templates: []string{compose.TemplateKafka, compose.TemplateConnect, compose.TemplatePostgres},
		Project:   st.Project,
		Ports:     st.Ports,
	}, time.Minute)
	// properties_write refuses a plain-text credential by design (U1-4's store
	// materialises ${secret:name} references), so the test writes the .env the
	// store would: the dev stack's throwaway credentials, never a real secret.
	st.writeEnv(t)
	st.exec(t, ctx, "plugin_install", compose.PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"}, 8*time.Minute)

	t.Cleanup(func() {
		cctx, stop := context.WithTimeout(context.Background(), 3*time.Minute)
		defer stop()
		if op, ok := st.Registry.Get("compose_down"); ok {
			raw, _ := json.Marshal(compose.StackParams{})
			if _, err := op.Execute(cctx, raw, st.Env); err != nil {
				t.Logf("testenv: compose_down of %s: %v", st.Project, err)
			}
		}
		removeProjectVolumes(cctx, st.Project)
	})

	st.exec(t, ctx, "compose_up", compose.StackParams{}, 10*time.Minute)
	testenv.WaitFor(t, 6*time.Minute, func() (bool, error) {
		classes, err := connectorPluginClasses(st.ConnectURL)
		if err != nil {
			return false, nil
		}
		for _, c := range classes {
			if c == "io.debezium.connector.postgresql.PostgresConnector" {
				return true, nil
			}
		}
		return false, nil
	})
	t.Logf("testenv: setup stack %s up (connect %s, postgres 127.0.0.1:%d)", st.Project, st.ConnectURL, st.Ports["postgres"])
	return st
}

// Exec runs one registered op against the stack and returns its result; an
// op error fails the test with the summary the runner would have reported.
func (s *SetupStack) Exec(t *testing.T, timeout time.Duration, name string, params any) ops.Result {
	t.Helper()
	return s.exec(t, context.Background(), name, params, timeout)
}

// exec is Exec without the test-facing timeout argument.
func (s *SetupStack) exec(t *testing.T, parent context.Context, name string, params any, timeout time.Duration) ops.Result {
	t.Helper()
	op, ok := s.Registry.Get(name)
	if !ok {
		t.Fatalf("testenv: %s is not registered", name)
	}
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("testenv: marshalling %s params: %v", name, err)
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	res, err := op.Execute(ctx, raw, s.Env)
	if err != nil {
		t.Fatalf("testenv: %s: %v (summary %q)", name, err, res.Summary)
	}
	return res
}

// writeEnv writes the .env every compose command of this project reads. The
// values are the dev stack's throwaway credentials: the .env is what the
// runner's secret store materialises for a real run.
func (s *SetupStack) writeEnv(t *testing.T) {
	t.Helper()
	body := "POSTGRES_USER=piperoom\nPOSTGRES_PASSWORD=piperoom-dev\nPOSTGRES_DB=orders\n"
	if err := os.WriteFile(filepath.Join(s.Dir, compose.EnvFileName), []byte(body), 0o600); err != nil {
		t.Fatalf("testenv: writing %s: %v", compose.EnvFileName, err)
	}
}

// Data decodes an op's typed Data into the JSON shape the registry produces.
func Data(t *testing.T, res ops.Result) map[string]any {
	t.Helper()
	raw, err := json.Marshal(res.Data)
	if err != nil {
		t.Fatalf("testenv: marshalling Data: %v", err)
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("testenv: decoding Data: %v", err)
	}
	return out
}

// connectorPluginClasses lists the classes Connect found at start-up.
func connectorPluginClasses(base string) ([]string, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(base + "/connector-plugins")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return nil, fmt.Errorf("connect: HTTP %d", resp.StatusCode)
	}
	var plugins []struct {
		Class string `json:"class"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&plugins); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(plugins))
	for _, p := range plugins {
		out = append(out, p.Class)
	}
	return out, nil
}

// setupProjectName is a unique lowercase Compose project name.
func setupProjectName(t *testing.T) string {
	t.Helper()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("testenv: project name: %v", err)
	}
	return "piperoom-setup-" + hex.EncodeToString(b[:])
}

// freeLoopbackPort binds an ephemeral port and releases it: the stack's
// services must not collide with the dev stack's 8083/29092/5432.
func freeLoopbackPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("testenv: finding a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// requireDocker skips the test when Docker is not installed or its daemon is
// unreachable.
func requireDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("testenv: docker is not on PATH: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput(); err != nil {
		t.Skipf("testenv: the docker daemon is not reachable: %v\n%s", err, out)
	}
}

// setupRun is the real command runner with the ops package's contract: a
// non-zero exit is a code, not an error.
func setupRun(ctx context.Context, name string, args ...string) ([]byte, int, error) {
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

// removeProjectVolumes removes the anonymous volumes compose_down keeps by
// design (the driver never deletes data), so a CI runner stays clean.
func removeProjectVolumes(ctx context.Context, project string) {
	out, err := exec.CommandContext(ctx, "docker", "volume", "ls",
		"--filter", "label=com.docker.compose.project="+project, "--quiet").Output()
	if err != nil {
		return
	}
	for _, id := range strings.Fields(string(out)) {
		_ = exec.CommandContext(ctx, "docker", "volume", "rm", "-f", id).Run()
	}
}
