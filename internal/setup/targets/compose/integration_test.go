//go:build integration

package compose

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
	"os/exec"
	"strings"
	"testing"
	"time"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// TestComposeStackToHealthyConnect is the row's integration test on Linux (and
// any machine with a Docker daemon): an empty directory becomes a rendered
// stack, a connector plugin installed from the allow-listed host by its pinned
// digest, and a healthy Connect that lists the Postgres connector.
//
// It skips without a Docker daemon. The CI full-stack job has one, so the skip
// never fires there and needs no entry in .github/integration-skip-allow.txt.
func TestComposeStackToHealthyConnect(t *testing.T) {
	requireDockerDaemon(t)

	dir := t.TempDir()
	project := integrationProjectName(t)
	kafkaPort, connectPort := freePort(t), freePort(t)

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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

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

	// The ops run with their own deadlines here: the catalogue's default
	// timeout (10 s) is suited to reads, not to an image pull or a 13 MB
	// download. See the PR's known-gaps section.
	execOp("compose_write", WriteParams{
		Templates: []string{TemplateKafka, TemplateConnect},
		Project:   project,
		Ports:     map[string]int{"kafka": kafkaPort, "connect": connectPort},
	}, time.Minute)
	execOp("properties_write", PropertiesParams{File: EnvFileName, Entries: map[string]string{
		// Only the two non-secret keys: the compose CLI parses the whole
		// --env-file, and an un-materialised ${secret:name} reference (U1-4)
		// is not a value it can parse. The kafka+connect render interpolates
		// neither key, so this file only has to exist and parse.
		"POSTGRES_DB":   "orders",
		"POSTGRES_USER": "piperoom",
	}}, time.Minute)
	execOp("plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"}, 8*time.Minute)

	// The real compose CLI must accept --no-interpolate and the render must
	// parse back into the masked preview.
	config := execOp("compose_config", StackParams{}, time.Minute)
	configData := resultMap(t, config)
	if body, _ := configData["body"].(string); !strings.Contains(body, "services:") {
		t.Errorf("compose_config body is not a compose render: %q", body)
	}

	t.Cleanup(func() {
		// compose_down keeps volumes by design; the test removes its own
		// project's anonymous volumes afterwards so a CI runner stays clean.
		cleanupCtx, stop := context.WithTimeout(context.Background(), 3*time.Minute)
		defer stop()
		if op, ok := reg.Get("compose_down"); ok {
			raw, _ := json.Marshal(StackParams{})
			_, _ = op.Execute(cleanupCtx, raw, env)
		}
		removeProjectVolumes(cleanupCtx, project)
	})

	execOp("compose_up", StackParams{}, 10*time.Minute)
	pluginsURL := fmt.Sprintf("http://127.0.0.1:%d/connector-plugins", connectPort)
	waitForConnectorPlugin(t, ctx, pluginsURL, "io.debezium.connector.postgresql.PostgresConnector", func() string {
		return stackDiagnostics(ctx, dir)
	})

	ps := execOp("compose_ps", StackParams{}, time.Minute)
	psData := resultMap(t, ps)
	if count, _ := psData["count"].(float64); count < 2 {
		t.Errorf("compose_ps count = %v, want kafka and connect", psData["count"])
	}
	inspect := execOp("container_inspect", InspectParams{Service: "connect"}, time.Minute)
	inspectData := resultMap(t, inspect)
	raw, _ := json.Marshal(inspectData)
	if strings.Contains(string(raw), "BOOTSTRAP_SERVERS=kafka") {
		t.Fatalf("container_inspect carries an environment value: %s", raw)
	}
	if !strings.Contains(string(raw), "BOOTSTRAP_SERVERS") {
		t.Errorf("container_inspect does not carry the environment names: %s", raw)
	}
	// U5a: the real docker logs output, read newest last and capped. The
	// Connect worker has logged its startup by now.
	logs := execOp("container_logs", LogsParams{Service: "connect"}, time.Minute)
	logsData := resultMap(t, logs)
	if count, _ := logsData["count"].(float64); count < 1 || count > LogLines {
		t.Errorf("container_logs count = %v, want 1..%d", logsData["count"], LogLines)
	}
	if !strings.HasPrefix(logs.Summary, "connect logs (container ") {
		t.Errorf("container_logs summary = %.200q", logs.Summary)
	}

	down := execOp("compose_down", StackParams{}, 3*time.Minute)
	downData := resultMap(t, down)
	if downData["volumes_kept"] != true {
		t.Errorf("compose_down data = %v, want volumes_kept true", downData)
	}
}

// resultMap puts a direct Execute's typed Data back into the JSON shape the
// registry would have produced.
func resultMap(t *testing.T, res ops.Result) map[string]any {
	t.Helper()
	raw, err := json.Marshal(res.Data)
	if err != nil {
		t.Fatalf("marshalling Data: %v", err)
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decoding Data: %v", err)
	}
	return out
}

// requireDockerDaemon skips the test when Docker is not installed or its
// daemon is unreachable.
func requireDockerDaemon(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker is not on PATH: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput(); err != nil {
		t.Skipf("the docker daemon is not reachable: %v\n%s", err, out)
	}
}

// integrationRun is the real command runner, with the same contract as
// ops.runCommand: a non-zero exit is a code, not an error.
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

// freePort binds an ephemeral port and releases it: the rendered stack must
// not collide with the dev stack's 8083.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// integrationProjectName is a unique compose project name (the template
// pattern is lowercase).
func integrationProjectName(t *testing.T) string {
	t.Helper()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("generating a project name: %v", err)
	}
	return "piperoom-it-" + hex.EncodeToString(b[:])
}

// waitForConnectorPlugin polls Connect's plugin list until class appears.
func waitForConnectorPlugin(t *testing.T, ctx context.Context, url, class string, diagnostics func() string) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(6 * time.Minute)
	last := "no response yet"
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("building the plugin request: %v", err)
		}
		resp, err := client.Do(req)
		switch {
		case err != nil:
			last = err.Error()
		case resp.StatusCode != http.StatusOK:
			last = fmt.Sprintf("HTTP %d", resp.StatusCode)
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
		default:
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			var plugins []struct {
				Class string `json:"class"`
			}
			if json.Unmarshal(body, &plugins) != nil {
				last = "the plugin list did not parse"
				break
			}
			for _, p := range plugins {
				if p.Class == class {
					return
				}
			}
			last = fmt.Sprintf("Connect lists %d plugins, none is %s", len(plugins), class)
		}
		if time.Now().After(deadline) {
			t.Fatalf("Connect never listed %s: %s\n%s", class, last, diagnostics())
		}
		select {
		case <-ctx.Done():
			t.Fatalf("giving up waiting for Connect: %v (last: %s)\n%s", ctx.Err(), last, diagnostics())
		case <-time.After(3 * time.Second):
		}
	}
}

// stackDiagnostics is what a failed wait prints: the containers' state and the
// last lines of their logs, so a red CI run explains itself.
func stackDiagnostics(ctx context.Context, dir string) string {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var b strings.Builder
	psArgv, err := Argv(dir, DefaultFile, EnvFileName, "ps", nil)
	if err == nil {
		out, code, _ := integrationRun(ctx, psArgv[0], psArgv[1:]...)
		fmt.Fprintf(&b, "docker compose ps (exit %d):\n%s\n", code, out)
	}
	logsArgv, err := Argv(dir, DefaultFile, EnvFileName, "logs", []string{"connect", "kafka"})
	if err == nil {
		out, code, _ := integrationRun(ctx, logsArgv[0], logsArgv[1:]...)
		if len(out) > 8192 {
			out = out[len(out)-8192:]
		}
		fmt.Fprintf(&b, "docker compose logs (exit %d, tail):\n%s\n", code, out)
	}
	return b.String()
}

// removeProjectVolumes removes the anonymous volumes compose_down keeps.
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
