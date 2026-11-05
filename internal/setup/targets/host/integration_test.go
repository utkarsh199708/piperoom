//go:build integration

package host

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/compose"
	"piperoom/internal/testenv"
	"piperoom/internal/testenv/setupstack"
)

// kafkaDistEnv names an extracted Kafka distribution (the directory holding
// bin/ and libs/) the test copies into the empty project: this row has no op
// that downloads one (see the PR's known gaps), so the test does what the
// person or a later op would.
const kafkaDistEnv = "PIPEROOM_IT_HOST_KAFKA_DIST"

// TestHostEmptyDirToRunningConnector is the row's integration test: an empty
// project directory becomes a worker file, a user-level systemd unit that runs
// Kafka Connect from the project, and a RUNNING Debezium Postgres connector.
// Kafka and Postgres come from the setup stack (Compose, its own free ports);
// the worker runs on the host under `systemctl --user`, which is what this
// target exists for.
//
// It skips without a reachable user systemd manager (a CI runner needs
// `loginctl enable-linger` and XDG_RUNTIME_DIR), without java on PATH, without
// PIPEROOM_IT_HOST_KAFKA_DIST, and without Docker.
func TestHostEmptyDirToRunningConnector(t *testing.T) {
	requireUserSystemd(t)
	if _, err := exec.LookPath("java"); err != nil {
		t.Skipf("java is not on PATH: %v", err)
	}
	dist := os.Getenv(kafkaDistEnv)
	if dist == "" {
		t.Skipf("%s is not set: point it at an extracted Kafka distribution", kafkaDistEnv)
	}
	if fi, err := os.Stat(dist + "/bin/connect-distributed.sh"); err != nil || fi.IsDir() {
		t.Fatalf("%s=%s has no bin/connect-distributed.sh: %v", kafkaDistEnv, dist, err)
	}
	st := setupstack.Setup(t) // Kafka and Postgres; skips without Docker

	dir := t.TempDir()
	if err := os.CopyFS(dir+"/"+DistDir, os.DirFS(dist)); err != nil {
		t.Fatalf("copying the distribution: %v", err)
	}
	reg, err := ops.New(ops.Config{ProjectDir: dir})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if _, err := Register(reg, Options{Class: protocol.ClassLocal}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	run := func(r *ops.Registry, op string, params any, timeout time.Duration) protocol.OpResult {
		t.Helper()
		raw, err := json.Marshal(params)
		if err != nil {
			t.Fatal(err)
		}
		res, err := r.Execute(ctx, protocol.OpRequest{ID: op, Op: op, Params: raw, Tier: protocol.TierT2,
			Class: protocol.ClassLocal, Deadline: time.Now().Add(timeout)})
		if err != nil {
			t.Fatalf("%s: %v (summary %q, data %s)", op, err, res.Summary, res.Data)
		}
		t.Logf("%s: %s", op, res.Summary)
		return res
	}

	// The plugin path: the Compose target's plugin_install fills the same
	// connect-plugins directory the host worker loads from.
	creg, err := ops.New(ops.Config{ProjectDir: dir})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = creg.Close() })
	if _, err := compose.Register(creg, compose.Options{PluginHosts: []string{"repo1.maven.org"}}); err != nil {
		t.Fatalf("compose.Register: %v", err)
	}
	run(creg, "plugin_install", compose.PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"}, 8*time.Minute)

	id := uniqueID(t)
	connectURL := fmt.Sprintf("http://127.0.0.1:%d", freePort(t))
	run(reg, "properties_write", PropertiesParams{File: "connect-distributed.properties", Entries: map[string]string{
		"bootstrap.servers":                 "127.0.0.1:" + strconv.Itoa(st.Ports["kafka"]),
		"group.id":                          id,
		"config.storage.topic":              id + "-configs",
		"offset.storage.topic":              id + "-offsets",
		"status.storage.topic":              id + "-status",
		"config.storage.replication.factor": "1",
		"offset.storage.replication.factor": "1",
		"status.storage.replication.factor": "1",
		"key.converter":                     "org.apache.kafka.connect.json.JsonConverter",
		"value.converter":                   "org.apache.kafka.connect.json.JsonConverter",
		"listeners":                         connectURL,
	}}, time.Minute)

	unit := UnitPrefix + id + ".service"
	t.Cleanup(func() { removeUnit(t, unit) })
	run(reg, "service_install", ServiceParams{Name: id, Mode: ModeDistributed}, time.Minute)
	run(reg, "service_restart", UnitParams{Name: id}, time.Minute)

	testenv.WaitFor(t, 4*time.Minute, func() (bool, error) {
		var plugins []struct {
			Class string `json:"class"`
		}
		if getJSON(connectURL+"/connector-plugins", &plugins) != nil {
			return false, nil
		}
		for _, p := range plugins {
			if p.Class == "io.debezium.connector.postgresql.PostgresConnector" {
				return true, nil
			}
		}
		return false, fmt.Errorf("the worker is up without the Postgres plugin: %v", plugins)
	})

	slot := "it_" + id[3:]
	body, _ := json.Marshal(map[string]any{"name": id, "config": map[string]string{
		"connector.class":   "io.debezium.connector.postgresql.PostgresConnector",
		"database.hostname": "127.0.0.1",
		"database.port":     strconv.Itoa(st.Ports["postgres"]),
		"database.user":     "piperoom",
		"database.password": "piperoom-dev", // the setup stack's throwaway credential
		"database.dbname":   "orders",
		"topic.prefix":      id,
		"plugin.name":       "pgoutput",
		"slot.name":         slot,
		"publication.name":  slot,
		"tasks.max":         "1",
	}})
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Post(connectURL+"/connectors", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("creating the connector: %v", err)
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("creating the connector: HTTP %d: %s", resp.StatusCode, msg)
	}

	testenv.WaitFor(t, 3*time.Minute, func() (bool, error) {
		var status struct {
			Connector struct{ State string } `json:"connector"`
			Tasks     []struct{ State, Trace string }
		}
		if getJSON(connectURL+"/connectors/"+id+"/status", &status) != nil {
			return false, nil
		}
		for _, task := range status.Tasks {
			if task.State == "FAILED" {
				return false, fmt.Errorf("task failed: %.500s", task.Trace)
			}
		}
		return status.Connector.State == "RUNNING" && len(status.Tasks) == 1 && status.Tasks[0].State == "RUNNING", nil
	})

	res := run(reg, "service_status", UnitParams{Name: id}, 30*time.Second)
	var data map[string]any
	if err := json.Unmarshal(res.Data, &data); err != nil || data["running"] != true {
		t.Fatalf("service_status after the connector is RUNNING: %s (%v)", res.Data, err)
	}
	t.Logf("FragmentPath %v (the unit file under the project: %s/%s/%s)", data["fragment_path"], dir, UnitDir, unit)
}

// requireUserSystemd skips unless `systemctl --user` reaches a manager.
func requireUserSystemd(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("systemctl"); err != nil {
		t.Skipf("systemctl is not on PATH: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "systemctl", "--user", "show-environment").CombinedOutput(); err != nil {
		t.Skipf("no user systemd manager (needs a login session or `loginctl enable-linger` plus XDG_RUNTIME_DIR): %v: %.200s", err, out)
	}
}

// removeUnit stops and unlinks the test's unit; disable undoes enable's link.
func removeUnit(t *testing.T, unit string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, args := range [][]string{
		{"--user", "stop", unit}, {"--user", "disable", unit}, {"--user", "reset-failed", unit}, {"--user", "daemon-reload"},
	} {
		if out, err := exec.CommandContext(ctx, "systemctl", args...).CombinedOutput(); err != nil {
			t.Logf("cleanup systemctl %v: %v: %.200s", args, err, out)
		}
	}
}

func getJSON(url string, dst any) error {
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(dst)
}

// uniqueID is a service name and Kafka name prefix unique to this run.
func uniqueID(t *testing.T) string {
	var b [3]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return "it-" + hex.EncodeToString(b[:])
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
