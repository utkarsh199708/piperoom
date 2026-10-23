package compose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// fakeRunner records every docker argv a compose op builds and answers from a
// function: unit tests need no Docker.
type fakeRunner struct {
	calls [][]string
	fn    func(argv []string) ([]byte, int, error)
}

func (f *fakeRunner) exec(_ context.Context, name string, args ...string) ([]byte, int, error) {
	argv := append([]string{name}, args...)
	f.calls = append(f.calls, argv)
	return f.fn(argv)
}

// newDockerHarness is a registry rooted at a temporary project directory with
// the driver registered and a fake command runner. The two files every compose
// read expects are written first.
func newDockerHarness(t *testing.T, opts Options, fn func(argv []string) ([]byte, int, error)) (*ops.Registry, *fakeRunner, string, *Driver) {
	t.Helper()
	dir := t.TempDir()
	fr := &fakeRunner{fn: fn}
	reg, err := ops.New(ops.Config{ProjectDir: dir, Exec: fr.exec})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	d, err := Register(reg, opts)
	if err != nil {
		t.Fatalf("compose.Register: %v", err)
	}
	writeStackFiles(t, dir)
	return reg, fr, dir, d
}

// writeStackFiles writes a rendered kafka+connect stack and its .env.
func writeStackFiles(t *testing.T, dir string) {
	t.Helper()
	rendered, err := (&Driver{}).Render(WriteParams{Templates: []string{TemplateKafka, TemplateConnect}})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, rendered.File), rendered.Doc, 0o644); err != nil {
		t.Fatalf("writing the render: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, EnvFileName), []byte("POSTGRES_DB=orders\n"), 0o600); err != nil {
		t.Fatalf("writing .env: %v", err)
	}
}

// runOp executes one op through the catalogue, as the runner does.
func runOp(t *testing.T, reg *ops.Registry, name string, params any) (protocol.OpResult, error) {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshalling params: %v", err)
	}
	return reg.Execute(context.Background(), protocol.OpRequest{Op: name, Params: raw})
}

// baseArgv is the pinned head every compose argv starts with.
func baseArgv(dir, sub string) []string {
	return []string{
		"docker", "compose",
		"--project-directory", dir,
		"-f", filepath.Join(dir, DefaultFile),
		"--env-file", filepath.Join(dir, EnvFileName),
		sub,
	}
}

func assertArgv(t *testing.T, got, want []string) {
	t.Helper()
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("argv = %q, want %q", got, want)
	}
}

func TestDockerInfo(t *testing.T) {
	versionJSON := `{"Client":{"Version":"27.0.3","ApiVersion":"1.46","Os":"linux","Arch":"amd64","Context":"desktop-linux"},` +
		`"Server":{"Version":"27.0.3","ApiVersion":"1.46","Os":"linux","Arch":"amd64"}}`
	composeJSON := `{"version":"v2.29.1","buildType":"github.com/docker/compose"}`
	cases := []struct {
		name               string
		goos               string
		env                map[string]string
		wantSocket         string
		wantHostNetworking bool
		wantContainerHost  string
	}{
		{"linux", "linux", nil, "/var/run/docker.sock", true, "172.17.0.1"},
		{"macOS", "darwin", nil, "/var/run/docker.sock", false, "host.docker.internal"},
		{"windows", "windows", nil, "//./pipe/docker_engine", false, "host.docker.internal"},
		{"windows named pipe via DOCKER_HOST", "windows", map[string]string{"DOCKER_HOST": "npipe:////./pipe/docker_engine"}, "//./pipe/docker_engine", false, "host.docker.internal"},
		{"tcp DOCKER_HOST", "linux", map[string]string{"DOCKER_HOST": "tcp://127.0.0.1:2375"}, "tcp://127.0.0.1:2375", true, "172.17.0.1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg, fr, _, _ := newDockerHarness(t, Options{GOOS: tc.goos, Getenv: func(k string) string { return tc.env[k] }},
				func(argv []string) ([]byte, int, error) {
					switch argv[1] {
					case "version":
						return []byte(versionJSON), 0, nil
					case "compose":
						return []byte(composeJSON), 0, nil
					}
					return nil, 1, fmt.Errorf("unexpected argv %q", argv)
				})
			res, err := runOp(t, reg, "docker_info", map[string]any{})
			if err != nil {
				t.Fatalf("docker_info: %v", err)
			}
			d := data(t, res)
			for key, want := range map[string]any{
				"socket_path":     tc.wantSocket,
				"host_networking": tc.wantHostNetworking,
				"container_host":  tc.wantContainerHost,
				"daemon_ok":       true,
				"compose_ok":      true,
				"client_version":  "27.0.3",
				"server_version":  "27.0.3",
				"compose_version": "2.29.1",
			} {
				if got := d[key]; got != want {
					t.Errorf("Data[%q] = %v, want %v", key, got, want)
				}
			}
			if len(fr.calls) != 2 {
				t.Fatalf("calls = %q, want two", fr.calls)
			}
			assertArgv(t, fr.calls[0], []string{"docker", "version", "--format", "{{json .}}"})
			assertArgv(t, fr.calls[1], []string{"docker", "compose", "version", "--format", "json"})
		})
	}
}

// TestDockerInfoDaemonDown: a daemon that is down is a finding the model can
// act on, not an op failure.
func TestDockerInfoDaemonDown(t *testing.T) {
	reg, _, _, _ := newDockerHarness(t, Options{GOOS: "linux"},
		func(argv []string) ([]byte, int, error) {
			if argv[1] == "version" {
				return []byte("Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?"), 1, nil
			}
			return []byte("docker: 'compose' is not a docker command."), 1, nil
		})
	res, err := runOp(t, reg, "docker_info", map[string]any{})
	if err != nil {
		t.Fatalf("docker_info returned an error for a down daemon: %v", err)
	}
	d := data(t, res)
	if d["daemon_ok"] != false || d["compose_ok"] != false {
		t.Fatalf("daemon_ok/compose_ok = %v/%v, want false/false", d["daemon_ok"], d["compose_ok"])
	}
	text, _ := json.Marshal(d)
	if !strings.Contains(string(text), "Cannot connect") {
		t.Errorf("Data does not carry the daemon's error: %s", text)
	}
	if !strings.Contains(res.Summary, "unreachable") {
		t.Errorf("summary = %q, want it to say the daemon is unreachable", res.Summary)
	}
}

func TestDockerInfoMissingCLI(t *testing.T) {
	reg, _, _, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
		return nil, -1, errors.New("exec: \"docker\": executable file not found in $PATH")
	})
	_, err := runOp(t, reg, "docker_info", map[string]any{})
	if !errors.Is(err, ops.ErrExec) {
		t.Fatalf("docker_info without a docker CLI = %v, want ops.ErrExec", err)
	}
}

func TestComposePS(t *testing.T) {
	const id = "abc123def456"
	cases := []struct {
		name      string
		out       string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "array form",
			out:       `[{"ID":"` + id + `","Name":"p-kafka-1","Service":"kafka","State":"running","Health":"healthy","Image":"apache/kafka:4.1.0"}]`,
			wantCount: 1,
		},
		{
			name:      "one object per line",
			out:       "{\"ID\":\"" + id + "\",\"Service\":\"kafka\",\"State\":\"running\"}\n{\"ID\":\"fed321cba654\",\"Service\":\"connect\",\"State\":\"exited\",\"ExitCode\":1}\n",
			wantCount: 2,
		},
		{"no containers", "", 0, false},
		{"malformed", "{not json", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg, fr, dir, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
				return []byte(tc.out), 0, nil
			})
			res, err := runOp(t, reg, "compose_ps", map[string]any{})
			if tc.wantErr {
				if err == nil {
					t.Fatal("compose_ps accepted malformed output")
				}
				return
			}
			if err != nil {
				t.Fatalf("compose_ps: %v", err)
			}
			d := data(t, res)
			if got := int(d["count"].(float64)); got != tc.wantCount {
				t.Errorf("count = %d, want %d", got, tc.wantCount)
			}
			if tc.wantCount == 0 && !strings.Contains(res.Summary, "not running") {
				t.Errorf("summary = %q, want it to say the stack is not running", res.Summary)
			}
			assertArgv(t, fr.calls[0], append(baseArgv(dir, "ps"), "--format", "json"))
		})
	}
}

func TestComposePSCommandFails(t *testing.T) {
	reg, _, _, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
		return []byte("no such project"), 1, nil
	})
	res, err := runOp(t, reg, "compose_ps", map[string]any{})
	if !errors.Is(err, ops.ErrExec) {
		t.Fatalf("compose_ps = %v, want ops.ErrExec", err)
	}
	if got := data(t, res)["exit_code"]; got != float64(1) {
		t.Errorf("Data exit_code = %v, want 1", got)
	}
}

func TestComposeConfigMasksCredentialValues(t *testing.T) {
	cases := []struct {
		name   string
		doc    string
		absent []string
	}{
		{
			name: "environment mapping",
			doc: "name: piperoom-setup\nservices:\n  postgres:\n    environment:\n" +
				"      POSTGRES_DB: orders\n      POSTGRES_PASSWORD: hunter2\n      POSTGRES_USER: piperoom\n",
			absent: []string{"hunter2"},
		},
		{
			name: "environment list",
			doc: "name: piperoom-setup\nservices:\n  postgres:\n    environment:\n" +
				"      - POSTGRES_PASSWORD=hunter2\n      - POSTGRES_DB=orders\n",
			absent: []string{"hunter2"},
		},
		{
			name:   "nested credential key",
			doc:    "name: piperoom-setup\nservices:\n  connect:\n    config:\n      sasl:\n        password: topsecret\n      topic: orders\n",
			absent: []string{"topsecret"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg, fr, dir, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
				return []byte(tc.doc), 0, nil
			})
			res, err := runOp(t, reg, "compose_config", map[string]any{})
			if err != nil {
				t.Fatalf("compose_config: %v", err)
			}
			d := data(t, res)
			body, _ := d["body"].(string)
			if body == "" {
				t.Fatal("compose_config returned no body")
			}
			for _, secret := range tc.absent {
				if strings.Contains(body, secret) || strings.Contains(res.Summary, secret) {
					t.Fatalf("the masked render still carries %q:\n%s", secret, body)
				}
			}
			if !strings.Contains(body, maskMarker) {
				t.Errorf("the masked render has no %q marker:\n%s", maskMarker, body)
			}
			if keys, _ := d["masked_keys"].([]any); len(keys) == 0 {
				t.Error("masked_keys is empty")
			}
			if !strings.Contains(body, "orders") {
				t.Errorf("the mask hid a non-credential value:\n%s", body)
			}
			assertArgv(t, fr.calls[0], append(baseArgv(dir, "config"), "--no-interpolate"))
		})
	}
}

func TestComposeConfigRefusesServices(t *testing.T) {
	reg, _, _, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
		return nil, 0, errors.New("must not run")
	})
	if _, err := runOp(t, reg, "compose_config", map[string]any{"services": []string{"kafka"}}); !errors.Is(err, ErrParams) {
		t.Fatalf("compose_config with services = %v, want ErrParams", err)
	}
}

// TestContainerInspectDropsEnvValues is the row's explicit test: the output
// has environment variable names and no values.
func TestContainerInspectDropsEnvValues(t *testing.T) {
	const id = "abc123def456"
	dir := t.TempDir()
	fixture := `{"Id":"` + id + `0000","Name":"/piperoom-setup-connect-1","Image":"sha256:deadbeef",` +
		`"Hostname":"secret-host","Config":{"Image":"quay.io/debezium/connect:3.2.0.Final@sha256:b79f",` +
		`"Env":["POSTGRES_PASSWORD=hunter2","BOOTSTRAP_SERVERS=kafka:9092","POSTGRES_PASSWORD=hunter2"],` +
		`"Cmd":["hunter2"],"Labels":{"com.docker.compose.project":"piperoom-setup","com.docker.compose.service":"connect","other":"hunter2"}},` +
		`"State":{"Status":"running","Running":true,"ExitCode":0,"Health":{"Status":"healthy"}},` +
		`"HostConfig":{"NetworkMode":"bridge"},` +
		`"NetworkSettings":{"Networks":{"piperoom_default":{}}},` +
		`"Mounts":[{"Type":"bind","Source":"` + filepath.ToSlash(filepath.Join(dir, PluginDir)) + `","Destination":"/kafka/connect","RW":true},` +
		`{"Type":"volume","Name":"vol1","Source":"/var/lib/docker/volumes/vol1/_data","Destination":"/var/lib/kafka/data","RW":true}]}`
	fr := &fakeRunner{fn: func(argv []string) ([]byte, int, error) {
		if argv[1] == "compose" {
			return []byte(id + "\n"), 0, nil
		}
		if argv[1] == "inspect" {
			return []byte(fixture), 0, nil
		}
		return nil, 1, fmt.Errorf("unexpected argv %q", argv)
	}}
	reg, err := ops.New(ops.Config{ProjectDir: dir, Exec: fr.exec})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if _, err := Register(reg, Options{}); err != nil {
		t.Fatalf("compose.Register: %v", err)
	}
	writeStackFiles(t, dir)

	res, err := runOp(t, reg, "container_inspect", map[string]any{"service": "connect"})
	if err != nil {
		t.Fatalf("container_inspect: %v", err)
	}
	whole := res.Summary + "\n" + string(res.Data)
	if strings.Contains(whole, "hunter2") {
		t.Fatalf("an environment value reached the result:\n%s", whole)
	}
	if strings.Contains(whole, "secret-host") {
		t.Fatalf("a field outside the whitelist reached the result:\n%s", whole)
	}
	d := data(t, res)
	containers, _ := d["containers"].([]any)
	if len(containers) != 1 {
		t.Fatalf("containers = %v, want one", d["containers"])
	}
	c := containers[0].(map[string]any)
	names, _ := c["env_names"].([]any)
	want := []string{"BOOTSTRAP_SERVERS", "POSTGRES_PASSWORD"}
	if fmt.Sprint(names) != fmt.Sprint(want) {
		t.Errorf("env_names = %v, want %v", names, want)
	}
	labels, _ := c["labels"].(map[string]any)
	if len(labels) != 2 {
		t.Errorf("labels = %v, want only the two compose labels", labels)
	}
	if c["state"] != "running" || c["health"] != "healthy" || c["running"] != true {
		t.Errorf("state/health/running = %v/%v/%v", c["state"], c["health"], c["running"])
	}
	mounts, _ := c["mounts"].([]any)
	if len(mounts) != 2 {
		t.Fatalf("mounts = %v, want two", mounts)
	}
	bind := mounts[0].(map[string]any)
	if bind["source"] != PluginDir {
		t.Errorf("bind source = %v, want the project-relative %q", bind["source"], PluginDir)
	}
	volume := mounts[1].(map[string]any)
	if volume["name"] != "vol1" {
		t.Errorf("volume name = %v", volume["name"])
	}
	if _, ok := volume["source"]; ok {
		t.Errorf("a host volume path outside the project reached the result: %v", volume["source"])
	}
	if len(fr.calls) != 2 {
		t.Fatalf("calls = %q, want two", fr.calls)
	}
	assertArgv(t, fr.calls[0], append(baseArgv(dir, "ps"), "-q", "connect"))
	assertArgv(t, fr.calls[1], []string{"docker", "inspect", "--type", "container", "--format", "{{json .}}", id})
}

func TestContainerInspectNoContainer(t *testing.T) {
	reg, _, _, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
		return []byte("\n"), 0, nil
	})
	_, err := runOp(t, reg, "container_inspect", map[string]any{"service": "connect"})
	if !errors.Is(err, ErrStack) {
		t.Fatalf("container_inspect with an empty ps = %v, want ErrStack", err)
	}
}

func TestContainerInspectBadJSON(t *testing.T) {
	reg, _, _, _ := newDockerHarness(t, Options{}, func(argv []string) ([]byte, int, error) {
		if argv[1] == "compose" {
			return []byte("abc123def456\n"), 0, nil
		}
		return []byte("{truncated"), 0, nil
	})
	if _, err := runOp(t, reg, "container_inspect", map[string]any{"service": "connect"}); err == nil {
		t.Fatal("container_inspect accepted truncated inspect output")
	}
}

// TestReadOpsCatalogue pins the tier, target and layer of every read op, and
// that a bad param set is refused before any command runs.
func TestReadOpsCatalogue(t *testing.T) {
	reg, fr, _, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
		return nil, 0, nil
	})
	for _, name := range []string{"docker_info", "compose_ps", "compose_config", "container_inspect"} {
		op, ok := reg.Get(name)
		if !ok {
			t.Fatalf("%s is not registered", name)
		}
		if op.Tier() != protocol.TierT0 || op.Target() != targetKind || op.Layer() != layerName {
			t.Errorf("%s: tier %s target %s layer %s", name, op.Tier(), op.Target(), op.Layer())
		}
		if op.ParamsSchema() == nil {
			t.Errorf("%s has no params schema", name)
		}
	}
	before := len(fr.calls)
	if _, err := runOp(t, reg, "container_inspect", map[string]any{"service": "-f"}); !errors.Is(err, ErrParams) {
		t.Fatalf("container_inspect(-f) = %v, want ErrParams", err)
	}
	if len(fr.calls) != before {
		t.Fatal("a refused container_inspect still ran a command")
	}
}
