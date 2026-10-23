package compose

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// TestStackArgvPerOp is the row's argv test per op: the pinned head, the one
// fixed flag sequence of the subcommand, then the sorted services.
func TestStackArgvPerOp(t *testing.T) {
	cases := []struct {
		op       string
		params   map[string]any
		wantTail []string
		down     bool
	}{
		{"compose_up", map[string]any{"services": []string{"kafka", "connect"}}, []string{"up", "-d", "connect", "kafka"}, false},
		{"compose_restart", map[string]any{}, []string{"restart"}, false},
		{"compose_down", map[string]any{"services": []string{"connect"}}, []string{"down", "connect"}, true},
		{"compose_ps", map[string]any{}, []string{"ps", "--format", "json"}, false},
		{"compose_config", map[string]any{}, []string{"config", "--no-interpolate"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			reg, fr, dir, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
				switch tc.op {
				case "compose_ps":
					return []byte("[]"), 0, nil
				case "compose_config":
					return []byte("name: piperoom-setup\nservices:\n  kafka:\n    image: apache/kafka:4.1.0\n"), 0, nil
				}
				return []byte("ok"), 0, nil
			})
			res, err := runOp(t, reg, tc.op, tc.params)
			if err != nil {
				t.Fatalf("%s: %v", tc.op, err)
			}
			assertArgv(t, fr.calls[0], append(baseArgv(dir, tc.wantTail[0]), tc.wantTail[1:]...))
			if tc.op == "compose_down" {
				if data(t, res)["volumes_kept"] != true {
					t.Error("compose_down does not say its volumes were kept")
				}
				for _, arg := range fr.calls[0] {
					if arg == "-v" || arg == "--volumes" {
						t.Fatalf("compose_down argv removes volumes: %q", fr.calls[0])
					}
				}
			}
		})
	}
}

// TestStackFilesRequired: compose refuses a missing --env-file, so the op
// reports which step is missing before docker runs.
func TestStackFilesRequired(t *testing.T) {
	dir := t.TempDir()
	fr := &fakeRunner{fn: func([]string) ([]byte, int, error) {
		t.Fatal("a command ran before the stack's files existed")
		return nil, 0, nil
	}}
	reg, err := ops.New(ops.Config{ProjectDir: dir, Exec: fr.exec})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if _, err := Register(reg, Options{}); err != nil {
		t.Fatalf("compose.Register: %v", err)
	}
	if _, err := runOp(t, reg, "compose_up", map[string]any{}); !errors.Is(err, ErrStack) {
		t.Fatalf("compose_up in an empty directory = %v, want ErrStack", err)
	}
	rendered, err := (&Driver{}).Render(WriteParams{Templates: []string{TemplateKafka}})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, rendered.File), rendered.Doc, 0o644); err != nil {
		t.Fatal(err)
	}
	// The compose file is there; .env is not.
	if _, err := runOp(t, reg, "compose_up", map[string]any{}); !errors.Is(err, ErrStack) {
		t.Fatalf("compose_up without .env = %v, want ErrStack", err)
	}
}

func TestStackCommandFails(t *testing.T) {
	reg, _, _, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
		return []byte("Error response from daemon: pull access denied"), 1, nil
	})
	res, err := runOp(t, reg, "compose_up", map[string]any{})
	if !errors.Is(err, ops.ErrExec) {
		t.Fatalf("compose_up = %v, want ops.ErrExec", err)
	}
	d := data(t, res)
	if d["exit_code"] != float64(1) {
		t.Errorf("exit_code = %v, want 1", d["exit_code"])
	}
	if out, _ := d["output"].(string); !strings.Contains(out, "pull access denied") {
		t.Errorf("Data output = %q, want the docker error", out)
	}
	if !strings.Contains(res.Summary, "pull access denied") {
		t.Errorf("summary = %q, want the first error line", res.Summary)
	}
}

func TestStackParams(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]any
		want   error
	}{
		{"a flag as a service", map[string]any{"services": []string{"-d"}}, ErrParams},
		{"a path as a service", map[string]any{"services": []string{"../connect"}}, ErrParams},
		{"too many services", map[string]any{"services": []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q"}}, ErrParams},
		{"an unknown field", map[string]any{"volumes": true}, ErrParams},
		{"a non-compose file", map[string]any{"file": "worker.properties"}, ErrParams},
		{"an escaping file", map[string]any{"file": "../compose.yaml"}, ErrPath},
	}
	reg, fr, _, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
		return nil, 0, nil
	})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := len(fr.calls)
			if _, err := runOp(t, reg, "compose_down", tc.params); !errors.Is(err, tc.want) {
				t.Fatalf("compose_down(%v) = %v, want %v", tc.params, err, tc.want)
			}
			if len(fr.calls) != before {
				t.Fatal("a refused op still ran a command")
			}
		})
	}
}

// TestStackArgvHasNoVolumeRemoval: no caller-supplied flag can reach the argv,
// and `down` carries no -v.
func TestStackArgvHasNoVolumeRemoval(t *testing.T) {
	dir := t.TempDir()
	for _, sub := range []string{"down", "restart", "up"} {
		for _, flags := range [][]string{nil, {"-v"}, {"--volumes"}, {"-t", "0"}, {"--remove-orphans"}} {
			if _, err := stackArgv(dir, DefaultFile, sub, flags, nil); err == nil && len(flags) > 0 {
				t.Errorf("stackArgv(%q, %q) accepted caller flags", sub, flags)
			}
		}
	}
	argv, err := stackArgv(dir, DefaultFile, "up", []string{"-d"}, []string{"connect", "kafka"})
	if err != nil {
		t.Fatalf("stackArgv: %v", err)
	}
	want := []string{"docker", "compose", "--project-directory", dir,
		"-f", filepath.Join(dir, DefaultFile), "--env-file", filepath.Join(dir, EnvFileName),
		"up", "-d", "connect", "kafka"}
	assertArgv(t, argv, want)
}

func TestWriteOpsCatalogue(t *testing.T) {
	reg, _, _, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
		return []byte("ok"), 0, nil
	})
	for _, name := range []string{"compose_up", "compose_restart", "compose_down"} {
		op, ok := reg.Get(name)
		if !ok {
			t.Fatalf("%s is not registered", name)
		}
		if op.Tier() != protocol.TierT1 || op.Target() != targetKind || op.Layer() != layerName {
			t.Errorf("%s: tier %s target %s layer %s", name, op.Tier(), op.Target(), op.Layer())
		}
	}
	for _, name := range []string{"plugin_install"} {
		op, ok := reg.Get(name)
		if !ok {
			t.Fatalf("%s is not registered", name)
		}
		if op.Tier() != protocol.TierT1 || op.Target() != targetKind || op.Layer() != "kafka-connect" {
			t.Errorf("%s: tier %s target %s layer %s", name, op.Tier(), op.Target(), op.Layer())
		}
	}
	if _, err := Register(reg, Options{}); err == nil {
		t.Error("a second Register on the same catalogue passed")
	}
	if _, err := Register(nil, Options{}); err == nil {
		t.Error("Register accepted a nil registry")
	}
	if _, err := Register(reg, Options{Class: "prd"}); err == nil {
		t.Error("Register accepted an unknown class")
	}
}

// TestStackSummary keeps the down promise visible in the one line a person
// reads.
func TestStackSummary(t *testing.T) {
	op := &stackWriteOp{sub: "down"}
	if got := op.successSummary("compose.yaml", nil); !strings.Contains(got, "volumes kept") {
		t.Errorf("down summary = %q, want it to say the volumes were kept", got)
	}
	if got := (&stackWriteOp{sub: "up"}).successSummary("compose.yaml", []string{"kafka"}); !strings.Contains(got, "kafka") {
		t.Errorf("up summary = %q, want the scope", got)
	}
}
