package server

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"piperoom/internal/setup/knowledge"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// fakeExec is the injectable command runner: it records every argv and answers
// with the fixture's respond function.
type fakeExec struct {
	mu      sync.Mutex
	respond func(argv []string) ([]byte, int, error)
	calls   [][]string
}

func (f *fakeExec) run(_ context.Context, name string, args ...string) ([]byte, int, error) {
	argv := append([]string{name}, args...)
	f.mu.Lock()
	f.calls = append(f.calls, argv)
	f.mu.Unlock()
	if f.respond == nil {
		return nil, 0, nil
	}
	return f.respond(argv)
}

func (f *fakeExec) argvs() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]string, len(f.calls))
	copy(out, f.calls)
	return out
}

// fixture is a registered driver over a temporary project directory.
type fixture struct {
	t    *testing.T
	dir  string
	exec *fakeExec
	reg  *ops.Registry
	drv  *Driver
	java string
}

const (
	fakeJava     = "/usr/lib/jvm/java-21-openjdk/bin/java"
	fakeJavaHome = "/usr/lib/jvm/java-21-openjdk"
)

func newFixture(t *testing.T, opts Options) *fixture {
	t.Helper()
	return newFixtureIn(t, t.TempDir(), opts)
}

func newFixtureIn(t *testing.T, dir string, opts Options) *fixture {
	t.Helper()
	f := &fixture{t: t, dir: dir, exec: &fakeExec{}, java: fakeJava}
	reg, err := ops.New(ops.Config{
		ProjectDir: dir,
		Exec:       f.exec.run,
		LookPath: func(name string) (string, error) {
			if name != "java" || f.java == "" {
				return "", errors.New("not found")
			}
			return f.java, nil
		},
		Now: func() time.Time { return time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	// The unit paths are POSIX; the tests that render a unit run on every
	// platform against a POSIX project path they choose themselves, so the
	// driver's platform is fixed to Linux the way the host target's tests do.
	if opts.GOOS == "" {
		opts.GOOS = "linux"
	}
	if opts.ResolvePath == nil {
		opts.ResolvePath = func(p string) (string, error) { return p, nil }
	}
	if opts.Index == nil {
		ix, err := knowledge.LoadIndex()
		if err != nil {
			t.Fatalf("knowledge.LoadIndex: %v", err)
		}
		opts.Index = ix
	}
	drv, err := Register(reg, opts)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	f.reg, f.drv = reg, drv
	return f
}

// run executes one op through the catalogue, as the runner does.
func (f *fixture) run(op string, params any) (protocol.OpResult, error) {
	f.t.Helper()
	var raw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			f.t.Fatalf("marshal: %v", err)
		}
		raw = b
	}
	return f.reg.Execute(context.Background(), protocol.OpRequest{ID: "r1", Op: op, Params: raw, Tier: protocol.TierT2, Class: protocol.ClassLocal})
}

// data decodes an op result's Data into a map.
func (f *fixture) data(res protocol.OpResult) map[string]any {
	f.t.Helper()
	var out map[string]any
	if err := json.Unmarshal(res.Data, &out); err != nil {
		f.t.Fatalf("decoding Data: %v (%s)", err, res.Data)
	}
	return out
}

// write creates a project file with mode.
func (f *fixture) write(name, content string, mode os.FileMode) {
	f.t.Helper()
	p := filepath.Join(f.dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		f.t.Fatal(err)
	}
}

// read returns a project file's content.
func (f *fixture) read(name string) string {
	f.t.Helper()
	b, err := os.ReadFile(filepath.Join(f.dir, filepath.FromSlash(name)))
	if err != nil {
		f.t.Fatalf("reading %s: %v", name, err)
	}
	return string(b)
}

// exists reports whether a project path exists.
func (f *fixture) exists(name string) bool {
	f.t.Helper()
	_, err := os.Lstat(filepath.Join(f.dir, filepath.FromSlash(name)))
	return err == nil
}

// distribution lays down the minimum a unit needs: the launcher and the
// properties file.
func (f *fixture) distribution() {
	f.t.Helper()
	f.write(DistDir+"/run.sh", "#!/bin/bash\nexec java io.debezium.server.Main\n", 0o755)
	f.write(ConfigFile, "debezium.sink.type=http\n", 0o644)
}
