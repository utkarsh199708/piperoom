package host

import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/compose"
)

// fakeJava is where the fake PATH lookup finds java.
const fakeJava = "/opt/jdk-21/bin/java"

// fakeExec records every command and answers through respond.
type fakeExec struct {
	mu      sync.Mutex
	calls   [][]string
	respond func(args []string) ([]byte, int, error)
}

func (f *fakeExec) run(_ context.Context, name string, args ...string) ([]byte, int, error) {
	f.mu.Lock()
	f.calls = append(f.calls, append([]string{name}, args...))
	f.mu.Unlock()
	if f.respond != nil {
		return f.respond(args)
	}
	return nil, 0, nil
}

func (f *fakeExec) argvs() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]string(nil), f.calls...)
}

// fixture is a project directory with a registry and the host driver.
type fixture struct {
	t    *testing.T
	dir  string
	reg  *ops.Registry
	drv  *Driver
	exec *fakeExec
	java string // what the fake PATH lookup returns; "" means not found
}

func newFixture(t *testing.T, opts Options) *fixture {
	t.Helper()
	return newFixtureIn(t, t.TempDir(), opts)
}

func newFixtureIn(t *testing.T, dir string, opts Options) *fixture {
	t.Helper()
	if (opts.GOOS == "" || opts.GOOS == "linux") && runtime.GOOS == "windows" {
		// A simulated Linux runner on a Windows test machine gets real
		// backslash temp paths, which no systemd unit can hold (the unit-path
		// rule refuses them, as it should) and NTFS modes. These run on the
		// ubuntu and macOS legs; the GOOS "windows" fixtures still run here.
		t.Skip("simulates a Linux runner; needs a POSIX temp dir (runs on ubuntu and macOS)")
	}
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
		Now: func() time.Time { return time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if opts.GOOS == "" {
		opts.GOOS = "linux"
	}
	if opts.ResolvePath == nil {
		opts.ResolvePath = func(p string) (string, error) { return p, nil }
	}
	if opts.geteuid == nil {
		// An unprivileged runner unless a test says otherwise: a CI
		// container that runs tests as root must not flip every fixture to
		// the system scope.
		opts.geteuid = func() int { return 1000 }
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
	raw, err := json.Marshal(params)
	if err != nil {
		f.t.Fatalf("marshal: %v", err)
	}
	return f.reg.Execute(context.Background(), protocol.OpRequest{ID: "r1", Op: op, Params: raw, Tier: protocol.TierT2, Class: protocol.ClassLocal})
}

// env is an ops.Env over the fixture's project, for the Preview methods.
func (f *fixture) env() ops.Env {
	c, err := ops.OpenConfiner(f.dir)
	if err != nil {
		f.t.Fatalf("OpenConfiner: %v", err)
	}
	f.t.Cleanup(func() { _ = c.Close() })
	return ops.Env{ProjectDir: c.Dir(), Confine: c, Exec: f.exec.run, Now: time.Now,
		LookPath: func(string) (string, error) { return f.java, nil }}
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

func (f *fixture) read(name string) string {
	f.t.Helper()
	b, err := os.ReadFile(filepath.Join(f.dir, filepath.FromSlash(name)))
	if err != nil {
		f.t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// slashed is a host path in the forward slash form a worker properties value
// carries: the same on Linux, C:/... for a C:\... project directory.
func slashed(p string) string { return strings.ReplaceAll(p, "\\", "/") }

// distribution lays out a Kafka distribution and both worker files.
func (f *fixture) distribution() {
	f.write("kafka/bin/connect-distributed.sh", "#!/bin/sh\n", 0o755)
	f.write("kafka/bin/connect-standalone.sh", "#!/bin/sh\n", 0o755)
	f.write("connect-distributed.properties", "bootstrap.servers=127.0.0.1:9092\n", 0o600)
	f.write("connect-standalone.properties", "bootstrap.servers=127.0.0.1:9092\n", 0o600)
}

func TestCatalogue(t *testing.T) {
	f := newFixture(t, Options{})
	want := map[string]string{
		"service_status": protocol.TierT0, "properties_read": protocol.TierT0,
		"properties_write": protocol.TierT1,
		"service_install":  protocol.TierT2, "service_restart": protocol.TierT2,
		"journal_tail": protocol.TierT0,
	}
	for name, tier := range want {
		op, ok := f.reg.Get(name)
		if !ok {
			t.Fatalf("%s is not registered", name)
		}
		if op.Tier() != tier || op.Target() != "host" || op.Layer() != "environment" {
			t.Errorf("%s: tier %s target %s layer %s", name, op.Tier(), op.Target(), op.Layer())
		}
		if op.ParamsSchema()["additionalProperties"] != false {
			t.Errorf("%s: schema allows additional properties", name)
		}
	}
	// What the unit executes or loads is out of file_write's and
	// file_delete's reach from the start.
	for _, p := range []string{"kafka/config/x.txt", "connect-plugins/a/b.txt", "systemd/notes.txt"} {
		f.write(p, "keep\n", 0o644)
		if _, err := f.run("file_write", map[string]string{"path": p, "content": "x"}); !errors.Is(err, ops.ErrExecutedName) {
			t.Errorf("file_write %s: %v, want ErrExecutedName", p, err)
		}
		if _, err := f.run("file_delete", map[string]string{"path": p}); !errors.Is(err, ops.ErrExecutedName) {
			t.Errorf("file_delete %s: %v, want ErrExecutedName", p, err)
		}
		if got := f.read(p); got != "keep\n" {
			t.Errorf("%s changed: %q", p, got)
		}
	}
	// One room has one target: the Compose driver cannot join this registry.
	if _, err := compose.Register(f.reg, compose.Options{}); err == nil {
		t.Error("compose.Register on a host registry succeeded; properties_write would be ambiguous")
	}
	if PluginDir != compose.PluginDir {
		t.Errorf("PluginDir %q differs from compose.PluginDir %q: plugin_install fills the wrong directory", PluginDir, compose.PluginDir)
	}
}

func TestRegisterRefusesUnknownClass(t *testing.T) {
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	if _, err := Register(reg, Options{Class: "staging"}); err == nil {
		t.Fatal("unknown class accepted")
	}
	if _, err := Register(nil, Options{}); err == nil {
		t.Fatal("nil registry accepted")
	}
}

func TestUnsupportedOSRefusesServiceOps(t *testing.T) {
	// A target OS this build has no service manager for: no systemd unit, no
	// service control manager. (Windows is a manager now: row U8b, tested in
	// winsvc_test.go with a fake and on the Windows dev machine.)
	f := newFixture(t, Options{GOOS: "plan9"})
	f.distribution()
	for _, op := range []string{"service_status", "service_install", "service_restart"} {
		if _, err := f.run(op, map[string]string{}); !errors.Is(err, ErrUnsupported) {
			t.Errorf("%s on an OS without a service manager: %v, want ErrUnsupported", op, err)
		}
	}
	if len(f.exec.argvs()) != 0 {
		t.Errorf("commands ran: %v", f.exec.argvs())
	}
	if _, err := f.run("properties_read", map[string]string{"file": "connect-distributed.properties"}); err != nil {
		t.Errorf("properties_read on an OS without a service manager: %v", err)
	}
}

// shellNames are programs that run a command string.
var shellNames = map[string]bool{"sh": true, "bash": true, "dash": true, "zsh": true, "ksh": true, "busybox": true,
	"/bin/sh": true, "/bin/bash": true, "env": true, "/usr/bin/env": true, "cmd": true, "cmd.exe": true,
	"powershell": true, "powershell.exe": true, "pwsh": true, "sudo": true, "su": true, "pkexec": true}

// TestNoShellExecution is the package's AST test (the rule of
// internal/setup/ops/shell_ast_test.go): no non-test file imports os/exec or
// syscall, and every call of an Exec field names a program that is a string
// constant of this package and not a shell, so argv is the only way a
// command is built. Two files are exempt, each for one call into a Windows API
// that starts a process without a shell: child.go (exec.Command with the argv
// slice the service template built; a service must keep its child alive and
// watch its exit, which ops.Env.Exec cannot do) and winsvc_windows.go (syscall
// for the Win32 error mapping; it starts nothing).
func TestNoShellExecution(t *testing.T) {
	exempt := map[string]map[string]bool{
		"child.go":          {"os/exec": true},
		"winsvc_windows.go": {"syscall": true},
	}
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	consts := map[string]string{}
	var files []*ast.File
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		files = append(files, f)
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, n := range vs.Names {
					if i < len(vs.Values) {
						if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
							consts[n.Name], _ = strconv.Unquote(lit.Value)
						}
					}
				}
			}
		}
	}
	calls := 0
	for _, f := range files {
		name := fset.Position(f.Pos()).Filename
		name = name[strings.LastIndexAny(name, `/\`)+1:]
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if p != "os/exec" && p != "syscall" && p != "golang.org/x/sys/unix" {
				continue
			}
			if exempt[name][p] {
				continue
			}
			t.Errorf("%s imports %s: commands go through ops.Env.Exec with an argv", fset.Position(imp.Pos()), p)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Exec" {
				return true
			}
			calls++
			if len(call.Args) < 2 {
				t.Errorf("%s: Exec call without a program", fset.Position(call.Pos()))
				return true
			}
			var prog string
			switch a := call.Args[1].(type) {
			case *ast.BasicLit:
				prog, _ = strconv.Unquote(a.Value)
			case *ast.Ident:
				v, ok := consts[a.Name]
				if !ok {
					t.Errorf("%s: program %s is not a string constant", fset.Position(call.Pos()), a.Name)
				}
				prog = v
			default:
				t.Errorf("%s: program is not a constant", fset.Position(call.Pos()))
			}
			if shellNames[prog] || prog == "" {
				t.Errorf("%s: program %q runs a command string", fset.Position(call.Pos()), prog)
			}
			return true
		})
	}
	if calls == 0 {
		t.Fatal("no Exec call found: the check would pass vacuously")
	}
}
