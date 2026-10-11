package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/discover"
)

// Fakes for the read-only interfaces. Connect fails so the real Discover
// returns an error both while it is a stub and once it is implemented.
type fakeConnect struct{ err error }

func (f fakeConnect) ListConnectors(context.Context) ([]string, error) { return nil, f.err }
func (f fakeConnect) Config(context.Context, string) (map[string]string, error) {
	return nil, clients.ErrNotFound
}

type fakeKafka struct{}

func (fakeKafka) Topics(context.Context) ([]string, error)         { return nil, nil }
func (fakeKafka) ConsumerGroups(context.Context) ([]string, error) { return nil, nil }
func (fakeKafka) GroupLag(context.Context, string) (clients.GroupLag, error) {
	return clients.GroupLag{}, clients.ErrNotFound
}

type fakeRegistry struct{}

func (fakeRegistry) Subjects(context.Context) ([]string, error) { return nil, nil }

var errListFailed = errors.New("list connectors failed")

// fakeBuild returns failing fake clients and records whether close ran.
func fakeBuild(closed *bool) buildFunc {
	return func(context.Context, config.Config, bool) (discover.Deps, func(), error) {
		deps := discover.Deps{Connect: fakeConnect{err: errListFailed}, Kafka: fakeKafka{}, Registry: fakeRegistry{}}
		return deps, func() { *closed = true }, nil
	}
}

func okDiscover(context.Context, discover.Deps, discover.Options) (discover.Result, error) {
	return discover.Result{
		Pipelines: []discover.Pipeline{
			{ID: "pg-orders", Connector: "pg-orders-connector", TopicPrefix: "pg", SourceTables: []string{"public.orders"}},
			{ID: "pg-billing", Connector: "pg-billing-connector", TopicPrefix: "billing"},
		},
		Skipped:  []discover.Skipped{{Connector: "s3-sink", Reason: "not a Debezium Postgres source"}},
		Warnings: []string{"two connectors share prefix pg"},
	}, nil
}

const draft = "pipelines: []\n"

func okRender(w io.Writer, _ discover.Result) error {
	_, err := io.WriteString(w, draft)
	return err
}

// testRunner never touches the network: every seam is a fake unless the
// test swaps one in.
func testRunner(closed *bool) runner {
	return runner{build: fakeBuild(closed), discover: okDiscover, render: okRender}
}

func runT(t *testing.T, r runner, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errb bytes.Buffer
	code = r.run(context.Background(), args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestFlagErrors(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name string
		args []string
	}{
		{"missing out", nil},
		{"empty out", []string{"-out", ""}},
		{"unknown flag", []string{"-out", filepath.Join(dir, "a.yaml"), "-nope"}},
		{"positional arg", []string{"-out", filepath.Join(dir, "b.yaml"), "extra"}},
		{"bad timeout", []string{"-out", filepath.Join(dir, "c.yaml"), "-timeout", "soon"}},
		{"zero timeout", []string{"-out", filepath.Join(dir, "d.yaml"), "-timeout", "0s"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			built := false
			r := testRunner(new(bool))
			r.build = func(context.Context, config.Config, bool) (discover.Deps, func(), error) {
				built = true
				return discover.Deps{}, func() {}, nil
			}
			code, _, stderr := runT(t, r, tt.args...)
			if code != exitUsage {
				t.Fatalf("exit = %d, want %d; stderr:\n%s", code, exitUsage, stderr)
			}
			if built {
				t.Error("clients were built despite bad flags")
			}
			entries, _ := os.ReadDir(dir)
			if len(entries) != 0 {
				t.Errorf("bad flags created files: %v", entries)
			}
		})
	}
}

func TestHelpExitsZero(t *testing.T) {
	code, _, stderr := runT(t, testRunner(new(bool)), "-h")
	if code != exitOK {
		t.Fatalf("exit = %d, want 0", code)
	}
	if !strings.Contains(stderr, "-out") {
		t.Errorf("usage missing -out:\n%s", stderr)
	}
}

func TestWritesFileAndSummary(t *testing.T) {
	out := filepath.Join(t.TempDir(), "owners.draft.yaml")
	closed := false
	code, stdout, stderr := runT(t, testRunner(&closed), "-out", out)
	if code != exitOK {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != draft {
		t.Errorf("file = %q, want %q", got, draft)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty when -out is a file", stdout)
	}
	for _, want := range []string{
		"warning: two connectors share prefix pg\n",
		"discover: 2 pipelines, 1 skipped connectors, 1 warnings\n",
		"discover: 1 pipelines have no source_tables (fill them in before loading)\n",
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr missing %q:\n%s", want, stderr)
		}
	}
	if !closed {
		t.Error("clients were not closed")
	}
}

func TestNoEmptySourceTablesLineWhenAllFilled(t *testing.T) {
	r := testRunner(new(bool))
	r.discover = func(context.Context, discover.Deps, discover.Options) (discover.Result, error) {
		return discover.Result{Pipelines: []discover.Pipeline{{ID: "p", SourceTables: []string{"public.t"}}}}, nil
	}
	code, _, stderr := runT(t, r, "-out", filepath.Join(t.TempDir(), "x.yaml"))
	if code != exitOK {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	if strings.Contains(stderr, "no source_tables") {
		t.Errorf("unexpected empty-source_tables line:\n%s", stderr)
	}
}

func TestStdout(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	code, stdout, stderr := runT(t, testRunner(new(bool)), "-out", "-")
	if code != exitOK {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	if stdout != draft {
		t.Errorf("stdout = %q, want %q", stdout, draft)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("-out - created files: %v", entries)
	}
}

func TestExistingFileWithoutForce(t *testing.T) {
	out := filepath.Join(t.TempDir(), "owners.draft.yaml")
	const old = "hand edited\n"
	if err := os.WriteFile(out, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	built := false
	r := testRunner(new(bool))
	r.build = func(ctx context.Context, cfg config.Config, src bool) (discover.Deps, func(), error) {
		built = true
		return fakeBuild(new(bool))(ctx, cfg, src)
	}
	code, _, stderr := runT(t, r, "-out", out)
	if code != exitError {
		t.Fatalf("exit = %d, want %d; stderr:\n%s", code, exitError, stderr)
	}
	if !strings.Contains(stderr, "already exists") {
		t.Errorf("stderr does not explain the refusal:\n%s", stderr)
	}
	if built {
		t.Error("cluster was read although the output could not be written")
	}
	assertFile(t, out, old)
}

func TestFileAppearsDuringRun(t *testing.T) {
	// The pre-check passes, then another process creates the file: O_EXCL
	// must still refuse to overwrite it.
	out := filepath.Join(t.TempDir(), "owners.draft.yaml")
	const old = "written meanwhile\n"
	r := testRunner(new(bool))
	r.discover = func(ctx context.Context, d discover.Deps, o discover.Options) (discover.Result, error) {
		if err := os.WriteFile(out, []byte(old), 0o644); err != nil {
			t.Fatal(err)
		}
		return okDiscover(ctx, d, o)
	}
	code, _, stderr := runT(t, r, "-out", out)
	if code != exitError {
		t.Fatalf("exit = %d, want %d; stderr:\n%s", code, exitError, stderr)
	}
	assertFile(t, out, old)
}

func TestForceOverwrites(t *testing.T) {
	out := filepath.Join(t.TempDir(), "owners.draft.yaml")
	if err := os.WriteFile(out, []byte("a much longer old file than the new draft\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, _, stderr := runT(t, testRunner(new(bool)), "-force", "-out", out)
	if code != exitOK {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	assertFile(t, out, draft)
}

func TestFailedDiscoverTouchesNoFile(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "existing.yaml")
	const old = "keep me\n"
	if err := os.WriteFile(existing, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	fresh := filepath.Join(dir, "fresh.yaml")

	// The real library with failing fake clients: an error now (stub) and
	// later (wrapped ListConnectors error).
	lib := runner{build: fakeBuild(new(bool)), discover: discover.Discover, render: discover.Render}
	cases := []struct {
		name string
		args []string
	}{
		{"existing with force", []string{"-force", "-out", existing}},
		{"new file", []string{"-out", fresh}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			closed := false
			lib.build = fakeBuild(&closed)
			code, _, stderr := runT(t, lib, c.args...)
			if code != exitError {
				t.Fatalf("exit = %d, want %d; stderr:\n%s", code, exitError, stderr)
			}
			if !strings.Contains(stderr, "discover: discovery failed") {
				t.Errorf("stderr = %q", stderr)
			}
			if !closed {
				t.Error("clients were not closed after a failed run")
			}
		})
	}
	assertFile(t, existing, old)
	if _, err := os.Stat(fresh); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("failed discovery created %s (stat err %v)", fresh, err)
	}
}

func TestFailedRenderTouchesNoFile(t *testing.T) {
	out := filepath.Join(t.TempDir(), "owners.draft.yaml")
	const old = "keep me\n"
	if err := os.WriteFile(out, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	r := testRunner(new(bool))
	r.render = func(w io.Writer, _ discover.Result) error {
		_, _ = io.WriteString(w, "partial")
		return errors.New("boom")
	}
	code, _, stderr := runT(t, r, "-force", "-out", out)
	if code != exitError {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	assertFile(t, out, old)
}

// setSecretEnv points every address at a recognisable fake host so tests can
// assert it never reaches stderr.
func setSecretEnv(t *testing.T) {
	t.Helper()
	t.Setenv("CONNECT_URL", "http://secret-host:8083")
	t.Setenv("SCHEMA_REGISTRY_URL", "http://secret-host:8081")
	t.Setenv("KAFKA_BROKERS", "secret-host:9092, secret-host2:9092")
	t.Setenv("SOURCE_DB_URL", "postgres://u:secret-pw@secret-host:5432/orders")
}

func TestBuilderErrorHidesAddresses(t *testing.T) {
	setSecretEnv(t)
	t.Setenv("CONNECT_PASSWORD", "secret-pw-connect")
	r := testRunner(new(bool))
	// A careless builder that leaks the URL: run must still redact it.
	r.build = func(_ context.Context, cfg config.Config, _ bool) (discover.Deps, func(), error) {
		return discover.Deps{}, nil, errors.New("discover: dial " + cfg.ConnectURL + "/connectors and " + cfg.SourceDBURL + " pw " + cfg.ConnectPassword)
	}
	code, _, stderr := runT(t, r, "-out", filepath.Join(t.TempDir(), "x.yaml"))
	if code != exitError {
		t.Fatalf("exit = %d, want %d; stderr:\n%s", code, exitError, stderr)
	}
	assertNoSecrets(t, stderr)
	if !strings.Contains(stderr, "$CONNECT_URL") {
		t.Errorf("stderr does not name the env var:\n%s", stderr)
	}
	if n := strings.Count(strings.TrimSpace(stderr), "\n"); n != 0 {
		t.Errorf("error message spans %d lines:\n%s", n+1, stderr)
	}
}

func TestDiscoverErrorAndWarningsHideAddresses(t *testing.T) {
	setSecretEnv(t)
	r := testRunner(new(bool))
	r.discover = func(context.Context, discover.Deps, discover.Options) (discover.Result, error) {
		return discover.Result{}, errors.New(`connect: GET /connectors: Get "http://secret-host:8083/connectors": dial tcp: lookup secret-host: no such host`)
	}
	code, _, stderr := runT(t, r, "-out", filepath.Join(t.TempDir(), "x.yaml"))
	if code != exitError {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	assertNoSecrets(t, stderr)

	r.discover = func(_ context.Context, _ discover.Deps, o discover.Options) (discover.Result, error) {
		if o.Logger == nil {
			t.Fatal("run did not pass its redacting logger")
		}
		o.Logger.Warn("discover: list subjects failed", "err", errors.New(`Get "http://secret-host:8081/subjects": EOF`),
			slog.Group("g", "host", "secret-host2"))
		o.Logger.With("dsn", "postgres://u:secret-pw@secret-host:5432/orders").Info("secret-host in message")
		return discover.Result{Warnings: []string{"subjects: dial secret-host2:9092 failed\nwarning: fake line"}}, nil
	}
	code, _, stderr = runT(t, r, "-out", filepath.Join(t.TempDir(), "y.yaml"))
	if code != exitOK {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	assertNoSecrets(t, stderr)
	if n := countLinesWithPrefix(stderr, "warning: "); n != 1 {
		t.Errorf("a newline in a warning produced an extra line:\n%s", stderr)
	}
}

func TestRealBuilderErrorsHideAddresses(t *testing.T) {
	// Each case makes a client constructor fail before any network I/O.
	tests := []struct {
		name, key, value, want string
	}{
		{"connect userinfo", "CONNECT_URL", "http://user:secret-pw@secret-host:8083", "CONNECT_"},
		{"connect username without password", "CONNECT_USERNAME", "bob", "CONNECT_"},
		{"registry bad scheme", "SCHEMA_REGISTRY_URL", "ftp://secret-host:8081", "SCHEMA_REGISTRY_"},
		{"kafka bad protocol", "KAFKA_SECURITY_PROTOCOL", "SECRET-HOST-PROTO", "KAFKA_"},
		{"no brokers", "KAFKA_BROKERS", " , ", "KAFKA_BROKERS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setSecretEnv(t)
			t.Setenv(tt.key, tt.value)
			deps, closeFn, err := buildDeps(context.Background(), config.Load(), false)
			if err == nil {
				closeFn()
				t.Fatalf("build succeeded: %+v", deps)
			}
			closeFn() // must be safe on failure
			msg := err.Error()
			assertNoSecrets(t, msg)
			if strings.Contains(strings.ToLower(msg), "secret-host-proto") {
				t.Errorf("error echoes the setting value: %s", msg)
			}
			if !strings.Contains(msg, tt.want) {
				t.Errorf("error %q does not name %s", msg, tt.want)
			}
		})
	}
}

func TestRealBuilderWithoutSourceDB(t *testing.T) {
	// Building does no network I/O (kgo dials lazily), so the dev defaults
	// construct fine with nothing listening.
	setSecretEnv(t)
	deps, closeFn, err := buildDeps(context.Background(), config.Load(), false)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer closeFn()
	if deps.Connect == nil || deps.Kafka == nil || deps.Registry == nil {
		t.Errorf("missing clients: %+v", deps)
	}
	if deps.Source != nil {
		t.Error("Source set without -source-db")
	}
}

func TestSplitBrokers(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"a:1", []string{"a:1"}},
		{" a:1 , b:2 ,, ", []string{"a:1", "b:2"}},
		{"", nil},
		{" , ", nil},
	}
	for _, tt := range tests {
		got := splitBrokers(tt.in)
		if strings.Join(got, "|") != strings.Join(tt.want, "|") || len(got) != len(tt.want) {
			t.Errorf("splitBrokers(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func countLinesWithPrefix(s, prefix string) int {
	n := 0
	for _, l := range strings.Split(s, "\n") {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

func assertNoSecrets(t *testing.T, s string) {
	t.Helper()
	for _, bad := range []string{"secret-host", "secret-pw"} {
		if strings.Contains(s, bad) {
			t.Errorf("output contains %q:\n%s", bad, s)
		}
	}
}

func assertFile(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(got) != want {
		t.Errorf("%s = %q, want %q", path, got, want)
	}
}
