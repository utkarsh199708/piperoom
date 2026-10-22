package quickstart

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/auth"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

// testOwner owns every layer of the owners files these tests write, as
// `discover -write-owners` does for the OS user.
const testOwner = "qs-owner"

func testLog() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// writeOwnersFile writes an owners file with one CDC pipeline whose layers
// are all owned by the given principals (one per layer, cycling).
func writeOwnersFile(t *testing.T, principals ...string) string {
	t.Helper()
	var b strings.Builder
	b.WriteString(`pipelines:
  - id: orders
    title: Orders CDC
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]
    sink_ts_column: updated_at
layers:
`)
	for i, l := range events.LayersOf(events.PipelineKindCDC) {
		b.WriteString("  " + l + ": " + principals[i%len(principals)] + "\n")
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// loopbackConfig is a startable quickstart config: loopback, dev login, write
// mode off, the default users file.
func loopbackConfig() config.Config {
	return config.Config{HTTPAddr: "127.0.0.1:8090", AuthMode: "dev", WriteMode: "off", UsersFile: defaultUsersFile}
}

// quickstartDir makes a working directory with an owners.yaml (and x.yaml)
// naming one user and chdirs into it, for the tests that check the default
// owners path.
func quickstartDir(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{DefaultOwnersFile, "x.yaml"} {
		body := ownersBody("user:" + testOwner)
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(dir)
}

// ownersBody is an owners file whose layers are all owned by principal.
func ownersBody(principal string) string {
	var b strings.Builder
	b.WriteString(`pipelines:
  - id: orders
    title: Orders CDC
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]
    sink_ts_column: updated_at
layers:
`)
	for _, l := range events.LayersOf(events.PipelineKindCDC) {
		b.WriteString("  " + l + ": " + principal + "\n")
	}
	return b.String()
}

func TestCheckAddr(t *testing.T) {
	for _, tc := range []struct {
		addr      string
		wantErr   bool
		nonLoopIs bool // the error is ErrNonLoopback
	}{
		{addr: "127.0.0.1:8090"},
		{addr: "127.0.0.1:0"},
		{addr: "[::1]:8090"},
		{addr: "localhost:8090"},
		{addr: "LOCALHOST:8090"},
		{addr: "0.0.0.0:8090", wantErr: true, nonLoopIs: true},
		{addr: ":8090", wantErr: true, nonLoopIs: true},
		{addr: "[::]:8090", wantErr: true, nonLoopIs: true},
		{addr: "192.168.1.5:8090", wantErr: true, nonLoopIs: true},
		{addr: "piperoom.example.com:8090", wantErr: true, nonLoopIs: true},
		{addr: "127.0.0.1", wantErr: true},
	} {
		t.Run(tc.addr, func(t *testing.T) {
			err := CheckAddr(tc.addr)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("want no error, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("want an error")
			}
			if errors.Is(err, ErrNonLoopback) != tc.nonLoopIs {
				t.Fatalf("errors.Is(err, ErrNonLoopback) = %v, want %v: %v", !tc.nonLoopIs, tc.nonLoopIs, err)
			}
			if !strings.Contains(err.Error(), "PIPEROOM_HTTP_ADDR") {
				t.Fatalf("the error should name PIPEROOM_HTTP_ADDR: %v", err)
			}
		})
	}
}

func TestCheckListener(t *testing.T) {
	for _, tc := range []struct {
		name string
		addr net.Addr
		ok   bool
	}{
		{"ipv4 loopback", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8090}, true},
		{"ipv6 loopback", &net.TCPAddr{IP: net.IPv6loopback, Port: 8090}, true},
		{"lan", &net.TCPAddr{IP: net.IPv4(10, 0, 0, 7), Port: 8090}, false},
		{"all interfaces", &net.TCPAddr{IP: net.IPv4zero, Port: 8090}, false},
		{"not tcp", &net.UnixAddr{Name: "/tmp/x.sock", Net: "unix"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckListener(tc.addr)
			if tc.ok != (err == nil) {
				t.Fatalf("err = %v, want ok=%v", err, tc.ok)
			}
			if err != nil && !errors.Is(err, ErrNonLoopback) {
				t.Fatalf("want ErrNonLoopback, got %v", err)
			}
			// The caller reports it through the server prefix: no "server:" here.
			if err != nil && !strings.HasPrefix(err.Error(), "listen: --quickstart: ") {
				t.Fatalf("reported as %q, want it to start with \"listen: --quickstart: \"", err)
			}
		})
	}
}

func TestURL(t *testing.T) {
	for _, tc := range []struct {
		addr net.Addr
		want string
	}{
		{&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8090}, "http://127.0.0.1:8090/"},
		{&net.TCPAddr{IP: net.IPv6loopback, Port: 41234}, "http://[::1]:41234/"},
	} {
		if got := URL(tc.addr); got != tc.want {
			t.Errorf("URL(%v) = %q, want %q", tc.addr, got, tc.want)
		}
	}
}

// Prepare forces dev login and the write mode and takes the owners file from
// --owners; a non-loopback address and PIPEROOM_WRITE_MODE=on are refused, the
// dev switches notwithstanding.
func TestPrepare(t *testing.T) {
	for _, tc := range []struct {
		name       string
		edit       func(*config.Config)
		owners     string
		wantErr    string // "" = accepted
		wantErrIs  error
		wantOwners string
		wantWarn   string // a line the log must hold
	}{
		{name: "defaults", wantOwners: DefaultOwnersFile},
		{name: "owners flag", owners: "x.yaml", wantOwners: "x.yaml"},
		{name: "write mode unset", edit: func(c *config.Config) { c.WriteMode = "" }, wantOwners: DefaultOwnersFile},
		{name: "oidc is forced to dev", edit: func(c *config.Config) { c.AuthMode = "oidc" }, wantOwners: DefaultOwnersFile,
			wantWarn: "--quickstart forces PIPEROOM_AUTH_MODE=dev"},
		{name: "dev switch is cleared", edit: func(c *config.Config) { c.DevAllowNonLoopback = "1" }, wantOwners: DefaultOwnersFile},
		{name: "write mode on", edit: func(c *config.Config) { c.WriteMode = "on" }, wantErrIs: ErrWriteMode},
		{name: "write mode typo", edit: func(c *config.Config) { c.WriteMode = "true" }, wantErr: "PIPEROOM_WRITE_MODE"},
		{name: "setup on while read-only", edit: func(c *config.Config) { c.SetupEnabled = "on" }, wantErr: "PIPEROOM_SETUP_ENABLED"},
		{name: "all interfaces", edit: func(c *config.Config) { c.HTTPAddr = "0.0.0.0:8090" }, wantErrIs: ErrNonLoopback},
		{name: "all interfaces with the dev switch and ack", edit: func(c *config.Config) {
			c.HTTPAddr = "0.0.0.0:8090"
			c.DevAllowNonLoopback = "1"
			c.DevAck = config.DevAckValue
		}, wantErrIs: ErrNonLoopback},
		{name: "lan address in oidc mode", edit: func(c *config.Config) { c.HTTPAddr = "192.168.1.5:8090"; c.AuthMode = "oidc" }, wantErrIs: ErrNonLoopback},
	} {
		t.Run(tc.name, func(t *testing.T) {
			quickstartDir(t)
			cfg := loopbackConfig()
			if tc.edit != nil {
				tc.edit(&cfg)
			}
			log, buf := testLog()
			s, cleanup, err := Prepare(Options{Config: cfg, OwnersPath: tc.owners, TempDir: t.TempDir(), Log: log})
			switch {
			case tc.wantErrIs != nil:
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, want %v", err, tc.wantErrIs)
				}
				return
			case tc.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, tc.wantErr)
				}
				return
			case err != nil:
				t.Fatal(err)
			}
			defer cleanup()
			if s.Config.AuthMode != auth.ModeDev || s.Config.DevAllowNonLoopback != "" || s.Config.OwnersFile != tc.wantOwners {
				t.Fatalf("auth %q, dev switch %q, owners %q; want dev, empty, %q", s.Config.AuthMode, s.Config.DevAllowNonLoopback, s.Config.OwnersFile, tc.wantOwners)
			}
			if s.WriteMode || s.SetupEnabled || s.Config.WriteMode != "off" || s.Config.SetupEnabled != "off" {
				t.Fatalf("options = write %v setup %v, config = write %q setup %q; want off/off by default",
					s.WriteMode, s.SetupEnabled, s.Config.WriteMode, s.Config.SetupEnabled)
			}
			if tc.wantWarn != "" && !strings.Contains(buf.String(), tc.wantWarn) {
				t.Fatalf("log %q lacks %q", buf.String(), tc.wantWarn)
			}
			// The forced settings pass the start checks that follow: dev login
			// on loopback with no warning, and write mode off.
			if _, err := config.ParseWriteMode(s.Config); err != nil {
				t.Fatalf("write mode after Prepare: %v", err)
			}
			if setup, err := config.ParseSetup(s.Config); err != nil || setup.Enabled {
				t.Fatalf("setup after Prepare: %+v, %v; want off", setup, err)
			}
		})
	}
}

// The zero-value Options plus a loopback Config is cmd/server --quickstart: no
// writes and no setup routes, whatever the environment.
func TestPrepareDefaultIsUnchanged(t *testing.T) {
	quickstartDir(t)
	s, cleanup, err := Prepare(Options{Config: loopbackConfig(), TempDir: t.TempDir(), Log: discardLog()})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if s.WriteMode || s.SetupEnabled {
		t.Fatalf("zero options: write %v setup %v, want off/off", s.WriteMode, s.SetupEnabled)
	}
	if s.Config.WriteMode != "off" || s.Config.SetupEnabled != "off" {
		t.Fatalf("config: write %q setup %q, want off/off", s.Config.WriteMode, s.Config.SetupEnabled)
	}
	if s.Owners == nil || s.Users == nil {
		t.Fatalf("owners %v, users %v; both are required", s.Owners, s.Users)
	}
}

// WriteMode and SetupEnabled reach the server the caller builds, and the
// default stays read-only; setup on without write mode is refused.
func TestOptionsReachServer(t *testing.T) {
	for _, tc := range []struct {
		name        string
		write, set  bool
		wantConfig  [2]string // WriteMode, SetupEnabled
		wantSetupIn bool
	}{
		{name: "default", wantConfig: [2]string{"off", "off"}},
		{name: "write on", write: true, wantConfig: [2]string{"on", "off"}},
		{name: "write on setup on", write: true, set: true, wantConfig: [2]string{"on", "on"}, wantSetupIn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			cfg := loopbackConfig()
			if tc.set {
				cfg.SecretsDir = filepath.Join(t.TempDir(), "secrets")
			}
			var got *Settings
			opts := Options{
				Config:       cfg,
				WriteMode:    tc.write,
				SetupEnabled: tc.set,
				OwnersPath:   writeOwnersFile(t, "user:"+testOwner),
				TempDir:      tmp,
				Log:          discardLog(),
				Serve: func(_ context.Context, s Settings) error {
					got = &s
					return nil
				},
			}
			if err := Run(context.Background(), opts); err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Fatal("Serve was not called")
			}
			if got.WriteMode != tc.write || got.SetupEnabled != tc.set {
				t.Fatalf("server saw write %v setup %v, want %v/%v", got.WriteMode, got.SetupEnabled, tc.write, tc.set)
			}
			if got.Config.WriteMode != tc.wantConfig[0] || got.Config.SetupEnabled != tc.wantConfig[1] {
				t.Fatalf("server config write %q setup %q, want %q/%q",
					got.Config.WriteMode, got.Config.SetupEnabled, tc.wantConfig[0], tc.wantConfig[1])
			}
			if got.Config.AuthMode != auth.ModeDev || !IsLoopbackHost(strings.Split(got.Config.HTTPAddr, ":")[0]) {
				t.Fatalf("server config auth %q addr %q, want dev on loopback", got.Config.AuthMode, got.Config.HTTPAddr)
			}
			if got.Owners == nil || got.Users == nil {
				t.Fatalf("owners %v, users %v; both are required", got.Owners, got.Users)
			}
			if setup, err := config.ParseSetup(got.Config); err != nil || setup.Enabled != tc.wantSetupIn {
				t.Fatalf("setup parsed from the server config: %+v, %v; want enabled=%v", setup, err, tc.wantSetupIn)
			}
			// Run removes the temp users file on the exit path.
			if left, _ := filepath.Glob(filepath.Join(tmp, TempDirPrefix+"*")); len(left) != 0 {
				t.Fatalf("the temp users dir outlived Run: %v", left)
			}
		})
	}
}

func TestRunRefusesSetupWithoutWriteMode(t *testing.T) {
	quickstartDir(t)
	err := Run(context.Background(), Options{
		Config:       loopbackConfig(),
		SetupEnabled: true,
		TempDir:      t.TempDir(),
		Log:          discardLog(),
		Serve:        func(context.Context, Settings) error { t.Fatal("Serve must not run"); return nil },
	})
	if !errors.Is(err, ErrSetupNeedsWriteMode) {
		t.Fatalf("err = %v, want ErrSetupNeedsWriteMode", err)
	}
}

func TestRunNeedsServe(t *testing.T) {
	err := Run(context.Background(), Options{Config: loopbackConfig()})
	if err == nil || !strings.Contains(err.Error(), "Options.Serve") {
		t.Fatalf("err = %v, want a refusal naming Options.Serve", err)
	}
}

// Setup on needs a secrets directory, and the refusal comes before Serve.
func TestRunSetupNeedsSecretsDir(t *testing.T) {
	quickstartDir(t)
	err := Run(context.Background(), Options{
		Config:       loopbackConfig(),
		WriteMode:    true,
		SetupEnabled: true,
		TempDir:      t.TempDir(),
		Log:          discardLog(),
		Serve:        func(context.Context, Settings) error { t.Fatal("Serve must not run"); return nil },
	})
	if err == nil || !strings.Contains(err.Error(), "PIPEROOM_SECRETS_DIR") {
		t.Fatalf("err = %v, want one naming PIPEROOM_SECRETS_DIR", err)
	}
}

// configDefaultUsersFile must be what config.Load gives an unset USERS_FILE,
// or the quickstart would mistake the default for an operator's choice.
func TestDefaultUsersFileMatchesConfig(t *testing.T) {
	t.Setenv("USERS_FILE", "")
	if got := config.Load().UsersFile; got != defaultUsersFile {
		t.Fatalf("config.Load().UsersFile with USERS_FILE unset = %q, want %q", got, defaultUsersFile)
	}
}

func TestOwnedLayers(t *testing.T) {
	own := &owners.Owners{Layers: owners.UserLayers(map[string]string{
		"warehouse": "a", "postgres": "a", "kafka": "a", "zz-custom": "a", "aa-custom": "a",
	})}
	want := []string{"postgres", "kafka", "warehouse", "aa-custom", "zz-custom"}
	if got := ownedLayers(own); !slices.Equal(got, want) {
		t.Fatalf("ownedLayers = %v, want %v", got, want)
	}
	if got := ownedLayers(&owners.Owners{}); got == nil || len(got) != 0 {
		t.Fatalf("no layers: got %#v, want an empty list", got)
	}
}

func TestSoleUser(t *testing.T) {
	for _, tc := range []struct {
		name   string
		layers map[string]owners.Principals
		want   string
		ok     bool
	}{
		{"one user", owners.UserLayers(map[string]string{"postgres": "me", "kafka": "me"}), "me", true},
		{"two users", owners.UserLayers(map[string]string{"postgres": "me", "kafka": "you"}), "", false},
		{"a group", map[string]owners.Principals{"postgres": {{Kind: owners.KindGroup, Name: "dataeng"}}}, "", false},
		{"no one", nil, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := SoleUser(&owners.Owners{Layers: tc.layers})
			if got != tc.want || ok != tc.ok {
				t.Fatalf("SoleUser = %q, %v; want %q, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}

// With USERS_FILE unset and an owners file that names one user, the
// quickstart writes a private one-user users file that agrees with the owners
// file, and the cleanup removes it.
func TestUsersWritesOneUserFile(t *testing.T) {
	own, err := owners.Load(writeOwnersFile(t, "user:"+testOwner))
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	cfg := config.Config{UsersFile: defaultUsersFile, OwnersFile: "owners.yaml"}
	log, buf := testLog()
	users, cleanup, err := usersFor(cfg, own, tmp, log)
	if err != nil {
		t.Fatal(err)
	}
	if err := room.ValidateUsers(users, own); err != nil {
		t.Fatalf("the generated users file disagrees with the owners file: %v", err)
	}
	u, ok := users.Get(testOwner)
	if !ok {
		t.Fatalf("user %q missing", testOwner)
	}
	if want := events.LayersOf(events.PipelineKindCDC); !slices.Equal(u.Layers, want) {
		t.Fatalf("layers = %v, want every layer of the owners file %v", u.Layers, want)
	}

	files, err := filepath.Glob(filepath.Join(tmp, TempDirPrefix+"*", UsersFileName))
	if err != nil || len(files) != 1 {
		t.Fatalf("users file under %s: %v, %v", tmp, files, err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	// One user: the file parses on its own to exactly that entry.
	parsed, err := room.ParseUsers(data)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(parsed.All()); n != 1 {
		t.Fatalf("users in file = %d, want 1:\n%s", n, data)
	}
	if runtime.GOOS != "windows" {
		fi, err := os.Stat(files[0])
		if err != nil {
			t.Fatal(err)
		}
		if perm := fi.Mode().Perm(); perm != 0o600 {
			t.Fatalf("users file mode %o, want 600", perm)
		}
	}
	if !strings.Contains(buf.String(), "wrote a one-user users file") || !strings.Contains(buf.String(), files[0]) {
		t.Fatalf("the log should name the file:\n%s", buf.String())
	}

	cleanup()
	cleanup() // a second call is harmless
	if left, _ := filepath.Glob(filepath.Join(tmp, TempDirPrefix+"*")); len(left) != 0 {
		t.Fatalf("cleanup left %v", left)
	}
}

// A USERS_FILE the operator set is used as is: nothing is written.
func TestUsersKeepsUsersFileSetByOperator(t *testing.T) {
	own, err := owners.Load(writeOwnersFile(t, "user:"+testOwner))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "mine.yaml")
	body := "users:\n  - id: " + testOwner + "\n    name: Mine\n    layers: [" + strings.Join(events.LayersOf(events.PipelineKindCDC), ", ") + "]\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	users, cleanup, err := usersFor(config.Config{UsersFile: path}, own, tmp, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if u, ok := users.Get(testOwner); !ok || u.Name != "Mine" {
		t.Fatalf("want the operator's file, got %+v (found %v)", u, ok)
	}
	if left, _ := filepath.Glob(filepath.Join(tmp, "*")); len(left) != 0 {
		t.Fatalf("nothing should be written, found %v", left)
	}

	// A USERS_FILE that does not load refuses the start.
	if _, _, err := usersFor(config.Config{UsersFile: filepath.Join(t.TempDir(), "missing.yaml")}, own, tmp, discardLog()); err == nil {
		t.Fatal("a missing USERS_FILE should refuse the start")
	}
}

// An owners file that names several principals cannot be served by a
// one-user file: the default users file is tried, and when it does not load
// the error names the principals and USERS_FILE.
func TestUsersSeveralOwners(t *testing.T) {
	own, err := owners.Load(writeOwnersFile(t, "user:priya", "user:arjun"))
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	// The test's working directory has no dev/users.yaml.
	cfg := config.Config{UsersFile: defaultUsersFile, OwnersFile: "team.yaml"}
	_, _, err = usersFor(cfg, own, tmp, discardLog())
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{"USERS_FILE", "user:arjun", "user:priya", "team.yaml"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
	if left, _ := filepath.Glob(filepath.Join(tmp, "*")); len(left) != 0 {
		t.Fatalf("nothing should be written, found %v", left)
	}
}

// An owner id the users file refuses (the derived-id prefix) stops the start
// and leaves no temp directory behind.
func TestUsersRefusedIDCleansUp(t *testing.T) {
	own := &owners.Owners{Layers: owners.UserLayers(map[string]string{"postgres": room.DerivedIDPrefix + "0123456789ab"})}
	tmp := t.TempDir()
	_, _, err := usersFor(config.Config{UsersFile: defaultUsersFile}, own, tmp, discardLog())
	if err == nil || !strings.Contains(err.Error(), "users file for") {
		t.Fatalf("err = %v, want a refusal of the generated users file", err)
	}
	if left, _ := filepath.Glob(filepath.Join(tmp, "*")); len(left) != 0 {
		t.Fatalf("the temp dir should be removed, found %v", left)
	}
}
