package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/auth"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/wiring"
)

// quickstartOwner owns every layer of the owners file the quickstart tests
// write, as `discover -write-owners` does for the OS user.
const quickstartOwner = "qs-owner"

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

func TestCheckStartFlags(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		demo, quickstart, allowRemote bool
		owners                        string
		wantErr                       string // "" = allowed
	}{
		{name: "no flags"},
		{name: "demo", demo: true},
		{name: "quickstart", quickstart: true},
		{name: "quickstart with owners", quickstart: true, owners: "x.yaml"},
		{name: "real with allow-remote", allowRemote: true},
		{name: "quickstart and demo", quickstart: true, demo: true, wantErr: "cannot be combined"},
		{name: "quickstart and allow-remote", quickstart: true, allowRemote: true, wantErr: "loopback only"},
		{name: "owners without quickstart", owners: "x.yaml", wantErr: "--owners needs --quickstart"},
		{name: "owners with demo", demo: true, owners: "x.yaml", wantErr: "--owners needs --quickstart"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkStartFlags(tc.demo, tc.quickstart, tc.allowRemote, tc.owners)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want no error, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

// --quickstart serves loopback only, with or without the dev switches; a
// refusal is errNonLoopback, a malformed address is not.
func TestCheckQuickstartAddr(t *testing.T) {
	for _, tc := range []struct {
		addr      string
		wantErr   bool
		nonLoopIs bool // the error is errNonLoopback
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
			err := checkQuickstartAddr(tc.addr)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("want no error, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("want an error")
			}
			if errors.Is(err, errNonLoopback) != tc.nonLoopIs {
				t.Fatalf("errors.Is(err, errNonLoopback) = %v, want %v: %v", !tc.nonLoopIs, tc.nonLoopIs, err)
			}
			if !strings.Contains(err.Error(), "PIPEROOM_HTTP_ADDR") {
				t.Fatalf("the error should name PIPEROOM_HTTP_ADDR: %v", err)
			}
		})
	}
}

func TestCheckQuickstartListener(t *testing.T) {
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
			err := checkQuickstartListener(tc.addr)
			if tc.ok != (err == nil) {
				t.Fatalf("err = %v, want ok=%v", err, tc.ok)
			}
			if err != nil && !errors.Is(err, errNonLoopback) {
				t.Fatalf("want errNonLoopback, got %v", err)
			}
		})
	}
}

func TestQuickstartURL(t *testing.T) {
	for _, tc := range []struct {
		addr net.Addr
		want string
	}{
		{&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8090}, "http://127.0.0.1:8090/"},
		{&net.TCPAddr{IP: net.IPv6loopback, Port: 41234}, "http://[::1]:41234/"},
	} {
		if got := quickstartURL(tc.addr); got != tc.want {
			t.Errorf("quickstartURL(%v) = %q, want %q", tc.addr, got, tc.want)
		}
	}
}

// quickstartConfig forces dev login and write mode off and takes the owners
// file from --owners; a non-loopback address and PIPEROOM_WRITE_MODE=on are
// refused, the dev switches notwithstanding.
func TestQuickstartConfig(t *testing.T) {
	base := func() config.Config {
		return config.Config{HTTPAddr: "127.0.0.1:8090", AuthMode: "dev", WriteMode: "off", OwnersFile: "dev/owners.yaml", UsersFile: configDefaultUsersFile}
	}
	for _, tc := range []struct {
		name       string
		edit       func(*config.Config)
		owners     string
		wantErr    string // "" = accepted
		wantErrIs  error
		wantOwners string
		wantWarn   string // a line the log must hold
	}{
		{name: "defaults", wantOwners: quickstartDefaultOwners},
		{name: "owners flag", owners: "x.yaml", wantOwners: "x.yaml"},
		{name: "write mode unset", edit: func(c *config.Config) { c.WriteMode = "" }, wantOwners: quickstartDefaultOwners},
		{name: "oidc is forced to dev", edit: func(c *config.Config) { c.AuthMode = "oidc" }, wantOwners: quickstartDefaultOwners,
			wantWarn: "--quickstart forces PIPEROOM_AUTH_MODE=dev"},
		{name: "dev switch is cleared", edit: func(c *config.Config) { c.DevAllowNonLoopback = "1" }, wantOwners: quickstartDefaultOwners},
		{name: "write mode on", edit: func(c *config.Config) { c.WriteMode = "on" }, wantErrIs: errQuickstartWriteMode},
		{name: "write mode typo", edit: func(c *config.Config) { c.WriteMode = "true" }, wantErr: "PIPEROOM_WRITE_MODE"},
		{name: "all interfaces", edit: func(c *config.Config) { c.HTTPAddr = "0.0.0.0:8090" }, wantErrIs: errNonLoopback},
		{name: "all interfaces with the dev switch and ack", edit: func(c *config.Config) {
			c.HTTPAddr = "0.0.0.0:8090"
			c.DevAllowNonLoopback = "1"
			c.DevAck = config.DevAckValue
		}, wantErrIs: errNonLoopback},
		{name: "lan address in oidc mode", edit: func(c *config.Config) { c.HTTPAddr = "192.168.1.5:8090"; c.AuthMode = "oidc" }, wantErrIs: errNonLoopback},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			if tc.edit != nil {
				tc.edit(&cfg)
			}
			log, buf := testLog()
			got, err := quickstartConfig(cfg, tc.owners, log)
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
			if got.AuthMode != auth.ModeDev || got.DevAllowNonLoopback != "" || got.OwnersFile != tc.wantOwners {
				t.Fatalf("auth %q, dev switch %q, owners %q; want dev, empty, %q", got.AuthMode, got.DevAllowNonLoopback, got.OwnersFile, tc.wantOwners)
			}
			if tc.wantWarn != "" && !strings.Contains(buf.String(), tc.wantWarn) {
				t.Fatalf("log %q lacks %q", buf.String(), tc.wantWarn)
			}
			// The forced settings pass the start checks that follow: dev
			// login on loopback with no warning, and write mode off.
			s, err := authSetup(got, false, false, discardLog())
			if err != nil || s.Mode != auth.ModeDev || s.DevNonLoopback {
				t.Fatalf("authSetup after quickstart: %+v, %v", s, err)
			}
			on, err := writeModeSetup(got, false, discardLog())
			if err != nil || on {
				t.Fatalf("write mode after quickstart: on=%v err=%v, want off", on, err)
			}
		})
	}
}

// configDefaultUsersFile must be what config.Load gives an unset USERS_FILE,
// or --quickstart would mistake the default for an operator's choice.
func TestConfigDefaultUsersFileMatchesConfig(t *testing.T) {
	t.Setenv("USERS_FILE", "")
	if got := config.Load().UsersFile; got != configDefaultUsersFile {
		t.Fatalf("config.Load().UsersFile with USERS_FILE unset = %q, want %q", got, configDefaultUsersFile)
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
			got, ok := soleUser(&owners.Owners{Layers: tc.layers})
			if got != tc.want || ok != tc.ok {
				t.Fatalf("soleUser = %q, %v; want %q, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}

// With USERS_FILE unset and an owners file that names one user, the
// quickstart writes a private one-user users file that agrees with the owners
// file, and the cleanup removes it.
func TestQuickstartUsersWritesOneUserFile(t *testing.T) {
	own, err := owners.Load(writeOwnersFile(t, "user:"+quickstartOwner))
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	cfg := config.Config{UsersFile: configDefaultUsersFile, OwnersFile: "owners.yaml"}
	log, buf := testLog()
	users, cleanup, err := quickstartUsers(cfg, own, tmp, log)
	if err != nil {
		t.Fatal(err)
	}
	if err := room.ValidateUsers(users, own); err != nil {
		t.Fatalf("the generated users file disagrees with the owners file: %v", err)
	}
	u, ok := users.Get(quickstartOwner)
	if !ok {
		t.Fatalf("user %q missing", quickstartOwner)
	}
	if want := events.LayersOf(events.PipelineKindCDC); !slices.Equal(u.Layers, want) {
		t.Fatalf("layers = %v, want every layer of the owners file %v", u.Layers, want)
	}

	files, err := filepath.Glob(filepath.Join(tmp, "piperoom-quickstart-*", quickstartUsersName))
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
	if left, _ := filepath.Glob(filepath.Join(tmp, "piperoom-quickstart-*")); len(left) != 0 {
		t.Fatalf("cleanup left %v", left)
	}
}

// A USERS_FILE the operator set is used as is: nothing is written.
func TestQuickstartUsersKeepsUsersFileSetByOperator(t *testing.T) {
	own, err := owners.Load(writeOwnersFile(t, "user:"+quickstartOwner))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "mine.yaml")
	body := "users:\n  - id: " + quickstartOwner + "\n    name: Mine\n    layers: [" + strings.Join(events.LayersOf(events.PipelineKindCDC), ", ") + "]\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	users, cleanup, err := quickstartUsers(config.Config{UsersFile: path}, own, tmp, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if u, ok := users.Get(quickstartOwner); !ok || u.Name != "Mine" {
		t.Fatalf("want the operator's file, got %+v (found %v)", u, ok)
	}
	if left, _ := filepath.Glob(filepath.Join(tmp, "*")); len(left) != 0 {
		t.Fatalf("nothing should be written, found %v", left)
	}

	// A USERS_FILE that does not load refuses the start.
	if _, _, err := quickstartUsers(config.Config{UsersFile: filepath.Join(t.TempDir(), "missing.yaml")}, own, tmp, discardLog()); err == nil {
		t.Fatal("a missing USERS_FILE should refuse the start")
	}
}

// An owners file that names several principals cannot be served by a
// one-user file: the default users file is tried, and when it does not load
// the error names the principals and USERS_FILE.
func TestQuickstartUsersSeveralOwners(t *testing.T) {
	own, err := owners.Load(writeOwnersFile(t, "user:priya", "user:arjun"))
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	// The test runs in cmd/server, where dev/users.yaml does not exist.
	cfg := config.Config{UsersFile: configDefaultUsersFile, OwnersFile: "team.yaml"}
	_, _, err = quickstartUsers(cfg, own, tmp, discardLog())
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
func TestQuickstartUsersRefusedIDCleansUp(t *testing.T) {
	own := &owners.Owners{Layers: owners.UserLayers(map[string]string{"postgres": room.DerivedIDPrefix + "0123456789ab"})}
	tmp := t.TempDir()
	_, _, err := quickstartUsers(config.Config{UsersFile: configDefaultUsersFile}, own, tmp, discardLog())
	if err == nil || !strings.Contains(err.Error(), "users file for") {
		t.Fatalf("err = %v, want a refusal of the generated users file", err)
	}
	if left, _ := filepath.Glob(filepath.Join(tmp, "*")); len(left) != 0 {
		t.Fatalf("the temp dir should be removed, found %v", left)
	}
}

// noDataStack opens no client set: every pipeline gets the empty set (runbook
// tool only), so the quickstart start path runs with no data stack.
func noDataStack(ctx context.Context, cfg config.Config, awsCfg *aws.Config, own *owners.Owners, opts wiring.StackOptions) (*wiring.Stacks, error) {
	opts.Only = []string{}
	return wiring.OpenStacks(ctx, cfg, awsCfg, own, opts)
}

// TestQuickstartHelper is not a test: the quickstart tests re-run this test
// binary with PIPEROOM_TEST_QUICKSTART=1 so runWith executes --quickstart in
// its own process, with noDataStack in place of the real stack.
func TestQuickstartHelper(t *testing.T) {
	// A plain return, not t.Skip: the full-stack CI job fails on any skipped
	// test, and this one only does work inside the subprocess.
	if os.Getenv("PIPEROOM_TEST_QUICKSTART") != "1" {
		return
	}
	os.Args = []string{"server", "--quickstart", "--owners", os.Getenv("PIPEROOM_TEST_OWNERS")}
	if err := runWith(startDeps{openStacks: noDataStack, stdout: os.Stdout}); err != nil {
		os.Stdout.WriteString("RUN ERROR: " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(0)
}

// quickstartCmd builds the helper process: USERS_FILE, the write mode and the
// auth mode unset, the temp directory inside the test's own.
func quickstartCmd(ctx context.Context, t *testing.T, ownersPath, tmp string, env ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQuickstartHelper$")
	cmd.Env = append(os.Environ(),
		"PIPEROOM_TEST_QUICKSTART=1",
		"PIPEROOM_TEST_OWNERS="+ownersPath,
		"PIPEROOM_HTTP_ADDR=127.0.0.1:0",
		"USERS_FILE=",
		"PIPEROOM_WRITE_MODE=",
		"PIPEROOM_AUTH_MODE=",
		"PIPEROOM_WEB_DIR=",
		"PIPEROOM_LOG_FORMAT=",
		"PIPEROOM_LOG_LEVEL=",
		"TMPDIR="+tmp, "TMP="+tmp, "TEMP="+tmp,
	)
	cmd.Env = append(cmd.Env, env...)
	return cmd
}

// The board's done-when for P1a-2: `server --quickstart --owners x.yaml`
// serves a room list in under 10 s. The start runs whole in its own process
// (flags, config, the generated users file, the in-memory store, the agent
// endpoint, the HTTP handler); only the data stack is left out (noDataStack),
// since unit tests run without Docker.
func TestQuickstartServesRoomListWithin10s(t *testing.T) {
	const budget = 10 * time.Second
	ownersPath := writeOwnersFile(t, "user:"+quickstartOwner)
	tmp := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := quickstartCmd(ctx, t, ownersPath, tmp)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr lockedBuffer
	cmd.Stderr = &stderr

	start := time.Now()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// exited closes once the process is gone; waitErr is set before.
	exited := make(chan struct{})
	var waitErr error
	var outLines []string
	var outMu sync.Mutex
	urls := make(chan string, 1)
	go func() {
		defer close(exited)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Text()
			outMu.Lock()
			outLines = append(outLines, line)
			outMu.Unlock()
			if strings.HasPrefix(line, "http://") {
				select {
				case urls <- line:
				default:
				}
			}
		}
		waitErr = cmd.Wait()
	}()
	stop := func() {
		// os.Interrupt runs the graceful shutdown; Windows cannot send it to
		// another process, so it falls back to Kill there. Both fail
		// harmlessly once the process has exited.
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			_ = cmd.Process.Kill()
		}
		select {
		case <-exited:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
		}
	}
	defer stop()

	var base string
	select {
	case base = <-urls:
	case <-exited:
		t.Fatalf("server exited before printing a URL: %v\nstderr:\n%s", waitErr, stderr.String())
	case <-time.After(budget):
		t.Fatalf("no URL within %s\nstderr:\n%s", budget, stderr.String())
	}
	if !strings.HasPrefix(base, "http://127.0.0.1:") || !strings.HasSuffix(base, "/") || strings.HasSuffix(base, ":0/") {
		t.Fatalf("printed URL %q, want http://127.0.0.1:<bound port>/", base)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	get := func(path string) (int, []byte) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(base, "/")+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-User", quickstartOwner)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v\nstderr:\n%s", path, err, stderr.String())
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, body
	}
	code, body := get("/api/rooms")
	elapsed := time.Since(start)
	if code != http.StatusOK {
		t.Fatalf("GET /api/rooms = %d %s\nstderr:\n%s", code, body, stderr.String())
	}
	var rooms []json.RawMessage
	if err := json.Unmarshal(body, &rooms); err != nil {
		t.Fatalf("GET /api/rooms body %q is not a room list: %v", body, err)
	}
	if elapsed >= budget {
		t.Fatalf("room list served after %s, want under %s", elapsed, budget)
	}
	t.Logf("room list served %s after start", elapsed.Round(time.Millisecond))

	// Read-only: write mode is off, whatever the shell had (unset here).
	code, body = get("/healthz")
	var health struct {
		WriteMode string `json:"write_mode"`
	}
	if code != http.StatusOK || json.Unmarshal(body, &health) != nil || health.WriteMode != "off" {
		t.Fatalf("GET /healthz = %d %s, want write_mode off", code, body)
	}

	// The one-user users file lives in the temp directory while the server
	// runs.
	files, _ := filepath.Glob(filepath.Join(tmp, "piperoom-quickstart-*", quickstartUsersName))
	if len(files) != 1 {
		t.Fatalf("users file under the temp dir: %v", files)
	}

	stop()
	outMu.Lock()
	lines := slices.Clone(outLines)
	outMu.Unlock()
	// The URL is the one thing on stdout; everything else is in the log.
	if len(lines) != 1 {
		t.Fatalf("stdout = %q, want only the URL", lines)
	}
	logs := stderr.String()
	for _, want := range []string{"mode=quickstart", "write mode off", "wrote a one-user users file"} {
		if !strings.Contains(logs, want) {
			t.Errorf("the log lacks %q:\n%s", want, logs)
		}
	}
	if runtime.GOOS != "windows" {
		// The graceful stop removes the users file (Windows was killed).
		if left, _ := filepath.Glob(filepath.Join(tmp, "piperoom-quickstart-*")); len(left) != 0 {
			t.Fatalf("the temp users dir outlived the server: %v", left)
		}
	}
}

// The whole start refuses a non-loopback address with --quickstart before
// anything listens or is written, even with both dev switches set.
func TestQuickstartStartRefusesNonLoopback(t *testing.T) {
	ownersPath := writeOwnersFile(t, "user:"+quickstartOwner)
	tmp := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := quickstartCmd(ctx, t, ownersPath, tmp,
		"PIPEROOM_HTTP_ADDR=0.0.0.0:0",
		"PIPEROOM_DEV_ALLOW_NONLOOPBACK=1",
		"PIPEROOM_DEV_ACK="+config.DevAckValue,
	)
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "RUN ERROR") || !strings.Contains(string(out), "loopback only") {
		t.Fatalf("PIPEROOM_HTTP_ADDR=0.0.0.0:0: err %v, want a start-up refusal\n%s", err, out)
	}
	if strings.Contains(string(out), "http://") || strings.Contains(string(out), "server: listening") {
		t.Fatalf("the refused start listened or printed a URL:\n%s", out)
	}
	if left, _ := filepath.Glob(filepath.Join(tmp, "piperoom-quickstart-*")); len(left) != 0 {
		t.Fatalf("the refused start wrote %v", left)
	}
}

// PIPEROOM_WRITE_MODE=on with --quickstart refuses the start.
func TestQuickstartStartRefusesWriteModeOn(t *testing.T) {
	ownersPath := writeOwnersFile(t, "user:"+quickstartOwner)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := quickstartCmd(ctx, t, ownersPath, t.TempDir(), "PIPEROOM_WRITE_MODE=on")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "RUN ERROR") || !strings.Contains(string(out), "PIPEROOM_WRITE_MODE=on") {
		t.Fatalf("PIPEROOM_WRITE_MODE=on: err %v, want a start-up refusal naming it\n%s", err, out)
	}
}

// lockedBuffer is a bytes.Buffer safe for the exec copier goroutine and the
// test reading it.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
