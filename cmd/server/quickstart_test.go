package main

// The --quickstart start lives in internal/quickstart; these tests exercise
// the cmd/server wrapper and the real server composition it hands the package:
// the start runs whole in its own process (flags, config, the generated users
// file, the in-memory store, the agent endpoint, the HTTP handler); only the
// data stack is left out (noDataStack), since unit tests run without Docker.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/quickstart"
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
		name                                string
		demo, quickstart, solo, allowRemote bool
		owners                              string
		wantErr                             string // "" = allowed
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
		{name: "solo alone", solo: true, wantErr: "--solo needs --demo or --quickstart"},
		{name: "demo with solo", demo: true, solo: true},
		{name: "quickstart with solo", quickstart: true, solo: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkStartFlags(tc.demo, tc.quickstart, tc.solo, tc.allowRemote, tc.owners)
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

// noDataStack opens no client set: every pipeline gets the empty set (runbook
// tool only), so the quickstart start path runs with no data stack.
//
// It also reports opts.ReadOnly on stderr (the log stream, so stdout stays the
// URL alone) for the tests that check the start passes the write mode down.
func noDataStack(ctx context.Context, cfg config.Config, awsCfg *aws.Config, own *owners.Owners, opts wiring.StackOptions) (*wiring.Stacks, error) {
	fmt.Fprintf(os.Stderr, "TEST openStacks ReadOnly=%t\n", opts.ReadOnly)
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
// auth mode unset, the temp directory inside the test's own. helper names the
// helper test to re-run in the subprocess.
func quickstartCmd(ctx context.Context, t *testing.T, helper, ownersPath, tmp string, env ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run="+helper)
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

// optionsHelper is not a test: the options test re-runs this test binary with
// PIPEROOM_TEST_QUICKSTART_OPTIONS=1 so quickstart.Run serves through the real
// cmd/server composition in its own process, with the write mode and setup
// switches the test asks for. write mode on needs no model: the endpoint is
// built, never called.
func optionsHelper(t *testing.T) {
	if os.Getenv("PIPEROOM_TEST_QUICKSTART_OPTIONS") != "1" {
		return
	}
	cfg := config.Load()
	log, err := newLogger(cfg, os.Stderr)
	if err != nil {
		os.Stdout.WriteString("RUN ERROR: " + err.Error() + "\n")
		os.Exit(1)
	}
	slog.SetDefault(log)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err = quickstart.Run(ctx, quickstart.Options{
		Config:       cfg,
		WriteMode:    os.Getenv("PIPEROOM_TEST_QS_WRITE") == "on",
		SetupEnabled: os.Getenv("PIPEROOM_TEST_QS_SETUP") == "on",
		OwnersPath:   os.Getenv("PIPEROOM_TEST_OWNERS"),
		Stdout:       os.Stdout,
		Log:          log,
		Serve: func(ctx context.Context, s quickstart.Settings) error {
			m := serveMode{quickstart: true, log: log, stdout: s.Stdout}
			settings, err := parseServer(s.Config, m)
			if err != nil {
				return err
			}
			return serve(ctx, startDeps{openStacks: noDataStack, stdout: os.Stdout},
				s.Config, s.Owners, s.Users, m, settings)
		},
	})
	if err != nil {
		os.Stdout.WriteString("RUN ERROR: " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(0)
}

func TestQuickstartOptionsHelper(t *testing.T) { optionsHelper(t) }

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
	cmd := quickstartCmd(ctx, t, "^TestQuickstartHelper$", ownersPath, tmp)
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
	// No setup endpoint without PIPEROOM_SETUP_ENABLED=on.
	if code, body := get("/api/runners/ws"); code != http.StatusNotFound {
		t.Fatalf("GET /api/runners/ws = %d %s, want 404 with setup off", code, body)
	}

	// The one-user users file lives in the temp directory while the server
	// runs.
	files, _ := filepath.Glob(filepath.Join(tmp, quickstart.TempDirPrefix+"*", quickstart.UsersFileName))
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
	// Write mode is off, so run opens the stacks with ReadOnly (P1a-3).
	for _, want := range []string{"mode=quickstart", "write mode off", "wrote a users file", "TEST openStacks ReadOnly=true"} {
		if !strings.Contains(logs, want) {
			t.Errorf("the log lacks %q:\n%s", want, logs)
		}
	}
	if runtime.GOOS != "windows" {
		// The graceful stop removes the users file (Windows was killed).
		if left, _ := filepath.Glob(filepath.Join(tmp, quickstart.TempDirPrefix+"*")); len(left) != 0 {
			t.Fatalf("the temp users dir outlived the server: %v", left)
		}
	}
}

// WriteMode and SetupEnabled reach the server runQuickstart hands the package:
// write mode on opens the stacks for writing (ReadOnly=false) and shows on
// /healthz; setup on mounts GET /api/runners/ws; setup off leaves it 404.
func TestQuickstartOptionsReachServer(t *testing.T) {
	for _, tc := range []struct {
		name       string
		write      bool
		setup      bool
		wantHealth string
		wantWS     int
		wantRO     string
	}{
		{name: "default read-only", wantHealth: "off", wantWS: http.StatusNotFound, wantRO: "true"},
		{name: "write on setup off", write: true, wantHealth: "on", wantWS: http.StatusNotFound, wantRO: "false"},
		{name: "write on setup on", write: true, setup: true, wantHealth: "on", wantWS: http.StatusUpgradeRequired, wantRO: "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ownersPath := writeOwnersFile(t, "user:"+quickstartOwner)
			tmp := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			env := []string{
				"PIPEROOM_TEST_QUICKSTART_OPTIONS=1",
				"PIPEROOM_TEST_QS_WRITE=" + onOff(tc.write),
				"PIPEROOM_TEST_QS_SETUP=" + onOff(tc.setup),
			}
			if tc.write {
				env = append(env, "PIPEROOM_WRITE_MODE=on")
			}
			if tc.setup {
				env = append(env,
					"PIPEROOM_SETUP_ENABLED=on",
					"PIPEROOM_SECRETS_DIR="+filepath.Join(t.TempDir(), "secrets"))
			}
			cmd := quickstartCmd(ctx, t, "^TestQuickstartOptionsHelper$", ownersPath, tmp, env...)
			base, logs, stop := startQuickstart(t, cmd)
			defer stop()

			client := &http.Client{Timeout: 5 * time.Second}
			get := func(path string) (int, []byte) {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(base, "/")+path, nil)
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("X-User", quickstartOwner)
				resp, err := client.Do(req)
				if err != nil {
					t.Fatalf("GET %s: %v\nstderr:\n%s", path, err, logs.String())
				}
				defer resp.Body.Close()
				body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
				if err != nil {
					t.Fatal(err)
				}
				return resp.StatusCode, body
			}

			code, body := get("/healthz")
			var health struct {
				WriteMode string `json:"write_mode"`
			}
			if code != http.StatusOK || json.Unmarshal(body, &health) != nil || health.WriteMode != tc.wantHealth {
				t.Fatalf("GET /healthz = %d %s, want write_mode %s", code, body, tc.wantHealth)
			}
			// A plain GET is not a WebSocket upgrade: mounted answers 426 from
			// the accept, unmounted 404 like any unknown route.
			if code, body := get("/api/runners/ws"); code != tc.wantWS {
				t.Fatalf("GET /api/runners/ws = %d %s, want %d", code, body, tc.wantWS)
			}
			if want := "TEST openStacks ReadOnly=" + tc.wantRO; !strings.Contains(logs.String(), want) {
				t.Fatalf("the log lacks %q:\n%s", want, logs.String())
			}
		})
	}
}

// startQuickstart waits for the helper to print its URL and returns it with
// the log so far and a stop func (the graceful interrupt, kill on Windows).
func startQuickstart(t *testing.T, cmd *exec.Cmd) (string, *lockedBuffer, func()) {
	t.Helper()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var logs lockedBuffer
	cmd.Stderr = &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	var waitErr error
	urls := make(chan string, 1)
	go func() {
		defer close(exited)
		sc := bufio.NewScanner(stdout)
		first := true
		for sc.Scan() {
			if first {
				urls <- sc.Text()
				first = false
			}
		}
		waitErr = cmd.Wait()
	}()
	stop := func() {
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
	select {
	case base := <-urls:
		if !strings.HasPrefix(base, "http://127.0.0.1:") {
			t.Fatalf("printed URL %q, want http://127.0.0.1:<port>/", base)
		}
		return base, &logs, stop
	case <-exited:
		t.Fatalf("helper exited before printing a URL: %v\nstderr:\n%s", waitErr, logs.String())
	case <-time.After(30 * time.Second):
		t.Fatalf("no URL within 30s\nstderr:\n%s", logs.String())
	}
	return "", nil, func() {}
}

func onOff(on bool) string {
	if on {
		return "on"
	}
	return "off"
}

// The whole start refuses a non-loopback address with --quickstart before
// anything listens or is written, even with both dev switches set.
func TestQuickstartStartRefusesNonLoopback(t *testing.T) {
	ownersPath := writeOwnersFile(t, "user:"+quickstartOwner)
	tmp := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := quickstartCmd(ctx, t, "^TestQuickstartHelper$", ownersPath, tmp,
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
	if left, _ := filepath.Glob(filepath.Join(tmp, quickstart.TempDirPrefix+"*")); len(left) != 0 {
		t.Fatalf("the refused start wrote %v", left)
	}
}

// PIPEROOM_WRITE_MODE=on with --quickstart refuses the start.
func TestQuickstartStartRefusesWriteModeOn(t *testing.T) {
	ownersPath := writeOwnersFile(t, "user:"+quickstartOwner)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := quickstartCmd(ctx, t, "^TestQuickstartHelper$", ownersPath, t.TempDir(), "PIPEROOM_WRITE_MODE=on")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "RUN ERROR") || !strings.Contains(string(out), "PIPEROOM_WRITE_MODE=on") {
		t.Fatalf("PIPEROOM_WRITE_MODE=on: err %v, want a start-up refusal naming it\n%s", err, out)
	}
}

// A listen failure comes after the one-user users file is written, so the
// start must still remove its temp directory, and the error must carry the
// "server:" prefix once (#600 review).
func TestQuickstartListenErrorRemovesTempDir(t *testing.T) {
	// Occupy a loopback port so the server's own Listen fails.
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	ownersPath := writeOwnersFile(t, "user:"+quickstartOwner)
	tmp := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := quickstartCmd(ctx, t, "^TestQuickstartHelper$", ownersPath, tmp, "PIPEROOM_HTTP_ADDR="+busy.Addr().String())
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "RUN ERROR") {
		t.Fatalf("listen on the occupied %s: err %v, want a start-up error\n%s", busy.Addr(), err, out)
	}
	var runErr string
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "RUN ERROR: ") {
			runErr = strings.TrimPrefix(line, "RUN ERROR: ")
		}
	}
	want := "server: listen on " + busy.Addr().String() + ": "
	if !strings.HasPrefix(runErr, want) || strings.Count(runErr, "server:") != 1 {
		t.Fatalf("run error = %q, want it to start %q with one \"server:\" prefix", runErr, want)
	}
	// The users file was written before the listen (the log says so), and the
	// failed start removed it.
	if !strings.Contains(string(out), "wrote a users file") {
		t.Fatalf("the start did not reach the users file before the listen:\n%s", out)
	}
	if left, _ := filepath.Glob(filepath.Join(tmp, quickstart.TempDirPrefix+"*")); len(left) != 0 {
		t.Fatalf("the temp users dir outlived the failed listen: %v", left)
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
