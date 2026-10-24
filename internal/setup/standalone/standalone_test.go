package standalone

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/quickstart"
)

// testConfig is a startable standalone config: loopback on an ephemeral port,
// dev login, quickstart's default users file. Run forces write mode and setup
// on; the values here are what the environment would hold.
func testConfig() config.Config {
	return config.Config{HTTPAddr: "127.0.0.1:0", AuthMode: "dev", WriteMode: "off", SetupEnabled: "off", UsersFile: "dev/users.yaml"}
}

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// noAgent ends every agent run at once: the server tests exercise the HTTP
// surface, the event log and the watcher, not the model loop.
func noAgent(context.Context, agent.Alert, agent.Deps, agent.Options) error { return nil }

// ownersBody is an owners file with one CDC pipeline whose every layer the
// principal owns.
func ownersBody(principal, title string) string {
	var b strings.Builder
	b.WriteString("pipelines:\n  - id: orders\n    title: " + title + "\n    topic_prefix: pg\n")
	b.WriteString("    source_tables: [public.orders]\n    connectors: [pg-orders-connector]\n")
	b.WriteString("    consumer_groups: [order-sync]\n    sink_tables: [analytics.orders]\n    sink_ts_column: updated_at\n")
	b.WriteString("layers:\n")
	for _, l := range events.LayersOf(events.PipelineKindCDC) {
		b.WriteString("  " + l + ": " + principal + "\n")
	}
	return b.String()
}

func writeOwners(t *testing.T, dir, body string) string {
	t.Helper()
	path := filepath.Join(dir, quickstart.DefaultOwnersFile)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// lockedBuffer is a concurrency-safe log and stdout sink: the server and the
// watcher write from different goroutines while the test reads.
type lockedBuffer struct {
	mu sync.Mutex
	b  strings.Builder
	// urls receives every line that starts with http://, in order.
	urls chan string
}

func newLockedBuffer() *lockedBuffer { return &lockedBuffer{urls: make(chan string, 16)} }

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b.Write(p)
	for _, line := range strings.Split(string(p), "\n") {
		if strings.HasPrefix(line, "http://") {
			select {
			case l.urls <- strings.TrimSpace(line):
			default:
			}
		}
	}
	return len(p), nil
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// waitURL waits for the next printed URL and fails the test after timeout.
func (l *lockedBuffer) waitURL(t *testing.T, timeout time.Duration) string {
	t.Helper()
	select {
	case u := <-l.urls:
		return u
	case <-time.After(timeout):
		t.Fatalf("no URL printed within %s; output so far:\n%s", timeout, l.String())
		return ""
	}
}

// startRun starts Run in the background and returns its error channel.
func startRun(t *testing.T, opts Options) (chan error, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, opts) }()
	return done, cancel
}

func stopRun(t *testing.T, done chan error, cancel context.CancelFunc) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancel: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

// TestRunPrintsURL proves the done-when "prints a URL in < 10 s" from an
// empty project directory: the bootstrap owners file, the users file and the
// listener all happen without a stack, and the URL goes out before the serve
// loop blocks.
func TestRunPrintsURL(t *testing.T) {
	dir := t.TempDir() // empty: no owners.yaml
	out := newLockedBuffer()
	started := time.Now()
	done, cancel := startRun(t, Options{
		Config:     testConfig(),
		ProjectDir: dir,
		Stdout:     out,
		Log:        discardLog(),
		Agent:      noAgent,
	})
	defer stopRun(t, done, cancel)

	url := out.waitURL(t, 10*time.Second)
	if elapsed := time.Since(started); elapsed >= 10*time.Second {
		t.Fatalf("the URL took %s, want under 10 s", elapsed)
	}
	if !strings.HasPrefix(url, "http://127.0.0.1:") {
		t.Fatalf("URL %q, want a loopback 127.0.0.1 URL", url)
	}
	resp, err := http.Get(url + "healthz")
	if err != nil {
		t.Fatalf("GET %shealthz: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status %d, want 200", resp.StatusCode)
	}
	// The first start from an empty directory leaves a loadable owners file
	// naming only layers, and the setup secrets directory exists.
	own, err := owners.Load(filepath.Join(dir, quickstart.DefaultOwnersFile))
	if err != nil {
		t.Fatalf("bootstrap owners file: %v", err)
	}
	if len(own.Pipelines) != 0 {
		t.Fatalf("bootstrap owners file has %d pipelines, want none", len(own.Pipelines))
	}
	for _, l := range events.LayersOf(events.PipelineKindCDC) {
		if _, ok := own.Layers[l]; !ok {
			t.Fatalf("bootstrap owners file does not own layer %q", l)
		}
	}
	if info, err := os.Stat(filepath.Join(dir, ".piperoom", "secrets")); err != nil || !info.IsDir() {
		t.Fatalf("setup secrets directory: %v", err)
	}
	// The secrets directory is under the project, so the project's .gitignore
	// must name it (U1-4's rule) even before any runner writes the store.
	if gi, err := os.ReadFile(filepath.Join(dir, ".gitignore")); err != nil || !strings.Contains(string(gi), ".piperoom/") {
		t.Fatalf(".gitignore = %q, %v; want an entry for .piperoom/", gi, err)
	}
}

// The done-when "refuses a non-loopback address": the refusal comes before
// anything is written into the project.
func TestRunRefusesNonLoopback(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:0", "[::]:0", ":0", "192.168.1.5:8090", "piperoom.example.com:8090"} {
		t.Run(addr, func(t *testing.T) {
			dir := t.TempDir()
			cfg := testConfig()
			cfg.HTTPAddr = addr
			err := Run(context.Background(), Options{
				Config:     cfg,
				ProjectDir: dir,
				Stdout:     io.Discard,
				Log:        discardLog(),
			})
			if err == nil {
				t.Fatal("want a refusal")
			}
			if !errors.Is(err, quickstart.ErrNonLoopback) {
				t.Fatalf("err = %v, want ErrNonLoopback", err)
			}
			if _, statErr := os.Stat(filepath.Join(dir, quickstart.DefaultOwnersFile)); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("a refused start wrote an owners file: %v", statErr)
			}
		})
	}
}

// The done-when "every room event is appended to the log file": a room opened
// over HTTP lands in setup-log.jsonl as one JSON object per event, room.created
// included (the subscription replays it).
func TestRunAppendsEveryRoomEvent(t *testing.T) {
	dir := t.TempDir()
	writeOwners(t, dir, ownersBody("alice", "Orders CDC"))
	out := newLockedBuffer()
	done, cancel := startRun(t, Options{
		Config:     testConfig(),
		ProjectDir: dir,
		Stdout:     out,
		Log:        discardLog(),
		Agent:      noAgent,
	})
	defer stopRun(t, done, cancel)
	url := out.waitURL(t, 10*time.Second)

	body := strings.NewReader(`{"title":"orders lag","severity":"sev2","pipeline_id":"orders"}`)
	req, err := http.NewRequest(http.MethodPost, url+"api/rooms", body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User", "alice")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /api/rooms: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		t.Fatalf("POST /api/rooms status %d, want 201: %s", resp.StatusCode, raw)
	}
	var snap struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode the room snapshot: %v", err)
	}
	if snap.ID == "" {
		t.Fatal("the room snapshot has no id")
	}

	// A second event after the room exists: the log follows the live stream,
	// not just the replay at open.
	msg, err := http.NewRequest(http.MethodPost, url+"api/rooms/"+snap.ID+"/comments",
		strings.NewReader(`{"text":"checking the WAL level"}`))
	if err != nil {
		t.Fatal(err)
	}
	msg.Header.Set("Content-Type", "application/json")
	msg.Header.Set("X-User", "alice")
	cresp, err := http.DefaultClient.Do(msg)
	if err != nil {
		t.Fatalf("POST a comment: %v", err)
	}
	cresp.Body.Close()
	if cresp.StatusCode != http.StatusOK && cresp.StatusCode != http.StatusCreated {
		t.Fatalf("POST a comment status %d, want 200 or 201", cresp.StatusCode)
	}

	// The log subscription runs off the request path; poll for the lines.
	path := filepath.Join(dir, LogFileName)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if countEvents(t, path, snap.ID, events.RoomCreated) > 0 && countEvents(t, path, snap.ID, events.CommentAdded) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("setup-log.jsonl lacks room.created or comment.added for room %s; file:\n%s", snap.ID, readFile(t, path))
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Every line on disk is a complete JSON event of this room.
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	lines := 0
	for sc.Scan() {
		var e events.Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("line %d of setup-log.jsonl is not one JSON event: %v: %s", lines+1, err, sc.Text())
		}
		if e.RoomID != snap.ID || e.Type == "" || e.Seq == 0 {
			t.Fatalf("line %d is not a complete room event: %+v", lines+1, e)
		}
		lines++
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if lines == 0 {
		t.Fatal("setup-log.jsonl is empty")
	}
}

func countEvents(t *testing.T, path, roomID string, typ events.Type) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var e events.Event
		if json.Unmarshal([]byte(line), &e) != nil {
			continue
		}
		if e.RoomID == roomID && e.Type == typ {
			n++
		}
	}
	return n
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return "(unreadable: " + err.Error() + ")"
	}
	return string(data)
}

// The done-when "--watch restarts the standalone server on a written
// owners.yaml and the watcher ticks": a second URL is printed after the
// rewrite, and the tick lines are in the log.
func TestRunWatchRestartsOnOwnersWrite(t *testing.T) {
	dir := t.TempDir()
	ownersPath := writeOwners(t, dir, ownersBody("alice", "Orders CDC"))
	out := newLockedBuffer()
	logs := newLockedBuffer()
	done, cancel := startRun(t, Options{
		Config:        testConfig(),
		ProjectDir:    dir,
		Watch:         true,
		WatchInterval: 20 * time.Millisecond,
		Stdout:        out,
		Log:           slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Agent:         noAgent,
	})
	defer stopRun(t, done, cancel)

	first := out.waitURL(t, 10*time.Second)
	// Let the watcher tick at least once before the write, so the tick lines
	// are not only a consequence of the change.
	waitFor(t, 5*time.Second, func() bool {
		return strings.Contains(logs.String(), "watch tick")
	}, "the watcher logged no tick")

	if err := os.WriteFile(ownersPath, []byte(ownersBody("alice", "Orders CDC (renamed)")), 0o600); err != nil {
		t.Fatal(err)
	}
	second := out.waitURL(t, 10*time.Second)
	if second == first {
		t.Fatalf("the restart reused the printed URL %q", first)
	}
	waitFor(t, 5*time.Second, func() bool {
		return strings.Contains(logs.String(), "owners file written; restarting the standalone server")
	}, "no restart line in the log")
	// The restarted server listens and answers.
	resp, err := http.Get(second + "healthz")
	if err != nil {
		t.Fatalf("GET %shealthz after the restart: %v", second, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz after the restart: status %d", resp.StatusCode)
	}
}

// waitFor polls cond until it is true or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWatchIntervalFloor(t *testing.T) {
	err := Run(context.Background(), Options{
		Config:        testConfig(),
		ProjectDir:    t.TempDir(),
		Watch:         true,
		WatchInterval: time.Microsecond,
		Log:           discardLog(),
	})
	if err == nil || !strings.Contains(err.Error(), "--watch-interval") {
		t.Fatalf("err = %v, want a refusal naming --watch-interval", err)
	}
}

// TestRunWatchSurvivesBadOwnersWrite: a write that does not load (a typo, a
// half-finished save) must not stop the serving process. The running server
// keeps its loaded file, and the next good write restarts as usual.
func TestRunWatchSurvivesBadOwnersWrite(t *testing.T) {
	dir := t.TempDir()
	ownersPath := writeOwners(t, dir, ownersBody("alice", "Orders CDC"))
	out := newLockedBuffer()
	logs := newLockedBuffer()
	done, cancel := startRun(t, Options{
		Config:        testConfig(),
		ProjectDir:    dir,
		Watch:         true,
		WatchInterval: 20 * time.Millisecond,
		Stdout:        out,
		Log:           slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Agent:         noAgent,
	})
	defer stopRun(t, done, cancel)

	first := out.waitURL(t, 10*time.Second)
	if err := os.WriteFile(ownersPath, []byte("layers: [not, a, mapping\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return strings.Contains(logs.String(), "does not load")
	}, "the bad write was not logged")
	// The first server still answers: the bad file restarted nothing.
	resp, err := http.Get(first + "healthz")
	if err != nil {
		t.Fatalf("GET %shealthz after a bad owners write: %v", first, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz after a bad owners write: status %d", resp.StatusCode)
	}
	select {
	case err := <-done:
		t.Fatalf("the watch stopped on a bad owners write: %v", err)
	default:
	}
	// A good write still restarts.
	if err := os.WriteFile(ownersPath, []byte(ownersBody("alice", "Orders CDC (fixed)")), 0o600); err != nil {
		t.Fatal(err)
	}
	second := out.waitURL(t, 10*time.Second)
	if second == first {
		t.Fatalf("the restart reused the printed URL %q", first)
	}
}

// TestRunWatchRestartsForSeveralOwners: the setup room's registration writes
// an owners file naming several people; the restart that picks it up must
// start (quickstart writes a users file for them) rather than die looking for
// dev/users.yaml.
func TestRunWatchRestartsForSeveralOwners(t *testing.T) {
	dir := t.TempDir()
	ownersPath := writeOwners(t, dir, ownersBody("alice", "Orders CDC"))
	out := newLockedBuffer()
	done, cancel := startRun(t, Options{
		Config:        testConfig(),
		ProjectDir:    dir,
		Watch:         true,
		WatchInterval: 20 * time.Millisecond,
		Stdout:        out,
		Log:           discardLog(),
		Agent:         noAgent,
	})
	defer stopRun(t, done, cancel)

	first := out.waitURL(t, 10*time.Second)
	var multi strings.Builder
	multi.WriteString(ownersBody("alice", "Orders CDC"))
	// Reassign half the layers to a second person, as a registration would.
	body := strings.Replace(multi.String(), ": alice\n", ": priya\n", 2)
	if err := os.WriteFile(ownersPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	second := out.waitURL(t, 10*time.Second)
	if second == first {
		t.Fatalf("the restart reused the printed URL %q", first)
	}
	resp, err := http.Get(second + "healthz")
	if err != nil {
		t.Fatalf("GET %shealthz after the multi-owner restart: %v", second, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz after the multi-owner restart: status %d", resp.StatusCode)
	}
}

// TestRunWritesGitignore: prepare names both the secrets directory and the
// event log in the project's .gitignore before anything serves.
func TestRunWritesGitignore(t *testing.T) {
	dir := t.TempDir()
	writeOwners(t, dir, ownersBody("alice", "Orders CDC"))
	if _, err := prepare(Options{Config: testConfig(), ProjectDir: dir}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, ".gitignore"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{".piperoom/", LogFileName} {
		if !strings.Contains(string(raw), want+"\n") {
			t.Errorf(".gitignore lacks %q:\n%s", want, raw)
		}
	}
}

// TestRunGitignoresModelEnvWithSecretsElsewhere: with PIPEROOM_SECRETS_DIR
// outside the project, .piperoom/ is still ignored, because the model picker
// writes its key to <project>/.piperoom/model.env whatever the secrets
// directory is.
func TestRunGitignoresModelEnvWithSecretsElsewhere(t *testing.T) {
	dir := t.TempDir()
	writeOwners(t, dir, ownersBody("alice", "Orders CDC"))
	cfg := testConfig()
	cfg.SecretsDir = filepath.Join(t.TempDir(), "secrets")
	if _, err := prepare(Options{Config: cfg, ProjectDir: dir}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, ".gitignore"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{".piperoom/", LogFileName} {
		if !strings.Contains(string(raw), want+"\n") {
			t.Errorf(".gitignore lacks %q with the secrets directory elsewhere:\n%s", want, raw)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, ".piperoom", "secrets")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the project got a .piperoom/secrets directory although PIPEROOM_SECRETS_DIR points elsewhere: %v", err)
	}
}
