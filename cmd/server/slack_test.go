package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/slackbridge"
	"piperoom/internal/store/memstore"
)

const testToken = "xoxb-test-not-a-real-token"

// fakeSlack is chat.postMessage: it records each post and answers ok.
type fakeSlack struct {
	mu    sync.Mutex
	posts []map[string]any
	auth  []string
}

func (f *fakeSlack) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var m map[string]any
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&m)
	f.mu.Lock()
	f.posts = append(f.posts, m)
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"ok":true,"channel":"C1","ts":"1.1"}`)
}

func (f *fakeSlack) snapshot() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.posts...)
}

func newTestService(t *testing.T, st *memstore.Store, hook func(context.Context, string)) *room.Service {
	t.Helper()
	own, err := owners.Load("../../dev/owners.yaml")
	if err != nil {
		t.Fatal(err)
	}
	areg, err := demoActionRegistry()
	if err != nil {
		t.Fatal(err)
	}
	svc, err := room.New(room.Config{
		Store: st, Owners: own, Users: room.DemoUsers(), Actions: areg,
		Logger: slog.New(slog.DiscardHandler), OnRoomOpened: hook,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Shutdown)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return svc
}

func waitPosts(t *testing.T, fs *fakeSlack, n int) []map[string]any {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if p := fs.snapshot(); len(p) >= n {
			return p
		}
	}
	t.Fatalf("got %d Slack posts, want %d", len(fs.snapshot()), n)
	return nil
}

func TestSlackMirrorsRoomIntoThread(t *testing.T) {
	fs := &fakeSlack{}
	srv := httptest.NewServer(fs)
	defer srv.Close()
	st := memstore.New()
	m, err := newMirrors(context.Background(), st, slackbridge.NewMemThreads(),
		slackSettings{Token: testToken, ChannelID: "C1", apiBase: srv.URL}, slog.New(slog.DiscardHandler))
	if err != nil || m == nil {
		t.Fatalf("newMirrors = %v, %v", m, err)
	}
	svc := newTestService(t, st, m.onRoomOpened)
	t.Cleanup(func() { m.stop(mirrorStopTimeout) }) // runs before svc.Shutdown (LIFO)

	kabir, _ := room.DemoUsers().Get("kabir")
	snap, err := svc.CreateRoom(context.Background(), kabir, room.CreateRoomInput{Title: "Orders stale", Severity: "sev2", PipelineID: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	posts := waitPosts(t, fs, 1)
	if posts[0]["channel"] != "C1" || posts[0]["thread_ts"] != nil {
		t.Fatalf("first post must be a parent in C1: %v", posts[0])
	}
	if _, err := svc.Close(context.Background(), kabir, snap.ID, "fixed"); err != nil {
		t.Fatal(err)
	}
	posts = waitPosts(t, fs, 2)
	if posts[1]["thread_ts"] != "1.1" || posts[1]["channel"] != "C1" {
		t.Fatalf("room.closed must be a reply in the parent's thread: %v", posts[1])
	}
	if txt, _ := posts[1]["text"].(string); !strings.Contains(strings.ToLower(txt), "closed") {
		t.Fatalf("reply does not look like the close: %q", txt)
	}
	fs.mu.Lock()
	for _, a := range fs.auth {
		if a != "Bearer "+testToken {
			t.Errorf("Authorization = %q", a)
		}
	}
	fs.mu.Unlock()

	// A second start for the same room (e.g. hook and startup racing) is harmless.
	if err := m.startOpen(context.Background(), svc); err != nil {
		t.Fatal(err)
	}
}

func TestSlackDisabledWithoutEnv(t *testing.T) {
	t.Setenv("SLACK_BOT_TOKEN", "")
	t.Setenv("SLACK_CHANNEL_ID", "")
	t.Setenv("PIPEROOM_PUBLIC_URL", "")
	var logs bytes.Buffer
	m, err := newMirrors(context.Background(), memstore.New(), slackbridge.NewMemThreads(), slackEnv(),
		slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil || m != nil {
		t.Fatalf("newMirrors with no env = %v, %v; want nil, nil", m, err)
	}
	if !strings.Contains(logs.String(), "slack bridge disabled") {
		t.Fatalf("missing disabled log: %s", logs.String())
	}

	// Even with a reachable Slack API, no token and no channel means no calls:
	// run then gives the room service no hook.
	hits := 0
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { mu.Lock(); hits++; mu.Unlock() }))
	defer srv.Close()
	st := memstore.New()
	m, err = newMirrors(context.Background(), st, slackbridge.NewMemThreads(), slackSettings{apiBase: srv.URL}, slog.New(slog.DiscardHandler))
	if err != nil || m != nil {
		t.Fatalf("newMirrors with an API but no settings = %v, %v; want nil, nil", m, err)
	}
	svc := newTestService(t, st, nil)
	kabir, _ := room.DemoUsers().Get("kabir")
	if _, err := svc.CreateRoom(context.Background(), kabir, room.CreateRoomInput{Title: "x", Severity: "sev3", PipelineID: "orders"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if hits != 0 {
		t.Fatalf("%d Slack calls with the bridge disabled", hits)
	}
}

func TestSlackEnvReadsAndHidesToken(t *testing.T) {
	t.Setenv("SLACK_BOT_TOKEN", " "+testToken+" ")
	t.Setenv("SLACK_CHANNEL_ID", "C0123ABC")
	t.Setenv("PIPEROOM_PUBLIC_URL", "https://piperoom.example")
	s := slackEnv()
	if s.Token.Reveal() != testToken || s.ChannelID != "C0123ABC" || s.PublicURL != "https://piperoom.example" {
		t.Fatalf("slackEnv = %+v", s)
	}
	var b bytes.Buffer
	slog.New(slog.NewTextHandler(&b, nil)).Info("settings", "slack", s, "token", s.Token)
	if strings.Contains(b.String(), testToken) {
		t.Fatalf("token leaked into a log line: %s", b.String())
	}
}

// TestBadSlackChannelFailsStartup runs the real startup path in demo mode.
func TestBadSlackChannelFailsStartup(t *testing.T) {
	t.Setenv("SLACK_BOT_TOKEN", testToken)
	t.Setenv("SLACK_CHANNEL_ID", "#incidents")
	t.Setenv("OWNERS_FILE", "../../dev/owners.yaml")
	var logs bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- run(true, 0, "127.0.0.1:0", false, slog.New(slog.NewTextHandler(&logs, nil))) }()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("startup with a bad SLACK_CHANNEL_ID did not fail")
	}
	if err == nil || !strings.Contains(err.Error(), "channel") {
		t.Fatalf("run = %v, want a channel error", err)
	}
	if strings.Contains(err.Error(), testToken) || strings.Contains(logs.String(), testToken) {
		t.Fatalf("token leaked: err=%v logs=%s", err, logs.String())
	}
}
