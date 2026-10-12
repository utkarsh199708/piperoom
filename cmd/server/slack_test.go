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

	"piperoom/internal/room"
	"piperoom/internal/slackbridge"
	"piperoom/internal/store/memstore"
)

const fakeToken = "xoxb-test-not-a-real-token"

// fakeSlack is a chat.postMessage stand-in that records every post.
type fakeSlack struct {
	srv   *httptest.Server
	mu    sync.Mutex
	posts []map[string]any
	auth  []string
}

func newFakeSlack(t *testing.T) *fakeSlack {
	t.Helper()
	f := &fakeSlack{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
		f.mu.Lock()
		f.posts = append(f.posts, body)
		f.auth = append(f.auth, r.Header.Get("Authorization"))
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true,"channel":"C1","ts":"1.1"}`)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeSlack) snapshot() ([]map[string]any, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.posts...), append([]string(nil), f.auth...)
}

func setSlackEnv(t *testing.T, token, channel string) {
	t.Helper()
	t.Setenv("SLACK_BOT_TOKEN", token)
	t.Setenv("SLACK_CHANNEL_ID", channel)
	t.Setenv("PIPEROOM_PUBLIC_URL", "")
}

// newSlackService wires mirrors and the room service the way run does.
func newSlackService(t *testing.T, s slackSettings) (*room.Service, *mirrors) {
	t.Helper()
	st := memstore.New()
	m, err := newMirrors(context.Background(), st, slackbridge.NewMemThreads(), s, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("newMirrors: %v", err)
	}
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun, OnRoomOpened: m.onRoomOpened()})
	// LIFO: the service closes first, then the mirrors stop, like run.
	t.Cleanup(func() { m.stop(mirrorStopTimeout) })
	t.Cleanup(svc.Close)
	return svc, m
}

func TestSlackMirrorsRoomIntoOneThread(t *testing.T) {
	slack := newFakeSlack(t)
	setSlackEnv(t, fakeToken, "C1")
	s := slackEnv()
	s.apiBase = slack.srv.URL
	svc, m := newSlackService(t, s)
	if m == nil {
		t.Fatal("bridge disabled with token and channel set")
	}

	arjun := mustUser(t, svc, "arjun")
	snap, err := svc.CreateRoom(context.Background(), arjun, "orders lag", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	if _, err := svc.CloseRoom(context.Background(), arjun, snap.ID, "schema fixed"); err != nil {
		t.Fatalf("close room: %v", err)
	}

	// The mirror returns after room.closed; posts are paced ~1 s apart.
	deadline := time.Now().Add(10 * time.Second)
	var posts []map[string]any
	var auth []string
	for time.Now().Before(deadline) {
		posts, auth = slack.snapshot()
		if len(posts) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(posts) != 2 {
		t.Fatalf("got %d Slack posts, want 2 (parent + room.closed): %v", len(posts), posts)
	}
	if _, ok := posts[0]["thread_ts"]; ok || posts[0]["channel"] != "C1" {
		t.Fatalf("first post is not a parent in C1: %v", posts[0])
	}
	if posts[1]["thread_ts"] != "1.1" || posts[1]["channel"] != "C1" {
		t.Fatalf("second post is not a reply in thread 1.1: %v", posts[1])
	}
	if txt, _ := posts[1]["text"].(string); !strings.Contains(txt, "schema fixed") {
		t.Fatalf("reply text %q does not carry the room.closed resolution", txt)
	}
	for _, a := range auth {
		if a != "Bearer "+fakeToken {
			t.Fatalf("Authorization header = %q", a)
		}
	}
}

func TestSlackDisabledMakesNoCalls(t *testing.T) {
	slack := newFakeSlack(t)
	setSlackEnv(t, "", "")
	s := slackEnv()
	s.apiBase = slack.srv.URL

	var logs bytes.Buffer
	m, err := newMirrors(context.Background(), memstore.New(), slackbridge.NewMemThreads(), s, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil || m != nil {
		t.Fatalf("newMirrors with env unset = %v, %v; want nil, nil", m, err)
	}
	if !strings.Contains(logs.String(), "slack bridge disabled") {
		t.Fatalf("missing 'slack bridge disabled' log: %q", logs.String())
	}
	// A nil *mirrors is a no-op everywhere, so run is unchanged.
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun, OnRoomOpened: m.onRoomOpened()})
	t.Cleanup(svc.Close)
	if err := m.startOpen(context.Background(), svc); err != nil {
		t.Fatalf("startOpen on nil mirrors: %v", err)
	}
	if _, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t", "sev2", "orders"); err != nil {
		t.Fatalf("create room: %v", err)
	}
	m.stop(time.Second)
	time.Sleep(100 * time.Millisecond)
	if posts, _ := slack.snapshot(); len(posts) != 0 {
		t.Fatalf("bridge disabled but Slack got %d posts", len(posts))
	}
}

func TestSlackBadConfigFailsWithoutToken(t *testing.T) {
	for _, tc := range []struct{ name, token, channel string }{
		{"channel name", fakeToken, "#incidents"},
		{"token only", fakeToken, ""},
		{"channel only", "", "C1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setSlackEnv(t, tc.token, tc.channel)
			var logs bytes.Buffer
			m, err := newMirrors(context.Background(), memstore.New(), slackbridge.NewMemThreads(), slackEnv(), slog.New(slog.NewTextHandler(&logs, nil)))
			if err == nil {
				m.stop(time.Second)
				t.Fatal("want a start-up error")
			}
			if strings.Contains(err.Error(), fakeToken) || strings.Contains(logs.String(), fakeToken) {
				t.Fatalf("token leaked: err=%q logs=%q", err, logs.String())
			}
		})
	}
}

// Rooms already open at start-up are mirrored; a later hook call for the same
// room is deduplicated by the bridge (ErrAlreadyRunning), so one parent only.
func TestSlackStartOpenMirrorsExistingRooms(t *testing.T) {
	slack := newFakeSlack(t)
	setSlackEnv(t, fakeToken, "C1")
	s := slackEnv()
	s.apiBase = slack.srv.URL
	st := memstore.New()
	pre := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	snap, err := pre.CreateRoom(context.Background(), mustUser(t, pre, "arjun"), "t", "sev2", "orders")
	pre.Close()
	if err != nil {
		t.Fatalf("create room: %v", err)
	}

	m, err := newMirrors(context.Background(), st, slackbridge.NewMemThreads(), s, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("newMirrors: %v", err)
	}
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun, OnRoomOpened: m.onRoomOpened()})
	t.Cleanup(func() { m.stop(mirrorStopTimeout) })
	t.Cleanup(svc.Close)
	if err := m.startOpen(context.Background(), svc); err != nil {
		t.Fatalf("startOpen: %v", err)
	}
	m.start(snap.ID) // duplicate: must not post a second parent
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if posts, _ := slack.snapshot(); len(posts) >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(1500 * time.Millisecond) // a second parent would be paced ~1 s later
	posts, _ := slack.snapshot()
	if len(posts) != 1 {
		t.Fatalf("got %d posts, want exactly one parent: %v", len(posts), posts)
	}
	if _, ok := posts[0]["thread_ts"]; ok {
		t.Fatalf("post is not a parent: %v", posts[0])
	}
}
