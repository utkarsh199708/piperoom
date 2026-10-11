package slackbridge

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/events"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type post struct{ channel, thread, text string }

type fakePoster struct {
	mu    sync.Mutex
	posts []post
	err   error
}

func (f *fakePoster) Post(_ context.Context, ch, th, text string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return "", f.err
	}
	f.posts = append(f.posts, post{ch, th, text})
	return "100.1", nil
}

type fakeStore struct{ ch chan events.Event }

func (fakeStore) Append(context.Context, string, events.Type, events.Actor, any) (events.Event, error) {
	return events.Event{}, errors.New("unused")
}
func (fakeStore) Since(context.Context, string, int64, int) ([]events.Event, error) { return nil, nil }
func (s fakeStore) Subscribe(context.Context, string, int64) (<-chan events.Event, error) {
	return s.ch, nil
}

func ev(seq int64, typ events.Type, actor events.Actor, payload any) events.Event {
	raw, _ := json.Marshal(payload)
	return events.Event{RoomID: "r1", Seq: seq, Type: typ, Actor: actor, Payload: raw}
}

func TestFormatEscapesAndFilters(t *testing.T) {
	e := ev(1, events.AgentMessage, events.AgentActor, events.AgentMessagePayload{Text: "<!channel> a & b"})
	text, ok := Format(e, "")
	if !ok || strings.Contains(text, "<!channel>") || !strings.Contains(text, "&lt;!channel&gt; a &amp; b") {
		t.Fatalf("not escaped: %q", text)
	}
	for _, typ := range []events.Type{events.AgentToolCall, events.AgentToolResult, events.SteerSent} {
		if _, ok := Format(ev(2, typ, events.AgentActor, map[string]string{}), ""); ok {
			t.Errorf("%s should not be mirrored", typ)
		}
	}
	long := strings.Repeat("x", maxTextRunes*2)
	text, _ = Format(ev(3, events.AgentMessage, events.AgentActor, events.AgentMessagePayload{Text: long}), "")
	if len(text) > maxTextRunes+100 {
		t.Errorf("not truncated: %d", len(text))
	}
}

func TestBridgeThreadsAndEchoSuppression(t *testing.T) {
	st := fakeStore{ch: make(chan events.Event, 10)}
	fp := &fakePoster{}
	th := NewMemThreads()
	b := New(st, fp, th, "C1", "https://pr.example", discard)
	human := events.Actor{Kind: events.ActorHuman, ID: "u1", Name: "Ana"}

	b.markInbound("r1", "u1", "from slack")
	st.ch <- ev(1, events.RoomCreated, events.Actor{Kind: events.ActorSystem}, events.RoomCreatedPayload{Title: "lag", Severity: "sev2", PipelineID: "p"})
	st.ch <- ev(2, events.AgentToolCall, events.AgentActor, events.AgentToolCallPayload{})
	st.ch <- ev(3, events.CommentAdded, human, events.CommentAddedPayload{Text: "from slack"})
	st.ch <- ev(4, events.CommentAdded, human, events.CommentAddedPayload{Text: "from web"})
	st.ch <- ev(5, events.RoomClosed, human, events.RoomClosedPayload{Resolution: "fixed"})

	if err := b.Run(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	if len(fp.posts) != 3 {
		t.Fatalf("posts = %+v", fp.posts)
	}
	if fp.posts[0].thread != "" || fp.posts[1].thread != "100.1" || fp.posts[2].thread != "100.1" {
		t.Errorf("threading wrong: %+v", fp.posts)
	}
	if !strings.Contains(fp.posts[0].text, "https://pr.example/rooms/r1") {
		t.Errorf("no room link: %q", fp.posts[0].text)
	}
	if got, _ := th.Get("r1"); got.LastSeq != 5 || got.TS != "100.1" {
		t.Errorf("thread state = %+v", got)
	}
}

func TestBridgePostFailureDoesNotBlock(t *testing.T) {
	st := fakeStore{ch: make(chan events.Event, 2)}
	fp := &fakePoster{err: errors.New("down")}
	b := New(st, fp, NewMemThreads(), "C1", "", discard)
	st.ch <- ev(1, events.AgentMessage, events.AgentActor, events.AgentMessagePayload{Text: "hi"})
	st.ch <- ev(2, events.RoomClosed, events.AgentActor, events.RoomClosedPayload{})
	if err := b.Run(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
}

func TestClientPost(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer tok" {
			t.Error("missing token")
		}
		switch calls {
		case 1:
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(429)
		case 2:
			_, _ = io.WriteString(w, `{"ok":true,"ts":"9.9"}`)
		default:
			_, _ = io.WriteString(w, `{"ok":false,"error":"channel_not_found"}`)
		}
	}))
	defer srv.Close()
	c := NewClient("tok", srv.URL)
	ts, err := c.Post(context.Background(), "C", "", "x")
	if err != nil || ts != "9.9" {
		t.Fatalf("ts=%q err=%v", ts, err)
	}
	if _, err := c.Post(context.Background(), "C", "", "x"); !errors.Is(err, ErrSlack) {
		t.Fatalf("err = %v", err)
	}
}

type fakeComments struct {
	roomID, text string
	actor        events.Actor
	n            int
}

func (f *fakeComments) AddComment(_ context.Context, r string, a events.Actor, t string) error {
	f.roomID, f.actor, f.text = r, a, t
	f.n++
	return nil
}

type mapper map[string]events.Actor

func (m mapper) Resolve(id string) (events.Actor, bool) { a, ok := m[id]; return a, ok }

func signed(secret string, ts time.Time, body string) *http.Request {
	tss := strconv.FormatInt(ts.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("v0:" + tss + ":" + body))
	r := httptest.NewRequest(http.MethodPost, "/slack/events", strings.NewReader(body))
	r.Header.Set("X-Slack-Request-Timestamp", tss)
	r.Header.Set("X-Slack-Signature", "v0="+hex.EncodeToString(mac.Sum(nil)))
	return r
}

func TestInboundHandler(t *testing.T) {
	th := NewMemThreads()
	th.Put("r1", Thread{TS: "100.1"})
	b := New(fakeStore{}, &fakePoster{}, th, "C1", "", discard)
	fc := &fakeComments{}
	h := NewHandler("sec", "C1", b, fc, mapper{"U1": {Kind: events.ActorHuman, ID: "ana", Name: "Ana"}}, discard)
	now := time.Now()
	msg := func(user, thread, channel, extra string) string {
		return `{"type":"event_callback","event":{"type":"message","user":"` + user + `","text":"hello","channel":"` + channel + `","thread_ts":"` + thread + `","ts":"100.2"` + extra + `}}`
	}
	do := func(r *http.Request) int {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}

	if c := do(signed("sec", now, msg("U1", "100.1", "C1", ""))); c != 200 || fc.n != 1 || fc.roomID != "r1" || fc.actor.ID != "ana" {
		t.Fatalf("happy path: code=%d %+v", c, fc)
	}
	cases := map[string]*http.Request{
		"bad sig":      signed("wrong", now, msg("U1", "100.1", "C1", "")),
		"stale":        signed("sec", now.Add(-time.Hour), msg("U1", "100.1", "C1", "")),
		"unmapped":     signed("sec", now, msg("U9", "100.1", "C1", "")),
		"other chan":   signed("sec", now, msg("U1", "100.1", "C2", "")),
		"unknown thr":  signed("sec", now, msg("U1", "55.5", "C1", "")),
		"bot":          signed("sec", now, msg("U1", "100.1", "C1", `,"bot_id":"B1"`)),
		"edit subtype": signed("sec", now, msg("U1", "100.1", "C1", `,"subtype":"message_changed"`)),
	}
	for name, r := range cases {
		before := fc.n
		code := do(r)
		if fc.n != before {
			t.Errorf("%s: comment written", name)
		}
		if name == "bad sig" || name == "stale" {
			if code != 401 {
				t.Errorf("%s: code %d", name, code)
			}
		}
	}
	// Retries are acknowledged but not processed again.
	r := signed("sec", now, msg("U1", "100.1", "C1", ""))
	r.Header.Set("X-Slack-Retry-Num", "1")
	before := fc.n
	if c := do(r); c != 200 || fc.n != before {
		t.Errorf("retry: code=%d", c)
	}
	// URL verification handshake.
	w := httptest.NewRecorder()
	h.ServeHTTP(w, signed("sec", now, `{"type":"url_verification","challenge":"abc"}`))
	if w.Body.String() != "abc" {
		t.Errorf("challenge = %q", w.Body.String())
	}
}
