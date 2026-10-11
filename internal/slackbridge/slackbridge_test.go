package slackbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"piperoom/internal/events"
)

const testToken = "xoxb-test-SECRET-123"

// ---- fakes ----

type fakeSlack struct {
	mu      sync.Mutex
	reqs    []map[string]any
	headers []http.Header
	// reply picks the response for request n (1-based).
	reply func(n int, w http.ResponseWriter)
}

func (f *fakeSlack) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	f.mu.Lock()
	f.reqs = append(f.reqs, m)
	f.headers = append(f.headers, r.Header.Clone())
	n := len(f.reqs)
	f.mu.Unlock()
	if f.reply != nil {
		f.reply(n, w)
		return
	}
	ts := "1700000000.000" + strconv.Itoa(100+n)
	_, _ = io.WriteString(w, `{"ok":true,"channel":"CREAL","ts":"`+ts+`"}`)
}

type fakeStore struct {
	evs      []events.Event
	subAfter int64
	subbed   atomic.Bool
}

func (s *fakeStore) Append(context.Context, string, events.Type, events.Actor, any) (events.Event, error) {
	return events.Event{}, errors.New("unused")
}
func (s *fakeStore) Since(context.Context, string, int64, int) ([]events.Event, error) {
	return nil, nil
}
func (s *fakeStore) Subscribe(ctx context.Context, _ string, after int64) (<-chan events.Event, error) {
	s.subAfter = after
	s.subbed.Store(true)
	ch := make(chan events.Event, len(s.evs)+1)
	for _, e := range s.evs {
		if e.Seq > after {
			ch <- e
		}
	}
	go func() { <-ctx.Done(); close(ch) }()
	return ch, nil
}

type clock struct {
	mu    sync.Mutex
	t     time.Time
	waits []time.Duration
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.waits = append(c.waits, d)
	c.t = c.t.Add(d)
	c.mu.Unlock()
	return nil
}

func ev(seq int64, typ events.Type, actor string, payload any) events.Event {
	raw, _ := json.Marshal(payload)
	return events.Event{RoomID: "r1", Seq: seq, Type: typ, Actor: events.Actor{Kind: events.ActorHuman, ID: actor, Name: actor}, Payload: raw}
}

func newBridge(t *testing.T, st events.Store, fs *fakeSlack, mod func(*Options)) (*Bridge, *clock, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(fs)
	t.Cleanup(srv.Close)
	o := Options{Token: testToken, ChannelID: "CCONF", PublicURL: "https://pr.example/", APIBaseURL: srv.URL}
	if mod != nil {
		mod(&o)
	}
	b, err := New(st, o)
	if err != nil {
		t.Fatal(err)
	}
	ck := &clock{t: time.Unix(1700000000, 0)}
	b.client.now, b.client.sleep = ck.now, ck.sleep
	return b, ck, srv
}

var allTypes = []events.Type{
	events.RoomCreated, events.RoomClosed, events.AgentStatus, events.AgentStepStart, events.AgentToolCall,
	events.AgentToolResult, events.AgentMessage, events.AgentHypothesis, events.CommentAdded, events.SteerSuggested,
	events.SteerSent, events.ControlRequested, events.ControlGranted, events.ProposalCreated, events.ApprovalGiven,
	events.ApprovalRejected, events.ActionExecuted, events.HandoffCreated, events.HandoffAccepted, events.TracePinned,
}

func payloadFor(t events.Type) any {
	switch t {
	case events.RoomCreated:
		return events.RoomCreatedPayload{Title: "orders lag", Severity: "sev2", PipelineID: "orders", Alert: json.RawMessage(`{"secret":"ALERTBODY"}`)}
	case events.AgentHypothesis:
		return events.AgentHypothesisPayload{Text: "schema change", Confidence: "high"}
	case events.ProposalCreated:
		return events.ProposalCreatedPayload{ProposalID: "p1", Title: "Fix it", RequiredLayers: []string{"kafka"}, Risk: "low", Rollback: "undo",
			Steps: []events.ProposalStep{{Action: "restart", Layer: "kafka", Params: json.RawMessage(`{"a": 1}`), Description: "d"}}}
	case events.ApprovalGiven:
		return events.ApprovalGivenPayload{ProposalID: "p1", Layer: "kafka"}
	case events.ApprovalRejected:
		return events.ApprovalRejectedPayload{ProposalID: "p1", Layer: "kafka", Reason: "no"}
	case events.ActionExecuted:
		return events.ActionExecutedPayload{ProposalID: "p1", StepIndex: 0, OK: true, Output: "done"}
	case events.HandoffCreated:
		return events.HandoffCreatedPayload{To: events.Actor{Name: "Bo"}, Summary: events.HandoffSummary{WhatBroke: "x", Why: "y"}}
	case events.RoomClosed:
		return events.RoomClosedPayload{Resolution: "fixed"}
	}
	return map[string]string{"secret": "TOOLDATA"}
}

func allEvents() []events.Event {
	var out []events.Event
	for i, t := range allTypes {
		out = append(out, ev(int64(i+1), t, "ana", payloadFor(t)))
	}
	return out
}

func str(m map[string]any, k string) string { s, _ := m[k].(string); return s }

// ---- tests ----

func TestParentThenThread(t *testing.T) {
	evs := allEvents()
	// room.closed must be last in seq order for this test.
	evs[0].Seq, evs[1].Seq = 1, 20
	for i := 2; i < 20; i++ {
		evs[i].Seq = int64(i)
	}
	evs = append(evs[:1], append(evs[2:], evs[1])...)
	fs := &fakeSlack{}
	th := NewMemThreads()
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, func(o *Options) { o.Threads = th })
	if err := b.Run(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	if fs.count() != 9 {
		t.Fatalf("requests = %d", fs.count())
	}
	if _, has := fs.reqs[0]["thread_ts"]; has || str(fs.reqs[0], "channel") != "CCONF" {
		t.Errorf("parent wrong: %v", fs.reqs[0])
	}
	for i, r := range fs.reqs[1:] {
		if str(r, "thread_ts") != "1700000000.000101" || str(r, "channel") != "CREAL" {
			t.Errorf("reply %d: thread=%v channel=%v", i, r["thread_ts"], r["channel"])
		}
	}
	last := fs.reqs[8]
	if last["reply_broadcast"] != true || fs.reqs[1]["reply_broadcast"] != false {
		t.Errorf("broadcast flags wrong")
	}
	got, ok, _ := th.Get(context.Background(), "r1")
	if !ok || got.TS != "1700000000.000101" || got.LastSeq != 20 || got.Channel != "CREAL" {
		t.Errorf("thread = %+v", got)
	}
	for _, r := range fs.reqs {
		raw, _ := json.Marshal(r)
		if bytes.Contains(raw, []byte("ALERTBODY")) || bytes.Contains(raw, []byte("TOOLDATA")) {
			t.Error("alert or tool data leaked")
		}
		if r["unfurl_links"] != false || r["unfurl_media"] != false || r["parse"] != "none" {
			t.Errorf("flags: %v", r)
		}
	}
}

func TestImportantTable(t *testing.T) {
	want := map[events.Type]bool{
		events.RoomCreated: true, events.RoomClosed: true, events.AgentHypothesis: true, events.ProposalCreated: true,
		events.ApprovalGiven: true, events.ApprovalRejected: true, events.ActionExecuted: true,
		events.HandoffCreated: true, events.HandoffAccepted: true,
	}
	if len(allTypes) != 20 {
		t.Fatal("type list out of date")
	}
	for _, ty := range allTypes {
		if Important(ty) != want[ty] {
			t.Errorf("%s: got %v", ty, Important(ty))
		}
	}
}

func allTexts(r map[string]any) []map[string]any {
	var out []map[string]any
	blocks, _ := r["blocks"].([]any)
	for _, b := range blocks {
		bm := b.(map[string]any)
		if tx, ok := bm["text"].(map[string]any); ok {
			out = append(out, tx)
		}
		els, _ := bm["elements"].([]any)
		for _, e := range els {
			out = append(out, e.(map[string]any))
		}
	}
	return out
}

func TestEscaping(t *testing.T) {
	title := "<!channel> & <https://evil.example|click> \x00bad\xff"
	evs := []events.Event{
		ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: title, Severity: "sev1", PipelineID: "p`x"}),
		ev(2, events.ActionExecuted, "ana", events.ActionExecutedPayload{ProposalID: "p", OK: false, Output: "a```b <!here>"}),
		ev(3, events.RoomClosed, "ana", events.RoomClosedPayload{Resolution: "x"}),
	}
	fs := &fakeSlack{}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, nil)
	if err := b.Run(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	first := allTexts(fs.reqs[0])[0]["text"].(string)
	if !strings.Contains(first, "&lt;!channel&gt; &amp; &lt;https://evil.example|click&gt; bad") || strings.Contains(first, "<!") {
		t.Errorf("title = %q", first)
	}
	for _, r := range fs.reqs {
		for _, tx := range allTexts(r) {
			text := tx["text"].(string)
			if !strings.Contains(text, "|Open room>") && tx["verbatim"] != true {
				t.Errorf("untrusted text without verbatim: %q", text)
			}
			if !utf8.ValidString(text) || len(text) == 0 || len(text) > 3000 {
				t.Errorf("bad text %q", text)
			}
		}
	}
	code := allTexts(fs.reqs[1])[0]["text"].(string)
	if strings.Count(code, "```") != 2 {
		t.Errorf("code block can be closed early: %q", code)
	}
	link := allTexts(fs.reqs[0])
	if l := link[len(link)-1]["text"].(string); l != "<https://pr.example/rooms/r1|Open room>" {
		t.Errorf("link = %q", l)
	}
}

func TestCaps(t *testing.T) {
	var steps []events.ProposalStep
	big := `{"k":"` + strings.Repeat("&", 10<<10) + `"}`
	for i := 0; i < 30; i++ {
		steps = append(steps, events.ProposalStep{Action: "a", Layer: "kafka", Params: json.RawMessage(big), Description: strings.Repeat("é", 2000)})
	}
	evs := []events.Event{
		ev(1, events.ProposalCreated, "ana", events.ProposalCreatedPayload{ProposalID: "p", Title: "t", Steps: steps, Risk: strings.Repeat("<", 9999)}),
		ev(2, events.RoomClosed, "ana", events.RoomClosedPayload{}),
	}
	fs := &fakeSlack{}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, nil)
	if err := b.Run(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	r := fs.reqs[1] // [0] is the minimal parent
	if blocks := r["blocks"].([]any); len(blocks) > 50 {
		t.Errorf("blocks = %d", len(blocks))
	}
	var sawMore bool
	for _, tx := range allTexts(r) {
		s := tx["text"].(string)
		if len(s) > 3000 {
			t.Errorf("text %d bytes", len(s))
		}
		sawMore = sawMore || strings.Contains(s, "+20 more steps")
	}
	if !sawMore {
		t.Error("no +20 more steps")
	}
	if len(str(r, "text")) > 300 {
		t.Errorf("fallback %d bytes", len(str(r, "text")))
	}
}

func TestClip(t *testing.T) {
	for _, in := range []string{strings.Repeat("&", 100), strings.Repeat("日本語", 50), strings.Repeat("<a>", 40), "short"} {
		for _, n := range []int{4, 5, 10, 33, 100} {
			out := clip(in, n)
			if out != "(empty)" && len(out) > n || !utf8.ValidString(out) {
				t.Errorf("clip(%q,%d) = %q", in, n, out)
			}
			if strings.Contains(out, "&") && strings.Contains(strings.TrimSuffix(out, ellipsis), "&") {
				for _, seg := range strings.Split(out, "&")[1:] {
					if !strings.HasPrefix(seg, "amp;") && !strings.HasPrefix(seg, "lt;") && !strings.HasPrefix(seg, "gt;") {
						t.Errorf("split entity in %q", out)
					}
				}
			}
		}
	}
	if clip("\x00\x01  ", 10) != "(empty)" {
		t.Error("empty not replaced")
	}
}

func clientFor(t *testing.T, fs *fakeSlack) (*client, *clock) {
	srv := httptest.NewServer(fs)
	t.Cleanup(srv.Close)
	c := newClient(&http.Client{Timeout: time.Second}, srv.URL, testToken)
	ck := &clock{t: time.Unix(1700000000, 0)}
	c.now, c.sleep = ck.now, ck.sleep
	return c, ck
}

func TestRetries(t *testing.T) {
	ok := func(w http.ResponseWriter) { _, _ = io.WriteString(w, `{"ok":true,"channel":"C","ts":"1.2"}`) }
	t.Run("retry-after", func(t *testing.T) {
		fs := &fakeSlack{reply: func(n int, w http.ResponseWriter) {
			if n == 1 {
				w.Header().Set("Retry-After", "3")
				w.WriteHeader(429)
				return
			}
			ok(w)
		}}
		c, ck := clientFor(t, fs)
		if _, err := c.post(context.Background(), message{}); err != nil {
			t.Fatal(err)
		}
		if fs.count() != 2 || len(ck.waits) != 1 || ck.waits[0] != 3*time.Second {
			t.Errorf("reqs=%d waits=%v", fs.count(), ck.waits)
		}
	})
	t.Run("no header", func(t *testing.T) {
		fs := &fakeSlack{reply: func(n int, w http.ResponseWriter) {
			if n == 1 {
				w.WriteHeader(429)
				return
			}
			ok(w)
		}}
		c, ck := clientFor(t, fs)
		if _, err := c.post(context.Background(), message{}); err != nil || len(ck.waits) != 1 || ck.waits[0] != time.Second {
			t.Errorf("err=%v waits=%v", err, ck.waits)
		}
	})
	t.Run("too long", func(t *testing.T) {
		fs := &fakeSlack{reply: func(n int, w http.ResponseWriter) { w.Header().Set("Retry-After", "600"); w.WriteHeader(429) }}
		c, ck := clientFor(t, fs)
		_, err := c.post(context.Background(), message{})
		if !errors.Is(err, ErrRateLimited) || fs.count() != 1 || len(ck.waits) != 0 {
			t.Errorf("err=%v reqs=%d waits=%v", err, fs.count(), ck.waits)
		}
	})
	t.Run("5xx", func(t *testing.T) {
		fs := &fakeSlack{reply: func(n int, w http.ResponseWriter) { w.WriteHeader(503) }}
		c, ck := clientFor(t, fs)
		if _, err := c.post(context.Background(), message{}); err == nil || fs.count() != 5 {
			t.Fatalf("err=%v reqs=%d", err, fs.count())
		}
		want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
		if len(ck.waits) != 4 {
			t.Fatalf("waits = %v", ck.waits)
		}
		for i := range want {
			if ck.waits[i] != want[i] {
				t.Errorf("waits = %v", ck.waits)
			}
		}
	})
	t.Run("cancel during wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		fs := &fakeSlack{reply: func(n int, w http.ResponseWriter) { cancel(); w.WriteHeader(503) }}
		c, _ := clientFor(t, fs)
		if _, err := c.post(ctx, message{}); !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("no retry after transport error", func(t *testing.T) {
		fs := &fakeSlack{reply: func(n int, w http.ResponseWriter) {
			hj, _ := w.(http.Hijacker)
			conn, _, _ := hj.Hijack()
			conn.Close()
		}}
		c, _ := clientFor(t, fs)
		if _, err := c.post(context.Background(), message{}); err == nil || fs.count() != 1 {
			t.Errorf("err=%v reqs=%d", err, fs.count())
		}
	})
	t.Run("ok without ts", func(t *testing.T) {
		fs := &fakeSlack{reply: func(n int, w http.ResponseWriter) { _, _ = io.WriteString(w, `{"ok":true}`) }}
		c, _ := clientFor(t, fs)
		if _, err := c.post(context.Background(), message{}); err == nil {
			t.Error("want error")
		}
	})
}

func TestPacing(t *testing.T) {
	fs := &fakeSlack{}
	c, ck := clientFor(t, fs)
	for i := 0; i < 2; i++ {
		if _, err := c.post(context.Background(), message{}); err != nil {
			t.Fatal(err)
		}
	}
	if len(ck.waits) != 1 || ck.waits[0] != time.Second {
		t.Errorf("waits = %v", ck.waits)
	}
}

func TestTokenHygieneAndFatal(t *testing.T) {
	fs := &fakeSlack{reply: func(n int, w http.ResponseWriter) { _, _ = io.WriteString(w, `{"ok":false,"error":"invalid_auth"}`) }}
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	evs := []events.Event{ev(1, events.RoomCreated, "ana", payloadFor(events.RoomCreated))}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, func(o *Options) { o.Logger = log })
	err := b.Run(context.Background(), "r1")
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != "invalid_auth" {
		t.Fatalf("err = %v", err)
	}
	if got := fs.headers[0].Get("Authorization"); got != "Bearer "+testToken {
		t.Errorf("auth header = %q", got)
	}
	if _, has := fs.reqs[0]["token"]; has {
		t.Error("token in body")
	}
	if strings.Contains(err.Error(), testToken) || strings.Contains(logs.String(), testToken) {
		t.Error("token leaked")
	}
	if !strings.Contains(logs.String(), "level=ERROR") || strings.Contains(logs.String(), "orders lag") {
		t.Errorf("log = %s", logs.String())
	}
}

func TestNonFatalErrorAdvances(t *testing.T) {
	fs := &fakeSlack{reply: func(n int, w http.ResponseWriter) {
		if n == 2 {
			_, _ = io.WriteString(w, `{"ok":false,"error":"msg_too_long"}`)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true,"channel":"C","ts":"1.1"}`)
	}}
	th := NewMemThreads()
	evs := []events.Event{
		ev(1, events.RoomCreated, "ana", payloadFor(events.RoomCreated)),
		ev(2, events.AgentHypothesis, "ana", payloadFor(events.AgentHypothesis)),
		ev(3, events.RoomClosed, "ana", payloadFor(events.RoomClosed)),
	}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, func(o *Options) { o.Threads = th })
	if err := b.Run(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := th.Get(context.Background(), "r1"); got.LastSeq != 3 || fs.count() != 3 {
		t.Errorf("thread=%+v reqs=%d", got, fs.count())
	}
}

func TestResume(t *testing.T) {
	th := NewMemThreads()
	_ = th.Put(context.Background(), "r1", Thread{Channel: "COLD", TS: "9.9", LastSeq: 5})
	st := &fakeStore{evs: []events.Event{
		ev(1, events.RoomCreated, "ana", payloadFor(events.RoomCreated)),
		ev(6, events.RoomCreated, "ana", payloadFor(events.RoomCreated)),
		ev(7, events.ApprovalGiven, "ana", payloadFor(events.ApprovalGiven)),
		ev(8, events.RoomClosed, "ana", payloadFor(events.RoomClosed)),
	}}
	fs := &fakeSlack{}
	b, _, _ := newBridge(t, st, fs, func(o *Options) { o.Threads = th })
	if err := b.Run(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	if st.subAfter != 5 || fs.count() != 2 {
		t.Fatalf("after=%d reqs=%d", st.subAfter, fs.count())
	}
	for _, r := range fs.reqs {
		if str(r, "thread_ts") != "9.9" || str(r, "channel") != "COLD" {
			t.Errorf("req = %v", r)
		}
	}
}

func TestHypothesisDedup(t *testing.T) {
	evs := []events.Event{
		ev(1, events.RoomCreated, "ana", payloadFor(events.RoomCreated)),
		ev(2, events.AgentHypothesis, "ana", payloadFor(events.AgentHypothesis)),
		ev(3, events.AgentHypothesis, "ana", payloadFor(events.AgentHypothesis)),
		ev(4, events.RoomClosed, "ana", payloadFor(events.RoomClosed)),
	}
	fs := &fakeSlack{}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, nil)
	_ = b.Run(context.Background(), "r1")
	if fs.count() != 3 {
		t.Errorf("reqs = %d", fs.count())
	}
}

func TestNewChecks(t *testing.T) {
	st := &fakeStore{}
	if _, err := New(st, Options{}); !errors.Is(err, ErrDisabled) {
		t.Errorf("empty: %v", err)
	}
	bad := []Options{
		{Token: "t"}, {ChannelID: "C1"},
		{Token: "t", ChannelID: "#general"}, {Token: "t", ChannelID: "C 1"},
		{Token: "t", ChannelID: "C1", PublicURL: "ftp://x"}, {Token: "t", ChannelID: "C1", PublicURL: "/rooms"},
		{Token: "t", ChannelID: "C1", PublicURL: "https://x/a|b"},
	}
	for _, o := range bad {
		if _, err := New(st, o); err == nil || errors.Is(err, ErrDisabled) {
			t.Errorf("%+v: err = %v", o, err)
		}
	}
	if _, err := New(st, Options{Token: "t", ChannelID: "C1", PublicURL: "https://x"}); err != nil {
		t.Errorf("valid: %v", err)
	}
	// Second Run on the same room is refused.
	b, _, _ := newBridge(t, st, &fakeSlack{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx, "r1") }()
	for i := 0; i < 200; i++ {
		b.mu.Lock()
		r := b.running["r1"]
		b.mu.Unlock()
		if r && st.subbed.Load() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := b.Run(ctx, "r1"); !errors.Is(err, ErrAlreadyRunning) {
		t.Errorf("second run: %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("first run: %v", err)
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	var hits int
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer evil.Close()
	fs := &fakeSlack{reply: func(n int, w http.ResponseWriter) {
		w.Header().Set("Location", evil.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}}
	evs := []events.Event{ev(1, events.RoomCreated, "ana", payloadFor(events.RoomCreated)), ev(2, events.RoomClosed, "ana", payloadFor(events.RoomClosed))}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, nil)
	_ = b.Run(context.Background(), "r1")
	if hits != 0 {
		t.Error("redirect followed")
	}
}

func (f *fakeSlack) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.reqs) }

func TestCancelKeepsLastSeq(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fs := &fakeSlack{reply: func(n int, w http.ResponseWriter) {
		if n == 1 {
			_, _ = io.WriteString(w, `{"ok":true,"channel":"C","ts":"1.1"}`)
			return
		}
		cancel() // shutdown while the reply is being retried
		w.WriteHeader(503)
	}}
	th := NewMemThreads()
	evs := []events.Event{
		ev(1, events.RoomCreated, "ana", payloadFor(events.RoomCreated)),
		ev(2, events.AgentHypothesis, "ana", payloadFor(events.AgentHypothesis)),
	}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, func(o *Options) { o.Threads = th })
	if err := b.Run(ctx, "r1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if got, _, _ := th.Get(context.Background(), "r1"); got.LastSeq != 1 {
		t.Errorf("LastSeq = %d, want 1", got.LastSeq)
	}
}

type closingStore struct{ fakeStore }

func (s *closingStore) Subscribe(context.Context, string, int64) (<-chan events.Event, error) {
	ch := make(chan events.Event)
	close(ch)
	return ch, nil
}

func TestUnexpectedCloseIsError(t *testing.T) {
	b, _, _ := newBridge(t, &closingStore{}, &fakeSlack{}, nil)
	if err := b.Run(context.Background(), "r1"); err == nil {
		t.Error("want error when the subscription closes under a live ctx")
	}
}

func TestCleanDropsFormatChars(t *testing.T) {
	if got := clean("a‮b​c"); got != "abc" {
		t.Errorf("clean = %q", got)
	}
}

func TestLayersCapped(t *testing.T) {
	layers := make([]string, 200)
	for i := range layers {
		layers[i] = strings.Repeat("l", 60)
	}
	evs := []events.Event{
		ev(1, events.RoomCreated, "ana", payloadFor(events.RoomCreated)),
		ev(2, events.ProposalCreated, "ana", events.ProposalCreatedPayload{ProposalID: "p", Title: "t", RequiredLayers: layers}),
		ev(3, events.ProposalCreated, "ana", events.ProposalCreatedPayload{ProposalID: "q", Title: "t"}),
		ev(4, events.RoomClosed, "ana", payloadFor(events.RoomClosed)),
	}
	fs := &fakeSlack{}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, nil)
	if err := b.Run(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	var sawMore, sawNone bool
	for _, tx := range allTexts(fs.reqs[1]) {
		s := tx["text"].(string)
		if len(s) > 3000 {
			t.Errorf("text %d bytes", len(s))
		}
		sawMore = sawMore || strings.Contains(s, "+190 more")
	}
	for _, tx := range allTexts(fs.reqs[2]) {
		sawNone = sawNone || strings.Contains(tx["text"].(string), "(none)")
	}
	if !sawMore || !sawNone {
		t.Errorf("more=%v none=%v", sawMore, sawNone)
	}
}

func TestMinimalParentSavedBeforeReply(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fs := &fakeSlack{reply: func(n int, w http.ResponseWriter) {
		if n == 1 { // the minimal parent
			_, _ = io.WriteString(w, `{"ok":true,"channel":"C","ts":"7.7"}`)
			return
		}
		cancel() // shutdown while the reply is in flight
		w.WriteHeader(503)
	}}
	th := NewMemThreads()
	evs := []events.Event{ev(3, events.AgentHypothesis, "ana", payloadFor(events.AgentHypothesis))}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, func(o *Options) { o.Threads = th })
	if err := b.Run(ctx, "r1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	got, ok, _ := th.Get(context.Background(), "r1")
	if !ok || got.TS != "7.7" || got.Channel != "C" || got.LastSeq != 0 {
		t.Errorf("thread = %+v; the parent must be saved and the event left for the restart", got)
	}
}

type failingThreads struct{ MemThreads }

func (f *failingThreads) Put(context.Context, string, Thread) error { return errors.New("db down") }

func TestPutFailureIsLoggedNotFatal(t *testing.T) {
	var logs bytes.Buffer
	evs := []events.Event{
		ev(1, events.RoomCreated, "ana", payloadFor(events.RoomCreated)),
		ev(2, events.RoomClosed, "ana", payloadFor(events.RoomClosed)),
	}
	fs := &fakeSlack{}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, func(o *Options) {
		o.Threads = &failingThreads{MemThreads: *NewMemThreads()}
		o.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	})
	if err := b.Run(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	if fs.count() != 2 || !strings.Contains(logs.String(), "db down") {
		t.Errorf("reqs=%d logs=%s", fs.count(), logs.String())
	}
}

func TestHandoffLists(t *testing.T) {
	open := []string{"a", "b", "c", "d", "e", "f", "g"}
	evs := []events.Event{
		ev(1, events.RoomCreated, "ana", payloadFor(events.RoomCreated)),
		ev(2, events.HandoffCreated, "ana", events.HandoffCreatedPayload{To: events.Actor{ID: "u2"},
			Summary: events.HandoffSummary{WhatBroke: "x", Why: "y", Open: open, Risks: []string{"<r>"}}}),
		ev(3, events.RoomClosed, "ana", payloadFor(events.RoomClosed)),
	}
	fs := &fakeSlack{}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, nil)
	if err := b.Run(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	var all string
	for _, tx := range allTexts(fs.reqs[1]) {
		all += tx["text"].(string) + "\n"
	}
	for _, want := range []string{"to u2", "• e", "+2 more", "• &lt;r&gt;"} {
		if !strings.Contains(all, want) {
			t.Errorf("missing %q in %q", want, all)
		}
	}
	if strings.Contains(all, "• f") {
		t.Error("list not capped at 5")
	}
}

func TestErrCode(t *testing.T) {
	cases := map[string]error{
		"not_in_channel": &APIError{Code: "not_in_channel"},
		"rate_limited":   fmt.Errorf("x: %w", ErrRateLimited),
		"http_503":       fmt.Errorf("x: %w", &httpError{503}),
		"timeout":        fmt.Errorf("x: %w", context.DeadlineExceeded),
		"canceled":       context.Canceled,
		"transport":      errors.New("connection reset"),
	}
	for want, err := range cases {
		if got := errCode(err); got != want {
			t.Errorf("errCode(%v) = %q, want %q", err, got, want)
		}
	}
}
