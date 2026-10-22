package slackbridge

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"piperoom/internal/events"
)

const hostile = "<@U0BADBAD1> <!channel> <https://evil.example|click> & `x`"

func hostilePage() TeamPage {
	return TeamPage{
		RoomID: "r_1", PipelineID: "orders", Channel: "C0123ABCD", Team: "checkout",
		By:    events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun <!here>"},
		Scope: events.ScopeUpstreamApp, Hypothesis: "deploy dropped a column " + hostile, Confidence: "high",
		Steps: []int{1, 3}, ChangeID: 812,
		Change: &TeamPageChange{Kind: "deploy", Service: "checkout`api", Summary: "release " + hostile,
			URL: "https://ci.example/run/7?a=1&b=2", At: time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)},
	}
}

// links returns the texts Slack parses as markup (verbatim false).
func links(r map[string]any) []string {
	var out []string
	for _, tx := range allTexts(r) {
		if tx["verbatim"] != true {
			out = append(out, tx["text"].(string))
		}
	}
	return out
}

// TC3: one post to the team's channel; untrusted text is escaped; the only
// links are the room's and the change's.
func TestPageTeamRendering(t *testing.T) {
	fs := &fakeSlack{}
	var logs bytes.Buffer
	b, _, _ := newBridge(t, &fakeStore{}, fs, func(o *Options) { o.Logger = slog.New(slog.NewTextHandler(&logs, nil)) })
	if err := b.PageTeam(context.Background(), hostilePage()); err != nil {
		t.Fatal(err)
	}
	if fs.count() != 1 {
		t.Fatalf("posts = %d, want exactly 1", fs.count())
	}
	req := fs.reqs[0]
	if str(req, "channel") != "C0123ABCD" || req["thread_ts"] != nil || str(req, "parse") != "none" || req["unfurl_links"] != false {
		t.Fatalf("post fields = %v", req)
	}
	all := joinTexts(req) + str(req, "text")
	for _, bad := range []string{"<@U0BADBAD1", "<!channel", "<!here", "<https://evil", "checkout`api"} {
		if strings.Contains(all, bad) {
			t.Errorf("unescaped %q in %q", bad, all)
		}
	}
	for _, want := range []string{"&lt;@U0BADBAD1&gt;", "&lt;!channel&gt;", "*Evidence:* steps 1, 3", "`deploy` of `checkout'api` at 2026-10-22T09:00:00Z",
		"team `checkout`", "an application change outside the pipeline", "not read by the room"} {
		if !strings.Contains(all, want) {
			t.Errorf("missing %q in %q", want, all)
		}
	}
	got := links(req)
	want := []string{"<https://pr.example/rooms/r_1|Open room>", "<https://ci.example/run/7?a=1&amp;b=2|Open the change>"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("links = %q, want %q", got, want)
	}
	for _, tx := range allTexts(req) {
		if s := tx["text"].(string); !utf8.ValidString(s) || s == "" || len(s) > 3000 {
			t.Errorf("bad text object %q", s)
		}
	}
	if strings.Contains(logs.String(), "dropped") || strings.Contains(logs.String(), testToken) {
		t.Fatalf("page text or token logged: %s", logs.String())
	}
}

// A change URL that is not a plain absolute http(s) URL is never linked.
func TestPageTeamURLRules(t *testing.T) {
	for _, u := range []string{"", "javascript:alert(1)", "ftp://x.example/a", "https://user:pw@x.example/", "https://x.example/a|b",
		"https://x.example/a>b", "https://x.example/a b", "https://x.example/é", "/relative", "https://" + strings.Repeat("a", 1100)} {
		if got, ok := pageURL(u); ok {
			t.Errorf("pageURL(%q) = %q, want refused", u, got)
		}
		p := hostilePage()
		p.Change.URL = u
		m := newRenderer("https://pr.example").teamPage(p)
		var n int
		for _, b := range m.Blocks {
			for _, e := range b.Elements {
				if !e.Verbatim {
					n++
				}
			}
		}
		if n != 1 {
			t.Errorf("url %q: %d link objects, want only the room link", u, n)
		}
	}
	if got, ok := pageURL("http://x.example/p?q=1&r=2"); !ok || got != "http://x.example/p?q=1&amp;r=2" {
		t.Fatalf("plain url = %q %v", got, ok)
	}
}

// Without a PublicURL, a cited change the log lost, and many steps.
func TestPageTeamVariants(t *testing.T) {
	p := hostilePage()
	p.Change, p.Scope = nil, events.ScopeVendor
	p.Steps = make([]int, 25)
	for i := range p.Steps {
		p.Steps[i] = i + 1
	}
	m := newRenderer("").teamPage(p)
	var text string
	for _, b := range m.Blocks {
		if b.Text != nil {
			text += b.Text.Text + "\n"
		}
		for _, e := range b.Elements {
			if !e.Verbatim {
				t.Errorf("link without a public url: %q", e.Text)
			}
		}
	}
	for _, want := range []string{"#812 (no longer in the change log)", "a vendor incident", "20, +5 more"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in %q", want, text)
		}
	}
}

// E6: a bad channel never reaches Slack; Slack's refusals come back with
// their code and are not retried; 5xx is retried by the client.
func TestPageTeamErrors(t *testing.T) {
	fs := &fakeSlack{}
	b, _, _ := newBridge(t, &fakeStore{}, fs, nil)
	for _, ch := range []string{"", "#checkout", "C01 23", "U0123ABCD", "C0123ABCD|x"} {
		p := hostilePage()
		p.Channel = ch
		if err := b.PageTeam(context.Background(), p); !errors.Is(err, ErrBadTeamChannel) || ErrorCode(err) != "bad_channel" {
			t.Errorf("channel %q: err = %v", ch, err)
		}
	}
	if fs.count() != 0 {
		t.Fatalf("a bad channel was posted to (%d)", fs.count())
	}

	cases := []struct {
		name  string
		reply func(n int, w http.ResponseWriter)
		code  string
		posts int
	}{
		{"not in channel", func(_ int, w http.ResponseWriter) { _, _ = io.WriteString(w, `{"ok":false,"error":"not_in_channel"}`) }, "not_in_channel", 1},
		{"forbidden", func(_ int, w http.ResponseWriter) { w.WriteHeader(http.StatusForbidden) }, "http_403", 1},
		{"5xx then ok", func(n int, w http.ResponseWriter) {
			if n == 1 {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			_, _ = io.WriteString(w, `{"ok":true,"channel":"C0123ABCD","ts":"1.2"}`)
		}, "", 2},
	}
	for _, c := range cases {
		fs := &fakeSlack{reply: c.reply}
		b, _, _ := newBridge(t, &fakeStore{}, fs, nil)
		err := b.PageTeam(context.Background(), hostilePage())
		if c.code == "" && err != nil || c.code != "" && ErrorCode(err) != c.code {
			t.Errorf("%s: err = %v (code %q), want %q", c.name, err, ErrorCode(err), c.code)
		}
		if fs.count() != c.posts {
			t.Errorf("%s: posts = %d, want %d", c.name, fs.count(), c.posts)
		}
	}

	// A cancelled request does not post.
	fs = &fakeSlack{}
	b, _, _ = newBridge(t, &fakeStore{}, fs, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.PageTeam(ctx, hostilePage()); err == nil || ErrorCode(err) != "canceled" {
		t.Fatalf("cancelled: %v (%s)", err, ErrorCode(err))
	}
}
