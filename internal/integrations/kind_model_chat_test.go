package integrations

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"piperoom/internal/integrations/spec"
	"piperoom/internal/slackbridge"
)

func anthropicServer(t *testing.T, status int) (*httptest.Server, *string) {
	t.Helper()
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-Api-Key")
		if r.URL.Path != "/v1/messages" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"authentication_error","message":"bad key ` + canary + `"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5-5",` +
			`"content":[{"type":"text","text":"p"}],"stop_reason":"max_tokens","stop_sequence":null,` +
			`"usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &gotKey
}

func TestAnthropicChecks(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   spec.CheckStatus
	}{{200, spec.CheckPass}, {401, spec.CheckFail}, {403, spec.CheckFail}, {404, spec.CheckFail}, {429, spec.CheckWarn}, {500, spec.CheckFail}} {
		srv, gotKey := anthropicServer(t, tc.status)
		d := fastDeps().withDefaults()
		d.anthropicBase = srv.URL
		checks := d.anthropicKind().Test(context.Background(), vals(map[string]string{}, map[string]string{"api_key": canary}))
		c := wantStatus(t, checks, "model", tc.want)
		if tc.status == 200 && c.Detail != "answered by claude-opus-5-5" {
			t.Fatalf("detail = %q", c.Detail)
		}
		if *gotKey != canary {
			t.Fatal("the key was not sent")
		}
		wantStatus(t, checks, "live_swap", spec.CheckWarn)
		noCanary(t, checks)
	}
}

func TestAnthropicDownAndNoKey(t *testing.T) {
	d := fastDeps().withDefaults()
	d.anthropicBase = "http://127.0.0.1:" + closedPort(t)
	checks := d.anthropicKind().Test(context.Background(), vals(map[string]string{}, map[string]string{"api_key": canary}))
	wantStatus(t, checks, "model", spec.CheckFail)
	checks = d.anthropicKind().Test(context.Background(), vals(map[string]string{}, nil))
	wantStatus(t, checks, "input", spec.CheckFail)
	if _, _, err := d.anthropicKind().Build(context.Background(), vals(nil, map[string]string{"api_key": canary})); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("build err = %v", err)
	}
}

func slackServer(t *testing.T, authOK bool, channel string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+canary {
			_, _ = w.Write([]byte(`{"ok":false,"error":"not_authed"}`))
			return
		}
		switch r.URL.Path {
		case "/auth.test":
			if !authOK {
				_, _ = w.Write([]byte(`{"ok":false,"error":"invalid_auth"}`))
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"user":"piperoom","team":"Acme"}`))
		case "/conversations.info":
			_ = r.ParseForm()
			if r.Form.Get("channel") != "C0123ABC" {
				_, _ = w.Write([]byte(`{"ok":false,"error":"channel_not_found"}`))
				return
			}
			_, _ = w.Write([]byte(channel))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestSlackChecks(t *testing.T) {
	member := `{"ok":true,"channel":{"name":"incidents","is_member":true,"is_private":true}}`
	tests := []struct {
		name    string
		authOK  bool
		channel string
		chID    string
		want    map[string]spec.CheckStatus
	}{
		{"pass", true, member, "C0123ABC", map[string]spec.CheckStatus{"auth": spec.CheckPass, "channel": spec.CheckPass}},
		{"bad token", false, member, "C0123ABC", map[string]spec.CheckStatus{"auth": spec.CheckFail, "channel": spec.CheckSkipped}},
		{"not member", true, `{"ok":true,"channel":{"name":"incidents","is_member":false,"is_private":true}}`, "C0123ABC", map[string]spec.CheckStatus{"channel": spec.CheckFail}},
		{"public", true, `{"ok":true,"channel":{"name":"incidents","is_member":true,"is_private":false}}`, "C0123ABC", map[string]spec.CheckStatus{"channel": spec.CheckWarn}},
		{"not found", true, member, "C9999999", map[string]spec.CheckStatus{"channel": spec.CheckFail}},
		{"missing scope", true, `{"ok":false,"error":"missing_scope"}`, "C0123ABC", map[string]spec.CheckStatus{"channel": spec.CheckFail}},
		{"bad json", true, `not json`, "C0123ABC", map[string]spec.CheckStatus{"channel": spec.CheckFail}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := fastDeps()
			d.slackBase = slackServer(t, tc.authOK, tc.channel).URL
			d = d.withDefaults()
			checks := d.slackKind().Test(context.Background(), vals(map[string]string{"channel_id": tc.chID}, map[string]string{"bot_token": canary}))
			for id, st := range tc.want {
				c := wantStatus(t, checks, id, st)
				if st == spec.CheckFail && c.Fix == "" && tc.name != "bad json" {
					t.Fatalf("check %s failed without a fix", id)
				}
			}
			noCanary(t, checks)
		})
	}
}

func TestSlackDownAndBuild(t *testing.T) {
	d := fastDeps()
	d.slackBase = "http://127.0.0.1:" + closedPort(t)
	d = d.withDefaults()
	v := vals(map[string]string{"channel_id": "C0123ABC"}, map[string]string{"bot_token": canary})
	wantStatus(t, d.slackKind().Test(context.Background(), v), "auth", spec.CheckFail)

	client, closeFn, err := d.slackKind().Build(context.Background(), v)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	o, ok := client.(slackbridge.Options)
	if !ok || o.ChannelID != "C0123ABC" || o.Token != canary || o.HTTPClient == nil {
		t.Fatalf("options = %+v", o)
	}
	if strings.Contains(sprintAll(o), canary) {
		t.Fatal("options print the token")
	}
}

func sprintAll(v any) string {
	return strings.Join([]string{fmt.Sprintf("%v", v), fmt.Sprintf("%+v", v), fmt.Sprintf("%#v", v)}, "|")
}

func TestAnthropicTimeout(t *testing.T) {
	// The handler reads the body first: only then does the server notice
	// the client hanging up and cancel r.Context().
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	}))
	defer srv.Close()
	d := fastDeps().withDefaults()
	d.anthropicBase = srv.URL
	checks := d.anthropicKind().Test(context.Background(), vals(nil, map[string]string{"api_key": canary}))
	if c := wantStatus(t, checks, "model", spec.CheckFail); !strings.Contains(c.Detail, "timed out") {
		t.Fatalf("detail = %q", c.Detail)
	}
}
