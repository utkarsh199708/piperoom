package evals

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The tap forwards bytes unchanged both ways, passes headers on, and counts
// only requests whose first user message names a room.
func TestModelTapForwardsAndAttributes(t *testing.T) {
	var gotBody, gotKey string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody, gotKey = string(b), r.Header.Get("x-api-key")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: ping\ndata: {}\n\n")
	}))
	defer up.Close()
	tap, err := NewModelTap(up.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer tap.Close()

	send := func(body string) string {
		req, _ := http.NewRequest(http.MethodPost, tap.URL()+"/v1/messages", strings.NewReader(body))
		req.Header.Set("x-api-key", "k")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}
	room := `{"model":"m1","system":[{"type":"text","text":"sys"}],"tools":[{"name":"a"}],` +
		`"messages":[{"role":"user","content":[{"type":"text","text":"Investigate.\nAlert: x\nRoom: r_1\n"}]}]}`
	if got := send(room); got != "event: ping\ndata: {}\n\n" {
		t.Errorf("response changed: %q", got)
	}
	if gotBody != room || gotKey != "k" {
		t.Errorf("request changed: %q key %q", gotBody, gotKey)
	}
	// A request with another model and the same prompt; and one with no room.
	send(strings.Replace(room, `"m1"`, `"m2"`, 1))
	send(`{"model":"judge","system":"other","messages":[{"role":"user","content":"grade this"}]}`)

	seen := tap.Room("r_1")
	if seen.Requests != 2 || strings.Join(seen.Models, ",") != "m1,m2" || len(seen.PromptHashes) != 1 {
		t.Errorf("room = %+v", seen)
	}
	if len(tap.PromptHashes()) != 1 {
		t.Errorf("unattributed request counted: %v", tap.PromptHashes())
	}
	// The hash is over system and tools, compacted.
	if want := PromptHash(json.RawMessage(`[{"type":"text","text":"sys"}]`), json.RawMessage(`[ {"name": "a"} ]`)); seen.PromptHashes[0] != want {
		t.Errorf("hash %s, want %s", seen.PromptHashes[0], want)
	}
	if PromptHash(json.RawMessage(`"a"`), nil) == PromptHash(json.RawMessage(`"b"`), nil) {
		t.Error("a changed system prompt keeps its hash")
	}
}

// #340: a body over the cap is forwarded whole, with its own length, and is
// not observed; a cut body would break "forwards unchanged".
func TestModelTapForwardsOversizedBodyWhole(t *testing.T) {
	var got []byte
	var gotLen int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		gotLen = r.ContentLength
	}))
	defer up.Close()
	tap, err := newModelTap(up.URL, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer tap.Close()
	body := `{"model":"m1","messages":[{"role":"user","content":"Investigate.\nRoom: r_big\n` + strings.Repeat("x", 100) + `"}]}`
	resp, err := http.Post(tap.URL()+"/v1/messages", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if string(got) != body || gotLen != int64(len(body)) {
		t.Errorf("upstream got %d bytes (Content-Length %d), want %d", len(got), gotLen, len(body))
	}
	if r := tap.Room("r_big"); r.Requests != 0 {
		t.Errorf("oversized body observed: %+v", r)
	}
}
