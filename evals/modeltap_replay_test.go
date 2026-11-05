package evals

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
)

// anthropicReplayHash is the Anthropic tap's hash of the recorded request in
// testdata/modeltap/anthropic_main_turn.json (the body of the V1a-0 golden
// internal/agent/testdata/golden/loop_main_turn.json), computed by the tap as
// it was before V11 (at 7d0e5c5e). V11 adds a second, in-process tap and
// leaves this one alone: the value must never change.
const anthropicReplayHash = "sha256:6c07e0db2b8a7a08"

// TestAnthropicTapHashUnchangedOnReplay replays a recorded Messages request
// through the HTTP tap: the upstream receives it byte for byte, and the tap
// attributes it to the room and hashes it to the pinned value.
func TestAnthropicTapHashUnchangedOnReplay(t *testing.T) {
	body, err := os.ReadFile("testdata/modeltap/anthropic_main_turn.json")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var got []byte
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = b
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer up.Close()
	tap, err := NewModelTap(up.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tap.Close() }()
	resp, err := http.Post(tap.URL()+"/v1/messages?beta=true", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(got, body) {
		t.Fatal("the tap changed the request body")
	}
	if h := tap.PromptHashes(); len(h) != 1 || h[0] != anthropicReplayHash {
		t.Fatalf("tap hashes %v, want [%s]", h, anthropicReplayHash)
	}
	r := tap.Room("r_golden")
	if r.Requests != 1 || len(r.Models) != 1 || r.Models[0] != "claude-opus-5-5" || r.PromptHashes[0] != anthropicReplayHash {
		t.Fatalf("room = %+v", r)
	}
	if k := PromptHashKind(tap.PromptHashes()); k != PromptKindTap {
		t.Fatalf("kind %q", k)
	}
}
