package httpapi_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func code1(t *testing.T, resp *http.Response) string {
	t.Helper()
	c, _ := errCode(t, resp)
	return c
}

// JSON bodies: over 1 MB is 413 too_large; trailing data after the object,
// invalid UTF-8 and lone surrogate escapes are 400 validation, and nothing is
// written.
func TestDecodeBodyLimitsAndJunk(t *testing.T) {
	srv, svc := newServer(t)
	priya := mustUserFrom(t, svc, "priya")
	snap, err := svc.CreateRoom(context.Background(), priya, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/api/rooms/" + snap.ID + "/comments"
	before := roomEventCount(t, svc, snap.ID)

	tests := []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{"over 1 MB", `{"text":"` + strings.Repeat("x", 1<<20) + `"}`, http.StatusRequestEntityTooLarge, "too_large"},
		{"trailing object", `{"text":"a"}{"text":"b"}`, http.StatusBadRequest, "validation"},
		{"trailing garbage", `{"text":"a"} junk`, http.StatusBadRequest, "validation"},
		{"invalid utf-8", "{\"text\":\"a\xffb\"}", http.StatusBadRequest, "validation"},
		{"lone high surrogate", `{"text":"a\ud800b"}`, http.StatusBadRequest, "validation"},
		{"lone low surrogate", `{"text":"\udc00"}`, http.StatusBadRequest, "validation"},
		{"high then high", `{"text":"\ud83d\ud83d"}`, http.StatusBadRequest, "validation"},
		{"high at end", `{"text":"x\uD83D"}`, http.StatusBadRequest, "validation"},
		{"surrogate in key", `{"te\ud800xt":"a"}`, http.StatusBadRequest, "validation"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := doJSON(t, http.MethodPost, url, "priya", tt.body)
			if resp.StatusCode != tt.status {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				t.Fatalf("status %d, want %d: %s", resp.StatusCode, tt.status, b)
			}
			if c := code1(t, resp); c != tt.code {
				t.Fatalf("code %q, want %q", c, tt.code)
			}
		})
	}
	if n := roomEventCount(t, svc, snap.ID); n != before {
		t.Fatalf("refused bodies wrote %d events", n-before)
	}

	// Still accepted: a valid surrogate pair, an escaped backslash before
	// "ud800" (literal text, not an escape), trailing whitespace.
	for _, body := range []string{`{"text":"smile 😀"}`, `{"text":"path \\ud800"}`, "{\"text\":\"ok\"}\n\t "} {
		resp := doJSON(t, http.MethodPost, url, "priya", body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d", body, resp.StatusCode)
		}
	}
}

// Comment and steer text is capped (maxTypedText): longer is 400 validation.
func TestTypedTextCap(t *testing.T) {
	srv, svc := newServer(t)
	priya := mustUserFrom(t, svc, "priya")
	snap, err := svc.CreateRoom(context.Background(), priya, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	ok := strings.Repeat("é", 16<<10/2) // exactly 16 KiB of UTF-8
	long := ok + "x"
	for _, path := range []string{"/comments", "/steer"} {
		url := srv.URL + "/api/rooms/" + snap.ID + path
		resp := doJSON(t, http.MethodPost, url, "priya", fmt.Sprintf(`{"text":%q}`, long))
		if resp.StatusCode != http.StatusBadRequest || code1(t, resp) != "validation" {
			t.Fatalf("%s long: status %d", path, resp.StatusCode)
		}
		resp = doJSON(t, http.MethodPost, url, "priya", fmt.Sprintf(`{"text":%q}`, ok))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s at cap: status %d", path, resp.StatusCode)
		}
	}
}

// Approve/reject: an unknown proposal is 404 whatever the body; an empty
// layer is 400 validation (not 403).
func TestApproveRejectInputOrder(t *testing.T) {
	srv, svc, _, newProposal := devApprovalEnv(t, "")
	for _, action := range []string{"approve", "reject"} {
		for _, body := range []string{`{}`, `{"layer":"postgres"}`, `{"layer":"","params_hash":"x"}`} {
			resp := doJSON(t, http.MethodPost, srv.URL+"/api/proposals/p_nope/"+action, "priya", body)
			if resp.StatusCode != http.StatusNotFound || code1(t, resp) != "not_found" {
				t.Fatalf("%s unknown %s: status %d", action, body, resp.StatusCode)
			}
		}
		pid := newProposal("postgres")
		body := fmt.Sprintf(`{"layer":"","params_hash":%q}`, hashOfOpen(t, svc, pid))
		resp := doJSON(t, http.MethodPost, srv.URL+"/api/proposals/"+pid+"/"+action, "priya", body)
		if resp.StatusCode != http.StatusBadRequest || code1(t, resp) != "validation" {
			t.Fatalf("%s empty layer: status %d", action, resp.StatusCode)
		}
	}
}

// dev-login does not echo an arbitrary amount of the caller's input.
func TestDevLoginUnknownUserNotEchoed(t *testing.T) {
	srv, _ := newServer(t)
	user := strings.Repeat("A", 5000)
	resp := doJSON(t, http.MethodPost, srv.URL+"/auth/dev-login", "", fmt.Sprintf(`{"user":%q}`, user))
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(b) > 300 || strings.Contains(string(b), strings.Repeat("A", 100)) {
		t.Fatalf("401 body echoes the input (%d bytes)", len(b))
	}
}
