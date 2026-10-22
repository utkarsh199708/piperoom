package httpapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/httpapi"
)

// maxRequestBody mirrors the 1 MiB request body cap in httpapi (docs/api.md).
const maxRequestBody = 1 << 20

// oversizedJSON returns ~1.1 MB of well-formed JSON carrying sentinel. Only
// its size is wrong, never its syntax: that is the KAN-388 case, and the
// answer must be a size error, not "invalid JSON body".
func oversizedJSON(sentinel string) string {
	const head = `{"title":"too big","severity":"sev2","pipeline_id":"orders","sentinel":"`
	const tail = `"}`
	pad := 1100*1024 - len(head) - len(tail) - len(sentinel)
	return head + sentinel + strings.Repeat("a", pad) + tail
}

// jsonOfSize returns well-formed JSON of exactly size bytes. "pad" is an
// unknown field, which the create-room decoder ignores.
func jsonOfSize(t *testing.T, size int) string {
	t.Helper()
	const head = `{"title":"bounded","severity":"sev2","pipeline_id":"orders","pad":"`
	const tail = `"}`
	if size < len(head)+len(tail) {
		t.Fatalf("size %d is smaller than the JSON skeleton", size)
	}
	return head + strings.Repeat("a", size-len(head)-len(tail)) + tail
}

// wantTooLarge checks docs/api.md's 413 envelope exactly (one error object
// with code and message) and that the answer never echoes request bytes.
func wantTooLarge(t *testing.T, resp *http.Response, limit, sentinel string) {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read error body: %v", err)
	}
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body %s", resp.StatusCode, body)
	}
	if sentinel != "" && bytes.Contains(body, []byte(sentinel)) {
		t.Fatalf("413 body echoes request bytes: %s", body)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		t.Fatalf("413 body is not a JSON object: %v; body %s", err, body)
	}
	if len(top) != 1 || top["error"] == nil {
		t.Fatalf("413 envelope = %s, want only an error object", body)
	}
	var e struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(top["error"], &e); err != nil {
		t.Fatalf("decode 413 error object: %v; body %s", err, body)
	}
	if e.Code != "too_large" {
		t.Fatalf("code = %q, want too_large; body %s", e.Code, body)
	}
	if want := "request body too large (limit " + limit + ")"; e.Message != want {
		t.Fatalf("message = %q, want %q", e.Message, want)
	}
}

// TestOversizedBodyIs413 covers every body-reading route that used to turn
// the size error into 400 validation (TC1, TC4, TC6).
func TestOversizedBodyIs413(t *testing.T) {
	srv, _ := newServer(t)
	const sentinel = "KAN388-SENTINEL-DO-NOT-ECHO"
	routes := []struct{ name, path string }{
		{"create room", "/api/rooms"},
		{"alert webhook", "/api/alerts/webhook"},
		{"optional body route", "/api/rooms/r1/suggestions/s1/decline"},
	}
	for _, rt := range routes {
		t.Run(rt.name, func(t *testing.T) {
			resp := doJSON(t, http.MethodPost, srv.URL+rt.path, "arjun", oversizedJSON(sentinel))
			wantTooLarge(t, resp, "1 MiB", sentinel)
		})
	}
}

// TestBodyAtCap: exactly the cap is accepted, one byte more is 413 (TC2).
func TestBodyAtCap(t *testing.T) {
	srv, _ := newServer(t)
	for _, rt := range []struct{ name, path string }{
		{"create room", "/api/rooms"},
		{"alert webhook", "/api/alerts/webhook"},
	} {
		t.Run(rt.name+" exactly at cap", func(t *testing.T) {
			resp := doJSON(t, http.MethodPost, srv.URL+rt.path, "arjun", jsonOfSize(t, maxRequestBody))
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want 201; body %s", resp.StatusCode, body)
			}
		})
		t.Run(rt.name+" one byte over", func(t *testing.T) {
			resp := doJSON(t, http.MethodPost, srv.URL+rt.path, "arjun", jsonOfSize(t, maxRequestBody+1))
			wantTooLarge(t, resp, "1 MiB", "")
		})
	}
}

// TestMalformedBodyIs400: malformed input keeps the old 400 validation
// answer; only an oversized body is 413 (TC3).
func TestMalformedBodyIs400(t *testing.T) {
	srv, _ := newServer(t)
	cases := []struct{ name, body string }{
		{"syntax error", `{"title":`},
		{"truncated", `{"title":"x"`},
		{"wrong field type", `{"title":123,"severity":"sev2","pipeline_id":"orders"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", tc.body)
			b := readErr(t, resp)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			if b.Error.Code != "validation" || b.Error.Message != "invalid JSON body" {
				t.Fatalf("error = %+v, want validation/invalid JSON body", b.Error)
			}
		})
	}
}

// readerOnly hides the concrete reader from http.NewRequest, so the client
// sends the body chunked with no Content-Length (TC5).
type readerOnly struct{ io.Reader }

func TestChunkedOversizedBodyIs413(t *testing.T) {
	srv, _ := newServer(t)
	const sentinel = "KAN388-CHUNKED-SENTINEL"
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/rooms", readerOnly{strings.NewReader(oversizedJSON(sentinel))})
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-User", "arjun")
	req.Header.Set("Content-Type", "application/json")
	if req.ContentLength != 0 {
		t.Fatalf("precondition: want no Content-Length, got %d", req.ContentLength)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	wantTooLarge(t, resp, "1 MiB", sentinel)
}

// TestOversizedAdminTestBodyIs413: the admin route shares the decoder and its
// own smaller cap, so its answer names that cap.
func TestOversizedAdminTestBodyIs413(t *testing.T) {
	srv, _ := newAdminServer(t, httpapi.WithAdmin(&fakeAdmin{}))
	const sentinel = "KAN388-ADMIN-SENTINEL"
	body := `{"id":"` + sentinel + strings.Repeat("a", 4<<10) + `"}`
	resp := adminReq(t, http.MethodPost, srv.URL+"/api/admin/connections/test", "ada", testJSON, body)
	wantTooLarge(t, resp, "4 KiB", sentinel)
}
