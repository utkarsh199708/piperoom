package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"piperoom/internal/config"
)

func TestRoomsDeleteSendsTheRequest(t *testing.T) {
	var got *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"room_id":"r_1","pipeline_id":"orders","deleted_by":"ada","deleted_at":"2026-10-13T09:00:00Z","reason":"x y","event_count":42,"archived":true}`))
	}))
	defer srv.Close()
	t.Setenv("PIPEROOM_COOKIE", "")

	var out bytes.Buffer
	st := &stack{cfg: config.Config{HTTPAddr: "127.0.0.1:1"}}
	// Flags after the id, the order an operator types them.
	err := roomsCmd(context.Background(), st, []string{"delete", "r_1", "--reason", "x y", "--as", "ada", "--server", srv.URL}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if got.Method != http.MethodDelete || got.URL.Path != "/api/rooms/r_1" || got.URL.Query().Get("reason") != "x y" || got.URL.Query().Has("archive") {
		t.Fatalf("request = %s %s", got.Method, got.URL)
	}
	if got.Header.Get("X-User") != "ada" || got.Header.Get("Content-Type") != "application/json" || got.Header.Get("Cookie") != "" {
		t.Fatalf("headers = %v", got.Header)
	}
	if s := out.String(); !strings.Contains(s, "deleted room r_1 (pipeline orders): 42 events; by ada at 2026-10-13T09:00:00Z; archived first") {
		t.Fatalf("output = %q", s)
	}

	// Flags first, cookie from the environment, no --as.
	t.Setenv("PIPEROOM_COOKIE", "__Host-piperoom_session=abc")
	out.Reset()
	if err := roomsCmd(context.Background(), st, []string{"delete", "--server", srv.URL, "--archive", "--reason", "r", "r_2"}, &out); err != nil {
		t.Fatal(err)
	}
	if got.URL.Path != "/api/rooms/r_2" || got.URL.Query().Get("archive") != "true" || got.Header.Get("Cookie") != "__Host-piperoom_session=abc" || got.Header.Get("X-User") != "" {
		t.Fatalf("cookie request = %s %v", got.URL, got.Header)
	}
	if strings.Contains(out.String(), "abc") {
		t.Fatal("the cookie was printed")
	}
}

func TestRoomsDeleteReportsServerErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":"not_allowed","message":"deleting a room needs the admin role"}}`))
	}))
	defer srv.Close()
	t.Setenv("PIPEROOM_COOKIE", "")
	err := roomsCmd(context.Background(), &stack{}, []string{"delete", "r_1", "--reason", "x", "--as", "arjun", "--server", srv.URL}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "403 not_allowed") {
		t.Fatalf("err = %v, want the server's 403 not_allowed", err)
	}

	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer plain.Close()
	err = roomsCmd(context.Background(), &stack{}, []string{"delete", "r_1", "--reason", "x", "--as", "ada", "--server", plain.URL}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "HTTP 502") {
		t.Fatalf("err = %v, want HTTP 502", err)
	}

	// Server down.
	down := httptest.NewServer(http.NotFoundHandler())
	url := down.URL
	down.Close()
	if err := roomsCmd(context.Background(), &stack{}, []string{"delete", "r_1", "--reason", "x", "--as", "ada", "--server", url}, &bytes.Buffer{}); err == nil {
		t.Fatal("a server that is down must be an error")
	}
}

func TestRoomsDeleteUsage(t *testing.T) {
	t.Setenv("PIPEROOM_COOKIE", "")
	for _, args := range [][]string{
		nil,
		{"list"},
		{"delete"},
		{"delete", "r_1"},                   // no reason
		{"delete", "r_1", "--reason", "  "}, // blank reason
		{"delete", "r_1", "r_2", "--reason", "x"},     // two ids
		{"delete", "r_1", "--reason", "x", "--bogus"}, // unknown flag
	} {
		err := roomsCmd(context.Background(), &stack{}, args, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "usage: devctl rooms delete") {
			t.Errorf("args %q: err = %v, want the usage line", args, err)
		}
	}
	// Nobody to act as.
	err := roomsCmd(context.Background(), &stack{}, []string{"delete", "r_1", "--reason", "x"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "--as") {
		t.Fatalf("err = %v, want a hint about --as", err)
	}
	if err := roomsCmd(context.Background(), &stack{}, []string{"delete", "r_1", "--reason", "x", "--as", "ada", "--server", "ftp://x"}, &bytes.Buffer{}); err == nil {
		t.Fatal("a non-http server URL must be refused")
	}
}

func TestServerURL(t *testing.T) {
	for in, want := range map[string]string{
		"127.0.0.1:8090": "http://127.0.0.1:8090",
		":8090":          "http://127.0.0.1:8090",
		"0.0.0.0:8090":   "http://127.0.0.1:8090",
		"[::]:8090":      "http://127.0.0.1:8090",
		"[::1]:8090":     "http://[::1]:8090",
	} {
		if got := serverURL(in); got != want {
			t.Errorf("serverURL(%q) = %q, want %q", in, got, want)
		}
	}
}
