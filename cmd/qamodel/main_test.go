package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a writer the test can read while run still writes to it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// runBackground starts run and returns the URL it printed, once it listens.
func runBackground(t *testing.T, args ...string) (url string, stdout *syncBuffer, stop func() int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stdout, stderr := &syncBuffer{}, &syncBuffer{}
	done := make(chan int, 1)
	go func() { done <- run(ctx, args, stdout, stderr) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if first, _, ok := strings.Cut(stdout.String(), "\n"); ok {
			if u, found := strings.CutPrefix(first, "qamodel: puppet model listening on "); found {
				url = u
				break
			}
		}
		select {
		case code := <-done:
			t.Fatalf("run exited with %d before listening; stderr: %s", code, stderr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("no ready line after 10s; stdout %q stderr %q", stdout, stderr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	var once sync.Once
	var code int
	stop = func() int {
		once.Do(func() {
			cancel()
			select {
			case code = <-done:
			case <-time.After(10 * time.Second):
				t.Error("run did not return within 10s of its context ending")
				code = -1
			}
		})
		return code
	}
	t.Cleanup(func() { stop() })
	return url, stdout, stop
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

func TestServesTheControlAPIAndPrintsHowToUseIt(t *testing.T) {
	url, stdout, stop := runBackground(t, "--addr", "127.0.0.1:0", "--reply-timeout", "45s")

	if code, body := get(t, url+"/qa/pending"); code != http.StatusOK || strings.TrimSpace(body) != "[]" {
		t.Errorf("GET /qa/pending = %d %q", code, body)
	}
	banner := stdout.String()
	for _, want := range []string{
		"ANTHROPIC_BASE_URL=" + url, "ANTHROPIC_API_KEY=qa-puppet", "$env:ANTHROPIC_BASE_URL",
		"/qa/wait?timeout=60s", "/qa/turns/t1/reply", "PIPEROOM_TRIAGE_PASS=off", "within 45s",
	} {
		if !strings.Contains(banner, want) {
			t.Errorf("banner lacks %q:\n%s", want, banner)
		}
	}
	if code := stop(); code != exitOK {
		t.Errorf("exit code = %d, want 0", code)
	}
}

// A model request that waits for a reply must not hold the shutdown up.
func TestShutdownReleasesAParkedRequest(t *testing.T) {
	url, _, stop := runBackground(t, "--addr", "127.0.0.1:0")

	body := `{"model":"m","max_tokens":10,"stream":true,"messages":[{"role":"user","content":"Room: r_1"}]}`
	status := make(chan int, 1)
	go func() {
		resp, err := http.Post(url+"/v1/messages?beta=true", "application/json", strings.NewReader(body))
		if err != nil {
			status <- -1
			return
		}
		resp.Body.Close()
		status <- resp.StatusCode
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		_, out := get(t, url+"/qa/pending")
		var list []struct {
			RoomHint string `json:"room_hint"`
		}
		if json.Unmarshal([]byte(out), &list) == nil && len(list) == 1 && list[0].RoomHint == "r_1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the request was never parked")
		}
		time.Sleep(5 * time.Millisecond)
	}

	start := time.Now()
	if code := stop(); code != exitOK {
		t.Errorf("exit code = %d, want 0", code)
	}
	if d := time.Since(start); d > 4*time.Second {
		t.Errorf("shutdown took %v with a request parked", d)
	}
	select {
	case s := <-status:
		if s != http.StatusServiceUnavailable && s != -1 {
			t.Errorf("the parked request got status %d, want 503 (or a closed connection)", s)
		}
	case <-time.After(5 * time.Second):
		t.Error("the parked request never returned")
	}
}

func TestRefusesBadArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"non-loopback address", []string{"--addr", "0.0.0.0:0"}, "not a loopback address"},
		{"hostname that is not localhost", []string{"--addr", "example.com:9911"}, "not a loopback address"},
		{"no port", []string{"--addr", "127.0.0.1"}, "not a loopback address"},
		{"zero timeout", []string{"--reply-timeout", "0s"}, "must be positive"},
		{"stray argument", []string{"extra"}, "unexpected argument"},
		{"unknown flag", []string{"--nope"}, "flag provided but not defined"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out, errb bytes.Buffer
			code := run(context.Background(), tt.args, &out, &errb)
			if code != exitUsage || !strings.Contains(errb.String(), tt.want) {
				t.Errorf("run(%v) = %d, stderr %q; want exit 2 mentioning %q", tt.args, code, errb.String(), tt.want)
			}
			if out.Len() != 0 {
				t.Errorf("stdout has output on a usage error: %q", out.String())
			}
		})
	}
}

func TestAllowRemoteAcceptsAnAnyAddress(t *testing.T) {
	_, _, stop := runBackground(t, "--addr", "0.0.0.0:0", "--allow-remote")
	if code := stop(); code != exitOK {
		t.Errorf("exit code = %d", code)
	}
}

func TestListenErrorIsReported(t *testing.T) {
	url, _, _ := runBackground(t, "--addr", "127.0.0.1:0")
	addr := strings.TrimPrefix(url, "http://")
	var out, errb bytes.Buffer
	if code := run(context.Background(), []string{"--addr", addr}, &out, &errb); code != exitError || !strings.Contains(errb.String(), "listen") {
		t.Errorf("run on a busy address = %d, stderr %q; want exit 1 mentioning listen", code, errb.String())
	}
}

func TestHelpExitsZero(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run(context.Background(), []string{"--help"}, &out, &errb); code != exitOK || !strings.Contains(errb.String(), "usage: qamodel") {
		t.Errorf("--help = %d %q", code, errb.String())
	}
}

func TestIsLoopback(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:9911": true, "[::1]:9911": true, "localhost:9911": true, "LOCALHOST:1": true, "127.5.5.5:1": true,
		"0.0.0.0:9911": false, ":9911": false, "[::]:9911": false, "10.0.0.5:9911": false, "example.com:80": false, "garbage": false,
	} {
		if got := isLoopback(addr); got != want {
			t.Errorf("isLoopback(%q) = %v, want %v", addr, got, want)
		}
	}
}
