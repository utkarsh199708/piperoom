package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestOllamaPull(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr string // substring; "" = success
	}{
		{"success", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "{\"status\":\"pulling manifest\"}\n{\"status\":\"downloading\",\"total\":10,\"completed\":5}\n\n{\"status\":\"success\"}\n")
		}, ""},
		{"not found", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error":"pull model manifest: file does not exist"}`, http.StatusNotFound)
		}, "status 404"},
		{"server error", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}, "status 500"},
		{"error in stream", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "{\"status\":\"pulling manifest\"}\n{\"error\":\"no space left on device\"}\n")
		}, "no space left on device"},
		{"bad json", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "{\"status\":\"pulling manifest\"}\nnot json\n")
		}, "bad progress line"},
		{"ends before success", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "{\"status\":\"pulling manifest\"}\n")
		}, "before success"},
		{"line too long", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, strings.Repeat("x", llmMaxLine+10)+"\n")
		}, "read stream"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotBody, gotPath string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				gotBody, gotPath = string(b), r.URL.Path
				tc.handler(w, r)
			}))
			defer srv.Close()
			err := ollamaPull(context.Background(), srv.Client(), srv.URL, "llama3.1:8b", nil)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("err = %v", err)
				}
				if gotPath != "/api/pull" || !strings.Contains(gotBody, `"model":"llama3.1:8b"`) {
					t.Errorf("request = %s %s", gotPath, gotBody)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestOllamaPullTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := ollamaPull(ctx, srv.Client(), srv.URL, "m", nil)
	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
}

func TestLLMUpComposeThenPull(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "{\"status\":\"success\"}\n")
	}))
	defer srv.Close()
	var calls []string
	st := &stack{
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		llmHTTP: srv.Client(),
		composeFn: func(_ context.Context, _ map[string]string, args ...string) error {
			calls = append(calls, strings.Join(args, " "))
			return nil
		},
	}
	if err := st.llmUp(context.Background(), srv.URL, "m", time.Minute); err != nil {
		t.Fatal(err)
	}
	if want := []string{"--profile llm up -d --wait ollama"}; !slices.Equal(calls, want) {
		t.Errorf("compose calls = %v, want %v", calls, want)
	}
}

// TestComposeLLMProfile keeps the Ollama service pinned, loopback-only,
// profile-gated and set to a context window that does not truncate.
func TestComposeLLMProfile(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "dev", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]q3Service `yaml:"services"`
		Volumes  map[string]any       `yaml:"volumes"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse compose file: %v", err)
	}
	o, ok := doc.Services["ollama"]
	if !ok {
		t.Fatal("no ollama service")
	}
	if !slices.Equal(o.Profiles, []string{"llm"}) {
		t.Errorf("profiles = %v, want [llm]", o.Profiles)
	}
	if !pinned(o.Image) {
		t.Errorf("image %q is not pinned", o.Image)
	}
	if !slices.Equal(o.Ports, []string{"127.0.0.1:11434:11434"}) {
		t.Errorf("ports = %v", o.Ports)
	}
	if o.Environment["OLLAMA_CONTEXT_LENGTH"] != "131072" {
		t.Errorf("OLLAMA_CONTEXT_LENGTH = %q", o.Environment["OLLAMA_CONTEXT_LENGTH"])
	}
	if len(o.Healthcheck.Test) == 0 {
		t.Error("no healthcheck")
	}
	if _, ok := doc.Volumes["ollama-models"]; !ok || !slices.Contains(o.Volumes, "ollama-models:/root/.ollama") {
		t.Errorf("models volume missing: %v / %v", doc.Volumes, o.Volumes)
	}
}
