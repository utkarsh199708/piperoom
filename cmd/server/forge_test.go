package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/forge"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

func forgeOwners(url string) *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", Repos: []owners.Repo{{
		ID: "connect-config", URL: url, Base: "main",
		Paths: []owners.RepoPath{{Glob: "connectors/*.json", Layer: "kafka-connect"}},
	}}}}}
}

func writeAppKey(t *testing.T) string {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "app.pem")
	b := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// forgeSetup: off without FORGE_KIND, an error for a half-set or broken
// forge and for a repo on another host, a reader otherwise.
func TestForgeSetup(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	key := writeAppKey(t)
	gh := config.Config{ForgeKind: "github", GitHubAppID: "1", GitHubAppInstallationID: "2", GitHubAppPrivateKeyFile: key}
	cases := []struct {
		name    string
		cfg     config.Config
		own     *owners.Owners
		reader  bool
		wantErr string
	}{
		{"off", config.Config{}, forgeOwners("https://github.com/acme/connect-config"), false, ""},
		{"app id without FORGE_KIND", config.Config{GitHubAppID: "1"}, nil, false, "FORGE_KIND"},
		{"github", gh, forgeOwners("https://github.com/acme/connect-config"), true, ""},
		{"repo on another host", gh, forgeOwners("https://ghe.example.com/acme/connect-config"), false, "web host"},
		{"key file missing", config.Config{ForgeKind: "github", GitHubAppID: "1", GitHubAppInstallationID: "2",
			GitHubAppPrivateKeyFile: filepath.Join(t.TempDir(), "none.pem")}, nil, false, "private key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := forgeSetup(tc.cfg, tc.own, quiet)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("forgeSetup = %v, want an error naming %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if (r != nil) != tc.reader {
				t.Fatalf("reader %v, want %v", r, tc.reader)
			}
		})
	}
}

type stubForgePR struct {
	pr   forge.PullRequest
	err  error
	repo forge.RepoRef
}

func (s *stubForgePR) PullRequest(_ context.Context, repo forge.RepoRef, _ int) (forge.PullRequest, error) {
	s.repo = repo
	return s.pr, s.err
}

// The adapter maps the forge's types onto the contract's and its rate-limit
// answer onto room.PRRateLimitError.
func TestForgePRReader(t *testing.T) {
	at := time.Date(2026, 10, 23, 14, 2, 0, 0, time.UTC)
	stub := &stubForgePR{pr: forge.PullRequest{Repo: "connect-config", Number: 12, State: "merged", HeadRef: "piperoom/r-p-0",
		HeadSHA: "abc", MergeSHA: "def", MergedAt: at, Checks: forge.PullRequestChecks{State: "failure", Total: 2, Failed: []string{"build"}}}}
	r := forgePRReader{c: stub}
	got, err := r.PullRequest(context.Background(), clients.RepoRef{ID: "connect-config", Owner: "acme", Name: "connect-config", Base: "main"}, 12)
	if err != nil {
		t.Fatal(err)
	}
	if stub.repo != (forge.RepoRef{ID: "connect-config", Owner: "acme", Name: "connect-config", Base: "main"}) {
		t.Fatalf("forge repo %+v", stub.repo)
	}
	if got.State != "merged" || got.MergeSHA != "def" || !got.MergedAt.Equal(at) || got.HeadRef != "piperoom/r-p-0" ||
		got.Checks.State != "failure" || got.Checks.Total != 2 || len(got.Checks.Failed) != 1 {
		t.Fatalf("pull request %+v", got)
	}

	stub.err = fmt.Errorf("forge: GET /repos/acme/connect-config/pulls/12: %w", forge.ErrRateLimited)
	_, err = r.PullRequest(context.Background(), clients.RepoRef{ID: "connect-config"}, 12)
	var rl *room.PRRateLimitError
	if !errors.As(err, &rl) || !errors.Is(err, forge.ErrRateLimited) {
		t.Fatalf("rate limit error %v, want a room.PRRateLimitError wrapping forge.ErrRateLimited", err)
	}
	stub.err = clients.ErrNotFound
	if _, err = r.PullRequest(context.Background(), clients.RepoRef{ID: "connect-config"}, 12); !errors.Is(err, clients.ErrNotFound) || errors.As(err, &rl) {
		t.Fatalf("not found error %v", err)
	}
}
