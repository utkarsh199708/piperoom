//go:build integration

package forge

// The sandbox integration test (design §10, §13): it runs only when the
// PIPEROOM_IT_GITHUB_* variables name a GitHub App installed on a sandbox
// repository, and is skipped otherwise (the dev stack has no GitHub).
//
//	PIPEROOM_IT_GITHUB_APP_ID, PIPEROOM_IT_GITHUB_INSTALLATION_ID,
//	PIPEROOM_IT_GITHUB_PRIVATE_KEY_FILE, PIPEROOM_IT_GITHUB_REPO (owner/name)
//	optional: PIPEROOM_IT_GITHUB_BASE (main), PIPEROOM_IT_GITHUB_API_URL,
//	PIPEROOM_IT_GITHUB_FILE (README.md), PIPEROOM_IT_GITHUB_WRITE=1 to open,
//	check and then close a PR and delete its branch.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/netsec"
)

func itClient(t *testing.T) (*Client, RepoRef) {
	t.Helper()
	appID, _ := strconv.ParseInt(os.Getenv("PIPEROOM_IT_GITHUB_APP_ID"), 10, 64)
	inst, _ := strconv.ParseInt(os.Getenv("PIPEROOM_IT_GITHUB_INSTALLATION_ID"), 10, 64)
	keyFile := os.Getenv("PIPEROOM_IT_GITHUB_PRIVATE_KEY_FILE")
	owner, name, ok := strings.Cut(os.Getenv("PIPEROOM_IT_GITHUB_REPO"), "/")
	if appID == 0 || inst == 0 || keyFile == "" || !ok {
		t.Skip("PIPEROOM_IT_GITHUB_* not set: no sandbox repository")
	}
	key, err := LoadPrivateKeyFile(keyFile, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(Config{APIURL: os.Getenv("PIPEROOM_IT_GITHUB_API_URL"), AppID: appID, InstallationID: inst, Key: key})
	if err != nil {
		t.Fatal(err)
	}
	base := os.Getenv("PIPEROOM_IT_GITHUB_BASE")
	if base == "" {
		base = "main"
	}
	return c, RepoRef{ID: "it", Owner: owner, Name: name, Base: base}
}

func TestIntegrationReads(t *testing.T) {
	c, repo := itClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	info, err := c.RepoInfo(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("repo info: %+v", info)
	head, err := c.BaseHead(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	entries, truncated, err := c.ListFiles(ctx, repo, head)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d files (truncated %v)", len(entries), truncated)
	p := os.Getenv("PIPEROOM_IT_GITHUB_FILE")
	if p == "" {
		p = "README.md"
	}
	f, err := c.File(ctx, repo, head, p)
	if err != nil {
		t.Fatal(err)
	}
	if f.BlobSHA != BlobSHA(f.Content) {
		t.Fatal("blob id mismatch")
	}
}

func TestIntegrationOpenPullRequest(t *testing.T) {
	if os.Getenv("PIPEROOM_IT_GITHUB_WRITE") != "1" {
		t.Skip("PIPEROOM_IT_GITHUB_WRITE is not 1")
	}
	c, repo := itClient(t)
	w, _ := NewWriter(c)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	head, err := c.BaseHead(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	var rb [4]byte
	_, _ = rand.Read(rb[:])
	id := "it_" + hex.EncodeToString(rb[:])
	req := PullRequestRequest{BaseSHA: head, Branch: FixBranch("r_"+id, "p_"+id, 0), Title: "piperoom integration " + id,
		Body: "Opened by the forge integration test; closed by it.", CommitMessage: "piperoom integration " + id,
		Files: []FileChange{{Path: "piperoom-it/" + id + ".txt", Content: []byte("integration " + id + "\n")}}}
	pr, err := w.OpenPullRequest(ctx, repo, req)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := c.token(ctx, repo, scopeWrite)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup uses raw requests outside the client's allow-list on purpose:
	// the client can neither close a PR nor delete a branch it did not just
	// create.
	t.Cleanup(func() {
		raw(t, c, tok, http.MethodPatch, repoPath(repo)+"/pulls/"+strconv.Itoa(pr.Number), `{"state":"closed"}`)
		raw(t, c, tok, http.MethodDelete, repoPath(repo)+"/git/refs/heads/"+escapePath(req.Branch), "")
	})
	again, err := w.OpenPullRequest(ctx, repo, req)
	if err != nil || again.Number != pr.Number {
		t.Fatalf("retry: %+v %v", again, err)
	}
	// Whether GitHub signs a commit an App makes through the API (design §12).
	var cm struct {
		Commit struct {
			Verification struct {
				Verified bool   `json:"verified"`
				Reason   string `json:"reason"`
			} `json:"verification"`
		} `json:"commit"`
	}
	b := raw(t, c, tok, http.MethodGet, repoPath(repo)+"/commits/"+pr.HeadSHA, "")
	_ = json.Unmarshal(b, &cm)
	t.Logf("PR %s; commit verified=%v reason=%s", pr.URL, cm.Commit.Verification.Verified, cm.Commit.Verification.Reason)
}

func raw(t *testing.T, c *Client, tok netsec.Secret, method, p, body string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.WithValue(ctx, authKey{}, tok), method, c.base.String()+p, rd)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", apiVersion)
	req.Header.Set("User-Agent", "piperoom-it")
	resp, err := c.hc.Do(req)
	if err != nil {
		t.Errorf("%s %s: %v", method, p, err)
		return nil
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, MaxBodyBytes))
	if resp.StatusCode >= 300 {
		t.Errorf("%s %s: status %d", method, p, resp.StatusCode)
	}
	return out
}
