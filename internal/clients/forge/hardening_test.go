package forge

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The App key never reaches a log: slog's JSON handler renders it through
// LogValue as [redacted], at the top level and inside a group.
func TestAppKeyLogValue(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	key := AppKey{k: rsaKey(t)}
	log.Info("forge config", "key", key, slog.Group("app", "key", key))
	var got struct {
		Key string `json:"key"`
		App struct {
			Key string `json:"key"`
		} `json:"app"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("log line %q: %v", buf.String(), err)
	}
	if got.Key != "[redacted]" || got.App.Key != "[redacted]" {
		t.Fatalf("key logged as %q / %q", got.Key, got.App.Key)
	}
	if strings.Contains(buf.String(), key.k.N.String()[:20]) || strings.Contains(buf.String(), "PRIVATE") {
		t.Fatalf("key material in the log: %s", buf.String())
	}
}

// Checks past the first page are read: a failure on page 3 is a failure,
// not a pass. Past the page cap the result is pending, or failure when a
// read page already failed: never success.
func TestChecksPaging(t *testing.T) {
	g, base := newGH(t, baseFiles())
	c := g.client(t)
	ctx := ctx10(t)
	g.pulls = []*ghPull{{number: 1, state: "open", headRef: "piperoom/r_1-p_1-0", headSHA: base}}
	runs := func(n int, failAt int) []map[string]any {
		var out []map[string]any
		for i := range n {
			concl := "success"
			if i == failAt {
				concl = "failure"
			}
			out = append(out, map[string]any{"name": fmt.Sprintf("job-%d", i), "status": "completed", "conclusion": concl})
		}
		return out
	}

	g.runs = runs(250, 230)
	pr, err := c.PullRequest(ctx, tRepo, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pr.Checks.State != ChecksFailure || pr.Checks.Total != 250 || len(pr.Checks.Failed) != 1 || pr.Checks.Failed[0] != "job-230" {
		t.Fatalf("failure on page 3: %+v", pr.Checks)
	}
	if n := g.count("GET", `/check-runs$`); n != 3 {
		t.Fatalf("%d check-runs pages read, want 3", n)
	}

	g.runs = runs(250, -1)
	g.statuses = nil
	for i := range 150 {
		g.statuses = append(g.statuses, map[string]any{"context": fmt.Sprintf("ci/%d", i), "state": "success"})
	}
	g.statuses[120]["state"] = "error"
	if pr, _ := c.PullRequest(ctx, tRepo, 1); pr.Checks.State != ChecksFailure || pr.Checks.Total != 400 {
		t.Fatalf("status failure on page 2: %+v", pr.Checks)
	}

	// Past the cap: all read pages pass, the rest is unread: pending.
	g.statuses = nil
	g.runs = runs(checksPerPage*maxCheckPages+5, -1)
	if pr, _ := c.PullRequest(ctx, tRepo, 1); pr.Checks.State != ChecksPending {
		t.Fatalf("past the cap: %+v, want pending", pr.Checks)
	}
	// Past the cap with a failure read: failure.
	g.runs = runs(checksPerPage*maxCheckPages+5, 3)
	if pr, _ := c.PullRequest(ctx, tRepo, 1); pr.Checks.State != ChecksFailure {
		t.Fatalf("past the cap with a failure: %+v, want failure", pr.Checks)
	}
}

// An executable file keeps mode 100755 when a fix edits it; a new file and
// an ordinary one are 100644.
func TestWriterKeepsExecutableMode(t *testing.T) {
	files := baseFiles()
	files["docker/connect/entrypoint.sh"] = "#!/bin/sh\nexec connect\n"
	g, base := newGH(t, files)
	g.modes["docker/connect/entrypoint.sh"] = "100755"
	w, _ := NewWriter(g.client(t))
	req := fixReq(base, "docker/connect/entrypoint.sh", "#!/bin/sh\nexec connect --fixed\n")
	req.Files = append(req.Files,
		FileChange{Path: "docker/connect/Dockerfile", Content: []byte(newDockerfile)},
		FileChange{Path: "docker/connect/new.env", Content: []byte("A=1\n")})
	if _, err := w.OpenPullRequest(ctx10(t), tRepo, req); err != nil {
		t.Fatal(err)
	}
	var tree struct {
		Tree []struct{ Path, Mode string }
	}
	for _, r := range g.requests() {
		if r.Method == "POST" && strings.HasSuffix(r.Path, "/git/trees") {
			_ = json.Unmarshal(r.Body, &tree)
		}
	}
	want := map[string]string{"docker/connect/entrypoint.sh": "100755", "docker/connect/Dockerfile": "100644", "docker/connect/new.env": "100644"}
	if len(tree.Tree) != len(want) {
		t.Fatalf("tree entries = %+v", tree.Tree)
	}
	for _, e := range tree.Tree {
		if want[e.Path] != e.Mode {
			t.Errorf("%s: mode %s, want %s", e.Path, e.Mode, want[e.Path])
		}
	}
}

// A pre-created branch or PR is reused only when the App wrote it: a branch
// someone else pushed with exactly the change, or an open PR on the App's
// branch opened by someone else, is ErrBranchExists, and nothing is written.
func TestWriterReuseRequiresApp(t *testing.T) {
	req := func(base string) PullRequestRequest { return fixReq(base, "docker/connect/Dockerfile", newDockerfile) }

	// Someone pushed the exact change to the fix branch first.
	g, base := newGH(t, baseFiles())
	w, _ := NewWriter(g.client(t))
	g.branches[tBranch] = g.commitOn("main", map[string]string{"docker/connect/Dockerfile": newDockerfile})
	g.branches["main"] = base
	if _, err := w.OpenPullRequest(ctx10(t), tRepo, req(base)); !errors.Is(err, ErrBranchExists) {
		t.Fatalf("someone else's branch: %v", err)
	}
	if len(g.pulls) != 0 || g.count("POST", `/pulls$`) != 0 {
		t.Fatal("a PR was opened on someone else's branch")
	}

	// The App's branch, but the open PR on it is someone else's.
	g2, base2 := newGH(t, baseFiles())
	w2, _ := NewWriter(g2.client(t))
	if _, err := w2.OpenPullRequest(ctx10(t), tRepo, req(base2)); err != nil {
		t.Fatal(err)
	}
	g2.pulls[0].user, g2.pulls[0].userType = "mallory", "User"
	if _, err := w2.OpenPullRequest(ctx10(t), tRepo, req(base2)); !errors.Is(err, ErrBranchExists) {
		t.Fatalf("someone else's PR: %v", err)
	}
	// Restored to the App's: the retry reuses it.
	g2.pulls[0].user, g2.pulls[0].userType = tBot, "Bot"
	if pr, err := w2.OpenPullRequest(ctx10(t), tRepo, req(base2)); err != nil || pr.Number != 1 {
		t.Fatalf("the App's own PR: %+v %v", pr, err)
	}
	// A PR from a fork with the same branch name is not the App's.
	g2.pulls[0].headRepo = "mallory/connect-config"
	if _, err := w2.OpenPullRequest(ctx10(t), tRepo, req(base2)); !errors.Is(err, ErrBranchExists) {
		t.Fatalf("fork PR: %v", err)
	}
	if g2.count("GET", `^/app$`) != 1 {
		t.Fatalf("the App was read %d times, want once (cached)", g2.count("GET", `^/app$`))
	}
}

// A slow token mint for one repo does not hold up a mint for another: the
// cache lock is per repo and scope, not held across the request.
func TestTokenMintPerKeyLock(t *testing.T) {
	g, _ := newGH(t, baseFiles())
	c := g.client(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var mints atomic.Int32
	g.setHook(func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "access_tokens") && mints.Add(1) == 1 {
			close(entered)
			<-release
		}
		return false
	})
	done := make(chan error, 1)
	slow := tRepo
	slow.Name = "slow-repo"
	go func() {
		_, err := c.token(ctx10(t), slow, scopeRead)
		done <- err
	}()
	<-entered
	fast := make(chan error, 1)
	go func() {
		_, err := c.token(ctx10(t), tRepo, scopeRead)
		fast <- err
	}()
	select {
	case err := <-fast:
		if err != nil {
			t.Fatalf("fast mint: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a slow mint for another repo blocked this one")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("slow mint: %v", err)
	}
}
