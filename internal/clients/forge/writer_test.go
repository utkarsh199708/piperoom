package forge

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

const tBranch = "piperoom/r_01J-p_01K-0"

func fixReq(base, p, content string) PullRequestRequest {
	return PullRequestRequest{BaseSHA: base, Branch: tBranch, Title: "connect image: bump Debezium", Body: "Cause: ...",
		CommitMessage: "connect image: bump Debezium\n\nPiperoom-Room: r_01J", Files: []FileChange{{Path: p, Content: []byte(content)}}}
}

const newDockerfile = "FROM quay.io/debezium/connect:2.5\nARG DEBEZIUM_VERSION=2.7.3.Final\n"

func TestOpenPullRequest(t *testing.T) {
	g, base := newGH(t, baseFiles())
	w, _ := NewWriter(g.client(t))
	req := fixReq(base, "docker/connect/Dockerfile", newDockerfile)
	req.Files = append(req.Files, FileChange{Path: "connectors/new/orders-2.json", Content: []byte(`{"name":"orders-2"}`)})
	req.Draft = true
	pr, err := w.OpenPullRequest(ctx10(t), tRepo, req)
	if err != nil {
		t.Fatal(err)
	}
	if pr.Number != 1 || pr.State != PRStateOpen || pr.HeadRef != tBranch || pr.Repo != tRepo.ID ||
		pr.URL != "https://"+g.client(t).WebHost()+"/acme/connect-config/pull/1" || pr.Checks.State != ChecksNone {
		t.Fatalf("pr = %+v", pr)
	}
	// The URL is built from the web host, never from html_url.
	if strings.Contains(pr.URL, "evil") {
		t.Fatal("html_url used")
	}
	head := g.branches[tBranch]
	if pr.HeadSHA != head {
		t.Fatalf("head %s, branch at %s", pr.HeadSHA, head)
	}
	cm := g.commits[head]
	if cm.parent != base || string(cm.files["docker/connect/Dockerfile"]) != newDockerfile || string(cm.files["README.md"]) != "readme\n" ||
		string(cm.files["connectors/new/orders-2.json"]) == "" {
		t.Fatalf("commit = %+v", cm)
	}
	if g.branches["main"] != base {
		t.Fatal("base branch moved")
	}
	if n := g.count("POST", `/git/refs$`); n != 1 {
		t.Fatalf("%d refs created", n)
	}
	var pull struct{ Draft bool }
	for _, r := range g.requests() {
		if r.Method == "POST" && strings.HasSuffix(r.Path, "/pulls") {
			if !strings.Contains(string(r.Body), `"draft":true`) || !strings.Contains(string(r.Body), `"base":"main"`) {
				t.Fatalf("pull body %s", r.Body)
			}
		}
	}
	_ = pull
}

func TestOpenPullRequestBaseMoved(t *testing.T) {
	g, base := newGH(t, baseFiles())
	w, _ := NewWriter(g.client(t))
	// An unrelated file moving on the base is fine: the branch starts at base.
	g.commitOn("main", map[string]string{"README.md": "readme 2\n"})
	if _, err := w.OpenPullRequest(ctx10(t), tRepo, fixReq(base, "docker/connect/Dockerfile", newDockerfile)); err != nil {
		t.Fatalf("unrelated move: %v", err)
	}
	if g.commits[g.branches[tBranch]].parent != base {
		t.Fatal("branch not from base_sha")
	}

	g2, base2 := newGH(t, baseFiles())
	w2, _ := NewWriter(g2.client(t))
	g2.commitOn("main", map[string]string{"docker/connect/Dockerfile": "FROM other\n"})
	_, err := w2.OpenPullRequest(ctx10(t), tRepo, fixReq(base2, "docker/connect/Dockerfile", newDockerfile))
	if !errors.Is(err, ErrBaseMoved) || !strings.Contains(err.Error(), "docker/connect/Dockerfile") {
		t.Fatalf("err = %v", err)
	}
	// A new file that appeared on the base is a move too.
	g2.commitOn("main", map[string]string{"connectors/x.json": "{}"})
	if _, err := w2.OpenPullRequest(ctx10(t), tRepo, fixReq(base2, "connectors/x.json", "{}\n")); !errors.Is(err, ErrBaseMoved) {
		t.Fatalf("new file appeared: %v", err)
	}
	for _, r := range g2.requests() {
		if r.Method != http.MethodGet && !strings.HasSuffix(r.Path, "access_tokens") {
			t.Fatalf("wrote on a moved base: %s %s", r.Method, r.Path)
		}
	}
}

func TestOpenPullRequestIdempotent(t *testing.T) {
	g, base := newGH(t, baseFiles())
	w, _ := NewWriter(g.client(t))
	req := fixReq(base, "docker/connect/Dockerfile", newDockerfile)
	first, err := w.OpenPullRequest(ctx10(t), tRepo, req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := w.OpenPullRequest(ctx10(t), tRepo, req)
	if err != nil || again.Number != first.Number || again.HeadSHA != first.HeadSHA {
		t.Fatalf("retry = %+v, %v", again, err)
	}
	if len(g.pulls) != 1 || g.count("POST", `/git/refs$`) != 1 || g.count("POST", `/git/commits$`) != 1 {
		t.Fatalf("retry wrote again: %d pulls", len(g.pulls))
	}
	// Same branch, other content: refused, no PR.
	other := fixReq(base, "docker/connect/Dockerfile", "FROM x\n")
	if _, err := w.OpenPullRequest(ctx10(t), tRepo, other); !errors.Is(err, ErrBranchExists) {
		t.Fatalf("other content: %v", err)
	}
	// The PR was closed: a retry opens a new one on the same branch, and the
	// branch (not this call's) is never deleted.
	g.pulls[0].state = "closed"
	g.setHook(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/pulls") {
			ghErr(w, 422, "Validation Failed")
			return true
		}
		return false
	})
	if _, err := w.OpenPullRequest(ctx10(t), tRepo, req); err == nil {
		t.Fatal("PR create failure not reported")
	}
	if g.count("DELETE", `.`) != 0 || g.branches[tBranch] == "" {
		t.Fatal("deleted a branch this call did not create")
	}
}

func TestOpenPullRequestDeletesOwnBranchOnFailure(t *testing.T) {
	g, base := newGH(t, baseFiles())
	w, _ := NewWriter(g.client(t))
	g.setHook(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/pulls") {
			ghErr(w, 422, "Validation Failed")
			return true
		}
		return false
	})
	_, err := w.OpenPullRequest(ctx10(t), tRepo, fixReq(base, "docker/connect/Dockerfile", newDockerfile))
	if err == nil || !strings.Contains(err.Error(), "was deleted") {
		t.Fatalf("err = %v", err)
	}
	if _, ok := g.branches[tBranch]; ok || g.count("DELETE", `/git/refs/heads/piperoom/r_01J-p_01K-0$`) != 1 {
		t.Fatal("own branch not deleted")
	}

	g2, base2 := newGH(t, baseFiles())
	w2, _ := NewWriter(g2.client(t))
	g2.setHook(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/pulls") || r.Method == "DELETE" {
			ghErr(w, 502, "bad gateway")
			return true
		}
		return false
	})
	_, err = w2.OpenPullRequest(ctx10(t), tRepo, fixReq(base2, "docker/connect/Dockerfile", newDockerfile))
	if err == nil || !strings.Contains(err.Error(), "is left") {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenPullRequestRefusals(t *testing.T) {
	g, base := newGH(t, baseFiles())
	w, _ := NewWriter(g.client(t))
	mod := func(f func(*PullRequestRequest)) PullRequestRequest {
		r := fixReq(base, "docker/connect/Dockerfile", newDockerfile)
		f(&r)
		return r
	}
	many := make([]FileChange, MaxFixFiles+1)
	for i := range many {
		many[i] = FileChange{Path: "f" + strings.Repeat("x", i), Content: []byte("x")}
	}
	cases := map[string]PullRequestRequest{
		"branch main":    mod(func(r *PullRequestRequest) { r.Branch = "main" }),
		"branch other":   mod(func(r *PullRequestRequest) { r.Branch = "feature/x" }),
		"branch dots":    mod(func(r *PullRequestRequest) { r.Branch = "piperoom/../main" }),
		"base sha":       mod(func(r *PullRequestRequest) { r.BaseSHA = "main" }),
		"no title":       mod(func(r *PullRequestRequest) { r.Title = "" }),
		"title newline":  mod(func(r *PullRequestRequest) { r.Title = "a\nb" }),
		"no message":     mod(func(r *PullRequestRequest) { r.CommitMessage = "" }),
		"no files":       mod(func(r *PullRequestRequest) { r.Files = nil }),
		"too many files": mod(func(r *PullRequestRequest) { r.Files = many }),
		"dup path":       mod(func(r *PullRequestRequest) { r.Files = append(r.Files, r.Files[0]) }),
		"workflow":       mod(func(r *PullRequestRequest) { r.Files[0].Path = ".github/workflows/ci.yml" }),
		"codeowners":     mod(func(r *PullRequestRequest) { r.Files[0].Path = "docs/CODEOWNERS" }),
		"traversal":      mod(func(r *PullRequestRequest) { r.Files[0].Path = "a/../b" }),
		"nul":            mod(func(r *PullRequestRequest) { r.Files[0].Content = []byte("a\x00b") }),
		"not utf8":       mod(func(r *PullRequestRequest) { r.Files[0].Content = []byte{0xff} }),
		"too large":      mod(func(r *PullRequestRequest) { r.Files[0].Content = make([]byte, MaxRepoFileBytes+1) }),
		"body large":     mod(func(r *PullRequestRequest) { r.Body = strings.Repeat("b", MaxFixParamsBytes+1) }),
	}
	for name, req := range cases {
		if _, err := w.OpenPullRequest(ctx10(t), tRepo, req); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := w.OpenPullRequest(ctx10(t), RepoRef{ID: "x", Owner: tOwner, Name: tName, Base: "piperoom/x"}, fixReq(base, "a", "b")); err == nil {
		t.Error("piperoom/ base accepted")
	}
	if n := len(g.requests()); n != 0 {
		t.Fatalf("%d requests sent for refused PRs", n)
	}
	if _, err := NewWriter(nil); err == nil {
		t.Fatal("nil client accepted")
	}
}

func TestOpenPullRequestUnderSymlinkRefused(t *testing.T) {
	files := baseFiles()
	files["conf"] = "elsewhere"
	g, base := newGH(t, files)
	g.special["conf"] = "symlink"
	w, _ := NewWriter(g.client(t))
	_, err := w.OpenPullRequest(ctx10(t), tRepo, fixReq(base, "conf/new/app.yml", "a: 1\n"))
	if !errors.Is(err, ErrNotRegularFile) {
		t.Fatalf("err = %v", err)
	}
	// An edited path that is itself a symlink is refused too.
	_, err = w.OpenPullRequest(ctx10(t), tRepo, fixReq(base, "conf", "x\n"))
	if !errors.Is(err, ErrNotRegularFile) {
		t.Fatalf("symlink edit: %v", err)
	}
	if g.count("POST", `/git/`) != 0 {
		t.Fatal("wrote under a symlink")
	}
}

func TestOpenPullRequestNoWriteToken(t *testing.T) {
	g, base := newGH(t, baseFiles())
	g.setHook(func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "access_tokens") && strings.Contains(r.URL.Path, "") {
			ghErr(w, 422, "The permissions requested are not granted to this installation.")
			return true
		}
		return false
	})
	w, _ := NewWriter(g.client(t))
	if _, err := w.OpenPullRequest(ctx10(t), tRepo, fixReq(base, "README.md", "x\n")); err == nil || !strings.Contains(err.Error(), "installation") {
		t.Fatalf("err = %v", err)
	}
}
