package fake

import (
	"context"
	"errors"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/forge"
)

var ref = forge.RepoRef{ID: "connect-config", Owner: "acme", Name: "connect-config", Base: "main"}

func req(base, content string) forge.PullRequestRequest {
	return forge.PullRequestRequest{BaseSHA: base, Branch: forge.FixBranch("r_1", "p_1", 0), Title: "t", CommitMessage: "m",
		Files: []forge.FileChange{{Path: "docker/Dockerfile", Content: []byte(content)}}}
}

func TestFakeForge(t *testing.T) {
	ctx := context.Background()
	f := New()
	base := f.AddRepo(ref, forge.RepoInfo{Visibility: "private", DefaultBranch: "main"}, map[string]string{"docker/Dockerfile": "FROM a\n", "README.md": "r"})
	f.AddSpecial(ref, "README.md", "symlink")

	if head, err := f.BaseHead(ctx, ref); err != nil || head != base {
		t.Fatalf("head %s %v", head, err)
	}
	file, err := f.File(ctx, ref, base, "docker/Dockerfile")
	if err != nil || string(file.Content) != "FROM a\n" || file.BlobSHA != forge.BlobSHA([]byte("FROM a\n")) {
		t.Fatalf("file %+v %v", file, err)
	}
	if _, err := f.File(ctx, ref, base, "docker"); !errors.Is(err, forge.ErrNotRegularFile) {
		t.Fatalf("dir: %v", err)
	}
	if _, err := f.File(ctx, ref, base, "README.md"); !errors.Is(err, forge.ErrNotRegularFile) {
		t.Fatalf("symlink: %v", err)
	}
	if _, err := f.File(ctx, ref, base, "nope"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if es, _, _ := f.ListFiles(ctx, ref, base); len(es) != 1 || es[0].Path != "docker/Dockerfile" {
		t.Fatalf("list %+v", es)
	}

	pr, err := f.OpenPullRequest(ctx, ref, req(base, "FROM b\n"))
	if err != nil || pr.Number != 1 || pr.State != forge.PRStateOpen {
		t.Fatalf("open %+v %v", pr, err)
	}
	if again, err := f.OpenPullRequest(ctx, ref, req(base, "FROM b\n")); err != nil || again.Number != 1 {
		t.Fatalf("retry %+v %v", again, err)
	}
	if _, err := f.OpenPullRequest(ctx, ref, req(base, "FROM c\n")); !errors.Is(err, forge.ErrBranchExists) {
		t.Fatalf("other content: %v", err)
	}
	if got, _ := f.FileAt(ref, "main", "docker/Dockerfile"); got != "FROM a\n" {
		t.Fatal("base changed by a PR")
	}
	if err := f.PushToPull(ref, 1, map[string]string{"x": "y"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.PullRequest(ctx, ref, 1); got.HeadSHA == pr.HeadSHA {
		t.Fatal("push did not move the head")
	}
	if err := f.Merge(ref, 1); err != nil {
		t.Fatal(err)
	}
	got, _ := f.PullRequest(ctx, ref, 1)
	if got.State != forge.PRStateMerged || got.MergeSHA == "" || got.MergedAt.IsZero() {
		t.Fatalf("merged %+v", got)
	}
	if c, _ := f.FileAt(ref, "main", "docker/Dockerfile"); c != "FROM b\n" {
		t.Fatalf("merge content %q", c)
	}
	// The base moved on the touched file since base.
	r2 := req(base, "FROM d\n")
	r2.Branch = forge.FixBranch("r_1", "p_2", 0)
	if _, err := f.OpenPullRequest(ctx, ref, r2); !errors.Is(err, forge.ErrBaseMoved) {
		t.Fatalf("moved: %v", err)
	}
	if err := f.Close(ref, 9); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("close missing: %v", err)
	}
	if len(f.Opened()) != 1 {
		t.Fatalf("opened %d", len(f.Opened()))
	}
	r3 := req(base, "x")
	r3.Branch = "main"
	if _, err := f.OpenPullRequest(ctx, ref, r3); err == nil {
		t.Fatal("non-fix branch accepted")
	}
	f.SetErr(errors.New("down"))
	if _, err := f.RepoInfo(ctx, ref); err == nil {
		t.Fatal("SetErr ignored")
	}
}
