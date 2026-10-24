package clients_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// Q7 (Q0 item 6): the pull-request fix's contract helpers.

func TestFixBranch(t *testing.T) {
	cases := []struct {
		room, proposal string
		step           int
		want           string
	}{
		{"r_01J", "p_01K", 0, "piperoom/r_01J-p_01K-0"},
		{"r_01J", "p_01K", 2, "piperoom/r_01J-p_01K-2"},
		{"r_a", "p_b", 10, "piperoom/r_a-p_b-10"},
	}
	for _, c := range cases {
		got := clients.FixBranch(c.room, c.proposal, c.step)
		if got != c.want {
			t.Errorf("FixBranch(%q, %q, %d) = %q, want %q", c.room, c.proposal, c.step, got, c.want)
		}
		if !strings.HasPrefix(got, clients.FixBranchPrefix) {
			t.Errorf("%q does not start with FixBranchPrefix", got)
		}
		// Room ids are ULIDs with a prefix: the branch is a valid ref name.
		if !clients.ValidBranchName(got) {
			t.Errorf("FixBranch result %q fails ValidBranchName", got)
		}
	}
}

func TestValidBranchName(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"main", true},
		{"master", true},
		{"release/2026.10", true},
		{"feature/a-b_c.d", true},
		{"v1.2.3", true},
		{strings.Repeat("a", 200), true},
		{"", false},
		{strings.Repeat("a", 201), false},
		{"has space", false},
		{"semi;colon", false},
		{"tilde~1", false},
		{"caret^", false},
		{"colon:x", false},
		{"q?", false},
		{"star*", false},
		{"br[ack", false},
		{"back\\slash", false},
		{"at@{1}", false},
		{"a..b", false},
		{"a//b", false},
		{"-leading-dash", false},
		{"/leading-slash", false},
		{"trailing-slash/", false},
		{"trailing-dot.", false},
		{".hidden", false},
		{"a/.hidden", false},
		{"x.lock", false},
		{"a/x.lock/b", false},
		{"x.lockfile", true},
		{"nul\x00", false},
		{"ünïcode", false},
	}
	for _, c := range cases {
		if got := clients.ValidBranchName(c.name); got != c.want {
			t.Errorf("ValidBranchName(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestCheckRepoPath(t *testing.T) {
	cases := []struct {
		path string
		glob bool
		ok   bool
	}{
		// Paths a fix PR may write.
		{"docker/connect/Dockerfile", false, true},
		{"connectors/orders-pg.json", false, true},
		{"README.md", false, true},
		{"deploy/helm/values.yaml", false, true},
		{"docs/github/notes.md", false, true},   // "github" without the dot is not .github
		{"src/codeowners_test.go", false, true}, // only the exact name CODEOWNERS is denied
		{"config/gitignore.txt", false, true},   // likewise .gitignore
		{strings.Repeat("a", clients.MaxRepoPathBytes), false, true},
		// Shape.
		{"", false, false},
		{strings.Repeat("a", clients.MaxRepoPathBytes+1), false, false},
		{"/etc/passwd", false, false},
		{"a\\b", false, false},
		{"a/../b", false, false},
		{"../b", false, false},
		{"./a", false, false},
		{"a/.", false, false},
		{".", false, false},
		{"..", false, false},
		{"a//b", false, false},
		{"a/", false, false},
		{"a\x00b", false, false},
		{"a\nb", false, false},
		{"a\tb", false, false},
		{"a\x7fb", false, false},
		{"a\u0085b", false, false}, // a C1 control character
		{"bad\xffutf8", false, false},
		// Deny list: directories, any depth, any case.
		{".github/workflows/ci.yml", false, false},
		{".GitHub/workflows/ci.yml", false, false},
		{"sub/.github/x.yml", false, false},
		{".git/config", false, false},
		{".gitlab/ci.yml", false, false},
		{".circleci/config.yml", false, false},
		{".buildkite/pipeline.yml", false, false},
		{".azure-pipelines/x.yml", false, false},
		{".woodpecker/x.yml", false, false},
		{".husky/pre-commit", false, false},
		{".devcontainer/devcontainer.json", false, false},
		{".github", false, false},
		// Deny list: file names, anywhere, any case.
		{"CODEOWNERS", false, false},
		{"docs/CODEOWNERS", false, false},
		{"docs/codeowners", false, false},
		{".gitmodules", false, false},
		{".gitattributes", false, false},
		{"sub/.gitignore", false, false},
		{".gitlab-ci.yml", false, false},
		{".travis.yml", false, false},
		{".drone.yml", false, false},
		{"Jenkinsfile", false, false},
		{"ci/jenkinsfile", false, false},
		{"azure-pipelines.yml", false, false},
		{"bitbucket-pipelines.yml", false, false},
		{".pre-commit-config.yaml", false, false},
		// A wildcard is a literal character in a path, never a pattern.
		{"weird*name.txt", false, true},
		// Globs.
		{"connectors/*.json", true, true},
		{"docker/**", true, true},
		{"**/*.yaml", true, true},
		{"*", true, true},
		{"deploy/?/values.yaml", true, true},
		{"deploy/[ab]/values.yaml", true, true},
		{".github/**", true, false},
		{".GITHUB/*.yml", true, false},
		{"sub/.git/**", true, false},
		{"docs/CODEOWNERS", true, false},
		{".git*", true, false},        // spelled to reach .github, .gitignore, ...
		{"[.]github/**", true, false}, // likewise
		{"?github/x", true, false},
		{"docs/Jenkins?ile", true, false},
		{"deploy/[a", true, false}, // malformed
		{"../**", true, false},
		{"a/./b/*", true, false},
		{"/abs/*", true, false},
	}
	for _, c := range cases {
		err := clients.CheckRepoPath(c.path, c.glob)
		if (err == nil) != c.ok {
			t.Errorf("CheckRepoPath(%q, glob=%v) = %v, want ok=%v", c.path, c.glob, err, c.ok)
			continue
		}
		if err != nil && !errors.Is(err, clients.ErrRepoPathDenied) {
			t.Errorf("CheckRepoPath(%q, glob=%v) error %v does not wrap ErrRepoPathDenied", c.path, c.glob, err)
		}
	}
}

// The deny list is the leads' (a contract change): every listed name is
// refused as a path, so a shortened list fails here.
func TestCheckRepoPathDenyListPinned(t *testing.T) {
	dirs := []string{".github", ".git", ".gitlab", ".circleci", ".buildkite", ".azure-pipelines", ".woodpecker", ".husky", ".devcontainer"}
	files := []string{"CODEOWNERS", ".gitmodules", ".gitattributes", ".gitignore", ".gitlab-ci.yml", ".travis.yml", ".drone.yml",
		"Jenkinsfile", "azure-pipelines.yml", "bitbucket-pipelines.yml", ".pre-commit-config.yaml"}
	for _, d := range dirs {
		for _, p := range []string{d + "/x", "a/" + d + "/x", strings.ToUpper(d) + "/x"} {
			if err := clients.CheckRepoPath(p, false); !errors.Is(err, clients.ErrRepoPathDenied) {
				t.Errorf("CheckRepoPath(%q) = %v, want ErrRepoPathDenied", p, err)
			}
		}
	}
	for _, f := range files {
		for _, p := range []string{f, "a/b/" + f, strings.ToLower(f)} {
			if err := clients.CheckRepoPath(p, false); !errors.Is(err, clients.ErrRepoPathDenied) {
				t.Errorf("CheckRepoPath(%q) = %v, want ErrRepoPathDenied", p, err)
			}
		}
	}
}

// The PR wire shapes: merged_at disappears when zero, failed when empty.
func TestPullRequestJSON(t *testing.T) {
	pr := clients.PullRequest{Repo: "connect-config", Number: 12, URL: "https://github.com/acme/connect-config/pull/12",
		State: clients.PRStateOpen, HeadRef: "piperoom/r_1-p_1-0", HeadSHA: "abc", Checks: clients.PullRequestChecks{State: clients.ChecksPending, Total: 2}}
	b, err := json.Marshal(pr)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"repo":"connect-config","number":12,"url":"https://github.com/acme/connect-config/pull/12","state":"open","head_ref":"piperoom/r_1-p_1-0","head_sha":"abc","checks":{"state":"pending","total":2}}`
	if string(b) != want {
		t.Fatalf("PullRequest JSON\n got %s\nwant %s", b, want)
	}
	pr.State, pr.MergeSHA, pr.MergedAt = clients.PRStateMerged, "def", time.Date(2026, 10, 23, 14, 2, 0, 0, time.UTC)
	b, _ = json.Marshal(pr)
	if !strings.Contains(string(b), `"merge_sha":"def","merged_at":"2026-10-23T14:02:00Z"`) {
		t.Fatalf("merged PR JSON lacks merge fields: %s", b)
	}
	// RepoFile content never marshals.
	f, _ := json.Marshal(clients.RepoFile{Path: "a", Content: []byte("secret-ish")})
	if strings.Contains(string(f), "secret-ish") {
		t.Fatalf("RepoFile content marshalled: %s", f)
	}
}

func TestFixCaps(t *testing.T) {
	if clients.MaxRepoFileBytes != 64<<10 || clients.MaxFixFiles != 10 || clients.MaxFixParamsBytes != 64<<10 ||
		clients.MaxFixDiffBytes != 128<<10 || clients.MaxRepoListEntries != 1000 || clients.MaxRepoPathBytes != 255 {
		t.Fatal("Q7 caps changed: they are the design's (docs/design/fix-pr.md §5.1)")
	}
	if clients.ChangePullRequest != "pull_request" || clients.IsExternalChange(clients.ChangePullRequest) {
		t.Fatal("ChangePullRequest must be \"pull_request\" and not external")
	}
}
