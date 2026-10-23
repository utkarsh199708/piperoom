package forge

import (
	"errors"
	"strings"
	"testing"

	"piperoom/internal/owners"
)

func TestFixBranch(t *testing.T) {
	b := FixBranch("r_01JABC", "p_01KDEF", 2)
	if b != "piperoom/r_01JABC-p_01KDEF-2" || !ValidBranchName(b) {
		t.Fatalf("FixBranch = %q", b)
	}
}

func TestValidBranchName(t *testing.T) {
	ok := []string{"main", "release/1.2", "feature_x-y", "a.b", "v1"}
	bad := []string{"", "-x", "/x", "x/", "x.", "a..b", "a//b", "a@{1}", ".hidden", "a/.b", "x.lock", "a/b.lock/c",
		"sp ace", "a~1", "a^", "a:b", "a\\b", strings.Repeat("a", 201), "ü"}
	for _, s := range ok {
		if !ValidBranchName(s) {
			t.Errorf("%q refused", s)
		}
	}
	for _, s := range bad {
		if ValidBranchName(s) {
			t.Errorf("%q accepted", s)
		}
	}
}

func TestCheckRepoPath(t *testing.T) {
	cases := []struct {
		p    string
		glob bool
		ok   bool
	}{
		{"docker/connect/Dockerfile", false, true},
		{"connectors/orders-pg.json", false, true},
		{"src/main/resources/application.yml", false, true},
		{"github/config.yml", false, true},
		{"docs/github-actions.md", false, true},
		{"", false, false},
		{"/etc/passwd", false, false},
		{"a/", false, false},
		{"a//b", false, false},
		{"./a", false, false},
		{"a/./b", false, false},
		{"a/../b", false, false},
		{"..", false, false},
		{"a\\b", false, false},
		{"a\x00b", false, false},
		{"a\nb", false, false},
		{"a\x7fb", false, false},
		{string([]byte{0xff}), false, false},
		{strings.Repeat("a", MaxRepoPathBytes+1), false, false},
		{strings.Repeat("a", MaxRepoPathBytes), false, true},
		{".github/workflows/ci.yml", false, false},
		{".GitHub/workflows/ci.yml", false, false},
		{"sub/.git/config", false, false},
		{".gitlab/ci.yml", false, false},
		{".circleci/config.yml", false, false},
		{".husky/pre-commit", false, false},
		{".devcontainer/devcontainer.json", false, false},
		{".github", false, false},
		{"CODEOWNERS", false, false},
		{"docs/codeowners", false, false},
		{".gitmodules", false, false},
		{".gitattributes", false, false},
		{"x/.gitignore", false, false},
		{".gitlab-ci.yml", false, false},
		{".travis.yml", false, false},
		{".drone.yml", false, false},
		{"ci/Jenkinsfile", false, false},
		{"azure-pipelines.yml", false, false},
		{"bitbucket-pipelines.yml", false, false},
		{".pre-commit-config.yaml", false, false},
		{"connectors/orders-*.json", true, true},
		{"src/**/application*.yml", true, true},
		{"**", true, true},
		{"conf/[ab].yml", true, true},
		{"conf/[.yml", true, false},
		{"conf/a**b", true, false},
		{".github/**", true, false},
		{"**/CODEOWNERS", true, false},
		{"a/../*.json", true, false},
	}
	for _, c := range cases {
		err := CheckRepoPath(c.p, c.glob)
		if (err == nil) != c.ok {
			t.Errorf("CheckRepoPath(%q, %v) = %v, want ok %v", c.p, c.glob, err, c.ok)
		}
		if err != nil && !errors.Is(err, ErrRepoPathDenied) {
			t.Errorf("%q: error does not wrap ErrRepoPathDenied: %v", c.p, err)
		}
	}
}

func TestBlobSHA(t *testing.T) {
	// git hash-object of "hello\n" and of the empty blob.
	if got := BlobSHA([]byte("hello\n")); got != "ce013625030ba8dba906f756967f9e9ca394464a" {
		t.Fatalf("BlobSHA = %s", got)
	}
	if got := BlobSHA(nil); got != "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391" {
		t.Fatalf("empty = %s", got)
	}
}

func TestFileChangeString(t *testing.T) {
	if s := (FileChange{Path: "a", Content: []byte("secret=1")}).String(); strings.Contains(s, "secret") {
		t.Fatalf("content printed: %s", s)
	}
}

// The client and owners.yaml apply one path rule set: the same inputs give
// the same answer from forge.CheckRepoPath and owners.CheckRepoPath, and a
// refusal from either matches both packages' ErrRepoPathDenied. Before they
// were two sets, and the client let through .git* files and glob characters
// that owners.yaml refuses.
func TestCheckRepoPathAgreesWithOwners(t *testing.T) {
	cases := []struct {
		p    string
		glob bool
		ok   bool
	}{
		{"connectors/.gitkeep", false, false},
		{"connectors/.github-x.json", false, false},
		{"connectors/a*b.json", false, false},
		{"connectors/a?b.json", false, false},
		{"connectors/[ab].json", false, false},
		{".gitkeep", false, false},
		{"connectors/orders-pg.json", false, true},
		{"docker/connect/Dockerfile", false, true},
		{"src/main/resources/application.yml", false, true},
		{"docs/github-actions.md", false, true},
		{"connectors/*.json", true, true},
		{"src/**/application*.yml", true, true},
		{"connectors/.git*", true, false},
	}
	for _, c := range cases {
		ferr, oerr := CheckRepoPath(c.p, c.glob), owners.CheckRepoPath(c.p, c.glob)
		if (ferr == nil) != c.ok || (oerr == nil) != c.ok {
			t.Errorf("%q (glob %v): forge %v, owners %v, want ok %v", c.p, c.glob, ferr, oerr, c.ok)
		}
		if ferr != nil && (!errors.Is(ferr, ErrRepoPathDenied) || !errors.Is(ferr, owners.ErrRepoPathDenied)) {
			t.Errorf("%q: forge error does not wrap ErrRepoPathDenied: %v", c.p, ferr)
		}
	}
}
