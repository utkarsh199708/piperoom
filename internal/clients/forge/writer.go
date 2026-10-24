package forge

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients/netsec"
)

// Limits of one fix PR the writer checks itself, behind the action's Validate.
const (
	maxTitleBytes = 256
	// cleanupTimeout bounds the delete of the writer's own new branch after
	// a failed PR create, on a fresh context.
	cleanupTimeout = 10 * time.Second
)

// Writer opens fix pull requests (ForgeWriter). Wiring builds it only with
// write mode on (docs/design/fix-pr.md §10); with write mode off no Writer
// exists. Safe for concurrent use.
type Writer struct{ c *Client }

var _ ForgeWriter = (*Writer)(nil) // clients.ForgeWriter (an alias)

// NewWriter returns the writer over c's App installation.
func NewWriter(c *Client) (*Writer, error) {
	if c == nil {
		return nil, errors.New("forge: NewWriter: nil client")
	}
	return &Writer{c: c}, nil
}

// String keeps a printed Writer free of the client's secrets.
func (w *Writer) String() string { return "forge.Writer{" + w.c.base.Host + "}" }

// GoString is String for %#v.
func (w *Writer) GoString() string { return w.String() }

// checkRequest refuses a request the writer must never send, before any I/O.
func checkRequest(repo RepoRef, req PullRequestRequest) error {
	bad := func(f string, a ...any) error { return fmt.Errorf("%w: %s", ErrRefused, fmt.Sprintf(f, a...)) }
	if err := checkRepo(repo); err != nil {
		return err
	}
	if !ValidBranchName(repo.Base) || strings.HasPrefix(repo.Base, FixBranchPrefix) {
		return bad("the base branch is not a valid branch name outside %s", FixBranchPrefix)
	}
	if err := checkSHA(req.BaseSHA); err != nil {
		return err
	}
	if !strings.HasPrefix(req.Branch, FixBranchPrefix) || !ValidBranchName(req.Branch) || req.Branch == repo.Base {
		return bad("the fix branch must be a valid %s<room>-<proposal>-<step> name", FixBranchPrefix)
	}
	if t := req.Title; t == "" || len(t) > maxTitleBytes || !utf8.ValidString(t) || strings.IndexFunc(t, unicode.IsControl) >= 0 {
		return bad("the title must be one line of 1 to %d bytes", maxTitleBytes)
	}
	if len(req.Body) > MaxFixParamsBytes || !utf8.ValidString(req.Body) {
		return bad("the body must be valid UTF-8 of at most %d bytes", MaxFixParamsBytes)
	}
	if req.CommitMessage == "" || len(req.CommitMessage) > MaxFixParamsBytes || !utf8.ValidString(req.CommitMessage) {
		return bad("the commit message must be valid UTF-8 of 1 to %d bytes", MaxFixParamsBytes)
	}
	if len(req.Files) == 0 || len(req.Files) > MaxFixFiles {
		return bad("a fix PR changes 1 to %d files, not %d", MaxFixFiles, len(req.Files))
	}
	seen := map[string]bool{}
	for i, f := range req.Files {
		if err := CheckRepoPath(f.Path, false); err != nil {
			return fmt.Errorf("forge: file %d: %w", i, err)
		}
		if seen[f.Path] {
			return bad("file %d: path listed twice", i)
		}
		seen[f.Path] = true
		if len(f.Content) > MaxRepoFileBytes {
			return fmt.Errorf("forge: file %d: %w: %d bytes, at most %d", i, ErrRepoFileTooLarge, len(f.Content), MaxRepoFileBytes)
		}
		// Text only (design §6.2 rule 5): the action checks it first; the
		// writer refuses anything else too.
		if !utf8.Valid(f.Content) || bytes.ContainsFunc(f.Content, func(r rune) bool {
			return unicode.IsControl(r) && r != '\t' && r != '\n' && r != '\r'
		}) {
			return bad("file %d: content must be UTF-8 text without control characters", i)
		}
	}
	return nil
}

// OpenPullRequest commits req.Files on top of req.BaseSHA to the new branch
// req.Branch and opens a pull request into repo.Base. The requests it sends,
// in order: the base head; per file its blob at BaseSHA and at the head (and,
// for a new file, its parent directories at BaseSHA); the BaseSHA commit; one
// blob per file; one tree; the branch ref; then either a commit, the new
// ref and the PR, or (a retried call) the open PRs of the branch and the PR.
func (w *Writer) OpenPullRequest(ctx context.Context, repo RepoRef, req PullRequestRequest) (PullRequest, error) {
	if err := checkRequest(repo, req); err != nil {
		return PullRequest{}, err
	}
	c := w.c
	tok, err := c.token(ctx, repo, scopeWrite)
	if err != nil {
		return PullRequest{}, err
	}
	head, err := c.branchHead(ctx, repo, repo.Base, scopeWrite)
	if err != nil {
		return PullRequest{}, fmt.Errorf("forge: reading base %s of %s: %w", repo.Base, repo.ID, err)
	}
	existing, err := w.checkMoved(ctx, repo, req, head)
	if err != nil {
		return PullRequest{}, err
	}

	var base struct {
		Tree struct {
			SHA string `json:"sha"`
		} `json:"tree"`
	}
	if err := c.do(ctx, tok, request{ep: epCommit, method: http.MethodGet, path: repoPath(repo) + "/git/commits/" + req.BaseSHA}, &base); err != nil {
		return PullRequest{}, fmt.Errorf("forge: reading base commit of %s: %w", repo.ID, err)
	}
	if checkSHA(base.Tree.SHA) != nil {
		return PullRequest{}, fmt.Errorf("forge: base commit of %s has no tree", repo.ID)
	}
	modes, err := w.fileModes(ctx, tok, repo, base.Tree.SHA, existing)
	if err != nil {
		return PullRequest{}, err
	}
	tree, err := w.writeTree(ctx, tok, repo, base.Tree.SHA, req.Files, modes)
	if err != nil {
		return PullRequest{}, err
	}

	branchSHA, err := c.branchHead(ctx, repo, req.Branch, scopeWrite)
	switch {
	case err == nil:
		return w.reuse(ctx, tok, repo, req, branchSHA, tree)
	case !isNotFound(err):
		return PullRequest{}, fmt.Errorf("forge: checking branch %s of %s: %w", req.Branch, repo.ID, err)
	}

	var commit struct {
		SHA string `json:"sha"`
	}
	cbody := map[string]any{"message": req.CommitMessage, "tree": tree, "parents": []string{req.BaseSHA}}
	if err := c.do(ctx, tok, request{ep: epNewCommit, method: http.MethodPost, path: repoPath(repo) + "/git/commits", body: cbody, accept: []int{http.StatusCreated}}, &commit); err != nil {
		return PullRequest{}, fmt.Errorf("forge: creating the commit in %s: %w", repo.ID, err)
	}
	if checkSHA(commit.SHA) != nil {
		return PullRequest{}, fmt.Errorf("forge: commit answer of %s has no sha", repo.ID)
	}
	rbody := map[string]string{"ref": "refs/heads/" + req.Branch, "sha": commit.SHA}
	if err := c.do(ctx, tok, request{ep: epNewRef, method: http.MethodPost, path: repoPath(repo) + "/git/refs", body: rbody, accept: []int{http.StatusCreated}}, nil); err != nil {
		var se *StatusError
		if errors.As(err, &se) && se.Code == http.StatusUnprocessableEntity {
			// Created by a concurrent call between the check and now; a retry
			// reuses it when it holds this change.
			return PullRequest{}, fmt.Errorf("forge: branch %s of %s appeared while opening the PR: %w: %w", req.Branch, repo.ID, ErrBranchExists, err)
		}
		return PullRequest{}, fmt.Errorf("forge: creating branch %s in %s: %w", req.Branch, repo.ID, err)
	}
	pr, err := w.createPull(ctx, tok, repo, req)
	if err != nil {
		// Delete only the branch this call created, on a fresh context: the
		// caller's may be what failed.
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		del := request{ep: epDeleteOwnRef, method: http.MethodDelete, path: repoPath(repo) + "/git/refs/heads/" + escapePath(req.Branch), accept: []int{http.StatusNoContent}}
		if derr := c.do(dctx, tok, del, nil); derr != nil {
			return PullRequest{}, fmt.Errorf("forge: opening the PR in %s failed and branch %s is left: %w", repo.ID, req.Branch, errors.Join(err, derr))
		}
		return PullRequest{}, fmt.Errorf("forge: opening the PR in %s failed; branch %s was deleted: %w", repo.ID, req.Branch, err)
	}
	return pr, nil
}

// blobAt returns the blob id of p at sha, "" when p is absent there.
func (w *Writer) blobAt(ctx context.Context, repo RepoRef, sha, p string) (string, error) {
	ans, dir, err := w.c.contents(ctx, repo, sha, p, scopeWrite)
	switch {
	case isNotFound(err):
		return "", nil
	case err != nil:
		return "", err
	case dir:
		return "", fmt.Errorf("%w: %s is a directory", ErrNotRegularFile, p)
	}
	if err := regular(ans, p); err != nil {
		return "", err
	}
	return ans.SHA, nil
}

// checkMoved returns ErrBaseMoved naming every file of req that differs
// between req.BaseSHA and head. A new file's parent directories must be
// directories (or absent) at BaseSHA: a tree entry under a symlink or a
// submodule is never written. It returns the paths of req that exist at
// BaseSHA.
func (w *Writer) checkMoved(ctx context.Context, repo RepoRef, req PullRequestRequest, head string) ([]string, error) {
	var moved, existing []string
	dirs := map[string]bool{}
	for _, f := range req.Files {
		at, err := w.blobAt(ctx, repo, req.BaseSHA, f.Path)
		if err != nil {
			return nil, fmt.Errorf("forge: reading %s at the base commit: %w", f.Path, err)
		}
		if at != "" {
			existing = append(existing, f.Path)
		}
		if at == "" {
			for d := path.Dir(f.Path); d != "."; d = path.Dir(d) {
				dirs[d] = true
			}
		}
		if head == req.BaseSHA {
			continue
		}
		now, err := w.blobAt(ctx, repo, head, f.Path)
		if err != nil {
			return nil, fmt.Errorf("forge: reading %s at the base head: %w", f.Path, err)
		}
		if now != at {
			moved = append(moved, f.Path)
		}
	}
	if len(moved) > 0 {
		return nil, fmt.Errorf("%w: %s changed on %s since %.7s; nothing was written", ErrBaseMoved, strings.Join(moved, ", "), repo.Base, req.BaseSHA)
	}
	ds := make([]string, 0, len(dirs))
	for d := range dirs {
		ds = append(ds, d)
	}
	sort.Strings(ds) // parents first
	absent := map[string]bool{}
	for _, d := range ds {
		if absent[path.Dir(d)] {
			absent[d] = true
			continue
		}
		_, dir, err := w.c.contents(ctx, repo, req.BaseSHA, d, scopeWrite)
		switch {
		case isNotFound(err):
			absent[d] = true
		case err != nil:
			return nil, fmt.Errorf("forge: reading directory %s at the base commit: %w", d, err)
		case !dir:
			return nil, fmt.Errorf("%w: %s is not a directory at the base commit", ErrNotRegularFile, d)
		}
	}
	return existing, nil
}

// Git file modes the writer keeps or writes.
const (
	modeFile = "100644"
	modeExec = "100755"
)

// treeEntry is one entry of a non-recursive tree listing.
type treeEntry struct {
	Path string `json:"path"`
	Mode string `json:"mode"`
	Type string `json:"type"`
	SHA  string `json:"sha"`
}

// fileModes returns the git mode of each existing path in the tree rootTree
// (the base commit's), walking one directory listing per level: a fix keeps
// an executable file executable (100755) and every other file 100644. The
// contents API does not report the mode, and a recursive listing can be cut
// on a large repository, so the walk reads only the directories on the
// paths. A path whose entry is missing, or not a regular file, is refused:
// the writer never guesses a mode.
func (w *Writer) fileModes(ctx context.Context, tok netsec.Secret, repo RepoRef, rootTree string, paths []string) (map[string]string, error) {
	modes := make(map[string]string, len(paths))
	listings := map[string][]treeEntry{}
	list := func(sha string) ([]treeEntry, error) {
		if l, ok := listings[sha]; ok {
			return l, nil
		}
		if checkSHA(sha) != nil {
			return nil, fmt.Errorf("forge: a tree of %s has no valid id", repo.ID)
		}
		var out struct {
			Tree      []treeEntry `json:"tree"`
			Truncated bool        `json:"truncated"`
		}
		if err := w.c.do(ctx, tok, request{ep: epTree, method: http.MethodGet, path: repoPath(repo) + "/git/trees/" + sha}, &out); err != nil {
			return nil, fmt.Errorf("forge: reading a tree of %s: %w", repo.ID, err)
		}
		if out.Truncated {
			return nil, fmt.Errorf("forge: a directory listing of %s was cut; the file modes cannot be read", repo.ID)
		}
		listings[sha] = out.Tree
		return out.Tree, nil
	}
	for _, p := range paths {
		cur := rootTree
		segs := strings.Split(p, "/")
		for i, seg := range segs {
			entries, err := list(cur)
			if err != nil {
				return nil, err
			}
			var e *treeEntry
			for j := range entries {
				if entries[j].Path == seg {
					e = &entries[j]
					break
				}
			}
			if e == nil {
				return nil, fmt.Errorf("forge: %s is not in the base tree of %s", p, repo.ID)
			}
			if i < len(segs)-1 {
				if e.Type != "tree" {
					return nil, fmt.Errorf("%w: a parent of %s is not a directory", ErrNotRegularFile, p)
				}
				cur = e.SHA
				continue
			}
			if e.Type != "blob" || (e.Mode != modeFile && e.Mode != modeExec) {
				return nil, fmt.Errorf("%w: %s", ErrNotRegularFile, p)
			}
			modes[p] = e.Mode
		}
	}
	return modes, nil
}

// writeTree creates one blob per file and one tree on baseTree. Blobs and
// trees are content-addressed objects no ref points at yet.
// modes holds the mode of each file that exists in the base tree (fileModes);
// a new file is 100644.
func (w *Writer) writeTree(ctx context.Context, tok netsec.Secret, repo RepoRef, baseTree string, files []FileChange, modes map[string]string) (string, error) {
	c := w.c
	entries := make([]map[string]string, 0, len(files))
	for _, f := range files {
		var blob struct {
			SHA string `json:"sha"`
		}
		body := map[string]string{"content": base64.StdEncoding.EncodeToString(f.Content), "encoding": "base64"}
		if err := c.do(ctx, tok, request{ep: epNewBlob, method: http.MethodPost, path: repoPath(repo) + "/git/blobs", body: body, accept: []int{http.StatusCreated}}, &blob); err != nil {
			return "", fmt.Errorf("forge: creating the blob of %s in %s: %w", f.Path, repo.ID, err)
		}
		if blob.SHA != BlobSHA(f.Content) {
			return "", fmt.Errorf("forge: the blob GitHub stored for %s is not the content sent", f.Path)
		}
		mode := modeFile
		if modes[f.Path] == modeExec {
			mode = modeExec
		}
		entries = append(entries, map[string]string{"path": f.Path, "mode": mode, "type": "blob", "sha": blob.SHA})
	}
	var tree struct {
		SHA string `json:"sha"`
	}
	body := map[string]any{"base_tree": baseTree, "tree": entries}
	if err := c.do(ctx, tok, request{ep: epNewTree, method: http.MethodPost, path: repoPath(repo) + "/git/trees", body: body, accept: []int{http.StatusCreated}}, &tree); err != nil {
		return "", fmt.Errorf("forge: creating the tree in %s: %w", repo.ID, err)
	}
	if checkSHA(tree.SHA) != nil {
		return "", fmt.Errorf("forge: tree answer of %s has no sha", repo.ID)
	}
	return tree.SHA, nil
}

// reuse handles a branch that exists already. It is reused only when its
// head commit is exactly this change (parent BaseSHA, the same tree) and is
// one the App wrote: author and committer are the App's bot and GitHub
// verified the commit (a commit made through the API without an author is
// signed by GitHub; one that only names the bot is not). An open PR on it is
// returned only when the App opened it. Anyone else's branch or PR, even
// with the same content, is ErrBranchExists: its title, body and later
// pushes are not Piperoom's.
func (w *Writer) reuse(ctx context.Context, tok netsec.Secret, repo RepoRef, req PullRequestRequest, headSHA, tree string) (PullRequest, error) {
	c := w.c
	var cm struct {
		Tree struct {
			SHA string `json:"sha"`
		} `json:"tree"`
		Parents []struct {
			SHA string `json:"sha"`
		} `json:"parents"`
		Author struct {
			Name string `json:"name"`
		} `json:"author"`
		Committer struct {
			Name string `json:"name"`
		} `json:"committer"`
		Verification struct {
			Verified bool `json:"verified"`
		} `json:"verification"`
	}
	if err := c.do(ctx, tok, request{ep: epCommit, method: http.MethodGet, path: repoPath(repo) + "/git/commits/" + headSHA}, &cm); err != nil {
		return PullRequest{}, fmt.Errorf("forge: reading branch %s of %s: %w", req.Branch, repo.ID, err)
	}
	if len(cm.Parents) != 1 || cm.Parents[0].SHA != req.BaseSHA || cm.Tree.SHA != tree {
		return PullRequest{}, fmt.Errorf("%w: %s in %s", ErrBranchExists, req.Branch, repo.ID)
	}
	bot, err := c.botLogin(ctx)
	if err != nil {
		return PullRequest{}, err
	}
	if cm.Author.Name != bot || cm.Committer.Name != bot || !cm.Verification.Verified {
		return PullRequest{}, fmt.Errorf("%w: %s in %s holds a commit the App did not write", ErrBranchExists, req.Branch, repo.ID)
	}
	var open []pullAnswer
	q := url.Values{"head": {repo.Owner + ":" + req.Branch}, "base": {repo.Base}, "state": {"open"}}
	if err := c.do(ctx, tok, request{ep: epPulls, method: http.MethodGet, path: repoPath(repo) + "/pulls", query: q}, &open); err != nil {
		return PullRequest{}, fmt.Errorf("forge: listing the PRs of %s in %s: %w", req.Branch, repo.ID, err)
	}
	for _, a := range open {
		if a.Head.Ref == req.Branch {
			if a.User.Login != bot || a.User.Type != "Bot" || a.Head.SHA != headSHA ||
				a.Head.Repo == nil || !strings.EqualFold(a.Head.Repo.FullName, repo.Owner+"/"+repo.Name) {
				return PullRequest{}, fmt.Errorf("%w: an open pull request on %s in %s was not opened by the App", ErrBranchExists, req.Branch, repo.ID)
			}
			pr, err := c.pull(repo, a)
			if err != nil {
				return PullRequest{}, err
			}
			pr.Checks = PullRequestChecks{State: ChecksNone}
			return pr, nil
		}
	}
	// The branch holds the change but its PR failed to open (or was closed):
	// open it now. The branch is not this call's, so it is never deleted.
	return w.createPull(ctx, tok, repo, req)
}

func (w *Writer) createPull(ctx context.Context, tok netsec.Secret, repo RepoRef, req PullRequestRequest) (PullRequest, error) {
	var a pullAnswer
	body := map[string]any{"title": req.Title, "body": req.Body, "head": req.Branch, "base": repo.Base, "draft": req.Draft}
	if err := w.c.do(ctx, tok, request{ep: epNewPull, method: http.MethodPost, path: repoPath(repo) + "/pulls", body: body, accept: []int{http.StatusCreated}}, &a); err != nil {
		return PullRequest{}, err
	}
	pr, err := w.c.pull(repo, a)
	if err != nil {
		return PullRequest{}, err
	}
	// A new PR's checks have not started; the follower reads them (Q7e).
	pr.Checks = PullRequestChecks{State: ChecksNone}
	return pr, nil
}
