// Package fake is an in-memory forge (forge.ForgeReader and
// forge.ForgeWriter) with repositories, commits, branches and pull requests,
// for the demo, the eval replay and the room, tool and agent tests. Its
// writer keeps the real one's rules: a new piperoom/ branch from BaseSHA,
// ErrBaseMoved per touched file, idempotent retries, ErrBranchExists.
package fake

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/forge"
)

type commit struct {
	parent string
	tree   string
	files  map[string][]byte
}

type pull struct {
	pr   forge.PullRequest
	base string
}

type repo struct {
	ref      forge.RepoRef
	info     forge.RepoInfo
	commits  map[string]*commit
	branches map[string]string
	special  map[string]string // path -> "symlink" | "submodule"
	pulls    []*pull
}

// Forge is the in-memory forge. Safe for concurrent use.
type Forge struct {
	mu    sync.Mutex
	repos map[string]*repo // by "owner/name"
	err   error
	// Now stamps merges; nil means time.Now.
	Now func() time.Time
	// WebHost builds PR URLs; "" means github.com.
	WebHost string
	opened  []forge.PullRequestRequest
}

var (
	_ forge.ForgeReader = (*Forge)(nil)
	_ forge.ForgeWriter = (*Forge)(nil)
)

// New returns an empty forge.
func New() *Forge { return &Forge{repos: map[string]*repo{}} }

func key(r forge.RepoRef) string { return r.Owner + "/" + r.Name }

func hashOf(parts ...string) string {
	h := sha1.New()
	for _, p := range parts {
		fmt.Fprintf(h, "%d:%s;", len(p), p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func treeOf(files map[string][]byte) string {
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	parts := []string{"tree"}
	for _, p := range paths {
		parts = append(parts, p, forge.BlobSHA(files[p]))
	}
	return hashOf(parts...)
}

func (r *repo) add(parent string, files map[string][]byte, salt string) string {
	t := treeOf(files)
	sha := hashOf("commit", parent, t, salt)
	r.commits[sha] = &commit{parent: parent, tree: t, files: files}
	return sha
}

// AddRepo adds a repository whose base branch holds files in one commit,
// and returns that commit.
func (f *Forge) AddRepo(ref forge.RepoRef, info forge.RepoInfo, files map[string]string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := &repo{ref: ref, info: info, commits: map[string]*commit{}, branches: map[string]string{}, special: map[string]string{}}
	fs := map[string][]byte{}
	for p, c := range files {
		fs[p] = []byte(c)
	}
	sha := r.add("", fs, "init "+key(ref))
	r.branches[ref.Base] = sha
	f.repos[key(ref)] = r
	return sha
}

// Commit adds a commit on branch that sets the files in set and removes del,
// and returns it. It panics on an unknown repo or branch (test setup).
func (f *Forge) Commit(ref forge.RepoRef, branch string, set map[string]string, del ...string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.repos[key(ref)]
	parent := r.branches[branch]
	files := map[string][]byte{}
	for p, c := range r.commits[parent].files {
		files[p] = c
	}
	for p, c := range set {
		files[p] = []byte(c)
	}
	for _, p := range del {
		delete(files, p)
	}
	sha := r.add(parent, files, "commit "+strconv.Itoa(len(r.commits)))
	r.branches[branch] = sha
	return sha
}

// AddSpecial marks path as a symlink or submodule (File refuses it).
func (f *Forge) AddSpecial(ref forge.RepoRef, p, kind string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.repos[key(ref)].special[p] = kind
}

// SetErr makes every later call fail with err (nil restores).
func (f *Forge) SetErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// Opened returns the requests OpenPullRequest accepted, in order.
func (f *Forge) Opened() []forge.PullRequestRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]forge.PullRequestRequest(nil), f.opened...)
}

func (f *Forge) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

func (f *Forge) repo(ref forge.RepoRef) (*repo, error) {
	if f.err != nil {
		return nil, f.err
	}
	r, ok := f.repos[key(ref)]
	if !ok {
		return nil, fmt.Errorf("fake forge: repository %s: %w", key(ref), clients.ErrNotFound)
	}
	return r, nil
}

func (f *Forge) findPull(ref forge.RepoRef, number int) (*pull, error) {
	r, err := f.repo(ref)
	if err != nil {
		return nil, err
	}
	for _, p := range r.pulls {
		if p.pr.Number == number {
			return p, nil
		}
	}
	return nil, fmt.Errorf("fake forge: pull request %d: %w", number, clients.ErrNotFound)
}

// Merge merges pull request number with a merge commit on its base.
func (f *Forge) Merge(ref forge.RepoRef, number int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, err := f.findPull(ref, number)
	if err != nil {
		return err
	}
	r := f.repos[key(ref)]
	head := r.commits[p.pr.HeadSHA]
	sha := r.add(r.branches[p.base], head.files, "merge "+strconv.Itoa(number))
	r.branches[p.base] = sha
	p.pr.State, p.pr.MergeSHA, p.pr.MergedAt = forge.PRStateMerged, sha, f.now().UTC()
	return nil
}

// Close closes pull request number without merge.
func (f *Forge) Close(ref forge.RepoRef, number int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, err := f.findPull(ref, number)
	if err != nil {
		return err
	}
	p.pr.State = forge.PRStateClosed
	return nil
}

// PushToPull adds a reviewer's commit to the PR's branch (head changes).
func (f *Forge) PushToPull(ref forge.RepoRef, number int, set map[string]string) error {
	f.mu.Lock()
	p, err := f.findPull(ref, number)
	f.mu.Unlock()
	if err != nil {
		return err
	}
	sha := f.Commit(ref, p.pr.HeadRef, set)
	f.mu.Lock()
	p.pr.HeadSHA = sha
	f.mu.Unlock()
	return nil
}

// SetChecks sets the folded checks of pull request number.
func (f *Forge) SetChecks(ref forge.RepoRef, number int, c forge.PullRequestChecks) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, err := f.findPull(ref, number)
	if err != nil {
		return err
	}
	p.pr.Checks = c
	return nil
}

// RepoInfo implements forge.ForgeReader.
func (f *Forge) RepoInfo(_ context.Context, ref forge.RepoRef) (forge.RepoInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, err := f.repo(ref)
	if err != nil {
		return forge.RepoInfo{}, err
	}
	return r.info, nil
}

// BaseHead implements forge.ForgeReader.
func (f *Forge) BaseHead(_ context.Context, ref forge.RepoRef) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, err := f.repo(ref)
	if err != nil {
		return "", err
	}
	sha, ok := r.branches[ref.Base]
	if !ok {
		return "", fmt.Errorf("fake forge: branch %s: %w", ref.Base, clients.ErrNotFound)
	}
	return sha, nil
}

func (r *repo) commit(sha string) (*commit, error) {
	c, ok := r.commits[sha]
	if !ok {
		return nil, fmt.Errorf("fake forge: commit %.7s: %w", sha, clients.ErrNotFound)
	}
	return c, nil
}

// File implements forge.ForgeReader.
func (f *Forge) File(_ context.Context, ref forge.RepoRef, sha, p string) (forge.RepoFile, error) {
	if err := forge.CheckRepoPath(p, false); err != nil {
		return forge.RepoFile{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	r, err := f.repo(ref)
	if err != nil {
		return forge.RepoFile{}, err
	}
	c, err := r.commit(sha)
	if err != nil {
		return forge.RepoFile{}, err
	}
	if _, ok := r.special[p]; ok {
		return forge.RepoFile{}, fmt.Errorf("%w: %s", forge.ErrNotRegularFile, p)
	}
	b, ok := c.files[p]
	if !ok {
		for q := range c.files {
			if strings.HasPrefix(q, p+"/") {
				return forge.RepoFile{}, fmt.Errorf("%w: %s is a directory", forge.ErrNotRegularFile, p)
			}
		}
		return forge.RepoFile{}, fmt.Errorf("fake forge: %s: %w", p, clients.ErrNotFound)
	}
	out := forge.RepoFile{Path: p, SHA: sha, BlobSHA: forge.BlobSHA(b), Size: len(b)}
	if len(b) > forge.MaxRepoFileBytes {
		return out, fmt.Errorf("%w: %s", forge.ErrRepoFileTooLarge, p)
	}
	out.Content = append([]byte(nil), b...)
	return out, nil
}

// ListFiles implements forge.ForgeReader.
func (f *Forge) ListFiles(_ context.Context, ref forge.RepoRef, sha string) ([]forge.RepoEntry, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, err := f.repo(ref)
	if err != nil {
		return nil, false, err
	}
	c, err := r.commit(sha)
	if err != nil {
		return nil, false, err
	}
	var out []forge.RepoEntry
	for p, b := range c.files {
		if _, sp := r.special[p]; sp || forge.CheckRepoPath(p, false) != nil {
			continue
		}
		out = append(out, forge.RepoEntry{Path: p, BlobSHA: forge.BlobSHA(b), Size: len(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	if len(out) > forge.MaxRepoListEntries {
		return out[:forge.MaxRepoListEntries], true, nil
	}
	return out, false, nil
}

// PullRequest implements forge.ForgeReader.
func (f *Forge) PullRequest(_ context.Context, ref forge.RepoRef, number int) (forge.PullRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, err := f.findPull(ref, number)
	if err != nil {
		return forge.PullRequest{}, err
	}
	pr := p.pr
	pr.Checks.Failed = append([]string(nil), pr.Checks.Failed...)
	return pr, nil
}

// OpenPullRequest implements forge.ForgeWriter with the real writer's rules.
func (f *Forge) OpenPullRequest(_ context.Context, ref forge.RepoRef, req forge.PullRequestRequest) (forge.PullRequest, error) {
	if !strings.HasPrefix(req.Branch, forge.FixBranchPrefix) || !forge.ValidBranchName(req.Branch) || req.Branch == ref.Base {
		return forge.PullRequest{}, fmt.Errorf("%w: bad fix branch", forge.ErrRefused)
	}
	if len(req.Files) == 0 || len(req.Files) > forge.MaxFixFiles || req.Title == "" {
		return forge.PullRequest{}, fmt.Errorf("%w: bad request", forge.ErrRefused)
	}
	for _, fc := range req.Files {
		if err := forge.CheckRepoPath(fc.Path, false); err != nil {
			return forge.PullRequest{}, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	r, err := f.repo(ref)
	if err != nil {
		return forge.PullRequest{}, err
	}
	base, err := r.commit(req.BaseSHA)
	if err != nil {
		return forge.PullRequest{}, err
	}
	head := r.commits[r.branches[ref.Base]]
	var moved []string
	for _, fc := range req.Files {
		a, aok := base.files[fc.Path]
		b, bok := head.files[fc.Path]
		if aok != bok || string(a) != string(b) {
			moved = append(moved, fc.Path)
		}
	}
	if len(moved) > 0 {
		return forge.PullRequest{}, fmt.Errorf("%w: %s", forge.ErrBaseMoved, strings.Join(moved, ", "))
	}
	files := map[string][]byte{}
	for p, c := range base.files {
		files[p] = c
	}
	for _, fc := range req.Files {
		files[fc.Path] = append([]byte(nil), fc.Content...)
	}
	tree := treeOf(files)
	if sha, ok := r.branches[req.Branch]; ok {
		c := r.commits[sha]
		if c.parent != req.BaseSHA || c.tree != tree {
			return forge.PullRequest{}, fmt.Errorf("%w: %s", forge.ErrBranchExists, req.Branch)
		}
		for _, p := range r.pulls {
			if p.pr.HeadRef == req.Branch && p.pr.State == forge.PRStateOpen {
				return p.pr, nil
			}
		}
	} else {
		r.branches[req.Branch] = r.add(req.BaseSHA, files, "fix "+req.Branch)
	}
	host := f.WebHost
	if host == "" {
		host = "github.com"
	}
	n := len(r.pulls) + 1
	p := &pull{base: ref.Base, pr: forge.PullRequest{
		Repo: ref.ID, Number: n, URL: "https://" + host + "/" + key(ref) + "/pull/" + strconv.Itoa(n),
		State: forge.PRStateOpen, HeadRef: req.Branch, HeadSHA: r.branches[req.Branch],
		Checks: forge.PullRequestChecks{State: forge.ChecksNone},
	}}
	r.pulls = append(r.pulls, p)
	f.opened = append(f.opened, req)
	return p.pr, nil
}

// FileAt returns the content of p at the head of branch, for assertions.
func (f *Forge) FileAt(ref forge.RepoRef, branch, p string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.repos[key(ref)]
	if !ok {
		return "", false
	}
	c, ok := r.commits[r.branches[branch]]
	if !ok {
		return "", false
	}
	b, ok := c.files[path.Clean(p)]
	return string(b), ok
}
