package evals

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sync"

	"piperoom/internal/actions"
	"piperoom/internal/actions/fixpr"
	"piperoom/internal/clients"
	"piperoom/internal/clients/forge"
	"piperoom/internal/clients/forge/fake"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/forgetools"
)

// The forge replay of the pull-request fix (Q7j, docs/design/fix-pr.md §10
// "Evals (Q7j)").
//
// A scenario whose pipeline lists repos: gets what Q7f's wiring gives a room
// with a forge: the repository read tools (repo_list_files, repo_read_file,
// pr_status) and repo_open_pull_request. Every other scenario gets neither,
// so its tool and action lists stay byte for byte what they were.
//
// The forge is internal/clients/forge/fake, seeded per run from the
// scenario's fixture repositories (resources.repos): each repo's files in one
// commit on its base branch, shown under the scenario's own base_sha. The
// repository tools run live over it, as runbook_lookup runs over its embedded
// library: their answers are the real tools' (masking, numbered lines, the
// injection warning of internal/injectscan), and the files they read are the
// same bytes repo_open_pull_request's Validate and Preview read. The writer
// is a recording no-op: it opens the PR in the in-memory fake (so the room
// gets a PR reference back) and logs the write; nothing leaves the process.

// RepoFixture is one repository of resources.repos: what its base branch
// holds when the run starts.
type RepoFixture struct {
	// BaseSHA is the commit the base branch points at, as the scenario's
	// recordings and labels name it (40 lower-case hex). The fake computes
	// its own ids; the replay forge shows this one instead.
	BaseSHA string `yaml:"base_sha"`
	// Visibility is the repo's visibility; empty is private. A public repo
	// is refused by repo_open_pull_request unless owners.yaml sets
	// public_ok.
	Visibility string `yaml:"visibility"`
	// Files are the files at BaseSHA, by repository path.
	Files map[string]string `yaml:"files"`
	// Dir is a directory, relative to the scenario file, whose regular
	// files are added to Files under their relative paths. It holds what a
	// scenario file must not carry inline: a planted injection, which
	// internal/injectscan's precision test would flag in any
	// evals/scenarios/*.yaml text. LoadScenario reads it.
	Dir string `yaml:"dir"`
}

// visibilityOrDefault is the repo's visibility as the forge reports it.
func (r RepoFixture) visibilityOrDefault() string {
	if r.Visibility == "" {
		return "private"
	}
	return r.Visibility
}

// isForgeTool reports a repository read tool: it runs live over the replay
// forge, never from recordings.
func isForgeTool(name string) bool {
	switch name {
	case forgetools.NameListFiles, forgetools.NameReadFile, forgetools.NamePRStatus:
		return true
	}
	return false
}

var fixtureSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

// loadRepoDirs reads each fixture's Dir (relative to dir, the scenario
// file's directory) into its Files and clears Dir. A path in both is an
// error, as is a symlink or anything but a regular file.
func (r *Resources) loadRepoDirs(dir string) error {
	for _, id := range slices.Sorted(maps.Keys(r.Repos)) {
		fx := r.Repos[id]
		if fx.Dir == "" {
			continue
		}
		root := filepath.Join(dir, filepath.FromSlash(fx.Dir))
		files := maps.Clone(fx.Files)
		if files == nil {
			files = map[string]string{}
		}
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if !d.Type().IsRegular() {
				return fmt.Errorf("%s: not a regular file", p)
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			if _, dup := files[rel]; dup {
				return fmt.Errorf("%s is in both files and dir", rel)
			}
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			files[rel] = string(b)
			return nil
		})
		if err != nil {
			return fmt.Errorf("evals: resources.repos.%s.dir: %w", id, err)
		}
		fx.Files, fx.Dir = files, ""
		r.Repos[id] = fx
	}
	return nil
}

// validateRepos checks resources.repos against the pipeline's repos: block.
func (r Resources) validateRepos() []error {
	var errs []error
	listed := map[string]bool{}
	for _, p := range r.PipelineRepos {
		listed[p.ID] = true
	}
	for _, id := range slices.Sorted(maps.Keys(r.Repos)) {
		fx := r.Repos[id]
		where := "resources.repos." + id
		if !listed[id] {
			errs = append(errs, fmt.Errorf("%s: repo %q is not in pipeline.repos", where, id))
		}
		if fx.Dir != "" {
			errs = append(errs, fmt.Errorf("%s.dir: only LoadScenario reads a fixture directory", where))
		}
		if !fixtureSHA.MatchString(fx.BaseSHA) {
			errs = append(errs, fmt.Errorf("%s.base_sha: %q is not 40 lower-case hex digits", where, fx.BaseSHA))
		}
		switch fx.Visibility {
		case "", "private", "internal", "public":
		default:
			errs = append(errs, fmt.Errorf("%s.visibility: %q is not private, internal or public", where, fx.Visibility))
		}
		if len(fx.Files) == 0 {
			errs = append(errs, fmt.Errorf("%s: no files", where))
		}
		for _, p := range slices.Sorted(maps.Keys(fx.Files)) {
			if err := forge.CheckRepoPath(p, false); err != nil {
				errs = append(errs, fmt.Errorf("%s.files: %q: %v", where, p, err))
			}
			if len(fx.Files[p]) > forge.MaxRepoFileBytes {
				errs = append(errs, fmt.Errorf("%s.files: %q is over %d bytes", where, p, forge.MaxRepoFileBytes))
			}
		}
	}
	return errs
}

// repoWarnings names the pipeline repos the scenario gives no fixture: the
// forge has no such repository, so every read of it is "not found".
func (s Scenario) repoWarnings() []string {
	var w []string
	for _, p := range s.Pipeline.Repos {
		if _, ok := s.Resources.Repos[p.ID]; !ok {
			w = append(w, fmt.Sprintf("pipeline.repos: %s has no resources.repos fixture; the replay forge does not have it", p.ID))
		}
	}
	return w
}

// replayForge is a run's forge: the fake, seeded from the fixtures, behind
// the scenario's own commit ids. Safe for concurrent use.
type replayForge struct {
	f   *fake.Forge
	log *writeLog

	mu sync.Mutex
	// alias maps a repo ("owner/name") and a scenario commit id to the
	// fake's; back is the reverse.
	alias, back map[string]map[string]string
}

var (
	_ clients.ForgeReader = (*replayForge)(nil)
	_ clients.ForgeWriter = (*replayForge)(nil)
)

// newReplayForge seeds a forge with res's fixtures, one commit per repo on
// the base branch its pipeline entry names. Nil when the pipeline lists no
// repos.
func newReplayForge(res Resources, log *writeLog) (*replayForge, error) {
	if len(res.PipelineRepos) == 0 {
		return nil, nil
	}
	rf := &replayForge{f: fake.New(), log: log, alias: map[string]map[string]string{}, back: map[string]map[string]string{}}
	for _, p := range res.PipelineRepos {
		fx, ok := res.Repos[p.ID]
		if !ok {
			continue
		}
		ref := forge.RefOf(p.Ref())
		if ref.Owner == "" || ref.Name == "" || ref.Base == "" {
			return nil, fmt.Errorf("evals: pipeline.repos %s: no owner, name or base", p.ID)
		}
		k := ref.Owner + "/" + ref.Name
		if _, dup := rf.alias[k]; dup {
			return nil, fmt.Errorf("evals: pipeline.repos %s: two repos have url %s", p.ID, p.URL)
		}
		real := rf.f.AddRepo(ref, forge.RepoInfo{Visibility: fx.visibilityOrDefault(), DefaultBranch: ref.Base}, fx.Files)
		rf.alias[k] = map[string]string{fx.BaseSHA: real}
		rf.back[k] = map[string]string{real: fx.BaseSHA}
	}
	return rf, nil
}

func repoKey(r forge.RepoRef) string { return r.Owner + "/" + r.Name }

// toFake is the fake's commit id for a scenario one; any other id passes
// through (the fake's own, or one it does not know).
func (rf *replayForge) toFake(r forge.RepoRef, sha string) string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if real, ok := rf.alias[repoKey(r)][sha]; ok {
		return real
	}
	return sha
}

// fromFake is the scenario's id for one of the fake's commits.
func (rf *replayForge) fromFake(r forge.RepoRef, sha string) string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if s, ok := rf.back[repoKey(r)][sha]; ok {
		return s
	}
	return sha
}

func (rf *replayForge) RepoInfo(ctx context.Context, r forge.RepoRef) (forge.RepoInfo, error) {
	return rf.f.RepoInfo(ctx, r)
}

func (rf *replayForge) BaseHead(ctx context.Context, r forge.RepoRef) (string, error) {
	sha, err := rf.f.BaseHead(ctx, r)
	if err != nil {
		return "", err
	}
	return rf.fromFake(r, sha), nil
}

func (rf *replayForge) File(ctx context.Context, r forge.RepoRef, sha, p string) (forge.RepoFile, error) {
	f, err := rf.f.File(ctx, r, rf.toFake(r, sha), p)
	f.SHA = rf.fromFake(r, f.SHA)
	return f, err
}

func (rf *replayForge) ListFiles(ctx context.Context, r forge.RepoRef, sha string) ([]forge.RepoEntry, bool, error) {
	return rf.f.ListFiles(ctx, r, rf.toFake(r, sha))
}

func (rf *replayForge) PullRequest(ctx context.Context, r forge.RepoRef, number int) (forge.PullRequest, error) {
	pr, err := rf.f.PullRequest(ctx, r, number)
	pr.HeadSHA, pr.MergeSHA = rf.fromFake(r, pr.HeadSHA), rf.fromFake(r, pr.MergeSHA)
	return pr, err
}

// OpenPullRequest is the recording no-op writer: the PR is opened in the
// in-memory fake, with the real writer's rules (base moved, branch exists),
// and the write is logged by repo, branch and paths, never file content.
func (rf *replayForge) OpenPullRequest(ctx context.Context, r forge.RepoRef, req forge.PullRequestRequest) (forge.PullRequest, error) {
	paths := make([]string, 0, len(req.Files))
	for _, f := range req.Files {
		paths = append(paths, f.Path)
	}
	if rf.log != nil {
		rf.log.record("OpenPullRequest", map[string]any{"repo": r.ID, "branch": req.Branch, "paths": paths})
	}
	req.BaseSHA = rf.toFake(r, req.BaseSHA)
	pr, err := rf.f.OpenPullRequest(ctx, r, req)
	if err != nil {
		return forge.PullRequest{}, err
	}
	pr.HeadSHA = rf.fromFake(r, pr.HeadSHA)
	return pr, nil
}

// forgeToolDefs returns the repository tools' definitions, over an empty
// fake that is never called.
func forgeToolDefs() ([]tools.Tool, error) {
	reg := tools.NewRegistry()
	if err := registerForgeTools(reg, fake.New(), &owners.Owners{}); err != nil {
		return nil, err
	}
	return reg.All(), nil
}

// registerForgeTools registers the three repository tools over fg, scoped
// by own, as Q7f's wiring registers them (forgetools.Deps without Opened).
func registerForgeTools(reg *tools.Registry, fg clients.ForgeReader, own *owners.Owners) error {
	if err := forgetools.Register(reg, forgetools.Deps{Forge: fg, Owners: own, Repos: forgetools.RepoSourceFunc(forgeRepos)}); err != nil {
		return fmt.Errorf("evals: forge tools: %w", err)
	}
	return nil
}

// forgeRepos is a pipeline's repos: block as the forge tools take it
// (internal/wiring/forge.go's forgeRepos).
func forgeRepos(p owners.Pipeline) []forgetools.Repo {
	out := make([]forgetools.Repo, 0, len(p.Repos))
	for _, r := range p.Repos {
		fr := forgetools.Repo{Ref: forge.RefOf(r.Ref())}
		for _, g := range r.Paths {
			fr.Paths = append(fr.Paths, forgetools.RepoPath{Glob: g.Glob, Layer: g.Layer})
		}
		out = append(out, fr)
	}
	return out
}

// registerFixPR adds repo_open_pull_request over fg (reader and recording
// writer). Nothing when the pipeline lists no repos (fg nil).
func registerFixPR(reg *actions.Registry, fg *replayForge) error {
	if fg == nil {
		return nil
	}
	if err := reg.Register(fixpr.New(fixpr.Deps{Reader: fg, Writer: fg})); err != nil {
		return fmt.Errorf("evals: %s: %w", fixpr.Name, err)
	}
	return nil
}

// recordingPRAction is recordingAction for a pull-request action: it keeps
// the optional interfaces the room and the agent type-assert for (Deferred,
// Layered, Preview, PullRequestOpener), which an embedded actions.Action
// would hide, so the room takes the step's layer from the globs, keeps the
// proposal to PR steps, previews the diff and stores the PR it opens.
type recordingPRAction struct {
	*recordingAction
	inner prAction
}

// prAction is what repo_open_pull_request implements.
type prAction interface {
	actions.Action
	actions.Deferred
	actions.Layered
	actions.Preview
	actions.PullRequestOpener
}

var (
	_ actions.Deferred          = (*recordingPRAction)(nil)
	_ actions.Layered           = (*recordingPRAction)(nil)
	_ actions.Preview           = (*recordingPRAction)(nil)
	_ actions.PullRequestOpener = (*recordingPRAction)(nil)
)

func (a *recordingPRAction) Deferred() {}

func (a *recordingPRAction) LayerFor(p owners.Pipeline, params json.RawMessage) (string, error) {
	return a.inner.LayerFor(p, params)
}

func (a *recordingPRAction) Preview(ctx context.Context, params json.RawMessage) (actions.PreviewResult, error) {
	return a.inner.Preview(ctx, params)
}

// ExecutePullRequest plays the step through the run's execScript (its
// scripted outcome and the replay's transitions); a successful one then
// opens the PR on the replay forge, whose writer only records it.
func (a *recordingPRAction) ExecutePullRequest(ctx context.Context, params json.RawMessage) (string, events.PullRequestRef, error) {
	if a.log.exec == nil {
		return a.inner.ExecutePullRequest(ctx, params)
	}
	out, err := a.log.exec.execute(a.Name(), params)
	if err != nil {
		return out, events.PullRequestRef{}, err
	}
	_, ref, err := a.inner.ExecutePullRequest(ctx, params)
	if err != nil {
		return "", events.PullRequestRef{}, err
	}
	return out, ref, nil
}

// wrapAction is the recording wrapper of a: recordingPRAction for a
// pull-request action, recordingAction for every other.
func (l *attemptLog) wrapAction(a actions.Action) actions.Action {
	ra := &recordingAction{Action: a, log: l}
	if pa, ok := a.(prAction); ok {
		return &recordingPRAction{recordingAction: ra, inner: pa}
	}
	return ra
}
