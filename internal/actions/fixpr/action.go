package fixpr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/forge"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// Name is the registered name of the action.
const Name = "repo_open_pull_request"

const (
	// readTimeout bounds the reads of one Validate or Preview (AGENTS.md §4
	// rule 4); each forge request has its own 10 s deadline inside it.
	readTimeout = 10 * time.Second
	// writeTimeout bounds OpenPullRequest: up to ten blobs, a tree, a commit,
	// a ref and the PR, each a request of at most 10 s.
	writeTimeout = 2 * time.Minute
	defaultHost  = "github.com"
)

var errNoWriter = errors.New("no write credentials: the forge writer is not configured (PIPEROOM_WRITE_MODE=on and a forge with a GitHub App are both needed)")

// Deps are the collaborators of the action.
type Deps struct {
	// Reader reads the repositories: required. Validate and Preview use it
	// only; they never hold the writer.
	Reader clients.ForgeReader
	// Writer opens the PR in Execute; nil (untyped) with write mode off, and
	// then Execute refuses while the proposal is still created, previewed
	// and approved (P0 ruling #606).
	Writer clients.ForgeWriter
	// WebHost is the forge's web host (config Forge.WebHost: github.com for
	// api.github.com, else the GitHub Enterprise host); "" means github.com.
	// The title and body may link to it.
	WebHost string
	// PublicURL is PIPEROOM_PUBLIC_URL: the footer links the room there, and
	// the title and body may link to its host. "" names the room id only.
	PublicURL string

	// timeout overrides readTimeout; tests only.
	timeout time.Duration
}

// New returns repo_open_pull_request (design §6): it opens one pull request
// that changes files of one layer in a repository the room's pipeline lists,
// on a new piperoom/ branch from the commit the agent read. Wiring (Q7f)
// registers it; this package registers nothing.
func New(d Deps) actions.Action { return &action{d: d} }

type action struct{ d Deps }

var (
	_ actions.Action            = (*action)(nil)
	_ actions.Scoped            = (*action)(nil)
	_ actions.Preview           = (*action)(nil)
	_ actions.Layered           = (*action)(nil)
	_ actions.Deferred          = (*action)(nil)
	_ actions.PullRequestOpener = (*action)(nil)
)

func (a *action) Name() string { return Name }

// Layer is "": the approving layer is the path globs' (LayerFor), never a
// fixed one and never the proposer's.
func (a *action) Layer() string { return "" }

// Deferred marks the action: its Execute only opens a PR that people merge
// and deploy later. Never policy-approved; a proposal holding it holds only
// Deferred steps, at most actions.MaxDeferredSteps (the room service
// enforces both).
func (a *action) Deferred() {}

func (a *action) Describe() string {
	return "Open a pull request that changes text files in a repository the pipeline lists in owners.yaml repos: " +
		"(a connector JSON kept in git, a Connect image Dockerfile, a version pin, a consumer's config). " +
		"Read each file first with repo_read_file and pass its base_sha; state each change as search-and-replace edits " +
		"(old must occur exactly once) or, for a new file only, its whole content; never delete. Change the fewest lines, " +
		"all files of one step under globs of one layer, at most 10 files. Never write a credential (use ${secret:...}), " +
		"never a masked or cut line. title: one line; body: the cause and the evidence, no images, no HTML, links only to " +
		"the forge or Piperoom. applied_when says how Piperoom sees the merged change in the system. " +
		"The approving layer is the globs' layer. Approving opens the PR; nothing changes in any system until a reviewer merges it. " +
		"A PR step goes in a proposal of PR steps only (at most 3); a live stop-gap is a separate proposal."
}

func (a *action) ParamsSchema() map[string]any { return paramsSchema() }

// webHost is the forge's web host, lower-case.
func (a *action) webHost() string {
	if h := strings.ToLower(strings.TrimSpace(a.d.WebHost)); h != "" {
		return h
	}
	return defaultHost
}

// allowedHosts are the hosts the title and body may link to.
func (a *action) allowedHosts() []string {
	hosts := []string{a.webHost()}
	if u, err := url.Parse(a.d.PublicURL); err == nil && u.Hostname() != "" {
		if h := strings.ToLower(u.Hostname()); h != hosts[0] {
			hosts = append(hosts, h)
		}
	}
	return hosts
}

func (a *action) fail(err error) error { return fmt.Errorf("fixpr: %s: %w", Name, err) }

// CheckScope refuses, without I/O, a repo the pipeline does not list, a path
// outside its globs or under the deny list, files of two layers in one step,
// and an applied_when connector that is not the pipeline's.
func (a *action) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	p, err := parse(raw, a.allowedHosts())
	if err != nil {
		return a.fail(err)
	}
	if _, err := inScope(pl, p); err != nil {
		return a.fail(err)
	}
	return nil
}

// LayerFor returns the single layer of the globs the step's files match
// (design §6.2 rule 3). No I/O.
func (a *action) LayerFor(pl owners.Pipeline, raw json.RawMessage) (string, error) {
	p, err := parse(raw, a.allowedHosts())
	if err != nil {
		return "", a.fail(err)
	}
	r, err := inScope(pl, p)
	if err != nil {
		return "", a.fail(err)
	}
	return r.layer, nil
}

// scoped parses raw and checks it against the room's pipeline in ctx. It
// fails closed: only a pipeline names repositories, so no pipeline (and the
// tools.WithUnscoped opt-out too) is tools.ErrNoScope.
func (a *action) scoped(ctx context.Context, raw json.RawMessage) (resolved, error) {
	p, err := parse(raw, a.allowedHosts())
	if err != nil {
		return resolved{}, err
	}
	pl, ok := tools.PipelineFrom(ctx)
	if !ok {
		return resolved{}, fmt.Errorf("%w: a pull-request fix needs the room's whole pipeline in ctx (its repos: block names the repositories)", tools.ErrNoScope)
	}
	return inScope(pl, p)
}

// plan is a checked step with the files read and changed.
type plan struct {
	resolved
	info  forge.RepoInfo
	files []planFile
	diffs []fileDiff
}

type planFile struct {
	path    string
	isNew   bool
	old     string
	content string
}

// Validate is design §6.2: the scope (rules 2 to 4), the text rules (5 to
// 7) and the reads (rule 8) through the reader only. It needs the room's
// pipeline in ctx.
func (a *action) Validate(ctx context.Context, raw json.RawMessage) error {
	if _, err := a.check(ctx, raw); err != nil {
		return a.fail(err)
	}
	return nil
}

// check runs every rule and returns the plan Preview and Execute build on.
func (a *action) check(ctx context.Context, raw json.RawMessage) (plan, error) {
	r, err := a.scoped(ctx, raw)
	if err != nil {
		return plan{}, err
	}
	if a.d.Reader == nil {
		return plan{}, errors.New("no forge reader: FORGE_KIND is not set, so no repository can be read")
	}
	d := a.d.timeout
	if d <= 0 {
		d = readTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	return a.read(ctx, r)
}

// read is rule 8.
func (a *action) read(ctx context.Context, r resolved) (plan, error) {
	pl := plan{resolved: r}
	info, err := a.d.Reader.RepoInfo(ctx, r.ref)
	if err != nil {
		return pl, fmt.Errorf("read repository %s: %w", r.ref.ID, err)
	}
	switch {
	case info.Archived:
		return pl, fmt.Errorf("repository %s is archived; it takes no pull request", r.ref.ID)
	case strings.EqualFold(info.Visibility, "public") && !r.repo.PublicOK:
		return pl, fmt.Errorf("repository %s is public: a fix PR would publish its diff, title and body; owners.yaml must set public_ok: true for it", r.ref.ID)
	}
	pl.info = info
	total := 0
	for i, c := range r.p.Changes {
		f, err := a.change(ctx, r, i, c)
		if err != nil {
			return pl, err
		}
		fd := unified(f.path, f.old, f.content, f.isNew)
		total += fd.rawBytes
		if total > forge.MaxFixDiffBytes {
			return pl, fmt.Errorf("the step's diff is over %d bytes; a fix PR changes the fewest lines (split it, or change less)", forge.MaxFixDiffBytes)
		}
		// Every added line of the result, not only the params' text: an
		// edit can join new text to the file's own into a credential line.
		for j, l := range fd.added {
			if err := checkSecrets(fmt.Sprintf("changes[%d]: added line %d", i, j+1), l); err != nil {
				return pl, err
			}
		}
		pl.files = append(pl.files, f)
		pl.diffs = append(pl.diffs, fd)
	}
	return pl, nil
}

// change reads one file at base_sha and applies its edits, or checks a new
// file's path is free.
func (a *action) change(ctx context.Context, r resolved, i int, c change) (planFile, error) {
	at := fmt.Sprintf("changes[%d] (%s)", i, c.Path)
	f, err := a.d.Reader.File(ctx, r.ref, r.p.BaseSHA, c.Path)
	if c.Content != nil {
		switch {
		case err == nil:
			return planFile{}, fmt.Errorf("%s: the file exists at base_sha; change it with edits, content is for new files only", at)
		case errors.Is(err, clients.ErrNotFound):
		case errors.Is(err, forge.ErrNotRegularFile):
			return planFile{}, fmt.Errorf("%s: the path is a directory, symlink or submodule at base_sha", at)
		default:
			return planFile{}, fmt.Errorf("%s: read at base_sha: %w", at, err)
		}
		if len(*c.Content) > forge.MaxRepoFileBytes {
			return planFile{}, fmt.Errorf("%s: the new file is over %d bytes", at, forge.MaxRepoFileBytes)
		}
		return planFile{path: c.Path, isNew: true, content: *c.Content}, nil
	}
	switch {
	case errors.Is(err, clients.ErrNotFound):
		return planFile{}, fmt.Errorf("%s: no such file at base_sha (read it with repo_read_file; content creates a new file)", at)
	case errors.Is(err, forge.ErrNotRegularFile):
		return planFile{}, fmt.Errorf("%s: not a regular file (a directory, symlink or submodule)", at)
	case errors.Is(err, forge.ErrRepoFileTooLarge):
		return planFile{}, fmt.Errorf("%s: the file is over %d bytes", at, forge.MaxRepoFileBytes)
	case err != nil:
		return planFile{}, fmt.Errorf("%s: read at base_sha: %w", at, err)
	}
	if err := checkBaseText(f.Content); err != nil {
		return planFile{}, fmt.Errorf("%s: the file %w", at, err)
	}
	old := string(f.Content)
	cur := old
	for j, e := range c.Edits {
		switch n := strings.Count(cur, e.Old); n {
		case 1:
			cur = strings.Replace(cur, e.Old, e.New, 1)
		case 0:
			return planFile{}, fmt.Errorf("%s: edits[%d]: old does not occur in the file at base_sha (after the earlier edits); read the file again and copy the text exactly", at, j)
		default:
			return planFile{}, fmt.Errorf("%s: edits[%d]: old occurs %d times; include more surrounding text so it occurs once", at, j, n)
		}
	}
	if len(cur) > forge.MaxRepoFileBytes {
		return planFile{}, fmt.Errorf("%s: the changed file is over %d bytes", at, forge.MaxRepoFileBytes)
	}
	if cur == old {
		return planFile{}, fmt.Errorf("%s: the edits leave the file unchanged", at)
	}
	return planFile{path: c.Path, old: old, content: cur}, nil
}

// branchFor is the branch Execute writes, or a placeholder before the step
// has its ids (a preview of a proposal being created).
func branchFor(ctx context.Context) string {
	if s, ok := actions.StepFrom(ctx); ok {
		return forge.FixBranch(s.RoomID, s.ProposalID, s.StepIndex)
	}
	return forge.FixBranchPrefix + "<room>-<proposal>-<step>"
}

// Preview implements actions.Preview: the PreviewResult text of design §6.3
// with the "base branch check" dry run, and in Diff the step's whole change:
// every file's unified diff, redacted line by line, the hosts the added
// lines name and the risk warnings (design §5.3 DiffPreview). Reader only.
func (a *action) Preview(ctx context.Context, raw json.RawMessage) (actions.PreviewResult, error) {
	res, dp, err := a.preview(ctx, raw)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	res.Diff = &dp
	return res, nil
}

func (a *action) preview(ctx context.Context, raw json.RawMessage) (actions.PreviewResult, DiffPreview, error) {
	pl, err := a.check(ctx, raw)
	if err != nil {
		return actions.PreviewResult{}, DiffPreview{}, a.fail(err)
	}
	d := a.d.timeout
	if d <= 0 {
		d = readTimeout
	}
	rctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	ref, p := pl.ref, pl.p
	branch := branchFor(ctx)
	risk := assess(pl.diffs)
	dp := DiffPreview{
		Repo: ref.ID, RepoURL: "https://" + a.webHost() + "/" + ref.Owner + "/" + ref.Name,
		Base: ref.Base, BaseSHA: p.BaseSHA, Branch: branch, Layer: pl.layer,
		Hosts: risk.hosts, Warnings: risk.warnings, Draft: pl.repo.Draft,
	}
	added, removed := 0, 0
	for _, f := range pl.diffs {
		dp.Files = append(dp.Files, f.FileDiff)
		added += f.Added
		removed += f.Removed
	}
	res := actions.PreviewResult{
		Target:     fmt.Sprintf("%s/%s/%s @ %s (repo %s, layer %s)", a.webHost(), ref.Owner, ref.Name, ref.Base, ref.ID, pl.layer),
		Reversible: true,
		Rollback:   "close the pull request (nothing to undo until it merges); after a merge, revert it in the repository",
	}
	kind := "pull request"
	if pl.repo.Draft {
		kind = "draft pull request"
	}
	res.Expected = fmt.Sprintf("a %s from %s into %s: %d file(s), +%d -%d. Nothing changes in any system until a reviewer merges it. "+
		"Piperoom will treat it as applied when %s. The body ends with: %s",
		kind, branch, ref.Base, len(pl.diffs), added, removed, appliedText(p.AppliedWhen), footer(a.d.PublicURL, stepOrZero(ctx), pl.layer))
	head, herr := a.d.Reader.BaseHead(rctx, ref)
	if herr != nil {
		res.Current = "base " + short(p.BaseSHA) + "; the base head could not be read"
		res.DryRun = &actions.DryRunResult{Source: "base branch check", Skipped: "the base branch head could not be read: " + oneLine(herr.Error(), 300)}
		return res, dp, nil
	}
	res.Current = fmt.Sprintf("base %s (the commit the agent read); base head now %s", short(p.BaseSHA), short(head))
	moved, err := a.moved(rctx, ref, p, head)
	if err != nil {
		res.DryRun = &actions.DryRunResult{Source: "base branch check", Skipped: "a file could not be read at the base head: " + oneLine(err.Error(), 300)}
		return res, dp, nil
	}
	if len(moved) == 0 {
		res.DryRun = &actions.DryRunResult{OK: true, Source: "base branch check",
			Text: fmt.Sprintf("no file of the change differs between base %s and the base head %s", short(p.BaseSHA), short(head))}
	} else {
		res.DryRun = &actions.DryRunResult{Source: "base branch check",
			Text: oneLine(fmt.Sprintf("changed on the base branch since %s: %s; the agent must read them again and re-propose (Execute would refuse)", short(p.BaseSHA), strings.Join(moved, ", ")), 2000)}
	}
	return res, dp, nil
}

// moved lists the paths of the change whose content differs between
// base_sha and head (design §14 D2: only touched files count).
func (a *action) moved(ctx context.Context, ref forge.RepoRef, p params, head string) ([]string, error) {
	if head == p.BaseSHA {
		return nil, nil
	}
	var out []string
	for _, c := range p.Changes {
		at, aerr := a.d.Reader.File(ctx, ref, p.BaseSHA, c.Path)
		now, herr := a.d.Reader.File(ctx, ref, head, c.Path)
		aMissing, hMissing := errors.Is(aerr, clients.ErrNotFound), errors.Is(herr, clients.ErrNotFound)
		switch {
		case aerr != nil && !aMissing:
			return nil, aerr
		case herr != nil && !hMissing:
			return nil, herr
		case aMissing != hMissing, !aMissing && at.BlobSHA != now.BlobSHA:
			out = append(out, c.Path)
		}
	}
	sort.Strings(out)
	return out, nil
}

// Execute opens the PR; see ExecutePullRequest.
func (a *action) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	s, _, err := a.ExecutePullRequest(ctx, raw)
	return s, err
}

// ExecutePullRequest implements actions.PullRequestOpener, design §6.4: it
// needs the step's ids (actions.StepFrom, which the executor's
// actions.WithStep fills) and the writer, validates again (scope and reads),
// builds the request from params alone, never from the preview, and opens
// the PR. A retried call
// writes the same branch and tree, and the writer returns the PR it opened.
func (a *action) ExecutePullRequest(ctx context.Context, raw json.RawMessage) (string, events.PullRequestRef, error) {
	step, ok := actions.StepFrom(ctx)
	if !ok || step.RoomID == "" || step.ProposalID == "" || step.StepIndex < 0 {
		return "", events.PullRequestRef{}, a.fail(errors.New("the executor gave no proposal step (StepFrom): the fix branch would have no name"))
	}
	if a.d.Writer == nil {
		// Scope first, so a refused step says why; the writer is never needed for that.
		if _, err := a.scoped(ctx, raw); err != nil {
			return "", events.PullRequestRef{}, a.fail(err)
		}
		return "", events.PullRequestRef{}, a.fail(errNoWriter)
	}
	pl, err := a.check(ctx, raw)
	if err != nil {
		return "", events.PullRequestRef{}, a.fail(err)
	}
	branch := forge.FixBranch(step.RoomID, step.ProposalID, step.StepIndex)
	if !forge.ValidBranchName(branch) {
		return "", events.PullRequestRef{}, a.fail(errors.New("the room and proposal ids do not make a valid branch name"))
	}
	req := forge.PullRequestRequest{
		BaseSHA:       pl.p.BaseSHA,
		Branch:        branch,
		Title:         neutralizeMentions(pl.p.Title),
		Body:          prBody(pl.p.Body, footer(a.d.PublicURL, step, pl.layer)),
		CommitMessage: fmt.Sprintf("%s\n\nPiperoom-Room: %s\nPiperoom-Proposal: %s\n", neutralizeMentions(pl.p.Title), step.RoomID, step.ProposalID),
		Draft:         pl.repo.Draft,
	}
	for _, f := range pl.files {
		req.Files = append(req.Files, forge.FileChange{Path: f.path, Content: []byte(f.content)})
	}
	wctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	pr, err := a.d.Writer.OpenPullRequest(wctx, pl.ref, req)
	switch {
	case errors.Is(err, forge.ErrBaseMoved):
		return "", events.PullRequestRef{}, a.fail(fmt.Errorf("the base branch changed a file of this step since the agent read it at %s; nothing was written: read the files again and propose again: %w", short(pl.p.BaseSHA), err))
	case errors.Is(err, forge.ErrBranchExists):
		return "", events.PullRequestRef{}, a.fail(fmt.Errorf("branch %s already holds a different change; nothing was written: %w", branch, err))
	case err != nil:
		return "", events.PullRequestRef{}, a.fail(fmt.Errorf("open the pull request: %w", err))
	}
	ref := events.PullRequestRef{Repo: pl.ref.ID, Number: pr.Number, URL: pr.URL, Branch: branch, HeadSHA: pr.HeadSHA}
	return fmt.Sprintf("opened pull request #%d: %s (branch %s into %s). Nothing changes until a reviewer merges it.", pr.Number, pr.URL, branch, pl.ref.Base), ref, nil
}

// stepOrZero is the step in ctx, or a zero StepRef for a preview made
// before the step has ids.
func stepOrZero(ctx context.Context) actions.StepRef {
	s, _ := actions.StepFrom(ctx)
	return s
}

// footer is the fixed text after the body (design §6.5), which the approver
// saw in the preview.
func footer(publicURL string, s actions.StepRef, layer string) string {
	room, prop := s.RoomID, s.ProposalID
	if room == "" {
		room, prop = "<room>", "<proposal>"
	}
	where := "room " + room
	if base := strings.TrimRight(strings.TrimSpace(publicURL), "/"); base != "" && s.RoomID != "" {
		where = "room " + base + "/rooms/" + url.PathEscape(s.RoomID)
	}
	approved := "the " + mdEscape(layer) + " owners"
	if len(s.Approvals) > 0 {
		names := make([]string, len(s.Approvals))
		for i, n := range s.Approvals {
			names[i] = mdEscape(n)
		}
		approved = strings.Join(names, ", ")
	}
	return fmt.Sprintf("Opened by Piperoom from %s (proposal %s, step %d). Approved in Piperoom by %s. "+
		"This PR changes nothing until merged; this repository's own review rules apply.", where, prop, s.StepIndex, approved)
}

func prBody(body, foot string) string {
	body = strings.TrimRight(neutralizeMentions(body), "\n")
	if body == "" {
		return "---\n" + foot + "\n"
	}
	return body + "\n\n---\n" + foot + "\n"
}

// appliedText renders applied_when for the card ("applied when ...").
func appliedText(a appliedWhen) string {
	switch a.Kind {
	case AppliedConnectorConfig:
		keys := make([]string, 0, len(a.Keys))
		for k, v := range a.Keys {
			keys = append(keys, oneLine(k, 80)+"="+oneLine(v, 80))
		}
		sort.Strings(keys)
		return fmt.Sprintf("connector %s's live config holds %s", oneLine(a.Connector, 80), strings.Join(keys, ", "))
	case AppliedPlugin:
		return fmt.Sprintf("Connect lists plugin %s at version %s", oneLine(a.Class, 120), oneLine(a.Version, 40))
	case AppliedChange:
		s := "a what-changed entry of kind " + strings.Join(a.Kinds, " or ") + " appears after the merge"
		if a.Service != "" {
			s += " for service " + oneLine(a.Service, 80)
		}
		return s
	default:
		n := a.Settle
		if n == 0 {
			n = 10
		}
		return fmt.Sprintf("%d minutes have passed after the merge", n)
	}
}

func short(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}
