package forge

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/redact"
)

// Check names on a pull request (PullRequestChecks.Failed).
const (
	maxFailedChecks = 10
	maxCheckName    = 100
	checksPerPage   = 100
)

var (
	shaRE  = regexp.MustCompile(`^[0-9a-f]{40}$`)
	nameRE = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}$`)
)

// checkRepo refuses a RepoRef the client cannot address safely. Owner and
// Name come from owners.yaml, but a typo must not become another path.
func checkRepo(r RepoRef) error {
	for _, s := range []string{r.Owner, r.Name} {
		if !nameRE.MatchString(s) || s == "." || s == ".." {
			return fmt.Errorf("%w: repository owner and name must match [A-Za-z0-9_.-]+ (repo %q)", ErrRefused, oneLine(r.ID, 80))
		}
	}
	return nil
}

func checkSHA(sha string) error {
	if !shaRE.MatchString(sha) {
		return fmt.Errorf("%w: a commit must be 40 lowercase hex characters", ErrRefused)
	}
	return nil
}

func repoPath(r RepoRef) string { return "/repos/" + r.Owner + "/" + r.Name }

// escapePath escapes each segment of a checked repository path or branch.
func escapePath(p string) string {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}

// RepoInfo reads visibility, archived and the default branch.
func (c *Client) RepoInfo(ctx context.Context, repo RepoRef) (RepoInfo, error) {
	return c.repoInfo(ctx, repo, scopeRead)
}

func (c *Client) repoInfo(ctx context.Context, repo RepoRef, s scope) (RepoInfo, error) {
	if err := checkRepo(repo); err != nil {
		return RepoInfo{}, err
	}
	tok, err := c.token(ctx, repo, s)
	if err != nil {
		return RepoInfo{}, err
	}
	var out struct {
		Private       bool   `json:"private"`
		Visibility    string `json:"visibility"`
		Archived      bool   `json:"archived"`
		DefaultBranch string `json:"default_branch"`
	}
	if err := c.do(ctx, tok, request{ep: epRepo, method: http.MethodGet, path: repoPath(repo)}, &out); err != nil {
		return RepoInfo{}, err
	}
	vis := out.Visibility
	switch vis {
	case "public", "private", "internal":
	default:
		// Older GitHub Enterprise answers have no visibility; never read an
		// unknown value as private.
		vis = "public"
		if out.Private {
			vis = "private"
		}
	}
	return RepoInfo{Visibility: vis, Archived: out.Archived, DefaultBranch: oneLine(out.DefaultBranch, 200)}, nil
}

// BaseHead returns the commit the base branch points at now.
func (c *Client) BaseHead(ctx context.Context, repo RepoRef) (string, error) {
	return c.branchHead(ctx, repo, repo.Base, scopeRead)
}

func (c *Client) branchHead(ctx context.Context, repo RepoRef, branch string, s scope) (string, error) {
	if err := checkRepo(repo); err != nil {
		return "", err
	}
	if !ValidBranchName(branch) {
		return "", fmt.Errorf("%w: branch name is not valid", ErrRefused)
	}
	tok, err := c.token(ctx, repo, s)
	if err != nil {
		return "", err
	}
	var out struct {
		Ref    string `json:"ref"`
		Object struct {
			SHA  string `json:"sha"`
			Type string `json:"type"`
		} `json:"object"`
	}
	if err := c.do(ctx, tok, request{ep: epRef, method: http.MethodGet, path: repoPath(repo) + "/git/ref/heads/" + escapePath(branch)}, &out); err != nil {
		return "", err
	}
	// git/ref answers an exact ref only; check anyway that it is this one
	// and points at a commit.
	if out.Ref != "refs/heads/"+branch || out.Object.Type != "commit" || checkSHA(out.Object.SHA) != nil {
		return "", fmt.Errorf("forge: branch %s of %s does not point at a commit", branch, repo.ID)
	}
	return out.Object.SHA, nil
}

// contentsAnswer is GET /repos/{o}/{r}/contents/{path}: an object for a file,
// symlink or submodule, an array for a directory.
type contentsAnswer struct {
	Type     string `json:"type"`
	Path     string `json:"path"`
	SHA      string `json:"sha"`
	Size     int    `json:"size"`
	Encoding string `json:"encoding"`
	Content  string `json:"content"`
}

// contents reads path at commit sha. dir reports a directory listing.
func (c *Client) contents(ctx context.Context, repo RepoRef, sha, p string, s scope) (ans contentsAnswer, dir bool, err error) {
	tok, err := c.token(ctx, repo, s)
	if err != nil {
		return ans, false, err
	}
	var raw json.RawMessage
	r := request{ep: epContents, method: http.MethodGet, path: repoPath(repo) + "/contents/" + escapePath(p), query: url.Values{"ref": {sha}}}
	if err := c.do(ctx, tok, r, &raw); err != nil {
		return ans, false, err
	}
	if t := strings.TrimSpace(string(raw)); strings.HasPrefix(t, "[") {
		return ans, true, nil
	}
	if err := json.Unmarshal(raw, &ans); err != nil {
		return ans, false, fmt.Errorf("forge: contents of %s: answer is not the expected JSON: %w", p, err)
	}
	return ans, false, nil
}

// regular checks a contents answer for p is the regular file p itself. The
// contents API follows a symlink whose target is a file and answers with the
// target, so an answer for another path is a symlink.
func regular(ans contentsAnswer, p string) error {
	if ans.Type != "file" || ans.Path != p {
		return fmt.Errorf("%w: %s", ErrNotRegularFile, p)
	}
	if checkSHA(ans.SHA) != nil {
		return fmt.Errorf("forge: contents of %s: no blob id", p)
	}
	return nil
}

// File reads one regular file at commit sha.
func (c *Client) File(ctx context.Context, repo RepoRef, sha, p string) (RepoFile, error) {
	if err := checkRepo(repo); err != nil {
		return RepoFile{}, err
	}
	if err := checkSHA(sha); err != nil {
		return RepoFile{}, err
	}
	if err := CheckRepoPath(p, false); err != nil {
		return RepoFile{}, err
	}
	ans, dir, err := c.contents(ctx, repo, sha, p, scopeRead)
	if err != nil {
		return RepoFile{}, err
	}
	if dir {
		return RepoFile{}, fmt.Errorf("%w: %s is a directory", ErrNotRegularFile, p)
	}
	if err := regular(ans, p); err != nil {
		return RepoFile{}, err
	}
	f := RepoFile{Path: p, SHA: sha, BlobSHA: ans.SHA, Size: ans.Size}
	if ans.Size > MaxRepoFileBytes || ans.Size < 0 {
		return f, fmt.Errorf("%w: %s is %d bytes, at most %d are read", ErrRepoFileTooLarge, p, ans.Size, MaxRepoFileBytes)
	}
	if ans.Encoding != "base64" {
		return f, fmt.Errorf("forge: contents of %s: encoding %q, want base64", p, oneLine(ans.Encoding, 20))
	}
	b, err := base64.StdEncoding.DecodeString(strings.NewReplacer("\n", "", "\r", "").Replace(ans.Content))
	if err != nil {
		return f, fmt.Errorf("forge: contents of %s: content is not base64: %w", p, err)
	}
	// The blob id is the hash of the content: a mismatch means a cut or
	// altered answer, never a file to show.
	if len(b) != ans.Size || BlobSHA(b) != ans.SHA {
		return f, fmt.Errorf("forge: contents of %s: content does not match its blob id", p)
	}
	f.Content = b
	return f, nil
}

// BlobSHA is the git blob id of content: sha1("blob <len>\x00" + content).
func BlobSHA(content []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(content))
	h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

// ListFiles lists the regular files (mode 100644 or 100755) of the tree at
// commit sha whose path passes CheckRepoPath, at most MaxRepoListEntries.
// truncated reports a cut listing, by the cap or by GitHub (a tree of more
// than 100,000 entries or 7 MB).
func (c *Client) ListFiles(ctx context.Context, repo RepoRef, sha string) ([]RepoEntry, bool, error) {
	if err := checkRepo(repo); err != nil {
		return nil, false, err
	}
	if err := checkSHA(sha); err != nil {
		return nil, false, err
	}
	tok, err := c.token(ctx, repo, scopeRead)
	if err != nil {
		return nil, false, err
	}
	var out struct {
		Tree []struct {
			Path string `json:"path"`
			Mode string `json:"mode"`
			Type string `json:"type"`
			SHA  string `json:"sha"`
			Size int    `json:"size"`
		} `json:"tree"`
		Truncated bool `json:"truncated"`
	}
	r := request{ep: epTree, method: http.MethodGet, path: repoPath(repo) + "/git/trees/" + sha, query: url.Values{"recursive": {"1"}}}
	if err := c.do(ctx, tok, r, &out); err != nil {
		return nil, false, err
	}
	truncated := out.Truncated
	entries := make([]RepoEntry, 0, min(len(out.Tree), MaxRepoListEntries))
	for _, e := range out.Tree {
		if e.Type != "blob" || (e.Mode != "100644" && e.Mode != "100755") || checkSHA(e.SHA) != nil || CheckRepoPath(e.Path, false) != nil {
			continue
		}
		if len(entries) == MaxRepoListEntries {
			truncated = true
			break
		}
		entries = append(entries, RepoEntry{Path: e.Path, BlobSHA: e.SHA, Size: e.Size})
	}
	return entries, truncated, nil
}

// pullAnswer is the part of a pull request answer the client reads.
type pullAnswer struct {
	Number   int        `json:"number"`
	State    string     `json:"state"`
	Merged   bool       `json:"merged"`
	MergedAt *time.Time `json:"merged_at"`
	MergeSHA string     `json:"merge_commit_sha"`
	Head     struct {
		Ref  string `json:"ref"`
		SHA  string `json:"sha"`
		Repo *struct {
			FullName string `json:"full_name"`
		} `json:"repo"`
	} `json:"head"`
	User struct {
		Login string `json:"login"`
		Type  string `json:"type"`
	} `json:"user"`
}

// pull maps an answer. The URL is built from the web host, never taken from
// the answer, so a link in the room always points at this forge.
func (c *Client) pull(repo RepoRef, a pullAnswer) (PullRequest, error) {
	if a.Number <= 0 || checkSHA(a.Head.SHA) != nil {
		return PullRequest{}, fmt.Errorf("forge: pull request answer for %s has no number or head commit", repo.ID)
	}
	pr := PullRequest{
		Repo: repo.ID, Number: a.Number,
		URL:     "https://" + c.webHost + "/" + repo.Owner + "/" + repo.Name + "/pull/" + strconv.Itoa(a.Number),
		HeadRef: oneLine(a.Head.Ref, 255), HeadSHA: a.Head.SHA,
	}
	if a.Head.Repo != nil {
		pr.HeadRepo = oneLine(a.Head.Repo.FullName, 201)
	}
	switch {
	case a.Merged || (a.MergedAt != nil && !a.MergedAt.IsZero()):
		pr.State = PRStateMerged
		if checkSHA(a.MergeSHA) == nil {
			pr.MergeSHA = a.MergeSHA
		}
		if a.MergedAt != nil {
			pr.MergedAt = a.MergedAt.UTC()
		}
	case a.State == "open":
		pr.State = PRStateOpen
	case a.State == "closed":
		pr.State = PRStateClosed
	default:
		return PullRequest{}, fmt.Errorf("forge: pull request %d of %s has state %q", a.Number, repo.ID, oneLine(a.State, 20))
	}
	return pr, nil
}

// PullRequest reads one pull request with its head checks. HeadRepo is the
// answer's head.repo.full_name ("" for a deleted fork).
func (c *Client) PullRequest(ctx context.Context, repo RepoRef, number int) (PullRequest, error) {
	if err := checkRepo(repo); err != nil {
		return PullRequest{}, err
	}
	if number <= 0 {
		return PullRequest{}, fmt.Errorf("%w: pull request number must be positive", ErrRefused)
	}
	tok, err := c.token(ctx, repo, scopeRead)
	if err != nil {
		return PullRequest{}, err
	}
	var a pullAnswer
	if err := c.do(ctx, tok, request{ep: epPull, method: http.MethodGet, path: repoPath(repo) + "/pulls/" + strconv.Itoa(number)}, &a); err != nil {
		return PullRequest{}, err
	}
	pr, err := c.pull(repo, a)
	if err != nil {
		return PullRequest{}, err
	}
	pr.Checks, err = c.checks(ctx, tok, repo, pr.HeadSHA)
	if err != nil {
		return PullRequest{}, err
	}
	return pr, nil
}

// maxCheckPages caps the pages of check runs and of commit statuses read
// per call: checksPerPage * maxCheckPages = 1000 of each. A head with more
// is reported pending (or failing, when a read page already failed), never
// success: an unread page could hold a failure.
const maxCheckPages = 10

type checkRun struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}

type commitStatus struct {
	Context string `json:"context"`
	State   string `json:"state"`
}

// checks folds the check runs and the combined commit status on sha, paging
// through both (up to maxCheckPages each).
func (c *Client) checks(ctx context.Context, tok netsec.Secret, repo RepoRef, sha string) (PullRequestChecks, error) {
	base := repoPath(repo) + "/commits/" + sha
	var (
		runs                     []checkRun
		sts                      []commitStatus
		runsTotal, statusesTotal int
		allRuns, allStatuses     bool
	)
	for page := 1; page <= maxCheckPages; page++ {
		q := url.Values{"per_page": {strconv.Itoa(checksPerPage)}, "page": {strconv.Itoa(page)}}
		var out struct {
			TotalCount int        `json:"total_count"`
			CheckRuns  []checkRun `json:"check_runs"`
		}
		if err := c.do(ctx, tok, request{ep: epCheckRuns, method: http.MethodGet, path: base + "/check-runs", query: q}, &out); err != nil {
			return PullRequestChecks{}, err
		}
		runsTotal = max(runsTotal, out.TotalCount)
		runs = append(runs, out.CheckRuns...)
		if len(runs) >= runsTotal || len(out.CheckRuns) == 0 {
			allRuns = len(runs) >= runsTotal
			break
		}
	}
	for page := 1; page <= maxCheckPages; page++ {
		q := url.Values{"per_page": {strconv.Itoa(checksPerPage)}, "page": {strconv.Itoa(page)}}
		var out struct {
			TotalCount int            `json:"total_count"`
			Statuses   []commitStatus `json:"statuses"`
		}
		if err := c.do(ctx, tok, request{ep: epStatus, method: http.MethodGet, path: base + "/status", query: q}, &out); err != nil {
			return PullRequestChecks{}, err
		}
		statusesTotal = max(statusesTotal, out.TotalCount)
		sts = append(sts, out.Statuses...)
		if len(sts) >= statusesTotal || len(out.Statuses) == 0 {
			allStatuses = len(sts) >= statusesTotal
			break
		}
	}
	return foldChecks(runs, sts, runsTotal, statusesTotal, !allRuns || !allStatuses), nil
}

// foldChecks folds read check runs and statuses; unread reports that some
// were not read (the page cap, or a short answer), which is pending unless a
// read one failed.
func foldChecks(runs []checkRun, sts []commitStatus, runsTotal, statusesTotal int, unread bool) PullRequestChecks {
	var failed []string
	pending := unread
	for _, r := range runs {
		switch {
		case r.Status != "completed":
			pending = true
		case r.Conclusion == "failure" || r.Conclusion == "timed_out" || r.Conclusion == "cancelled" ||
			r.Conclusion == "action_required" || r.Conclusion == "startup_failure":
			failed = append(failed, r.Name)
		case r.Conclusion == "stale":
			pending = true
		}
	}
	for _, s := range sts {
		switch s.State {
		case "failure", "error":
			failed = append(failed, s.Context)
		case "pending":
			pending = true
		}
	}
	out := PullRequestChecks{Total: max(runsTotal, len(runs)) + max(statusesTotal, len(sts))}
	switch {
	case len(failed) > 0:
		out.State = ChecksFailure
	case pending:
		out.State = ChecksPending
	case out.Total == 0:
		out.State = ChecksNone
	default:
		out.State = ChecksSuccess
	}
	for _, n := range failed {
		if len(out.Failed) == maxFailedChecks {
			break
		}
		red, _ := redact.Text(oneLine(n, 4*maxCheckName))
		// oneLine adds a 3-byte ellipsis when it cuts.
		out.Failed = append(out.Failed, oneLine(red, maxCheckName-len("…")))
	}
	return out
}

// isNotFound reports a 404.
func isNotFound(err error) bool { return errors.Is(err, clients.ErrNotFound) }
