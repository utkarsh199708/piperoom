package forge

// Local copies of Q0 item 6 (docs/design/fix-pr.md §5.1). The leads' contract
// PR Q7-0c moves them into internal/clients/clients.go; this file then turns
// into aliases (type RepoRef = clients.RepoRef, ...) so no caller changes.
// Names, fields, JSON tags and semantics follow the design exactly.

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/owners"
)

// RepoRef is one repository of a pipeline's owners.yaml repos: block, resolved
// by the caller (action, tool, room) from the repo id. The model only ever
// names the id; Owner, Name and Base come from owners.yaml. (Q7)
type RepoRef struct {
	ID    string `json:"id"`    // owners.yaml repos[].id
	Owner string `json:"owner"` // from repos[].url: https://<host>/<owner>/<name>
	Name  string `json:"name"`
	Base  string `json:"base"` // repos[].base, the branch PRs target
	// Host is the web host of repos[].url (owners.Repo.Ref fills it). When
	// set, the client refuses the repo unless it is the forge's WebHost.
	Host string `json:"host,omitempty"`
}

// RepoInfo is what a fix PR needs to know about the repository itself. (Q7)
type RepoInfo struct {
	Visibility    string `json:"visibility"` // public | private | internal
	Archived      bool   `json:"archived"`
	DefaultBranch string `json:"default_branch"`
}

// RepoFile is one regular file read at a commit. Content is untrusted text,
// at most MaxRepoFileBytes, never logged and never put in an event. (Q7)
type RepoFile struct {
	Path    string `json:"path"`
	SHA     string `json:"sha"`      // the commit the file was read at; never a branch name
	BlobSHA string `json:"blob_sha"` // git blob id; equal blob ids mean equal content
	Size    int    `json:"size"`
	Content []byte `json:"-"`
}

// RepoEntry is one path of a tree listing. (Q7)
type RepoEntry struct {
	Path    string `json:"path"`
	BlobSHA string `json:"blob_sha"`
	Size    int    `json:"size"`
}

// ForgeReader is optional and read-only: built whenever a forge is configured,
// write mode on or off. Every method takes a 10 s deadline from the caller and
// scopes its token to repo alone (docs/design/fix-pr.md §4.2). (Q7)
type ForgeReader interface {
	// RepoInfo reads visibility, archived and the default branch.
	RepoInfo(ctx context.Context, repo RepoRef) (RepoInfo, error)
	// BaseHead returns the commit the base branch points at now.
	BaseHead(ctx context.Context, repo RepoRef) (sha string, err error)
	// File reads one regular file at commit sha. ErrNotFound when absent;
	// ErrNotRegularFile for a directory, symlink or submodule;
	// ErrRepoFileTooLarge above MaxRepoFileBytes (Size is still set).
	File(ctx context.Context, repo RepoRef, sha, path string) (RepoFile, error)
	// ListFiles lists the regular files of the tree at commit sha, at most
	// MaxRepoListEntries; truncated reports a cut listing.
	ListFiles(ctx context.Context, repo RepoRef, sha string) (entries []RepoEntry, truncated bool, err error)
	// PullRequest reads one pull request with its head checks. ErrNotFound when absent.
	PullRequest(ctx context.Context, repo RepoRef, number int) (PullRequest, error)
}

// ForgeWriter is built only with write mode on and a forge configured. It
// writes one new branch and opens one pull request; it never pushes to an
// existing branch, never merges, never deletes a ref it did not create in
// the same call. (Q7)
type ForgeWriter interface {
	// OpenPullRequest commits req.Files on top of req.BaseSHA to the new
	// branch req.Branch and opens a pull request into repo.Base.
	//   - ErrBaseMoved: a file of req.Files differs between req.BaseSHA and
	//     the base head now (a file that did not exist at BaseSHA exists now,
	//     or the reverse). Nothing is written.
	//   - A branch req.Branch that already holds exactly this change (same
	//     parent, same tree) in a commit the App wrote is reused, and an open
	//     pull request on it the App opened is returned instead of a new one,
	//     so a retried call is idempotent; a branch with other content, or a
	//     branch or PR someone else made, is ErrBranchExists.
	OpenPullRequest(ctx context.Context, repo RepoRef, req PullRequestRequest) (PullRequest, error)
}

// PullRequestRequest is one fix PR. Files hold the whole new content of each
// file (an existing file keeps its mode, 100644 or 100755; a new file is
// 100644); the action computes them from the approved
// edits (docs/design/fix-pr.md §6.1). (Q7)
type PullRequestRequest struct {
	BaseSHA       string
	Branch        string // FixBranch(...)
	Title         string
	Body          string
	CommitMessage string
	Draft         bool
	Files         []FileChange
}

// FileChange is one file of a fix PR. Content is never logged. (Q7)
type FileChange struct {
	Path    string
	Content []byte
}

// String hides the content so a FileChange can be printed or logged.
func (f FileChange) String() string { return fmt.Sprintf("{%s %d bytes}", f.Path, len(f.Content)) }

// PullRequest is the state of one pull request. Checks.Failed holds check
// names written by the repository's CI: untrusted, redacted, cut. (Q7)
type PullRequest struct {
	Repo    string `json:"repo"` // RepoRef.ID
	Number  int    `json:"number"`
	URL     string `json:"url"`   // the html URL on the forge's web host
	State   string `json:"state"` // PRStateOpen | PRStateMerged | PRStateClosed
	HeadRef string `json:"head_ref"`
	HeadSHA string `json:"head_sha"`
	// HeadRepo is the "owner/name" of the repository the head branch is in:
	// the PR's own repository, or a fork's. "" when GitHub no longer knows
	// it (a deleted fork).
	HeadRepo string            `json:"head_repo"`
	MergeSHA string            `json:"merge_sha,omitempty"`
	MergedAt time.Time         `json:"merged_at,omitzero"`
	Checks   PullRequestChecks `json:"checks"`
}

// PullRequestChecks folds check runs and commit statuses on the head. (Q7)
type PullRequestChecks struct {
	State  string   `json:"state"` // ChecksNone | ChecksPending | ChecksSuccess | ChecksFailure
	Total  int      `json:"total"`
	Failed []string `json:"failed,omitempty"` // at most 10 names of at most 100 bytes
}

// Pull request and check states. Merged is a state of its own: a merged PR
// is closed on the forge, but the room must never read it as "not applied".
const (
	PRStateOpen   = "open"
	PRStateMerged = "merged"
	PRStateClosed = "closed" // closed without merge
	ChecksNone    = "none"
	ChecksPending = "pending"
	ChecksSuccess = "success"
	ChecksFailure = "failure"
)

// Caps of the PR fix. (Q7)
const (
	MaxRepoFileBytes   = 64 << 10  // one file, before and after the change
	MaxFixFiles        = 10        // files per step
	MaxFixParamsBytes  = 64 << 10  // a step's params (edits, title, body)
	MaxFixDiffBytes    = 128 << 10 // a step's unified diff; a bigger one is refused, never cut
	MaxRepoListEntries = 1000
	MaxRepoPathBytes   = owners.MaxRepoPathBytes
)

// Errors of item 6. ErrNotFound is clients.ErrNotFound (a 404 matches it).
var (
	ErrBaseMoved        = errors.New("clients: a file of the change was modified on the base branch")
	ErrBranchExists     = errors.New("clients: the fix branch exists with other content")
	ErrNotRegularFile   = errors.New("clients: not a regular file")
	ErrRepoFileTooLarge = errors.New("clients: repository file too large")
	ErrRepoPathDenied   = owners.ErrRepoPathDenied // one error for both rule sites
)

// FixBranchPrefix starts every branch a ForgeWriter writes.
const FixBranchPrefix = "piperoom/"

// FixBranch is the only branch name a ForgeWriter writes:
// "piperoom/<roomID>-<proposalID>-<step>". roomID and proposalID are the
// room's own ids (r_..., p_...). (Q7)
func FixBranch(roomID, proposalID string, step int) string {
	return FixBranchPrefix + roomID + "-" + proposalID + "-" + strconv.Itoa(step)
}

var branchRE = regexp.MustCompile(`^[A-Za-z0-9._/-]{1,200}$`)

// ValidBranchName reports whether s is a branch name owners.yaml may use as a
// base: ^[A-Za-z0-9._/-]{1,200}$, no "..", "//", "@{", no leading "-" or "/",
// no trailing "/" or ".", no segment starting with "." or ending in ".lock". (Q7)
func ValidBranchName(s string) bool {
	if !branchRE.MatchString(s) || strings.Contains(s, "..") || strings.Contains(s, "//") || strings.Contains(s, "@{") {
		return false
	}
	if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "/") || strings.HasSuffix(s, "/") || strings.HasSuffix(s, ".") {
		return false
	}
	for _, seg := range strings.Split(s, "/") {
		if strings.HasPrefix(seg, ".") || strings.HasSuffix(seg, ".lock") {
			return false
		}
	}
	return true
}

// CheckRepoPath checks one repository path (or, with glob true, a pattern of
// owners.yaml repos[].paths) against the fixed rules of
// docs/design/fix-pr.md §6.2. It is owners.CheckRepoPath: one rule set for
// what owners.yaml accepts, what a tool reads and what the writer commits, so
// the client never allows a path (a .git* file, a glob character in a plain
// path) that owners.yaml refuses. The error wraps ErrRepoPathDenied, which is
// owners.ErrRepoPathDenied. No setting relaxes it. (Q7)
func CheckRepoPath(p string, glob bool) error { return owners.CheckRepoPath(p, glob) }
