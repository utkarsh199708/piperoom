package forge

// The Q0 item 6 types (docs/design/fix-pr.md §5.1) are the contract's, in
// internal/clients/clients.go since Q7-0c. They are aliased here so the
// package's callers keep writing forge.RepoRef, forge.PullRequest, ... and get
// exactly the clients types: a *Client is a clients.ForgeReader and a *Writer
// a clients.ForgeWriter with no adapter.

import (
	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// Contract types (clients.go, Q7).
type (
	RepoRef            = clients.RepoRef
	RepoInfo           = clients.RepoInfo
	RepoFile           = clients.RepoFile
	RepoEntry          = clients.RepoEntry
	ForgeReader        = clients.ForgeReader
	ForgeWriter        = clients.ForgeWriter
	PullRequestRequest = clients.PullRequestRequest
	FileChange         = clients.FileChange
	PullRequest        = clients.PullRequest
	PullRequestChecks  = clients.PullRequestChecks
)

// RefOf is the forge reference of an owners.yaml repo reference, Host
// included, so the client's host check sees the url's host.
func RefOf(r owners.RepoRef) RepoRef {
	return RepoRef{ID: r.ID, Owner: r.Owner, Name: r.Name, Base: r.Base, Host: r.Host}
}

// Pull request and check states (clients.go).
const (
	PRStateOpen   = clients.PRStateOpen
	PRStateMerged = clients.PRStateMerged
	PRStateClosed = clients.PRStateClosed
	ChecksNone    = clients.ChecksNone
	ChecksPending = clients.ChecksPending
	ChecksSuccess = clients.ChecksSuccess
	ChecksFailure = clients.ChecksFailure
)

// Caps of the PR fix (clients.go).
const (
	MaxRepoFileBytes   = clients.MaxRepoFileBytes
	MaxFixFiles        = clients.MaxFixFiles
	MaxFixParamsBytes  = clients.MaxFixParamsBytes
	MaxFixDiffBytes    = clients.MaxFixDiffBytes
	MaxRepoListEntries = clients.MaxRepoListEntries
	MaxRepoPathBytes   = clients.MaxRepoPathBytes
)

// Errors of item 6 (clients.go). ErrNotFound is clients.ErrNotFound (a 404
// matches it).
var (
	ErrBaseMoved        = clients.ErrBaseMoved
	ErrBranchExists     = clients.ErrBranchExists
	ErrNotRegularFile   = clients.ErrNotRegularFile
	ErrRepoFileTooLarge = clients.ErrRepoFileTooLarge
	ErrRepoPathDenied   = clients.ErrRepoPathDenied
)

// FixBranchPrefix starts every branch a ForgeWriter writes.
const FixBranchPrefix = clients.FixBranchPrefix

// FixBranch is clients.FixBranch: "piperoom/<roomID>-<proposalID>-<step>".
func FixBranch(roomID, proposalID string, step int) string {
	return clients.FixBranch(roomID, proposalID, step)
}

// ValidBranchName is clients.ValidBranchName.
func ValidBranchName(s string) bool { return clients.ValidBranchName(s) }

// CheckRepoPath checks one repository path (or, with glob true, a pattern)
// against both rule sets: the contract's clients.CheckRepoPath and
// owners.CheckRepoPath, which owners.yaml applies. A path passes only when
// both accept it, so the client never writes a path (a .git* file, a glob
// character in a plain path) that owners.yaml refuses. A refusal matches
// both clients.ErrRepoPathDenied and owners.ErrRepoPathDenied. No setting
// relaxes it. (Q7)
func CheckRepoPath(p string, glob bool) error {
	if err := clients.CheckRepoPath(p, glob); err != nil {
		return pathDenied{err}
	}
	if err := owners.CheckRepoPath(p, glob); err != nil {
		return pathDenied{err}
	}
	return nil
}

// pathDenied is a CheckRepoPath refusal: its text is the rule's, and it
// matches the ErrRepoPathDenied of both packages.
type pathDenied struct{ err error }

func (e pathDenied) Error() string { return e.err.Error() }

func (e pathDenied) Unwrap() []error {
	return []error{e.err, clients.ErrRepoPathDenied, owners.ErrRepoPathDenied}
}
