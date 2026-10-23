// Package fixpr holds the pull-request fix action of the Q wave's step 7
// (Q-board row Q7c, docs/design/fix-pr.md §6): repo_open_pull_request opens
// one pull request that changes text files in a repository the room's
// pipeline lists in owners.yaml repos:, under path globs of one layer, on a
// new piperoom/ branch from the commit the agent read.
//
// Validate and Preview read only, through forge.ForgeReader; Execute writes
// only through forge.ForgeWriter, which wiring builds with write mode on.
// Piperoom never merges: the repository's own reviewers and CI decide. The
// approving layer comes from the globs (Layered), the action is never
// approved by a policy (Deferred), and the step's branch is named after the
// proposal step the executor puts in ctx (StepFrom). Those three contract
// items (Q0 item 6) have not landed in internal/actions yet: contract.go
// holds local copies under the design's names. Wiring (Q7f) registers the
// action; this package registers nothing.
package fixpr
