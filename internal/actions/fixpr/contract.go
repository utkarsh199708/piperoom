package fixpr

import "piperoom/internal/events"

// The Q0 item 6 additions of docs/design/fix-pr.md §5.2 and §5.3 are the
// contract's since Q7-0c: actions.Layered, actions.Deferred,
// actions.PullRequestOpener, actions.StepRef with actions.WithStep and
// actions.StepFrom, events.PullRequestRef and events.DiffPreview
// (actions.PreviewResult.Diff). The action uses them directly; only the
// diff types are aliased, for the package's own diff code.

// DiffPreview is the contract's events.DiffPreview (PreviewResult.Diff).
type DiffPreview = events.DiffPreview

// FileDiff is the contract's events.FileDiff.
type FileDiff = events.FileDiff

// File statuses of FileDiff.
const (
	StatusModified = "modified"
	StatusAdded    = "added"
)
