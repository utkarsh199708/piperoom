package fixpr

import (
	"context"
	"encoding/json"
	"fmt"

	"piperoom/internal/actions"
	"piperoom/internal/owners"
)

// Local copies of the Q0 item 6 contract additions of docs/design/fix-pr.md
// §5.2 and §5.3 that the action needs. The leads' contract PR Q7-0c has not
// landed, so internal/actions/action.go and internal/events/events.go do not
// have them yet. Names, fields, JSON tags and semantics follow the design
// exactly; when Q7-0c lands each one becomes an alias of the contract one
// (type StepRef = actions.StepRef, func WithStep = actions.WithStep, ...)
// and no caller changes. The contract change request in the Q7c PR lists
// them.

// Layered is implemented by an action whose approving layer depends on its
// params (repo_open_pull_request: the layer of the path globs the files
// match). Its Layer() returns "". The room service takes the step's layer
// from LayerFor with the room's pipeline, at creation and again before
// Execute, and refuses a step whose Layer differs: the proposer never picks
// the approver. It must not do I/O. (design §5.2; actions.Layered after
// Q7-0c)
type Layered interface {
	LayerFor(p owners.Pipeline, params json.RawMessage) (string, error)
}

// Deferred is implemented by an action whose Execute only asks for a change
// that people apply later outside Piperoom (the repo's reviewers merge the
// PR, the customer's CD deploys it). The room service and the agent apply to
// every step of such an action: never approved by a policy (auto_approve:,
// autonomy:, setup T1); a proposal holding a Deferred step holds only
// Deferred steps, at most MaxDeferredSteps; the agent does not verify on
// action_result but waits for the apply signal. (design §5.2;
// actions.Deferred after Q7-0c)
type Deferred interface {
	Deferred()
}

// MaxDeferredSteps caps the Deferred steps of one proposal. (design §5.2)
const MaxDeferredSteps = 3

// CheckDeferredSteps applies the proposal rules of Deferred to the actions
// of one proposal's steps, in order: a proposal with a Deferred step holds
// only Deferred steps, at most MaxDeferredSteps. A live stop-gap goes in a
// separate proposal (design §14 D4). The room service (Q7k) owns the rule;
// this is the one place it is written down until Q7-0c, so the room and the
// tests share it.
func CheckDeferredSteps(steps []actions.Action) error {
	deferred := 0
	for _, a := range steps {
		if _, ok := a.(Deferred); ok {
			deferred++
		}
	}
	switch {
	case deferred == 0:
		return nil
	case deferred != len(steps):
		return fmt.Errorf("fixpr: a proposal with a pull-request step holds only pull-request steps (%d of %d are); propose a live stop-gap separately", deferred, len(steps))
	case deferred > MaxDeferredSteps:
		return fmt.Errorf("fixpr: a proposal holds at most %d pull-request steps, not %d", MaxDeferredSteps, deferred)
	}
	return nil
}

// PullRequestRef names the PR a step opened (the design's
// events.PullRequestRef, ActionExecutedPayload.PullRequest). (design §5.3)
type PullRequestRef struct {
	Repo    string `json:"repo"` // owners.yaml repos[].id
	Number  int    `json:"number"`
	URL     string `json:"url"`
	Branch  string `json:"branch"`
	HeadSHA string `json:"head_sha"` // the commit Piperoom wrote
}

// PullRequestOpener is implemented by an action whose Execute opens a pull
// request. The executor prefers ExecutePullRequest over Execute and stores
// the reference on action.executed, which starts the room's follower (Q7e).
// The string is Execute's short result. (design §5.2;
// actions.PullRequestOpener after Q7-0c, with events.PullRequestRef)
type PullRequestOpener interface {
	ExecutePullRequest(ctx context.Context, params json.RawMessage) (string, PullRequestRef, error)
}

// StepRef identifies the proposal step being executed. The executor puts it
// in ctx before Execute (WithStep); this action names the fix branch after
// it and refuses to run without it. Approvals are "layer: display name" of
// the approval.given events, in required_layers order. (design §5.2)
type StepRef struct {
	RoomID     string
	ProposalID string
	StepIndex  int
	Title      string // the proposal title, sanitized as stored
	Approvals  []string
}

type stepKey struct{}

// WithStep returns ctx carrying s. (design §5.2; actions.WithStep after Q7-0c)
func WithStep(ctx context.Context, s StepRef) context.Context {
	return context.WithValue(ctx, stepKey{}, s)
}

// StepFrom returns the StepRef WithStep stored. (design §5.2)
func StepFrom(ctx context.Context) (StepRef, bool) {
	s, ok := ctx.Value(stepKey{}).(StepRef)
	return s, ok
}

// DiffPreview is a fix PR step's whole change, read by Preview at BaseSHA.
// Every line passed redact.Text before it was stored, so a secret already in
// the repo never enters the room log; the approved change is the step's
// params, which params_hash covers. Never cut: a step whose diff exceeds
// MaxFixDiffBytes is refused by Validate. (design §5.3, events.DiffPreview;
// PreviewResult.Diff after Q7-0c)
type DiffPreview struct {
	Repo     string     `json:"repo"`
	RepoURL  string     `json:"repo_url"`
	Base     string     `json:"base"`
	BaseSHA  string     `json:"base_sha"`
	Branch   string     `json:"branch"` // the branch Execute will create
	Layer    string     `json:"layer"`
	Files    []FileDiff `json:"files"`
	Hosts    []string   `json:"hosts,omitempty"`    // hosts of URLs in added lines, sorted, unique
	Warnings []string   `json:"warnings,omitempty"` // §6.3 risk flags, one line each
	Draft    bool       `json:"draft,omitempty"`
}

// FileDiff is one file of a DiffPreview. (design §5.3)
type FileDiff struct {
	Path    string `json:"path"`
	Status  string `json:"status"`  // modified | added
	Unified string `json:"unified"` // unified diff, 3 lines of context, redacted
	Added   int    `json:"added"`
	Removed int    `json:"removed"`
}

// File statuses of FileDiff.
const (
	StatusModified = "modified"
	StatusAdded    = "added"
)
