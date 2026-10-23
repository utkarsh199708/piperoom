package room

import (
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/events"
)

// maxSnapshotPullRequests caps RoomSnapshot.PullRequests (docs/api.md "Q7
// snapshot field": at most 20).
const maxSnapshotPullRequests = 20

// PullRequest is one fix PR a step of the room opened, as the snapshot shows
// it (docs/api.md "Q7 snapshot field"). It starts from the step's
// action.executed.pull_request and is updated by every pull_request.updated
// of the same step (Q7e).
type PullRequest struct {
	ProposalID  string          `json:"proposal_id"`
	StepIndex   int             `json:"step_index"`
	Repo        string          `json:"repo"`
	Number      int             `json:"number"`
	URL         string          `json:"url"`
	Branch      string          `json:"branch"`
	State       string          `json:"state"` // open | merged | closed
	HeadChanged bool            `json:"head_changed"`
	Checks      events.PRChecks `json:"checks"`
	MergedAt    *time.Time      `json:"merged_at"`
	// UpdatedAt is the time of the latest pull_request.updated of the PR, the
	// action.executed time before the first one.
	UpdatedAt time.Time `json:"updated_at"`

	// wroteHead is the commit Piperoom wrote (action.executed.pull_request
	// head_sha): head_changed compares the forge's head with it. headSHA and
	// mergeSHA are the last observed values, so the follower writes an event
	// only on a change. Unexported: only the service reads them.
	wroteHead string
	headSHA   string
	mergeSHA  string
	// final is the pull_request.updated that merged or closed the PR, and
	// verified is set once an agent.verification for its proposal followed
	// it. A run that starts while final is set and verified is not gets the
	// pr_merged / pr_closed input again (prReplayInputs): the run that read
	// it may have died with its apply wait (restart, lease move).
	final    *events.Event
	verified bool
}

// ref is the PullRequestRef of the step that opened the PR.
func (pr PullRequest) ref() events.PullRequestRef {
	return events.PullRequestRef{Repo: pr.Repo, Number: pr.Number, URL: pr.URL, Branch: pr.Branch, HeadSHA: pr.wroteHead}
}

// followed reports whether the follower still polls the PR: it is open.
func (pr PullRequest) followed() bool { return pr.State == clients.PRStateOpen }

// addPullRequest folds the PR an action.executed opened in at, newest first.
// A repeated event for the same step changes nothing. Over the cap the oldest
// PR that is no longer open goes first, so a PR the room still follows is
// never dropped while a finished one is kept. It never changes prs.
func addPullRequest(prs []PullRequest, p events.ActionExecutedPayload, at time.Time) []PullRequest {
	for _, pr := range prs {
		if pr.ProposalID == p.ProposalID && pr.StepIndex == p.StepIndex {
			return prs
		}
	}
	ref := *p.PullRequest
	out := make([]PullRequest, 0, len(prs)+1)
	out = append(out, PullRequest{
		ProposalID: p.ProposalID, StepIndex: p.StepIndex,
		Repo: ref.Repo, Number: ref.Number, URL: ref.URL, Branch: ref.Branch,
		State:     clients.PRStateOpen,
		Checks:    events.PRChecks{State: clients.ChecksNone},
		UpdatedAt: at,
		wroteHead: ref.HeadSHA, headSHA: ref.HeadSHA,
	})
	out = append(out, prs...)
	for len(out) > maxSnapshotPullRequests {
		drop := len(out) - 1
		for i := len(out) - 1; i >= 0; i-- {
			if !out[i].followed() {
				drop = i
				break
			}
		}
		out = append(out[:drop], out[drop+1:]...)
	}
	return out
}
