package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"piperoom/internal/clients"
	"piperoom/internal/clients/forge"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

// forgeSetup builds the room's pull request reader (Q7e, the fix PR
// follower) from FORGE_KIND and the GITHUB_APP_* settings
// (config.ParseForge). FORGE_KIND empty: no reader (nil, nil), and a
// pipeline that lists repos: logs one warning, since the PR fix is then off
// (docs/design/fix-pr.md §3 rule 7). A forge that is configured but cannot be
// built stops the start, and so does a repos[].url on another host than the
// forge's web host (design §5.6): this App could never reach it.
//
// The fix PR tools and the action (Q7d, Q7c) are wired by internal/wiring
// (Q7f); this reader only follows the PRs a step opened.
func forgeSetup(cfg config.Config, own *owners.Owners, log *slog.Logger) (room.PullRequestReader, error) {
	f, err := config.ParseForge(cfg)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	if f.Kind == config.ForgeKindOff {
		if anyRepos(own) {
			log.Warn("server: repos: is set but no forge is configured; the PR fix is off")
		}
		return nil, nil
	}
	if err := own.CheckRepoHosts(f.WebHost); err != nil {
		return nil, fmt.Errorf("server: owners.yaml repos: %w", err)
	}
	key, err := forge.LoadPrivateKeyFile(f.PrivateKeyFile, log)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	c, err := forge.New(forge.Config{APIURL: f.APIURL, AppID: f.AppID, InstallationID: f.InstallationID, Key: key})
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	log.Info("server: forge", "kind", f.Kind, "web_host", f.WebHost)
	return forgePRReader{c: c}, nil
}

// anyRepos reports whether a pipeline of own lists repos:.
func anyRepos(own *owners.Owners) bool {
	if own == nil {
		return false
	}
	for _, p := range own.Pipelines {
		if len(p.Repos) > 0 {
			return true
		}
	}
	return false
}

// forgePR is the forge client's PullRequest method (forge.ForgeReader).
type forgePR interface {
	PullRequest(ctx context.Context, repo forge.RepoRef, number int) (forge.PullRequest, error)
}

// forgePRReader adapts the forge client (whose types are its local copies of
// Q0 item 6) to room.PullRequestReader (the clients contract types), and
// turns its rate-limit answer into room.PRRateLimitError.
type forgePRReader struct{ c forgePR }

func (r forgePRReader) PullRequest(ctx context.Context, repo clients.RepoRef, number int) (clients.PullRequest, error) {
	pr, err := r.c.PullRequest(ctx, forge.RepoRef{ID: repo.ID, Owner: repo.Owner, Name: repo.Name, Base: repo.Base}, number)
	if err != nil {
		if errors.Is(err, forge.ErrRateLimited) {
			rl := &room.PRRateLimitError{Err: err}
			var se *forge.StatusError
			if errors.As(err, &se) {
				rl.Reset = se.Reset
			}
			return clients.PullRequest{}, rl
		}
		return clients.PullRequest{}, err
	}
	return clients.PullRequest{
		Repo: pr.Repo, Number: pr.Number, URL: pr.URL, State: pr.State,
		HeadRef: pr.HeadRef, HeadSHA: pr.HeadSHA, MergeSHA: pr.MergeSHA, MergedAt: pr.MergedAt,
		Checks: clients.PullRequestChecks{State: pr.Checks.State, Total: pr.Checks.Total, Failed: pr.Checks.Failed},
	}, nil
}
