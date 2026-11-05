package wiring

import (
	"fmt"
	"log/slog"

	"piperoom/internal/actions"
	"piperoom/internal/actions/fixpr"
	"piperoom/internal/clients"
	"piperoom/internal/clients/forge"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/forgetools"
)

// The forge of the pull-request fix (Q7f, docs/design/fix-pr.md §10).
//
// FORGE_KIND empty (the default): nothing is built, no repository tool and
// no repo_open_pull_request is registered, and every tool list stays as it
// was. FORGE_KIND=github: one GitHub App client for the process, shared by
// every client set (the App, not a pipeline, holds the credential). Its
// reader serves the repository tools and the action's Validate and Preview
// in both write modes; the writer is built only with write mode on
// (PIPEROOM_WRITE_MODE=on and not StackOptions.ReadOnly), so with write mode
// off no ForgeWriter exists anywhere in the process (the P1a-3 test covers
// it), and the action refuses at Execute while the proposal is still
// created, previewed and approved (P0 ruling #606).

// forgeReader and forgeWriter are the forge interfaces the set holds: the
// contract's clients.ForgeReader and clients.ForgeWriter.
type (
	forgeReader = clients.ForgeReader
	forgeWriter = clients.ForgeWriter
)

// forgeSet is the process's forge, as each client set holds it. The zero
// value is "no forge".
type forgeSet struct {
	reader forgeReader
	// writer is nil unless write mode is on.
	writer forgeWriter
	// webHost is the forge's web host (config Forge.WebHost); publicURL is
	// PIPEROOM_PUBLIC_URL, which the PR footer links the room on.
	webHost   string
	publicURL string
}

// on reports whether a forge is configured.
func (f forgeSet) on() bool { return f.reader != nil }

// forgeOpener builds the process's forge; tests replace it.
type forgeOpener func(cfg config.Config, write bool) (forgeSet, error)

// openForge builds the forge from cfg: nothing with FORGE_KIND empty, else
// the GitHub App reader (the key file is read once here, and a key that is
// not PEM RSA stops the start) and, with write mode on only, its writer. No
// request is sent.
func openForge(cfg config.Config, write bool) (forgeSet, error) {
	f, err := config.ParseForge(cfg)
	if err != nil {
		return forgeSet{}, fmt.Errorf("wiring: %w", err)
	}
	if f.Kind == config.ForgeKindOff {
		return forgeSet{}, nil
	}
	key, err := forge.LoadPrivateKeyFile(f.PrivateKeyFile, slog.Default())
	if err != nil {
		return forgeSet{}, fmt.Errorf("wiring: forge: %w", err)
	}
	c, err := forge.New(forge.Config{APIURL: f.APIURL, AppID: f.AppID, InstallationID: f.InstallationID, Key: key})
	if err != nil {
		return forgeSet{}, fmt.Errorf("wiring: forge: %w", err)
	}
	fs := forgeSet{reader: c, webHost: c.WebHost(), publicURL: cfg.PublicURL}
	if write {
		w, err := forge.NewWriter(c)
		if err != nil {
			return forgeSet{}, fmt.Errorf("wiring: forge writer: %w", err)
		}
		fs.writer = w
	}
	return fs, nil
}

// registerForgeTools registers repo_list_files, repo_read_file and pr_status
// over the set's forge reader; nothing without a forge. The room's record
// of opened PRs (forgetools.Deps.Opened) is the room service's and is not
// wired here, so pr_status reports head_changed as unknown (follow-up).
func registerForgeTools(reg *tools.Registry, w *Wired) error {
	if !w.forge.on() {
		return nil
	}
	return forgetools.Register(reg, forgetools.Deps{Forge: w.forge.reader, Owners: w.Owners, Repos: forgetools.RepoSourceFunc(forgeRepos)})
}

// forgeRepos is a pipeline's repos: block as the forge tools take it.
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

// registerFixPR registers repo_open_pull_request over the set's forge: the
// reader always, the writer only with write mode on. Nothing without a forge.
func registerFixPR(areg *actions.Registry, w *Wired) error {
	if !w.forge.on() {
		return nil
	}
	d := fixpr.Deps{Reader: w.forge.reader, WebHost: w.forge.webHost, PublicURL: w.forge.publicURL}
	if w.write && w.forge.writer != nil {
		d.Writer = w.forge.writer
	}
	return areg.Register(fixpr.New(d))
}
