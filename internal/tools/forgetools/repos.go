package forgetools

import (
	"piperoom/internal/clients/forge"
	"piperoom/internal/owners"
)

// Repo is one entry of a pipeline's owners.yaml repos: block, as these tools
// need it: the resolved reference and the path globs with their layers. It
// mirrors owners.Repo and owners.RepoPath of docs/design/fix-pr.md §5.8
// (Q7a, built in parallel); the wiring row adapts owners.Repo to it with
// forge.RefOf(Repo.Ref()) (which keeps the url's Host, so the forge client's
// host check and refKey see it) and Paths until Q7a lands, then this type may
// become an alias.
type Repo struct {
	Ref   forge.RepoRef
	Paths []RepoPath
}

// RepoPath is one glob of a repo and the layer whose owners approve a change
// under it.
type RepoPath struct {
	Glob  string
	Layer string
}

// RepoSource gives the repos: block of a pipeline. With Q7a it is
// func(p) { return convert(p.Repos) }; until then wiring passes nothing and
// the tools are not registered.
type RepoSource interface {
	Repos(p owners.Pipeline) []Repo
}

// RepoSourceFunc adapts a function to RepoSource.
type RepoSourceFunc func(p owners.Pipeline) []Repo

// Repos calls f.
func (f RepoSourceFunc) Repos(p owners.Pipeline) []Repo { return f(p) }

// Errors of LayerOf: owners.ErrPathNotListed and owners.ErrPathAmbiguous
// (design §5.8), so a caller matches either package's name.
var (
	ErrPathNotListed = owners.ErrPathNotListed
	ErrPathAmbiguous = owners.ErrPathAmbiguous
)

// LayerOf returns the layer of the globs that match p (design §3 rule 5). It
// is owners.Repo.LayerOf, the rule owners.yaml itself applies: CheckRepoPath
// first, no match is ErrPathNotListed, matches of two layers are
// ErrPathAmbiguous (never first-match), and the glob matcher is the owners
// one (a table, not backtracking, so many "**" segments stay cheap). A glob
// that fails CheckRepoPath(glob, true) matches nothing.
func (r Repo) LayerOf(p string) (string, error) {
	or := owners.Repo{ID: r.Ref.ID}
	for _, g := range r.Paths {
		if owners.CheckRepoPath(g.Glob, true) != nil {
			continue
		}
		or.Paths = append(or.Paths, owners.RepoPath{Glob: g.Glob, Layer: g.Layer})
	}
	return or.LayerOf(p)
}

// matchGlob matches p against a glob: owners.MatchGlob.
func matchGlob(glob, p string) bool { return owners.MatchGlob(glob, p) }
