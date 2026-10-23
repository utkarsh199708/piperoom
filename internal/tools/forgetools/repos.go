package forgetools

import (
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"piperoom/internal/clients/forge"
	"piperoom/internal/owners"
)

// Repo is one entry of a pipeline's owners.yaml repos: block, as these tools
// need it: the resolved reference and the path globs with their layers. It
// mirrors owners.Repo and owners.RepoPath of docs/design/fix-pr.md §5.8
// (Q7a, built in parallel); the wiring row adapts owners.Repo to it with
// Repo.Ref() and Paths until Q7a lands, then this type may become an alias.
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

// Errors of LayerOf (owners.ErrPathNotListed and owners.ErrPathAmbiguous of
// design §5.8, local until Q7a lands).
var (
	ErrPathNotListed = errors.New("forgetools: path is not under the repo's globs")
	ErrPathAmbiguous = errors.New("forgetools: path matches globs of two layers")
)

// LayerOf returns the layer of the globs that match p (design §3 rule 5):
// CheckRepoPath first, no match is ErrPathNotListed, matches of two layers
// are ErrPathAmbiguous (never first-match).
func (r Repo) LayerOf(p string) (string, error) {
	if err := forge.CheckRepoPath(p, false); err != nil {
		return "", err
	}
	layers := map[string]bool{}
	for _, g := range r.Paths {
		if matchGlob(g.Glob, p) {
			layers[g.Layer] = true
		}
	}
	switch len(layers) {
	case 0:
		return "", fmt.Errorf("%w: %s", ErrPathNotListed, p)
	case 1:
		for l := range layers {
			return l, nil
		}
	}
	names := make([]string, 0, len(layers))
	for l := range layers {
		names = append(names, l)
	}
	sort.Strings(names)
	return "", fmt.Errorf("%w: %s (%s)", ErrPathAmbiguous, p, strings.Join(names, ", "))
}

// matchGlob matches p against a glob: path.Match per segment, where "*" does
// not cross "/", plus "**" as a whole segment matching zero or more
// segments. A malformed glob matches nothing.
func matchGlob(glob, p string) bool {
	if forge.CheckRepoPath(glob, true) != nil {
		return false
	}
	return matchSegs(strings.Split(glob, "/"), strings.Split(p, "/"))
}

func matchSegs(g, p []string) bool {
	for len(g) > 0 {
		if g[0] == "**" {
			for i := 0; i <= len(p); i++ {
				if matchSegs(g[1:], p[i:]) {
					return true
				}
			}
			return false
		}
		if len(p) == 0 {
			return false
		}
		if ok, err := path.Match(g[0], p[0]); err != nil || !ok {
			return false
		}
		g, p = g[1:], p[1:]
	}
	return len(p) == 0
}
