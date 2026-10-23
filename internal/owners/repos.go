package owners

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// This file holds the per-pipeline `repos:` block of owners.yaml (Q7a,
// docs/design/fix-pr.md §3 and §5.8): the repositories a pull-request fix may
// change, and which layer approves each path in them.
//
//	Pipeline.Repos / Repo(id)        the repos of one pipeline
//	Repo.LayerOf / Allows            the approving layer of one path (never first-match)
//	Repo.Ref                         the forge reference a client is called with
//	Owners.TenantRepo                resolve a tenant's repo id (setup rooms, §3.10)
//	CheckRepoPath / ValidBranchName  the fixed path and branch rules
//
// RepoRef, CheckRepoPath, ValidBranchName, MaxRepoPathBytes and
// ErrRepoPathDenied are Q0 item 6 contract items (design §5.1) that have not
// landed in internal/clients yet. They are defined here with the design's
// names and shapes, so the contract PR turns them into aliases of the clients
// ones (Q7a's contract change request).

// RepoRef is one repository of a pipeline's repos: block, resolved by the
// caller from the repo id. The model only ever names the id; Owner, Name and
// Base come from owners.yaml. Same fields and JSON tags as the design's
// clients.RepoRef (§5.1).
type RepoRef struct {
	ID    string `json:"id"`    // owners.yaml repos[].id
	Owner string `json:"owner"` // from repos[].url: https://<host>/<owner>/<name>
	Name  string `json:"name"`
	Base  string `json:"base"` // repos[].base, the branch PRs target
	// Host is the host of repos[].url. The forge client refuses a ref whose
	// Host is not its web host, so a url on another host is never read or
	// written with this forge's App.
	Host string `json:"host,omitempty"`
}

// MaxRepoPathBytes bounds one repository path or glob (design §5.1).
const MaxRepoPathBytes = 255

// ErrRepoPathDenied is wrapped by every CheckRepoPath refusal (design §5.1).
var ErrRepoPathDenied = errors.New("clients: repository path is not allowed")

// ErrPathNotListed: no glob of the repo matches the path, so no layer owns it
// and a fix may not change it.
var ErrPathNotListed = errors.New("owners: path is not listed in the repo's paths")

// ErrPathAmbiguous: globs of two different layers match the path. It is
// refused rather than resolved by glob order: who approves a change must not
// depend on the order an admin wrote the globs in.
var ErrPathAmbiguous = errors.New("owners: path matches globs of two layers")

// ErrRepoNotListed: the pipeline's repos: block has no repo with that id.
var ErrRepoNotListed = errors.New("owners: repo is not listed in the pipeline's repos")

// Repo is one entry of a pipeline's repos: block (design §5.8).
type Repo struct {
	// ID is a slug (repoIDRE). One id names one repository in the whole file:
	// the same id in two pipelines has the same url and base.
	ID string `yaml:"id"`
	// URL is https://<host>/<owner>/<name>, nothing more. The host is checked
	// against the forge's web host by CheckRepoHosts at start, and again by
	// the forge client on every call through RepoRef.Host (Load reads no
	// config).
	URL string `yaml:"url"`
	// Base is the branch the fix PRs target.
	Base string `yaml:"base"`
	// Draft opens the PRs as drafts.
	Draft bool `yaml:"draft"`
	// PublicOK allows a public repository (the action refuses one otherwise).
	PublicOK bool `yaml:"public_ok"`
	// Paths are the globs a fix may change, each with its approving layer.
	Paths []RepoPath `yaml:"paths"`

	// owner and name are parsed from URL by Load.
	owner, name string
}

// RepoPath is one glob of a repo and the layer that approves a change to the
// files it matches.
type RepoPath struct {
	Glob  string `yaml:"glob"`
	Layer string `yaml:"layer"`
}

// Repo returns the pipeline's repo with the given id, as a copy the caller may
// change.
func (p Pipeline) Repo(id string) (Repo, bool) {
	for _, r := range p.Repos {
		if r.ID == id {
			r.Paths = slices.Clone(r.Paths)
			return r, true
		}
	}
	return Repo{}, false
}

// RepoLayerOf answers which layer of the pipeline approves a change to path in
// repo repoID: ErrRepoNotListed for a repo the pipeline does not list, else
// Repo.LayerOf.
func (p Pipeline) RepoLayerOf(repoID, path string) (string, error) {
	r, ok := p.Repo(repoID)
	if !ok {
		return "", fmt.Errorf("pipeline %q: repo %s: %w", p.ID, quoteName(repoID), ErrRepoNotListed)
	}
	return r.LayerOf(path)
}

// Ref returns the forge reference of the repo. Owner and Name are filled for a
// repo of a loaded file; a Repo built in code without Load has them parsed from
// URL here, and left empty when URL is not a valid repo URL.
func (r Repo) Ref() RepoRef {
	owner, name := r.owner, r.name
	if owner == "" || name == "" {
		owner, name, _ = parseRepoURL(r.URL)
	}
	return RepoRef{ID: r.ID, Owner: owner, Name: name, Base: r.Base, Host: repoURLHost(r.URL)}
}

// repoURLHost returns the lower-cased host (with any port) of a repo url,
// "" when it does not parse.
func repoURLHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Host)
}

// CheckRepoHosts refuses a file whose repos[].url names a host other than
// webHost, the forge's web host (forge.Client.WebHost: github.com for
// api.github.com, else the API host). Wiring calls it at start, once the
// forge is configured; the error names the pipeline and repo id, never the
// url.
func (o *Owners) CheckRepoHosts(webHost string) error {
	if o == nil {
		return nil
	}
	want := strings.ToLower(webHost)
	for _, p := range o.Pipelines {
		for _, r := range p.Repos {
			if h := repoURLHost(r.URL); h == "" || h != want {
				return fmt.Errorf("pipeline %q: repo %s: url host is not the forge's web host %q", p.ID, quoteName(r.ID), webHost)
			}
		}
	}
	return nil
}

// LayerOf returns the layer of the globs that match path. The path is checked
// with CheckRepoPath first, so a denied path (CI files, CODEOWNERS, .git*) is
// refused whatever the globs say. No match is ErrPathNotListed; matches of two
// different layers are ErrPathAmbiguous, never the first match.
func (r Repo) LayerOf(p string) (string, error) {
	if err := CheckRepoPath(p, false); err != nil {
		return "", err
	}
	layer := ""
	for _, rp := range r.Paths {
		if !matchGlob(rp.Glob, p) {
			continue
		}
		switch {
		case layer == "":
			layer = rp.Layer
		case layer != rp.Layer:
			return "", fmt.Errorf("repo %s: %w (%s and %s)", quoteName(r.ID), ErrPathAmbiguous, quoteName(layer), quoteName(rp.Layer))
		}
	}
	if layer == "" {
		return "", fmt.Errorf("repo %s: %w", quoteName(r.ID), ErrPathNotListed)
	}
	return layer, nil
}

// Allows reports whether a fix may change path: LayerOf succeeds.
func (r Repo) Allows(p string) bool {
	_, err := r.LayerOf(p)
	return err == nil
}

// TenantRepo resolves a repo id for a pipeline of tenant: the first pipeline
// of that tenant (file order) that lists it. Load guarantees every pipeline
// that lists one id has the same url and base, and that pipelines of another
// tenant do not list it, so which pipeline answers does not change the url or
// the base. The globs are that pipeline's. false when no pipeline of the
// tenant lists the id.
func (o *Owners) TenantRepo(tenant, id string) (Repo, bool) {
	if o == nil {
		return Repo{}, false
	}
	for _, p := range o.Pipelines {
		if p.TenantName() != tenant {
			continue
		}
		if r, ok := p.Repo(id); ok {
			return r, true
		}
	}
	return Repo{}, false
}

// validateRepos checks one pipeline's repos: block (design §3 rules 1 to 6;
// the cross-pipeline half of rule 1 is validateRepoIDs). It fills each repo's
// parsed owner and name. Errors name the repo id and the field, never a value
// that is not a plain name.
func (p *Pipeline) validateRepos() error {
	ids := map[string]bool{}
	urls := map[string]string{} // lower-cased owner/name@host -> repo id
	layers := p.Layers()
	for i := range p.Repos {
		r := &p.Repos[i]
		if !repoIDRE.MatchString(r.ID) {
			return fmt.Errorf("repos[%d]: id %s must match %s", i, quoteName(r.ID), repoIDRE)
		}
		if ids[r.ID] {
			return fmt.Errorf("repos: id %q is listed twice", r.ID)
		}
		ids[r.ID] = true
		owner, name, err := parseRepoURL(r.URL)
		if err != nil {
			return fmt.Errorf("repo %q: url: %w", r.ID, err)
		}
		r.owner, r.name = owner, name
		// Two ids for one repository in one pipeline would give one file two
		// sets of globs, and so possibly two approving layers.
		key := strings.ToLower(r.URL)
		if prev, ok := urls[key]; ok {
			return fmt.Errorf("repos %q and %q: url names the same repository twice; list it once", prev, r.ID)
		}
		urls[key] = r.ID
		if !ValidBranchName(r.Base) {
			return fmt.Errorf("repo %q: base: want a branch name (letters, digits, '.', '_', '-' and '/'; no '..', no segment starting with '.' or ending in .lock)", r.ID)
		}
		if strings.HasPrefix(r.Base, fixBranchPrefix) {
			return fmt.Errorf("repo %q: base: a branch under %s is a fix branch Piperoom writes, not a base", r.ID, fixBranchPrefix)
		}
		if err := checkRepoPaths(r.Paths, layers); err != nil {
			return fmt.Errorf("repo %q: %w", r.ID, err)
		}
	}
	return nil
}

// fixBranchPrefix is the prefix of every branch a fix PR writes
// (clients.FixBranch, design §5.1).
const fixBranchPrefix = "piperoom/"

// checkRepoPaths checks a repo's paths: list: not empty, every glob a valid
// pattern outside the deny list, every layer one of the pipeline's, no glob
// twice, and no literal glob (one without a wildcard) that a glob of another
// layer also matches. The last rule catches the overlaps Load can decide
// without enumerating the repository; any other overlap is refused per path
// by LayerOf (ErrPathAmbiguous).
func checkRepoPaths(paths []RepoPath, layers []string) error {
	if len(paths) == 0 {
		return fmt.Errorf("paths: lists no glob (a repo with no paths allows no change)")
	}
	seen := map[string]bool{}
	for i, rp := range paths {
		if err := CheckRepoPath(rp.Glob, true); err != nil {
			return fmt.Errorf("paths[%d]: glob: %w", i, err)
		}
		if rp.Layer == "" {
			return fmt.Errorf("paths[%d]: layer: is required (the layer whose owner approves a change to these files)", i)
		}
		if !slices.Contains(layers, rp.Layer) {
			return fmt.Errorf("paths[%d]: layer %s is not a layer of the pipeline (want one of %s)", i, quoteName(rp.Layer), strings.Join(layers, ", "))
		}
		if seen[rp.Glob] {
			return fmt.Errorf("paths[%d]: the glob is listed twice", i)
		}
		seen[rp.Glob] = true
	}
	for i, lit := range paths {
		if hasGlobMeta(lit.Glob) {
			continue
		}
		for j, other := range paths {
			if i != j && other.Layer != lit.Layer && matchGlob(other.Glob, lit.Glob) {
				return fmt.Errorf("paths[%d] (layer %s) and paths[%d] (layer %s) match the same file; one file has one approving layer", i, quoteName(lit.Layer), j, quoteName(other.Layer))
			}
		}
	}
	return nil
}

// validateRepoIDs checks the cross-pipeline rules: one repo id names one
// repository in the whole file (the same url and base wherever it appears),
// and, with a tenants: block, belongs to one tenant, like a team. A tenant's
// repos: list (the setup form's default repos) may not name an id that only
// another tenant's pipelines list.
func (o *Owners) validateRepoIDs() error {
	type first struct {
		pipeline, url, base, tenant string
	}
	byID := map[string]first{}
	for _, p := range o.Pipelines {
		for _, r := range p.Repos {
			f, ok := byID[r.ID]
			if !ok {
				byID[r.ID] = first{p.ID, r.URL, r.Base, p.TenantName()}
				continue
			}
			switch {
			case !strings.EqualFold(f.url, r.URL):
				return fmt.Errorf("repo %q: pipelines %q and %q give it two urls; a repo id names one repository in the file", r.ID, f.pipeline, p.ID)
			case f.base != r.Base:
				return fmt.Errorf("repo %q: pipelines %q and %q give it two bases; a repo id names one repository in the file", r.ID, f.pipeline, p.ID)
			case f.tenant != p.TenantName():
				return fmt.Errorf("repo %q: listed by pipelines of tenants %q (%q) and %q (%q); a repo serves one tenant", r.ID, f.tenant, f.pipeline, p.TenantName(), p.ID)
			}
		}
	}
	names := o.TenantNames()
	for _, t := range names {
		for _, id := range o.Tenants[t].Repos {
			if f, ok := byID[id]; ok && f.tenant != t {
				return fmt.Errorf("tenants.%s: repos: %q is listed by pipeline %q of tenant %q, not this tenant", t, id, f.pipeline, f.tenant)
			}
		}
	}
	return nil
}

// repoSegmentRE is the owner and name charset of a repo url.
var repoSegmentRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// parseRepoURL checks url against design §3 rule 2 and returns its owner and
// name. The error never quotes the url: a token pasted into it must not reach
// the start-up log.
func parseRepoURL(raw string) (owner, name string, err error) {
	if raw == "" {
		return "", "", errors.New("is required (https://<host>/<owner>/<name>)")
	}
	// '%' is refused outright so an encoded '/' or '.' cannot hide a segment.
	if strings.ContainsAny(raw, "?#%\\ \t\r\n") {
		return "", "", errors.New("want https://<host>/<owner>/<name> with no query, fragment, escape or space")
	}
	u, perr := url.Parse(raw)
	if perr != nil {
		return "", "", errors.New("is not a url (want https://<host>/<owner>/<name>)")
	}
	switch {
	case u.Scheme != "https":
		return "", "", errors.New("want an https:// url")
	case u.User != nil:
		return "", "", errors.New("must not carry user info (a token in the url would be written to every log); the forge credentials are a setting")
	case u.Opaque != "" || u.Hostname() == "":
		return "", "", errors.New("want https://<host>/<owner>/<name>")
	}
	segs := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if !strings.HasPrefix(u.Path, "/") || len(segs) != 2 {
		return "", "", errors.New("want exactly https://<host>/<owner>/<name> (no extra path, no trailing '/')")
	}
	owner, name = segs[0], segs[1]
	for _, s := range []string{owner, name} {
		if !repoSegmentRE.MatchString(s) || strings.Trim(s, ".") == "" {
			return "", "", errors.New("owner and name must match ^[A-Za-z0-9_.-]+$ and not be only dots")
		}
	}
	if strings.HasSuffix(strings.ToLower(name), ".git") {
		return "", "", errors.New("drop the .git suffix (the url is the repository's web address)")
	}
	return owner, name, nil
}

// Deny list of design §6.2 rule 4: CI and repository-governance files. A
// change to one runs with the repository's secrets on a same-repo PR, or
// decides who reviews. Matched case-insensitively on every segment.
var (
	deniedDirs = []string{
		".github", ".git", ".gitlab", ".circleci", ".buildkite", ".azure-pipelines",
		".woodpecker", ".husky", ".devcontainer",
	}
	deniedFiles = []string{
		"codeowners", ".gitmodules", ".gitattributes", ".gitignore", ".gitlab-ci.yml",
		".travis.yml", ".drone.yml", "jenkinsfile", "azure-pipelines.yml",
		"bitbucket-pipelines.yml", ".pre-commit-config.yaml",
	}
)

// CheckRepoPath checks one repository path, or with glob true a pattern of
// owners.yaml repos[].paths, against the fixed rules of design §6.2: valid
// UTF-8, clean, relative, at most MaxRepoPathBytes, no empty, "." or ".."
// segment, no backslash, NUL or control character, and not under the deny
// list. A plain path may not hold a glob character ('*', '?', '[', ']'); a
// glob must be a valid path.Match pattern per segment, with "**" only as a
// whole segment. On top of the design's list, any segment starting with
// ".git" is denied (".gitkeep" and the like are never a pipeline fix). The
// error wraps ErrRepoPathDenied and never quotes the path. No setting relaxes
// it.
func CheckRepoPath(p string, glob bool) error {
	deny := func(why string) error { return fmt.Errorf("%w: %s", ErrRepoPathDenied, why) }
	switch {
	case p == "":
		return deny("empty")
	case len(p) > MaxRepoPathBytes:
		return deny(fmt.Sprintf("longer than %d bytes", MaxRepoPathBytes))
	case !utf8.ValidString(p):
		return deny("not valid UTF-8")
	case strings.ContainsFunc(p, func(r rune) bool { return r == '\\' || unicode.IsControl(r) }):
		return deny("holds a backslash or a control character")
	case strings.HasPrefix(p, "/"):
		return deny("absolute (want a path relative to the repository root)")
	}
	segs := strings.Split(p, "/")
	for i, s := range segs {
		if s == "" || s == "." || s == ".." {
			return deny("has an empty, '.' or '..' segment (want a clean relative path)")
		}
		if glob {
			if s != "**" && strings.Contains(s, "**") {
				return deny("'**' must be a whole segment")
			}
			if _, err := path.Match(s, ""); err != nil {
				return deny("not a valid glob")
			}
		} else if strings.ContainsAny(s, "*?[]") {
			return deny("holds a glob character")
		}
		low := strings.ToLower(s)
		if strings.HasPrefix(low, ".git") || slices.Contains(deniedDirs, low) {
			return deny("under a CI or repository-governance directory, or a .git file")
		}
		if i == len(segs)-1 && slices.Contains(deniedFiles, low) {
			return deny("a CI or repository-governance file")
		}
	}
	return nil
}

// matchGlob reports whether pattern matches the whole of name: each segment
// with path.Match (so '*' never crosses '/'), plus "**" as a whole segment
// that matches zero or more segments. Both are checked by CheckRepoPath
// before they get here; a bad pattern matches nothing.
func matchGlob(pattern, name string) bool {
	pat, segs := strings.Split(pattern, "/"), strings.Split(name, "/")
	// match[i][j]: pat[i:] matches segs[j:]. A table rather than backtracking,
	// so a glob of many "**" segments costs len(pat)*len(segs), not an
	// exponential search on a path the model chose.
	match := make([][]bool, len(pat)+1)
	for i := range match {
		match[i] = make([]bool, len(segs)+1)
	}
	match[len(pat)][len(segs)] = true
	for i := len(pat) - 1; i >= 0; i-- {
		for j := len(segs); j >= 0; j-- {
			if pat[i] == "**" {
				// zero segments, or one more segment under the same "**"
				match[i][j] = match[i+1][j] || (j < len(segs) && match[i][j+1])
				continue
			}
			if j < len(segs) && match[i+1][j+1] {
				ok, err := path.Match(pat[i], segs[j])
				match[i][j] = err == nil && ok
			}
		}
	}
	return match[0][0]
}

// MatchGlob reports whether a repos[].paths glob matches the whole of a
// repository path, by the rules LayerOf uses (matchGlob): one matcher for
// owners.yaml and the forge tools, polynomial in the number of "**"
// segments. A glob that fails CheckRepoPath(glob, true) matches nothing.
func MatchGlob(pattern, name string) bool {
	if CheckRepoPath(pattern, true) != nil {
		return false
	}
	return matchGlob(pattern, name)
}

// hasGlobMeta reports whether a glob holds a wildcard.
func hasGlobMeta(g string) bool { return strings.ContainsAny(g, "*?[") }

// ValidBranchName reports whether s is a branch name owners.yaml may use as a
// base (design §5.1): ^[A-Za-z0-9._/-]{1,200}$, no "..", "//" or "@{", no
// leading '-' or '/', no trailing '/' or '.', no segment starting with '.' or
// ending in ".lock".
func ValidBranchName(s string) bool {
	if len(s) == 0 || len(s) > 200 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '.' && c != '_' && c != '/' && c != '-' {
			return false
		}
	}
	if strings.Contains(s, "..") || strings.Contains(s, "//") || strings.Contains(s, "@{") ||
		s[0] == '-' || s[0] == '/' || strings.HasSuffix(s, "/") || strings.HasSuffix(s, ".") {
		return false
	}
	for _, seg := range strings.Split(s, "/") {
		if strings.HasPrefix(seg, ".") || strings.HasSuffix(seg, ".lock") {
			return false
		}
	}
	return true
}
