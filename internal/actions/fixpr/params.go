package fixpr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/forge"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// Params of repo_open_pull_request (design §6.1). base_sha and the edits
// fully determine every new file, so params_hash covers the whole diff
// without the diff in params.
type params struct {
	Repo        string      `json:"repo"`
	BaseSHA     string      `json:"base_sha"`
	Title       string      `json:"title"`
	Body        string      `json:"body"`
	Changes     []change    `json:"changes"`
	AppliedWhen appliedWhen `json:"applied_when"`
}

// change is one file: edits of an existing file, or the content of a new
// one. Never both, and never a delete (design §14 D1).
type change struct {
	Path    string  `json:"path"`
	Edits   []edit  `json:"edits,omitempty"`
	Content *string `json:"content,omitempty"`
}

// edit replaces Old, which must occur exactly once in the file at base_sha
// after the earlier edits of the same file, by New.
type edit struct {
	Old string `json:"old"`
	New string `json:"new"`
}

// appliedWhen says how Piperoom will know the merged change reached the
// running system (design §8.2); the agent polls it after pr_merged (Q7f).
type appliedWhen struct {
	Kind      string            `json:"kind"`
	Connector string            `json:"connector,omitempty"`
	Keys      map[string]string `json:"keys,omitempty"`
	Class     string            `json:"class,omitempty"`
	Version   string            `json:"version,omitempty"`
	Kinds     []string          `json:"kinds,omitempty"`
	Service   string            `json:"service,omitempty"`
	Settle    int               `json:"settle,omitempty"` // minutes, merge kind only
}

// Kinds of applied_when (design §8.2).
const (
	AppliedConnectorConfig = "connector_config"
	AppliedPlugin          = "plugin"
	AppliedChange          = "change"
	AppliedMerge           = "merge"
)

// Limits of the params (design §6.1, §6.2).
const (
	maxTitleRunes   = 120
	maxTitleBytes   = 240 // after mention neutralising; the writer caps at 256
	maxBodyBytes    = 4 << 10
	maxEditsPerFile = 20
	maxAppliedKeys  = 20
	maxSettle       = 60
	maxNameBytes    = 255
)

var shaRE = regexp.MustCompile(`^[0-9a-f]{40}$`)

// paramsSchema is the JSON Schema of the params; additionalProperties is
// false at every level, so the room's exact-key check applies everywhere.
func paramsSchema() map[string]any {
	str := func(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }
	editSchema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"old": map[string]any{"type": "string", "minLength": 1, "description": "Text that occurs exactly once in the file at base_sha (after the earlier edits of this file). Copy it from repo_read_file; never a masked or cut line."},
			"new": str("Its replacement. Never a credential: use a ${secret:...} reference."),
		},
		"required":             []string{"old", "new"},
		"additionalProperties": false,
	}
	changeSchema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path":    str("Repository path under one of the repo's globs (repo_list_files)."),
			"edits":   map[string]any{"type": "array", "minItems": 1, "maxItems": maxEditsPerFile, "items": editSchema, "description": "Search-and-replace edits of an existing file. Exactly one of edits and content."},
			"content": str("The whole content of a NEW file (one that does not exist at base_sha). Exactly one of edits and content."),
		},
		"required":             []string{"path"},
		"additionalProperties": false,
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"repo":     str("The repo id from the pipeline's repos (repo_list_files)."),
			"base_sha": map[string]any{"type": "string", "pattern": `^[0-9a-f]{40}$`, "description": "The commit the files were read at (repo_read_file's base_sha)."},
			"title":    map[string]any{"type": "string", "minLength": 1, "maxLength": maxTitleRunes, "description": "The PR title: one line."},
			"body":     map[string]any{"type": "string", "maxLength": maxBodyBytes, "description": "The PR body in Markdown: cause and evidence (tool steps), never record values. No images, no HTML, links only to the forge or Piperoom."},
			"changes":  map[string]any{"type": "array", "minItems": 1, "maxItems": forge.MaxFixFiles, "items": changeSchema},
			"applied_when": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"kind":      map[string]any{"type": "string", "enum": []string{AppliedConnectorConfig, AppliedPlugin, AppliedChange, AppliedMerge}},
					"connector": str("connector_config: the pipeline's connector whose live config shows the change."),
					"keys":      map[string]any{"type": "object", "maxProperties": maxAppliedKeys, "additionalProperties": map[string]any{"type": "string"}, "description": "connector_config: key -> value the live config must hold (no credential keys)."},
					"class":     str("plugin: the connector plugin class Connect must list."),
					"version":   str("plugin: the plugin version Connect must list."),
					"kinds":     map[string]any{"type": "array", "items": map[string]any{"type": "string", "enum": clients.ChangeKinds()}, "description": "change: what-changed kinds that mark the deploy."},
					"service":   str("change: optional service name of the deploy."),
					"settle":    map[string]any{"type": "integer", "minimum": 1, "maximum": maxSettle, "description": "merge: minutes after the merge (default 10), for a change nothing in Piperoom can observe."},
				},
				"required":             []string{"kind"},
				"additionalProperties": false,
			},
		},
		"required":             []string{"repo", "base_sha", "title", "body", "changes", "applied_when"},
		"additionalProperties": false,
	}
}

// parse is rule 1 plus the per-field text rules (5, 6, 7) that need no
// repository read: shape, sizes, text only, secrets, links. allowed are the
// hosts the title and body may link to.
func parse(raw json.RawMessage, allowed []string) (params, error) {
	var p params
	if len(raw) > forge.MaxFixParamsBytes {
		return p, fmt.Errorf("params are %d bytes; at most %d", len(raw), forge.MaxFixParamsBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("bad params: %w", err)
	}
	if dec.More() {
		return p, fmt.Errorf("bad params: trailing data after the object")
	}
	if p.Repo == "" || len(p.Repo) > maxNameBytes {
		return p, fmt.Errorf("repo is required (a repo id of the pipeline's repos)")
	}
	if !shaRE.MatchString(p.BaseSHA) {
		return p, fmt.Errorf("base_sha must be the 40 lower-case hex digits of the commit the files were read at")
	}
	if err := checkTitle(p.Title, allowed); err != nil {
		return p, err
	}
	if len(p.Body) > maxBodyBytes {
		return p, fmt.Errorf("body is %d bytes; at most %d", len(p.Body), maxBodyBytes)
	}
	for _, c := range []func() error{
		func() error { return checkText("body", p.Body) },
		func() error { return checkSecrets("body", p.Body) },
		func() error { return checkLinks("body", p.Body, allowed) },
	} {
		if err := c(); err != nil {
			return p, err
		}
	}
	if len(p.Changes) == 0 || len(p.Changes) > forge.MaxFixFiles {
		return p, fmt.Errorf("changes must list 1 to %d files, not %d", forge.MaxFixFiles, len(p.Changes))
	}
	seen := map[string]bool{}
	for i, c := range p.Changes {
		if err := checkChange(i, c); err != nil {
			return p, err
		}
		// Paths compare case-insensitively: two spellings of one file on a
		// case-insensitive checkout would make two blobs of one file.
		k := strings.ToLower(c.Path)
		if seen[k] {
			return p, fmt.Errorf("changes[%d]: the path is listed twice; put every edit of a file in one change", i)
		}
		seen[k] = true
	}
	if err := checkAppliedWhen(p.AppliedWhen); err != nil {
		return p, err
	}
	return p, nil
}

func checkTitle(t string, allowed []string) error {
	n := utf8.RuneCountInString(t)
	switch {
	case strings.TrimSpace(t) == "":
		return fmt.Errorf("title is required")
	case n > maxTitleRunes:
		return fmt.Errorf("title is %d characters; at most %d", n, maxTitleRunes)
	case strings.ContainsAny(t, "\r\n"):
		return fmt.Errorf("title must be one line")
	case len(neutralizeMentions(t)) > maxTitleBytes:
		return fmt.Errorf("title is longer than %d bytes", maxTitleBytes)
	}
	if err := checkText("title", t); err != nil {
		return err
	}
	if err := checkSecrets("title", t); err != nil {
		return err
	}
	return checkLinks("title", t, allowed)
}

// checkChange is the no-I/O part of rules 3 to 6 for one file: a clean,
// allowed path; edits or content, never both; text only; no secret.
func checkChange(i int, c change) error {
	at := fmt.Sprintf("changes[%d]", i)
	if err := checkPath(c.Path); err != nil {
		return fmt.Errorf("%s: path: %w", at, err)
	}
	switch {
	case len(c.Edits) > 0 && c.Content != nil:
		return fmt.Errorf("%s: set edits (an existing file) or content (a new file), never both", at)
	case len(c.Edits) == 0 && c.Content == nil:
		return fmt.Errorf("%s: set edits (an existing file) or content (a new file); a fix PR never deletes a file", at)
	case len(c.Edits) > maxEditsPerFile:
		return fmt.Errorf("%s: %d edits; at most %d per file", at, len(c.Edits), maxEditsPerFile)
	}
	if c.Content != nil {
		if *c.Content == "" {
			return fmt.Errorf("%s: content of a new file is empty", at)
		}
		if err := checkText(at+": content", *c.Content); err != nil {
			return err
		}
		return checkSecrets(at+": content", *c.Content)
	}
	for j, e := range c.Edits {
		ef := fmt.Sprintf("%s: edits[%d]", at, j)
		if e.Old == "" {
			return fmt.Errorf("%s: old is empty (it must name text that occurs once in the file)", ef)
		}
		if e.Old == e.New {
			return fmt.Errorf("%s: old and new are equal", ef)
		}
		if err := checkText(ef+": old", e.Old); err != nil {
			return err
		}
		if err := checkText(ef+": new", e.New); err != nil {
			return err
		}
		if err := checkSecrets(ef+": new", e.New); err != nil {
			return err
		}
	}
	return nil
}

// checkPath runs the fixed path rules: forge.CheckRepoPath applies both the
// contract's clients.CheckRepoPath (design §5.1) and owners.yaml's (Q7a, which
// also denies any .git* segment), so a path passes only when both agree.
func checkPath(p string) error { return forge.CheckRepoPath(p, false) }

// checkAppliedWhen is rule 9 without the pipeline (the connector's scope is
// in inScope).
func checkAppliedWhen(a appliedWhen) error {
	bad := func(f string, args ...any) error { return fmt.Errorf("applied_when: "+f, args...) }
	empty := func(fields ...string) bool {
		return !slices.ContainsFunc(fields, func(s string) bool { return s != "" })
	}
	switch a.Kind {
	case AppliedConnectorConfig:
		if a.Connector == "" || len(a.Connector) > maxNameBytes {
			return bad("connector_config needs the connector whose live config shows the change")
		}
		if len(a.Keys) == 0 || len(a.Keys) > maxAppliedKeys {
			return bad("connector_config needs 1 to %d keys", maxAppliedKeys)
		}
		for k, v := range a.Keys {
			if k == "" || len(k) > maxNameBytes {
				return bad("a key is empty or longer than %d bytes", maxNameBytes)
			}
			if actions.LooksSecret(k) && !keyOnlySecret(k) {
				return bad("a key looks like a credential; Piperoom never compares a secret value")
			}
			if err := checkSecrets("applied_when: keys", k+"="+v); err != nil {
				return err
			}
		}
		if !empty(a.Class, a.Version, a.Service) || len(a.Kinds) > 0 || a.Settle != 0 {
			return bad("connector_config takes connector and keys only")
		}
	case AppliedPlugin:
		if a.Class == "" || a.Version == "" || len(a.Class) > maxNameBytes || len(a.Version) > 64 {
			return bad("plugin needs the plugin class and version Connect must list")
		}
		if !empty(a.Connector, a.Service) || len(a.Keys) > 0 || len(a.Kinds) > 0 || a.Settle != 0 {
			return bad("plugin takes class and version only")
		}
	case AppliedChange:
		if len(a.Kinds) == 0 {
			return bad("change needs one or more what-changed kinds")
		}
		for _, k := range a.Kinds {
			if !slices.Contains(clients.ChangeKinds(), k) {
				return bad("change kind %q is not one of %s", oneLine(k, 40), strings.Join(clients.ChangeKinds(), ", "))
			}
		}
		if len(a.Service) > maxNameBytes || !empty(a.Connector, a.Class, a.Version) || len(a.Keys) > 0 || a.Settle != 0 {
			return bad("change takes kinds and an optional service only")
		}
	case AppliedMerge:
		if a.Settle < 0 || a.Settle > maxSettle {
			return bad("merge settle is 1 to %d minutes (0 means the default 10)", maxSettle)
		}
		if !empty(a.Connector, a.Class, a.Version, a.Service) || len(a.Keys) > 0 || len(a.Kinds) > 0 {
			return bad("merge takes settle only")
		}
	default:
		return bad("kind must be one of %s, %s, %s, %s", AppliedConnectorConfig, AppliedPlugin, AppliedChange, AppliedMerge)
	}
	for _, s := range []string{a.Connector, a.Class, a.Version, a.Service} {
		if err := checkText("applied_when", s); err != nil {
			return err
		}
	}
	return nil
}

// resolved is the params checked against the room's pipeline.
type resolved struct {
	p     params
	repo  owners.Repo
	ref   forge.RepoRef
	layer string
}

// inScope is rules 2, 3, 4 and the connector part of 9 (CheckScope): the
// repo is in the pipeline's repos, every path is under globs of one layer,
// and a named connector is the pipeline's.
func inScope(pl owners.Pipeline, p params) (resolved, error) {
	r, ok := pl.Repo(p.Repo)
	if !ok {
		ids := make([]string, 0, len(pl.Repos))
		for _, x := range pl.Repos {
			ids = append(ids, x.ID)
		}
		allowed := "the pipeline lists no repos"
		if len(ids) > 0 {
			allowed = "allowed repos: " + strings.Join(ids, ", ")
		}
		return resolved{}, fmt.Errorf("%w: repo %q is not in pipeline %q's repos (%s)", tools.ErrOutOfScope, oneLine(p.Repo, 80), pl.ID, allowed)
	}
	ref := forge.RefOf(r.Ref())
	if ref.Owner == "" || ref.Name == "" || !forge.ValidBranchName(ref.Base) || strings.HasPrefix(ref.Base, forge.FixBranchPrefix) {
		return resolved{}, fmt.Errorf("%w: repo %q of pipeline %q has no valid url or base in owners.yaml", tools.ErrOutOfScope, r.ID, pl.ID)
	}
	layer := ""
	for i, c := range p.Changes {
		l, err := r.LayerOf(c.Path)
		if err != nil {
			return resolved{}, fmt.Errorf("%w: changes[%d]: %v", tools.ErrOutOfScope, i, err)
		}
		if layer != "" && l != layer {
			return resolved{}, fmt.Errorf("changes[%d]: the path's approving layer is %q but an earlier file's is %q; one step changes the files of one layer (propose one step per layer)", i, l, layer)
		}
		layer = l
	}
	if c := p.AppliedWhen.Connector; c != "" && !slices.Contains(pl.Connectors, c) {
		return resolved{}, fmt.Errorf("%w: applied_when: connector %q is not one of pipeline %q's connectors", tools.ErrOutOfScope, oneLine(c, 80), pl.ID)
	}
	return resolved{p: p, repo: r, ref: ref, layer: layer}, nil
}
