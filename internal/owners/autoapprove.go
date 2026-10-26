package owners

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// This file holds the P2 "ceremony scales down" helpers of owners.yaml
// (tasks/P2-solo-ceremony.md, sub-task P2a):
//
//	Pipeline.AutoApprove / AutoApproveList   the actions a pipeline lets run on a timer
//	LoadWithActions / ValidateAutoApprove    check those names against the action registry
//	HasPipeline                              is this id a declared pipeline? check it before reading !IsOwned as "unowned"
//	IsOwned                                  does anyone own (pipeline, layer)? false for an unowned layer and for an unknown pipeline
//	OwnsAll                                  may this session approve every one of these layers? (approve-all)
//
// None of them does I/O except LoadWithActions, which reads the file.

// LoadWithActions is Load plus a check that every pipeline `auto_approve:`
// entry and every `autonomy:` action names an action in actionNames. The
// server calls it once the action registry exists, so a typo such as
// `connect_restart_taks` fails at start and does not turn into a policy that
// silently never matches.
//
// actionNames is the complete set of names the caller can run. A nil or empty
// set is an empty registry, not "skip the check": any entry is then refused.
// Pass the names of the FULL proposal registry (the one the agent proposes
// from), never an executor registry emptied because write mode is off: a
// read-only server would then refuse to start over a valid file.
func LoadWithActions(path string, actionNames []string) (*Owners, error) {
	o, err := Load(path)
	if err != nil {
		return nil, err
	}
	if err := o.checkAutoApproveNames(actionNames, nil); err != nil {
		return nil, fmt.Errorf("owners: validate %s: %w", path, err)
	}
	if err := o.checkAutonomyNames(actionNames, nil); err != nil {
		return nil, fmt.Errorf("owners: validate %s: %w", path, err)
	}
	return o, nil
}

// LoadWithDeferred is LoadWithActions that also refuses an `auto_approve:`
// entry naming one of deferredNames, the actions.Deferred actions (Q7a,
// docs/design/fix-pr.md §5.8): a pull-request fix is never approved by a
// policy. The autonomy: actions are checked against actionNames too. It is the
// design's two-argument LoadWithActions under a new name, so the existing
// callers compile until they pass the deferred names.
func LoadWithDeferred(path string, actionNames, deferredNames []string) (*Owners, error) {
	o, err := Load(path)
	if err != nil {
		return nil, err
	}
	if err := o.checkAutoApproveNames(actionNames, deferredNames); err != nil {
		return nil, fmt.Errorf("owners: validate %s: %w", path, err)
	}
	if err := o.checkAutonomyNames(actionNames, deferredNames); err != nil {
		return nil, fmt.Errorf("owners: validate %s: %w", path, err)
	}
	return o, nil
}

// ValidateAutoApprove refuses a pipeline `auto_approve:` entry that is not in
// actionNames. It exists for a caller that loaded the file with Load before the
// action registry was built (the server re-validates at start). Same rules as
// LoadWithActions: the error names the pipeline and the entry, and a nil or
// empty actionNames refuses every entry. As there, pass the FULL proposal
// registry's names, not an executor registry emptied for write-mode-off.
func (o *Owners) ValidateAutoApprove(actionNames []string) error {
	if o == nil {
		return nil
	}
	return o.ValidateAutoApproveDeferred(actionNames, nil)
}

// ValidateAutoApproveDeferred is ValidateAutoApprove that also refuses an
// entry naming one of deferredNames (see LoadWithDeferred).
func (o *Owners) ValidateAutoApproveDeferred(actionNames, deferredNames []string) error {
	if o == nil {
		return nil
	}
	if err := o.checkAutoApproveNames(actionNames, deferredNames); err != nil {
		return fmt.Errorf("owners: %w", err)
	}
	return nil
}

// checkAutoApproveNames returns the first entry (file order) that is not in
// actionNames, or that is in deferredNames. It lists the known names so the
// fix is one edit away.
func (o *Owners) checkAutoApproveNames(actionNames, deferredNames []string) error {
	known := make(map[string]bool, len(actionNames))
	for _, n := range actionNames {
		known[n] = true
	}
	for i := range o.Pipelines {
		p := &o.Pipelines[i]
		for _, a := range p.AutoApprove {
			if slices.Contains(deferredNames, a) {
				return fmt.Errorf("pipeline %q: auto_approve: %q is never approved by a policy (it opens a pull request that a reviewer merges)", p.ID, a)
			}
			if !known[a] {
				return fmt.Errorf("pipeline %q: auto_approve: unknown action %q (known: %s)", p.ID, a, knownList(known))
			}
		}
	}
	return nil
}

// knownList renders the known action names sorted, for an error message.
func knownList(known map[string]bool) string {
	if len(known) == 0 {
		return "none"
	}
	names := make([]string, 0, len(known))
	for n := range known {
		names = append(names, n)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// checkAutoApproveSyntax checks one pipeline's list: every entry is a
// snake_case action name and none repeats. A repeat is refused, not merged,
// because the room log names the list entry that approved a fix and two
// identical entries would make that ambiguous.
func checkAutoApproveSyntax(entries []string) error {
	for _, a := range entries {
		if !validActionName(a) {
			return fmt.Errorf("auto_approve: %q is not an action name (want lowercase letters, digits and underscores, starting with a letter)", a)
		}
	}
	if d := firstDuplicate(entries); d != "" {
		return fmt.Errorf("auto_approve lists %q twice", d)
	}
	return nil
}

// validActionName reports whether s matches ^[a-z][a-z0-9_]*$. A byte loop
// instead of a package-level regexp keeps the package free of global state.
func validActionName(s string) bool {
	if s == "" || s[0] < 'a' || s[0] > 'z' {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}

// AutoApproveList returns a copy of the pipeline's `auto_approve:` actions, in
// file order. The result is never nil (it marshals as [] and not null) and the
// caller may modify it: the loaded config is shared by every request.
func (p *Pipeline) AutoApproveList() []string {
	if p == nil {
		return []string{}
	}
	return append([]string{}, p.AutoApprove...)
}

// HasPipeline reports whether the file declares a pipeline with this id. It is
// the check a caller needs before it treats !IsOwned as "unowned", because
// IsOwned is false for an unknown pipeline too. Nil-safe. The empty id is not a
// pipeline.
func (o *Owners) HasPipeline(id string) bool {
	if o == nil {
		return false
	}
	_, ok := o.Pipeline(id)
	return ok
}

// IsOwned reports whether anyone is named for the layer of the pipeline: an
// approver or a backup in the pipeline's `owners:` entry or, when it has none
// for that layer, in the top-level `layers:` entry. A layer that is not owned
// has nobody who can approve it, so the room lets the driver do it instead.
//
// It uses the same lookup as CanApprove, so the two never disagree: a pipeline
// `owners:` entry replaces the top-level one (an entry with no principals does
// not fall back to it), and a pipeline id that matches no pipeline has no
// rights. Only the empty id reads `layers:` alone. A backup counts because
// CanApprove lets a backup approve.
//
// "Not owned" is the PERMISSIVE answer for the driver fallback, which lets the
// driver approve a layer nobody owns. An unknown pipeline id returns false here
// because it has no rights to grant (the safe answer for CanApprove), but that
// must not be read as "unowned": a caller that decides a fallback from
// !IsOwned must first check HasPipeline, or a mistyped or removed pipeline id
// would hand its approvals to the driver.
func (o *Owners) IsOwned(pipelineID, layer string) bool {
	if o == nil {
		return false
	}
	approvers, backup := o.rightsFor(pipelineID, layer)
	return len(approvers) > 0 || len(backup) > 0
}

// OwnsAll reports whether the session may approve every layer in layers of the
// pipeline, which is what approve-all needs before it records one approval per
// layer. It calls CanApprove per layer, so group membership, backups and the
// pipeline-over-top-level precedence behave exactly as for a single approval.
//
// An empty layers is false. "Every layer of nothing" is vacuously true, and
// that would let a caller bug (a proposal with no required layers read as nil)
// show a one-click approve to anyone, so it fails closed. A layer nobody owns
// is false too: the driver's fallback for unowned layers is the room's
// decision, not an ownership right.
func (o *Owners) OwnsAll(pipelineID string, layers []string, userID string, groups []string) bool {
	if o == nil || len(layers) == 0 {
		return false
	}
	for _, layer := range layers {
		if _, ok := o.CanApprove(pipelineID, layer, userID, groups); !ok {
			return false
		}
	}
	return true
}
