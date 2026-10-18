package owners

import (
	"fmt"
	"sort"
	"strings"
)

// This file holds the P2 "ceremony scales down" helpers of owners.yaml
// (tasks/P2-solo-ceremony.md, sub-task P2a):
//
//	Pipeline.AutoApprove / AutoApproveList   the actions a pipeline lets run on a timer
//	LoadWithActions / ValidateAutoApprove    check those names against the action registry
//	IsOwned                                  does anyone own (pipeline, layer)? false = unowned, the driver approves
//	OwnsAll                                  may this session approve every one of these layers? (approve-all)
//
// None of them does I/O except LoadWithActions, which reads the file.

// LoadWithActions is Load plus a check that every pipeline `auto_approve:`
// entry names an action in actionNames. The server calls it once the action
// registry exists, so a typo such as `connect_restart_taks` fails at start and
// does not turn into a policy that silently never matches.
//
// actionNames is the complete set of names the caller can run. A nil or empty
// set is an empty registry, not "skip the check": any entry is then refused.
func LoadWithActions(path string, actionNames []string) (*Owners, error) {
	o, err := Load(path)
	if err != nil {
		return nil, err
	}
	if err := o.checkAutoApproveNames(actionNames); err != nil {
		return nil, fmt.Errorf("owners: validate %s: %w", path, err)
	}
	return o, nil
}

// ValidateAutoApprove refuses a pipeline `auto_approve:` entry that is not in
// actionNames. It exists for a caller that loaded the file with Load before the
// action registry was built (the server re-validates at start). Same rules as
// LoadWithActions: the error names the pipeline and the entry, and a nil or
// empty actionNames refuses every entry.
func (o *Owners) ValidateAutoApprove(actionNames []string) error {
	if o == nil {
		return nil
	}
	if err := o.checkAutoApproveNames(actionNames); err != nil {
		return fmt.Errorf("owners: %w", err)
	}
	return nil
}

// checkAutoApproveNames returns the first entry (file order) that is not in
// actionNames. It lists the known names so the fix is one edit away.
func (o *Owners) checkAutoApproveNames(actionNames []string) error {
	known := make(map[string]bool, len(actionNames))
	for _, n := range actionNames {
		known[n] = true
	}
	for i := range o.Pipelines {
		p := &o.Pipelines[i]
		for _, a := range p.AutoApprove {
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

// IsOwned reports whether anyone is named for the layer of the pipeline: an
// approver or a backup in the pipeline's `owners:` entry or, when it has none
// for that layer, in the top-level `layers:` entry. A layer that is not owned
// has nobody who can approve it, so the room lets the driver do it instead.
//
// It uses the same lookup as CanApprove, so the two never disagree: a pipeline
// `owners:` entry replaces the top-level one (an entry with no principals does
// not fall back to it), and a pipeline id that matches no pipeline is not owned
// (fail closed). Only the empty id reads `layers:` alone. A backup counts
// because CanApprove lets a backup approve.
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
