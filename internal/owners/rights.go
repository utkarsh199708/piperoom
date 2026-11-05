package owners

import (
	"fmt"
	"slices"
	"sort"

	"gopkg.in/yaml.v3"
)

// LayerRights is one entry of a pipeline `owners:` block:
//
//	owners:
//	  kafka-connect: { approvers: [group:dataeng-oncall], backup: [user:priya] }
//
// Approvers may approve the layer. A backup may approve it too; the grant is
// recorded as "backup:..." so the room can name them.
type LayerRights struct {
	Approvers Principals `yaml:"approvers"`
	Backup    Principals `yaml:"backup"`
}

// UnmarshalYAML rejects unknown keys: yaml.Unmarshal ignores them, so a typo
// such as `backups:` would silently drop the backup.
func (r *LayerRights) UnmarshalYAML(node *yaml.Node) error {
	if err := checkKeys(node, "owners", "approvers", "backup"); err != nil {
		return err
	}
	type plain LayerRights // no methods, so Decode does not recurse
	return node.Decode((*plain)(r))
}

// This file holds the pure rights helpers that the auth layer calls once it
// has a session (docs/design/auth.md section 2). None of them does I/O. The
// inputs are the session's user id and its IdP groups (nil when the token had
// no groups claim, which grants no group rights).
//
//	CanApprove        may this session approve (pipeline, layer)? returns the Grant string
//	LayersFor         the layers this session may approve (GET /api/me layers)
//	IsAuditor         named in auditors: (may export)
//	IsMemberPrincipal approver or backup anywhere (the member role; NOT auditors-only)
//	Named             named anywhere, auditors included (the login gate)
//	LayerOwner        display label for a layer ("priya, group:dataeng-oncall")
//	NamedPrincipals   every principal the file names (solo start check)

// GrantUserPrefix, GrantGroupPrefix and GrantBackupPrefix are the prefixes of the string
// CanApprove returns. They match events.Identity.Grant. "solo" is not decided
// here; the solo user is a config setting.
const (
	GrantUserPrefix   = "user:"   // + user id
	GrantGroupPrefix  = "group:"  // + group name
	GrantBackupPrefix = "backup:" // + canonical principal, e.g. "backup:user:priya"
)

// rightsFor returns the approvers and backup for a layer of a pipeline. A
// pipeline `owners:` entry for the layer replaces the top-level `layers:`
// entry. Only an empty pipeline id uses `layers:` alone. A non-empty id that
// matches no pipeline gets no rights (fail closed): a renamed or removed
// pipeline must not quietly fall back to the default owners.
func (o *Owners) rightsFor(pipelineID, layer string) (approvers, backup Principals) {
	if pipelineID == "" {
		return o.Layers[layer], nil
	}
	for i := range o.Pipelines {
		if o.Pipelines[i].ID != pipelineID {
			continue
		}
		if r, ok := o.Pipelines[i].Owners[layer]; ok {
			return r.Approvers, r.Backup
		}
		return o.Layers[layer], nil
	}
	return nil, nil
}

// CanApprove reports whether the session may approve or reject the layer of
// the pipeline. On success grant is one of:
//
//	"user:<id>"           named as a user approver
//	"group:<name>"        in a named approver group
//	"backup:user:<id>"    named as a user backup
//	"backup:group:<name>" in a backup group
//
// A user match wins over a group match; an approver match wins over a backup
// match. Nil or empty groups give no group rights.
func (o *Owners) CanApprove(pipelineID, layer, userID string, groups []string) (grant string, ok bool) {
	if o == nil {
		return "", false
	}
	approvers, backup := o.rightsFor(pipelineID, layer)
	if g, ok := grantFor(approvers, userID, groups); ok {
		return g.String(), true
	}
	if g, ok := grantFor(backup, userID, groups); ok {
		return GrantBackupPrefix + g.String(), true
	}
	return "", false
}

// grantFor finds the principal that gives the session its right, preferring a
// user match over a group match.
func grantFor(ps Principals, userID string, groups []string) (Principal, bool) {
	for _, p := range ps {
		if p.Kind == KindUser && p.Matches(userID, groups) {
			return p, true
		}
	}
	for _, p := range ps {
		if p.Kind == KindGroup && p.Matches(userID, groups) {
			return p, true
		}
	}
	return Principal{}, false
}

// IsAuditor reports whether the session is named in `auditors:`.
func (o *Owners) IsAuditor(userID string, groups []string) bool {
	if o == nil {
		return false
	}
	_, ok := o.Auditors.Matches(userID, groups)
	return ok
}

// IsMemberPrincipal reports whether the session is named as an approver or
// backup, in `layers:` or in any pipeline `owners:` block. Being in
// `auditors:` does not count: an auditor-only principal is a viewer
// (docs/design/auth.md section 2.1). It does not look at users.yaml; the
// caller adds that.
func (o *Owners) IsMemberPrincipal(userID string, groups []string) bool {
	if o == nil {
		return false
	}
	found := false
	o.eachRights(func(_, _ string, approvers, backup Principals) {
		if _, ok := approvers.Matches(userID, groups); ok {
			found = true
		}
		if _, ok := backup.Matches(userID, groups); ok {
			found = true
		}
	})
	return found
}

// Named reports whether the session is named anywhere: layers, pipeline
// owners (approver or backup) or auditors. This is the owners.yaml half of the
// login gate.
func (o *Owners) Named(userID string, groups []string) bool {
	return o.IsMemberPrincipal(userID, groups) || o.IsAuditor(userID, groups)
}

// LayersFor returns the layers the session may approve, for GET /api/me
// "layers": every layer where the session is an approver or backup in
// `layers:` or in any pipeline `owners:` block. The result is in
// KnownLayers order, without duplicates, and is never nil.
func (o *Owners) LayersFor(userID string, groups []string) []string {
	out := []string{}
	if o == nil {
		return out
	}
	hit := map[string]bool{}
	o.eachRights(func(_, layer string, approvers, backup Principals) {
		if _, ok := approvers.Matches(userID, groups); ok {
			hit[layer] = true
		}
		if _, ok := backup.Matches(userID, groups); ok {
			hit[layer] = true
		}
	})
	for _, l := range KnownLayers() {
		if hit[l] {
			out = append(out, l)
		}
	}
	return out
}

// LayerOwner returns the display label for a layer: the principals joined by
// ", " with the `user:` prefix dropped, e.g. "priya, group:dataeng-oncall". It
// reads `layers:` only (layer ownership is global for the topology label) and
// grants no right.
func (o *Owners) LayerOwner(layer string) (string, bool) {
	ps, ok := o.Layers[layer]
	if !ok || len(ps) == 0 {
		return "", false
	}
	return ps.Display(), true
}

// NamedPrincipals returns every distinct principal the file names (layers,
// pipeline owners approvers and backups, auditors, tenant members and admins),
// sorted by canonical form.
func (o *Owners) NamedPrincipals() []Principal {
	if o == nil {
		return nil
	}
	set := map[Principal]bool{}
	add := func(ps Principals) {
		for _, p := range ps {
			set[p] = true
		}
	}
	o.eachRights(func(_, _ string, approvers, backup Principals) {
		add(approvers)
		add(backup)
	})
	add(o.Auditors)
	for _, t := range o.Tenants {
		add(t.Members)
		add(t.Admins)
	}
	out := make([]Principal, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// eachRights calls fn for every (pipeline, layer) rights entry: each top-level
// layer (pipeline "") and each pipeline owners entry.
func (o *Owners) eachRights(fn func(pipelineID, layer string, approvers, backup Principals)) {
	for layer, ps := range o.Layers {
		fn("", layer, ps, nil)
	}
	for i := range o.Pipelines {
		p := &o.Pipelines[i]
		for layer, r := range p.Owners {
			fn(p.ID, layer, r.Approvers, r.Backup)
		}
	}
}

// validateRights checks layers:, auditors: and the pipeline owners: blocks.
// Principal syntax (bad prefix, empty name) is already checked while parsing.
func (o *Owners) validateRights() error {
	layers := make([]string, 0, len(o.Layers))
	for layer := range o.Layers {
		layers = append(layers, layer)
	}
	sort.Strings(layers) // same error for the same file
	for _, layer := range layers {
		if !slices.Contains(KnownLayers(), layer) {
			return fmt.Errorf("unknown layer %q (known: %v)", layer, KnownLayers())
		}
		if len(o.Layers[layer]) == 0 {
			return fmt.Errorf("layers: %q has no owner (want a user id, user:<id> or group:<name>)", layer)
		}
	}
	for i := range o.Pipelines {
		p := &o.Pipelines[i]
		pl := make([]string, 0, len(p.Owners))
		for layer := range p.Owners {
			pl = append(pl, layer)
		}
		sort.Strings(pl)
		for _, layer := range pl {
			if !slices.Contains(KnownLayers(), layer) {
				return fmt.Errorf("pipeline %q: owners: unknown layer %q (known: %v)", p.ID, layer, KnownLayers())
			}
			if len(p.Owners[layer].Approvers) == 0 {
				return fmt.Errorf("pipeline %q: owners: layer %q has no approvers", p.ID, layer)
			}
		}
	}
	return nil
}
