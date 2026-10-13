package room

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"slices"
	"strings"

	"piperoom/internal/auth"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// Roles (docs/design/auth.md section 2.1). The login gate decides who gets in;
// the role decides what they may do. Both are computed from the user id and the
// session's IdP groups on every request, never stored.
const (
	// RoleMember may use every room action. A users.yaml user, or a principal
	// named as approver or backup in owners.yaml.
	RoleMember = "member"
	// RoleViewer may read and watch only. Someone who passes the gate only
	// through PIPEROOM_AUTH_VIEWER_GROUPS or auditors:.
	RoleViewer = "viewer"
)

// groupsMissingMsg is the 403 text when an approval needs a group right and the
// token had no groups claim (design section 2.2).
const groupsMissingMsg = "no groups claim in token (IdP overage?)"

// RoleFor applies the login gate and the role rule to a user id and the groups
// of the session. ok is false when the gate refuses the person: not in
// users.yaml, not named in owners.yaml, not in a viewer group. A nil groups
// slice (no claim) gives no group rights.
func (s *Service) RoleFor(userID string, groups []string) (role string, ok bool) {
	if _, inFile := s.users.Get(userID); inFile || s.owners.IsMemberPrincipal(userID, groups) {
		return RoleMember, true
	}
	// Past here the person is no member. An auditors: entry and a viewer group
	// each let them in as a viewer; auditors: never makes a member.
	if s.owners.IsAuditor(userID, groups) || s.inViewerGroup(groups) {
		return RoleViewer, true
	}
	return "", false
}

func (s *Service) inViewerGroup(groups []string) bool {
	for _, g := range groups {
		if _, ok := s.viewerGroups[g]; ok {
			return true
		}
	}
	return false
}

// IsAuditor reports whether the person is named in owners.yaml auditors:.
func (s *Service) IsAuditor(userID string, groups []string) bool {
	return s.owners.IsAuditor(userID, groups)
}

// LayersFor returns the layers the person may approve, for GET /api/me:
// the users.yaml layers, the owners.yaml layers they own as a user, through a
// group or as a backup, and every layer for the solo user. The result is in
// events.Layers order and is never nil.
func (s *Service) LayersFor(u User) []string {
	if s.soloUser != "" && u.ID == s.soloUser {
		return append([]string{}, events.Layers...)
	}
	fromOwners := s.owners.LayersFor(u.ID, u.Groups)
	out := []string{}
	for _, l := range events.Layers {
		if slices.Contains(u.Layers, l) || slices.Contains(fromOwners, l) {
			out = append(out, l)
		}
	}
	return out
}

// UserBySub returns the users.yaml user with this IdP subject (sub: wins over
// an email match, design section 2.1).
func (s *Service) UserBySub(sub string) (User, bool) { return s.users.BySub(sub) }

// UserByEmail returns the users.yaml user with this email, compared without
// regard to case. The caller must have checked email_verified.
func (s *Service) UserByEmail(email string) (User, bool) { return s.users.ByEmail(email) }

// LookupPerson finds a person to give control to or to hand off to (design
// section 2.1). users.yaml first: such a person is always a member and needs no
// session. Otherwise the newest live session of that user id, and only when the
// person's role for the session's groups is member. Anything else, including a
// viewer, an auditor-only principal, a person with no live session and an
// unknown id, is 400 validation. A group-only member must therefore have logged
// in recently; nobody gets the driver role without a current identity.
func (s *Service) LookupPerson(ctx context.Context, id string) (User, error) {
	if u, ok := s.users.Get(id); ok {
		return u, nil
	}
	if s.sessions == nil || id == "" {
		return User{}, validation("unknown user")
	}
	sess, err := s.sessions.LiveByUserID(ctx, id, s.now(), s.sessionIdle)
	if errors.Is(err, auth.ErrNotFound) {
		return User{}, validation("unknown user, or not signed in right now")
	}
	if err != nil {
		return User{}, fmt.Errorf("room: look up person %q: %w", id, err)
	}
	if role, ok := s.RoleFor(sess.UserID, sess.Groups); !ok || role != RoleMember {
		return User{}, validation("this user is a viewer and cannot take control or receive a hand-off")
	}
	name := sess.Name
	if name == "" {
		name = sess.UserID
	}
	return User{ID: sess.UserID, Name: name, Groups: sess.Groups, GroupsMissing: sess.GroupsMissing}, nil
}

// authorizeLayer decides whether user may approve or reject the layer of a
// pipeline and returns the grant string for the audit trail ("user:priya",
// "group:oncall", "backup:user:meera", "solo"). confirm is the typed
// "<pipeline_id>/<layer>" string and is checked only for an approval
// (needConfirm) by the solo user.
//
// Sources, in order: the solo user (every layer); owners.yaml principals,
// pipeline owners: blocks and backups; the users.yaml layers list, kept as a
// dev-mode source, except where a pipeline owners: block replaces the layer.
func (s *Service) authorizeLayer(pipelineID string, user User, layer, confirm string, needConfirm bool) (string, error) {
	if s.soloUser != "" && user.ID == s.soloUser {
		if needConfirm {
			want := pipelineID + "/" + layer
			if subtle.ConstantTimeCompare([]byte(confirm), []byte(want)) != 1 {
				return "", validation(fmt.Sprintf("solo mode: type confirm %q to approve", want))
			}
		}
		return "solo", nil
	}
	if s.owners != nil {
		if g, ok := s.owners.CanApprove(pipelineID, layer, user.ID, user.Groups); ok {
			return g, nil
		}
	}
	if user.OwnsLayer(layer) && !s.layerOverridden(pipelineID, layer) {
		return owners.GrantUserPrefix + user.ID, nil
	}
	if user.GroupsMissing && s.layerListsGroup(pipelineID, layer) {
		return "", notLayerOwner(groupsMissingMsg)
	}
	return "", notLayerOwner(fmt.Sprintf("user %s does not own layer %q", user.ID, layer))
}

// layerOverridden reports whether a pipeline owners: block replaces the layer's
// default owners, in which case the users.yaml layers list does not apply.
func (s *Service) layerOverridden(pipelineID, layer string) bool {
	if s.owners == nil {
		return false
	}
	p, ok := s.owners.Pipeline(pipelineID)
	if !ok {
		return false
	}
	_, over := p.Owners[layer]
	return over
}

// layerListsGroup reports whether the layer's approvers or backups for the
// pipeline include a group: principal, so a missing groups claim could be why
// the person was refused.
func (s *Service) layerListsGroup(pipelineID, layer string) bool {
	if s.owners == nil {
		return false
	}
	ps := s.owners.Layers[layer]
	if p, ok := s.owners.Pipeline(pipelineID); ok {
		if r, over := p.Owners[layer]; over {
			ps = slices.Concat(r.Approvers, r.Backup)
		}
	}
	for _, p := range ps {
		if p.Kind == owners.KindGroup {
			return true
		}
	}
	return false
}

// backupNote is the system message the room gets when a backup approves
// (design section 2.3).
func backupNote(u User, layer, grant string) string {
	who := u.Name
	if who == "" {
		who = u.ID
	}
	return fmt.Sprintf("%s approved layer %s as a backup approver (%s)", who, layer, strings.TrimPrefix(grant, owners.GrantBackupPrefix))
}

func groupSet(groups []string) map[string]struct{} {
	m := make(map[string]struct{}, len(groups))
	for _, g := range groups {
		if g = strings.TrimSpace(g); g != "" {
			m[g] = struct{}{}
		}
	}
	return m
}
