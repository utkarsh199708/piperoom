package room

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"slices"
	"sort"
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

// IsSolo reports whether PIPEROOM_SOLO_USER is this user (design section 5.3).
func (s *Service) IsSolo(userID string) bool {
	return s.soloUser != "" && userID == s.soloUser
}

// LayersFor returns the layers the person may approve, for GET /api/me:
// the users.yaml layers, the owners.yaml layers they own as a user, through a
// group or as a backup, and every layer for the solo user. Only layers of the
// pipeline kinds owners.yaml uses count (knownLayers, P4a-4). The result is in
// owners.KnownLayers order and is never nil.
func (s *Service) LayersFor(u User) []string {
	known := s.knownLayers()
	if s.soloUser != "" && u.ID == s.soloUser {
		return known
	}
	fromOwners := s.owners.LayersFor(u.ID, u.Groups)
	out := []string{}
	for _, l := range known {
		// A users.yaml layer counts only where authorizeLayer would honour it,
		// so the UI never shows an approve button that answers 403.
		if (slices.Contains(u.Layers, l) && s.ownersNameUser(l, u.ID)) || slices.Contains(fromOwners, l) {
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
// session. Otherwise the newest usable session of that user id (Deps.SessionLive,
// the rule every request is held to), and only when the person's role for the
// session's groups is member. Anything else, including a viewer, an
// auditor-only principal, a person with no usable session and an unknown id,
// is 400 validation. A group-only member must therefore have logged
// in recently; nobody gets the driver role without a current identity.
func (s *Service) LookupPerson(ctx context.Context, id string) (User, error) {
	if u, ok := s.users.Get(id); ok {
		return u, nil
	}
	if s.sessions == nil || id == "" {
		return User{}, errNotAPerson()
	}
	sess, err := s.sessions.LiveByUserID(ctx, id, s.now(), s.sessionLive)
	if errors.Is(err, auth.ErrNotFound) {
		return User{}, errNotAPerson()
	}
	if err != nil {
		return User{}, fmt.Errorf("room: look up person %q: %w", id, err)
	}
	if role, ok := s.RoleFor(sess.UserID, sess.Groups); !ok || role != RoleMember {
		return User{}, errNotAPerson()
	}
	name, _ := s.ShownName(sess.UserID, sess.Name, sess.Email)
	return User{ID: sess.UserID, Name: name, Groups: sess.Groups, GroupsMissing: sess.GroupsMissing}, nil
}

// serverActorNames are the names the server's own actors write under. A person
// may not pass for them either.
var serverActorNames = []string{events.AgentActor.Name, systemActor.Name, executorActor.Name}

// Reasons ShownName gives in resembles when no users.yaml person or server
// actor is matched.
const (
	resemblesSuffixShape = "suffix-shape"
	resemblesMixedScript = "mixed-script"
)

// ShownName is the name the room shows for a person (design section 2.1,
// security audit A-ID-2). A users.yaml person is shown with their users.yaml
// name. Anyone else chose their own name in the IdP, so it is shown as
// "<name> (<email>)", or "<name> (<id>)" without a verified email or when the
// email itself is non-ASCII or looks like a users.yaml person (suffixTag), when
//
//   - the whole name, or the name with its bracketed parts removed, renders
//     like a users.yaml id, name or email, or a server actor (auth.Skeleton);
//   - it holds a bracketed part shaped like this suffix (an email or a u_ id),
//     so it cannot forge the server's own marking; or
//   - it mixes Latin with another script.
//
// resembles is the users.yaml id or server actor name it looks like, or
// "suffix-shape" / "mixed-script"; "" when the name is shown as it is. An empty
// name becomes the email, else the id. Callers pass the name as the IdP sent
// it, never a name this function already returned.
func (s *Service) ShownName(id, name, email string) (shown, resembles string) {
	if u, ok := s.users.Get(id); ok {
		return u.Name, ""
	}
	if name == "" {
		name = email
	}
	if name == "" {
		return id, ""
	}
	resembles = s.resembles(name)
	if resembles == "" {
		return name, ""
	}
	return name + " (" + s.suffixTag(id, email) + ")", resembles
}

// suffixTag is what ShownName puts in brackets: the verified email, unless the
// email could repeat the forgery it marks. An IdP's email_verified only proves
// the mailbox, not that it differs visibly from a users.yaml person's, so an
// email with a non-ASCII rune ("priya@example.com" with a Cyrillic a) or one
// that renders like a users.yaml id, name or email ("priya@examp1e.com") is
// replaced by the derived u_ id, which the server minted and cannot be chosen.
func (s *Service) suffixTag(id, email string) string {
	if email == "" || !isASCII(email) {
		return id
	}
	if _, ok := s.users.Lookalike(email); ok {
		return id
	}
	return email
}

// resembles is ShownName's test; see there.
func (s *Service) resembles(name string) string {
	for _, n := range []string{name, auth.WithoutBrackets(name)} {
		if u, ok := s.users.Lookalike(n); ok {
			return u.ID
		}
		for _, a := range serverActorNames {
			if auth.LooksLike(n, a) {
				return a
			}
		}
	}
	if auth.HasSuffixShape(name) {
		return resemblesSuffixShape
	}
	if auth.MixesLatin(name) {
		return resemblesMixedScript
	}
	return ""
}

// PersonEntry is one entry of GET /api/people: someone a driver may give control or a
// hand-off to. Email is set only for an IdP user whose token said
// email_verified; users.yaml users have none. Verified is true for users.yaml
// users only (design section 2.1).
type PersonEntry struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Email    string `json:"email,omitempty"`
	Verified bool   `json:"verified"`
	// Layers are the layers the person may approve (LayersFor), [] when none.
	// Group grants count for an IdP member through the session's groups.
	Layers []string `json:"layers"`
}

// maxPeople bounds the list.
const maxPeople = 1000

// People lists the people LookupPerson would accept: every users.yaml user, then
// each member with a live session (newest session per user, role member for its
// groups). users.yaml users come first in file order, IdP members after them
// ordered by name then id. A viewer or an auditor-only principal is not
// listed.
func (s *Service) People(ctx context.Context) ([]PersonEntry, error) {
	out := make([]PersonEntry, 0, len(s.users.All()))
	listed := map[string]bool{}
	for _, u := range s.users.All() {
		out = append(out, PersonEntry{ID: u.ID, Name: u.Name, Verified: true, Layers: s.LayersFor(u)})
		listed[u.ID] = true
	}
	if s.sessions == nil {
		return out, nil
	}
	live, err := s.sessions.LiveSessions(ctx, s.now(), s.sessionLive, maxPeople)
	if err != nil {
		return nil, fmt.Errorf("room: list people: %w", err)
	}
	var idp []PersonEntry
	for _, sess := range live {
		if listed[sess.UserID] {
			continue
		}
		if role, ok := s.RoleFor(sess.UserID, sess.Groups); !ok || role != RoleMember {
			continue
		}
		name, _ := s.ShownName(sess.UserID, sess.Name, sess.Email)
		idp = append(idp, PersonEntry{ID: sess.UserID, Name: name, Email: sess.Email, Layers: s.LayersFor(User{ID: sess.UserID, Groups: sess.Groups})})
	}
	sort.Slice(idp, func(i, j int) bool {
		if idp[i].Name != idp[j].Name {
			return idp[i].Name < idp[j].Name
		}
		return idp[i].ID < idp[j].ID
	})
	return append(out, idp...), nil
}

// errNotAPerson is the one answer LookupPerson gives for every refusal (unknown
// id, no live session, viewer, auditor-only), so the reply does not tell a
// caller which ids exist or what role someone has.
func errNotAPerson() error {
	return validation("that person cannot be given control or a hand-off: unknown, not signed in, or not a member")
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
			if err := checkSoloConfirm(pipelineID, layer, confirm); err != nil {
				return "", err
			}
		}
		return "solo", nil
	}
	if s.owners != nil {
		if g, ok := s.owners.CanApprove(pipelineID, layer, user.ID, user.Groups); ok {
			return g, nil
		}
	}
	if user.OwnsLayer(layer) && !s.userLayersBlocked(pipelineID, layer) && s.ownersNameUser(layer, user.ID) {
		return owners.GrantUserPrefix + user.ID, nil
	}
	if user.GroupsMissing && s.layerListsGroup(pipelineID, layer) {
		return "", notLayerOwner(groupsMissingMsg)
	}
	return "", notLayerOwner(fmt.Sprintf("user %s does not own layer %q", user.ID, layer))
}

// checkSoloConfirm is the solo user's typed "<pipeline_id>/<layer>" check. The
// approve path runs it after the params_hash check, so a stale solo user is
// told 409 params_mismatch before 400 about the confirm text.
func checkSoloConfirm(pipelineID, layer, confirm string) error {
	want := pipelineID + "/" + layer
	if subtle.ConstantTimeCompare([]byte(confirm), []byte(want)) != 1 {
		return validation(fmt.Sprintf("solo mode: type confirm %q to approve", want))
	}
	return nil
}

// userLayersBlocked reports whether the users.yaml layers list must not grant
// this layer: a pipeline owners: block replaces the layer's default owners, or
// the pipeline id is not empty and matches no pipeline (fail closed, like
// owners.CanApprove).
func (s *Service) userLayersBlocked(pipelineID, layer string) bool {
	if s.owners == nil {
		return false
	}
	p, ok := s.owners.Pipeline(pipelineID)
	if !ok {
		return pipelineID != ""
	}
	_, over := p.Owners[layer]
	return over
}

// ownersNameUser reports whether a users.yaml layer may stand (design section
// 2.3, security audit A-AP-3): the owners file's layers: has no entry for the
// layer (no owners file, or one without layers:, as in unit tests; the start
// check refuses that in a real config), or the entry names this same user as a
// user: principal. Where owners.yaml names a layer's approvers, a users.yaml
// claim cannot add one, not even to a layer that lists a group: the server
// cannot vouch for that membership. ValidateUsers refuses such a config at
// start-up; this keeps the rule for a config that never went through it.
func (s *Service) ownersNameUser(layer, userID string) bool {
	if s.owners == nil {
		return true
	}
	ps, ok := s.owners.Layers[layer]
	if !ok || len(ps) == 0 {
		return true
	}
	return ps.Contains(owners.Principal{Kind: owners.KindUser, Name: userID})
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

// ErrExportNotAllowed is the 403 not_allowed answer for an export (design
// section 3.4).
func ErrExportNotAllowed() *Error {
	return &Error{Status: 403, Code: "not_allowed", Msg: "exporting a room needs the approver or auditor role"}
}

// CheckExportAccess decides whether user may export the room (design section
// 3.4): an auditors: principal, or an approver or backup for any layer of the
// room's pipeline (owners.yaml principals; in dev mode the users.yaml layers;
// the solo user). The current driver is not on the list, and neither is a plain
// member or a viewer. It answers *Error 403 not_allowed, or 404 for an unknown
// room when the person is not an auditor.
func (s *Service) CheckExportAccess(ctx context.Context, user User, roomID string) error {
	if s.IsAuditor(user.ID, user.Groups) {
		return nil
	}
	snap, err := s.GetRoom(ctx, roomID)
	if err != nil {
		return err
	}
	for _, layer := range owners.KnownLayers() {
		if _, err := s.authorizeLayer(snap.PipelineID, user, layer, "", false); err == nil {
			return nil
		}
	}
	return ErrExportNotAllowed()
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
