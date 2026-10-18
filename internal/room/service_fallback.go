package room

import (
	"errors"
	"fmt"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Unowned-layer fallback (P2b-3, tasks/P2-solo-ceremony.md): a required layer
// nobody may approve is listed in proposal.created.fallback_layers, and the
// room's current driver approves it with grant "fallback".

// fallbackLayersFor returns the layers of required that nobody may approve in
// the room's pipeline, in required's order; never nil, so the event and the
// snapshot say [] rather than leave it to the reader.
//
// It fails closed (no fallback) when no owners config is loaded or the
// pipeline id matches no pipeline: owners.IsOwned answers false for an
// unknown id, which must not read as "unowned", or a mistyped or removed
// pipeline id would hand every approval to the driver.
//
// A layer counts as owned when owners.yaml names an approver or backup for it
// (IsOwned) and also when a users.yaml layers: entry would let someone approve
// it (authorizeLayer's dev-mode branch). The second keeps the driver from
// approving, by fallback, a layer a person can approve as its owner.
func (s *Service) fallbackLayersFor(pipelineID string, required []string) []string {
	out := []string{}
	if s.owners == nil || !s.owners.HasPipeline(pipelineID) {
		return out
	}
	for _, layer := range required {
		if !s.layerHasApprover(pipelineID, layer) {
			out = append(out, layer)
		}
	}
	return out
}

// layerHasApprover reports whether anybody may approve the layer of the
// pipeline as its owner, by the rules authorizeLayer applies. The solo user is
// not counted: solo mode approves anything, and the fallback list says who
// owns the layer, not who may approve it.
func (s *Service) layerHasApprover(pipelineID, layer string) bool {
	if s.owners.IsOwned(pipelineID, layer) {
		return true
	}
	if s.users == nil || s.userLayersBlocked(pipelineID, layer) {
		return false
	}
	for _, u := range s.users.All() {
		if u.OwnsLayer(layer) && s.ownersNameUser(layer, u.ID) {
			return true
		}
	}
	return false
}

// proposalFallbackLayers returns the fallback_layers of an open proposal of
// the room, as folded from its proposal.created; nil when the proposal is not
// open in the snapshot. Caller holds st's lock.
func proposalFallbackLayers(st *roomState, proposalID string) []string {
	for _, p := range st.snap.OpenProposals {
		if p.ProposalID == proposalID {
			return p.FallbackLayers
		}
	}
	return nil
}

// authorizeProposalLayer is authorizeLayer for a decision on one proposal:
// owner, backup and solo rights first, exactly as authorizeLayer answers
// them, then the fallback (checkFallbackLocked). It returns the principal
// detail; GrantFallback for the fallback. approve and approve-all both call
// it, so the two routes never disagree about a layer. Caller holds st's lock.
func (s *Service) authorizeProposalLayer(st *roomState, p store.ProposalRow, user User, layer, confirm string, needConfirm bool) (string, error) {
	detail, err := s.authorizeLayer(st.snap.PipelineID, user, layer, confirm, needConfirm)
	if err == nil {
		return detail, nil
	}
	var re *Error
	if !errors.As(err, &re) || re.Code != codeNotLayerOwner {
		return "", err
	}
	if !contains(proposalFallbackLayers(st, p.ProposalID), layer) {
		return "", err
	}
	if ferr := s.checkFallbackLocked(st, p.ProposalID, user, layer); ferr != nil {
		return "", ferr
	}
	return events.GrantFallback, nil
}

// checkFallbackLocked is the fallback rule: the layer is in the proposal's
// fallback_layers, still has no approver, and user is a member who is the
// room's driver now. appendApprovalLocked runs it again as the condition of
// the append (emitCheckedLocked), so a driver change another replica logged
// between the request's check and the append refuses the approval. Caller
// holds st's lock.
func (s *Service) checkFallbackLocked(st *roomState, proposalID string, user User, layer string) error {
	if !contains(proposalFallbackLayers(st, proposalID), layer) || s.layerHasApprover(st.snap.PipelineID, layer) {
		return notLayerOwner(fmt.Sprintf("user %s does not own layer %q", user.ID, layer))
	}
	if role, ok := s.RoleFor(user.ID, user.Groups); !ok || role != RoleMember {
		return notLayerOwner(fmt.Sprintf("nobody owns layer %q; only the room's driver may approve it, and a viewer cannot drive", layer))
	}
	if !s.isDriver(st, user.ID) {
		return notLayerOwner(fmt.Sprintf("nobody owns layer %q; only the room's current driver may approve it", layer))
	}
	return nil
}
