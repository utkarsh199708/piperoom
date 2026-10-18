package room

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"piperoom/internal/events"
)

// ApproveAllProposal is POST /api/proposals/{pid}/approve-all (P0 item 4,
// tasks/P2-solo-ceremony.md P2b): one call by a person who may approve every
// required layer records one approval.given per layer still pending, in
// required_layers order, and the steps then run exactly as after the last
// single approve. It returns those events.
//
// The rules are approve's, applied to the whole proposal under one hold of
// the room's lock, and checked in approve's order:
//   - paramsHash is required (400 validation);
//   - a rejected, blocked or executed proposal is 409 already_decided, and
//     the room must be open (409 room_closed);
//   - the caller must be allowed every required layer, an already approved
//     one included (authorizeAllLayers), else 403 not_layer_owner naming the
//     first layer the caller lacks, and nothing is recorded;
//   - a proposal whose every layer is approved is 409 already_decided (after
//     the 403, as approve answers a decided layer only to its owner);
//   - paramsHash must be the proposal's (409 params_mismatch);
//   - the solo user types confirm "<pipeline_id>/*" once for the call (400
//     validation otherwise), checked after the hash like approve's; anyone
//     else's confirm is ignored.
//
// A layer someone else approved before the call is skipped, not recorded
// again. Each approval goes through appendApprovalLocked, the per-layer path
// of approve, so the one-decision-per-layer store rule, the backup note, the
// metric and the executor hand-off (with its write-mode gate) are the same.
// The log is append-only: if appending a later layer fails (another replica
// decided it first: 409 already_decided; the log is full), the layers before
// it stay recorded and the call answers that error; a retry records the rest.
func (s *Service) ApproveAllProposal(ctx context.Context, user User, proposalID, paramsHash, confirm string) ([]events.Event, error) {
	if err := rejectUnstorable(paramsHash); err != nil {
		return nil, err
	}
	if paramsHash == "" {
		return nil, validation(paramsRequiredMsg)
	}
	s.syncProposalRoom(ctx, proposalID)
	p, st, err := s.lockProposalRoom(ctx, user, proposalID)
	if err != nil {
		return nil, err
	}
	defer s.unlockRoom(st)
	if err := s.decisionLockedCheck(ctx, st, p); err != nil {
		return nil, err
	}
	if !s.roomOpen(st) {
		return nil, roomClosed("room is closed")
	}
	details, err := s.authorizeAllLayers(st.snap.PipelineID, user, p.RequiredLayers)
	if err != nil {
		return nil, err
	}
	// Fully approved while its row still says "open": queued for the
	// executor, or held by write mode with the row's "blocked" write lost.
	if allApproved(st.snap, proposalID) {
		return nil, alreadyDecided("every layer of this proposal is already approved")
	}
	if err := s.checkParamsHash(ctx, &p, paramsHash, false); err != nil {
		return nil, err
	}
	if slices.Contains(details, events.GrantSolo) {
		if err := checkSoloConfirm(st.snap.PipelineID, "*", confirm); err != nil {
			return nil, err
		}
	}
	// P2b-4: a pending proposal.scheduled countdown of this proposal stops here, without an event.
	out := make([]events.Event, 0, len(p.RequiredLayers))
	for i, layer := range p.RequiredLayers {
		if approvalFor(st.snap, proposalID, layer) != "pending" {
			continue
		}
		ev, err := s.appendApprovalLocked(ctx, st, p, user, layer, details[i])
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	if allApproved(st.snap, proposalID) {
		// The same hand-off as the last single approve: the room's one
		// executor queue, which refuses to run anything with write mode off.
		s.enqueueExecutionLocked(st, p)
	}
	return out, nil
}

// authorizeAllLayers runs authorizeLayer for every layer, in order, and
// returns each layer's principal detail (identity.grant), or the refusal for
// the first layer the user may not approve, naming it. Its owners.yaml part
// is CanApprove per layer, the answer owners.OwnsAll gives; like a single
// approve it also honours the solo user and, in dev mode, the users.yaml
// layers, so the two routes never disagree about a layer. No layers is a
// refusal too (fail closed, as OwnsAll): "every layer of nothing" must not
// read as allowed.
func (s *Service) authorizeAllLayers(pipelineID string, user User, layers []string) ([]string, error) {
	if len(layers) == 0 {
		return nil, notLayerOwner("this proposal names no required layer, so approve-all has nothing it may approve")
	}
	details := make([]string, len(layers))
	for i, layer := range layers {
		// P2b-3: a layer in the proposal's fallback_layers is allowed here for the room's current driver (detail "fallback").
		d, err := s.authorizeLayer(pipelineID, user, layer, "", false)
		if err != nil {
			var re *Error
			if errors.As(err, &re) && re.Code == codeNotLayerOwner {
				return nil, notLayerOwner(fmt.Sprintf("layer %q: %s (approve-all needs every required layer)", layer, re.Msg))
			}
			return nil, err
		}
		details[i] = d
	}
	return details, nil
}
