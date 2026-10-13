package room

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gowebpki/jcs"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// paramsHashVersion is the "v" member of the hashed object. It changes only if
// the object's shape changes (docs/design/auth.md section 3.1).
const paramsHashVersion = 2

// errParamsNumber means a proposal holds a number a double cannot carry
// exactly: an integer above 2^53, more than 17 significant digits, a value
// outside the double range. JCS writes numbers as doubles, so the hash would
// cover a different value than the approver sees. Quote such values as strings.
var errParamsNumber = errors.New("a number in the proposal cannot be written exactly as a double (an integer above 2^53 or a value outside the double range); write large integers such as offsets as strings")

// hashedProposal is the object params_hash covers (design section 3.1). The
// member order here does not matter: JCS sorts the keys.
type hashedProposal struct {
	V              int                   `json:"v"`
	RoomID         string                `json:"room_id"`
	ProposalID     string                `json:"proposal_id"`
	Title          string                `json:"title"`
	Risk           string                `json:"risk"`
	Rollback       string                `json:"rollback"`
	RequiredLayers []string              `json:"required_layers"`
	Steps          []events.ProposalStep `json:"steps"`
}

// ComputeParamsHash returns the hex SHA-256 of the RFC 8785 (JCS) form of the
// section 3.1 object for a proposal. It is the one place the hash is defined:
// creation, the approve and reject check, the executor re-check, the snapshot
// of a proposal that predates the hash and the export all call it, so they
// cannot drift apart.
//
// It fails with errParamsNumber when a number is not exact as a double, and
// with the JCS error when the steps hold a duplicate object key (a jsonb column
// would keep only one of the two, so the stored row would hash differently).
func ComputeParamsHash(roomID, proposalID, title, risk, rollback string, requiredLayers []string, steps []events.ProposalStep) (string, error) {
	if steps == nil {
		steps = []events.ProposalStep{}
	}
	raw, err := json.Marshal(hashedProposal{
		V: paramsHashVersion, RoomID: roomID, ProposalID: proposalID,
		Title: title, Risk: risk, Rollback: rollback,
		RequiredLayers: nonNilStrings(requiredLayers), Steps: steps,
	})
	if err != nil {
		return "", fmt.Errorf("room: marshal proposal for hash: %w", err)
	}
	form, inexact := hashForm(raw)
	if inexact > 0 {
		return "", errParamsNumber
	}
	canon, err := jcs.Transform(form)
	if err != nil {
		return "", fmt.Errorf("room: canonicalize proposal for hash: %w", err)
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}

// checkParamsHash compares the hash an approver or rejecter echoed with the
// proposal's own (design section 3.2). Caller holds s.mu.
//
// Old data: a proposal created before auth PR 4 has no stored hash. Its params
// are frozen, so the hash is computed now from the stored row and written back
// (SetProposalParamsHash), exactly what creation would have stored; the
// snapshot shows the same value (eventParamsHash), so the web app has it to
// send. If such a proposal cannot be hashed (a number above 2^53, a duplicate
// key) it cannot be decided, and the message says so.
func (s *Service) checkParamsHash(ctx context.Context, p *store.ProposalRow, given string) error {
	if p.ParamsHash == "" {
		h, _, err := rowParamsHash(*p)
		if err != nil {
			return validation("this proposal was created before params_hash existed and cannot be hashed (" + err.Error() + "); ask the agent to propose the fix again")
		}
		if given != h {
			return paramsMismatch(paramsMismatchMsg)
		}
		if err := s.store.SetProposalParamsHash(ctx, p.ProposalID, h); err != nil {
			return mapStoreErr(err)
		}
		p.ParamsHash = h
		return nil
	}
	if given != p.ParamsHash {
		return paramsMismatch(paramsMismatchMsg)
	}
	return nil
}

const paramsMismatchMsg = "params_hash does not match this proposal; reload the room and review the proposal again"

// identityFor builds the identity recorded with a decision: the session facts
// the HTTP layer put on the user, plus how the right to decide was derived
// (the grant). A user with no session (a dev-mode X-User request, a test) gets
// method "dev" and no session id; the identity is never empty.
func identityFor(u User, grant string) events.Identity {
	id := u.Identity
	if id.Method == "" {
		id.Method = "dev"
	}
	id.Grant = grant
	if !id.AuthTime.IsZero() {
		id.AuthTime = id.AuthTime.UTC()
	}
	return id
}

// approvalRow is the approvals table row for one decision.
func approvalRow(proposalID, layer, decision, reason, paramsHash string, id events.Identity) store.ApprovalRow {
	return store.ApprovalRow{
		ProposalID: proposalID, Layer: layer, Decision: decision, Reason: reason, ParamsHash: paramsHash,
		IdentityIss: id.Iss, IdentitySub: id.Sub, IdentitySessionID: id.SessionID,
		IdentityMethod: id.Method, IdentityGrant: id.Grant, IdentityAuthTime: id.AuthTime,
	}
}

// rowParamsHash recomputes the hash from a stored proposal row, the same way
// creation computed it from the in-memory proposal. It also returns the decoded
// steps, so a caller that goes on to run them runs exactly what was hashed.
func rowParamsHash(p store.ProposalRow) (string, []events.ProposalStep, error) {
	var steps []events.ProposalStep
	if err := json.Unmarshal(p.Steps, &steps); err != nil {
		return "", nil, fmt.Errorf("room: decode proposal %s steps: %w", p.ProposalID, err)
	}
	h, err := ComputeParamsHash(p.RoomID, p.ProposalID, p.Title, p.Risk, p.Rollback, p.RequiredLayers, steps)
	return h, steps, err
}
