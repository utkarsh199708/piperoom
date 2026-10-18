package room

import (
	"strconv"
	"time"

	"piperoom/internal/events"
)

// Person is a bare identity (no role/state), used inside suggestions.
// Verified mirrors events.Actor.Verified (docs/design/auth.md section 2.1),
// as do the Verified fields of Member and Driver.
type Person struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Verified bool   `json:"verified"`
}

// Member is someone who has participated in a room.
type Member struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Verified bool   `json:"verified"`
	Role     string `json:"role"` // "driver" | "watching"
}

// Driver is the current driver with their lease.
type Driver struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Verified   bool      `json:"verified"`
	LeaseUntil time.Time `json:"lease_until"`
	// LeaseExpired is the "nobody is driving" signal (docs/api.md, Lease
	// expiry): the last control.lease_expired names this driver and the
	// lease_until of the control.granted that set the current lease. The
	// next control.granted clears it. Omitted when false.
	LeaseExpired bool `json:"lease_expired,omitempty"`
}

// AgentStatus mirrors events.AgentStatusPayload.
type AgentStatus struct {
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`
}

// Hypothesis mirrors events.AgentHypothesisPayload.
type Hypothesis struct {
	Text         string   `json:"text"`
	BasedOnSteps []string `json:"based_on_steps"`
	Confidence   string   `json:"confidence"`
	// Confirmed is derived (F9): at least one confirmed and no disputed
	// verdict on Seq. The agent-written flag in the event is ignored.
	Confirmed bool `json:"confirmed"`
	// Seq is the seq of the agent.hypothesis event this state came from;
	// hypothesis.verdict answers it.
	Seq int64 `json:"seq"`
	// Verdicts is the latest verdict per person on Seq, in seq order.
	Verdicts []Verdict `json:"verdicts"`
}

// Verdict is one person's current confirm or dispute of the hypothesis.
type Verdict struct {
	Verdict string    `json:"verdict"`
	By      Person    `json:"by"`
	Note    string    `json:"note"`
	At      time.Time `json:"at"`
}

// deriveConfirmed sets Confirmed from the verdicts.
func (h *Hypothesis) deriveConfirmed() {
	confirmed := false
	for _, v := range h.Verdicts {
		if v.Verdict == "disputed" {
			h.Confirmed = false
			return
		}
		if v.Verdict == "confirmed" {
			confirmed = true
		}
	}
	h.Confirmed = confirmed
}

// Triage is the newest agent.triage (F6): the triage pass's first look,
// shown as "triage, unverified". A later agent.hypothesis does not clear it.
type Triage struct {
	Status         string    `json:"status"`
	Reason         string    `json:"reason,omitempty"`
	Headline       string    `json:"headline,omitempty"`
	SuspectedLayer string    `json:"suspected_layer,omitempty"`
	Confidence     string    `json:"confidence,omitempty"`
	EvidenceRefs   []string  `json:"evidence_refs"`
	NextCheck      string    `json:"next_check,omitempty"`
	At             time.Time `json:"at"`
}

// Proposal is a pending fix as shown in a snapshot.
type Proposal struct {
	ProposalID     string                `json:"proposal_id"`
	Title          string                `json:"title"`
	Steps          []events.ProposalStep `json:"steps"`
	Risk           string                `json:"risk"`
	Rollback       string                `json:"rollback"`
	RequiredLayers []string              `json:"required_layers"`
	Approvals      map[string]string     `json:"approvals"` // layer -> approved|pending|rejected
	// ParamsHash is the proposal's params_hash (design section 3.1); the UI
	// sends it back with approve and reject. "" only for a proposal that
	// predates the hash and cannot be hashed.
	ParamsHash string `json:"params_hash"`
	// FallbackLayers is proposal.created.fallback_layers: the required
	// layers nobody owns, which the room's driver approves (grant
	// "fallback", P2b). [] when every layer has an owner.
	FallbackLayers []string `json:"fallback_layers"`
	// Preview is what each step would do, written once by the system after
	// the proposal exists (proposal.previewed, F4). Informational: it is not
	// part of ParamsHash. nil until the event arrives.
	Preview *events.ProposalPreviewedPayload `json:"preview,omitempty"`
	// Blocked says why a fully approved proposal does not run:
	// BlockedWriteModeOff. Empty otherwise. Set on the room view only
	// (RoomFor), never by Apply: it depends on the server, not on the log.
	Blocked string `json:"blocked,omitempty"`
}

// ChecklistTick says who ticked one runbook item and when.
type ChecklistTick struct {
	By Person    `json:"by"`
	At time.Time `json:"at"`
}

// checklistKey is the key of RoomSnapshot.ChecklistDone.
func checklistKey(toolUseID string, hit, item int) string {
	return toolUseID + ":" + strconv.Itoa(hit) + ":" + strconv.Itoa(item)
}

// Suggestion is a non-driver's pending steer.
type Suggestion struct {
	SuggestionID string `json:"suggestion_id"`
	From         Person `json:"from"`
	Text         string `json:"text"`
}

// RoomSnapshot is the derived room state (docs/api.md), folded from events.
type RoomSnapshot struct {
	ID         string     `json:"id"`
	Title      string     `json:"title"`
	Severity   string     `json:"severity"`
	PipelineID string     `json:"pipeline_id"`
	Status     string     `json:"status"`
	OpenedAt   time.Time  `json:"opened_at"`
	ClosedAt   *time.Time `json:"closed_at"`
	Resolution string     `json:"resolution"`
	// Shadow is room.created.shadow: the room's pipeline had shadow mode on at
	// open, so the agent ran silently and proposes nothing
	// (docs/design/shadow.md).
	Shadow             bool         `json:"shadow"`
	LastSeq            int64        `json:"last_seq"`
	Driver             *Driver      `json:"driver"`
	AgentStatus        AgentStatus  `json:"agent_status"`
	Hypothesis         *Hypothesis  `json:"hypothesis"`
	Triage             *Triage      `json:"triage"`
	OpenProposals      []Proposal   `json:"open_proposals"`
	PendingSuggestions []Suggestion `json:"pending_suggestions"`
	Members            []Member     `json:"members"`
	// Insight is the room's cost and clock facts (F11), folded from the log.
	Insight Insight `json:"insight"`
	// Mode is what the server does with the caller's approvals (P0 items 1
	// and 4). Set on the room view only (RoomFor), never by Apply; nil, and
	// left out of the JSON, everywhere else.
	Mode *Mode `json:"mode,omitempty"`

	// Labelled is true once a room.labelled exists (F8). ChecklistDone holds
	// the ticked runbook items, keyed "<tool_use_id>:<hit>:<item>" (see
	// checklistKey); the last checklist.step_done per key wins and an untick
	// removes the key. Never null.
	Labelled      bool                     `json:"labelled"`
	ChecklistDone map[string]ChecklistTick `json:"checklist_done"`

	// Room limits (docs/design/retention.md). AutoCloseAt is set by the
	// service, not by Apply: it depends on PIPEROOM_MAX_OPEN_DAYS. LogCap is
	// "ok", "warning" (room.log_cap_warning) or "full" (agent.status
	// log_full).
	AutoCloseAt *time.Time `json:"auto_close_at"`
	Extended    bool       `json:"extended"`
	LogCap      string     `json:"log_cap"`

	// extendedUntil is room.extended.close_at; warnedFor is the close_at of
	// the newest room.retention_warning. Only the service reads them.
	extendedUntil *time.Time
	warnedFor     *time.Time

	// grantedLease is lease_until of the last control.granted: the lease as
	// the log knows it. Driver.LeaseUntil can be later (unlogged renewals,
	// see touchDriverLease); control.lease_expired names grantedLease, so
	// every client folding the log matches it (lease_expiry.go).
	grantedLease time.Time

	// Shadow facts folded from the log after room.closed. Unexported: they are
	// not part of the snapshot JSON (docs/api.md), only the service reads them.
	shadowRevealed bool
	// nudges is when each (proposal, layer) was last nudged (approval.nudged);
	// copy-on-write in Apply.
	nudges         map[string]time.Time
	shadowLabelled bool
	shadowGrade    *events.ShadowGradedPayload // the last grade; nil before any
	// pendingPreviews holds a proposal.previewed seen before its
	// proposal.created (a replay over a snapshot); it is attached when the
	// proposal appears.
	pendingPreviews map[string]events.ProposalPreviewedPayload

	// rejected holds the proposals the log rejected (approval.rejected). The
	// proposal leaves OpenProposals then, so without this a proposals row
	// whose "rejected" status write was lost would look open and take an
	// approval (decisionLockedCheck). Copy-on-write in Apply.
	rejected map[string]bool

	// Postmortem is the closing draft's state (F2): "none", "drafting" from
	// room.closed until postmortem.drafted or postmortem.failed, "drafted" or
	// "failed". The Markdown is rendered on request, never stored.
	Postmortem PostmortemState `json:"postmortem"`

	// handoffReq is the room's latest hand-off request (handoff.requested),
	// cleared by the handoff.created or handoff.failed with its request_id.
	// It is in flight while set and before its expires_at (docs/api.md,
	// Hand-off requests). Unexported, like the shadow facts.
	handoffReq handoffRequest
}

// PostmortemState mirrors the snapshot's postmortem member (docs/api.md).
type PostmortemState struct {
	Status string `json:"status"`           // none | drafting | drafted | failed
	Source string `json:"source,omitempty"` // handoff | model | events, when drafted
	Reason string `json:"reason,omitempty"` // fixed code, when failed
}

// Postmortem states of PostmortemState.Status.
const (
	PostmortemNone     = "none"
	PostmortemDrafting = "drafting"
	PostmortemDrafted  = "drafted"
	PostmortemFailed   = "failed"
)

// handoffRequest is the fold of the last unfinished handoff.requested.
type handoffRequest struct {
	id        string // "" when none is open
	expiresAt time.Time
}

// handoffInFlight reports whether a hand-off request is in flight at now.
func (s RoomSnapshot) handoffInFlight(now time.Time) bool {
	return s.handoffReq.id != "" && now.Before(s.handoffReq.expiresAt)
}

// Log cap states of RoomSnapshot.LogCap, and the agent state the system
// writes when the log is full.
const (
	LogCapOK      = "ok"
	LogCapWarning = "warning"
	LogCapFull    = "full"
	AgentLogFull  = "log_full"
)

// NewSnapshot returns a zero snapshot with non-nil slices and the agent state
// "idle" (docs/api.md: a new room's agent is idle; arrays are never null).
func NewSnapshot() RoomSnapshot {
	return RoomSnapshot{
		AgentStatus:        AgentStatus{State: "idle"},
		Insight:            Insight{TimeToFirstHypothesisS: -1},
		LogCap:             LogCapOK,
		Postmortem:         PostmortemState{Status: PostmortemNone},
		OpenProposals:      []Proposal{},
		PendingSuggestions: []Suggestion{},
		Members:            []Member{},
		ChecklistDone:      map[string]ChecklistTick{},
	}
}

// Clone returns a deep copy. The service returns clones so callers can read or
// encode a snapshot after the service has moved on (and folded more events).
func (s RoomSnapshot) Clone() RoomSnapshot {
	c := s
	if s.Driver != nil {
		d := *s.Driver
		c.Driver = &d
	}
	if s.Hypothesis != nil {
		h := *s.Hypothesis
		if s.Hypothesis.BasedOnSteps != nil {
			h.BasedOnSteps = append([]string{}, s.Hypothesis.BasedOnSteps...)
		}
		h.Verdicts = append([]Verdict{}, s.Hypothesis.Verdicts...)
		c.Hypothesis = &h
	}
	if s.Triage != nil {
		t := *s.Triage
		t.EvidenceRefs = append([]string{}, s.Triage.EvidenceRefs...)
		c.Triage = &t
	}
	if s.shadowGrade != nil {
		g := *s.shadowGrade
		c.shadowGrade = &g
	}
	c.Insight.FirstHypothesisAt = cloneTime(s.Insight.FirstHypothesisAt)
	if s.Insight.Usage.CostUSD != nil {
		v := *s.Insight.Usage.CostUSD
		c.Insight.Usage.CostUSD = &v
	}
	if s.pendingPreviews != nil {
		c.pendingPreviews = make(map[string]events.ProposalPreviewedPayload, len(s.pendingPreviews))
		for k, v := range s.pendingPreviews {
			c.pendingPreviews[k] = v
		}
	}
	if s.Mode != nil {
		m := *s.Mode
		m.AutoApprove = append([]string{}, s.Mode.AutoApprove...)
		c.Mode = &m
	}
	c.AutoCloseAt = cloneTime(s.AutoCloseAt)
	c.extendedUntil = cloneTime(s.extendedUntil)
	c.warnedFor = cloneTime(s.warnedFor)
	c.OpenProposals = cloneProposals(s.OpenProposals)
	c.PendingSuggestions = append([]Suggestion{}, s.PendingSuggestions...)
	c.Members = append([]Member{}, s.Members...)
	c.ChecklistDone = cloneTicks(s.ChecklistDone)
	return c
}

func cloneTicks(in map[string]ChecklistTick) map[string]ChecklistTick {
	out := make(map[string]ChecklistTick, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	c := *t
	return &c
}

func clonePreview(in *events.ProposalPreviewedPayload) *events.ProposalPreviewedPayload {
	if in == nil {
		return nil
	}
	c := *in
	c.Steps = make([]events.StepPreview, len(in.Steps))
	for i, sp := range in.Steps {
		c.Steps[i] = sp
		if sp.Preview != nil {
			pv := *sp.Preview
			pv.Downstream = append([]string(nil), sp.Preview.Downstream...)
			if sp.Preview.DryRun != nil {
				dr := *sp.Preview.DryRun
				pv.DryRun = &dr
			}
			c.Steps[i].Preview = &pv
		}
	}
	return &c
}

func cloneProposals(in []Proposal) []Proposal {
	out := make([]Proposal, len(in))
	for i, p := range in {
		out[i] = p
		out[i].Steps = append([]events.ProposalStep{}, p.Steps...)
		out[i].RequiredLayers = append([]string{}, p.RequiredLayers...)
		out[i].FallbackLayers = append([]string{}, p.FallbackLayers...)
		out[i].Preview = clonePreview(p.Preview)
		if p.Approvals != nil {
			out[i].Approvals = make(map[string]string, len(p.Approvals))
			for k, v := range p.Approvals {
				out[i].Approvals[k] = v
			}
		} else {
			out[i].Approvals = map[string]string{}
		}
	}
	return out
}

// Apply folds one event into the snapshot and returns the updated snapshot.
// It is a pure function: the input is not mutated. Arrays are normalised to
// [] and the agent state defaults to "idle" so the JSON never contains null
// (docs/api.md).
func Apply(snap RoomSnapshot, e events.Event) RoomSnapshot {
	snap.LastSeq = e.Seq
	// Copy the slice headers whose elements Apply may mutate, so the input
	// snapshot is never modified (Apply is a pure function).
	snap.Members = append([]Member(nil), snap.Members...)
	snap.OpenProposals = append([]Proposal(nil), snap.OpenProposals...)
	snap.PendingSuggestions = append([]Suggestion(nil), snap.PendingSuggestions...)
	snap.Insight = foldInsight(snap, e)
	snap.ChecklistDone = cloneTicks(snap.ChecklistDone)
	if e.Actor.Kind == events.ActorHuman {
		snap.Members = upsertMember(snap.Members, e.Actor, "watching")
	}
	switch e.Type {
	case events.RoomCreated:
		p, ok := decode[events.RoomCreatedPayload](e)
		if !ok {
			break
		}
		snap.ID = e.RoomID
		snap.Title = p.Title
		snap.Severity = p.Severity
		snap.PipelineID = p.PipelineID
		snap.Shadow = p.Shadow
		snap.Status = "open"
		snap.OpenedAt = e.At

	case events.ApprovalNudged:
		if p, ok := decode[events.ApprovalNudgedPayload](e); ok {
			m := make(map[string]time.Time, len(snap.nudges)+1)
			for k, v := range snap.nudges {
				m[k] = v
			}
			m[nudgeKey(p.ProposalID, p.Layer)] = e.At
			snap.nudges = m
		}

	case events.RoomLabelled:
		snap.Labelled = true

	case events.ChecklistStepDone:
		p, ok := decode[events.ChecklistStepDonePayload](e)
		if !ok {
			break
		}
		key := checklistKey(p.ToolUseID, p.Hit, p.Item)
		if p.Done {
			snap.ChecklistDone[key] = ChecklistTick{By: Person{ID: e.Actor.ID, Name: e.Actor.Name, Verified: e.Actor.Verified}, At: e.At}
		} else {
			delete(snap.ChecklistDone, key)
		}

	case events.ShadowRevealed:
		snap.shadowRevealed = true

	case events.ShadowLabelled:
		snap.shadowLabelled = true

	case events.ShadowGraded:
		if p, ok := decode[events.ShadowGradedPayload](e); ok {
			snap.shadowGrade = &p
		}

	case events.RoomClosed:
		p, ok := decode[events.RoomClosedPayload](e)
		if !ok {
			break
		}
		at := e.At
		snap.Status = "closed"
		snap.ClosedAt = &at
		snap.Resolution = p.Resolution
		// A shadow room's log holds no agent events: no draft.
		if !snap.Shadow && snap.Postmortem.Status == PostmortemNone {
			snap.Postmortem = PostmortemState{Status: PostmortemDrafting}
		}

	case events.PostmortemDrafted:
		if p, ok := decode[events.PostmortemDraftedPayload](e); ok {
			snap.Postmortem = PostmortemState{Status: PostmortemDrafted, Source: p.Source}
		}

	case events.PostmortemFailed:
		if p, ok := decode[events.PostmortemFailedPayload](e); ok {
			snap.Postmortem = PostmortemState{Status: PostmortemFailed, Reason: p.Reason}
		}

	case events.AgentStatus:
		p, ok := decode[events.AgentStatusPayload](e)
		if !ok {
			break
		}
		snap.AgentStatus = AgentStatus{State: p.State, Detail: p.Detail}
		if p.State == AgentLogFull {
			snap.LogCap = LogCapFull
		}

	case events.RoomLogCapWarning:
		if snap.LogCap != LogCapFull {
			snap.LogCap = LogCapWarning
		}

	case events.RoomRetentionWarning:
		if p, ok := decode[events.RoomRetentionWarningPayload](e); ok {
			at := p.CloseAt.UTC()
			snap.warnedFor = &at
		}

	case events.RoomExtended:
		if p, ok := decode[events.RoomExtendedPayload](e); ok {
			at := p.CloseAt.UTC()
			snap.Extended = true
			snap.extendedUntil = &at
		}

	case events.AgentTriage:
		p, ok := decode[events.AgentTriagePayload](e)
		if !ok {
			break
		}
		refs := append([]string{}, p.EvidenceRefs...)
		snap.Triage = &Triage{
			Status: p.Status, Reason: p.Reason, Headline: p.Headline, SuspectedLayer: p.SuspectedLayer,
			Confidence: p.Confidence, EvidenceRefs: refs, NextCheck: p.NextCheck, At: e.At,
		}

	case events.AgentHypothesis:
		p, ok := decode[events.AgentHypothesisPayload](e)
		if !ok {
			break
		}
		snap.Hypothesis = &Hypothesis{
			Text:         p.Text,
			BasedOnSteps: p.BasedOnSteps,
			Confidence:   p.Confidence,
			Seq:          e.Seq,
			Verdicts:     []Verdict{},
		}

	case events.HypothesisVerdict:
		p, ok := decode[events.HypothesisVerdictPayload](e)
		// A verdict for a hypothesis that was replaced is stale: ignore it.
		if !ok || snap.Hypothesis == nil || p.HypothesisSeq != snap.Hypothesis.Seq {
			break
		}
		// Copy first: Apply must not mutate the caller's snapshot.
		cur := *snap.Hypothesis
		snap.Hypothesis = &cur
		v := Verdict{Verdict: p.Verdict, By: Person{ID: e.Actor.ID, Name: e.Actor.Name, Verified: e.Actor.Verified}, Note: p.Note, At: e.At}
		// Latest per person, in seq order: a changed mind moves to the end.
		kept := make([]Verdict, 0, len(snap.Hypothesis.Verdicts)+1)
		for _, old := range snap.Hypothesis.Verdicts {
			if old.By.ID != v.By.ID {
				kept = append(kept, old)
			}
		}
		snap.Hypothesis.Verdicts = append(kept, v)
		snap.Hypothesis.deriveConfirmed()

	case events.SteerSuggested:
		p, ok := decode[events.SteerSuggestedPayload](e)
		if !ok {
			break
		}
		snap.PendingSuggestions = append(snap.PendingSuggestions, Suggestion{
			SuggestionID: p.SuggestionID,
			From:         Person{ID: e.Actor.ID, Name: e.Actor.Name, Verified: e.Actor.Verified},
			Text:         p.Text,
		})

	case events.SteerSent:
		p, ok := decode[events.SteerSentPayload](e)
		if !ok {
			break
		}
		if p.SuggestionID != "" {
			snap.PendingSuggestions = removeSuggestion(snap.PendingSuggestions, p.SuggestionID)
		}

	case events.SteerDeclined:
		if p, ok := decode[events.SteerDeclinedPayload](e); ok && p.SuggestionID != "" {
			snap.PendingSuggestions = removeSuggestion(snap.PendingSuggestions, p.SuggestionID)
		}

	case events.HandoffRequested:
		if p, ok := decode[events.HandoffRequestedPayload](e); ok {
			snap.handoffReq = handoffRequest{id: p.RequestID, expiresAt: p.ExpiresAt}
		}

	case events.HandoffCreated:
		if p, ok := decode[events.HandoffCreatedPayload](e); ok && p.RequestID != "" && p.RequestID == snap.handoffReq.id {
			snap.handoffReq = handoffRequest{}
		}

	case events.HandoffFailed:
		if p, ok := decode[events.HandoffFailedPayload](e); ok && p.RequestID == snap.handoffReq.id {
			snap.handoffReq = handoffRequest{}
		}

	case events.ControlGranted:
		p, ok := decode[events.ControlGrantedPayload](e)
		if !ok {
			break
		}
		if snap.Driver != nil && snap.Driver.ID != p.To.ID {
			snap.Members = setRole(snap.Members, snap.Driver.ID, "watching")
		}
		snap.Driver = &Driver{ID: p.To.ID, Name: p.To.Name, Verified: p.To.Verified, LeaseUntil: p.LeaseUntil}
		snap.grantedLease = p.LeaseUntil
		snap.Members = upsertMember(snap.Members, p.To, "driver")

	case events.ControlLeaseExpired:
		applyLeaseExpired(&snap, e)

	case events.ProposalCreated:
		p, ok := decode[events.ProposalCreatedPayload](e)
		if !ok {
			break
		}
		approvals := map[string]string{}
		for _, layer := range p.RequiredLayers {
			approvals[layer] = "pending"
		}
		np := Proposal{
			ProposalID:     p.ProposalID,
			Title:          p.Title,
			Steps:          p.Steps,
			Risk:           p.Risk,
			Rollback:       p.Rollback,
			RequiredLayers: p.RequiredLayers,
			Approvals:      approvals,
			ParamsHash:     eventParamsHash(e.RoomID, p),
			FallbackLayers: append([]string{}, p.FallbackLayers...),
		}
		if pv, ok := snap.pendingPreviews[p.ProposalID]; ok {
			np.Preview = &pv
			rest := make(map[string]events.ProposalPreviewedPayload, len(snap.pendingPreviews))
			for k, v := range snap.pendingPreviews {
				if k != p.ProposalID {
					rest[k] = v
				}
			}
			snap.pendingPreviews = rest
		}
		snap.OpenProposals = append(snap.OpenProposals, np)

	case events.ProposalPreviewed:
		p, ok := decode[events.ProposalPreviewedPayload](e)
		if !ok {
			break
		}
		found := false
		for i := range snap.OpenProposals {
			if snap.OpenProposals[i].ProposalID == p.ProposalID {
				// A second event replaces the first (a future refresh).
				pv := p
				snap.OpenProposals[i].Preview = &pv
				found = true
				break
			}
		}
		if !found {
			m := make(map[string]events.ProposalPreviewedPayload, len(snap.pendingPreviews)+1)
			for k, v := range snap.pendingPreviews {
				m[k] = v
			}
			m[p.ProposalID] = p
			snap.pendingPreviews = m
		}

	case events.ApprovalGiven:
		p, ok := decode[events.ApprovalGivenPayload](e)
		if !ok {
			break
		}
		setApproval(snap.OpenProposals, p.ProposalID, p.Layer, "approved")

	case events.ApprovalRejected:
		p, ok := decode[events.ApprovalRejectedPayload](e)
		if !ok {
			break
		}
		setApproval(snap.OpenProposals, p.ProposalID, p.Layer, "rejected")
		rej := make(map[string]bool, len(snap.rejected)+1)
		for k := range snap.rejected {
			rej[k] = true
		}
		rej[p.ProposalID] = true
		snap.rejected = rej
		// A rejection decides the proposal; it is no longer open.
		snap.OpenProposals = removeProposal(snap.OpenProposals, p.ProposalID)

	case events.ActionExecuted:
		p, ok := decode[events.ActionExecutedPayload](e)
		if !ok {
			break
		}
		// The proposal leaves the open list when its last step runs, or when
		// any step fails (the executor stops there, so it is decided).
		if !p.OK || isLastStep(snap.OpenProposals, p.ProposalID, p.StepIndex) {
			snap.OpenProposals = removeProposal(snap.OpenProposals, p.ProposalID)
		}
	}

	return normalize(snap)
}

// eventParamsHash is the params_hash a proposal.created event carries. An event
// written before auth PR 4 has none: the hash is then computed from the event's
// own fields, which is the value creation would have stored (params are frozen).
// It is "" only when the proposal cannot be hashed (see ComputeParamsHash).
func eventParamsHash(roomID string, p events.ProposalCreatedPayload) string {
	if p.ParamsHash != "" {
		return p.ParamsHash
	}
	h, err := ComputeParamsHash(roomID, p.ProposalID, p.Title, p.Risk, p.Rollback, p.RequiredLayers, p.Steps)
	if err != nil {
		return ""
	}
	return h
}

// normalize returns the snapshot with every array non-nil and the agent state
// defaulted, so encoding it never produces null or an empty state
// (docs/api.md: arrays are never null, a new room's agent is "idle").
func normalize(snap RoomSnapshot) RoomSnapshot {
	if snap.Members == nil {
		snap.Members = []Member{}
	}
	if snap.OpenProposals == nil {
		snap.OpenProposals = []Proposal{}
	}
	for i := range snap.OpenProposals {
		// The slice was copied at the top of Apply, so writing fields of its
		// elements (never shared backing arrays) keeps Apply pure.
		if snap.OpenProposals[i].Steps == nil {
			snap.OpenProposals[i].Steps = []events.ProposalStep{}
		}
		if snap.OpenProposals[i].RequiredLayers == nil {
			snap.OpenProposals[i].RequiredLayers = []string{}
		}
		if snap.OpenProposals[i].Approvals == nil {
			snap.OpenProposals[i].Approvals = map[string]string{}
		}
	}
	if snap.PendingSuggestions == nil {
		snap.PendingSuggestions = []Suggestion{}
	}
	if snap.AgentStatus.State == "" {
		snap.AgentStatus.State = "idle"
	}
	if snap.LogCap == "" {
		snap.LogCap = LogCapOK
	}
	if snap.Hypothesis != nil && (snap.Hypothesis.BasedOnSteps == nil || snap.Hypothesis.Verdicts == nil) {
		h := *snap.Hypothesis
		if h.BasedOnSteps == nil {
			h.BasedOnSteps = []string{}
		}
		if h.Verdicts == nil {
			h.Verdicts = []Verdict{}
		}
		snap.Hypothesis = &h
	}
	return snap
}

func decode[T any](e events.Event) (T, bool) {
	v, err := events.Decode[T](e)
	if err != nil {
		var zero T
		return zero, false
	}
	return v, true
}

// upsertMember adds or updates a member without mutating the input slice. The
// name and verified flag follow the latest event of that person.
func upsertMember(members []Member, a events.Actor, role string) []Member {
	for i := range members {
		if members[i].ID == a.ID {
			if members[i].Name == a.Name && members[i].Verified == a.Verified && (role != "driver" || members[i].Role == "driver") {
				return members
			}
			// Copy-on-write so a shared backing array is not mutated.
			out := append([]Member(nil), members...)
			out[i].Name = a.Name
			out[i].Verified = a.Verified
			if role == "driver" {
				out[i].Role = "driver"
			}
			return out
		}
	}
	return append(members, Member{ID: a.ID, Name: a.Name, Verified: a.Verified, Role: role})
}

// setRole changes a member's role without mutating the input slice.
func setRole(members []Member, id, role string) []Member {
	for i := range members {
		if members[i].ID == id {
			if members[i].Role == role {
				return members
			}
			out := append([]Member(nil), members...)
			out[i].Role = role
			return out
		}
	}
	return members
}

func removeSuggestion(suggestions []Suggestion, id string) []Suggestion {
	out := make([]Suggestion, 0, len(suggestions))
	for _, s := range suggestions {
		if s.SuggestionID != id {
			out = append(out, s)
		}
	}
	return out
}

func removeProposal(proposals []Proposal, id string) []Proposal {
	out := make([]Proposal, 0, len(proposals))
	for _, p := range proposals {
		if p.ProposalID != id {
			out = append(out, p)
		}
	}
	return out
}

func isLastStep(proposals []Proposal, proposalID string, stepIndex int) bool {
	for _, p := range proposals {
		if p.ProposalID == proposalID {
			return len(p.Steps) == 0 || stepIndex >= len(p.Steps)-1
		}
	}
	return false
}

// setApproval records a decision on a proposal's approvals map without
// mutating a shared map.
func setApproval(proposals []Proposal, proposalID, layer, decision string) {
	for i := range proposals {
		if proposals[i].ProposalID == proposalID {
			m := make(map[string]string, len(proposals[i].Approvals)+1)
			for k, v := range proposals[i].Approvals {
				m[k] = v
			}
			m[layer] = decision
			proposals[i].Approvals = m
			return
		}
	}
}
