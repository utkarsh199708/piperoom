package room

import (
	"slices"
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
	// Scope, Team and ChangeID are copied from the payload (X4): scope
	// "unknown" when it has none, team "" and change_id 0 when unset.
	Scope    events.HypothesisScope `json:"scope"`
	Team     string                 `json:"team"`
	ChangeID int64                  `json:"change_id"`
}

// TeamPaged is one room.team_paged of the room (X4d): which team the driver
// paged, for which hypothesis, by whom and when.
type TeamPaged struct {
	Team          string    `json:"team"`
	HypothesisSeq int64     `json:"hypothesis_seq"`
	By            Person    `json:"by"`
	At            time.Time `json:"at"`
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
	// Seq is the seq of the proposal.created: it orders an open proposal
	// against RoomSnapshot.LastFix, which has left this list (docs/api.md).
	Seq int64 `json:"seq"`
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
	// Scheduled is the pending policy countdown (proposal.scheduled, P2b):
	// set while the policy has neither run nor been stopped; nil otherwise.
	Scheduled *Scheduled `json:"scheduled,omitempty"`

	// X8 staging (docs/api.md "X8 snapshot fields"). Stageable and
	// StagingRequired depend on the server's actions and owners.yaml, so the
	// room view sets them (RoomFor); Apply leaves Stageable empty and
	// StagingRequired false. StageRunning is the newest proposal.stage_started
	// with no proposal.staged after it (RoomFor hides it past expires_at);
	// Staged is the newest proposal.staged, kept while its params_hash differs
	// from the proposal's (only a matching one counts).
	Stageable       []int                         `json:"stageable"`
	StageRunning    *StageRunning                 `json:"stage_running,omitempty"`
	Staged          *events.ProposalStagedPayload `json:"staged,omitempty"`
	StagingRequired bool                          `json:"staging_required,omitempty"`
	// stages counts the proposal's proposal.stage_started events: at most
	// maxStagesPerProposal (service_stage.go).
	stages int

	// Origin is proposal.created.origin: "agent" or "driver"
	// (POST /api/rooms/{id}/proposals); "" reads as "agent" and is normalised
	// to it when the snapshot is folded. The auto-approve policy never
	// schedules a driver's proposal (autoapprove.go). (P3c)
	Origin string `json:"origin"`
	// Class is proposal.created.class (X2/X6): "recovery" or "performance";
	// "" (omitted) reads as "fix", so a fix proposal marshals as before.
	Class string `json:"class,omitempty"`
	// sched is the fold of the proposal's policy countdown (autoapprove.go).
	sched schedState
	// approvers is the actor name of each layer's approval.given
	// ("layer -> display name"): the executor's actions.StepRef.Approvals
	// (Q7k). Copy-on-write in Apply.
	approvers map[string]string
	// approvalActors is the actor of each layer's approval.given
	// ("layer -> actor"): pipeline_register's ApprovedBy and the overlap
	// approver check read it (U1-14). Copy-on-write in Apply.
	approvalActors map[string]events.Actor
	// approvalGrants is the identity.grant of each layer's approval.given
	// ("layer -> principal detail"): the principal the approval route matched
	// ("user:<id>", "group:<name>", "backup:group:<name>", or "admin",
	// "fallback", "solo", "policy"). pipeline_register's overlap re-check
	// reads it, so an approval an IdP group gave is matched from the log
	// without users.yaml (U1-14). Copy-on-write in Apply.
	approvalGrants map[string]string
	// outcomes is each step's action.executed outcome so far, by step index
	// (outcomeNotRun until one arrives). Copy-on-write in Apply. It feeds the
	// proposal_executed input of a driver's proposal (proposalExecutedInput).
	outcomes []stepOutcome
}

// stepOutcome is one step's folded action.executed result.
type stepOutcome uint8

const (
	outcomeNotRun stepOutcome = iota
	outcomeOK
	outcomeFailed
	outcomeUnknown // action.executed outcome_unknown
)

// finishedProposal is a driver-authored proposal whose run ended with the
// event at seq: its last step ran, or a step failed (the executor stops
// there). Set by Apply for that event only, so the input it yields is derived
// once per log, the same way on every replica and on every rebuild. (P3c)
type finishedProposal struct {
	seq        int64
	proposalID string
	origin     string
	outcomes   []stepOutcome // one per step; outcomeNotRun for a step that never ran
}

// LastFix is the room's newest fix whose run ended (docs/api.md "Last fix"):
// the newest proposal, by its proposal.created, whose last step ran or whose
// step failed. An executed proposal has left OpenProposals, so this is what
// tells the room list that a fix ran (KAN-594). Never mutated once set: Apply
// replaces it.
type LastFix struct {
	ProposalID string `json:"proposal_id"`
	// Seq is the seq of its proposal.created, like Proposal.Seq.
	Seq   int64 `json:"seq"`
	Steps int   `json:"steps"`
	// FailedStep is the 1-based step that failed or has an unknown outcome
	// (the executor stops there); 0 when every step ran.
	FailedStep int `json:"failed_step"`
	// Verification is from the proposal's newest agent.verification; nil before
	// one exists.
	Verification *FixVerification `json:"verification,omitempty"`
}

// FixVerification is the part of an agent.verification the room's status reads.
type FixVerification struct {
	Recovered bool   `json:"recovered"`
	Verdict   string `json:"verdict"`
	// repeat is agent.verification.repeat: a repeat never overwrites a full
	// verdict (foldFixVerdict).
	repeat bool
}

// proposalOrigin normalises proposal.created.origin for the room view: "" (an
// agent proposal written before P3c) reads as "agent" (docs/api.md).
func proposalOrigin(o string) string {
	if o == "" {
		return events.ProposalOriginAgent
	}
	return o
}

// StageRunning is a proposal.stage_started of the proposal with no
// proposal.staged after it (open_proposals[].stage_running, X8). ParamsHash
// is the stage's, for the room service; it is not part of the JSON.
type StageRunning struct {
	By         Person    `json:"by"`
	StartedAt  time.Time `json:"started_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	Steps      []int     `json:"steps"`
	paramsHash string
}

// Scheduled mirrors the pending proposal.scheduled of a proposal
// (open_proposals[].scheduled, docs/api.md).
type Scheduled struct {
	RunAt       time.Time `json:"run_at"`
	CancelUntil time.Time `json:"cancel_until"`
}

// schedState is the log's answer to "is the policy countdown of this
// proposal still live?". set: a proposal.scheduled was folded (at most one
// counts). stopped: a proposal.cancelled or a person's approval.given came
// after it, so the policy never runs. policyN: how many policy
// approvals landed; once above 0 the countdown is over but the policy may
// still owe the remaining layers (a replica that stopped part-way), and every
// approved layer must be one of them (fireCheckLocked). A value type, so a
// proposal copy never shares it.
type schedState struct {
	set         bool
	runAt       time.Time
	cancelUntil time.Time
	stopped     bool
	policyN     int
}

// pending reports whether the policy still owes this proposal its approvals.
func (s schedState) pending() bool { return s.set && !s.stopped }

// cancellable reports whether a person may still cancel the countdown: it is
// pending and the policy has not started approving.
func (s schedState) cancellable() bool { return s.pending() && s.policyN == 0 }

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
	Shadow bool `json:"shadow"`
	// Origin is room.created.origin: "forecast" for a room a forecast signal
	// opened (X3), omitted otherwise. Forecast is the newest forecast signal
	// of the room's watcher evidence (forecast.go), omitted when none.
	Origin      string      `json:"origin,omitempty"`
	Forecast    *Forecast   `json:"forecast,omitempty"`
	LastSeq     int64       `json:"last_seq"`
	Driver      *Driver     `json:"driver"`
	AgentStatus AgentStatus `json:"agent_status"`
	Hypothesis  *Hypothesis `json:"hypothesis"`
	// TeamPages are the room.team_paged events in seq order (X4d); never null.
	TeamPages          []TeamPaged  `json:"team_pages"`
	Triage             *Triage      `json:"triage"`
	OpenProposals      []Proposal   `json:"open_proposals"`
	PendingSuggestions []Suggestion `json:"pending_suggestions"`
	Members            []Member     `json:"members"`
	// PullRequests are the fix PRs the room's steps opened (Q7,
	// docs/api.md "Q7 snapshot field"): room-level, newest first, at most
	// maxSnapshotPullRequests; never null. An executed proposal leaves
	// OpenProposals, but its PR stays here and stays followed (Q7e).
	PullRequests []PullRequest `json:"pull_requests"`
	// LastFix is the room's newest fix whose run ended; omitted until one exists.
	LastFix *LastFix `json:"last_fix,omitempty"`

	// finished is the driver-authored proposal the last folded event ended
	// (action.executed), nil after any other event. Never mutated once set.
	finished *finishedProposal

	// Insight is the room's cost and clock facts (F11), folded from the log.
	Insight Insight `json:"insight"`
	// Mode is what the server does with the caller's approvals (P0 items 1
	// and 4). Set on the room view only (RoomFor), never by Apply; nil, and
	// left out of the JSON, everywhere else.
	Mode *Mode `json:"mode,omitempty"`
	// Setup is the setup room's state (U wave; docs/api.md "U-wave snapshot
	// fields"), folded from the setup.* events of the log. Nil on every
	// incident room, so it is absent there.
	Setup *SetupSnapshot `json:"setup,omitempty"`

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

	// alertRoom is true for a room an alert opened (room.created carries an
	// alert_source or an alert). controlRequested is true once a
	// control.requested was logged while the room had no driver. Together
	// with Driver and Shadow they decide the sole-member grant (P2b,
	// service_solemember.go). Unexported: not part of the snapshot JSON.
	alertRoom        bool
	controlRequested bool

	// agentRan is true once the log holds an event of the agent actor. A
	// forecast room without one is quiet (quietRoom): nothing has run in it.
	agentRan bool
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
		TeamPages:          []TeamPaged{},
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
	if s.Forecast != nil {
		f := *s.Forecast
		c.Forecast = &f
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
	if s.LastFix != nil {
		f := *s.LastFix
		if s.LastFix.Verification != nil {
			v := *s.LastFix.Verification
			f.Verification = &v
		}
		c.LastFix = &f
	}
	c.Setup = s.Setup.clone()
	c.AutoCloseAt = cloneTime(s.AutoCloseAt)
	c.extendedUntil = cloneTime(s.extendedUntil)
	c.warnedFor = cloneTime(s.warnedFor)
	c.OpenProposals = cloneProposals(s.OpenProposals)
	c.PendingSuggestions = append([]Suggestion{}, s.PendingSuggestions...)
	c.Members = append([]Member{}, s.Members...)
	c.ChecklistDone = cloneTicks(s.ChecklistDone)
	c.TeamPages = append([]TeamPaged{}, s.TeamPages...)
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

// cloneStaged deep-copies a proposal.staged payload: its steps and their
// results.
func cloneStaged(in *events.ProposalStagedPayload) *events.ProposalStagedPayload {
	if in == nil {
		return nil
	}
	c := *in
	c.Steps = make([]events.StepStage, len(in.Steps))
	for i, st := range in.Steps {
		c.Steps[i] = st
		if st.Result != nil {
			r := *st.Result
			if st.Result.DryRun != nil {
				dr := *st.Result.DryRun
				r.DryRun = &dr
			}
			c.Steps[i].Result = &r
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
		if p.Scheduled != nil {
			sc := *p.Scheduled
			out[i].Scheduled = &sc
		}
		out[i].Stageable = append([]int{}, p.Stageable...)
		if p.StageRunning != nil {
			sr := *p.StageRunning
			sr.Steps = append([]int{}, p.StageRunning.Steps...)
			out[i].StageRunning = &sr
		}
		out[i].Staged = cloneStaged(p.Staged)
		out[i].outcomes = append([]stepOutcome(nil), p.outcomes...)
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
	snap.finished = nil
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
	if e.Actor.Kind == events.ActorAgent {
		snap.agentRan = true
	}
	applyForecastEvidence(&snap, e)
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
		snap.Origin = p.Origin
		snap.alertRoom = p.AlertSource != "" || len(p.Alert) > 0
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

	case events.RoomEscalated:
		// The service writes only a raise; the fold takes the event as is.
		if p, ok := decode[events.RoomEscalatedPayload](e); ok && severities[p.Severity] {
			snap.Severity = p.Severity
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
			Scope:        p.Scope.Normalize(),
			Team:         p.Team,
			ChangeID:     p.ChangeID,
		}

	case events.RoomTeamPaged:
		p, ok := decode[events.TeamPagedPayload](e)
		if !ok {
			break
		}
		// A full slice expression: the append never writes into the caller's
		// backing array.
		snap.TeamPages = append(snap.TeamPages[:len(snap.TeamPages):len(snap.TeamPages)], TeamPaged{
			Team: p.Team, HypothesisSeq: p.HypothesisSeq,
			By: Person{ID: e.Actor.ID, Name: e.Actor.Name, Verified: e.Actor.Verified}, At: e.At,
		})

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

	case events.ControlRequested:
		if snap.Driver == nil {
			snap.controlRequested = true
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
			Seq:            e.Seq,
			Title:          p.Title,
			Steps:          p.Steps,
			Risk:           p.Risk,
			Rollback:       p.Rollback,
			RequiredLayers: p.RequiredLayers,
			Approvals:      approvals,
			ParamsHash:     eventParamsHash(e.RoomID, p),
			FallbackLayers: append([]string{}, p.FallbackLayers...),
			Origin:         proposalOrigin(p.Origin),
		}
		if p.Class != events.ActionClassFix {
			np.Class = p.Class
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
			m := make(map[string]events.ProposalPreviewedPayload, len(snap.pendingPreviews))
			for k, v := range snap.pendingPreviews {
				m[k] = v
			}
			m[p.ProposalID] = p
			snap.pendingPreviews = m
		}

	case events.ProposalStageStarted:
		if p, ok := decode[events.ProposalStageStartedPayload](e); ok {
			foldStageStarted(snap.OpenProposals, e, p)
		}

	case events.ProposalStaged:
		if p, ok := decode[events.ProposalStagedPayload](e); ok {
			foldStaged(snap.OpenProposals, p)
		}

	case events.ApprovalGiven:
		p, ok := decode[events.ApprovalGivenPayload](e)
		if !ok {
			break
		}
		setApproval(snap.OpenProposals, p.ProposalID, p.Layer, "approved")
		setApprover(snap.OpenProposals, p.ProposalID, p.Layer, e.Actor.Name)
		setApproverActor(snap.OpenProposals, p.ProposalID, p.Layer, e.Actor)
		grant := p.Grant
		if p.Identity != nil && p.Identity.Grant != "" {
			grant = p.Identity.Grant
		}
		setApproverGrant(snap.OpenProposals, p.ProposalID, p.Layer, grant)
		foldScheduleDecision(snap.OpenProposals, p.ProposalID, e.Actor.Kind == events.ActorSystem && p.Grant == events.GrantPolicy)

	case events.ProposalScheduled:
		if p, ok := decode[events.ProposalScheduledPayload](e); ok {
			foldScheduled(snap.OpenProposals, p)
		}

	case events.ProposalCancelled:
		if p, ok := decode[events.ProposalCancelledPayload](e); ok {
			foldScheduleDecision(snap.OpenProposals, p.ProposalID, false)
		}

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
		foldStepOutcome(&snap, e.Seq, p)
		if p.OK && p.PullRequest != nil {
			origin := ""
			if op, ok := openProposal(snap, p.ProposalID); ok {
				origin = op.Origin
			}
			snap.PullRequests = notePROrigin(addPullRequest(snap.PullRequests, p, e.At), p.ProposalID, p.StepIndex, origin)
		}
		// The proposal leaves the open list when its last step runs, or when
		// any step fails (the executor stops there, so it is decided).
		if !p.OK || isLastStep(snap.OpenProposals, p.ProposalID, p.StepIndex) {
			snap.LastFix = foldLastFix(snap.LastFix, snap.OpenProposals, p)
			snap.OpenProposals = removeProposal(snap.OpenProposals, p.ProposalID)
		}

	case events.PullRequestUpdated:
		if p, ok := decode[events.PullRequestUpdatedPayload](e); ok {
			snap.PullRequests = applyPRUpdate(snap.PullRequests, p, e.At)
			snap.PullRequests = notePRFinal(snap.PullRequests, p, e)
		}

	case events.AgentVerification:
		if p, ok := decode[events.AgentVerificationPayload](e); ok {
			snap.PullRequests = notePRVerified(snap.PullRequests, p.ProposalID)
			snap.LastFix = foldFixVerdict(snap.LastFix, p)
		}
	}

	// The setup member folds from the setup.* events (setup_snapshot.go). It
	// runs after the switch so a setup room folds exactly like every other room
	// plus this member.
	snap = applySetup(snap, e)

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
		if snap.OpenProposals[i].Stageable == nil {
			snap.OpenProposals[i].Stageable = []int{}
		}
	}
	if snap.PendingSuggestions == nil {
		snap.PendingSuggestions = []Suggestion{}
	}
	if snap.TeamPages == nil {
		snap.TeamPages = []TeamPaged{}
	}
	if snap.PullRequests == nil {
		snap.PullRequests = []PullRequest{}
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

// foldStepOutcome records an action.executed outcome on its open proposal
// and, when that ends a driver-authored proposal's run (the last step, or a
// failed or unknown step), sets snap.finished. Runs before the proposal leaves
// the open list. snap.OpenProposals is already Apply's copy; the outcomes
// slice is copied before it changes.
func foldStepOutcome(snap *RoomSnapshot, seq int64, p events.ActionExecutedPayload) {
	i := slices.IndexFunc(snap.OpenProposals, func(pr Proposal) bool { return pr.ProposalID == p.ProposalID })
	if i < 0 {
		return
	}
	pr := &snap.OpenProposals[i]
	n := len(pr.Steps)
	if p.StepIndex < 0 || p.StepIndex >= n {
		return
	}
	out := make([]stepOutcome, n)
	copy(out, pr.outcomes)
	switch {
	case p.OutcomeUnknown:
		out[p.StepIndex] = outcomeUnknown
	case p.OK:
		out[p.StepIndex] = outcomeOK
	default:
		out[p.StepIndex] = outcomeFailed
	}
	pr.outcomes = out
	if pr.Origin != events.ProposalOriginDriver {
		return
	}
	if p.OK && p.StepIndex < n-1 {
		return
	}
	snap.finished = &finishedProposal{seq: seq, proposalID: pr.ProposalID, origin: pr.Origin, outcomes: out}
}

// foldLastFix returns last_fix after the action.executed p ended an open
// proposal's run. The newest proposal by proposal.created wins, as in the
// room's title row, so an older fix that finishes later replaces nothing.
func foldLastFix(last *LastFix, open []Proposal, p events.ActionExecutedPayload) *LastFix {
	i := slices.IndexFunc(open, func(pr Proposal) bool { return pr.ProposalID == p.ProposalID })
	if i < 0 || (last != nil && open[i].Seq < last.Seq) {
		return last
	}
	fix := &LastFix{ProposalID: p.ProposalID, Seq: open[i].Seq, Steps: len(open[i].Steps)}
	if !p.OK {
		fix.FailedStep = max(p.StepIndex, 0) + 1
	}
	return fix
}

// foldFixVerdict returns last_fix with the verdict of an agent.verification
// of that proposal. A repeat (already verified, no checks) never overwrites a
// full verdict, as in the web reducer.
func foldFixVerdict(last *LastFix, p events.AgentVerificationPayload) *LastFix {
	if last == nil || last.ProposalID != p.ProposalID {
		return last
	}
	if p.Repeat && last.Verification != nil && !last.Verification.repeat {
		return last
	}
	fix := *last
	fix.Verification = &FixVerification{Recovered: p.Recovered, Verdict: p.Verdict, repeat: p.Repeat}
	return &fix
}

func isLastStep(proposals []Proposal, proposalID string, stepIndex int) bool {
	for _, p := range proposals {
		if p.ProposalID == proposalID {
			return len(p.Steps) == 0 || stepIndex >= len(p.Steps)-1
		}
	}
	return false
}

// foldStageStarted folds a proposal.stage_started into its open proposal:
// the stage runs until a proposal.staged (or its expires_at, which the room
// view and the stage route read), and it counts toward the per-proposal cap.
// A stage of a proposal that is no longer open is dropped.
func foldStageStarted(proposals []Proposal, e events.Event, p events.ProposalStageStartedPayload) {
	for i := range proposals {
		if proposals[i].ProposalID != p.ProposalID {
			continue
		}
		proposals[i].stages++
		proposals[i].StageRunning = &StageRunning{
			By:         Person{ID: e.Actor.ID, Name: e.Actor.Name, Verified: e.Actor.Verified},
			StartedAt:  e.At.UTC(),
			ExpiresAt:  p.ExpiresAt.UTC(),
			Steps:      append([]int{}, p.Steps...),
			paramsHash: p.ParamsHash,
		}
		return
	}
}

// foldStaged folds a proposal.staged into its open proposal: it ends the
// running stage and replaces the previous result (newest stage wins).
func foldStaged(proposals []Proposal, p events.ProposalStagedPayload) {
	for i := range proposals {
		if proposals[i].ProposalID != p.ProposalID {
			continue
		}
		proposals[i].StageRunning = nil
		proposals[i].Staged = cloneStaged(&p)
		return
	}
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

// setApprover records who gave a layer's approval (copy-on-write).
func setApprover(proposals []Proposal, proposalID, layer, name string) {
	for i := range proposals {
		if proposals[i].ProposalID == proposalID {
			m := make(map[string]string, len(proposals[i].approvers)+1)
			for k, v := range proposals[i].approvers {
				m[k] = v
			}
			m[layer] = name
			proposals[i].approvers = m
			return
		}
	}
}

// setApproverActor records the actor who gave a layer's approval
// (copy-on-write; U1-14: pipeline_register's ApprovedBy and the overlap
// approver check).
func setApproverActor(proposals []Proposal, proposalID, layer string, actor events.Actor) {
	for i := range proposals {
		if proposals[i].ProposalID == proposalID {
			m := make(map[string]events.Actor, len(proposals[i].approvalActors)+1)
			for k, v := range proposals[i].approvalActors {
				m[k] = v
			}
			m[layer] = actor
			proposals[i].approvalActors = m
			return
		}
	}
}

// setApproverGrant records the principal detail a layer's approval was given
// under (copy-on-write; U1-14: the overlap re-check of pipeline_register).
func setApproverGrant(proposals []Proposal, proposalID, layer, grant string) {
	for i := range proposals {
		if proposals[i].ProposalID == proposalID {
			m := make(map[string]string, len(proposals[i].approvalGrants)+1)
			for k, v := range proposals[i].approvalGrants {
				m[k] = v
			}
			m[layer] = grant
			proposals[i].approvalGrants = m
			return
		}
	}
}

// foldScheduled folds a proposal.scheduled into its open proposal. Only the
// first one counts (at most one per proposal), and a proposal a person has
// already decided on is not given one.
func foldScheduled(proposals []Proposal, p events.ProposalScheduledPayload) {
	for i := range proposals {
		if proposals[i].ProposalID != p.ProposalID || proposals[i].sched.set {
			continue
		}
		proposals[i].sched = schedState{set: true, runAt: p.RunAt, cancelUntil: p.CancelUntil}
		for _, d := range proposals[i].Approvals {
			if d != "pending" {
				// Never written by the service (the schedule check wants
				// every layer pending); a log that has it anyway gets no
				// policy run.
				proposals[i].sched.stopped = true
				return
			}
		}
		proposals[i].Scheduled = &Scheduled{RunAt: p.RunAt, CancelUntil: p.CancelUntil}
		return
	}
}

// foldScheduleDecision ends the visible countdown of a proposal. policy is
// true for the policy's own approval.given: the countdown ran out, and the
// policy goes on with the remaining layers. Anything else (a person's
// approval, a proposal.cancelled) stops the policy for good: the explicit
// stop lives here, in the fold every replica runs, so the fire path
// (fireCheckLocked) sees it whichever replica logged the decision.
func foldScheduleDecision(proposals []Proposal, proposalID string, policy bool) {
	for i := range proposals {
		if proposals[i].ProposalID != proposalID {
			continue
		}
		proposals[i].Scheduled = nil
		if policy {
			proposals[i].sched.policyN++
		} else if proposals[i].sched.set {
			proposals[i].sched.stopped = true
		}
		return
	}
}
