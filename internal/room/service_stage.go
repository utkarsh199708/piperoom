package room

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

// X8 staging: "Try in staging" (docs/api.md "X8 staging",
// POST /api/proposals/{pid}/stage).
//
// The driver asks; the route checks under the room's lock, runs each
// stageable step's Validate and CheckScope again outside it, then appends
// proposal.stage_started (a conditional append, so two replicas cannot both
// start a stage) and answers 202 at once. A goroutine on the service lifetime
// then calls Stage once per stageable step, in order, each bounded by
// actions.StageStepTimeout, and appends one proposal.staged. Nothing here
// touches production: Stage writes only through the actions' Deps.Staging, and
// the executor never calls it. "A stage is running" is the log's answer (a
// proposal.stage_started with no proposal.staged, before its expires_at), not
// this process's memory, so it holds across replicas and restarts; a stage
// whose server stopped is lost and may be started again after expires_at.

// maxStagesPerProposal caps proposal.stage_started per proposal (429).
const maxStagesPerProposal = 5

// stagedWriteTimeout bounds the append of proposal.staged; the stage's
// expires_at leaves 30 s for the event writes (actions.StageExpiry).
const stagedWriteTimeout = 20 * time.Second

func noStaging() *Error {
	return &Error{Status: http.StatusConflict, Code: "no_staging", Msg: "this pipeline declares no staging stack, or write mode is off: nothing can be staged"}
}

func notStageable() *Error {
	return &Error{Status: http.StatusConflict, Code: "not_stageable", Msg: "no step of this proposal can be tried in staging"}
}

func stageInProgress() *Error {
	return &Error{Status: http.StatusConflict, Code: "stage_in_progress", Msg: "a stage of this proposal is still running; wait for its result"}
}

func stagingRequired(msg string) *Error {
	return &Error{Status: http.StatusConflict, Code: "staging_required", Msg: msg}
}

func stageRateLimited() *Error {
	return &Error{Status: http.StatusTooManyRequests, Code: "rate_limited", Msg: fmt.Sprintf("at most %d stages per proposal", maxStagesPerProposal)}
}

// stagingOn is mode.staging and agent.Alert.Staging: write mode is on, the
// pipeline declares staging: in owners.yaml and, with per-pipeline client
// sets, its actions hold the staging clients (wiring builds them only then).
func (s *Service) stagingOn(pipelineID string) bool {
	if s.writeOff || s.owners == nil || pipelineID == "" {
		return false
	}
	p, ok := s.owners.Pipeline(pipelineID)
	if !ok || !p.HasStaging() {
		return false
	}
	if s.stackFor != nil {
		return s.stackFor(pipelineID).Staging
	}
	return true
}

// stageableSteps lists the indexes of the steps whose action implements
// actions.Stager, in order; never nil.
func stageableSteps(acts *actions.Registry, steps []events.ProposalStep) []int {
	out := []int{}
	for i, st := range steps {
		if a, ok := acts.Get(st.Action); ok {
			if _, ok := a.(actions.Stager); ok {
				out = append(out, i)
			}
		}
	}
	return out
}

// requireStagingFor is the pipeline's owners.yaml require_staging: list as a
// set; empty without owners or entries.
func (s *Service) requireStagingFor(pipelineID string) map[string]bool {
	out := map[string]bool{}
	if s.owners == nil {
		return out
	}
	p, ok := s.owners.Pipeline(pipelineID)
	if !ok {
		return out
	}
	for _, a := range p.RequireStagingList() {
		out[a] = true
	}
	return out
}

// stagedHealthy reports whether step i of p has a healthy stage that counts:
// the newest proposal.staged, with the proposal's current params_hash.
func stagedHealthy(p Proposal, i int) bool {
	st := p.Staged
	return st != nil && p.ParamsHash != "" && st.ParamsHash == p.ParamsHash && st.StepHealthy(i)
}

// stagingGate is the require_staging: rule: nil when every step whose action
// the pipeline names there has a healthy stage that counts, else 409
// staging_required naming the first such step and its action. approve,
// approve-all and the auto-approve policy all check it.
func (s *Service) stagingGate(pipelineID string, p Proposal) error {
	req := s.requireStagingFor(pipelineID)
	if len(req) == 0 {
		return nil
	}
	for i, st := range p.Steps {
		if req[st.Action] && !stagedHealthy(p, i) {
			return stagingRequired(fmt.Sprintf("step %d (%s) needs a healthy stage of this proposal before approval (owners.yaml require_staging:); try it in staging first", i, st.Action))
		}
	}
	return nil
}

// stagingGateLocked is stagingGate for an open proposal of the room. A
// proposal that is not open is left to the caller's other checks. Caller
// holds st's lock.
func (s *Service) stagingGateLocked(st *roomState, proposalID string) error {
	p, ok := openProposal(st.snap, proposalID)
	if !ok {
		return nil
	}
	return s.stagingGate(st.snap.PipelineID, p)
}

// stageRunningAt reports whether p has a stage that is still running at now.
func stageRunningAt(p Proposal, now time.Time) bool {
	return p.StageRunning != nil && now.Before(p.StageRunning.ExpiresAt)
}

// stageCheckLocked is the stage route's rule, in docs/api.md's order: the
// driver, an open room, the proposal open and not fully approved, staging on,
// a stageable step, no stage running and fewer than maxStagesPerProposal
// stages. It returns the proposal as folded and its stageable steps. Caller
// holds st's lock.
func (s *Service) stageCheckLocked(ctx context.Context, st *roomState, row store.ProposalRow, user User) (Proposal, []int, error) {
	if !s.isDriver(st, user.ID) {
		return Proposal{}, nil, ErrNotDriver()
	}
	if !s.roomOpen(st) {
		return Proposal{}, nil, roomClosed("room is closed")
	}
	if err := s.decisionLockedCheck(ctx, st, row); err != nil {
		return Proposal{}, nil, err
	}
	p, ok := openProposal(st.snap, row.ProposalID)
	if !ok {
		return Proposal{}, nil, alreadyDecided("proposal already decided")
	}
	if fullyApproved(p) {
		return Proposal{}, nil, alreadyDecided("every layer of this proposal is already approved")
	}
	if !s.stagingOn(st.snap.PipelineID) {
		return Proposal{}, nil, noStaging()
	}
	idx := stageableSteps(s.actionsFor(st.snap.PipelineID), p.Steps)
	if len(idx) == 0 {
		return Proposal{}, nil, notStageable()
	}
	if stageRunningAt(p, s.now()) {
		return Proposal{}, nil, stageInProgress()
	}
	if p.stages >= maxStagesPerProposal {
		return Proposal{}, nil, stageRateLimited()
	}
	return p, idx, nil
}

// StageProposal is POST /api/proposals/{pid}/stage (X8c). It returns the
// proposal.stage_started event; the result arrives later as proposal.staged.
func (s *Service) StageProposal(ctx context.Context, user User, proposalID string) (events.Event, error) {
	s.syncProposalRoom(ctx, proposalID)
	// First pass: the rules, under the lock, so a refused caller never makes
	// Validate read the pipeline's systems.
	row, st, err := s.lockProposalRoom(ctx, user, proposalID)
	if err != nil {
		return events.Event{}, err
	}
	p, idx, err := s.stageCheckLocked(ctx, st, row, user)
	pipelineID := st.snap.PipelineID
	s.unlockRoom(st)
	if err != nil {
		return events.Event{}, err
	}

	// Validate and CheckScope again, outside the lock (Validate may read the
	// pipeline's systems), under the room's pipeline scope like the executor.
	acts := s.actionsFor(pipelineID)
	vctx := s.actionCtx(ctx, row.RoomID, pipelineID)
	for _, i := range idx {
		step := p.Steps[i]
		a, ok := acts.Get(step.Action)
		if !ok {
			return events.Event{}, validation(fmt.Sprintf("step %d: unknown action %q", i, step.Action))
		}
		if err := a.Validate(vctx, step.Params); err != nil {
			return events.Event{}, validation(fmt.Sprintf("step %d (%s): %v", i, step.Action, err))
		}
		if err := s.scopeCheck(pipelineID, a, step.Params); err != nil {
			return events.Event{}, validation(fmt.Sprintf("step %d (%s): %v", i, step.Action, err))
		}
	}

	// Second pass: the same rules where the event lands. The proposal's
	// params are frozen, so the steps checked above are the ones staged; the
	// hash is compared anyway, so a stage never runs for other params.
	row, st, err = s.lockProposalRoom(ctx, user, proposalID)
	if err != nil {
		return events.Event{}, err
	}
	defer s.unlockRoom(st)
	var steps []events.ProposalStep
	check := func() error {
		cur, curIdx, err := s.stageCheckLocked(ctx, st, row, user)
		if err != nil {
			return err
		}
		if cur.ParamsHash != p.ParamsHash || !slices.Equal(curIdx, idx) {
			return errConflict()
		}
		steps = append([]events.ProposalStep(nil), cur.Steps...)
		return nil
	}
	if err := check(); err != nil {
		return events.Event{}, err
	}
	start := s.now().UTC()
	payload := events.ProposalStageStartedPayload{
		ProposalID: proposalID, ParamsHash: p.ParamsHash, Steps: idx,
		ExpiresAt: actions.StageExpiry(start, len(idx)).UTC(),
	}
	ev, err := s.emitCheckedLocked(ctx, st, row.RoomID, check, events.ProposalStageStarted, s.humanActor(user), payload)
	if err != nil {
		return events.Event{}, err
	}
	s.touchDriverLease(ctx, st, row.RoomID)
	s.log.Info("room: stage started", "room", row.RoomID, "proposal", proposalID, "steps", len(idx), "user", user.ID)
	// The stage runs on the service lifetime, not the request: the route
	// answers at once. A closing service starts nothing; the stage then
	// expires and may be started again.
	if s.addWorker() {
		go func(roomID string) {
			defer s.wg.Done()
			s.runStage(roomID, pipelineID, proposalID, p.ParamsHash, steps, idx)
		}(row.RoomID)
	} else {
		s.log.Warn("room: stage not run: the service is closing", "room", row.RoomID, "proposal", proposalID)
	}
	return ev, nil
}

// runStage stages each step in idx, in order, and appends proposal.staged.
// It stops, writing nothing, when the service shuts down: the stage is then
// lost and expires (proposal.stage_started's expires_at).
func (s *Service) runStage(roomID, pipelineID, proposalID, paramsHash string, steps []events.ProposalStep, idx []int) {
	acts := s.actionsFor(pipelineID)
	out := make([]events.StepStage, 0, len(idx))
	for _, i := range idx {
		if s.ctx.Err() != nil {
			s.log.Info("room: stage stopped by shutdown", "room", roomID, "proposal", proposalID)
			return
		}
		out = append(out, s.stageStep(acts, roomID, pipelineID, i, steps[i]))
	}
	if s.ctx.Err() != nil {
		s.log.Info("room: stage stopped by shutdown", "room", roomID, "proposal", proposalID)
		return
	}
	payload := events.ProposalStagedPayload{ProposalID: proposalID, ParamsHash: paramsHash, Steps: out, At: s.now().UTC()}

	ctx, cancel := context.WithTimeout(s.ctx, stagedWriteTimeout)
	defer cancel()
	s.syncRoom(ctx, roomID)
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		s.log.Warn("room: stage result not written: room unavailable", "room", roomID, "proposal", proposalID, "err", err)
		return
	}
	defer s.unlockRoom(st)
	if !s.roomOpen(st) {
		s.log.Info("room: stage result not written: the room closed", "room", roomID, "proposal", proposalID)
		return
	}
	// Written even when the proposal was decided meanwhile: the log keeps
	// what staging said, and the fold drops it for a proposal that left.
	if _, err := s.emitLocked(ctx, st, roomID, events.ProposalStaged, systemActor, payload); err != nil {
		s.log.Warn("room: stage result append failed", "room", roomID, "proposal", proposalID, "err", err)
		return
	}
	s.log.Info("room: stage finished", "room", roomID, "proposal", proposalID, "healthy", payload.Healthy())
	// A healthy stage may be what a require_staging: proposal waited for
	// before the auto-approve countdown could start.
	s.maybeScheduleLocked(ctx, st, roomID, proposalID)
}

// stageStep calls one step's Stage with the room's pipeline scope and a
// StageStepTimeout deadline. A panic in an action is the step's error, so a
// buggy action cannot take the server down.
func (s *Service) stageStep(acts *actions.Registry, roomID, pipelineID string, i int, step events.ProposalStep) (out events.StepStage) {
	out = events.StepStage{StepIndex: i}
	a, ok := acts.Get(step.Action)
	var stager actions.Stager
	if ok {
		stager, ok = a.(actions.Stager)
	}
	if !ok {
		out.Error = "this action cannot be staged"
		return out
	}
	timeout := s.stageStepTimeout
	if timeout <= 0 {
		timeout = actions.StageStepTimeout
	}
	ctx, cancel := context.WithTimeout(s.actionCtx(s.ctx, roomID, pipelineID), timeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("room: stage panicked", "room", roomID, "action", step.Action, "step", i)
			out = events.StepStage{StepIndex: i, Error: "the stage failed unexpectedly"}
		}
	}()
	res, err := stager.Stage(ctx, step.Params)
	if err != nil {
		msg := err.Error()
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() != nil {
			msg = fmt.Sprintf("no verdict within %s: %s", timeout, msg)
		}
		out.Error = cleanStageError(msg)
		return out
	}
	r := cleanStageText(res)
	out.Result = &r
	return out
}

// cleanStageText is the form of a StageResult written to proposal.staged:
// staging's own words lose bidi controls, NUL and invalid UTF-8
// (sanitizeAgentText), then pass actions.CleanStageResult (redact, caps).
func cleanStageText(r actions.StageResult) actions.StageResult {
	r.Target = sanitizeAgentText(r.Target)
	r.Detail = sanitizeAgentText(r.Detail)
	r.Leftover = sanitizeAgentText(r.Leftover)
	if r.DryRun != nil {
		d := *r.DryRun
		d.Text = sanitizeAgentText(d.Text)
		d.Source = sanitizeAgentText(d.Source)
		d.Skipped = sanitizeAgentText(d.Skipped)
		r.DryRun = &d
	}
	return actions.CleanStageResult(r)
}

// cleanStageError redacts a step's error and cuts it to
// events.MaxStageErrorBytes on a rune boundary.
func cleanStageError(msg string) string {
	msg = sanitizeAgentText(redactText(msg))
	if msg == "" {
		msg = "the stage gave no verdict"
	}
	return cutBytes(msg, events.MaxStageErrorBytes)
}

// cutBytes cuts s to at most n bytes, ending in "…", without splitting a
// rune.
func cutBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	const ell = "…"
	m := n - len(ell)
	for m > 0 && !utf8.RuneStart(s[m]) {
		m--
	}
	return s[:m] + ell
}

// maxStagedInputBytes caps the text of a "proposal_staged" agent input.
const maxStagedInputBytes = 4096

// stagedInputText renders a proposal.staged for the agent: each step's
// outcome and what staging said. It is staging's own words, which the agent
// wraps as untrusted data.
func stagedInputText(p events.ProposalStagedPayload) string {
	var b strings.Builder
	verdict := "not healthy"
	if p.Healthy() {
		verdict = "healthy"
	}
	fmt.Fprintf(&b, "staging result of proposal %s: %s (staging only; production is unchanged).", p.ProposalID, verdict)
	for _, st := range p.Steps {
		fmt.Fprintf(&b, "\nstep %d: ", st.StepIndex)
		if st.Result == nil {
			b.WriteString("no verdict: " + st.Error)
			continue
		}
		r := st.Result
		b.WriteString(r.Outcome())
		if r.Target != "" {
			b.WriteString(" (" + r.Target + ")")
		}
		if r.DryRun != nil && !r.DryRun.OK && r.DryRun.Skipped == "" {
			b.WriteString("; pre-check refused: " + r.DryRun.Text)
		}
		if r.Detail != "" {
			b.WriteString("; " + r.Detail)
		}
		if r.Leftover != "" {
			b.WriteString("; left on staging: " + r.Leftover)
		}
	}
	return cutBytes(b.String(), maxStagedInputBytes)
}
