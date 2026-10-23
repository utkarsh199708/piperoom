package room

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

// The pull request follower (Q7e; docs/design/fix-pr.md §8.1, docs/api.md
// "Q7 pull-request fix").
//
// action.executed with pull_request starts following that PR: the snapshot
// lists it in pull_requests with state open. One loop per replica checks
// every PRCheckEvery which followed PRs are due (each at most once per
// PRPollEvery) and reads them through the forge reader with a 10 s deadline.
// It appends pull_request.updated only when state, head_changed, merge_sha or
// the checks changed against what the log holds, so a PR that does not move
// writes nothing. A merge also records a pull_request pipeline change, and
// both a merge and a close reach the agent as HumanInput pr_merged and
// pr_closed (inputFor), which wake an idle agent. Following stops when the
// PR merges or closes or the room closes (only open rooms are polled).
//
// One poller per room across replicas: a room whose agent lease another
// replica holds is left to that replica (it holds the room); a room without
// a lease is polled under a cluster-wide lock on the room
// (Coordinator.LockKey), so two replicas never poll it at the same time, and
// the conditional append (emitCheckedLocked) lets exactly one event land
// when two replicas saw the same change one after the other.
//
// Errors back off per PR, doubling from PRPollEvery up to prMaxBackoff. A
// rate-limit answer (PRRateLimitError) stops every poll of this replica
// until its reset: the limit is the installation's, not the PR's.

// PullRequestReader reads one pull request of a repository a pipeline lists
// (clients.ForgeReader.PullRequest). cmd/server adapts the forge client to
// it; a rate-limit answer is returned as (or wraps) a *PRRateLimitError.
type PullRequestReader interface {
	PullRequest(ctx context.Context, repo clients.RepoRef, number int) (clients.PullRequest, error)
}

// PRRateLimitError is a forge rate-limit answer (403/429 with
// x-ratelimit-reset or Retry-After). Reset is when the limit ends; zero when
// the forge did not say.
type PRRateLimitError struct {
	Reset time.Time
	Err   error
}

func (e *PRRateLimitError) Error() string {
	if e.Err != nil {
		return "room: forge rate limited: " + e.Err.Error()
	}
	return "room: forge rate limited"
}

func (e *PRRateLimitError) Unwrap() error { return e.Err }

const (
	// PRPollEvery is how often one followed PR is read (design §14 D6).
	PRPollEvery = 60 * time.Second
	// PRCheckEvery is how often the loop looks for due PRs. A due PR waits
	// at most this long past its time.
	PRCheckEvery = 15 * time.Second
	// prMaxBackoff caps the wait after repeated errors (design §8.1).
	prMaxBackoff = 10 * time.Minute
	// prPollTimeout bounds one forge read (rule 4).
	prPollTimeout = 10 * time.Second
	// prLockWait bounds the wait for another replica's poll of an unleased
	// room: past it, that replica is polling and this one skips the room.
	prLockWait = time.Second
	// prChangeSource is the PipelineChange.Source of a merged fix PR: the
	// room saw it on the forge.
	prChangeSource = "forge"
	// maxPRFailedChecks and maxPRCheckName cap the CI check names an event
	// carries (untrusted text, events.PRChecks).
	maxPRFailedChecks = 10
	maxPRCheckName    = 100
)

// errPRNotDue is the conditional append's check: the log already holds this
// state, the PR is no longer followed, or the room closed.
var errPRNotDue = errors.New("room: pull request state already logged")

// prPoll is one followed PR's schedule on this replica.
type prPoll struct {
	next    time.Time     // zero: due now
	backoff time.Duration // 0 after a success
}

// prFollower is the follower's in-memory schedule. It holds no room state:
// the PRs and their last logged state come from the snapshots.
type prFollower struct {
	mu           sync.Mutex
	polls        map[string]*prPoll
	limitedUntil time.Time
}

func newPRFollower() *prFollower { return &prFollower{polls: map[string]*prPoll{}} }

func prKey(roomID string, pr PullRequest) string {
	return fmt.Sprintf("%s/%s/%d", roomID, pr.ProposalID, pr.StepIndex)
}

// due reports whether the PR may be polled at now.
func (f *prFollower) due(key string, now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if now.Before(f.limitedUntil) {
		return false
	}
	p := f.polls[key]
	return p == nil || !now.Before(p.next)
}

// limited reports whether a rate limit holds every poll at now.
func (f *prFollower) limited(now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return now.Before(f.limitedUntil)
}

// ok schedules the next poll one period after a successful read.
func (f *prFollower) ok(key string, now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polls[key] = &prPoll{next: now.Add(PRPollEvery)}
}

// fail doubles the PR's wait, from PRPollEvery up to prMaxBackoff, and
// returns it.
func (f *prFollower) fail(key string, now time.Time) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.polls[key]
	if p == nil {
		p = &prPoll{}
		f.polls[key] = p
	}
	switch {
	case p.backoff <= 0:
		p.backoff = PRPollEvery
	case p.backoff < prMaxBackoff:
		p.backoff = min(2*p.backoff, prMaxBackoff)
	}
	p.next = now.Add(p.backoff)
	return p.backoff
}

// rateLimited holds every poll until reset; an unknown reset counts as a
// failure of the PR instead (it backs off like any error).
func (f *prFollower) rateLimited(key string, now, reset time.Time) time.Time {
	if !reset.After(now) {
		return now.Add(f.fail(key, now))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if reset.After(f.limitedUntil) {
		f.limitedUntil = reset
	}
	if p := f.polls[key]; p != nil && p.next.Before(reset) {
		p.next = reset
	}
	return reset
}

// retry makes the PR due at the next check (its room was busy).
func (f *prFollower) retry(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.polls, key)
}

// keep drops the schedule of every PR not in live (merged, closed, or its
// room closed or gone), so the map holds only followed PRs.
func (f *prFollower) keep(live map[string]bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k := range f.polls {
		if !live[k] {
			delete(f.polls, k)
		}
	}
}

// startPRFollower starts the follower loop once, after Load, when a forge
// reader is configured. It stops when the service lifetime ends (Close).
func (s *Service) startPRFollower() {
	if s.prReader == nil {
		return
	}
	s.prOnce.Do(func() {
		if !s.addWorker() {
			return
		}
		newTicker := s.prTicker
		if newTicker == nil {
			newTicker = func(d time.Duration) (<-chan time.Time, func()) {
				t := time.NewTicker(d)
				return t.C, t.Stop
			}
		}
		tick, stop := newTicker(PRCheckEvery)
		go func() {
			defer s.wg.Done()
			defer stop()
			for {
				select {
				case <-s.ctx.Done():
					return
				case <-tick:
				}
				s.followPRs(s.ctx)
				if s.prTickDone != nil {
					s.prTickDone()
				}
			}
		}()
	})
}

// duePR is one followed PR due for a poll.
type duePR struct {
	key        string
	pipelineID string
	snap       *RoomSnapshot
	pr         PullRequest
}

// followPRs runs one check: it polls every due PR of every open room this
// replica may poll.
func (s *Service) followPRs(ctx context.Context) {
	if s.isClosed() {
		return
	}
	now := s.now()
	live := map[string]bool{}
	byRoom := map[string][]duePR{}
	for id, snap := range s.publishedRooms() {
		if snap.Status != "open" {
			continue
		}
		for _, pr := range snap.PullRequests {
			if !pr.followed() {
				continue
			}
			key := prKey(id, pr)
			live[key] = true
			if s.prf.due(key, now) {
				byRoom[id] = append(byRoom[id], duePR{key: key, pipelineID: snap.PipelineID, snap: snap, pr: pr})
			}
		}
	}
	s.prf.keep(live)
	if len(byRoom) == 0 {
		return
	}
	var held map[string]store.RoomLease
	if s.coord != nil {
		cctx, cancel := context.WithTimeout(ctx, coordTimeout)
		leases, err := s.coord.RoomLeases(cctx)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				s.log.Warn("room: pull request follower: list agent leases failed; retry at the next check", "err", err)
			}
			return
		}
		held = leases
	}
	ids := make([]string, 0, len(byRoom))
	for id := range byRoom {
		ids = append(ids, id)
	}
	sort.Strings(ids) // a stable order for logs and tests
	for _, id := range ids {
		if ctx.Err() != nil || s.prf.limited(s.now()) {
			return
		}
		l, leased := held[id]
		if leased && l.Owner != s.replica {
			continue // the replica holding the room follows its PRs
		}
		s.followRoomPRs(ctx, id, byRoom[id], s.coord != nil && !leased)
	}
}

// followRoomPRs polls the due PRs of one room. lock says the room has no
// lease holder: the poll then runs under the room's cluster-wide follower
// lock, and a replica that cannot take it at once leaves the room to the one
// that holds it.
func (s *Service) followRoomPRs(ctx context.Context, roomID string, due []duePR, lock bool) {
	if lock {
		lctx, cancel := context.WithTimeout(ctx, prLockWait)
		unlock, err := s.coord.LockKey(lctx, "pr-follow:"+roomID)
		cancel()
		if err != nil {
			return // another replica is polling this room now
		}
		defer unlock()
	}
	sort.Slice(due, func(i, j int) bool { return due[i].key < due[j].key })
	for _, d := range due {
		if ctx.Err() != nil || s.prf.limited(s.now()) {
			return
		}
		s.pollPR(ctx, roomID, d)
	}
}

// pollPR reads one PR and appends its pull_request.updated when it changed.
func (s *Service) pollPR(ctx context.Context, roomID string, d duePR) {
	now := s.now()
	repo, ok := s.stepPipeline(d.snap, d.pipelineID).Repo(d.pr.Repo)
	if !ok {
		wait := s.prf.fail(d.key, now)
		s.log.Warn("room: pull request follower: the repo is no longer listed in the pipeline's repos:", "room", roomID, "repo", d.pr.Repo, "number", d.pr.Number, "retry_in", wait)
		return
	}
	r := repo.Ref()
	pctx, cancel := context.WithTimeout(ctx, prPollTimeout)
	got, err := s.prReader.PullRequest(pctx, clients.RepoRef{ID: r.ID, Owner: r.Owner, Name: r.Name, Base: r.Base}, d.pr.Number)
	cancel()
	if err == nil {
		err = checkFollowedPR(d.pr, got)
	}
	if err != nil {
		if ctx.Err() != nil {
			return // shutting down
		}
		var rl *PRRateLimitError
		if errors.As(err, &rl) {
			until := s.prf.rateLimited(d.key, now, rl.Reset)
			s.log.Warn("room: pull request follower: rate limited by the forge; polls wait", "room", roomID, "repo", d.pr.Repo, "number", d.pr.Number, "until", until)
			return
		}
		wait := s.prf.fail(d.key, now)
		s.log.Warn("room: pull request follower: read failed; backing off", "room", roomID, "repo", d.pr.Repo, "number", d.pr.Number, "retry_in", wait, "err", err)
		return
	}
	s.prf.ok(d.key, now)
	upd := prUpdate(d.pr, got)
	if !prChanged(d.pr, upd) {
		return
	}
	if upd.State == clients.PRStateMerged {
		// Before the event: a failed append is retried at the next poll and
		// the change log keeps one row per merge (its fingerprint).
		s.recordPRMerge(ctx, d.pipelineID, d.pr, upd)
	}
	s.appendPRUpdate(ctx, roomID, d.key, upd)
}

// checkFollowedPR refuses an answer that is not the PR the step opened: a
// head branch other than the one Piperoom wrote, or a state the room does
// not know.
func checkFollowedPR(pr PullRequest, got clients.PullRequest) error {
	if got.HeadRef != "" && got.HeadRef != pr.Branch {
		return fmt.Errorf("room: pull request #%d: head branch is not %s", pr.Number, pr.Branch)
	}
	switch got.State {
	case clients.PRStateOpen, clients.PRStateMerged, clients.PRStateClosed:
		return nil
	}
	return fmt.Errorf("room: pull request #%d: unknown state %q", pr.Number, cleanPlanName(got.State))
}

// prUpdate is the pull_request.updated payload of an observed PR.
// head_changed compares the forge's head with the commit Piperoom wrote.
func prUpdate(pr PullRequest, got clients.PullRequest) events.PullRequestUpdatedPayload {
	upd := events.PullRequestUpdatedPayload{
		ProposalID:     pr.ProposalID,
		StepIndex:      pr.StepIndex,
		PullRequestRef: pr.ref(),
		State:          got.State,
		HeadChanged:    got.HeadSHA != "" && got.HeadSHA != pr.wroteHead,
		Checks:         prChecks(got.Checks),
	}
	if got.State == clients.PRStateMerged {
		upd.MergeSHA = cleanSHA(got.MergeSHA)
		if !got.MergedAt.IsZero() {
			upd.MergedAt = got.MergedAt.UTC()
		}
	}
	return upd
}

// prChecks copies the forge's checks with the names cut: at most
// maxPRFailedChecks names of at most maxPRCheckName bytes, cleaned like
// agent text (they are written by the repository's CI).
func prChecks(c clients.PullRequestChecks) events.PRChecks {
	out := events.PRChecks{State: c.State, Total: c.Total}
	switch c.State {
	case clients.ChecksNone, clients.ChecksPending, clients.ChecksSuccess, clients.ChecksFailure:
	default:
		out.State = clients.ChecksNone
	}
	for _, name := range c.Failed {
		if len(out.Failed) == maxPRFailedChecks {
			break
		}
		name = strings.TrimSpace(strings.Map(func(r rune) rune {
			if unicode.IsControl(r) {
				return ' '
			}
			return r
		}, sanitizeAgentText(name)))
		if len(name) > maxPRCheckName {
			name = strings.ToValidUTF8(name[:maxPRCheckName], "")
		}
		if name != "" {
			out.Failed = append(out.Failed, name)
		}
	}
	return out
}

// cleanSHA keeps a commit id only when it is hex (at most 64 characters).
func cleanSHA(s string) string {
	if len(s) == 0 || len(s) > 64 {
		return ""
	}
	for _, r := range s {
		if !strings.ContainsRune("0123456789abcdefABCDEF", r) {
			return ""
		}
	}
	return strings.ToLower(s)
}

// prChanged reports whether upd differs from what the log holds for the PR.
func prChanged(pr PullRequest, upd events.PullRequestUpdatedPayload) bool {
	return pr.State != upd.State || pr.HeadChanged != upd.HeadChanged || pr.mergeSHA != upd.MergeSHA ||
		pr.Checks.State != upd.Checks.State || pr.Checks.Total != upd.Checks.Total ||
		!slices.Equal(pr.Checks.Failed, upd.Checks.Failed)
}

// appendPRUpdate appends pull_request.updated as the system actor, under the
// room's lock and only if the log does not hold that state yet. A busy room
// is retried at the next check.
func (s *Service) appendPRUpdate(ctx context.Context, roomID, key string, upd events.PullRequestUpdatedPayload) {
	if s.isClosed() {
		return
	}
	st, busy := s.tryLockCachedRoom(roomID)
	if st == nil {
		if busy {
			s.prf.retry(key)
		}
		return
	}
	defer s.unlockRoom(st)
	check := func() error {
		if !s.roomOpen(st) || st.deleting {
			return errPRNotDue
		}
		cur, ok := findPR(st.snap.PullRequests, upd.ProposalID, upd.StepIndex)
		if !ok || !cur.followed() || !prChanged(cur, upd) {
			return errPRNotDue
		}
		return nil
	}
	e, err := s.emitCheckedLocked(ctx, st, roomID, check, events.PullRequestUpdated, systemActor, upd)
	switch {
	case errors.Is(err, errPRNotDue):
		return
	case errors.Is(err, store.ErrLogFull):
		s.log.Warn("room: pull request follower: room log is full; pull_request.updated not written", "room", roomID, "number", upd.Number)
		return
	case err != nil:
		s.prf.retry(key)
		s.log.Warn("room: pull request follower: append pull_request.updated failed; retry at the next check", "room", roomID, "number", upd.Number, "err", err)
		return
	}
	// A shared store's checked append does not wake the agent (emitLocked
	// does that for a single process): pr_merged and pr_closed must.
	if s.coord != nil && startsAgent(st.snap, e) {
		s.ensureAgentLocked(roomID, st)
	}
	s.log.Info("room: fix pull request updated", "room", roomID, "repo", upd.Repo, "number", upd.Number, "state", upd.State, "checks", upd.Checks.State)
}

// recordPRMerge writes the pull_request pipeline change of a merged fix PR
// (clients.ChangePullRequest: object "<repo id>#<number>", summary the PR
// title, after the merge SHA). Best effort: the store dedups on the
// fingerprint, and a failure only leaves the what-changed log without it.
func (s *Service) recordPRMerge(ctx context.Context, pipelineID string, pr PullRequest, upd events.PullRequestUpdatedPayload) {
	if s.changeW == nil || pipelineID == "" {
		return
	}
	object := fmt.Sprintf("%s#%d", pr.Repo, pr.Number)
	at := upd.MergedAt
	if at.IsZero() {
		at = s.now().UTC()
	}
	after, err := json.Marshal(map[string]string{"merge_sha": upd.MergeSHA})
	if err != nil {
		return
	}
	sum := sha256.Sum256([]byte(clients.ChangePullRequest + "|" + object + "|" + upd.MergeSHA))
	c := clients.PipelineChange{
		PipelineID:  pipelineID,
		Kind:        clients.ChangePullRequest,
		Object:      object,
		At:          at,
		Source:      prChangeSource,
		Fingerprint: hex.EncodeToString(sum[:]),
		After:       after,
		Summary:     s.prTitle(ctx, pr),
	}
	wctx, cancel := context.WithTimeout(ctx, prPollTimeout)
	defer cancel()
	if _, err := s.changeW.RecordChange(wctx, c); err != nil {
		s.log.Warn("room: pull request follower: record the merge as a pipeline change failed", "pipeline", pipelineID, "object", object, "err", err)
	}
}

// prTitle is the PR's title: the "title" param of the step that opened it
// (the approved text), or a fixed line when it cannot be read.
func (s *Service) prTitle(ctx context.Context, pr PullRequest) string {
	fallback := fmt.Sprintf("fix pull request #%d in %s merged", pr.Number, pr.Repo)
	row, err := s.store.GetProposal(ctx, pr.ProposalID)
	if err != nil {
		return fallback
	}
	var steps []events.ProposalStep
	if json.Unmarshal(row.Steps, &steps) != nil || pr.StepIndex < 0 || pr.StepIndex >= len(steps) {
		return fallback
	}
	var params struct {
		Title string `json:"title"`
	}
	if json.Unmarshal(steps[pr.StepIndex].Params, &params) != nil || strings.TrimSpace(params.Title) == "" {
		return fallback
	}
	return sanitizeAgentText(params.Title)
}

// findPR returns the PR a step opened.
func findPR(prs []PullRequest, proposalID string, step int) (PullRequest, bool) {
	for _, pr := range prs {
		if pr.ProposalID == proposalID && pr.StepIndex == step {
			return pr, true
		}
	}
	return PullRequest{}, false
}

// applyPRUpdate folds pull_request.updated into the PR of its step. It never
// changes prs. An update for a PR the snapshot does not hold changes nothing.
func applyPRUpdate(prs []PullRequest, p events.PullRequestUpdatedPayload, at time.Time) []PullRequest {
	i := slices.IndexFunc(prs, func(pr PullRequest) bool {
		return pr.ProposalID == p.ProposalID && pr.StepIndex == p.StepIndex
	})
	if i < 0 {
		return prs
	}
	out := slices.Clone(prs)
	pr := out[i]
	pr.State = p.State
	pr.HeadChanged = p.HeadChanged
	pr.Checks = events.PRChecks{State: p.Checks.State, Total: p.Checks.Total, Failed: slices.Clone(p.Checks.Failed)}
	pr.mergeSHA = p.MergeSHA
	pr.MergedAt = nil
	if p.State == clients.PRStateMerged {
		m := p.MergedAt
		if m.IsZero() {
			m = at
		}
		pr.MergedAt = &m
	}
	pr.UpdatedAt = at
	out[i] = pr
	return out
}

// prInput is the agent input of a pull_request.updated that merged or
// closed the PR (agent.HumanInput kinds pr_merged and pr_closed); nil for
// every other update.
func prInput(e events.Event) *agent.HumanInput {
	p, ok := decode[events.PullRequestUpdatedPayload](e)
	if !ok {
		return nil
	}
	step := p.StepIndex
	in := &agent.HumanInput{Seq: e.Seq, At: e.At, Actor: e.Actor, ProposalID: p.ProposalID, StepIndex: &step}
	edited := "no"
	if p.HeadChanged {
		edited = "yes"
	}
	switch p.State {
	case clients.PRStateMerged:
		at := p.MergedAt
		if at.IsZero() {
			at = e.At
		}
		in.Kind, in.At = "pr_merged", at
		in.Text = fmt.Sprintf("PR #%d in %s merged at %s (merge %s); edited after approval: %s",
			p.Number, p.Repo, at.UTC().Format("15:04"), shortSHA(p.MergeSHA), edited)
	case clients.PRStateClosed:
		in.Kind = "pr_closed"
		in.Text = fmt.Sprintf("PR #%d in %s was closed without merge at %s: the fix was not applied", p.Number, p.Repo, e.At.UTC().Format("15:04"))
	default:
		return nil
	}
	return in
}

// notePRFinal remembers, on the PR of p's step, the event that merged or
// closed it (a later open update clears it). It never changes prs.
func notePRFinal(prs []PullRequest, p events.PullRequestUpdatedPayload, e events.Event) []PullRequest {
	i := slices.IndexFunc(prs, func(pr PullRequest) bool {
		return pr.ProposalID == p.ProposalID && pr.StepIndex == p.StepIndex
	})
	if i < 0 {
		return prs
	}
	final := p.State == clients.PRStateMerged || p.State == clients.PRStateClosed
	if final && prs[i].final != nil {
		return prs // already final: the first event stays, and so does verified
	}
	out := slices.Clone(prs)
	out[i].final, out[i].verified = nil, false
	if final {
		ev := e
		out[i].final = &ev
	}
	return out
}

// notePRVerified marks the merged or closed PRs of proposalID as having an
// agent.verification after their final state. It never changes prs.
func notePRVerified(prs []PullRequest, proposalID string) []PullRequest {
	var out []PullRequest
	for i, pr := range prs {
		if pr.ProposalID != proposalID || pr.final == nil || pr.verified {
			continue
		}
		if out == nil {
			out = slices.Clone(prs)
		}
		out[i].verified = true
	}
	if out == nil {
		return prs
	}
	return out
}

// prReplayInputs are the pr_merged and pr_closed inputs of the room's fix
// PRs that merged or closed and have no agent.verification yet, rebuilt from
// the same event (prInput), so a new run reads exactly what the first one
// did: proposal id and step index in their fields, the PR number, repo,
// merge SHA and head_changed in Text. The inbox is in memory and the run
// that drained the input keeps its apply wait in memory too, so a restart or
// a lease move would otherwise lose the verdict for ever (review of #934,
// fix-PR Major 2).
func prReplayInputs(snap RoomSnapshot) []agent.HumanInput {
	var out []agent.HumanInput
	// Oldest first: PullRequests is newest first.
	for i := len(snap.PullRequests) - 1; i >= 0; i-- {
		pr := snap.PullRequests[i]
		if pr.final == nil || pr.verified {
			continue
		}
		if in := prInput(*pr.final); in != nil {
			out = append(out, *in)
		}
	}
	return out
}

// queuePRReplayLocked pushes prReplayInputs into the room's inbox, skipping
// any the inbox already holds (the pr_merged that started this run), so it
// is safe to call at every run start. Caller holds st's lock.
func (s *Service) queuePRReplayLocked(st *roomState) {
	if st.inbox == nil {
		return
	}
	for _, in := range prReplayInputs(st.snap) {
		if st.inbox.holds(in.Kind, in.ProposalID, in.StepIndex) {
			continue
		}
		s.feedInput(st, in)
	}
}

func shortSHA(s string) string {
	if len(s) > 7 {
		return s[:7]
	}
	if s == "" {
		return "unknown"
	}
	return s
}

// openPRLines are the hand-off's lines for the fix PRs still open: the
// summary must name them whatever the model wrote (design §8.1, §14 D7).
func openPRLines(snap RoomSnapshot, open []string) []string {
	for _, pr := range snap.PullRequests {
		if !pr.followed() {
			continue
		}
		if slices.ContainsFunc(open, func(l string) bool { return pr.URL != "" && strings.Contains(l, pr.URL) }) {
			continue
		}
		line := fmt.Sprintf("fix pull request #%d in %s is open and not merged: %s", pr.Number, pr.Repo, pr.URL)
		if pr.HeadChanged {
			line += " (edited after approval)"
		}
		open = append(open, line)
	}
	return open
}
