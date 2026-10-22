package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// Team pages (X4d; docs/api.md POST /api/rooms/{id}/page-team). The driver of
// a room whose hypothesis places the cause upstream (an application change or
// a vendor) pages the team that owns it: one Slack post to the team's channel,
// then room.team_paged. Never automatic, and the agent cannot write the event
// (agentEventTypes does not list it).
//
// The post is made outside the room's lock (no network call runs under it,
// roomlock.go), so the event is appended after Slack accepted the page: a
// failed post writes nothing and the driver may retry, while a page that went
// out is always recorded, even if the room changed meanwhile.

const (
	// teamPageWindow is how long a team paged in a room stays paged.
	teamPageWindow = time.Hour
	// teamPageTimeout bounds the Slack post, retries included.
	teamPageTimeout = 20 * time.Second
	// teamPageScanBatch is the page size of the log read that numbers steps.
	teamPageScanBatch = 500
	// teamPageChangeLookback is how far before the room's opening the cited
	// change is looked for; the agent's "what changed" window is shorter.
	teamPageChangeLookback = 7 * 24 * time.Hour
)

// TeamPager posts a team page. cmd/server implements it with the Slack bridge.
type TeamPager interface {
	PageTeam(ctx context.Context, p TeamPage) error
}

// TeamPage is what the room service hands the pager: typed fields read from
// the room log, the change log and owners.yaml. Hypothesis, Change and By.Name
// are untrusted text the pager must escape.
type TeamPage struct {
	RoomID     string
	PipelineID string
	Team       string
	Channel    string // owners.yaml teams[].slack_channel
	By         events.Actor
	Scope      events.HypothesisScope
	Hypothesis string
	Confidence string
	Steps      []int // 1-based numbers of the cited steps, as the room shows them
	ChangeID   int64
	Change     *clients.PipelineChange // nil when none is cited or it is gone
}

func notUpstream() *Error {
	return &Error{Status: http.StatusConflict, Code: "not_upstream",
		Msg: "the current hypothesis does not place the cause upstream (scope upstream_app or vendor); there is no team to page"}
}

func slackOff() *Error {
	return &Error{Status: http.StatusConflict, Code: "slack_off",
		Msg: "the Slack bridge is off (set SLACK_BOT_TOKEN and SLACK_CHANNEL_ID); nothing was sent"}
}

func pageRateLimited(msg string) *Error {
	return &Error{Status: http.StatusTooManyRequests, Code: "rate_limited", Msg: msg}
}

// slackUnavailable is 503 slack_unavailable: Slack did not take the page.
// code is the bridge's error class (a Slack error code such as
// not_in_channel), never message text.
func slackUnavailable(code string) *Error {
	return &Error{Status: http.StatusServiceUnavailable, Code: "slack_unavailable",
		Msg: fmt.Sprintf("Slack did not accept the page (%s); nothing was recorded, try again", code)}
}

// teamFor returns the owners.yaml team with that id among the teams a room of
// the pipeline may name (its tenant's, owners.TeamsFor).
func (s *Service) teamFor(pipelineID, id string) (owners.Team, bool) {
	if s.owners == nil {
		return owners.Team{}, false
	}
	for _, t := range s.owners.TeamsFor(pipelineID) {
		if t.ID == id {
			return t, true
		}
	}
	return owners.Team{}, false
}

// TeamPageError is a pager failure the driver may see. Code names its class
// (a Slack error code such as not_in_channel, or timeout), never message text.
type TeamPageError struct {
	Code string
	Err  error
}

func (e *TeamPageError) Error() string { return "team page: " + e.Code + ": " + e.Err.Error() }

// Unwrap returns the pager's error.
func (e *TeamPageError) Unwrap() error { return e.Err }

// pageErrCode is the code slackUnavailable shows: a TeamPageError's Code when
// it is a plain lower-case word, else "error".
func pageErrCode(err error) string {
	var pe *TeamPageError
	if errors.As(err, &pe) && pageCodeRE.MatchString(pe.Code) {
		return pe.Code
	}
	return "error"
}

var pageCodeRE = regexp.MustCompile(`^[a-z0-9_]{1,40}$`)

// PageTeam pages team for the room's current hypothesis (room.team_paged).
// The checks run in the order docs/api.md lists the errors.
func (s *Service) PageTeam(ctx context.Context, user User, roomID, team string) (events.Event, error) {
	if team == "" {
		return events.Event{}, validation("team is required")
	}
	s.syncRoom(ctx, roomID)
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	page, err := s.pageCheckLocked(st, user, team)
	if err != nil {
		s.unlockRoom(st)
		return events.Event{}, err
	}
	hypSeq, cited, openedAt := st.snap.Hypothesis.Seq, slices.Clone(st.snap.Hypothesis.BasedOnSteps), st.snap.OpenedAt
	if st.teamPaging == nil {
		st.teamPaging = map[string]bool{}
	}
	st.teamPaging[team] = true
	s.unlockRoom(st)
	defer func() {
		s.lockState(st)
		delete(st.teamPaging, team)
		st.mu.Unlock()
	}()

	// The post outlives the driver's request: a client that disconnects
	// while Slack is taking the page must not cancel a post that may already
	// be out, or it would go unrecorded and a retry would page twice.
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teamPageTimeout)
	defer cancel()
	page.Steps = s.stepNumbers(pctx, roomID, hypSeq, cited)
	page.Change = s.citedChange(pctx, page.PipelineID, page.ChangeID, openedAt)
	if err := s.teamPager.PageTeam(pctx, page); err != nil {
		code := pageErrCode(err)
		s.log.Warn("room: team page not sent", "room", roomID, "team", team, "code", code)
		return events.Event{}, slackUnavailable(code)
	}

	// The page is out: record it whatever happened to the room meanwhile, even
	// when the driver's request has gone.
	rctx, rcancel := compensateCtx(ctx)
	defer rcancel()
	st2, err := s.lockRoom(rctx, roomID)
	if err != nil {
		s.log.Warn("room: team page sent but not recorded", "room", roomID, "team", team, "err", err)
		return events.Event{}, err
	}
	defer s.unlockRoom(st2)
	e, err := s.emitLocked(rctx, st2, roomID, events.RoomTeamPaged, page.By,
		events.TeamPagedPayload{Team: team, HypothesisSeq: hypSeq, ChangeID: page.ChangeID})
	if err != nil {
		s.log.Warn("room: team page sent but not recorded", "room", roomID, "team", team, "err", err)
		return events.Event{}, err
	}
	if s.isDriver(st2, user.ID) {
		s.touchDriverLease(rctx, st2, roomID)
	}
	return e, nil
}

// pageCheckLocked applies the page-team rules and returns the page without
// its steps and change, which are read outside the lock. Caller holds st's
// lock.
func (s *Service) pageCheckLocked(st *roomState, user User, team string) (TeamPage, error) {
	if !s.roomOpen(st) {
		return TeamPage{}, roomClosed("room is closed")
	}
	if !s.isDriver(st, user.ID) {
		return TeamPage{}, ErrNotDriver()
	}
	// Another tenant's team is not in TeamsFor, so it reads as unknown; the id
	// is not echoed.
	t, ok := s.teamFor(st.snap.PipelineID, team)
	if !ok {
		return TeamPage{}, validation("team is not a team of this pipeline in owners.yaml")
	}
	h := st.snap.Hypothesis
	if h == nil || st.snap.Shadow {
		return TeamPage{}, noHypothesis()
	}
	if h.Scope != events.ScopeUpstreamApp && h.Scope != events.ScopeVendor {
		return TeamPage{}, notUpstream()
	}
	if s.teamPager == nil {
		return TeamPage{}, slackOff()
	}
	now := s.now()
	for _, p := range slices.Backward(st.snap.TeamPages) {
		if p.Team != team {
			continue
		}
		if ago := now.Sub(p.At); ago < teamPageWindow {
			next := p.At.Add(teamPageWindow).UTC().Format("15:04 UTC")
			return TeamPage{}, pageRateLimited(fmt.Sprintf("team %s was paged %d min ago in this room; it can be paged again at %s",
				team, int(max(ago, 0)/time.Minute), next))
		}
		break
	}
	if st.teamPaging[team] {
		return TeamPage{}, pageRateLimited(fmt.Sprintf("a page of team %s is being sent", team))
	}
	return TeamPage{
		RoomID: st.snap.ID, PipelineID: st.snap.PipelineID, Team: team, Channel: t.SlackChannel,
		By: s.humanActor(user), Scope: h.Scope, Hypothesis: h.Text, Confidence: h.Confidence,
		ChangeID: h.ChangeID,
	}, nil
}

// stepNumbers maps the cited step ids to the numbers the room shows: the
// position of each step's first agent.step.started in the log (the web's
// numbering). A step the log does not hold before the hypothesis is left out;
// a read error leaves the numbers out, never the page.
func (s *Service) stepNumbers(ctx context.Context, roomID string, upTo int64, cited []string) []int {
	if len(cited) == 0 {
		return nil
	}
	num := map[string]int{}
	var after int64
	for after < upTo {
		evs, err := s.store.Since(ctx, roomID, after, teamPageScanBatch)
		if err != nil {
			s.log.Warn("room: team page: step numbers not read", "room", roomID, "err", err)
			return nil
		}
		if len(evs) == 0 {
			break
		}
		for _, e := range evs {
			if e.Seq > upTo {
				break
			}
			if e.Type != events.AgentStepStart {
				continue
			}
			var p events.AgentStepStartPayload
			if json.Unmarshal(e.Payload, &p) != nil {
				continue
			}
			if _, seen := num[p.StepID]; !seen {
				num[p.StepID] = len(num) + 1
			}
		}
		after = evs[len(evs)-1].Seq
	}
	var out []int
	for _, id := range cited {
		if n, ok := num[id]; ok && !slices.Contains(out, n) {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out
}

// citedChange returns the change the hypothesis cites from the pipeline's
// change log, or nil (none cited, no change log, gone, or a read error: the
// page then names the id only).
func (s *Service) citedChange(ctx context.Context, pipelineID string, id int64, openedAt time.Time) *clients.PipelineChange {
	if id <= 0 || s.changes == nil {
		return nil
	}
	var since time.Time
	if !openedAt.IsZero() {
		since = openedAt.Add(-teamPageChangeLookback)
	}
	rows, err := s.changes.ListChanges(ctx, pipelineID, since, time.Time{}, nil, maxChangeLookup)
	if err != nil {
		s.log.Warn("room: team page: change not read", "pipeline", pipelineID, "err", err)
		return nil
	}
	for i := range rows {
		if rows[i].ID == id {
			return &rows[i]
		}
	}
	return nil
}

// maxChangeLookup bounds the change-log read of one page (store.MaxListChanges).
const maxChangeLookup = 1000
