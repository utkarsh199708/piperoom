package agent

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// External causes (X4c). Many incidents start outside the pipeline: an
// application deploy, a schema migration, a vendor's incident. what_changed
// lists them with the pipeline's own changes, newest first. Beside that list
// the runner adds its own ranking, outside the tool wrapper: the changes made
// outside the pipeline first, each by its distance to the alert, with the
// owners.yaml team that owns the changed service. The model then sets
// update_hypothesis's scope (and team and change_id), which this file checks
// against what the run really read; and once the scope is upstream_app or
// vendor every proposal is labelled a mitigation, because the root fix is the
// owning team's or the vendor's, not a pipeline change.

// whatChangedTool is the read whose Data lists clients.PipelineChange rows
// (internal/tools/changes.ToolName; named here so the agent does not import
// the tool package).
const whatChangedTool = "what_changed"

const (
	// maxRankedChanges caps the lines of the ranked note: it repeats rows the
	// tool output already holds, so it carries the nearest few only.
	maxRankedChanges = 8
	// maxSeenChanges caps the external change ids a run remembers for
	// update_hypothesis's change_id check. A what_changed answer lists at most
	// 40 rows; a room that read past this many distinct ones cites a re-read.
	maxSeenChanges = 512
	// maxStepsPerChange caps the what_changed steps remembered per change.
	maxStepsPerChange = 4
	// maxNoteTeams caps the teams the note lists.
	maxNoteTeams = 20
	// noteTextRunes caps the untrusted text of one ranked line.
	noteTextRunes = 240
)

// mitigationPrefix labels a proposal made while the hypothesis puts the cause
// outside the pipeline.
const mitigationPrefix = "Mitigation: "

// seenChange is what the run remembers of one external change a what_changed
// read returned.
type seenChange struct {
	kind  string
	team  string   // the owning team of its service, "" when none owns it
	steps []string // the what_changed steps that listed it, oldest first
}

// externalCauses is the run's view of external changes and of the scope of
// its latest hypothesis. Safe for concurrent use: reads run in parallel.
type externalCauses struct {
	mu       sync.Mutex
	pipeline string
	// alertAt is the alert's time (Alert.At, when the room opened), else
	// the run's start, which stands for it where no room gives one
	// (cmd/investigate, evals).
	alertAt time.Time
	teams   []Team
	seen    map[int64]*seenChange
	// scope and team are the latest recorded hypothesis's.
	scope events.HypothesisScope
	team  string
}

// newExternalCauses ranks against a.At, or against start when the alert
// carries no time.
func newExternalCauses(a Alert, start time.Time) *externalCauses {
	alertAt := a.At
	if alertAt.IsZero() {
		alertAt = start
	}
	return &externalCauses{pipeline: a.PipelineID, alertAt: alertAt, teams: a.Teams, seen: map[int64]*seenChange{}}
}

// teamOwning returns the id of the team whose Owns lists service, or "".
// owners.yaml gives a service one team at most.
func teamOwning(teams []Team, service string) string {
	if service == "" {
		return ""
	}
	for _, t := range teams {
		if slices.Contains(t.Owns, service) {
			return t.ID
		}
	}
	return ""
}

// scopeOfKind is the scope an external change kind puts the cause in.
func scopeOfKind(kind string) events.HypothesisScope {
	if kind == clients.ChangeVendorIncident {
		return events.ScopeVendor
	}
	return events.ScopeUpstreamApp
}

// externalOf decodes an external change's After; zero when absent or broken.
func externalOf(c clients.PipelineChange) clients.ExternalChange {
	var e clients.ExternalChange
	if len(c.After) > 0 && json.Unmarshal(c.After, &e) != nil {
		return clients.ExternalChange{}
	}
	return e
}

// changesIn reads the change rows out of a what_changed result's Data. It
// round-trips through JSON, so the tool's struct and a fake's map read the
// same; rows of another pipeline or of an unknown kind are dropped.
func changesIn(data any, pipeline string) []clients.PipelineChange {
	if data == nil {
		return nil
	}
	b, err := json.Marshal(data)
	if err != nil {
		return nil
	}
	var d struct {
		Changes []clients.PipelineChange `json:"changes"`
	}
	if json.Unmarshal(b, &d) != nil {
		return nil
	}
	kinds := clients.ChangeKinds()
	out := make([]clients.PipelineChange, 0, len(d.Changes))
	for _, c := range d.Changes {
		if (c.PipelineID != "" && c.PipelineID != pipeline) || !slices.Contains(kinds, c.Kind) {
			continue
		}
		out = append(out, c)
	}
	return out
}

// rankChanges orders changes for the alert: external kinds first, then by
// distance to alertAt (before or after), then newest first, then by id. The
// input is not changed.
func rankChanges(cs []clients.PipelineChange, alertAt time.Time) []clients.PipelineChange {
	out := slices.Clone(cs)
	dist := func(c clients.PipelineChange) time.Duration {
		d := c.At.Sub(alertAt)
		if d < 0 {
			return -d
		}
		return d
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if ea, eb := clients.IsExternalChange(a.Kind), clients.IsExternalChange(b.Kind); ea != eb {
			return ea
		}
		if da, db := dist(a), dist(b); da != db {
			return da < db
		}
		if !a.At.Equal(b.At) {
			return a.At.After(b.At)
		}
		return a.ID > b.ID
	})
	return out
}

// observe records the external changes of one what_changed read and returns
// the ranked note to put after its tool output, or "" when the read lists no
// external change (the tool output then reaches the model as before X4).
func (c *externalCauses) observe(stepID string, data any) string {
	if c == nil {
		return ""
	}
	rows := changesIn(data, c.pipeline)
	if !slices.ContainsFunc(rows, func(r clients.PipelineChange) bool { return clients.IsExternalChange(r.Kind) }) {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range rows {
		if !clients.IsExternalChange(r.Kind) || r.ID <= 0 {
			continue
		}
		s, ok := c.seen[r.ID]
		if !ok {
			if len(c.seen) >= maxSeenChanges {
				continue
			}
			s = &seenChange{}
			c.seen[r.ID] = s
		}
		s.kind, s.team = r.Kind, teamOwning(c.teams, externalOf(r).Service)
		if !slices.Contains(s.steps, stepID) {
			s.steps = append(s.steps, stepID)
			if len(s.steps) > maxStepsPerChange {
				s.steps = s.steps[1:]
			}
		}
	}
	return c.note(rankChanges(rows, c.alertAt))
}

// rankedNoteHead is the fixed agent text of the ranked note. It carries no
// tool data, so nothing a change's sender wrote can forge it.
const rankedNoteHead = "[agent rule] The changes above ranked for this alert: changes made outside the pipeline (deploy, migration, vendor_incident) first, each nearest the alert first. " +
	"A change near the alert is a suspect: verify it against the symptom before you blame it. " +
	"If your reads tie one to the symptom, call update_hypothesis with scope upstream_app (a deploy or migration) or vendor (a vendor incident), its change_id and its team when one is shown; " +
	"then propose only a mitigation, labelled as such: the root fix is the owning team's or the vendor's. If none ties, set scope pipeline."

// note renders the ranked list. The id, kind, distance and team are the
// runner's own (ids are numbers, kinds come from the closed list, teams from
// owners.yaml); everything the sender or the customer database wrote stays
// inside one untrusted-data wrapper per line. The caller holds mu.
func (c *externalCauses) note(ranked []clients.PipelineChange) string {
	var b strings.Builder
	b.WriteString(rankedNoteHead)
	for i, r := range ranked[:min(len(ranked), maxRankedChanges)] {
		fmt.Fprintf(&b, "\n%d. ", i+1)
		if r.ID > 0 {
			fmt.Fprintf(&b, "change_id %d, ", r.ID)
		} else {
			b.WriteString("no change_id (read live; cite the step), ")
		}
		b.WriteString(r.Kind + ", " + alertDistance(r.At, c.alertAt))
		ext := externalOf(r)
		if clients.IsExternalChange(r.Kind) {
			if team := teamOwning(c.teams, ext.Service); team != "" {
				b.WriteString(", team " + team)
			}
		}
		b.WriteString(": " + wrapUntrusted(changeText(r, ext)))
	}
	if n := len(ranked) - maxRankedChanges; n > 0 {
		fmt.Fprintf(&b, "\n(%d more in the list above)", n)
	}
	if len(c.teams) > 0 {
		b.WriteString("\nTeams you may name (owners.yaml): ")
		for i, t := range c.teams[:min(len(c.teams), maxNoteTeams)] {
			if i > 0 {
				b.WriteString("; ")
			}
			b.WriteString(sanitizeLabel(t.ID) + " owns " + escapeToolText(strings.Join(t.Owns, ", ")))
		}
	} else {
		b.WriteString("\nowners.yaml names no team for this pipeline: leave team empty.")
	}
	return b.String()
}

// changeText is the untrusted part of a ranked line: the object, the service
// when it differs, the actor, the migration version and the summary, masked,
// on one line and cut.
func changeText(r clients.PipelineChange, ext clients.ExternalChange) string {
	var b strings.Builder
	b.WriteString(r.Object)
	if ext.Service != "" && ext.Service != r.Object {
		b.WriteString(", service " + ext.Service)
	}
	if r.Actor != "" {
		b.WriteString(", by " + r.Actor)
	}
	if ext.Status != "" {
		b.WriteString(", " + ext.Status)
	}
	b.WriteString(": ")
	if ext.Version != "" {
		b.WriteString("[" + ext.Version + "] ")
	}
	b.WriteString(r.Summary)
	return oneLine(maskAlertText(b.String()), noteTextRunes)
}

// oneLine folds whitespace and control characters to single spaces and cuts
// to n runes, so untrusted text cannot start a line of its own.
func oneLine(s string, n int) string {
	s = strings.Join(strings.FieldsFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }), " ")
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}

// alertDistance says how far at is from the alert, in whole minutes.
func alertDistance(at, alertAt time.Time) string {
	d := at.Sub(alertAt)
	side := "before"
	if d > 0 {
		side = "after"
	} else {
		d = -d
	}
	d = d.Round(time.Minute)
	if d < time.Minute {
		return "within a minute of the alert"
	}
	var s string
	switch {
	case d < time.Hour:
		s = fmt.Sprintf("%dm", int(d/time.Minute))
	case d < 24*time.Hour:
		s = fmt.Sprintf("%dh%02dm", int(d/time.Hour), int(d%time.Hour/time.Minute))
	default:
		s = fmt.Sprintf("%dd%02dh", int(d/(24*time.Hour)), int(d%(24*time.Hour)/time.Hour))
	}
	return s + " " + side + " the alert"
}

// resolve checks a hypothesis's scope, team and change_id against what this
// run read and returns the values to emit. A nil c knows no change and no
// team. The errors are tools.ErrBadInput, so the model reads them and
// retries:
//   - change_id must be an external change a what_changed read of this run
//     listed, and that read's step must be cited; the change's kind fixes the
//     scope (deploy, migration: upstream_app; vendor_incident: vendor), so a
//     scope left out is taken from it and a contradicting one is refused;
//   - without change_id an omitted scope is unknown: nothing established;
//   - team is allowed only with upstream_app or vendor, must be a team of
//     Alert.Teams, and must be the team that owns the cited change's service
//     when one does; left out, it is that team.
func (c *externalCauses) resolve(in hypothesisInput) (events.HypothesisScope, string, int64, error) {
	if c == nil {
		c = &externalCauses{}
	}
	if in.ChangeID < 0 {
		return "", "", 0, fmt.Errorf("%w: change_id must be a change id from what_changed", tools.ErrBadInput)
	}
	scope := events.HypothesisScope(in.Scope).Normalize()
	team := strings.TrimSpace(in.Team)
	c.mu.Lock()
	defer c.mu.Unlock()
	if in.ChangeID > 0 {
		s, ok := c.seen[in.ChangeID]
		if !ok {
			return "", "", 0, fmt.Errorf("%w: change_id %d is not a deploy, migration or vendor incident a what_changed read of this run listed; read what_changed and cite a change_id it lists, or leave change_id out", tools.ErrBadInput, in.ChangeID)
		}
		want := scopeOfKind(s.kind)
		if in.Scope == "" {
			scope = want
		} else if scope != want {
			return "", "", 0, fmt.Errorf("%w: change_id %d is a %s, which puts the cause in scope %s, not %s; fix scope or leave change_id out", tools.ErrBadInput, in.ChangeID, s.kind, want, scope)
		}
		if !slices.ContainsFunc(s.steps, func(id string) bool { return slices.Contains(in.BasedOnSteps, id) }) {
			return "", "", 0, fmt.Errorf("%w: change_id %d: cite the what_changed step that lists it (%s) in based_on_steps", tools.ErrBadInput, in.ChangeID, s.steps[len(s.steps)-1])
		}
		switch {
		case team == "":
			team = s.team
		case s.team != "" && team != s.team:
			return "", "", 0, fmt.Errorf("%w: change_id %d's service is owned by team %s, not %s", tools.ErrBadInput, in.ChangeID, s.team, sanitizeLabel(clipID(team)))
		}
	}
	if team != "" {
		if scope != events.ScopeUpstreamApp && scope != events.ScopeVendor {
			return "", "", 0, fmt.Errorf("%w: team is set only with scope upstream_app or vendor", tools.ErrBadInput)
		}
		if !slices.ContainsFunc(c.teams, func(t Team) bool { return t.ID == team }) {
			if len(c.teams) == 0 {
				return "", "", 0, fmt.Errorf("%w: owners.yaml names no team for this pipeline; leave team empty", tools.ErrBadInput)
			}
			ids := make([]string, 0, len(c.teams))
			for _, t := range c.teams {
				ids = append(ids, t.ID)
			}
			return "", "", 0, fmt.Errorf("%w: team %s is not a team of this pipeline (teams: %s)", tools.ErrBadInput, sanitizeLabel(clipID(team)), strings.Join(ids, ", "))
		}
	}
	return scope, team, in.ChangeID, nil
}

// record keeps the scope and team of the hypothesis just emitted.
func (c *externalCauses) record(scope events.HypothesisScope, team string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.scope, c.team = scope, team
}

// current returns the latest hypothesis's scope and team; unknown and "" when
// no hypothesis set one.
func (c *externalCauses) current() (events.HypothesisScope, string) {
	if c == nil {
		return events.ScopeUnknown, ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scope.Normalize(), c.team
}

// rootFixOwner names who holds the root fix of a cause outside the pipeline
// (an application change: its team; a vendor incident: the vendor), "" for
// any other scope.
func rootFixOwner(scope events.HypothesisScope, team string) string {
	switch scope {
	case events.ScopeUpstreamApp:
		if team == "" {
			return "the owning team"
		}
		return "team " + team
	case events.ScopeVendor:
		return "the vendor"
	}
	return ""
}

// mitigationTitle labels a proposal title as a mitigation while the latest
// hypothesis puts the cause outside the pipeline (upstream_app, vendor): a
// pipeline change can then only soften the impact, never be the root fix.
// owner is who holds the root fix, "" when no label applies; a title that
// already says "mitigation" is kept.
func (c *externalCauses) mitigationTitle(title string) (out, owner string) {
	owner = rootFixOwner(c.current())
	if owner == "" {
		return title, ""
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(title)), "mitigation") {
		return title, owner
	}
	return mitigationPrefix + title, owner
}
