package room

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"unicode"

	"piperoom/internal/alertin"
	"piperoom/internal/events"
)

var systemActor = events.Actor{Kind: events.ActorSystem, ID: "system", Name: "Piperoom"}

var mentionRe = regexp.MustCompile(`(?i)(^|[^A-Za-z0-9_])@agent\b`)

const (
	maxTextRunes  = 4000
	maxTitleRunes = 200
)

func validation(msg string) *Error { return newErr(httpBadRequest, "validation", msg) }

func cleanText(s string, max int, field string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", validation(field + " is required")
	}
	if len([]rune(s)) > max {
		return "", validation(fmt.Sprintf("%s is longer than %d characters", field, max))
	}
	return s, nil
}

func oneLine(s string) string {
	return strings.Join(strings.FieldsFunc(s, func(r rune) bool { return unicode.IsControl(r) }), " ")
}

func (s *Service) writable(r *roomEntry) error {
	if r.st.snap.Status != StatusOpen {
		return newErr(httpConflict, "room_closed", "This room is closed.")
	}
	return nil
}

// driverGuard requires u to be the driver and renews the lease when the rules
// in docs/api.md call for it. r.mu must be held.
func (s *Service) driverGuard(ctx context.Context, r *roomEntry, u User) error {
	if err := s.writable(r); err != nil {
		return err
	}
	d := r.st.snap.Driver
	if d == nil || d.ID != u.ID {
		return newErr(httpForbidden, "not_driver", "Only the driver can do this.")
	}
	now := s.now()
	if d.LeaseUntil.Sub(now) < LeaseRenewBelow && now.Sub(r.st.lastGrant) >= LeaseRenewMinGap {
		_, err := s.appendLocked(ctx, r, events.ControlGranted, u.Actor(),
			events.ControlGrantedPayload{To: u.Actor(), LeaseUntil: now.Add(LeaseDuration)})
		return err
	}
	return nil
}

// CreateRoom opens a room; the creator becomes driver and the agent starts.
func (s *Service) CreateRoom(ctx context.Context, u User, in CreateRoomInput) (Snapshot, error) {
	title, err := cleanText(oneLine(in.Title), maxTitleRunes, "title")
	if err != nil {
		return Snapshot{}, err
	}
	if !slices.Contains([]string{"sev1", "sev2", "sev3", "sev4"}, in.Severity) {
		return Snapshot{}, validation("severity must be sev1, sev2, sev3 or sev4")
	}
	if _, ok := s.cfg.Owners.Pipeline(in.PipelineID); !ok {
		return Snapshot{}, validation("unknown pipeline_id")
	}
	return s.openRoom(ctx, u.Actor(), events.RoomCreatedPayload{Title: title, Severity: in.Severity, PipelineID: in.PipelineID}, "")
}

// IngestAlert opens a room for a verified alert, or returns the room that is
// already open for the same incident.
func (s *Service) IngestAlert(ctx context.Context, a alertin.Alert) (Snapshot, error) {
	if a.DedupKey != "" {
		s.idxMu.Lock()
		id, ok := s.dedup[a.DedupKey]
		s.idxMu.Unlock()
		if ok {
			if snap, err := s.GetRoom(ctx, id); err == nil && snap.Status == StatusOpen {
				return snap, nil
			}
		}
	}
	_, known := s.cfg.Owners.Pipeline(a.PipelineID)
	pl := a.RoomCreated()
	if !known {
		pl.PipelineID = ""
	}
	snap, err := s.openRoom(ctx, systemActor, pl, a.DedupKey)
	if err != nil {
		return Snapshot{}, err
	}
	if !known {
		// The alert named no pipeline we know, so there is nothing to scope tools
		// or proposals to: say so in the room instead of guessing.
		r, _ := s.room(snap.ID)
		r.mu.Lock()
		_, err = s.appendLocked(ctx, r, events.CommentAdded, systemActor, events.CommentAddedPayload{
			CommentID: newID("c_", s.now()),
			Text:      "This alert does not name a pipeline Piperoom knows. The agent was not started and proposals are disabled. Take control to investigate manually.",
		})
		snap = r.st.Snapshot()
		r.mu.Unlock()
		if err != nil {
			return Snapshot{}, err
		}
	}
	return snap, nil
}

func (s *Service) openRoom(ctx context.Context, actor events.Actor, pl events.RoomCreatedPayload, dedupKey string) (Snapshot, error) {
	id := newID("r_", s.now())
	if err := s.cfg.Store.CreateRoom(ctx, id); err != nil {
		return Snapshot{}, fmt.Errorf("room: create room: %w", err)
	}
	r := &roomEntry{id: id, st: NewState(id), inbox: newInbox()}
	r.mu.Lock()
	if _, err := s.appendLocked(ctx, r, events.RoomCreated, actor, pl); err != nil {
		r.mu.Unlock()
		return Snapshot{}, err
	}
	snap := r.st.Snapshot()
	r.mu.Unlock()
	s.mu.Lock()
	s.rooms[id] = r
	s.mu.Unlock()
	if dedupKey != "" {
		s.idxMu.Lock()
		s.dedup[dedupKey] = id
		s.idxMu.Unlock()
	}
	if _, ok := s.cfg.Owners.Pipeline(pl.PipelineID); ok {
		s.startAgent(r, pl.Title, pl.PipelineID, pl.Alert)
	}
	return snap, nil
}

// Comment adds a human comment, optionally anchored to a step.
func (s *Service) Comment(ctx context.Context, u User, roomID string, in CommentInput) (events.Event, error) {
	text, err := cleanText(in.Text, maxTextRunes, "text")
	if err != nil {
		return events.Event{}, err
	}
	r, err := s.room(roomID)
	if err != nil {
		return events.Event{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := s.writable(r); err != nil {
		return events.Event{}, err
	}
	return s.appendLocked(ctx, r, events.CommentAdded, u.Actor(), events.CommentAddedPayload{
		CommentID: newID("c_", s.now()), StepID: in.StepID, Text: text, MentionsAgent: mentionRe.MatchString(text),
	})
}

// Steer sends an instruction (driver) or a suggestion (everyone else).
func (s *Service) Steer(ctx context.Context, u User, roomID, text string) (events.Event, error) {
	text, err := cleanText(text, maxTextRunes, "text")
	if err != nil {
		return events.Event{}, err
	}
	r, err := s.room(roomID)
	if err != nil {
		return events.Event{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := s.writable(r); err != nil {
		return events.Event{}, err
	}
	if d := r.st.snap.Driver; d != nil && d.ID == u.ID {
		if err := s.driverGuard(ctx, r, u); err != nil {
			return events.Event{}, err
		}
		return s.appendLocked(ctx, r, events.SteerSent, u.Actor(), events.SteerSentPayload{Text: text})
	}
	return s.appendLocked(ctx, r, events.SteerSuggested, u.Actor(),
		events.SteerSuggestedPayload{SuggestionID: newID("g_", s.now()), Text: text})
}

// SendSuggestion forwards a pending suggestion to the agent (driver only).
func (s *Service) SendSuggestion(ctx context.Context, u User, roomID, suggestionID string) (events.Event, error) {
	r, err := s.room(roomID)
	if err != nil {
		return events.Event{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := s.driverGuard(ctx, r, u); err != nil {
		return events.Event{}, err
	}
	g := r.st.suggestions[suggestionID]
	if g == nil {
		return events.Event{}, newErr(http.StatusNotFound, "not_found", "suggestion not found")
	}
	if g.sent {
		return events.Event{}, newErr(httpConflict, "already_decided", "This suggestion was already sent.")
	}
	return s.appendLocked(ctx, r, events.SteerSent, u.Actor(), events.SteerSentPayload{SuggestionID: suggestionID, Text: g.Text})
}

// RequestControl asks the driver to hand over control.
func (s *Service) RequestControl(ctx context.Context, u User, roomID string) (events.Event, error) {
	r, err := s.room(roomID)
	if err != nil {
		return events.Event{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := s.writable(r); err != nil {
		return events.Event{}, err
	}
	if d := r.st.snap.Driver; d != nil && d.ID == u.ID {
		return events.Event{}, validation("you are already the driver")
	}
	return s.appendLocked(ctx, r, events.ControlRequested, u.Actor(), events.ControlRequestedPayload{})
}

// GrantControl moves the driver role: by the driver, or by anyone when there
// is no driver or the driver's lease has expired.
func (s *Service) GrantControl(ctx context.Context, u User, roomID, to string) (events.Event, error) {
	target, ok := s.cfg.Users.Get(to)
	if !ok {
		return events.Event{}, validation("unknown user in 'to'")
	}
	r, err := s.room(roomID)
	if err != nil {
		return events.Event{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := s.writable(r); err != nil {
		return events.Event{}, err
	}
	d := r.st.snap.Driver
	now := s.now()
	switch {
	case d == nil, d.ID == u.ID:
	case now.After(d.LeaseUntil):
	default:
		return events.Event{}, newErr(httpConflict, "lease_held", "The driver's lease is still held.")
	}
	return s.appendLocked(ctx, r, events.ControlGranted, u.Actor(),
		events.ControlGrantedPayload{To: target.Actor(), LeaseUntil: now.Add(LeaseDuration)})
}

// Close ends the room (driver only) and stops its agent.
func (s *Service) Close(ctx context.Context, u User, roomID, resolution string) (events.Event, error) {
	resolution = strings.TrimSpace(resolution)
	if len([]rune(resolution)) > maxTextRunes {
		return events.Event{}, validation("resolution is too long")
	}
	r, err := s.room(roomID)
	if err != nil {
		return events.Event{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := s.driverGuard(ctx, r, u); err != nil {
		return events.Event{}, err
	}
	ev, err := s.appendLocked(ctx, r, events.RoomClosed, u.Actor(), events.RoomClosedPayload{Resolution: resolution})
	if err != nil {
		return events.Event{}, err
	}
	if r.cancelAgent != nil {
		r.cancelAgent()
	}
	s.idxMu.Lock()
	for k, id := range s.dedup {
		if id == roomID {
			delete(s.dedup, k)
		}
	}
	s.idxMu.Unlock()
	return ev, nil
}

// Trace runs a record trace and pins the result to the room.
func (s *Service) Trace(ctx context.Context, u User, roomID string, in TraceInput) (events.Event, error) {
	r, err := s.room(roomID)
	if err != nil {
		return events.Event{}, err
	}
	r.mu.Lock()
	snap := r.st.Snapshot()
	err = s.writable(r)
	r.mu.Unlock()
	if err != nil {
		return events.Event{}, err
	}
	pipe, ok := s.cfg.Owners.Pipeline(snap.PipelineID)
	if !ok || s.cfg.Topology == nil {
		return events.Event{}, validation("this room has no pipeline to trace")
	}
	key := strings.TrimSpace(in.Key)
	if key == "" {
		return events.Event{}, validation("key is required")
	}
	if !slices.Contains(pipe.SourceTables, in.Table) {
		return events.Event{}, validation("table is not a source table of this pipeline")
	}
	tctx, cancel := context.WithTimeout(ctx, traceTimeout)
	defer cancel()
	stages, err := s.cfg.Topology.Trace(tctx, snap.PipelineID, in.Table, key)
	if err != nil {
		return events.Event{}, fmt.Errorf("room: trace: %w", err)
	}
	if stages == nil {
		stages = []events.TraceStage{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := s.writable(r); err != nil {
		return events.Event{}, err
	}
	return s.appendLocked(ctx, r, events.TracePinned, u.Actor(), events.TracePinnedPayload{Table: in.Table, Key: key, Stages: stages})
}
