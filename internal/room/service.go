// Package room implements the incident room: it folds an append-only event
// log into a RoomSnapshot, enforces the rules in docs/api.md (driver lease,
// layer ownership, one decision per layer), supervises the investigation agent
// and runs approved actions.
package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// DriverLease is how long control lasts; renewed by any driver action.
const DriverLease = 30 * time.Minute

// Deps wires the room service to its collaborators. Any field may be nil to
// get a safe default (used by tests and --demo).
type Deps struct {
	Store        store.Store
	Actions      *actions.Registry
	Tools        *tools.Registry
	Topology     topology.Builder
	Users        *Users
	Run          agent.Runner
	WriteHandoff agent.HandoffWriter
	Options      agent.Options
	Logger       *slog.Logger
	Now          func() time.Time
}

type roomState struct {
	snap        RoomSnapshot
	inbox       *inbox
	agentCancel context.CancelFunc
	running     map[string]bool // proposalID -> executor running guard
}

// Service is the room domain logic. All state derives from the event log; the
// in-memory cache is rebuilt from the log on start.
type Service struct {
	store        store.Store
	actions      *actions.Registry
	tools        *tools.Registry
	topo         topology.Builder
	users        *Users
	run          agent.Runner
	writeHandoff agent.HandoffWriter
	opts         agent.Options
	log          *slog.Logger
	now          func() time.Time

	mu    sync.Mutex
	rooms map[string]*roomState
}

// New builds a Service. Missing deps fall back to safe defaults.
func New(d Deps) *Service {
	now := d.Now
	if now == nil {
		now = time.Now
	}
	log := d.Logger
	if log == nil {
		log = slog.Default()
	}
	run := d.Run
	if run == nil {
		run = agent.Run
	}
	wh := d.WriteHandoff
	if wh == nil {
		wh = agent.WriteHandoff
	}
	users := d.Users
	if users == nil {
		users = DefaultUsers()
	}
	acts := d.Actions
	if acts == nil {
		acts = actions.NewRegistry()
	}
	tls := d.Tools
	if tls == nil {
		tls = tools.NewRegistry()
	}
	topo := d.Topology
	if topo == nil {
		topo = noopTopology{}
	}
	return &Service{
		store:        d.Store,
		actions:      acts,
		tools:        tls,
		topo:         topo,
		users:        users,
		run:          run,
		writeHandoff: wh,
		opts:         d.Options,
		log:          log,
		now:          now,
		rooms:        map[string]*roomState{},
	}
}

// LookupUser returns a user by id (for auth and the /api/me endpoint).
func (s *Service) LookupUser(id string) (User, bool) { return s.users.Get(id) }

// AllUsers returns the full directory.
func (s *Service) AllUsers() []User { return s.users.All() }

// Load rebuilds the snapshot cache from the event log and restarts the agent
// for rooms that are still open. Called once at startup.
func (s *Service) Load(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ensureAllLoadedLocked(ctx)
}

func (s *Service) ensureAllLoadedLocked(ctx context.Context) error {
	rooms, err := s.store.ListRooms(ctx)
	if err != nil {
		return fmt.Errorf("room: load: %w", err)
	}
	for _, r := range rooms {
		if _, ok := s.rooms[r.ID]; ok {
			continue
		}
		snap, err := s.rebuild(ctx, r.ID)
		if err != nil {
			s.log.Warn("room: rebuild failed", "room", r.ID, "err", err)
			continue
		}
		st := &roomState{snap: snap, inbox: newInbox(), running: map[string]bool{}}
		s.rooms[r.ID] = st
		if snap.Status == "open" {
			s.startAgentLocked(r.ID, snap.Title, snap.PipelineID, s.rawAlert(ctx, r.ID), st)
		}
	}
	return nil
}

func (s *Service) rawAlert(ctx context.Context, roomID string) json.RawMessage {
	first, err := s.store.Since(ctx, roomID, 0, 1)
	if err != nil || len(first) == 0 || first[0].Type != events.RoomCreated {
		return nil
	}
	p, ok := decode[events.RoomCreatedPayload](first[0])
	if !ok {
		return nil
	}
	return p.Alert
}

// getStateLocked returns the cached room state, rebuilding it from the log if
// the room is not cached. Caller must hold s.mu.
func (s *Service) getStateLocked(ctx context.Context, roomID string) (*roomState, error) {
	if st, ok := s.rooms[roomID]; ok {
		return st, nil
	}
	if _, err := s.store.GetRoom(ctx, roomID); err != nil {
		return nil, mapStoreErr(err)
	}
	snap, err := s.rebuild(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("room: rebuild %s: %w", roomID, err)
	}
	st := &roomState{snap: snap, inbox: newInbox(), running: map[string]bool{}}
	s.rooms[roomID] = st
	return st, nil
}

// rebuild folds the whole log into a snapshot, then refreshes the driver lease
// from the rooms row (renewals are persisted there, not in the log).
func (s *Service) rebuild(ctx context.Context, roomID string) (RoomSnapshot, error) {
	snap := NewSnapshot()
	after := int64(0)
	for {
		evs, err := s.store.Since(ctx, roomID, after, 1000)
		if err != nil {
			return snap, err
		}
		if len(evs) == 0 {
			break
		}
		for _, e := range evs {
			snap = Apply(snap, e)
			after = e.Seq
		}
		if len(evs) < 1000 {
			break
		}
	}
	if snap.ID == "" {
		snap.ID = roomID
	}
	if row, err := s.store.GetRoom(ctx, roomID); err == nil {
		if row.DriverID != "" && snap.Driver != nil && snap.Driver.ID == row.DriverID && row.DriverLeaseUntil != nil && row.DriverLeaseUntil.After(snap.Driver.LeaseUntil) {
			snap.Driver.LeaseUntil = *row.DriverLeaseUntil
		}
	}
	return snap, nil
}

// emitLocked appends one event, folds it into the cached snapshot and feeds
// the agent inbox. Caller must hold s.mu.
func (s *Service) emitLocked(ctx context.Context, st *roomState, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	e, err := s.store.Append(ctx, roomID, typ, actor, payload)
	if err != nil {
		return events.Event{}, fmt.Errorf("room: append %s: %w", typ, err)
	}
	st.snap = Apply(st.snap, e)
	s.feedInbox(st, e)
	return e, nil
}

// agentEmit is the agent Sink: the agent's events enter the same log and cache.
func (s *Service) agentEmit(ctx context.Context, roomID string, typ events.Type, payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.rooms[roomID]
	if !ok {
		return fmt.Errorf("room: agent emit on unknown room %q", roomID)
	}
	_, err := s.emitLocked(ctx, st, roomID, typ, events.AgentActor, payload)
	return err
}

// systemEmit appends an event on behalf of the server (executor, etc.).
func (s *Service) systemEmit(ctx context.Context, roomID string, typ events.Type, payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.rooms[roomID]
	if !ok {
		return fmt.Errorf("room: system emit on unknown room %q", roomID)
	}
	_, err := s.emitLocked(ctx, st, roomID, typ, systemActor, payload)
	return err
}

func (s *Service) isDriver(st *roomState, userID string) bool {
	d := st.snap.Driver
	return d != nil && d.ID == userID && s.now().Before(d.LeaseUntil)
}

func (s *Service) leaseExpired(st *roomState) bool {
	d := st.snap.Driver
	return d == nil || !s.now().Before(d.LeaseUntil)
}

// touchDriverLease renews the driver lease in the snapshot and persists it.
// Caller must hold s.mu.
func (s *Service) touchDriverLease(ctx context.Context, st *roomState, roomID string) {
	if st.snap.Driver == nil {
		return
	}
	lease := s.now().UTC().Add(DriverLease)
	st.snap.Driver.LeaseUntil = lease
	_ = s.store.UpdateDriver(ctx, roomID, st.snap.Driver.ID, st.snap.Driver.Name, lease)
}

func (s *Service) roomOpen(st *roomState) bool {
	return st.snap.Status == "open"
}

func humanActor(u User) events.Actor {
	return events.Actor{Kind: events.ActorHuman, ID: u.ID, Name: u.Name}
}

var systemActor = events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}

var executorActor = events.Actor{Kind: events.ActorSystem, ID: "executor", Name: "Executor"}

func mapStoreErr(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return notFound("not found")
	}
	return err
}

// ---- inbox ----

// inbox delivers human input to the agent between model turns. It implements
// agent.Inbox.
type inbox struct {
	mu    sync.Mutex
	items []agent.HumanInput
	wake  chan struct{}
}

func newInbox() *inbox {
	return &inbox{wake: make(chan struct{}, 1)}
}

func (in *inbox) push(inp agent.HumanInput) {
	in.mu.Lock()
	in.items = append(in.items, inp)
	in.mu.Unlock()
	select {
	case in.wake <- struct{}{}:
	default:
	}
}

// Drain returns all pending inputs without blocking.
func (in *inbox) Drain(ctx context.Context) ([]agent.HumanInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	out := in.items
	in.items = nil
	return out, nil
}

// Wait blocks until at least one input is pending or ctx ends.
func (in *inbox) Wait(ctx context.Context) error {
	select {
	case <-in.wake:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// feedInbox converts relevant events into agent.HumanInput. Caller holds s.mu.
func (s *Service) feedInbox(st *roomState, e events.Event) {
	if st.inbox == nil {
		return
	}
	var input *agent.HumanInput
	switch e.Type {
	case events.SteerSent:
		p, ok := decode[events.SteerSentPayload](e)
		if !ok {
			return
		}
		input = &agent.HumanInput{Seq: e.Seq, Actor: e.Actor, Kind: "steer", Text: p.Text}
	case events.CommentAdded:
		p, ok := decode[events.CommentAddedPayload](e)
		if !ok || !p.MentionsAgent {
			return
		}
		input = &agent.HumanInput{Seq: e.Seq, Actor: e.Actor, Kind: "comment", StepID: p.StepID, CommentID: p.CommentID, Text: p.Text}
	case events.ApprovalGiven:
		p, ok := decode[events.ApprovalGivenPayload](e)
		if !ok {
			return
		}
		input = &agent.HumanInput{Seq: e.Seq, Actor: e.Actor, Kind: "approval_result", Text: fmt.Sprintf("layer %q approved proposal %s", p.Layer, p.ProposalID)}
	case events.ApprovalRejected:
		p, ok := decode[events.ApprovalRejectedPayload](e)
		if !ok {
			return
		}
		input = &agent.HumanInput{Seq: e.Seq, Actor: e.Actor, Kind: "approval_result", Text: fmt.Sprintf("layer %q rejected proposal %s: %s", p.Layer, p.ProposalID, p.Reason)}
	case events.ActionExecuted:
		p, ok := decode[events.ActionExecutedPayload](e)
		if !ok {
			return
		}
		input = &agent.HumanInput{Seq: e.Seq, Actor: e.Actor, Kind: "action_result", Text: fmt.Sprintf("proposal %s step %d: %s", p.ProposalID, p.StepIndex, p.Output)}
	default:
		return
	}
	st.inbox.push(*input)
}

// ---- agent supervisor ----

// roomSink implements agent.Sink, appending agent events to the room log.
type roomSink struct {
	s      *Service
	roomID string
}

func (r roomSink) Emit(ctx context.Context, typ events.Type, payload any) error {
	return r.s.agentEmit(ctx, r.roomID, typ, payload)
}

// proposalCreator implements agent.Proposals.
type proposalCreator struct {
	s      *Service
	roomID string
}

func (p proposalCreator) Create(ctx context.Context, payload events.ProposalCreatedPayload) (string, error) {
	return p.s.CreateProposal(ctx, p.roomID, payload, events.AgentActor)
}

// startAgentLocked starts agent.Run for an open room. Caller holds s.mu.
func (s *Service) startAgentLocked(roomID, title, pipelineID string, raw json.RawMessage, st *roomState) {
	ctx, cancel := context.WithCancel(context.Background())
	st.agentCancel = cancel
	go func() {
		alert := agent.Alert{RoomID: roomID, Title: title, PipelineID: pipelineID, Raw: raw}
		if s.topo != nil {
			tctx, tcancel := context.WithTimeout(ctx, 10*time.Second)
			if g, err := s.topo.Build(tctx, pipelineID); err == nil {
				alert.PipelineMap = s.topo.Render(g)
			}
			tcancel()
		}
		deps := agent.Deps{
			Tools:     s.tools,
			Actions:   s.actions,
			Inbox:     st.inbox,
			Sink:      roomSink{s: s, roomID: roomID},
			Proposals: proposalCreator{s: s, roomID: roomID},
			Logger:    s.log,
		}
		if err := s.run(ctx, alert, deps, s.opts); err != nil && ctx.Err() == nil {
			s.log.Warn("room: agent run returned error", "room", roomID, "err", err)
			ectx, ecancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = s.agentEmit(ectx, roomID, events.AgentStatus, events.AgentStatusPayload{State: "error", Detail: truncateDetail(err.Error())})
			ecancel()
		}
	}()
}

func truncateDetail(s string) string {
	const max = 500
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// ---- topology fallback (until A3 lands) ----

type noopTopology struct{}

func (noopTopology) Build(ctx context.Context, pipelineID string) (topology.Graph, error) {
	return topology.Graph{PipelineID: pipelineID, Nodes: []topology.Node{}, Edges: []topology.Edge{}}, nil
}

func (noopTopology) Render(g topology.Graph) string { return "no topology available" }

func (noopTopology) Trace(ctx context.Context, pipelineID, table, key string) ([]events.TraceStage, error) {
	return nil, fmt.Errorf("room: trace: topology unavailable")
}

func (noopTopology) LayerOwner(pipelineID, layer string) (string, bool) { return "", false }
