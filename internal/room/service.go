// Package room is the incident room service: it enforces the rules in
// docs/api.md on top of the append-only event log, supervises one agent per
// room, and runs approved fixes.
package room

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// Config holds the service's collaborators.
type Config struct {
	Store   store.Store
	Owners  *owners.Owners
	Users   *Users
	Actions *actions.Registry
	// Tools is handed to the agent; may be nil when Runner is nil or ignores it.
	Tools    *tools.Registry
	Topology topology.Builder
	// Runner starts the agent for a room. Nil means rooms have no agent.
	Runner agent.Runner
	// Handoff writes hand-off summaries. Nil makes hand-offs fail with 503.
	Handoff      agent.HandoffWriter
	AgentOptions agent.Options
	Logger       *slog.Logger
	// Now is the clock for lease rules; defaults to time.Now. It should be the
	// same clock the Store stamps events with.
	Now func() time.Time
	// ExecTimeout bounds one action's Execute; default 60 s.
	ExecTimeout time.Duration
	// HandoffTimeout bounds the hand-off summary call; default 3 min.
	HandoffTimeout time.Duration
}

// Service implements the room rules.
type Service struct {
	cfg  Config
	log  *slog.Logger
	base context.Context
	stop context.CancelFunc
	wg   sync.WaitGroup // agents and executors
	exec sync.WaitGroup // executors only

	mu    sync.Mutex
	rooms map[string]*roomEntry

	idxMu        sync.Mutex
	proposalRoom map[string]string // proposal id -> room id
	handoffRoom  map[string]string
	dedup        map[string]string // alert dedup key -> open room id
}

type roomEntry struct {
	id          string
	mu          sync.Mutex // serializes rule checks + appends for this room
	st          *State
	inbox       *inbox
	cancelAgent context.CancelFunc
}

// New returns a Service. Call Start before serving requests.
func New(cfg Config) (*Service, error) {
	if cfg.Store == nil || cfg.Owners == nil || cfg.Users == nil || cfg.Actions == nil {
		return nil, errors.New("room: Store, Owners, Users and Actions are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.ExecTimeout <= 0 {
		cfg.ExecTimeout = 60 * time.Second
	}
	if cfg.HandoffTimeout <= 0 {
		cfg.HandoffTimeout = 3 * time.Minute
	}
	if err := cfg.Users.CheckOwners(cfg.Owners); err != nil {
		return nil, fmt.Errorf("room: users and owners disagree: %w", err)
	}
	base, stop := context.WithCancel(context.Background())
	return &Service{
		cfg: cfg, log: cfg.Logger, base: base, stop: stop,
		rooms: map[string]*roomEntry{}, proposalRoom: map[string]string{},
		handoffRoom: map[string]string{}, dedup: map[string]string{},
	}, nil
}

// Start rebuilds every room from the log. An agent that was running when the
// server stopped is not resumed (its conversation is not in the log); open
// rooms get an agent.status "error" event saying so.
func (s *Service) Start(ctx context.Context) error {
	ids, err := s.cfg.Store.RoomIDs(ctx)
	if err != nil {
		return fmt.Errorf("room: list rooms: %w", err)
	}
	for _, id := range ids {
		evs, err := s.allEvents(ctx, id)
		if err != nil {
			return fmt.Errorf("room: load %s: %w", id, err)
		}
		r := &roomEntry{id: id, st: Fold(id, evs), inbox: newInbox()}
		s.rooms[id] = r
		for _, e := range evs {
			s.index(id, e)
		}
		if r.st.snap.Status == StatusOpen && s.cfg.Runner != nil {
			r.mu.Lock()
			_, err := s.appendLocked(ctx, r, events.AgentStatus, events.Actor{Kind: events.ActorSystem, ID: "system", Name: "Piperoom"},
				events.AgentStatusPayload{State: "error", Detail: "server restarted; the agent was not resumed"})
			r.mu.Unlock()
			if err != nil {
				return fmt.Errorf("room: mark %s: %w", id, err)
			}
		}
	}
	return nil
}

// Shutdown stops every agent and executor and waits for them.
func (s *Service) Shutdown() {
	s.stop()
	s.wg.Wait()
}

// Wait blocks until background executors and agents have finished. Tests use it.
func (s *Service) Wait() { s.wg.Wait() }

// WaitIdle blocks until running executors have finished; agents may still run.
func (s *Service) WaitIdle() { s.exec.Wait() }

func (s *Service) now() time.Time { return s.cfg.Now().UTC() }

// Authenticate returns the user with this id.
func (s *Service) Authenticate(id string) (User, bool) { return s.cfg.Users.Get(id) }

// RoomExists reports whether roomID is a known room.
func (s *Service) RoomExists(_ context.Context, roomID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.rooms[roomID]
	return ok
}

func (s *Service) room(id string) (*roomEntry, error) {
	s.mu.Lock()
	r, ok := s.rooms[id]
	s.mu.Unlock()
	if !ok {
		return nil, newErr(http.StatusNotFound, "not_found", "room not found")
	}
	return r, nil
}

func (s *Service) allEvents(ctx context.Context, roomID string) ([]events.Event, error) {
	var out []events.Event
	var after int64
	for {
		page, err := s.cfg.Store.Since(ctx, roomID, after, 1000)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if len(page) < 1000 {
			return out, nil
		}
		after = page[len(page)-1].Seq
	}
}

// appendLocked appends an event and folds it into the room state. r.mu must be held.
func (s *Service) appendLocked(ctx context.Context, r *roomEntry, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	ev, err := s.cfg.Store.Append(ctx, r.id, typ, actor, payload)
	if err != nil {
		return events.Event{}, fmt.Errorf("room: append %s to %s: %w", typ, r.id, err)
	}
	r.st.Apply(ev)
	s.index(r.id, ev)
	s.feedAgent(r, ev)
	return ev, nil
}

// index records which room owns a proposal or hand-off id.
func (s *Service) index(roomID string, e events.Event) {
	switch e.Type {
	case events.ProposalCreated:
		if p, err := events.Decode[events.ProposalCreatedPayload](e); err == nil {
			s.idxMu.Lock()
			s.proposalRoom[p.ProposalID] = roomID
			s.idxMu.Unlock()
		}
	case events.HandoffCreated:
		if p, err := events.Decode[events.HandoffCreatedPayload](e); err == nil {
			s.idxMu.Lock()
			s.handoffRoom[p.HandoffID] = roomID
			s.idxMu.Unlock()
		}
	}
}

// ---- Reads ----

// GetRoom returns one room's snapshot.
func (s *Service) GetRoom(_ context.Context, roomID string) (Snapshot, error) {
	r, err := s.room(roomID)
	if err != nil {
		return Snapshot{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.st.Snapshot(), nil
}

// ListRooms returns every room, open ones first (newest first within each group).
func (s *Service) ListRooms(_ context.Context) ([]Snapshot, error) {
	s.mu.Lock()
	rs := make([]*roomEntry, 0, len(s.rooms))
	for _, r := range s.rooms {
		rs = append(rs, r)
	}
	s.mu.Unlock()
	out := make([]Snapshot, 0, len(rs))
	for _, r := range rs {
		r.mu.Lock()
		out = append(out, r.st.Snapshot())
		r.mu.Unlock()
	}
	sort.SliceStable(out, func(i, j int) bool {
		if (out[i].Status == StatusOpen) != (out[j].Status == StatusOpen) {
			return out[i].Status == StatusOpen
		}
		return out[i].OpenedAt.After(out[j].OpenedAt)
	})
	return out, nil
}

// Events returns the events after a sequence number (limit 1..1000, default 500).
func (s *Service) Events(ctx context.Context, roomID string, after int64, limit int) (EventsPage, error) {
	r, err := s.room(roomID)
	if err != nil {
		return EventsPage{}, err
	}
	if limit <= 0 {
		limit = 500
	}
	if limit > 1000 {
		limit = 1000
	}
	if after < 0 {
		after = 0
	}
	evs, err := s.cfg.Store.Since(ctx, roomID, after, limit)
	if err != nil {
		return EventsPage{}, fmt.Errorf("room: events %s: %w", roomID, err)
	}
	if evs == nil {
		evs = []events.Event{}
	}
	r.mu.Lock()
	last := r.st.snap.LastSeq
	r.mu.Unlock()
	return EventsPage{Events: evs, LastSeq: last}, nil
}

// Topology returns the pipeline map.
func (s *Service) Topology(ctx context.Context, pipelineID string) (topology.Graph, error) {
	if _, ok := s.cfg.Owners.Pipeline(pipelineID); !ok || s.cfg.Topology == nil {
		return topology.Graph{}, newErr(http.StatusNotFound, "not_found", "pipeline not found")
	}
	g, err := s.cfg.Topology.Build(ctx, pipelineID)
	if err != nil {
		return topology.Graph{}, fmt.Errorf("room: topology %s: %w", pipelineID, err)
	}
	if g.Nodes == nil {
		g.Nodes = []topology.Node{}
	}
	if g.Edges == nil {
		g.Edges = []topology.Edge{}
	}
	return g, nil
}
