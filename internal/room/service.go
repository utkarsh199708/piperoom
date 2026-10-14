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
	"piperoom/internal/auth"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// DriverLease is how long control lasts; renewed by any driver action.
const DriverLease = 30 * time.Minute

// leaseLogThreshold is the remaining-lease threshold under which a renewal is
// written to the log as control.granted (docs/api.md).
const leaseLogThreshold = 25 * time.Minute

// ErrStepOrder reports a proposal step used out of order: started twice, or
// finished without being started. It is the same sentinel as
// store.ErrStepOrder so both layers can be matched with errors.Is.
var ErrStepOrder = store.ErrStepOrder

// Deps wires the room service to its collaborators. Any field may be nil to
// get a safe default (used by tests), except Owners: scope checks fail closed
// without it.
type Deps struct {
	Store   store.Store
	Actions *actions.Registry
	Tools   *tools.Registry
	// StackFor, when set, gives each room the read tools and write actions
	// built over its own pipeline's clients (docs/design/tenancy.md section
	// 5), and Tools and Actions are not used. A nil field in the result
	// means none (fail closed), never the process-wide registry.
	StackFor     func(pipelineID string) PipelineStack
	Topology     topology.Builder
	Users        *Users
	Owners       *owners.Owners // pipeline definitions for actions.Scoped checks
	Run          agent.Runner
	WriteHandoff agent.HandoffWriter
	Options      agent.Options
	// ShadowOptions are the agent options of a shadow room's run
	// (docs/design/shadow.md section 3.3): the same model, fewer turns and a
	// lower token budget (config.ParseShadow).
	ShadowOptions agent.Options
	// ShadowGrace is how long a shadow run may keep going after its room
	// closes; 0 cancels it at close like a live run.
	ShadowGrace time.Duration
	Logger      *slog.Logger
	Now         func() time.Time

	// MaxAgents caps agent runs working at the same time across all rooms; a
	// room over the cap waits in a FIFO queue. 0 or less means DefaultMaxAgents.
	MaxAgents int
	// ExportMaxEvents caps the events one audit export may contain; a larger
	// room answers 413 export_too_large. Zero means DefaultExportMaxEvents.
	ExportMaxEvents int64

	// Sessions lets LookupPerson find a person who is not in users.yaml by their
	// newest live session (design section 2.1). Nil: users.yaml only.
	// SessionIdle is the idle limit that decides whether a session is live.
	Sessions    auth.Store
	SessionIdle time.Duration
	// ViewerGroups are the IdP groups that let a person in as a viewer
	// (PIPEROOM_AUTH_VIEWER_GROUPS).
	ViewerGroups []string
	// SoloUser is PIPEROOM_SOLO_USER: that user may approve every layer, with a
	// typed confirm and the grant "solo". The caller has already run
	// CheckSoloUser.
	SoloUser string

	// OnRoomOpened, when set, is called once per room opened by CreateRoom or
	// CreateRoomFromAlert, after room.created is appended. Each call runs in
	// its own goroutine with the service lifetime ctx, so calls may overlap;
	// the hook must return promptly once ctx ends because Close waits for it.
	OnRoomOpened func(ctx context.Context, roomID string)

	// Limits are the room limits (config.ParseLimits, docs/design/retention.md):
	// the open-room limit, the tool data cap and the archive directory. The
	// log caps (RoomMaxEvents, RoomMaxBytes) are enforced by the store, which
	// the caller configures with store.Capped.SetLimits. The zero value turns
	// every limit off (unit tests).
	Limits config.Limits

	// OnRoomClosed, when set, is called once per room closed by CloseRoom, after
	// room.closed is appended, the alert key released and the close persisted,
	// and after s.mu is released. It runs on the closing request, before
	// CloseRoom returns, with the service lifetime ctx, so a caller that closes
	// a room and then asks the hook's owner about it sees the hook already ran.
	// The hook must be fast and must not call back into a method that waits for
	// the room to settle. dedupKey is the key the room was opened under, as the
	// alert source gave it (alertin.Alert.DedupKey, for example "watcher:orders"),
	// without the pipeline prefix; it is "" for a room opened by hand or by an
	// alert with no key. Nil means no-op.
	OnRoomClosed func(ctx context.Context, roomID, pipelineID, dedupKey string)
}

type roomState struct {
	snap        RoomSnapshot
	inbox       *inbox
	agentCancel context.CancelFunc
	agentRun    uint64             // generation of the latest agent start; see releaseAgent
	execCancel  context.CancelFunc // cancels the room's executor on close
	exec        *execQueue         // nil until the first proposal is approved
	dedupKey    string             // alert index key of an alert room; "" otherwise

	// Shadow run state (shadow.go). shadowActive is true from the start of
	// the shadow supervisor until it has written the run's final status.
	// shadowInAttempt is true only while an attempt runs (runGuarded), so a
	// close gives grace to a run that is mid-investigation and cancels a run
	// that is queued or backing off. graceTimer cancels a run that is still
	// going PIPEROOM_SHADOW_GRACE after the close.
	shadowActive    bool
	shadowInAttempt bool
	graceTimer      *time.Timer
	// shadowLogFull is set when the shadow log reached the room's cap: the
	// run is cancelled and ends with status budget.
	shadowLogFull bool

	// deleting is set while DeleteRoom archives and deletes the room: every
	// append through the service is refused, so the archive and the delete
	// see the same log.
	deleting bool
	// logBytes is the room log's stored payload bytes as the store counts
	// them (store.Capped): read once when the room is loaded, then grown by
	// len(Payload) of each event appended. With snap.LastSeq (the event
	// count) it drives the cap warning without a query per append.
	logBytes int64
}

// Service is the room domain logic. All state derives from the event log; the
// in-memory cache is rebuilt from the log on start.
type Service struct {
	store        store.Store
	actions      *actions.Registry
	tools        *tools.Registry
	stackFor     func(pipelineID string) PipelineStack
	topo         topology.Builder
	users        *Users
	owners       *owners.Owners
	run          agent.Runner
	writeHandoff agent.HandoffWriter
	opts         agent.Options
	shadowOpts   agent.Options
	shadowGrace  time.Duration
	shadow       store.ShadowStore // nil when the store cannot hold shadow runs
	log          *slog.Logger
	now          func() time.Time
	onRoomOpened func(ctx context.Context, roomID string)
	onRoomClosed func(ctx context.Context, roomID, pipelineID, dedupKey string)
	slots        *agentSlots // global cap on concurrent agent runs
	exportMax    int64
	sessions     auth.Store
	sessionIdle  time.Duration
	viewerGroups map[string]struct{}
	soloUser     string
	limits       config.Limits
	// deleteMu serialises DeleteRoom calls: deletions are rare, and one at a
	// time makes a concurrent repeat find the first one's audit row.
	deleteMu sync.Mutex

	// ctx is the service lifetime: every goroutine the service starts (agent
	// supervisors, executor queues) derives from it. Close cancels it and
	// waits.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	closed bool
	rooms  map[string]*roomState
	// alertRooms maps the dedup key of each open webhook room to that room, so
	// a repeat of the same alert joins the room instead of opening another.
	alertRooms map[string]string
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
	shadowStore, _ := d.Store.(store.ShadowStore)
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		store:        d.Store,
		actions:      acts,
		tools:        tls,
		stackFor:     d.StackFor,
		topo:         topo,
		users:        users,
		owners:       d.Owners,
		run:          run,
		writeHandoff: wh,
		opts:         d.Options,
		shadowOpts:   d.ShadowOptions,
		shadowGrace:  d.ShadowGrace,
		shadow:       shadowStore,
		log:          log,
		now:          now,
		onRoomOpened: d.OnRoomOpened,
		onRoomClosed: d.OnRoomClosed,
		slots:        newAgentSlots(d.MaxAgents),
		exportMax:    d.ExportMaxEvents,
		sessions:     d.Sessions,
		sessionIdle:  d.SessionIdle,
		viewerGroups: groupSet(d.ViewerGroups),
		soloUser:     d.SoloUser,
		limits:       d.Limits,
		ctx:          ctx,
		cancel:       cancel,
		rooms:        map[string]*roomState{},
		alertRooms:   map[string]string{},
	}
	return s
}

// Close cancels the service lifetime context and waits for every goroutine it
// started (agent supervisors, executor queues) to stop. It is idempotent and
// returns promptly once all of them have exited.
func (s *Service) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	s.cancel()
	s.wg.Wait()
}

// LookupUser returns a user by id (for auth and the /api/me endpoint).
func (s *Service) LookupUser(id string) (User, bool) { return s.users.Get(id) }

// AllUsers returns the full directory.
func (s *Service) AllUsers() []User { return s.users.All() }

// Load rebuilds the snapshot cache from the event log. Called once at startup.
// It does not start any agent run: each open room gets an idle marker and a
// system message, and its agent starts on the next steer or on a comment that
// mentions the agent (ensureAgentLocked). A restart must not spend model calls
// on rooms nobody is watching.
func (s *Service) Load(ctx context.Context) error {
	if s.shadow == nil && s.owners.AnyShadow() {
		return fmt.Errorf("room: load: owners.yaml turns shadow mode on, but the store cannot hold shadow runs")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureAllLoadedLocked(ctx, true); err != nil {
		return err
	}
	s.sweepShadowLocked(ctx)
	return nil
}

// ensureAllLoadedLocked rebuilds every room not yet cached. restarted is true
// only for the start-up Load: those open rooms are marked as idle after a
// server restart. Caller must hold s.mu.
func (s *Service) ensureAllLoadedLocked(ctx context.Context, restarted bool) error {
	rooms, err := s.store.ListRooms(ctx)
	if err != nil {
		return fmt.Errorf("room: load: %w", err)
	}
	for _, r := range rooms {
		if _, ok := s.rooms[r.ID]; ok {
			continue
		}
		if r.LastSeq == 0 {
			// A row with no events is a leftover of a room.created append that
			// failed (or a crash before it); creation holds s.mu, so a live
			// room is never seen here without events. It is not a room.
			if restarted {
				s.log.Warn("room: skipping room row with no events", "room", r.ID)
			}
			continue
		}
		snap, err := s.rebuild(ctx, r.ID)
		if err != nil {
			s.log.Warn("room: rebuild failed", "room", r.ID, "err", err)
			continue
		}
		st := &roomState{snap: snap, inbox: newRoomInbox(snap.Shadow)}
		s.rooms[r.ID] = st
		s.loadUsageLocked(ctx, r.ID, st)
		if snap.Status == "open" {
			s.indexAlertRoomLocked(ctx, r.ID, st)
			s.interruptStaleProposalsLocked(ctx, r.ID, st)
			// A shadow room never resumes its agent, so the "steer to
			// resume" notice would be wrong there (the sweep in Load marks
			// its run interrupted instead).
			if restarted && !snap.Shadow {
				if err := s.noteRestartLocked(ctx, r.ID, st); err != nil {
					s.log.Warn("room: restart notice failed", "room", r.ID, "err", err)
				}
			}
		}
	}
	return nil
}

// restartDetail is the agent status detail a restart writes. A room already
// showing it is not marked again, so repeated restarts do not pile up notices.
const restartDetail = "server restarted"

// noteRestartLocked tells the room that the server restarted: the agent is idle
// and paused until someone steers or mentions it. It writes agent.status idle and
// a system message to the log, unless the room already shows that state. Caller
// must hold s.mu.
func (s *Service) noteRestartLocked(ctx context.Context, roomID string, st *roomState) error {
	if st.snap.AgentStatus.State == "idle" && st.snap.AgentStatus.Detail == restartDetail {
		return nil
	}
	if _, err := s.emitLocked(ctx, st, roomID, events.AgentStatus, events.AgentActor, events.AgentStatusPayload{
		State: "idle", Detail: restartDetail,
	}); err != nil {
		return err
	}
	_, err := s.emitLocked(ctx, st, roomID, events.AgentMessage, systemActor, events.AgentMessagePayload{
		Text: "server restarted — the agent is paused; the driver can steer, or anyone can mention @agent, to resume",
	})
	return err
}

// ensureAgentLocked starts the room's agent run if the room has none yet. A
// room restored by Load has none until someone steers it or mentions the agent;
// a room whose agent was already started is left alone. Caller must hold s.mu.
func (s *Service) ensureAgentLocked(ctx context.Context, roomID string, st *roomState) {
	// A shadow run is one-shot: steers, suggestions and @agent comments are
	// logged but never start or restart it (docs/design/shadow.md 3.3).
	// A full log never gets its agent back: every agent event would be refused.
	if st.snap.Shadow || st.agentCancel != nil || !s.roomOpen(st) || st.snap.LogCap == LogCapFull || st.deleting {
		return
	}
	s.startAgentLocked(roomID, st.snap.Title, st.snap.PipelineID, s.rawAlert(ctx, roomID), st)
}

// interruptStaleProposalsLocked records the outcome of proposals that were
// fully approved but never finished before this (re)start (review item 6):
// without it they would stay "open" forever in the snapshot. For each stale
// proposal it finishes every claimed store step (best effort), emits one
// action.executed with OK=false naming the step that was in flight, and sets
// the proposal status to "failed". Caller must hold s.mu.
func (s *Service) interruptStaleProposalsLocked(ctx context.Context, roomID string, st *roomState) {
	for _, p := range append([]Proposal{}, st.snap.OpenProposals...) {
		if !fullyApproved(p) {
			continue
		}
		// The snapshot is not the whole truth: if the last action.executed
		// append failed but the store status write succeeded, the DB already
		// says executed/failed/rejected while the snapshot still shows the
		// proposal open. Skip those — do not re-mark a finished proposal failed
		// (review item 6).
		if row, err := s.store.GetProposal(ctx, p.ProposalID); err == nil && row.Status != "open" {
			continue
		}
		next := s.interruptedStep(ctx, roomID, p)
		for i := range p.Steps {
			s.finishClaimedStep(p.ProposalID, i, "interrupted by a service restart")
		}
		if _, err := s.emitLocked(ctx, st, roomID, events.ActionExecuted, executorActor, events.ActionExecutedPayload{
			ProposalID: p.ProposalID,
			StepIndex:  next,
			OK:         false,
			ParamsHash: p.ParamsHash,
			Output:     "proposal was interrupted by a service restart before it finished; create a new proposal to retry",
		}); err != nil {
			s.log.Warn("room: emit interruption failed", "proposal", p.ProposalID, "err", err)
		}
		s.setProposalStatus(p.ProposalID, "failed")
	}
}

// interruptedStep returns the index of the step that had not finished: the
// first step with no recorded successful action.executed event, since steps
// run in order. It reads the log because the store keeps no per-proposal step
// list.
func (s *Service) interruptedStep(ctx context.Context, roomID string, p Proposal) int {
	done := map[int]bool{}
	after := int64(0)
	for {
		evs, err := s.store.Since(ctx, roomID, after, 1000)
		if err != nil || len(evs) == 0 {
			break
		}
		for _, e := range evs {
			after = e.Seq
			if e.Type != events.ActionExecuted {
				continue
			}
			pay, ok := decode[events.ActionExecutedPayload](e)
			if ok && pay.ProposalID == p.ProposalID && pay.OK {
				done[pay.StepIndex] = true
			}
		}
		if len(evs) < 1000 {
			break
		}
	}
	for i := range p.Steps {
		if !done[i] {
			return i
		}
	}
	return 0
}

func (s *Service) rawAlert(ctx context.Context, roomID string) json.RawMessage {
	p, _ := s.roomCreated(ctx, roomID)
	return p.Alert
}

// roomCreated returns the payload of a room's room.created event.
func (s *Service) roomCreated(ctx context.Context, roomID string) (events.RoomCreatedPayload, bool) {
	first, err := s.store.Since(ctx, roomID, 0, 1)
	if err != nil || len(first) == 0 || first[0].Type != events.RoomCreated {
		return events.RoomCreatedPayload{}, false
	}
	return decode[events.RoomCreatedPayload](first[0])
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
	st := &roomState{snap: snap, inbox: newRoomInbox(snap.Shadow)}
	s.rooms[roomID] = st
	s.loadUsageLocked(ctx, roomID, st)
	if snap.Status == "open" {
		s.indexAlertRoomLocked(ctx, roomID, st)
	}
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
	s.setDeadline(&snap)
	if row, err := s.store.GetRoom(ctx, roomID); err == nil {
		if row.DriverID != "" && snap.Driver != nil && snap.Driver.ID == row.DriverID && row.DriverLeaseUntil != nil && row.DriverLeaseUntil.After(snap.Driver.LeaseUntil) {
			// docs/api.md: times are RFC 3339 UTC; a persisted lease read back
			// from Postgres may carry the session's local zone.
			snap.Driver.LeaseUntil = row.DriverLeaseUntil.UTC()
		}
	}
	return snap, nil
}

// emitLocked appends one event, folds it into the cached snapshot and feeds
// the agent inbox. In a shadow room every event of the agent actor goes to the
// shadow log instead (docs/design/shadow.md 3.2): it is not folded and not fed,
// so no agent text, status or error reaches the room log, the WebSocket, the
// Slack mirror or the export before the reveal. This is the one place that
// decides it. Caller must hold s.mu.
//
// It also applies the room log caps (docs/design/retention.md section 5.3):
// the store refuses an event over the cap with store.ErrLogFull, and after
// each append the room's usage (the seq, and the payload bytes added up)
// decides the one 90% warning and the log_full stop. A room being deleted
// refuses every append.
func (s *Service) emitLocked(ctx context.Context, st *roomState, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	if st.snap.Shadow && actor == events.AgentActor {
		e, err := s.appendShadowLocked(ctx, roomID, typ, payload)
		if errors.Is(err, store.ErrLogFull) {
			s.shadowLogFullLocked(roomID, st)
		}
		return e, err
	}
	if st.deleting {
		return events.Event{}, fmt.Errorf("room: append %s: %w", typ, errRoomDeleting())
	}
	e, err := s.store.Append(ctx, roomID, typ, actor, payload)
	if err != nil {
		if errors.Is(err, store.ErrLogFull) {
			s.logFullLocked(ctx, st, roomID)
		}
		return events.Event{}, fmt.Errorf("room: append %s: %w", typ, err)
	}
	st.snap = Apply(st.snap, e)
	st.logBytes += int64(len(e.Payload))
	s.setDeadline(&st.snap)
	s.feedInbox(st, e)
	s.checkCapLocked(ctx, st, roomID)
	return e, nil
}

// agentEventTypes is the set of event types the investigation agent may append
// through its Sink (review item 9). The agent's own events are exactly the
// agent.* families (internal/agent: run.go and tools_room.go); proposals go
// through agent.Proposals (CreateProposal), which validates them, not through
// the Sink.
var agentEventTypes = map[events.Type]bool{
	events.AgentStatus:     true,
	events.AgentStepStart:  true,
	events.AgentToolCall:   true,
	events.AgentToolResult: true,
	events.AgentMessage:    true,
	events.AgentHypothesis: true,
	events.AgentUsage:      true,
}

// agentEmit is the agent Sink: the agent's events enter the same log and cache.
// Only agent.* event types are accepted, so the sink cannot forge
// control-plane events (control.granted, approval.given, ...).
func (s *Service) agentEmit(ctx context.Context, roomID string, typ events.Type, payload any) error {
	if !agentEventTypes[typ] {
		return fmt.Errorf("room: agent sink refused event type %q", typ)
	}
	// Model and tool text may hold NUL or invalid bytes; the run must not fail
	// on them, so they become U+FFFD before the append.
	payload, err := sanitizeAgentPayload(payload)
	if err != nil {
		return err
	}
	if typ == events.AgentToolResult {
		if payload, err = s.capToolData(payload); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.rooms[roomID]
	if !ok {
		return fmt.Errorf("room: agent emit on unknown room %q", roomID)
	}
	_, err = s.emitLocked(ctx, st, roomID, typ, events.AgentActor, payload)
	return err
}

// executorEmit appends an event on behalf of the action executor.
func (s *Service) executorEmit(ctx context.Context, roomID string, typ events.Type, payload any) error {
	if p, ok := payload.(events.ActionExecutedPayload); ok {
		p.Output = s.capOutput(p.Output)
		payload = p
	}
	return s.emitAs(ctx, roomID, executorActor, typ, payload)
}

func (s *Service) emitAs(ctx context.Context, roomID string, actor events.Actor, typ events.Type, payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.rooms[roomID]
	if !ok {
		return fmt.Errorf("room: emit on unknown room %q", roomID)
	}
	_, err := s.emitLocked(ctx, st, roomID, typ, actor, payload)
	return err
}

// isDriver reports whether userID holds the driver role. The role survives an
// expired lease: docs/api.md says a driver whose lease expired and who was not
// replaced is still treated as the driver, and their next action renews the
// lease.
func (s *Service) isDriver(st *roomState, userID string) bool {
	d := st.snap.Driver
	return d != nil && d.ID == userID
}

// leaseExpired reports whether the driver lease has lapsed (or nobody drives).
func (s *Service) leaseExpired(st *roomState) bool {
	d := st.snap.Driver
	return d == nil || !s.now().Before(d.LeaseUntil)
}

// touchDriverLease renews the driver lease in the snapshot and persists it.
// Per docs/api.md a renewal that happens while less than 25 minutes remain is
// written to the log as control.granted with the same driver and the new
// lease_until; since the renewal resets the lease to a full 30 minutes, that
// test by construction logs at most one such event per 5 minutes (review item
// 11). Caller must hold s.mu.
func (s *Service) touchDriverLease(ctx context.Context, st *roomState, roomID string) {
	d := st.snap.Driver
	if d == nil {
		return
	}
	now := s.now().UTC()
	lease := now.Add(DriverLease)
	remaining := d.LeaseUntil.Sub(now)
	st.snap.Driver.LeaseUntil = lease
	if err := s.store.UpdateDriver(ctx, roomID, d.ID, d.Name, lease); err != nil {
		s.log.Warn("room: persist driver lease failed", "room", roomID, "err", err)
	}
	if remaining >= leaseLogThreshold {
		return
	}
	if _, err := s.emitLocked(ctx, st, roomID, events.ControlGranted, s.humanActor(User{ID: d.ID, Name: d.Name}), events.ControlGrantedPayload{
		To:         s.humanActor(User{ID: d.ID, Name: d.Name}),
		LeaseUntil: lease,
	}); err != nil {
		s.log.Warn("room: emit lease renewal failed", "room", roomID, "err", err)
		return
	}
	s.log.Info("room: renewed driver lease", "room", roomID, "driver", d.ID, "lease_until", lease)
}

func (s *Service) roomOpen(st *roomState) bool {
	return st.snap.Status == "open"
}

// knownPipeline reports whether the owners config knows pipelineID. With no
// owners config loaded there is nothing to validate against, so every id is
// treated as known (unit tests).
func (s *Service) knownPipeline(pipelineID string) bool {
	if s.owners == nil {
		return true
	}
	_, ok := s.owners.Pipeline(pipelineID)
	return ok
}

// proposalsDisabled reports whether the room must not accept proposals: the
// pipeline is unknown to the owners config, so there is nothing to scope a
// fix against (docs/api.md:103). It derives from the snapshot's pipeline id,
// so it survives a restart: a webhook room opened for an unknown pipeline
// keeps proposals disabled after the service rebuilds from the log.
func (s *Service) proposalsDisabled(st *roomState) bool {
	return s.owners != nil && st.snap.PipelineID != "" && !s.knownPipeline(st.snap.PipelineID)
}

// humanActor is the event actor for a person. Verified comes from users.yaml
// itself, never from the caller's User value, so no code path can mark an IdP
// person as vouched for (design section 2.1, security audit A-ID-2).
func (s *Service) humanActor(u User) events.Actor {
	_, inFile := s.users.Get(u.ID)
	return events.Actor{Kind: events.ActorHuman, ID: u.ID, Name: u.Name, Verified: inFile}
}

var systemActor = events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System", Verified: true}

var executorActor = events.Actor{Kind: events.ActorSystem, ID: "executor", Name: "Executor", Verified: true}

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
	// closed is set for a shadow room: nothing is ever delivered, and Wait
	// returns agent.ErrInboxClosed so the run ends at its first end_turn.
	closed bool
}

func newInbox() *inbox {
	return &inbox{wake: make(chan struct{}, 1)}
}

// newClosedInbox is a shadow room's inbox: Drain returns nothing and Wait
// returns agent.ErrInboxClosed at once.
func newClosedInbox() *inbox {
	return &inbox{wake: make(chan struct{}, 1), closed: true}
}

// newRoomInbox returns the inbox for a room: closed for a shadow room.
func newRoomInbox(shadow bool) *inbox {
	if shadow {
		return newClosedInbox()
	}
	return newInbox()
}

// pushFront returns drained input to the head of the queue, ahead of anything
// that arrived since, and wakes a waiter. Used when a Drain could not get a
// slot for the input it took.
func (in *inbox) pushFront(items []agent.HumanInput) {
	if len(items) == 0 || in.closed {
		return
	}
	in.mu.Lock()
	in.items = append(append([]agent.HumanInput(nil), items...), in.items...)
	in.mu.Unlock()
	select {
	case in.wake <- struct{}{}:
	default:
	}
}

// pending returns how many inputs are waiting to be drained.
func (in *inbox) pending() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return len(in.items)
}

func (in *inbox) push(inp agent.HumanInput) {
	if in.closed {
		return
	}
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

// Wait blocks until at least one input is pending or ctx ends. A closed inbox
// returns agent.ErrInboxClosed at once.
func (in *inbox) Wait(ctx context.Context) error {
	if in.closed {
		return agent.ErrInboxClosed
	}
	select {
	case <-in.wake:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// feedInbox converts relevant events into agent.HumanInput. Caller holds s.mu.
func (s *Service) feedInbox(st *roomState, e events.Event) {
	if st.inbox == nil || st.snap.Shadow {
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
		approved := true
		input = &agent.HumanInput{
			Seq: e.Seq, Actor: e.Actor, Kind: "approval_result",
			Text:       fmt.Sprintf("layer %q approved proposal %s", p.Layer, p.ProposalID),
			ProposalID: p.ProposalID, Layer: p.Layer, Approved: &approved,
		}
	case events.ApprovalRejected:
		p, ok := decode[events.ApprovalRejectedPayload](e)
		if !ok {
			return
		}
		approved := false
		input = &agent.HumanInput{
			Seq: e.Seq, Actor: e.Actor, Kind: "approval_result",
			Text:       fmt.Sprintf("layer %q rejected proposal %s: %s", p.Layer, p.ProposalID, p.Reason),
			ProposalID: p.ProposalID, Layer: p.Layer, Approved: &approved,
		}
	case events.ActionExecuted:
		p, ok := decode[events.ActionExecutedPayload](e)
		if !ok {
			return
		}
		// OK tells the agent whether the step succeeded; it must not claim the
		// pipeline recovered from a failed step (agent.HumanInput.OK).
		stepOK := p.OK
		stepIndex := p.StepIndex
		input = &agent.HumanInput{
			Seq: e.Seq, Actor: e.Actor, Kind: "action_result",
			Text: fmt.Sprintf("proposal %s step %d: %s", p.ProposalID, p.StepIndex, p.Output),
			OK:   &stepOK, ProposalID: p.ProposalID, StepIndex: &stepIndex,
		}
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

// Agent auto-restart bounds (LEAD-ARCH round 4, arch should-fix): a failed
// agent run is retried with growing backoff so a transient error does not
// leave the room without an agent forever.
const (
	maxAgentAttempts  = 3
	agentRetryBackoff = 1 * time.Second // doubled after every failed attempt
)

// startAgentLocked starts the supervisor for an open room's agent. Caller holds
// s.mu. It does nothing after Close, so a shutting-down service never adds
// goroutines. The supervisor first waits for a global agent slot (agentSlots).
// The room counts as having a run from this call on, so more steers or
// mentions while it is queued do not queue it again.
func (s *Service) startAgentLocked(roomID, title, pipelineID string, raw json.RawMessage, st *roomState) {
	if s.closed {
		return
	}
	if st.snap.Shadow {
		s.startShadowLocked(roomID, title, pipelineID, raw, st)
		return
	}
	ctx, cancel := context.WithCancel(s.ctx)
	st.agentCancel = cancel
	st.agentRun++
	run := st.agentRun
	gate := &agentGate{s: s, st: st, roomID: roomID}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// However the supervisor ends, its slot or queue entry goes back.
		defer gate.release()
		alert := agent.Alert{RoomID: roomID, Title: title, PipelineID: pipelineID, Raw: raw}
		deps := agent.Deps{
			Tools:     s.toolsFor(pipelineID),
			Actions:   s.actionsFor(pipelineID),
			Inbox:     gatedInbox{in: st.inbox, g: gate, runCtx: ctx},
			Sink:      roomSink{s: s, roomID: roomID},
			Proposals: proposalCreator{s: s, roomID: roomID},
			Logger:    s.log,
		}
		backoff := agentRetryBackoff
		mapped := false
		for attempt := 1; ; attempt++ {
			if err := gate.acquire(ctx); err != nil {
				return // room closed or service stopped while queued
			}
			if !mapped {
				// Built once the slot is granted, so a queued room does not
				// run on a topology that went stale while it waited.
				mapped = true
				if s.topo != nil {
					tctx, tcancel := context.WithTimeout(ctx, 10*time.Second)
					if g, err := s.topo.Build(tctx, pipelineID); err == nil {
						alert.PipelineMap = s.topo.Render(g)
					}
					tcancel()
				}
			}
			err := s.runGuarded(ctx, roomID, alert, deps, s.opts)
			gate.release()
			// A spent token budget is a stop, not a failure: the runner has
			// already written the idle status and the message, so no retry
			// and no "error" status (agent.Runner contract).
			if err == nil || ctx.Err() != nil || isBudgetStop(err) {
				return
			}
			s.log.Warn("room: agent run returned error", "room", roomID, "attempt", attempt, "err", err)
			ectx, ecancel := context.WithTimeout(s.ctx, 5*time.Second)
			_ = s.agentEmit(ectx, roomID, events.AgentStatus, events.AgentStatusPayload{State: "error", Detail: truncateDetail(err.Error())})
			ecancel()
			if attempt >= maxAgentAttempts {
				s.log.Error("room: agent gave up after repeated failures", "room", roomID, "attempts", attempt)
				s.releaseAgent(st, run, cancel)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
		}
	}()
}

// runGuarded calls the agent runner and turns a panic into an error, so a bug
// in one room's run neither takes the server down nor leaves its slot held.
func (s *Service) runGuarded(ctx context.Context, roomID string, alert agent.Alert, deps agent.Deps, opts agent.Options) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("room: agent run panicked", "room", roomID, "panic", r)
			err = fmt.Errorf("room: agent run panicked: %v", r)
		}
	}()
	return s.run(ctx, alert, deps, opts)
}

// isBudgetStop reports whether a run ended because the room's token budget is
// spent (an error wrapping *agent.BudgetError).
func isBudgetStop(err error) bool {
	var be *agent.BudgetError
	return errors.As(err, &be)
}

// releaseAgent frees the room's agent slot when its supervisor gives up, so the
// next steer or @agent comment can start a new run. A supervisor whose slot has
// already been taken by a newer run (run is stale) releases nothing. cancel is
// the supervisor's own context, called to free it either way.
func (s *Service) releaseAgent(st *roomState, run uint64, cancel context.CancelFunc) {
	s.mu.Lock()
	if st.agentRun == run {
		st.agentCancel = nil
	}
	s.mu.Unlock()
	cancel()
}

func truncateDetail(s string) string {
	const max = 500
	if len(s) <= max {
		return s
	}
	cut := max
	// Do not split a UTF-8 rune.
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut] + "…"
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
