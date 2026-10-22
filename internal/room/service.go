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
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

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
	StackFor func(pipelineID string) PipelineStack
	// ActionCatalogue, when set, lists every write action for GET /api/actions
	// across every pipeline's client set (the union of every stack's Actions,
	// in stack order). AllActions deduplicates it by name. Nil falls back to
	// Actions.All(); with StackFor set it must be set too (cmd/server does).
	ActionCatalogue func() []actions.Action
	Topology        topology.Builder
	Users           *Users
	Owners          *owners.Owners // pipeline definitions for actions.Scoped checks
	Run             agent.Runner
	WriteHandoff    agent.HandoffWriter
	// WritePostmortem writes the closing summary of a room that never handed
	// off after its fix (F2). Nil gives the events-only draft:
	// cmd/server leaves it nil for PIPEROOM_POSTMORTEM_MODEL=off.
	WritePostmortem agent.PostmortemWriter
	// Triage, when set, runs the triage pass (F6) for every room opened in
	// this process, before its agent waits for a slot; not for shadow rooms
	// or rooms restored by Load. TriagePassTimeout is the pass's deadline
	// (agent.Endpoint.FirstLook); the agent waits for the pass at most that
	// plus 5 s. 0 means the default.
	Triage            agent.Triager
	TriagePassTimeout time.Duration
	Options           agent.Options
	// ShadowOptions are the agent options of a shadow room's run
	// (docs/design/shadow.md section 3.3): the same model, fewer turns and a
	// lower token budget (config.ParseShadow).
	ShadowOptions agent.Options
	// ShadowGrace is how long a shadow run may keep going after its room
	// closes; 0 cancels it at close like a live run.
	ShadowGrace time.Duration
	Logger      *slog.Logger
	Now         func() time.Time
	// LeaseTicker, when set, replaces the ticker of the driver lease expiry
	// check (lease_expiry.go): it returns the tick channel for period d and a
	// stop func. Tests pass a channel they feed by hand, together with Now,
	// so a check runs exactly when and at the time the test says. Nil uses
	// time.NewTicker.
	LeaseTicker func(d time.Duration) (<-chan time.Time, func())

	// MaxAgents caps agent runs working at the same time on this replica,
	// across all its rooms; a room over the cap waits in a queue that is
	// round-robin across pipelines and FIFO within one. 0 or less means
	// DefaultMaxAgents.
	MaxAgents int
	// ReplicaID names this process in room leases and executor claims when
	// the store is shared (a store.Coordinator; docs/design/replicas.md).
	// cmd/server passes PIPEROOM_REPLICA_ID plus a per-process suffix; empty
	// gets a random id. Unused with a single-process store.
	ReplicaID string
	// MaxAgentsGlobal caps working agent runs across every replica that
	// shares the store (PIPEROOM_MAX_AGENTS_GLOBAL), counted from the room
	// leases. 0 = off. Unused with a single-process store.
	MaxAgentsGlobal int
	// ReplicaTimings shortens the lease and sweep periods (tests); the zero
	// value is the production timing.
	ReplicaTimings ReplicaTimings
	// ResumeOnTakeover is PIPEROOM_RESUME_ON_TAKEOVER: a replica that takes
	// over the lease of a replica that stopped resumes an agent that was
	// working (at most 3 times per room per hour) instead of pausing it.
	// Off by default.
	ResumeOnTakeover bool
	// ExportMaxEvents caps the events one audit export may contain; a larger
	// room answers 413 export_too_large. Zero means DefaultExportMaxEvents.
	ExportMaxEvents int64

	// Sessions lets LookupPerson find a person who is not in users.yaml by their
	// newest live session (design section 2.1). Nil: users.yaml only.
	// SessionLive is the rule that decides whether a session is live: the
	// server's auth.Settings.Liveness, so a session no request may use is
	// never listed or given control (#417).
	Sessions    auth.Store
	SessionLive auth.Liveness
	// ViewerGroups are the IdP groups that let a person in as a viewer
	// (PIPEROOM_AUTH_VIEWER_GROUPS).
	ViewerGroups []string
	// SoloUser is PIPEROOM_SOLO_USER: that user may approve every layer, with a
	// typed confirm and the grant "solo". The caller has already run
	// CheckSoloUser.
	SoloUser string
	// WriteModeOff is PIPEROOM_WRITE_MODE=off (config.ParseWriteMode, P0 item
	// 1): proposals are still created, previewed and approved, but a fully
	// approved one is never queued for the executor, so nothing runs and no
	// action.executed is written; the room view says why
	// (open_proposals[].blocked). The zero value is write mode on, the
	// behaviour before the switch, so callers that predate it keep running
	// fixes; cmd/server always sets it from the config, whose default is off.
	WriteModeOff bool
	// AutoApproveDelay is PIPEROOM_AUTO_APPROVE_DELAY
	// (config.ParseAutoApproveDelay, P0 item 2): how long a proposal whose
	// every step is on its pipeline's owners.yaml auto_approve: list waits,
	// cancellable, before the policy approves it (autoapprove.go). 0 turns
	// the policy off, which is also the zero value: callers that predate it
	// never auto-approve. cmd/server has already refused a schema_* entry and
	// an entry that names no action of the proposal registry.
	AutoApproveDelay time.Duration
	// AutoApproveTicker, when set, replaces the ticker of the policy loop
	// (autoapprove.go), like LeaseTicker. Nil uses time.NewTicker.
	AutoApproveTicker func(d time.Duration) (<-chan time.Time, func())

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
	// and after the room's lock is released. It runs on the closing request, before
	// CloseRoom returns, with the service lifetime ctx, so a caller that closes
	// a room and then asks the hook's owner about it sees the hook already ran.
	// The hook must be fast and must not call back into a method that waits for
	// the room to settle. dedupKey is the key the room was opened under, as the
	// alert source gave it (alertin.Alert.DedupKey, for example "watcher:orders"),
	// without the pipeline prefix; it is "" for a room opened by hand or by an
	// alert with no key. Nil means no-op.
	OnRoomClosed func(ctx context.Context, roomID, pipelineID, dedupKey string)

	// OnRoomDeleted, when set, is called once per room this replica forgets
	// because it is gone from the store: deleted here (DeleteRoom), purged
	// here by retention, or deleted by another replica and noticed here (the
	// follower, a re-list, a command's re-read). deleted is the room.deleted
	// frame its WebSockets get last (docs/api.md, WebSocket), or the zero
	// Event when the room's audit row cannot be read (then only the close goes
	// out). It never runs under a room's lock and must return promptly;
	// cmd/server points it at the WebSocket gateway's CloseRoom, which is
	// idempotent, so a room reported twice closes its sockets once. Nil means
	// no-op.
	OnRoomDeleted func(roomID string, deleted events.Event)

	// OnActionExecuted, when set, is called once per action.executed this
	// replica folds into a cached room, whichever replica appended it, with
	// the room's pipeline, the proposal and the step that ran (its action
	// and frozen params), OK or not. The watcher's config_drift (X3b) uses
	// it to move a connector's approved config after an approved fix, and
	// the stored config version (X5c) names the proposal. Each call runs in
	// its own goroutine with the service lifetime ctx, never under a room's
	// lock; it must return promptly. Nil means no-op.
	OnActionExecuted func(ctx context.Context, pipelineID, proposalID string, step events.ProposalStep)
}

type roomState struct {
	// mu is the room's lock (roomlock.go). It guards every field below but
	// pub, which is atomic, and dedupKey, which is the registry's.
	mu sync.Mutex
	// gone is set when the room leaves the cache (evicted, deleted, or a
	// create that failed): whoever still holds the pointer finds it here.
	gone bool
	// pub is the snapshot as of the last unlock (unlockRoom): an
	// immutable copy that reads use without a lock. nil until the room is
	// first published.
	pub atomic.Pointer[RoomSnapshot]
	// changed says snap changed since it was last published.
	changed bool
	// stale is set when an append of ours landed after events of other
	// replicas that could not be read (fillGapLocked): the snapshot skipped
	// them, so the follower rebuilds it from the log (rebuildStale). Atomic
	// so the follower can check it without the lock. staleGaps are the
	// skipped seqs, whose human input the rebuild feeds to the inbox.
	stale     atomic.Bool
	staleGaps seqGaps
	// rebuildQueued is set while a rebuild of the stale room waits on its
	// timer (rebuildLater), so retries never pile up per notify.
	rebuildQueued atomic.Bool

	snap RoomSnapshot
	// leaseLogFullFor is the logged lease whose control.lease_expired a full
	// room log refused (lease_expiry.go); the check skips it from then on.
	leaseLogFullFor time.Time
	inbox           *inbox
	agentCancel     context.CancelFunc
	agentRun        uint64             // generation of the latest agent start; see releaseAgent
	execCancel      context.CancelFunc // cancels the room's executor on close
	exec            *execQueue         // nil until the first proposal is approved
	// dedupKey is the alert index key of an alert room; "" otherwise. It is
	// guarded by s.reg, not by mu: the alert index reads the keys of other
	// rooms (releaseAlertKeyLocked). Read it with dedupKeyOf.
	dedupKey string
	// alertRaw is room.created.alert, the raw alert every agent run of the
	// room starts from; nil for a room opened by hand.
	alertRaw json.RawMessage
	// openingFor is the pipeline of a room opened in this process, set before
	// it is cached and never changed: a registry mirror that tells lookups by
	// pipeline about a room whose create has not published it yet
	// (OpenRoomForPipeline). "" for a room loaded from the log, which is
	// published from the start.
	openingFor string
	// firstLook is set on a room opened in this process until its agent
	// starts: only such a room gets the triage pass (service_triage.go).
	firstLook bool

	// Shadow run state (shadow.go). shadowActive is true from the start of
	// the shadow supervisor until it has written the run's final status.
	// shadowInAttempt is true only while an attempt runs (runGuarded), so a
	// close gives grace to a run that is mid-investigation and cancels a run
	// that is queued or backing off. graceTimer cancels a run that is still
	// going PIPEROOM_SHADOW_GRACE after the close.
	shadowActive    bool
	shadowInAttempt bool
	graceTimer      *time.Timer

	// Multi-replica state (replicas.go). leased: this replica owns the room's
	// agent lease, so it runs the agent and feeds its inbox; it is changed
	// only by setLeased, which mirrors it in leasedHint for the lease loops,
	// which read it without the room's lock. leaseValid (Unix ns) is the local
	// deadline after which the agent stops unless a renew confirmed the lease;
	// the renew loop moves it without the lock, so a room stuck in a slow
	// call keeps a lease the store still confirms. pending is the human input
	// logged after the agent's last own event, which primes the inbox of a
	// replica that becomes the owner.
	leased     bool
	leasedHint atomic.Bool
	leaseValid atomic.Int64
	pending    []agent.HumanInput

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
	store           store.Store
	actions         *actions.Registry
	tools           *tools.Registry
	stackFor        func(pipelineID string) PipelineStack
	actionCatalogue func() []actions.Action
	topo            topology.Builder
	users           *Users
	owners          *owners.Owners
	run             agent.Runner
	writeHandoff    agent.HandoffWriter
	// writePostmortem is nil for an events-only draft (Deps.WritePostmortem).
	writePostmortem agent.PostmortemWriter
	triage          agent.Triager
	triageWait      time.Duration
	triageSem       chan struct{} // maxTriagePasses
	opts            agent.Options
	shadowOpts      agent.Options
	shadowGrace     time.Duration
	shadow          store.ShadowStore // nil when the store cannot hold shadow runs
	log             *slog.Logger
	now             func() time.Time
	onRoomOpened    func(ctx context.Context, roomID string)
	onRoomClosed    func(ctx context.Context, roomID, pipelineID, dedupKey string)
	onRoomDeleted   func(roomID string, deleted events.Event)
	onActionExec    func(ctx context.Context, pipelineID, proposalID string, step events.ProposalStep)
	slots           *agentSlots // global cap on concurrent agent runs
	exportMax       int64
	sessions        auth.Store
	sessionLive     auth.Liveness
	viewerGroups    map[string]struct{}
	soloUser        string
	writeOff        bool // Deps.WriteModeOff
	limits          config.Limits
	// Auto-approve policy (autoapprove.go). autoDelay is
	// Deps.AutoApproveDelay (0 = off). autoTickDone, when set (tests only),
	// runs after each check of the loop. autoSkip remembers what the loop
	// logged about each due proposal it did not approve, so each kind is
	// logged once (notePolicyOnce).
	autoDelay    time.Duration
	autoTicker   func(d time.Duration) (<-chan time.Time, func())
	autoOnce     sync.Once
	autoTickDone func()
	autoSkipMu   sync.Mutex
	autoSkip     map[string]map[string]bool // proposal -> notePolicyOnce kinds logged
	// deleteMu serialises DeleteRoom calls: deletions are rare, and one at a
	// time makes a concurrent repeat find the first one's audit row.
	deleteMu sync.Mutex

	// Multi-replica coordination (replicas.go). coord is nil with a store that
	// belongs to one process (memstore); then nothing below runs.
	coord   store.Coordinator
	replica string
	global  globalSlots // nil: no cap across replicas
	timing  ReplicaTimings
	follow  *follower
	// resumeOnTakeover is Deps.ResumeOnTakeover.
	resumeOnTakeover bool
	coordOnce        sync.Once

	// Driver lease expiry (lease_expiry.go). leaseTickDone, when set (tests
	// only), runs after each check of the loop.
	leaseTicker   func(d time.Duration) (<-chan time.Time, func())
	leaseOnce     sync.Once
	leaseTickDone func()

	// retryAfter, when set (tests only), replaces time.After for the wait
	// between failed agent and shadow attempts, so tests need not sit out the
	// production backoff. globalRetry is globalSlotRetry; tests shorten it.
	retryAfter  func(d time.Duration) <-chan time.Time
	globalRetry time.Duration

	// ctx is the service lifetime: every goroutine the service starts (agent
	// supervisors, executor queues) derives from it. Close cancels it and
	// waits.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// reg guards the room registry (registry.go): the cache, the loads in
	// flight, the alert index and closed. It is a leaf lock, never held
	// across I/O, a hook or another lock. Each room has its own lock
	// (roomlock.go).
	reg     sync.Mutex
	closed  bool
	rooms   map[string]*roomState
	loading map[string]*roomLoad
	// alertRooms maps the dedup key of each open webhook room to that room, so
	// a repeat of the same alert joins the room instead of opening another.
	alertRooms map[string]string
	// alertLocks serialise the opens of one alert key in this process
	// (lockAlertKey): the check for an open room and the create must be one
	// step, and no service-wide lock makes them so. Striped by key hash, so
	// unrelated keys rarely wait for each other. Each stripe is a channel
	// of capacity one used as a semaphore, so a caller can stop waiting when
	// its context ends (a slow open must not hold up its stripe's other keys
	// without bound).
	alertLocks [alertLockStripes]chan struct{}

	// list pushes rooms-list changes to SubscribeRooms (listhub.go).
	list *listHub
	// statsCache serves GET /api/pipelines/{pid}/agent-stats (agentstats.go).
	statsCache agentStatsCache
	// approvals counts approval.given by grant for /metrics (metrics.go).
	approvals approvalCounts
	// soleMembers counts the sole-member grants for /metrics
	// (service_solemember.go).
	soleMembers soleMemberCounter
	// runners is the setup runner registry: pairing codes and the attached
	// runners (runners.go; U wave).
	runners *RunnerRegistry
	// runnerExec dispatches OpRequests to the attached runner and reconciles
	// its results and Resume frames (runner_exec.go; U1-5a).
	runnerExec *RunnerExecutor
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
		store:           d.Store,
		actions:         acts,
		tools:           tls,
		stackFor:        d.StackFor,
		actionCatalogue: d.ActionCatalogue,
		topo:            topo,
		users:           users,
		owners:          d.Owners,
		run:             run,
		writeHandoff:    wh,
		writePostmortem: d.WritePostmortem,
		triage:          d.Triage,
		triageWait:      triageWaitFor(d.TriagePassTimeout),
		triageSem:       make(chan struct{}, maxTriagePasses),
		globalRetry:     globalSlotRetry,
		opts:            d.Options,
		shadowOpts:      d.ShadowOptions,
		shadowGrace:     d.ShadowGrace,
		shadow:          shadowStore,
		log:             log,
		now:             now,
		onRoomOpened:    d.OnRoomOpened,
		onRoomClosed:    d.OnRoomClosed,
		onRoomDeleted:   d.OnRoomDeleted,
		onActionExec:    d.OnActionExecuted,
		slots:           newAgentSlots(d.MaxAgents),
		exportMax:       d.ExportMaxEvents,
		sessions:        d.Sessions,
		sessionLive:     d.SessionLive,
		viewerGroups:    groupSet(d.ViewerGroups),
		soloUser:        d.SoloUser,
		writeOff:        d.WriteModeOff,
		limits:          d.Limits,
		autoDelay:       d.AutoApproveDelay,
		autoTicker:      d.AutoApproveTicker,
		leaseTicker:     d.LeaseTicker,
		ctx:             ctx,
		cancel:          cancel,
		rooms:           map[string]*roomState{},
		loading:         map[string]*roomLoad{},
		alertRooms:      map[string]string{},
		alertLocks:      newAlertLocks(),
		list:            newListHub(),
	}
	if coord, ok := d.Store.(store.Coordinator); ok {
		s.coord = coord
		s.replica = d.ReplicaID
		if s.replica == "" {
			s.replica = NewID("replica_")
		}
		s.timing = d.ReplicaTimings.withDefaults()
		s.follow = newFollower()
		s.resumeOnTakeover = d.ResumeOnTakeover
		if d.MaxAgentsGlobal > 0 {
			s.global = coordSlots{s: s, max: d.MaxAgentsGlobal}
		}
	}
	s.runners = NewRunnerRegistry(now, s.EmitSetup, log)
	s.runnerExec = NewRunnerExecutor(s.runners, RunnerResults{
		Result:  s.applyRunnerResult,
		Unknown: s.applyRunnerUnknown,
	}, log)
	return s
}

// Close cancels the service lifetime context and waits for every goroutine it
// started (agent supervisors, executor queues) to stop. It is idempotent and
// returns promptly once all of them have exited.
func (s *Service) Close() {
	// closed is set under reg, where addWorker checks it, so no goroutine is
	// added to wg once Wait may have started.
	s.reg.Lock()
	if s.closed {
		s.reg.Unlock()
		return
	}
	s.closed = true
	s.reg.Unlock()
	// Before the cancel: cancelling the running agents frees their slots, and
	// a slot granted then would start a queued room on a closing service.
	s.slots.shutdown()
	s.cancel()
	s.wg.Wait()
	if s.coord != nil {
		// Every agent here has stopped: hand the rooms over now instead of
		// letting their leases run out (docs/design/replicas.md section 4.1).
		ctx, cancel := s.coordCtx()
		defer cancel()
		if err := s.coord.ExpireRoomLeases(ctx, s.replica); err != nil {
			s.log.Warn("room: hand over agent leases at shutdown failed; they expire on their own", "err", err)
		}
	}
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
//
// With a shared store (docs/design/replicas.md section 4.3) the agent leases
// decide instead: a room whose lease another replica holds is left alone, and
// a room whose lease expired is taken over by the coordination loop Load
// starts at the end (an agent that was working resumes, an idle one gets the
// pause notice). Only an open room that shows a working agent but has no
// lease at all gets the restart notice here.
func (s *Service) Load(ctx context.Context) error {
	if s.shadow == nil && s.owners.AnyShadow() {
		return fmt.Errorf("room: load: owners.yaml turns shadow mode on, but the store cannot hold shadow runs")
	}
	if err := s.ensureAllLoaded(ctx, true); err != nil {
		return err
	}
	s.sweepShadow(ctx)
	s.sweepPostmortems()
	s.startCoordination()
	s.startLeaseExpiry()
	s.startAutoApprove()
	return nil
}

// ensureAllLoaded rebuilds every room not yet cached, and waits (bounded by
// ctx) for the loads other callers have in flight, so that on return every
// room of the store row list is cached: the passes that call it (Load, the
// open-room limit) must not leave a room out. restarted is true only for the
// start-up Load: those open rooms are marked as idle after a server restart.
func (s *Service) ensureAllLoaded(ctx context.Context, restarted bool) error {
	rooms, err := s.store.ListRooms(ctx)
	if err != nil {
		return fmt.Errorf("room: load: %w", err)
	}
	return s.loadRows(ctx, rooms, restarted, true)
}

// loadRows rebuilds the rooms among rows that are not cached yet (see
// ensureAllLoaded), each outside every lock (loadRoom); a room this call put
// into the cache is then locked for what a load writes into it. With wait, a
// load another caller has in flight is waited for (awaitRoom: no room lock,
// bounded by ctx); without, it is skipped, and that load caches the room.
func (s *Service) loadRows(ctx context.Context, rooms []store.Room, restarted, wait bool) error {
	var leases map[string]store.RoomLease
	if s.coord != nil && restarted {
		var err error
		if leases, err = s.coord.RoomLeases(ctx); err != nil {
			return fmt.Errorf("room: load: %w", err)
		}
	}
	for _, r := range rooms {
		if s.cached(r.ID) != nil {
			continue
		}
		if r.LastSeq == 0 {
			// A row with no events is a leftover of a room.created append that
			// failed (or a crash before it), or a room another replica is
			// opening right now (the follower loads it at its first event).
			// It is not a room yet.
			if restarted {
				s.log.Warn("room: skipping room row with no events", "room", r.ID)
			}
			continue
		}
		// Without wait, a load in flight elsewhere is skipped: one slow room
		// must not hold up ListRooms, and that load caches the room. A pass
		// that must see every room waits for it instead; the load runs with
		// no lock held, so this waits for that room's read only.
		var st *roomState
		var fresh bool
		var err error
		if wait {
			st, fresh, err = s.awaitRoom(ctx, r.ID)
		} else {
			st, fresh, err = s.loadRoom(ctx, r.ID, false)
		}
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("room: load: %w", ctx.Err())
			}
			s.log.Warn("room: rebuild failed", "room", r.ID, "err", err)
			continue
		}
		if !fresh {
			continue // a command or the follower loaded it meanwhile
		}
		// With a shared store a proposal may be running on another replica
		// right now; the stale-claim sweep decides instead. The store reads
		// are made before the lock (staleProposals), the writes after it.
		// With write mode off a fully approved proposal is blocked, not
		// interrupted: it stays open and the room view says why. A
		// single-process store does not outlive its process, so no proposal
		// a write-on run left half done can reach here.
		var stale []staleProposal
		if s.coord == nil && !s.writeOff {
			if p := st.pub.Load(); p != nil && p.Status == "open" {
				stale = s.staleProposals(ctx, r.ID, *p)
			}
		}
		var interrupted []staleProposal
		s.lockState(st)
		if snap := st.snap; !st.gone && snap.Status == "open" {
			interrupted = s.interruptStaleProposalsLocked(ctx, r.ID, st, stale)
			// A shadow room never resumes its agent, so the "steer to
			// resume" notice would be wrong there (the sweep in Load marks
			// its run interrupted instead).
			if restarted && !snap.Shadow && s.restartNoticeDue(r.ID, st, leases) {
				if err := s.noteRestartLocked(ctx, r.ID, st); err != nil {
					s.log.Warn("room: restart notice failed", "room", r.ID, "err", err)
				}
			}
		}
		s.unlockRoom(st)
		s.finishInterrupted(interrupted)
	}
	return nil
}

// restartNoticeDue reports whether Load writes the restart notice into an
// open room: always with a single-process store; with a shared one only when
// the room shows a working agent that holds no lease anywhere (an agent with a
// lease is running elsewhere or is taken over by the coordination loop).
func (s *Service) restartNoticeDue(roomID string, st *roomState, leases map[string]store.RoomLease) bool {
	if s.coord == nil {
		return true
	}
	if _, held := leases[roomID]; held {
		return false
	}
	return agentWasWorking(st)
}

// restartDetail is the agent status detail a restart writes. A room already
// showing it is not marked again, so repeated restarts do not pile up notices.
const restartDetail = "server restarted"

// noteRestartLocked tells the room that the server restarted: the agent is idle
// and paused until someone steers or mentions it. It writes agent.status idle and
// a system message to the log, unless the room already shows that state. Caller
// holds st's lock.
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
// a room whose agent was already started is left alone. Caller holds st's lock.
func (s *Service) ensureAgentLocked(roomID string, st *roomState) {
	// A shadow run is one-shot: steers, suggestions and @agent comments are
	// logged but never start or restart it (docs/design/shadow.md 3.3).
	// A full log never gets its agent back: every agent event would be refused.
	if st.snap.Shadow || st.agentCancel != nil || !s.roomOpen(st) || st.snap.LogCap == LogCapFull || st.deleting {
		return
	}
	s.startAgentLocked(roomID, st.snap.Title, st.snap.PipelineID, st.alertRaw, st)
}

// staleProposal is a proposal of a room being loaded that was fully
// approved but never finished before this (re)start (review item 6), with the
// index of the step that was in flight (interruptedStep).
type staleProposal struct {
	p    Proposal
	next int
	// writeBlocked: the row is store.ProposalStatusBlocked, approved while
	// write mode was off; it never ran.
	writeBlocked bool
}

// staleProposals finds the stale proposals of a room just loaded, from its
// snapshot snap: fully approved, still open in the snapshot, and still open
// in the store. It reads the store, so it runs before the room's lock is
// taken (interruptStaleProposalsLocked checks again under it).
func (s *Service) staleProposals(ctx context.Context, roomID string, snap RoomSnapshot) []staleProposal {
	var out []staleProposal
	for _, p := range snap.OpenProposals {
		if !fullyApproved(p) {
			continue
		}
		// The snapshot is not the whole truth: if the last action.executed
		// append failed but the store status write succeeded, the DB already
		// says executed/failed/rejected while the snapshot still shows the
		// proposal open. Skip those — do not re-mark a finished proposal failed
		// (review item 6).
		blocked := false
		if row, err := s.store.GetProposal(ctx, p.ProposalID); err == nil {
			if blocked = row.Status == store.ProposalStatusBlocked; !blocked && row.Status != "open" {
				continue
			}
		}
		out = append(out, staleProposal{p: p, next: s.interruptedStep(ctx, roomID, p), writeBlocked: blocked})
	}
	return out
}

// interruptStaleProposalsLocked records the outcome of the stale proposals
// found by staleProposals: without it they would stay "open" forever in the
// snapshot. For each one still open and fully approved in the snapshot it
// emits one action.executed with OK=false naming the step that was in
// flight, and returns those proposals; the caller then finishes their store
// steps and marks them failed (finishInterrupted) after releasing the lock.
// Caller holds st's lock.
func (s *Service) interruptStaleProposalsLocked(ctx context.Context, roomID string, st *roomState, stale []staleProposal) []staleProposal {
	var out []staleProposal
	for _, sp := range stale {
		// Something may have decided the proposal between the read and the
		// lock: only a proposal that is still open and fully approved here
		// is interrupted.
		p, ok := openProposal(st.snap, sp.p.ProposalID)
		if !ok || !fullyApproved(p) {
			continue
		}
		output := "proposal was interrupted by a service restart before it finished; create a new proposal to retry"
		if sp.writeBlocked {
			output = writeOffNotRunOutput
		}
		if _, err := s.emitLocked(ctx, st, roomID, events.ActionExecuted, executorActor, events.ActionExecutedPayload{
			ProposalID: p.ProposalID,
			StepIndex:  sp.next,
			OK:         false,
			ParamsHash: p.ParamsHash,
			Output:     output,
		}); err != nil {
			s.log.Warn("room: emit interruption failed", "proposal", p.ProposalID, "err", err)
		}
		// Marked failed in the store even when the append failed, so a
		// later load does not interrupt it again (staleProposals).
		out = append(out, staleProposal{p: p, next: sp.next})
	}
	return out
}

// finishInterrupted finishes every claimed store step of the proposals
// interruptStaleProposalsLocked interrupted (best effort) and sets their
// status to "failed". Store writes only: the caller holds no room lock.
func (s *Service) finishInterrupted(stale []staleProposal) {
	for _, sp := range stale {
		for i := range sp.p.Steps {
			s.finishClaimedStep(sp.p.ProposalID, i, "interrupted by a service restart")
		}
		s.setProposalStatus(sp.p.ProposalID, "failed")
	}
}

// openProposal returns the open proposal id of snap.
func openProposal(snap RoomSnapshot, id string) (Proposal, bool) {
	for _, p := range snap.OpenProposals {
		if p.ProposalID == id {
			return p, true
		}
	}
	return Proposal{}, false
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

// loadedRoom is what rebuild reads from the store for a room that enters the
// cache.
type loadedRoom struct {
	snap RoomSnapshot
	// pending is the human input logged after the agent's last own event
	// (trackPending).
	pending []agent.HumanInput
	// logBytes is the store's byte count of the log (loadUsage).
	logBytes int64
	// dedupKey is the alert index key from room.created (alertIndexKeyOf); ""
	// for a room no alert opened.
	dedupKey string
	// alertRaw is room.created.alert.
	alertRaw json.RawMessage
}

// state returns a new cache entry for the loaded room, published.
func (l loadedRoom) state() *roomState {
	st := &roomState{
		snap: l.snap, inbox: newRoomInbox(l.snap.Shadow), pending: l.pending,
		logBytes: l.logBytes, alertRaw: l.alertRaw,
	}
	st.publish()
	return st
}

// rebuild folds the whole log into a snapshot, then refreshes the driver lease
// from the rooms row (renewals are persisted there, not in the log) and reads
// the log's byte count. The room.created it folds gives the alert key and the
// raw alert, so nothing reads that event again.
func (s *Service) rebuild(ctx context.Context, roomID string) (loadedRoom, error) {
	return s.rebuildWith(ctx, roomID, nil)
}

// rebuildWith is rebuild that also hands every event read to visit (nil for
// none), in log order.
func (s *Service) rebuildWith(ctx context.Context, roomID string, visit func(events.Event)) (loadedRoom, error) {
	// The byte count is read before the log: an event appended meanwhile is
	// then folded (and counted by foldLocked) but not in the count, which can
	// trail the store, never count an event twice.
	logBytes := s.loadUsage(ctx, roomID)
	st := &roomState{snap: NewSnapshot()}
	var created *events.RoomCreatedPayload
	after := int64(0)
	for {
		evs, err := s.store.Since(ctx, roomID, after, 1000)
		if err != nil {
			return loadedRoom{}, err
		}
		if len(evs) == 0 {
			break
		}
		for _, e := range evs {
			if after == 0 && e.Type == events.RoomCreated {
				if p, ok := decode[events.RoomCreatedPayload](e); ok {
					created = &p
				}
			}
			st.snap = Apply(st.snap, e)
			trackPending(st, e)
			if visit != nil {
				visit(e)
			}
			after = e.Seq
		}
		if len(evs) < 1000 {
			break
		}
	}
	snap := st.snap
	if snap.ID == "" {
		snap.ID = roomID
	}
	s.setDeadline(&snap)
	if snap.Status == "closed" && snap.Postmortem.Status == PostmortemDrafting && snap.ClosedAt != nil && s.now().Sub(*snap.ClosedAt) > postmortemRestartWindow {
		// A room closed before drafts existed, or whose drafter died long ago:
		// it is not drafting any more (the GET still renders events-only).
		snap.Postmortem = PostmortemState{Status: PostmortemNone}
	}
	if row, err := s.store.GetRoom(ctx, roomID); err == nil {
		s.repairClosedRow(ctx, row, &snap)
		if row.DriverID != "" && snap.Driver != nil && snap.Driver.ID == row.DriverID && row.DriverLeaseUntil != nil && row.DriverLeaseUntil.After(snap.Driver.LeaseUntil) {
			// docs/api.md: times are RFC 3339 UTC; a persisted lease read back
			// from Postgres may carry the session's local zone.
			snap.Driver.LeaseUntil = row.DriverLeaseUntil.UTC()
		}
	}
	l := loadedRoom{snap: snap, pending: st.pending, logBytes: logBytes}
	if created != nil {
		l.dedupKey = alertIndexKeyOf(*created, snap.PipelineID)
		l.alertRaw = created.Alert
	}
	return l, nil
}

// emitLocked appends one event, folds it into the cached snapshot and feeds
// the agent inbox. In a shadow room every event of the agent actor goes to the
// shadow log instead (docs/design/shadow.md 3.2): it is not folded and not fed,
// so no agent text, status or error reaches the room log, the WebSocket, the
// Slack mirror or the export before the reveal. This is the one place that
// decides it. With a shared store, events other replicas appended since the
// last fold are folded first, so the snapshot never skips a seq. Caller holds
// st's lock.
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
	if s.coord != nil && e.Seq > st.snap.LastSeq+1 {
		s.fillGapLocked(ctx, roomID, st, e.Seq)
	}
	s.foldLocked(ctx, roomID, st, e, false)
	// Only the replica that wrote the event decides the cap warning, so the
	// warning is written once.
	s.checkCapLocked(ctx, st, roomID)
	return e, nil
}

// fillGapLocked folds the events between the snapshot and seq (exclusive)
// that another replica appended. If they still cannot be read, the room is
// marked stale and rebuilt from the log once the store answers again
// (rebuildStale). Caller holds st's lock.
//
// The read runs on a context detached from the caller's: our event is already
// stored and is folded next whatever happens here, and folding it over a gap
// skips the missing events for good (the follower and refresh see the
// snapshot at our seq). A request whose client left right after the append
// must not be the reason another replica's control.granted or approval is
// lost on this one. One retry covers a dropped connection.
func (s *Service) fillGapLocked(ctx context.Context, roomID string, st *roomState, seq int64) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), coordTimeout)
	defer cancel()
	retried := false
	for st.snap.LastSeq+1 < seq {
		evs, err := s.store.Since(ctx, roomID, st.snap.LastSeq, int(min(seq-1-st.snap.LastSeq, 1000)))
		if (err != nil || len(evs) == 0) && !retried && ctx.Err() == nil {
			retried = true
			continue
		}
		if err != nil || len(evs) == 0 {
			s.log.Error("room: fold events from other replicas failed; the cached room is rebuilt from the log", "room", roomID, "have", st.snap.LastSeq, "want", seq-1, "err", err)
			s.markStaleLocked(roomID, st, seq)
			return
		}
		for _, ev := range evs {
			if ev.Seq != st.snap.LastSeq+1 || ev.Seq >= seq {
				s.log.Error("room: events from other replicas do not continue the cached room; it is rebuilt from the log", "room", roomID, "have", st.snap.LastSeq, "got", ev.Seq, "want", seq-1)
				s.markStaleLocked(roomID, st, seq)
				return
			}
			s.foldLocked(ctx, roomID, st, ev, true)
		}
	}
}

// markStaleLocked records that the seqs between the snapshot and seq
// (exclusive) were skipped, and queues the rebuild (rebuildStale). Caller
// holds st's lock.
func (s *Service) markStaleLocked(roomID string, st *roomState, seq int64) {
	if st.snap.LastSeq+1 >= seq {
		return
	}
	st.staleGaps = append(st.staleGaps, seqGap{after: st.snap.LastSeq, before: seq})
	st.stale.Store(true)
	// The follower tries at once (after this lock is released); a failure
	// there waits rebuildRetry.
	s.follow.noteAppend(roomID, seq)
}

// seqGap is the seqs strictly between after and before.
type seqGap struct{ after, before int64 }

// seqGaps are the seqs a stale snapshot skipped.
type seqGaps []seqGap

func (g seqGaps) contains(seq int64) bool {
	for _, r := range g {
		if seq > r.after && seq < r.before {
			return true
		}
	}
	return false
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
	events.AgentTriage:     true,
	// The verdict on an executed fix (internal/agent verify.go); the
	// post-mortem and the verification card read it.
	events.AgentVerification: true,
}

// outsideRun is the run number of an agent event written outside any agent run
// (the hand-off summary's usage): runs are numbered from 1, and such an event
// is not fenced by a run's lease, since any replica may write the hand-off.
const outsideRun uint64 = 0

// agentEmit is the agent Sink: the agent's events enter the same log and cache.
// Only agent.* event types are accepted, so the sink cannot forge
// control-plane events (control.granted, approval.given, ...).
func (s *Service) agentEmit(ctx context.Context, roomID string, run uint64, typ events.Type, payload any) error {
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
	st := s.lockCachedRoom(roomID)
	if st == nil {
		return fmt.Errorf("room: agent emit on unknown room %q", roomID)
	}
	defer s.unlockRoom(st)
	if run != outsideRun {
		if err := s.fenceLocked(roomID, st, run); err != nil {
			return err
		}
	}
	_, err = s.emitLocked(ctx, st, roomID, typ, events.AgentActor, payload)
	return err
}

// fenceLocked refuses a write of agent run `run` that may no longer speak for
// the room (docs/design/replicas.md 4.1): with a shared store only the lease
// owner's current run may write, and only before the lease's local deadline, so
// a run whose lease is lost (or about to be) writes nothing even before its
// context cancellation reaches it. Caller holds st's lock.
func (s *Service) fenceLocked(roomID string, st *roomState, run uint64) error {
	if s.coord == nil {
		return nil
	}
	switch {
	case !st.leased:
		return fmt.Errorf("room: agent write on %s: %w", roomID, store.ErrLeaseLost)
	case run != st.agentRun:
		return fmt.Errorf("room: agent write on %s from a replaced run: %w", roomID, store.ErrLeaseLost)
	case time.Now().After(st.leaseDeadline()):
		return fmt.Errorf("room: agent write on %s after the lease deadline: %w", roomID, store.ErrLeaseLost)
	}
	return nil
}

// executorEmit appends an event on behalf of the action executor.
func (s *Service) executorEmit(ctx context.Context, roomID string, typ events.Type, payload any) error {
	if p, ok := payload.(events.ActionExecutedPayload); ok {
		// Mask before the cap: a cap must not cut a secret in two.
		p.Output = s.capOutput(maskExecOutput(p.Output))
		payload = p
	}
	return s.emitAs(ctx, roomID, executorActor, typ, payload)
}

func (s *Service) emitAs(ctx context.Context, roomID string, actor events.Actor, typ events.Type, payload any) error {
	st := s.lockCachedRoom(roomID)
	if st == nil {
		return fmt.Errorf("room: emit on unknown room %q", roomID)
	}
	defer s.unlockRoom(st)
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
// 11). Caller holds st's lock.
func (s *Service) touchDriverLease(ctx context.Context, st *roomState, roomID string) {
	d := st.snap.Driver
	if d == nil {
		return
	}
	now := s.now().UTC()
	lease := now.Add(DriverLease)
	remaining := d.LeaseUntil.Sub(now)
	st.snap.Driver.LeaseUntil = lease
	st.changed = true
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
	return s.pipelineProposalsDisabled(st.snap.PipelineID)
}

// proposalsDisabledKind marks errProposalsDisabled (Error.kind).
var proposalsDisabledKind = errors.New("room: proposals disabled")

// errProposalsDisabled is the refusal of a proposal in a room whose pipeline
// is unknown.
func errProposalsDisabled(pipelineID string) *Error {
	e := validation(fmt.Sprintf("pipeline %q is unknown; proposals are disabled for this room", pipelineID))
	e.kind = proposalsDisabledKind
	return e
}

// isProposalsDisabled reports whether err is errProposalsDisabled.
func isProposalsDisabled(err error) bool { return errors.Is(err, proposalsDisabledKind) }

// pipelineProposalsDisabled is proposalsDisabled by pipeline id, for code that
// does not hold the room's lock.
func (s *Service) pipelineProposalsDisabled(pipelineID string) bool {
	return s.owners != nil && pipelineID != "" && !s.knownPipeline(pipelineID)
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
	// quiet is the pending alert update (pushQuiet): Drain hands it out,
	// Wait is never woken for it, and a newer one replaces it.
	quiet *agent.HumanInput
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

// pushQuiet sets the room's pending alert update, replacing one not yet
// drained. It does not wake a waiting agent: an alert update is new numbers
// for the next turn, not a reason for one (agent.HumanInput "alert_update").
func (in *inbox) pushQuiet(inp agent.HumanInput) {
	if in.closed {
		return
	}
	in.mu.Lock()
	in.quiet = &inp
	in.mu.Unlock()
}

// Drain returns all pending inputs without blocking, the pending alert update
// first.
func (in *inbox) Drain(ctx context.Context) ([]agent.HumanInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	out := in.items
	if in.quiet != nil {
		out = append([]agent.HumanInput{*in.quiet}, out...)
		in.quiet = nil
	}
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

// reset replaces the queued input with items (the input a new lease owner
// primes the inbox with, docs/design/replicas.md section 4.2) and wakes a
// waiter when there is any.
func (in *inbox) reset(items []agent.HumanInput) {
	if in.closed {
		return
	}
	in.mu.Lock()
	in.items = append([]agent.HumanInput(nil), items...)
	in.mu.Unlock()
	if len(items) > 0 {
		select {
		case in.wake <- struct{}{}:
		default:
		}
	}
}

// feedInbox pushes the agent input an event carries into the room's inbox.
// With a shared store only the replica that owns the room's agent lease feeds
// it: another replica's inbox would only collect input no run here reads.
// Caller holds st's lock.
func (s *Service) feedInbox(st *roomState, e events.Event) {
	if input := inputFor(e); input != nil {
		s.feedInput(st, *input)
	}
}

// feedInput pushes one human input to the room's agent inbox, when this
// replica runs the room's agent. Caller holds st's lock.
func (s *Service) feedInput(st *roomState, in agent.HumanInput) {
	if st.inbox == nil || st.snap.Shadow {
		return
	}
	if s.coord != nil && !st.leased {
		return
	}
	st.inbox.push(in)
}

// inputFor converts an event into the agent.HumanInput it carries, or nil.
func inputFor(e events.Event) *agent.HumanInput {
	switch e.Type {
	case events.SteerSent:
		p, ok := decode[events.SteerSentPayload](e)
		if !ok {
			return nil
		}
		// A forwarded suggestion is its author's words, not the driver's:
		// the agent wraps it as untrusted data under the suggester's name.
		// One logged before From existed has no author to name, so it keeps
		// the old steer shape.
		if p.SuggestionID != "" && p.From != nil {
			return &agent.HumanInput{Seq: e.Seq, Actor: *p.From, Kind: "suggestion", Text: p.Text}
		}
		return &agent.HumanInput{Seq: e.Seq, Actor: e.Actor, Kind: "steer", Text: p.Text}
	case events.CommentAdded:
		p, ok := decode[events.CommentAddedPayload](e)
		if !ok || !p.MentionsAgent {
			return nil
		}
		return &agent.HumanInput{Seq: e.Seq, Actor: e.Actor, Kind: "comment", StepID: p.StepID, CommentID: p.CommentID, Text: p.Text}
	case events.HypothesisVerdict:
		p, ok := decode[events.HypothesisVerdictPayload](e)
		if !ok {
			return nil
		}
		// Delivered like an @agent comment (no ids): a dispute must reach the
		// model and a confirm lets it move on to propose_fix.
		text := p.Verdict + " the current hypothesis"
		if p.Note != "" {
			text += ": " + p.Note
		}
		return &agent.HumanInput{Seq: e.Seq, Actor: e.Actor, Kind: "comment", Text: text}
	case events.ApprovalGiven:
		p, ok := decode[events.ApprovalGivenPayload](e)
		if !ok {
			return nil
		}
		approved := true
		return &agent.HumanInput{
			Seq: e.Seq, Actor: e.Actor, Kind: "approval_result",
			Text:       fmt.Sprintf("layer %q approved your proposal", p.Layer),
			ProposalID: p.ProposalID, Layer: p.Layer, Approved: &approved,
		}
	case events.ApprovalRejected:
		p, ok := decode[events.ApprovalRejectedPayload](e)
		if !ok {
			return nil
		}
		approved := false
		return &agent.HumanInput{
			Seq: e.Seq, Actor: e.Actor, Kind: "approval_result",
			Text:       fmt.Sprintf("layer %q rejected your proposal: %s", p.Layer, p.Reason),
			ProposalID: p.ProposalID, Layer: p.Layer, Approved: &approved,
		}
	case events.ActionExecuted:
		p, ok := decode[events.ActionExecutedPayload](e)
		if !ok {
			return nil
		}
		// OK tells the agent whether the step succeeded; it must not claim the
		// pipeline recovered from a failed step (agent.HumanInput.OK).
		// An unknown outcome is OK nil: the agent must claim neither
		// recovery nor failure (docs/design/replicas.md section 5).
		var okp *bool
		if !p.OutcomeUnknown {
			stepOK := p.OK
			okp = &stepOK
		}
		stepIndex := p.StepIndex
		return &agent.HumanInput{
			Seq: e.Seq, Actor: e.Actor, Kind: "action_result",
			Text: fmt.Sprintf("step %d: %s", p.StepIndex+1, p.Output),
			OK:   okp, ProposalID: p.ProposalID, StepIndex: &stepIndex,
		}
	}
	return nil
}

// ---- agent supervisor ----

// roomSink implements agent.Sink, appending agent events to the room log.
// run is the generation of the run it serves (fenceLocked).
type roomSink struct {
	s      *Service
	roomID string
	run    uint64
}

func (r roomSink) Emit(ctx context.Context, typ events.Type, payload any) error {
	return r.s.agentEmit(ctx, r.roomID, r.run, typ, payload)
}

// proposalCreator implements agent.Proposals for one run, fenced like its
// Sink.
type proposalCreator struct {
	s      *Service
	roomID string
	run    uint64
}

func (p proposalCreator) Create(ctx context.Context, payload events.ProposalCreatedPayload) (string, error) {
	id, _, err := p.s.createProposal(ctx, p.roomID, payload, events.AgentActor, func(st *roomState) error {
		return p.s.fenceLocked(p.roomID, st, p.run)
	})
	return id, err
}

// Agent auto-restart bounds (LEAD-ARCH round 4, arch should-fix): a failed
// agent run is retried with growing backoff so a transient error does not
// leave the room without an agent forever.
const (
	maxAgentAttempts  = 3
	agentRetryBackoff = 1 * time.Second // doubled after every failed attempt
)

// startAgentLocked starts the supervisor for an open room's agent. Caller holds
// st's lock. It does nothing after Close, so a shutting-down service never adds
// goroutines. The supervisor first waits for a global agent slot (agentSlots).
// The room counts as having a run from this call on, so more steers or
// mentions while it is queued do not queue it again.
func (s *Service) startAgentLocked(roomID, title, pipelineID string, raw json.RawMessage, st *roomState) {
	// The supervisor's place in wg is reserved first, so nothing below
	// happens (no lease taken, no run counted) once Close has begun.
	if !s.addWorker() {
		return
	}
	// With a shared store only the lease owner runs the room's agent; when
	// another replica owns it, that replica reads this room's input from the
	// log (docs/design/replicas.md section 4).
	if !s.claimLeaseLocked(roomID, st) {
		s.wg.Done()
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
	gate := &agentGate{s: s, st: st, roomID: roomID, pipeline: pipelineID}
	var firstLook <-chan agent.TriageResult
	if st.firstLook && s.triage != nil {
		// Before the slot: a room queued behind the cap still gets its
		// diagnosis. It runs once per room, never on a restart.
		firstLook = s.startTriageLocked(ctx, roomID, run, s.newAlert(roomID, title, pipelineID, raw))
	}
	st.firstLook = false
	go func() {
		defer s.wg.Done()
		// However the supervisor ends, its slot or queue entry goes back.
		defer gate.release()
		alert := s.newAlert(roomID, title, pipelineID, raw)
		deps := agent.Deps{
			Tools:     s.toolsFor(pipelineID),
			Actions:   s.actionsFor(pipelineID),
			Inbox:     gatedInbox{in: st.inbox, g: gate, runCtx: ctx},
			Sink:      roomSink{s: s, roomID: roomID, run: run},
			Proposals: proposalCreator{s: s, roomID: roomID, run: run},
			Logger:    s.log,
		}
		backoff := agentRetryBackoff
		mapped := false
		for attempt := 1; ; attempt++ {
			if err := gate.acquire(ctx); err != nil {
				if ctx.Err() == nil && !s.isClosed() {
					// The slot could not be had for another reason (the
					// lease was lost while queued): give the room's agent
					// up, so the next steer can start one, here or on the
					// replica that owns the room now.
					s.log.Warn("room: agent slot not granted; agent released", "room", roomID, "err", err)
					s.releaseAgent(st, run, cancel)
				}
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
			if firstLook != nil {
				// The run holds its slot now; it waits for the pass only
				// up to its deadline, so a slow pass cannot hold the slot.
				alert.Triage = s.awaitTriage(ctx, roomID, firstLook)
				firstLook = nil
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
			_ = s.agentEmit(ectx, roomID, run, events.AgentStatus, events.AgentStatusPayload{State: "error", Detail: truncateDetail(err.Error())})
			ecancel()
			if isUnretryable(err) {
				// The same input fails the same way each time, and a replay
				// would repeat the story the agent already told.
				s.log.Warn("room: agent stopped (unretryable)", "room", roomID, "err", err)
				reason := truncateDetail(err.Error())
				// Only the proposal refusal gets the owners.yaml hint: a
				// model error in the same room is about the key or model.
				if isProposalsDisabled(err) && s.pipelineProposalsDisabled(pipelineID) {
					reason = fmt.Sprintf("proposals are disabled in this room because pipeline %q is not configured — add it to owners.yaml and restart the server", pipelineID)
				}
				mctx, mcancel := context.WithTimeout(s.ctx, 5*time.Second)
				_ = s.agentEmit(mctx, roomID, run, events.AgentMessage, events.AgentMessagePayload{Text: "The agent stopped: " + reason})
				mcancel()
				s.releaseAgent(st, run, cancel)
				return
			}
			if attempt >= maxAgentAttempts {
				s.log.Error("room: agent gave up after repeated failures", "room", roomID, "attempts", attempt)
				s.releaseAgent(st, run, cancel)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-s.backoffAfter(backoff):
			}
			backoff *= 2
		}
	}()
}

// backoffAfter is time.After for the retry backoff, or the test's replacement.
func (s *Service) backoffAfter(d time.Duration) <-chan time.Time {
	if s.retryAfter != nil {
		return s.retryAfter(d)
	}
	return time.After(d)
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

// isUnretryable reports an error a second attempt cannot cure: bad input, a
// validation failure, a closed room, or a model request the API refused
// (modelRefused).
func isUnretryable(err error) bool {
	if errors.Is(err, tools.ErrBadInput) || errors.Is(err, topology.ErrBadInput) {
		return true
	}
	var apiErr *anthropic.Error
	if errors.As(err, &apiErr) && modelRefused(apiErr) {
		return true
	}
	var re *Error
	return errors.As(err, &re) && (re.Code == codeValidation || re.Code == codeRoomClosed)
}

// modelRefused reports a model error every run repeats: a 4xx (a bad key, an
// unknown model) other than 408, 409 and 429, which the SDK already retried.
// A 413, or a 400 saying the prompt is too long, is about one run's history
// (as internal/agent's triage reads it): a fresh run starts from one message,
// so it is retried.
func modelRefused(apiErr *anthropic.Error) bool {
	switch c := apiErr.StatusCode; {
	case c < 400 || c >= 500:
		return false
	case c == http.StatusRequestTimeout, c == http.StatusConflict, c == http.StatusTooManyRequests, c == http.StatusRequestEntityTooLarge:
		return false
	case c == http.StatusBadRequest:
		return !strings.Contains(strings.ToLower(apiErr.RawJSON()), "too long")
	}
	return true
}

// releaseAgent frees the room's agent slot when its supervisor gives up, so the
// next steer or @agent comment can start a new run. A supervisor whose slot has
// already been taken by a newer run (run is stale) releases nothing. cancel is
// the supervisor's own context, called to free it either way.
func (s *Service) releaseAgent(st *roomState, run uint64, cancel context.CancelFunc) {
	s.lockState(st)
	if !st.gone && st.agentRun == run {
		st.agentCancel = nil
		// The room has no agent now: any replica may start the next one.
		s.dropLeaseLocked(st.snap.ID, st)
	}
	s.unlockRoom(st)
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
