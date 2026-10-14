package store

import (
	"context"
	"time"

	"piperoom/internal/events"
)

// Store is the full store surface the room service needs: the append-only
// event log (events.Store) plus the aux tables used to enforce rules across
// restarts. Both *PG and memstore.Store implement it.
type Store interface {
	events.Store

	CreateRoom(ctx context.Context, r Room) error
	GetRoom(ctx context.Context, id string) (Room, error)
	ListRooms(ctx context.Context) ([]Room, error)
	UpdateDriver(ctx context.Context, id, driverID, driverName string, leaseUntil time.Time) error
	CloseRoom(ctx context.Context, id string, closedAt time.Time) error
	// DeleteEmptyRoom removes a room row that has no events yet, for the
	// window between CreateRoom and the room.created append. A missing room is
	// not an error; a room that has events is ErrRoomNotEmpty and is kept.
	DeleteEmptyRoom(ctx context.Context, id string) error

	// LastRoomByDedupKey returns the newest room (by opened_at, ties by id)
	// whose room.created event carries dedupKey, open or closed, or an error
	// wrapping ErrNotFound. dedupKey is the full alert index key
	// ("<pipeline>|<key>") exactly as room.created stores it. An empty key
	// never matches.
	LastRoomByDedupKey(ctx context.Context, dedupKey string) (Room, error)

	CreateProposal(ctx context.Context, p ProposalRow) error
	DeleteProposal(ctx context.Context, proposalID string) error
	GetProposal(ctx context.Context, proposalID string) (ProposalRow, error)
	// RecordApproval stores one decision with its params_hash and identity; a
	// second decision on the same layer wraps ErrAlreadyExists.
	RecordApproval(ctx context.Context, a ApprovalRow) error
	// ListApprovals returns a proposal's decisions ordered by layer.
	ListApprovals(ctx context.Context, proposalID string) ([]ApprovalRow, error)
	// SetProposalParamsHash fills the hash of a proposal that has none (a row
	// from before auth PR 4). It never replaces an existing hash: a different
	// one wraps ErrParamsHashConflict, the same one is a no-op.
	SetProposalParamsHash(ctx context.Context, proposalID, hash string) error
	DeleteApproval(ctx context.Context, proposalID, layer string) error
	StartProposalStep(ctx context.Context, proposalID string, stepIndex int) error
	FinishProposalStep(ctx context.Context, proposalID string, stepIndex int, status, output string) error
	SetProposalStatus(ctx context.Context, proposalID, status string) error

	CreateSuggestion(ctx context.Context, r SuggestionRow) error
	DeleteSuggestion(ctx context.Context, suggestionID string) error
	GetSuggestion(ctx context.Context, suggestionID string) (SuggestionRow, error)
	// SetSuggestionStatus moves a suggestion from status from to to, only while
	// it still has from; otherwise it wraps ErrStatusChanged (another writer
	// was first) or ErrNotFound.
	SetSuggestionStatus(ctx context.Context, suggestionID, from, to string) error

	CreateHandoff(ctx context.Context, h HandoffRow) error
	DeleteHandoff(ctx context.Context, handoffID string) error
	GetHandoff(ctx context.Context, handoffID string) (HandoffRow, error)
	SetHandoffStatus(ctx context.Context, handoffID, status string) error
	VoidHandoffs(ctx context.Context, roomID, exceptHandoffID string) error
}

// PurgeResult counts what one PurgeClosedBefore call deleted. It carries
// counts and room ids only, never content, so it is safe to log.
type PurgeResult struct {
	RoomIDs      []string // rooms removed, so the caller can drop cached state
	Examined     int      // rooms the call looked at (removed, gone meanwhile, or failed)
	Failed       []PurgeFailure
	Events       int64
	Proposals    int64
	Approvals    int64
	Steps        int64
	Suggestions  int64
	Handoffs     int64
	SlackThreads int64
	ShadowEvents int64
	ShadowRuns   int64
}

// PurgeFailure is one room that could not be purged. Its transaction rolled
// back, so the room is intact.
type PurgeFailure struct {
	RoomID string
	Err    error
}

// Add folds o into r.
func (r *PurgeResult) Add(o PurgeResult) {
	r.RoomIDs = append(r.RoomIDs, o.RoomIDs...)
	r.Examined += o.Examined
	r.Failed = append(r.Failed, o.Failed...)
	r.Events += o.Events
	r.Proposals += o.Proposals
	r.Approvals += o.Approvals
	r.Steps += o.Steps
	r.Suggestions += o.Suggestions
	r.Handoffs += o.Handoffs
	r.SlackThreads += o.SlackThreads
	r.ShadowEvents += o.ShadowEvents
	r.ShadowRuns += o.ShadowRuns
}

// ArchiveFunc writes a room's archive before its delete and returns the last
// seq the archive covers. An error skips the delete of that room.
type ArchiveFunc func(ctx context.Context, roomID string) (lastSeq int64, err error)

// PurgeQuery selects the closed rooms one PurgeClosed call deletes.
type PurgeQuery struct {
	Cutoff     time.Time // closed_at before this
	PipelineID string    // "" = every pipeline
	MaxRooms   int
	// Exclude skips rooms (the caller passes rooms that already failed in
	// this run).
	Exclude []string
	// Archive, when set, runs for each room before its transaction; the room
	// is then deleted only while its last_seq is still the one the archive
	// returned (else the room fails with ErrRoomChanged and is tried again on
	// the next run). An archive error fails the room the same way.
	Archive ArchiveFunc
	// AuditReason and DeletedAt fill the room_deletions row each purged room
	// gets in its own transaction (deleted_by RetentionActor). "" means
	// "retention"; the zero time means the store's clock.
	AuditReason string
	DeletedAt   time.Time
}

// RetentionActor is room_deletions.deleted_by for a retention purge.
const RetentionActor = "system"

// Audit builds the room_deletions row of one purged room. Postgres keeps
// microseconds, so the time is cut there for every store alike.
func (q PurgeQuery) Audit(roomID, pipelineID string, events int64, now func() time.Time) RoomDeletion {
	reason := q.AuditReason
	if reason == "" {
		reason = "retention"
	}
	at := q.DeletedAt
	if at.IsZero() {
		at = now()
	}
	return RoomDeletion{
		RoomID: roomID, PipelineID: pipelineID, DeletedBy: RetentionActor,
		DeletedAt: at.UTC().Truncate(time.Microsecond), Reason: reason, EventCount: events,
	}
}

// Purger is the retention surface. It is separate from Store so a store (or a
// test fake) that cannot delete simply does not implement it. Both *PG and
// memstore.Store do.
type Purger interface {
	// PurgeClosed deletes up to q.MaxRooms closed rooms whose closed_at is
	// before q.Cutoff (of q.PipelineID only, when set), oldest first, with
	// every event and child row. Open rooms are never touched. Each room goes
	// in its own transaction with its own timeout, after its archive. One
	// room failing does not stop the others: it is reported in Failed and the
	// call carries on. The returned error is only for the call itself
	// (listing failed, ctx done). Examined < MaxRooms means no qualifying room
	// is left.
	PurgeClosed(ctx context.Context, q PurgeQuery) (PurgeResult, error)
}

// RoomDeletion is the room_deletions row: the content-free audit record of one
// admin deletion (DELETE /api/rooms/{id}).
type RoomDeletion struct {
	RoomID     string    `json:"room_id"`
	PipelineID string    `json:"pipeline_id"`
	DeletedBy  string    `json:"deleted_by"`
	DeletedAt  time.Time `json:"deleted_at"`
	Reason     string    `json:"reason"`
	EventCount int64     `json:"event_count"`
}

// Deleter is the admin deletion surface. Both *PG and memstore.Store
// implement it.
type Deleter interface {
	// DeleteRoom deletes the room d.RoomID, open or closed, with every row
	// keyed by it, and inserts d into room_deletions with PipelineID taken
	// from the room and EventCount set to the events deleted, all in one
	// transaction. wantSeq >= 0 deletes only while the room's last_seq is
	// wantSeq (ErrRoomChanged otherwise). A missing room wraps ErrNotFound.
	DeleteRoom(ctx context.Context, d RoomDeletion, wantSeq int64) (RoomDeletion, PurgeResult, error)
	// GetRoomDeletion returns the audit row of a deleted room, or an error
	// wrapping ErrNotFound.
	GetRoomDeletion(ctx context.Context, roomID string) (RoomDeletion, error)
}

// ShadowStore is the shadow log and run table (docs/design/shadow.md section
// 3.2). It is separate from Store like Purger: a store without it cannot host
// shadow rooms, and the room service's Load refuses to start when a pipeline
// is shadow and the store does not implement it. Both *PG and memstore.Store
// do.
type ShadowStore interface {
	// CreateShadowRun inserts the run row of a shadow room. It is idempotent:
	// a second call for the same room changes nothing and is not an error
	// (INSERT ... ON CONFLICT DO NOTHING), so the room service can call it
	// again wherever the first try may have failed.
	CreateShadowRun(ctx context.Context, r ShadowRun) error
	// GetShadowRun returns the run row of a room, or an error wrapping
	// ErrNotFound.
	GetShadowRun(ctx context.Context, roomID string) (ShadowRun, error)
	// UpdateShadowRun writes the supervisor's columns of r: status, model,
	// pipeline_map, started_at, finished_at, finished_after_close, tool_calls
	// and tokens. It never touches the columns AppendShadow owns
	// (first_hypothesis_at, final_hypothesis_at, final_hypothesis), nor
	// revealed_at or last_seq, so a read-modify-write cannot undo a hypothesis
	// appended in between. A missing row wraps ErrNotFound.
	UpdateShadowRun(ctx context.Context, r ShadowRun) error
	// AppendShadow appends one event to the room's shadow log with the next
	// gap-free seq. It runs CheckAppend first (the jsonb guard). An
	// agent.hypothesis event also sets first_hypothesis_at (once),
	// final_hypothesis_at and final_hypothesis in the same transaction, so a
	// crash never leaves the run row behind its log. A room without a run row
	// wraps ErrNotFound. The room log caps (SetLimits) hold for the shadow log
	// on their own counts, with the same classes as Append: past them it wraps
	// ErrLogFull and uses no seq.
	AppendShadow(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error)
	// ShadowSince returns shadow events with Seq > afterSeq in order, at most
	// limit.
	ShadowSince(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error)
	// CountShadowEvents counts the room's shadow events of one type.
	CountShadowEvents(ctx context.Context, roomID string, typ events.Type) (int, error)
	// MarkRevealed sets revealed_at once; first is false when it was set
	// before. A missing row wraps ErrNotFound.
	MarkRevealed(ctx context.Context, roomID string, at time.Time) (first bool, err error)
	// ListShadowRuns returns the runs of closed rooms, newest close first (ties
	// by room id), joined with the room row and room.created. pipelineID ""
	// means every pipeline; rooms closed before since are left out (the zero
	// time keeps all).
	ListShadowRuns(ctx context.Context, pipelineID string, since time.Time) ([]ShadowRunRoom, error)
	// UnrevealedShadowRuns lists the room ids of finished runs of closed rooms
	// with no revealed_at (the Load sweep), in room id order.
	UnrevealedShadowRuns(ctx context.Context) ([]string, error)
}
