package room

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// pairingWindow is the sliding window of the per-source and global pairing
// rate limits (docs/design/setup-agent.md §2.1).
const pairingWindow = 10 * time.Minute

// Pairing rate limits (docs/design/setup-agent.md §2.1). A code tolerates
// maxCodeAttempts pairing attempts (a runner may reconnect a few times while
// the person confirms on its terminal); a source address may attempt at most
// maxSourceAttempts per pairingWindow; and at most maxGlobalAttempts attempts
// in all per pairingWindow, after which pairing is closed for pairingWindow.
const (
	maxCodeAttempts   = 5
	maxSourceAttempts = 20
	maxGlobalAttempts = 100
)

// Runner is a paired setup runner (docs/design/setup-agent.md §2.1). The
// token is in memory only, never in an event or a log.
type Runner struct {
	Tenant  string
	RoomID  string
	Creator events.Actor
	Label   string
	OS      string
	Arch    string
	Tools   map[string]string
	// ProjectDir is the runner's project directory, from Hello.
	ProjectDir       string
	CatalogueVersion string
	ProtocolVersion  int
	ProtocolMinor    int
	AttachedAt       time.Time
	LastSeen         time.Time
	Token            string
	TokenExpiresAt   time.Time
}

// runnerKey binds a runner to its room.
type runnerKey struct {
	tenant  string
	roomID  string
	creator string // the creator's actor id
}

// pairingCode is an issued, not-yet-attached pairing code. The code value is
// never logged and never leaves the registry except to the one caller that
// issued it.
type pairingCode struct {
	code     string
	tenant   string
	roomID   string
	creator  events.Actor
	expires  time.Time
	attempts int
}

// attemptWindow counts attempts over a sliding pairingWindow.
type attemptWindow struct {
	at []time.Time
}

// add records an attempt at now and reports whether it was within the limit.
// Attempts older than the window are pruned first. limit must be > 0.
func (w *attemptWindow) add(now time.Time, limit int) bool {
	cut := now.Add(-pairingWindow)
	keep := w.at[:0]
	for _, t := range w.at {
		if t.After(cut) {
			keep = append(keep, t)
		}
	}
	w.at = keep
	if len(w.at) >= limit {
		return false
	}
	w.at = append(w.at, now)
	return true
}

// PairResult is the outcome of one pairing attempt (PairCode).
type PairResult struct {
	// Outcome is one of events.Pairing*.
	Outcome string
	// Tenant, RoomID and Creator name the (tenant, room, creator) the code was
	// bound to; set on Outcome ok, expired, rate_limited (per code) and
	// replaced.
	Tenant  string
	RoomID  string
	Creator events.Actor
}

// emitFunc writes one setup.* audit event. roomID may be "" when the attempt
// cannot be tied to a room (a bad code, or a global rate limit): the sink then
// records it at the server level, never losing the source.
type emitFunc func(ctx context.Context, roomID string, typ events.Type, payload any) error

// RunnerRegistry issues and validates the single-use pairing codes that bind a
// setup runner to (tenant, room, creator), and holds the attached runners.
// It is safe for concurrent use. The clock and the audit sink are injectable
// for tests. A code or a token is compared in constant time and never logged.
type RunnerRegistry struct {
	now  func() time.Time
	log  *slog.Logger
	emit emitFunc

	mu          sync.Mutex
	codes       map[string]*pairingCode
	runners     map[runnerKey]*Runner
	perSource   map[string]*attemptWindow
	global      attemptWindow
	closedUntil time.Time
}

// NewRunnerRegistry builds a registry. now may be nil (time.Now); emit may be
// nil, in which case audits are dropped (logic-only tests).
func NewRunnerRegistry(now func() time.Time, emit emitFunc, log *slog.Logger) *RunnerRegistry {
	if now == nil {
		now = time.Now
	}
	if log == nil {
		log = slog.Default()
	}
	return &RunnerRegistry{
		now:       now,
		log:       log,
		emit:      emit,
		codes:     map[string]*pairingCode{},
		runners:   map[runnerKey]*Runner{},
		perSource: map[string]*attemptWindow{},
	}
}

// randomCode returns protocol.CodeLength characters from alphabet, each worth
// 5 bits (40 bits in all), read from crypto/rand.
func randomCode(r io.Reader, alphabet string) (string, error) {
	var raw [5]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return "", err
	}
	var v uint64
	for _, b := range raw {
		v = v<<8 | uint64(b)
	}
	out := make([]byte, protocol.CodeLength)
	for i := range out {
		out[i] = alphabet[(v>>(35-5*i))&31]
	}
	return string(out), nil
}

// IssuePairingCode issues a single-use pairing code bound to
// (tenant, roomID, creator), valid for protocol.CodeTTL. U1-5b's
// POST /api/setup is its caller.
func (r *RunnerRegistry) IssuePairingCode(tenant, roomID string, creator events.Actor) (string, error) {
	code, err := randomCode(rand.Reader, protocol.CodeAlphabet)
	if err != nil {
		return "", fmt.Errorf("room: issue pairing code: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.pruneLocked(now)
	r.pruneCodesLocked(now)
	r.codes[code] = &pairingCode{
		code: code, tenant: tenant, roomID: roomID, creator: creator,
		expires: now.Add(protocol.CodeTTL),
	}
	return code, nil
}

// pruneLocked drops rate-limit entries older than the window. Expired codes
// are pruned separately (pruneCodesLocked) on issue, so a code looked up just
// after its TTL still answers "expired", not "bad_code".
// Caller holds r.mu.
func (r *RunnerRegistry) pruneLocked(now time.Time) {
	for src, w := range r.perSource {
		cut := now.Add(-pairingWindow)
		keep := w.at[:0]
		for _, t := range w.at {
			if t.After(cut) {
				keep = append(keep, t)
			}
		}
		w.at = keep
		if len(w.at) == 0 {
			delete(r.perSource, src)
		}
	}
}

// pruneCodesLocked drops expired codes. Called when a new code is issued so
// stale codes do not accumulate. Caller holds r.mu.
func (r *RunnerRegistry) pruneCodesLocked(now time.Time) {
	for code, pc := range r.codes {
		if !now.Before(pc.expires) {
			delete(r.codes, code)
		}
	}
}

// PairCode validates one pairing attempt: the code presented from source. It
// applies the rate limits (global, per source, per code), the single-use rule
// and the TTL, and emits setup.pairing for every attempt. The code is compared
// in constant time and never logged.
func (r *RunnerRegistry) PairCode(ctx context.Context, code, source string) (PairResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.pruneLocked(now)

	// Pairing closed after the global limit was hit: refuse everything for the
	// close window, audited with the source.
	if now.Before(r.closedUntil) {
		r.audit(ctx, "", events.PairingRateLimited, source, "")
		return PairResult{Outcome: events.PairingRateLimited}, nil
	}
	// The global window counts every attempt. The attempt that would be the
	// (maxGlobalAttempts+1)-th closes pairing for pairingWindow.
	if !r.global.add(now, maxGlobalAttempts) {
		r.closedUntil = now.Add(pairingWindow)
		r.audit(ctx, "", events.PairingRateLimited, source, "")
		return PairResult{Outcome: events.PairingRateLimited}, nil
	}
	// The per-source window.
	w := r.perSource[source]
	if w == nil {
		w = &attemptWindow{}
		r.perSource[source] = w
	}
	if !w.add(now, maxSourceAttempts) {
		r.audit(ctx, "", events.PairingRateLimited, source, "")
		return PairResult{Outcome: events.PairingRateLimited}, nil
	}

	pc := r.lookupCode(code)
	if pc == nil {
		r.audit(ctx, "", events.PairingBadCode, source, "")
		return PairResult{Outcome: events.PairingBadCode}, nil
	}
	if !now.Before(pc.expires) {
		delete(r.codes, code)
		r.audit(ctx, pc.roomID, events.PairingExpired, source, "")
		return PairResult{Outcome: events.PairingExpired, Tenant: pc.tenant, RoomID: pc.roomID, Creator: pc.creator}, nil
	}
	pc.attempts++
	if pc.attempts > maxCodeAttempts {
		delete(r.codes, code)
		r.audit(ctx, pc.roomID, events.PairingRateLimited, source, "")
		return PairResult{Outcome: events.PairingRateLimited, Tenant: pc.tenant, RoomID: pc.roomID, Creator: pc.creator}, nil
	}
	// The code stays until the runner attaches (Attach), so a runner that drops
	// between Welcome and Hello may retry up to maxCodeAttempts times. A
	// room's one runner is enforced in Attach.
	r.audit(ctx, pc.roomID, events.PairingOK, source, "")
	return PairResult{Outcome: events.PairingOK, Tenant: pc.tenant, RoomID: pc.roomID, Creator: pc.creator}, nil
}

// lookupCode finds a live code by constant-time comparison against every
// outstanding code. The code set is tiny (single use, 10-minute TTL), so an
// O(n) scan is the constant-time way to avoid a hash look-up leaking anything
// about which code was presented. Caller holds r.mu.
func (r *RunnerRegistry) lookupCode(code string) *pairingCode {
	if len(code) != protocol.CodeLength {
		return nil
	}
	for _, pc := range r.codes {
		if subtle.ConstantTimeCompare([]byte(pc.code), []byte(code)) == 1 {
			return pc
		}
	}
	return nil
}

// Attach registers a runner (after Hello) and consumes its pairing code: the
// code is single use, so the next presentation is a bad code. A runner that
// replaces another for the same (tenant, room, creator) is audited as a
// replacement. It emits setup.runner_attached.
func (r *RunnerRegistry) Attach(ctx context.Context, code string, rn Runner) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := runnerKey{tenant: rn.Tenant, roomID: rn.RoomID, creator: rn.Creator.ID}
	if _, ok := r.runners[key]; ok {
		// A second pairing for the same room replaces the first; audited.
		r.audit(ctx, rn.RoomID, events.PairingReplaced, "", rn.Label)
	}
	delete(r.codes, code)
	rn.AttachedAt = r.now()
	rn.LastSeen = rn.AttachedAt
	r.runners[key] = &rn
	r.emitRunnerAttached(ctx, rn)
	return nil
}

// Runner returns the runner bound to (tenant, roomID, creatorID), if attached.
func (r *RunnerRegistry) Runner(tenant, roomID, creatorID string) (*Runner, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rn, ok := r.runners[runnerKey{tenant: tenant, roomID: roomID, creator: creatorID}]
	if !ok {
		return nil, false
	}
	return rn, true
}

// Lost detaches the runner bound to (tenant, roomID, creatorID) and emits
// setup.runner_lost with its label and last-seen time.
func (r *RunnerRegistry) Lost(ctx context.Context, tenant, roomID, creatorID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := runnerKey{tenant: tenant, roomID: roomID, creator: creatorID}
	rn, ok := r.runners[key]
	if !ok {
		return
	}
	delete(r.runners, key)
	r.emitSetup(ctx, roomID, events.SetupRunnerLost, events.SetupRunnerLostPayload{
		RunnerLabel: rn.Label,
		LastSeen:    rn.LastSeen.UTC(),
	})
}

// Touch updates the runner's last-seen time after a frame arrives, so
// setup.runner_lost carries when the runner was last heard from, not when it
// attached. A frame that no longer names an attached runner is a no-op.
func (r *RunnerRegistry) Touch(tenant, roomID, creatorID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rn, ok := r.runners[runnerKey{tenant: tenant, roomID: roomID, creator: creatorID}]
	if !ok {
		return
	}
	rn.LastSeen = r.now()
}

// Refuse records that the runner refused an op (setup.runner_refused). It does
// not detach the runner.
func (r *RunnerRegistry) Refuse(ctx context.Context, tenant, roomID, creatorID string, payload events.SetupRunnerRefusedPayload) {
	key := runnerKey{tenant: tenant, roomID: roomID, creator: creatorID}
	r.mu.Lock()
	_, ok := r.runners[key]
	r.mu.Unlock()
	if !ok {
		return
	}
	r.emitSetup(ctx, roomID, events.SetupRunnerRefused, payload)
}

// audit emits setup.pairing for one attempt. roomID is "" for an attempt that
// names no room (a bad code or a global limit).
func (r *RunnerRegistry) audit(ctx context.Context, roomID, outcome, source, label string) {
	p := events.SetupPairingPayload{Outcome: outcome, Source: source}
	if label != "" {
		p.RunnerLabel = label
	}
	r.emitSetup(ctx, roomID, events.SetupPairing, p)
}

// emitRunnerAttached emits setup.runner_attached from the Runner's fields.
func (r *RunnerRegistry) emitRunnerAttached(ctx context.Context, rn Runner) {
	r.emitSetup(ctx, rn.RoomID, events.SetupRunnerAttached, events.SetupRunnerAttachedPayload{
		RunnerLabel:      rn.Label,
		OS:               rn.OS,
		Arch:             rn.Arch,
		Tools:            rn.Tools,
		ProjectDir:       rn.ProjectDir,
		CatalogueVersion: rn.CatalogueVersion,
		ProtocolVersion:  rn.ProtocolVersion,
		ProtocolMinor:    rn.ProtocolMinor,
	})
}

// emitSetup runs the audit sink and never fails the call: an audit event that
// cannot be written is a logged warning, not an error to the runner.
func (r *RunnerRegistry) emitSetup(ctx context.Context, roomID string, typ events.Type, payload any) {
	if r.emit == nil {
		return
	}
	if err := r.emit(ctx, roomID, typ, payload); err != nil {
		r.log.Warn("room: setup audit failed", "type", typ, "room", roomID, "err", err)
	}
}
