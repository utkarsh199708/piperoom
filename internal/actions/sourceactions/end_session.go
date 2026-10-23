package sourceactions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// EndSessionName is the registered name of the action.
const EndSessionName = "source_end_session"

// Bounds of older_than_minutes, the same as the source_blocking_sessions
// tool's, so a session the agent saw there is found again here.
const (
	defaultOlderThanMinutes = 5
	maxOlderThanMinutes     = 43200
)

// SessionEnder is the admin write of source_end_session
// (postgres.SourceAdminWriter.EndSession, Q2b): it re-reads pid and
// backend_start itself and refuses a mismatch (ErrSessionChanged), a
// walsender and Piperoom's own sessions.
type SessionEnder interface {
	EndSession(ctx context.Context, pid int64, startedAt time.Time) error
}

var _ SessionEnder = (*postgres.Writer)(nil)

// errRefused marks a session the step may not end now (gone, replaced, or
// the connector's own): Preview renders it as a refused step rather than a
// failed read.
var errRefused = errors.New("the step is refused")

// EndSessionDeps are the clients of source_end_session.
type EndSessionDeps struct {
	// Connect reads the connector config: its database.user names the
	// connector's own sessions, which are never ended.
	Connect tools.ConfigReader
	// Source is the read-only source client; it must implement
	// ScopedBlockerReader (Q2a), the read behind source_blocking_sessions in
	// a room, and BlockerReader for the explicit unscoped opt-out.
	Source clients.SourceDB
	// Writer is used by Execute only; nil (untyped) when write mode is off
	// or SOURCE_DB_WRITE_URL is empty, and then Execute refuses.
	Writer SessionEnder

	// now is the clock of the preview's ages; tests only (nil = time.Now).
	now func() time.Time
	// timeout overrides opTimeout; tests only.
	timeout time.Duration
}

// NewEndSession returns source_end_session: end one source database session
// that holds back logical replication (pg_terminate_backend on Postgres).
func NewEndSession(d EndSessionDeps) actions.Action { return &endSession{d: d} }

type endSession struct{ d EndSessionDeps }

var (
	_ actions.Scoped  = (*endSession)(nil)
	_ actions.Preview = (*endSession)(nil)
)

func (a *endSession) Name() string  { return EndSessionName }
func (a *endSession) Layer() string { return layerSource }

func (a *endSession) Describe() string {
	return "End one source database session that holds back the pipeline's replication slot (pg_terminate_backend): an idle or long transaction that stops the slot from advancing or keeps a slot creation waiting. " +
		"Its open transaction is rolled back and its client sees a dropped connection. " +
		"Take pid and started_at from source_blocking_sessions: the session must be listed there (with the same older_than_minutes) when the step is proposed and again right before it runs, with the same start time, and either hold or wait for a lock on one of the pipeline's source_tables (identity shown) or have blocks_slot true (it holds the oldest xid horizon, so this pipeline's slot cannot advance or be created; its identity stays withheld). " +
		"Never ends a walsender, Piperoom's own sessions, or a session of the connector's database.user."
}

func (a *endSession) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector":          map[string]any{"type": "string", "description": "The pipeline's Debezium source connector whose slot the session holds back."},
			"pid":                map[string]any{"type": "integer", "minimum": 1, "description": "The session's pid, from source_blocking_sessions."},
			"started_at":         map[string]any{"type": "string", "format": "date-time", "description": "The session's started_at (backend start), from source_blocking_sessions, exactly: with pid it identifies the session."},
			"older_than_minutes": map[string]any{"type": "integer", "minimum": 1, "maximum": maxOlderThanMinutes, "description": "The older_than_minutes the session was listed with. Default 5."},
		},
		"additionalProperties": false,
		"required":             []string{"connector", "pid", "started_at"},
	}
}

type endSessionParams struct {
	Connector        string    `json:"connector"`
	PID              int64     `json:"pid"`
	StartedAt        time.Time `json:"started_at"`
	OlderThanMinutes *int      `json:"older_than_minutes,omitempty"`
}

func (p endSessionParams) olderThan() time.Duration {
	m := defaultOlderThanMinutes
	if p.OlderThanMinutes != nil {
		m = *p.OlderThanMinutes
	}
	return time.Duration(m) * time.Minute
}

func parseEndSession(raw json.RawMessage) (endSessionParams, error) {
	var p endSessionParams
	if err := decodeStrict(EndSessionName, raw, &p); err != nil {
		return p, err
	}
	if err := checkConnectorName(EndSessionName, p.Connector); err != nil {
		return p, err
	}
	if p.PID <= 0 {
		return p, fmt.Errorf("sourceactions: %s: pid must be positive, got %d", EndSessionName, p.PID)
	}
	if p.StartedAt.IsZero() {
		return p, fmt.Errorf("sourceactions: %s: started_at is required (with pid it identifies the session)", EndSessionName)
	}
	if m := p.OlderThanMinutes; m != nil && (*m < 1 || *m > maxOlderThanMinutes) {
		return p, fmt.Errorf("sourceactions: %s: older_than_minutes must be 1 to %d, got %d", EndSessionName, maxOlderThanMinutes, *m)
	}
	return p, nil
}

// CheckScope refuses, without I/O, a connector outside pl and a pipeline that
// is not a CDC one. A pid cannot be mapped to a pipeline without I/O; the
// blocking-session list (Validate and Execute) is that check.
func (a *endSession) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	p, err := parseEndSession(raw)
	if err != nil {
		return err
	}
	return pipelineScope(EndSessionName, pl, p.Connector)
}

// connectorUser reads the connector config and returns its database.user
// ("" when unset or a config provider reference).
func (a *endSession) connectorUser(ctx context.Context, connector string) (string, error) {
	if a.d.Connect == nil {
		return "", fmt.Errorf("sourceactions: %s: no Connect client to read connector %q", EndSessionName, connector)
	}
	cfg, err := a.d.Connect.Config(ctx, connector)
	if err != nil {
		return "", fmt.Errorf("sourceactions: %s: read connector %q config: %w", EndSessionName, connector, err)
	}
	if !isDebeziumSource(cfg["connector.class"]) {
		return "", fmt.Errorf("sourceactions: %s: connector %q is not a Debezium source connector (connector.class %q)", EndSessionName, connector, cut(cfg["connector.class"]))
	}
	if u := cfg["database.user"]; !configRef(u) {
		return u, nil
	}
	return "", nil
}

// blockers reads the blocking sessions the way source_blocking_sessions
// does: scoped to pl's source_tables when pl is set (a session touching none
// of them comes back with InScope false), in full only for the explicit
// unscoped opt-out (pl nil). A client that cannot scope is refused, never
// read in full.
func (a *endSession) blockers(ctx context.Context, p endSessionParams, pl *owners.Pipeline) ([]DBSession, error) {
	if pl == nil {
		r, ok := a.d.Source.(BlockerReader)
		if !ok {
			return nil, fmt.Errorf("sourceactions: %s: the source client cannot list blocking sessions (no Blockers)", EndSessionName)
		}
		return r.Blockers(ctx, p.olderThan())
	}
	r, ok := a.d.Source.(ScopedBlockerReader)
	if !ok {
		return nil, fmt.Errorf("sourceactions: %s: the source client cannot scope blocking sessions to pipeline %q's tables (no BlockersOnTables)", EndSessionName, pl.ID)
	}
	return r.BlockersOnTables(ctx, p.olderThan(), pl.SourceTables)
}

// findBlocker reads the blocking sessions and returns the one p names. A
// session no longer listed or listed with another start time is refused: the
// step was approved for that session only. With pl set, a room may end a
// session that holds or waits for a lock on pl's source_tables (InScope), or
// one whose identity the scoped read withheld but that holds the server's
// oldest xid horizon (BlocksSlot): that session holds back this pipeline's
// slot whatever tables it touches (the classic slot-creation blocker locks
// none of them). Any other out-of-scope session is refused.
func (a *endSession) findBlocker(ctx context.Context, p endSessionParams, pl *owners.Pipeline, connUser string) (DBSession, error) {
	sessions, err := a.blockers(ctx, p, pl)
	if err != nil {
		return DBSession{}, fmt.Errorf("sourceactions: %s: list blocking sessions: %w", EndSessionName, err)
	}
	for _, s := range sessions {
		if s.PID != p.PID {
			continue
		}
		if !s.StartedAt.Equal(p.StartedAt) {
			return DBSession{}, fmt.Errorf("sourceactions: %s: pid %d now names a session that started %s, not %s: %w: %w",
				EndSessionName, p.PID, s.StartedAt.UTC().Format(time.RFC3339Nano), p.StartedAt.UTC().Format(time.RFC3339Nano), errRefused, postgres.ErrSessionChanged)
		}
		if pl != nil && !s.InScope {
			if !s.BlocksSlot {
				// It touches none of the pipeline's tables and does not hold
				// the horizon: another pipeline's problem, not this room's.
				return DBSession{}, fmt.Errorf("sourceactions: %s: pid %d holds or waits for no lock on pipeline %q's source_tables and does not hold the oldest xid horizon, so it is outside this room: %w: %w",
					EndSessionName, p.PID, pl.ID, errRefused, tools.ErrOutOfScope)
			}
			// blocks_slot is relative (the oldest horizon on the server), so
			// some session always has it. A hidden session is this room's to
			// end only when it is an ordinary client of the pipeline's own
			// source database whose transaction is older than
			// older_than_minutes; one in another database belongs to another
			// tenant and goes to a human.
			if !s.OldXact || !s.SameDB || !s.ClientBackend {
				return DBSession{}, fmt.Errorf("sourceactions: %s: pid %d is a session whose identity is withheld from this room and is not a client of pipeline %q's source database with a transaction older than %d min; hand it to the database owner: %w: %w",
					EndSessionName, p.PID, pl.ID, int(p.olderThan()/time.Minute), errRefused, tools.ErrOutOfScope)
			}
			if err := a.checkHiddenUser(ctx, p, connUser); err != nil {
				return DBSession{}, err
			}
			return s, nil
		}
		// EqualFold: Postgres folds an unquoted role name to lowercase, so
		// config database.user=Debezium shows as usename debezium.
		if connUser != "" && strings.EqualFold(s.User, connUser) {
			return DBSession{}, fmt.Errorf("sourceactions: %s: pid %d is a session of connector %q's own database.user; ending it would break the connector: %w", EndSessionName, p.PID, p.Connector, errRefused)
		}
		return s, nil
	}
	return DBSession{}, fmt.Errorf("sourceactions: %s: pid %d is not in source_blocking_sessions (older than %d min): it ended or no longer holds back replication; nothing to do: %w",
		EndSessionName, p.PID, int(p.olderThan()/time.Minute), errRefused)
}

// checkHiddenUser refuses a session whose identity the scoped read withheld
// when it may be the connector's own: the room cannot see its user, so the
// action compares it against database.user itself, through the unscoped read,
// and never puts that user (or anything else from the full row) into a
// message, a preview or a log. Fail-closed: an unknown database.user (a config
// provider reference), a client without the unscoped read, or a session the
// full read no longer lists with the same start time is refused.
func (a *endSession) checkHiddenUser(ctx context.Context, p endSessionParams, connUser string) error {
	if connUser == "" {
		return fmt.Errorf("sourceactions: %s: pid %d is a session of another user or database and connector %q's database.user is not readable (unset or a config provider reference), so it cannot be told apart from the connector's own session: %w",
			EndSessionName, p.PID, p.Connector, errRefused)
	}
	r, ok := a.d.Source.(BlockerReader)
	if !ok {
		return fmt.Errorf("sourceactions: %s: pid %d is a session of another user or database and the source client cannot check its user (no Blockers): %w", EndSessionName, p.PID, errRefused)
	}
	full, err := r.Blockers(ctx, p.olderThan())
	if err != nil {
		return fmt.Errorf("sourceactions: %s: check the user of pid %d: %w", EndSessionName, p.PID, err)
	}
	for _, s := range full {
		if s.PID != p.PID || !s.StartedAt.Equal(p.StartedAt) {
			continue
		}
		if strings.EqualFold(s.User, connUser) {
			return fmt.Errorf("sourceactions: %s: pid %d is a session of connector %q's own database.user; ending it would break the connector: %w", EndSessionName, p.PID, p.Connector, errRefused)
		}
		return nil
	}
	return fmt.Errorf("sourceactions: %s: pid %d (started %s) is no longer listed as a blocking session: %w: %w",
		EndSessionName, p.PID, p.StartedAt.UTC().Format(time.RFC3339Nano), errRefused, postgres.ErrSessionChanged)
}

// check is the shared read of Validate, Preview and Execute: the connector's
// user and the session in the blocking list, scoped to pl (nil = unscoped).
func (a *endSession) check(ctx context.Context, p endSessionParams, pl *owners.Pipeline) (DBSession, error) {
	ctx, cancel := withTimeout(ctx, a.d.timeout)
	defer cancel()
	user, err := a.connectorUser(ctx, p.Connector)
	if err != nil {
		return DBSession{}, err
	}
	return a.findBlocker(ctx, p, pl, user)
}

// Validate refuses bad params, a connector outside the room's pipeline (when
// ctx carries it) and a pid that is not, with its start time, in the
// blocking-session list now; with the room's pipeline in ctx the list is
// scoped to its source_tables and an out-of-scope pid is refused unless it
// holds the oldest xid horizon (findBlocker). A missing
// writer is refused at Execute only, so the agent can still propose the step
// with write mode off (P0 ruling #606).
func (a *endSession) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := parseEndSession(raw)
	if err != nil {
		return err
	}
	if err := validateScope(ctx, func(pl owners.Pipeline) error { return pipelineScope(EndSessionName, pl, p.Connector) }); err != nil {
		return err
	}
	var scope *owners.Pipeline
	if pl, ok := tools.PipelineFrom(ctx); ok {
		scope = &pl
	}
	_, err = a.check(ctx, p, scope)
	return err
}

// scopeOf is the blocking-list scope of a ctxScope result: the pipeline, or
// nil for the explicit unscoped opt-out.
func scopeOf(pl owners.Pipeline, scoped bool) *owners.Pipeline {
	if !scoped {
		return nil
	}
	return &pl
}

// Preview shows the session as the blocking list has it now: age,
// application, user, state and the redacted query, or, for a session whose
// identity the scoped read withheld, its ages, state and that it holds the
// horizon only. It never holds the writer.
func (a *endSession) Preview(ctx context.Context, raw json.RawMessage) (actions.PreviewResult, error) {
	p, err := parseEndSession(raw)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	pl, scoped, err := ctxScope(ctx, EndSessionName, func(pl owners.Pipeline) error { return pipelineScope(EndSessionName, pl, p.Connector) })
	if err != nil {
		return actions.PreviewResult{}, err
	}
	res := actions.PreviewResult{
		Target: fmt.Sprintf("session pid %d (started %s) on the source database", p.PID, p.StartedAt.UTC().Format(time.RFC3339)),
	}
	s, err := a.check(ctx, p, scopeOf(pl, scoped))
	if err != nil && !errors.Is(err, errRefused) {
		return actions.PreviewResult{}, err
	}
	if err != nil {
		res.Current = "the session is not in the blocking list as named"
		res.Expected = "no change: this step is refused (" + strings.TrimPrefix(err.Error(), "sourceactions: "+EndSessionName+": ") + ")"
		return res, nil
	}
	now := time.Now
	if a.d.now != nil {
		now = a.d.now
	}
	t := now()
	blocks := "holds a transaction older than the threshold"
	if s.BlocksSlot {
		blocks = "holds the server's oldest xmin, so the slot cannot advance past it"
	}
	if s.InScope {
		res.Current = fmt.Sprintf("pid %d: transaction age %s, connected %s ago, application %q, user %q, state %q; %s; query (untrusted, redacted): %q",
			s.PID, age(t, s.XactStart), age(t, s.StartedAt), oneLine(s.App, 64), oneLine(s.User, 64), oneLine(s.State, 32), blocks, safeQuery(s.Query))
	} else {
		// The scoped read withheld who it is: a session of another user or
		// database. The approvers see what holds back this pipeline's slot,
		// never whose session it is.
		res.Current = fmt.Sprintf("pid %d: a session of another user or database; identity withheld. Transaction age %s, connected %s ago, state %q; %s",
			s.PID, age(t, s.XactStart), age(t, s.StartedAt), oneLine(s.State, 32), blocks)
	}
	res.Expected = fmt.Sprintf("pg_terminate_backend(%d), only if pid %d still started at %s: its open transaction is rolled back, its client sees a dropped connection, and the slot can advance. Re-checked against the blocking list right before it runs.",
		p.PID, p.PID, p.StartedAt.UTC().Format(time.RFC3339Nano))
	res.Reversible = false
	res.Rollback = ""
	return res, nil
}

// age renders now - t, rounded to seconds; "unknown" for a zero time.
func age(now, t time.Time) string {
	if t.IsZero() {
		return "unknown"
	}
	d := now.Sub(t)
	if d < 0 {
		d = 0
	}
	return d.Round(time.Second).String()
}

// Execute re-checks that the session is still in the blocking list with the
// same start time (and in the pipeline's scope or holding its slot back),
// then ends it through the admin writer, which re-reads pid and backend_start
// once more in its own transaction.
func (a *endSession) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := parseEndSession(raw)
	if err != nil {
		return "", err
	}
	pl, scoped, err := ctxScope(ctx, EndSessionName, func(pl owners.Pipeline) error { return pipelineScope(EndSessionName, pl, p.Connector) })
	if err != nil {
		return "", err
	}
	if a.d.Writer == nil {
		return "", fmt.Errorf("sourceactions: %s: %w", EndSessionName, errNoWriter)
	}
	if _, err := a.check(ctx, p, scopeOf(pl, scoped)); err != nil {
		return "", err
	}
	if err := a.d.Writer.EndSession(ctx, p.PID, p.StartedAt); err != nil {
		return "", fmt.Errorf("sourceactions: %s: %w", EndSessionName, err)
	}
	return fmt.Sprintf("ended session pid %d (started %s); its open transaction was rolled back. Check the slot with the replication slot tools: it should advance.", p.PID, p.StartedAt.UTC().Format(time.RFC3339)), nil
}
