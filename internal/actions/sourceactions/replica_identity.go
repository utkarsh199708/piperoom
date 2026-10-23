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

// SetReplicaIdentityName is the registered name of the action. It keeps the
// pg_ prefix: REPLICA IDENTITY exists only on Postgres (the MySQL writer
// answers clients.ErrNotSupported).
const SetReplicaIdentityName = "pg_set_replica_identity"

// ReplicaIdentityWriter is the admin write of pg_set_replica_identity
// (postgres.SourceAdminWriter.SetReplicaIdentity, Q2b).
type ReplicaIdentityWriter interface {
	SetReplicaIdentity(ctx context.Context, table, mode, index string) error
}

var _ ReplicaIdentityWriter = (*postgres.Writer)(nil)

// rowEstimator is the planner's row estimate (*postgres.DB.EstimatedRows);
// optional on the source client, found with a type assertion.
type rowEstimator interface {
	EstimatedRows(ctx context.Context, table string) (int64, error)
}

var (
	_ rowEstimator              = (*postgres.DB)(nil)
	_ clients.ToastColumnReader = (*postgres.DB)(nil)
)

// ReplicaIdentityDeps are the clients of pg_set_replica_identity.
type ReplicaIdentityDeps struct {
	// Source is the read-only source client. It must implement
	// TableReplicationReader (Q2a); EstimatedRows and
	// clients.ToastColumnReader are optional and only size the preview.
	Source clients.SourceDB
	// Writer is used by Execute only. nil (untyped) when write mode is off
	// or SOURCE_DB_WRITE_URL is empty: Execute then refuses, while Validate
	// and Preview still work, so the agent can propose the step and a
	// reviewer can see what it would do (P0 ruling #606).
	Writer ReplicaIdentityWriter

	// timeout overrides opTimeout; tests only.
	timeout time.Duration
}

// NewSetReplicaIdentity returns pg_set_replica_identity: ALTER TABLE
// <schema.table> REPLICA IDENTITY FULL | DEFAULT | USING INDEX <index>.
func NewSetReplicaIdentity(d ReplicaIdentityDeps) actions.Action {
	return &setReplicaIdentity{d: d}
}

type setReplicaIdentity struct{ d ReplicaIdentityDeps }

var (
	_ actions.Scoped  = (*setReplicaIdentity)(nil)
	_ actions.Preview = (*setReplicaIdentity)(nil)
)

func (a *setReplicaIdentity) Name() string  { return SetReplicaIdentityName }
func (a *setReplicaIdentity) Layer() string { return layerSource }

func (a *setReplicaIdentity) Describe() string {
	return "Set the replica identity of one source table of the pipeline (ALTER TABLE <schema.table> REPLICA IDENTITY FULL | DEFAULT | USING INDEX <index>). " +
		"Use FULL for a table without a primary key whose UPDATEs and DELETEs fail or reach Debezium without a key, or when consumers need every old column; DEFAULT to go back to the primary key; index to use a unique index's columns. " +
		"Refused for a table outside the pipeline's source_tables, a table that already has the mode, and DEFAULT on a table without a primary key. " +
		"The ALTER takes a brief ACCESS EXCLUSIVE lock; FULL adds the old row to the WAL of every UPDATE and DELETE (the preview sizes it)."
}

func (a *setReplicaIdentity) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"table": map[string]any{"type": "string", "description": "The source table, \"schema.table\"; one of the pipeline's source_tables.", "pattern": tablePattern},
			"mode":  map[string]any{"type": "string", "enum": []string{"full", "default", "index"}, "description": "full: the whole old row; default: the primary key; index: the columns of the unique index named in index."},
			"index": map[string]any{"type": "string", "description": "Only with mode index: the unique, non-partial, non-deferrable index on NOT NULL columns to use.", "pattern": identRe.String()},
		},
		"additionalProperties": false,
		"required":             []string{"table", "mode"},
	}
}

type replicaIdentityParams struct {
	Table string `json:"table"`
	Mode  string `json:"mode"`
	Index string `json:"index,omitempty"`
}

// parseReplicaIdentity decodes params strictly and checks every identifier
// and the mode before anything reads or writes.
func parseReplicaIdentity(raw json.RawMessage) (replicaIdentityParams, error) {
	var p replicaIdentityParams
	if err := decodeStrict(SetReplicaIdentityName, raw, &p); err != nil {
		return p, err
	}
	if err := checkTable(SetReplicaIdentityName, p.Table); err != nil {
		return p, err
	}
	switch p.Mode {
	case "full", "default":
		if p.Index != "" {
			return p, fmt.Errorf("sourceactions: %s: mode %s takes no index", SetReplicaIdentityName, p.Mode)
		}
	case "index":
		if !identRe.MatchString(p.Index) {
			return p, fmt.Errorf("sourceactions: %s: mode index needs a valid index name, got %q", SetReplicaIdentityName, cut(p.Index))
		}
	default:
		return p, fmt.Errorf("sourceactions: %s: unknown mode %q (full, default or index)", SetReplicaIdentityName, cut(p.Mode))
	}
	return p, nil
}

// CheckScope refuses, without I/O, a table outside pl's source_tables and a
// pipeline that is not a CDC one.
func (a *setReplicaIdentity) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	p, err := parseReplicaIdentity(raw)
	if err != nil {
		return err
	}
	return replicaIdentityInScope(pl, p)
}

func replicaIdentityInScope(pl owners.Pipeline, p replicaIdentityParams) error {
	if err := pipelineKindCDC(SetReplicaIdentityName, pl); err != nil {
		return err
	}
	return sourceTablesScope(SetReplicaIdentityName, pl, p.Table)
}

func (a *setReplicaIdentity) reader() (TableReplicationReader, error) {
	r, ok := a.d.Source.(TableReplicationReader)
	if !ok {
		return nil, fmt.Errorf("sourceactions: %s: the source client cannot read replica identities (no TableReplication)", SetReplicaIdentityName)
	}
	return r, nil
}

// readTable reads the table's current replica identity and primary key.
func (a *setReplicaIdentity) readTable(ctx context.Context, p replicaIdentityParams) (TableReplication, error) {
	r, err := a.reader()
	if err != nil {
		return TableReplication{}, err
	}
	got, err := r.TableReplication(ctx, []string{p.Table})
	if err != nil {
		return TableReplication{}, fmt.Errorf("sourceactions: %s: read %s: %w", SetReplicaIdentityName, p.Table, err)
	}
	if len(got) != 1 {
		return TableReplication{}, fmt.Errorf("sourceactions: %s: read %s: %d entries for one table", SetReplicaIdentityName, p.Table, len(got))
	}
	return got[0], nil
}

// checkReplicaIdentity refuses a change that does nothing or would break the
// table's writers.
func checkReplicaIdentity(p replicaIdentityParams, tr TableReplication) error {
	if p.Mode != "index" && tr.ReplicaIdentity == p.Mode {
		return fmt.Errorf("sourceactions: %s: %s already has REPLICA IDENTITY %s; nothing to do", SetReplicaIdentityName, p.Table, strings.ToUpper(p.Mode))
	}
	// DEFAULT without a primary key is no identity at all: on a published
	// table Postgres then refuses every UPDATE and DELETE.
	if p.Mode == "default" && !tr.HasPrimaryKey {
		return fmt.Errorf("sourceactions: %s: %s has no primary key, so REPLICA IDENTITY DEFAULT would leave it without a key and UPDATEs and DELETEs on a published table would fail; use full or index", SetReplicaIdentityName, p.Table)
	}
	return nil
}

// Validate refuses bad params, a table outside the room's pipeline (when ctx
// carries it), a missing table and a change that does nothing or breaks
// writers. It reads only TableReplication. A missing writer is refused at
// Execute only (P0 ruling #606).
func (a *setReplicaIdentity) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := parseReplicaIdentity(raw)
	if err != nil {
		return err
	}
	if err := validateScope(ctx, func(pl owners.Pipeline) error { return replicaIdentityInScope(pl, p) }); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, a.d.timeout)
	defer cancel()
	tr, err := a.readTable(ctx, p)
	if err != nil {
		return err
	}
	return checkReplicaIdentity(p, tr)
}

// tableSize is what the preview knows of the table's size; a field is -1
// when it was not read.
type tableSize struct {
	rows       int64 // pg_class.reltuples
	toastBytes int64 // the TOAST relation's size
	widthBytes int64 // sum of the toastable columns' average stored width
	note       string
}

// readSize reads the table's size through the optional readers. A failure is
// a note in the preview, never an error: the size only informs the reviewer.
func (a *setReplicaIdentity) readSize(ctx context.Context, table string) tableSize {
	sz := tableSize{rows: -1, toastBytes: -1, widthBytes: -1}
	var notes []string
	if e, ok := a.d.Source.(rowEstimator); ok {
		n, err := e.EstimatedRows(ctx, table)
		if err != nil {
			notes = append(notes, "row estimate not read ("+errText(err)+")")
		} else {
			sz.rows = n
		}
	}
	if t, ok := a.d.Source.(clients.ToastColumnReader); ok {
		tt, _, err := t.ToastColumns(ctx, []string{table})
		switch {
		case err != nil:
			notes = append(notes, "column widths not read ("+errText(err)+")")
		case len(tt) == 1 && tt[0].Found:
			sz.toastBytes = tt[0].ToastBytes
			var w int64
			for _, c := range tt[0].Columns {
				if c.AvgWidth > 0 {
					w += int64(c.AvgWidth)
				}
			}
			sz.widthBytes = w
		}
	}
	sz.note = strings.Join(notes, "; ")
	return sz
}

// errText is an error for a preview note, cut short.
func errText(err error) string {
	switch {
	case errors.Is(err, clients.ErrNotSupported):
		return "not supported by the source"
	case errors.Is(err, tools.ErrOutOfScope):
		return "out of scope"
	}
	return oneLine(err.Error(), 120)
}

// Preview reads the table's replica identity and size on the read-only
// source client. It never holds the writer.
func (a *setReplicaIdentity) Preview(ctx context.Context, raw json.RawMessage) (actions.PreviewResult, error) {
	p, err := parseReplicaIdentity(raw)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	if _, _, err := ctxScope(ctx, SetReplicaIdentityName, func(pl owners.Pipeline) error { return replicaIdentityInScope(pl, p) }); err != nil {
		return actions.PreviewResult{}, err
	}
	ctx, cancel := withTimeout(ctx, a.d.timeout)
	defer cancel()
	tr, err := a.readTable(ctx, p)
	if err != nil {
		return actions.PreviewResult{}, fmt.Errorf("%w (preview)", err)
	}
	return replicaIdentityPreview(p, tr, a.readSize(ctx, p.Table)), nil
}

// identityWord spells a mode for SQL text.
func identityWord(mode, index string) string {
	switch mode {
	case "index":
		if index == "" {
			return "USING INDEX (unknown index)"
		}
		return "USING INDEX " + quoteIdent(index)
	case "":
		return "unknown"
	}
	return strings.ToUpper(mode)
}

// replicaIdentityPreview renders the preview from what was read.
func replicaIdentityPreview(p replicaIdentityParams, tr TableReplication, sz tableSize) actions.PreviewResult {
	pk := "no primary key"
	if tr.HasPrimaryKey {
		pk = "primary key"
	}
	res := actions.PreviewResult{
		Target:  fmt.Sprintf("table %s on the source Postgres", p.Table),
		Current: fmt.Sprintf("%s has REPLICA IDENTITY %s (%s)%s", p.Table, identityWord(tr.ReplicaIdentity, ""), pk, sizeText(sz)),
	}
	if err := checkReplicaIdentity(p, tr); err != nil {
		res.Expected = "no change: this step is refused (" + strings.TrimPrefix(err.Error(), "sourceactions: "+SetReplicaIdentityName+": ") + ")"
		return res
	}
	var b strings.Builder
	fmt.Fprintf(&b, "ALTER TABLE %s REPLICA IDENTITY %s. ", quoteTable(p.Table), identityWord(p.Mode, p.Index))
	b.WriteString("Lock: the ALTER takes an ACCESS EXCLUSIVE lock on the table for a moment (catalog only, no rewrite), but it waits behind every open transaction that touched the table and every read and write of the table queues behind it while it waits: check source_blocking_sessions first. ")
	b.WriteString(walText(p, tr, sz))
	res.Expected = strings.TrimSpace(b.String())

	switch tr.ReplicaIdentity {
	case "default", "full":
		res.Reversible = true
		res.Rollback = fmt.Sprintf(`%s {"table": %q, "mode": %q}`, SetReplicaIdentityName, p.Table, tr.ReplicaIdentity)
	case "index":
		res.Rollback = fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY USING INDEX <the previous index> (by a DBA: its name is not read here)", quoteTable(p.Table))
	default:
		res.Rollback = fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY %s (by a DBA; no registered action)", quoteTable(p.Table), identityWord(tr.ReplicaIdentity, ""))
	}
	return res
}

// sizeText is the size part of Current.
func sizeText(sz tableSize) string {
	var parts []string
	if sz.rows >= 0 {
		parts = append(parts, fmt.Sprintf("~%d rows (pg_class.reltuples)", sz.rows))
	}
	if sz.toastBytes >= 0 {
		parts = append(parts, fmt.Sprintf("TOAST %s", bytesText(sz.toastBytes)))
	}
	if sz.note != "" {
		parts = append(parts, sz.note)
	}
	if len(parts) == 0 {
		return "; size not read"
	}
	return "; " + strings.Join(parts, ", ")
}

// walText estimates the WAL change of the new mode from the table size.
// Under FULL, Postgres logs the whole old row with every UPDATE and DELETE,
// out-of-line TOAST values flattened in, so the old row's width is the extra
// WAL per change.
func walText(p replicaIdentityParams, tr TableReplication, sz tableSize) string {
	switch {
	case p.Mode == "full":
		per := int64(-1)
		if sz.widthBytes >= 0 || sz.toastBytes >= 0 {
			per = max(sz.widthBytes, 0)
			if sz.toastBytes > 0 && sz.rows > 0 {
				per += sz.toastBytes / sz.rows
			}
		}
		if per <= 0 {
			return "Extra WAL: every UPDATE and DELETE also writes the whole old row (TOASTed values included) to WAL and to the change events' before field; the row width could not be read, so the amount is unknown."
		}
		s := fmt.Sprintf("Extra WAL: every UPDATE and DELETE also writes the whole old row, at least ~%s per change from the column widths and TOAST size (TOASTed values are written in full)", bytesText(per))
		if sz.rows > 0 {
			s += fmt.Sprintf("; changing every one of the ~%d rows once would add ~%s of WAL", sz.rows, bytesText(per*sz.rows))
		}
		return s + ". Debezium's update and delete events then carry the full before image."
	case tr.ReplicaIdentity == "full":
		return "Less WAL: UPDATEs and DELETEs stop logging the whole old row; Debezium's before field then holds only the key columns (consumers that read other old columns get nulls)."
	case p.Mode == "index":
		return "WAL: UPDATEs and DELETEs log the index's columns of the old row; Debezium's key becomes those columns. Execute fails (not found) if the index does not exist; Postgres refuses one that is not unique, partial or on nullable columns."
	}
	return "WAL: UPDATEs and DELETEs log the primary key of the old row."
}

// bytesText renders a byte count for a preview.
func bytesText(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

// Execute sets the replica identity through the admin writer, after the scope
// gate and with no re-read: Validate ran right before it, and the ALTER is
// idempotent for the same mode.
func (a *setReplicaIdentity) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := parseReplicaIdentity(raw)
	if err != nil {
		return "", err
	}
	if _, _, err := ctxScope(ctx, SetReplicaIdentityName, func(pl owners.Pipeline) error { return replicaIdentityInScope(pl, p) }); err != nil {
		return "", err
	}
	if a.d.Writer == nil {
		return "", fmt.Errorf("sourceactions: %s: %w", SetReplicaIdentityName, errNoWriter)
	}
	if err := a.d.Writer.SetReplicaIdentity(ctx, p.Table, p.Mode, p.Index); err != nil {
		return "", fmt.Errorf("sourceactions: %s: %w", SetReplicaIdentityName, err)
	}
	return fmt.Sprintf("set REPLICA IDENTITY %s on %s. New UPDATEs and DELETEs use it; events already sent keep their old before image.", identityWord(p.Mode, p.Index), p.Table), nil
}
