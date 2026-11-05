package oracle

import (
	"context"
	"fmt"

	"piperoom/internal/clients"
)

// Writer is the Oracle clients.SetupSourceWriter of a setup room
// (docs/design/setup-agent.md §4 item 4), wrapped around the
// SetupSourceWriter that executes the writes: the runner's remote client in a
// setup room, or the U9a client's oracle.Writer when the runner holds its own
// write pool. It adds what the contract cannot express per source: Oracle
// identifier checks (upper case, as the dictionary stores names) and the
// canary nonce's shape before any I/O, and the refusal of every ALTER SYSTEM,
// because Oracle's one setup setting (archive log mode) needs a restart no
// op performs. No method reads or returns a password.
type Writer struct{ w clients.SetupSourceWriter }

var _ clients.SetupSourceWriter = (*Writer)(nil)

// NewWriter wraps the source setup writer. A nil writer is ErrNoWriter, so a
// setup room with write mode off gets a refusal at construction.
func NewWriter(w clients.SetupSourceWriter) (*Writer, error) {
	if w == nil {
		return nil, ErrNoWriter
	}
	return &Writer{w: w}, nil
}

// CreatePublication adds the unconditional ALL COLUMNS supplemental log group
// to each table: Oracle has no publication object, and this is what the
// Debezium Oracle connector needs in its place (the U9a writer ignores name).
func (w *Writer) CreatePublication(ctx context.Context, name string, tables []string) error {
	if err := checkTables(tables); err != nil {
		return fmt.Errorf("oracle writer: supplemental logging: %w", err)
	}
	if w == nil || w.w == nil {
		return ErrNoWriter
	}
	if err := w.w.CreatePublication(ctx, name, tables); err != nil {
		return fmt.Errorf("oracle writer: supplemental logging: %w", err)
	}
	return nil
}

// CreateCanaryTable creates the ladder's canary table (idempotent) with its
// ALL COLUMNS supplemental log group; Driver.CanaryTable names it.
func (w *Writer) CreateCanaryTable(ctx context.Context, table string) error {
	if _, err := quoteTable(table); err != nil {
		return fmt.Errorf("oracle writer: create canary table: %w", err)
	}
	if w == nil || w.w == nil {
		return ErrNoWriter
	}
	if err := w.w.CreateCanaryTable(ctx, table); err != nil {
		return fmt.Errorf("oracle writer: create canary table %s: %w", clipName(table), err)
	}
	return nil
}

// InsertCanary inserts one row with nonce (16 random bytes, hex, from the
// ladder runner) into the canary table.
func (w *Writer) InsertCanary(ctx context.Context, table, nonce string) error {
	if _, err := quoteTable(table); err != nil {
		return fmt.Errorf("oracle writer: insert canary: %w", err)
	}
	if !canaryNonceRe.MatchString(nonce) {
		return fmt.Errorf("%w: the canary nonce is 16 random bytes as 32 lower-case hex characters", ErrParams)
	}
	if w == nil || w.w == nil {
		return ErrNoWriter
	}
	if err := w.w.InsertCanary(ctx, table, nonce); err != nil {
		return fmt.Errorf("oracle writer: insert canary into %s: %w", clipName(table), err)
	}
	return nil
}

// CreateReadRole creates the LogMiner user with the Debezium docs' grants and
// SELECT on tables, and returns only the secret-store name of its generated
// password. It suits a non-CDB: a CDB's common user needs CONTAINER=ALL
// grants the U9a writer does not issue, so the driver gives an ask there.
func (w *Writer) CreateReadRole(ctx context.Context, role string, tables []string) (string, error) {
	if err := checkUser(role); err != nil {
		return "", fmt.Errorf("oracle writer: create read role: %w", err)
	}
	if err := checkTables(tables); err != nil {
		return "", fmt.Errorf("oracle writer: create read role %s: %w", clipName(role), err)
	}
	if w == nil || w.w == nil {
		return "", ErrNoWriter
	}
	name, err := w.w.CreateReadRole(ctx, role, tables)
	if err != nil {
		return "", fmt.Errorf("oracle writer: create read role %s: %w", clipName(role), err)
	}
	if !secretNameRe.MatchString(name) {
		return "", fmt.Errorf("oracle writer: create read role %s: the writer returned no usable secret name: %w", clipName(role), ErrParams)
	}
	return name, nil
}

// AlterSystem refuses before any I/O. The only Oracle setting a setup needs is
// archive log mode, and ALTER DATABASE ARCHIVELOG runs only on a MOUNTed
// instance, so it is clients.ErrNotSupported with the DBA's steps (the
// archive_log_mode check's ask); every other name is ErrParams.
func (w *Writer) AlterSystem(ctx context.Context, name, value string) (bool, error) {
	if name == "archive_log" {
		return false, fmt.Errorf("oracle writer: alter system archive_log: %w: switching archive log mode restarts the instance; a DBA runs %s", clients.ErrNotSupported, archiveLogSteps)
	}
	return false, fmt.Errorf("%w: %q is not a setting the Oracle setup changes (archive log mode is the only one, and it is a DBA's restart)", ErrParams, clipName(name))
}
