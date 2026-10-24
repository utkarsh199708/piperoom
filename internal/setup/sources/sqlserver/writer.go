package sqlserver

import (
	"context"
	"fmt"

	"piperoom/internal/clients"
)

// Writer is the SQL Server clients.SetupSourceWriter of a setup room, wrapped
// around the writer that executes (internal/clients/sqlserver.SetupWriter on
// the runner, or the runner's remote client once it carries SQL Server ops).
// It adds the checks the contract cannot express per source before any I/O:
// rule 6 identifiers within sysname, capture instance names within
// sp_cdc_enable_table's limit, a non-reserved login, the canary nonce's shape,
// and AlterSystem as clients.ErrNotSupported (SQL Server has no ALTER SYSTEM;
// sp_configure is not on the setup allow-list). No method reads or returns a
// password.
type Writer struct{ w clients.SetupSourceWriter }

var _ clients.SetupSourceWriter = (*Writer)(nil)

// NewWriter wraps the source setup writer; a nil writer is ErrNoWriter.
func NewWriter(w clients.SetupSourceWriter) (*Writer, error) {
	if w == nil {
		return nil, ErrNoWriter
	}
	return &Writer{w: w}, nil
}

func (w *Writer) inner() (clients.SetupSourceWriter, error) {
	if w == nil || w.w == nil {
		return nil, ErrNoWriter
	}
	return w.w, nil
}

// CreatePublication enables CDC on the database and on tables under the
// capture instance name (name_schema_table for each of several tables).
func (w *Writer) CreatePublication(ctx context.Context, name string, tables []string) error {
	if err := checkTables(tables); err != nil {
		return fmt.Errorf("sqlserver writer: enable CDC: %w", err)
	}
	if err := checkCaptureInstance(name, tables); err != nil {
		return fmt.Errorf("sqlserver writer: enable CDC: %w", err)
	}
	in, err := w.inner()
	if err != nil {
		return err
	}
	if err := in.CreatePublication(ctx, name, tables); err != nil {
		return fmt.Errorf("sqlserver writer: enable CDC as %s: %w", name, err)
	}
	return nil
}

// CreateCanaryTable creates the ladder's canary table (nonce-keyed, CDC on);
// DefaultCanaryTable is its usual name.
func (w *Writer) CreateCanaryTable(ctx context.Context, table string) error {
	if _, _, err := tableParts(table); err != nil {
		return fmt.Errorf("sqlserver writer: create canary table: %w", err)
	}
	in, err := w.inner()
	if err != nil {
		return err
	}
	if err := in.CreateCanaryTable(ctx, table); err != nil {
		return fmt.Errorf("sqlserver writer: create canary table %s: %w", table, err)
	}
	return nil
}

// InsertCanary inserts one canary row with the ladder's nonce.
func (w *Writer) InsertCanary(ctx context.Context, table, nonce string) error {
	if _, _, err := tableParts(table); err != nil {
		return fmt.Errorf("sqlserver writer: insert canary: %w", err)
	}
	if !canaryNonceRe.MatchString(nonce) {
		return fmt.Errorf("%w: the canary nonce is 16 random bytes as 32 lower-case hex characters", ErrParams)
	}
	in, err := w.inner()
	if err != nil {
		return err
	}
	if err := in.InsertCanary(ctx, table, nonce); err != nil {
		return fmt.Errorf("sqlserver writer: insert canary into %s: %w", table, err)
	}
	return nil
}

// CreateReadRole creates the capture login with its grants and returns only
// the secret store's name for its generated password.
func (w *Writer) CreateReadRole(ctx context.Context, role string, tables []string) (string, error) {
	if err := checkLogin(role); err != nil {
		return "", fmt.Errorf("sqlserver writer: create login: %w", err)
	}
	if err := checkTables(tables); err != nil {
		return "", fmt.Errorf("sqlserver writer: create login %s: %w", role, err)
	}
	in, err := w.inner()
	if err != nil {
		return "", err
	}
	name, err := in.CreateReadRole(ctx, role, tables)
	if err != nil {
		return "", fmt.Errorf("sqlserver writer: create login %s: %w", role, err)
	}
	if !secretNameRe.MatchString(name) {
		return "", fmt.Errorf("sqlserver writer: create login %s: the writer returned no usable secret name: %w", role, ErrParams)
	}
	return name, nil
}

// AlterSystem has no SQL Server equivalent on the setup allow-list: it is
// clients.ErrNotSupported before any I/O, whatever the wrapped writer is.
func (w *Writer) AlterSystem(_ context.Context, name, _ string) (bool, error) {
	return false, fmt.Errorf("sqlserver writer: alter system %s: %w", clipName(name), clients.ErrNotSupported)
}
