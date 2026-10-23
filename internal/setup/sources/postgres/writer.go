package postgres

import (
	"context"
	"fmt"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/setup/plan"
)

// Writer is the Postgres SetupSourceWriter of a setup room
// (clients.SetupSourceWriter, docs/design/setup-agent.md §4 item 4). It adds
// what the contract cannot express per source: identifier checks before any
// I/O, the ALTER SYSTEM allow-list, the canary table's shape, the read role's
// exact grants, and the filtered/FOR ALL TABLES publication choice. The writes
// themselves are the runner's ops: this type holds no pool and no credential,
// and no method reads or returns a password.
type Writer struct{ w clients.SetupSourceWriter }

// NewWriter wraps the source setup writer. A nil writer is ErrNoWriter, so a
// setup room with write mode off gets a refusal at construction, not at the
// first write.
func NewWriter(w clients.SetupSourceWriter) (*Writer, error) {
	if w == nil {
		return nil, ErrNoWriter
	}
	return &Writer{w: w}, nil
}

// PublicationMode is how a publication lists its tables.
type PublicationMode string

const (
	// PublicationFiltered creates CREATE PUBLICATION ... FOR TABLE <tables>:
	// only the pipeline's tables are decoded.
	PublicationFiltered PublicationMode = "filtered"
	// PublicationAllTables creates CREATE PUBLICATION ... FOR ALL TABLES.
	PublicationAllTables PublicationMode = "all_tables"
)

// Publication is the publication a setup creates.
type Publication struct {
	Name   string
	Mode   PublicationMode
	Tables []string
}

// AllTablesPublisher is the optional capability a FOR ALL TABLES publication
// needs. clients.SetupSourceWriter.CreatePublication is the table-filtered
// form, so a writer without this method cannot create the unfiltered one; the
// method is a local adapter until the contract (or the runner's
// source_create_publication op) carries a mode (see the PR's contract change
// request).
type AllTablesPublisher interface {
	CreateAllTablesPublication(ctx context.Context, name string) error
}

// CreatePublication creates p.Name as a filtered or FOR ALL TABLES
// publication. A filtered publication lists exactly p.Tables; the unfiltered
// one publishes every current and future table and so takes no table list.
func (w *Writer) CreatePublication(ctx context.Context, p Publication) error {
	if w == nil || w.w == nil {
		return ErrNoWriter
	}
	if _, err := quoteIdent(p.Name); err != nil {
		return fmt.Errorf("postgres writer: create publication: %w", err)
	}
	switch p.Mode {
	case "", PublicationFiltered:
		if err := checkTables(p.Tables); err != nil {
			return fmt.Errorf("postgres writer: create publication %s: %w", p.Name, err)
		}
		if err := w.w.CreatePublication(ctx, p.Name, p.Tables); err != nil {
			return fmt.Errorf("postgres writer: create publication %s: %w", p.Name, err)
		}
		return nil
	case PublicationAllTables:
		if len(p.Tables) != 0 {
			return fmt.Errorf("%w: a FOR ALL TABLES publication lists no tables", ErrParams)
		}
		aw, ok := w.w.(AllTablesPublisher)
		if !ok {
			return fmt.Errorf("postgres writer: create publication %s: the setup writer has no FOR ALL TABLES form: %w", p.Name, clients.ErrNotSupported)
		}
		if err := aw.CreateAllTablesPublication(ctx, p.Name); err != nil {
			return fmt.Errorf("postgres writer: create publication %s: %w", p.Name, err)
		}
		return nil
	default:
		return fmt.Errorf("%w: publication mode %q (filtered or all_tables)", ErrParams, clipName(string(p.Mode)))
	}
}

// CreateCanaryTable creates the ladder's canary table (idempotent). Its shape
// is CanaryTableSQL; the connector needs a primary key, which the id column
// gives it.
func (w *Writer) CreateCanaryTable(ctx context.Context, table string) error {
	if w == nil || w.w == nil {
		return ErrNoWriter
	}
	if _, err := quoteTable(table); err != nil {
		return fmt.Errorf("postgres writer: create canary table: %w", err)
	}
	if err := w.w.CreateCanaryTable(ctx, table); err != nil {
		return fmt.Errorf("postgres writer: create canary table %s: %w", table, err)
	}
	return nil
}

// InsertCanary inserts one row with nonce (16 random bytes, hex, from the
// ladder runner) into the canary table.
func (w *Writer) InsertCanary(ctx context.Context, table, nonce string) error {
	if w == nil || w.w == nil {
		return ErrNoWriter
	}
	if _, err := quoteTable(table); err != nil {
		return fmt.Errorf("postgres writer: insert canary: %w", err)
	}
	if !canaryNonceRe.MatchString(nonce) {
		return fmt.Errorf("%w: the canary nonce is 16 random bytes as 32 lower-case hex characters", ErrParams)
	}
	if err := w.w.InsertCanary(ctx, table, nonce); err != nil {
		return fmt.Errorf("postgres writer: insert canary into %s: %w", table, err)
	}
	return nil
}

// CreateReadRole creates role with the ReadRoleGrants grants, a
// runner-generated password and the REPLICATION attribute the connector's
// slot needs (the runner creates the role; this package never sees the
// password), and returns only the name the runner stored the password under.
// The password is never in a return value, an error or this package.
func (w *Writer) CreateReadRole(ctx context.Context, role string, tables []string) (string, error) {
	if w == nil || w.w == nil {
		return "", ErrNoWriter
	}
	if _, err := quoteRole(role); err != nil {
		return "", fmt.Errorf("postgres writer: create read role: %w", err)
	}
	if err := checkTables(tables); err != nil {
		return "", fmt.Errorf("postgres writer: create read role %s: %w", role, err)
	}
	name, err := w.w.CreateReadRole(ctx, role, tables)
	if err != nil {
		return "", fmt.Errorf("postgres writer: create read role %s: %w", role, err)
	}
	if !secretNameRe.MatchString(name) {
		return "", fmt.Errorf("postgres writer: create read role %s: the runner returned no usable secret name: %w", role, ErrParams)
	}
	return name, nil
}

// AlterSystem sets one allow-listed server setting and reports whether it
// needs a restart. A name outside plan.AlterSystemParams is refused before any
// I/O: the refusal is decided here, never by the endpoint.
func (w *Writer) AlterSystem(ctx context.Context, name, value string) (bool, error) {
	if !plan.AlterSystemAllowed(name) {
		return false, fmt.Errorf("%w: %q is not on the ALTER SYSTEM allow-list (%s)", ErrParams, clipName(name), strings.Join(plan.AlterSystemParams(), ", "))
	}
	if w == nil || w.w == nil {
		return false, ErrNoWriter
	}
	if !settingValueRe.MatchString(value) {
		return false, fmt.Errorf("%w: the value of %s must be letters, digits, '.', '_', '+', '-' or a space (at most 64 bytes)", ErrParams, name)
	}
	pending, err := w.w.AlterSystem(ctx, name, value)
	if err != nil {
		return false, fmt.Errorf("postgres writer: alter system %s: %w", name, err)
	}
	return pending, nil
}

// CanaryTableSQL is the DDL of the ladder's canary table: an identity primary
// key (so REPLICA IDENTITY DEFAULT carries the key), the nonce the ladder looks
// for, and the insert time.
func CanaryTableSQL(table string) (string, error) {
	q, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	return "CREATE TABLE IF NOT EXISTS " + q + " (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, nonce text NOT NULL, inserted_at timestamptz NOT NULL DEFAULT now())", nil
}

// InsertCanarySQL is the canary insert; the nonce is the one parameter.
func InsertCanarySQL(table string) (string, error) {
	q, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	return "INSERT INTO " + q + " (nonce) VALUES ($1)", nil
}

// ReadRoleGrants is every statement CreateReadRole runs for role besides
// creating it: SELECT on exactly the listed tables and membership in
// pg_read_all_stats, nothing else (no schema-wide or ALL TABLES grant, no
// write privilege, no pg_monitor).
func ReadRoleGrants(role string, tables []string) ([]string, error) {
	rq, err := quoteRole(role)
	if err != nil {
		return nil, err
	}
	if err := checkTables(tables); err != nil {
		return nil, err
	}
	quoted := make([]string, len(tables))
	for i, t := range tables {
		if quoted[i], err = quoteTable(t); err != nil {
			return nil, err
		}
	}
	stats, err := quoteIdent(StatsRole)
	if err != nil {
		return nil, err
	}
	return []string{
		"GRANT SELECT ON TABLE " + strings.Join(quoted, ", ") + " TO " + rq,
		"GRANT " + stats + " TO " + rq,
	}, nil
}

// CreatePublicationSQL is the filtered publication's DDL.
func CreatePublicationSQL(name string, tables []string) (string, error) {
	q, err := quoteIdent(name)
	if err != nil {
		return "", err
	}
	if err := checkTables(tables); err != nil {
		return "", err
	}
	quoted := make([]string, len(tables))
	for i, t := range tables {
		if quoted[i], err = quoteTable(t); err != nil {
			return "", err
		}
	}
	return "CREATE PUBLICATION " + q + " FOR TABLE " + strings.Join(quoted, ", "), nil
}

// CreateAllTablesPublicationSQL is the unfiltered publication's DDL.
func CreateAllTablesPublicationSQL(name string) (string, error) {
	q, err := quoteIdent(name)
	if err != nil {
		return "", err
	}
	return "CREATE PUBLICATION " + q + " FOR ALL TABLES", nil
}
