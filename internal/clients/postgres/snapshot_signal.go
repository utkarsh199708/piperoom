package postgres

import (
	"context"
	"fmt"
	"time"

	"piperoom/internal/clients/pgconv"
)

// SnapshotSignalState is what pg_snapshot_signal (internal/actions/pgactions)
// reads before it proposes, previews or runs an INSERT into a Debezium signal
// table: the signal table and the tables to snapshot. It is read on the
// read-only pool, never the writer's.
type SnapshotSignalState struct {
	// PublicationExists: the connector's publication exists.
	PublicationExists bool `json:"publication_exists"`
	// SignalExists: the signal table (signal.data.collection) exists.
	SignalExists bool `json:"signal_exists"`
	// SignalColumns: the signal table has the id, type and data columns
	// Debezium reads (and the writer inserts).
	SignalColumns bool `json:"signal_columns"`
	// SignalPublished: the signal table's rows are in the publication, so
	// the connector sees the signal row in its change stream.
	SignalPublished bool `json:"signal_published"`
	// Tables are the tables to snapshot, in the order asked.
	Tables []SnapshotTableState `json:"tables"`
}

// SnapshotTableState is one table of SnapshotSignalState.
type SnapshotTableState struct {
	Table string `json:"table"` // "schema.table"
	// Exists: an ordinary or partitioned table of that name exists.
	Exists bool `json:"exists"`
	// EstimatedRows is pg_class.reltuples, -1 when never analyzed or the
	// table does not exist.
	EstimatedRows int64 `json:"estimated_rows"`
}

// snapshotSignalSQL reads publication $1, signal table $2.$3 and the tables
// $4 (schemas) / $5 (names), in pairs, in one statement. Every input travels
// as a parameter; the per-table arrays keep the order of the input.
var snapshotSignalSQL = `
		WITH ` + publishedCTEs + `,
		asked AS (
			SELECT v.o, c.oid, c.reltuples
			FROM unnest($4::text[], $5::text[]) WITH ORDINALITY AS v(schemaname, tablename, o)
			LEFT JOIN pg_namespace n ON n.nspname = v.schemaname::name
			LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = v.tablename::name AND c.relkind IN ('r', 'p'))
		SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1),
		       sc.oid IS NOT NULL,
		       COALESCE((SELECT count(*) FROM pg_attribute a
		                 WHERE a.attrelid = sc.oid AND a.attnum > 0 AND NOT a.attisdropped
		                   AND a.attname IN ('id', 'type', 'data')) = 3, false),
		       COALESCE(sc.oid IS NOT NULL AND ` + publishedMember("sc.oid") + `, false),
		       ARRAY(SELECT oid IS NOT NULL FROM asked ORDER BY o),
		       ARRAY(SELECT COALESCE(reltuples::bigint, -1) FROM asked ORDER BY o)
		FROM (SELECT 1) AS one
		LEFT JOIN pg_namespace sn ON sn.nspname = $2::name
		LEFT JOIN pg_class sc ON sc.relnamespace = sn.oid AND sc.relname = $3::name`

// SnapshotSignalState reads, for pg_snapshot_signal, whether publication
// exists, whether signalTable ("schema.table") exists with the id, type and
// data columns and is published by publication, and each of tables
// ("schema.table") with its row estimate. Identifiers are checked before any
// I/O and travel as parameters. Not on clients.SourceDB: the action finds it
// with a type assertion, like PublicationAddState.
func (d *DB) SnapshotSignalState(ctx context.Context, publication, signalTable string, tables []string) (SnapshotSignalState, error) {
	pub, err := validateIdent(publication)
	if err != nil {
		return SnapshotSignalState{}, err
	}
	sigSchema, sigName, err := splitTable(signalTable)
	if err != nil {
		return SnapshotSignalState{}, err
	}
	schemas, names := make([]string, 0, len(tables)), make([]string, 0, len(tables))
	for _, t := range tables {
		s, n, err := splitTable(t)
		if err != nil {
			return SnapshotSignalState{}, err
		}
		schemas, names = append(schemas, s), append(names, n)
	}
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	var st SnapshotSignalState
	var exists []bool
	var rows []int64
	err = d.queryRow(ctx, snapshotSignalSQL, pub, sigSchema, sigName, schemas, names).Scan(
		&st.PublicationExists, &st.SignalExists, &st.SignalColumns, &st.SignalPublished, &exists, &rows)
	if err != nil {
		return SnapshotSignalState{}, fmt.Errorf("postgres: read signal table %s and %d snapshot tables: %w", signalTable, len(tables), err)
	}
	if len(exists) != len(tables) || len(rows) != len(tables) {
		return SnapshotSignalState{}, fmt.Errorf("postgres: read snapshot tables: got %d/%d rows for %d tables", len(exists), len(rows), len(tables))
	}
	st.Tables = make([]SnapshotTableState, len(tables))
	for i, t := range tables {
		st.Tables[i] = SnapshotTableState{Table: t, Exists: exists[i], EstimatedRows: rows[i]}
		if !exists[i] {
			st.Tables[i].EstimatedRows = -1
		}
	}
	return st, nil
}
