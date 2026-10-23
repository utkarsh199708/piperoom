package remoteclients

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
)

// Source is the source Postgres client over the runner: the reads clients.SourceDB
// and the Q2a SourceSettingsReader take, the Q2b admin writes and the setup-only
// writes of clients.SetupSourceWriter. Every method is one op on the runner
// against the source database the room registered, so an admin write never runs
// on the server's own pool.
//
// Result shapes (the runner's OpResult.Data; each is pinned by a test):
//
//	source_settings             {"settings": [postgres.DBSetting, ...]}
//	source_table_replication    {"tables": [postgres.TableReplication, ...]}
//	source_role_info            {"role": postgres.RoleInfo}
//	source_blockers             {"sessions": [postgres.DBSession, ...]}
//	source_replication_slots    {"slots": [clients.ReplicationSlot, ...]}
//	source_recent_ddl           {"changes": [clients.DDLChange, ...]}
//	source_long_transactions    {"transactions": [clients.Transaction, ...]}
//	source_row                  {"row": {...} | null, "found": bool}
//	source_create_read_role     {"secret_name": "name"}
//	source_alter_system         {"pending_restart": bool}
//
// A write op returns Data only where the method has something to report.
type Source struct{ s *Session }

var (
	_ clients.SourceDB              = (*Source)(nil)
	_ postgres.SourceSettingsReader = (*Source)(nil)
	_ postgres.SourceAdminWriter    = (*Source)(nil)
	_ clients.SetupSourceWriter     = (*Source)(nil)
)

// ---- params and result envelopes ----

type tablesParams struct {
	Tables []string `json:"tables"`
}

type roleTablesParams struct {
	Role   string   `json:"role"`
	Tables []string `json:"tables"`
}

// blockersParams: Scoped tells the runner to anonymise every session outside
// Tables (BlockersOnTables), even when Tables is empty, as the postgres
// client does; without it a pipeline with no source_tables would read every
// session's identity.
type blockersParams struct {
	OlderThanMS int64    `json:"older_than_ms"`
	Scoped      bool     `json:"scoped,omitempty"`
	Tables      []string `json:"tables,omitempty"`
}

type recentDDLParams struct {
	Since time.Time `json:"since"`
	Limit int       `json:"limit"`
}

type olderThanParams struct {
	OlderThanMS int64 `json:"older_than_ms"`
}

type rowParams struct {
	Table    string `json:"table"`
	PKColumn string `json:"pk_column"`
	PK       string `json:"pk"`
}

type replicaIdentityParams struct {
	Table string `json:"table"`
	Mode  string `json:"mode"`
	Index string `json:"index,omitempty"`
}

type heartbeatTableParams struct {
	Table string `json:"table"`
}

type endSessionParams struct {
	PID       int64     `json:"pid"`
	StartedAt time.Time `json:"started_at"`
}

type publicationParams struct {
	Publication string   `json:"publication"`
	Tables      []string `json:"tables"`
}

type canaryInsertParams struct {
	Table string `json:"table"`
	Nonce string `json:"nonce"`
}

// alterSystemParams is plan.AlterSystemStep: the room's guard reads the
// parameter name through the same struct, so the allow-list judges the name
// this write is bound to.
type alterSystemParams = plan.AlterSystemStep

// ---- reads ----

// Settings reads the fixed CDC settings of the source (wal_level,
// max_replication_slots, max_wal_senders, max_slot_wal_keep_size,
// wal_sender_timeout and rds.logical_replication when the server has it),
// each with its pending_restart flag.
func (s *Source) Settings(ctx context.Context) ([]postgres.DBSetting, error) {
	res, err := s.s.do(ctx, OpSourceSettings, protocol.TierT0, nil)
	if err != nil {
		return nil, err
	}
	var out []postgres.DBSetting
	if err := decode(res, "settings", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// TableReplication reads each table's primary key presence and replica
// identity, in the order given. A table that does not exist is
// clients.ErrNotFound.
func (s *Source) TableReplication(ctx context.Context, tables []string) ([]postgres.TableReplication, error) {
	res, err := s.s.do(ctx, OpSourceTableReplication, protocol.TierT0, tablesParams{Tables: tables})
	if err != nil {
		return nil, err
	}
	var out []postgres.TableReplication
	if err := decode(res, "tables", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// RoleInfo reads whether role exists, can log in and may replicate, and its
// SELECT privilege on each table. A missing role is not an error.
func (s *Source) RoleInfo(ctx context.Context, role string, tables []string) (postgres.RoleInfo, error) {
	if err := requireArg(OpSourceRoleInfo, "role", role); err != nil {
		return postgres.RoleInfo{}, err
	}
	res, err := s.s.do(ctx, OpSourceRoleInfo, protocol.TierT0, roleTablesParams{Role: role, Tables: tables})
	if err != nil {
		return postgres.RoleInfo{}, err
	}
	var out postgres.RoleInfo
	if err := decode(res, "role", &out); err != nil {
		return postgres.RoleInfo{}, err
	}
	return out, nil
}

// Blockers lists the sessions that can hold back logical replication, with
// full identity on every row.
func (s *Source) Blockers(ctx context.Context, olderThan time.Duration) ([]postgres.DBSession, error) {
	return s.blockers(ctx, olderThan, nil, false)
}

// BlockersOnTables is Blockers limited to a pipeline's view: every blocker is
// listed, but a session that does not hold or wait for a lock on one of tables
// has its user, application and query blanked by the runner.
func (s *Source) BlockersOnTables(ctx context.Context, olderThan time.Duration, tables []string) ([]postgres.DBSession, error) {
	return s.blockers(ctx, olderThan, tables, true)
}

func (s *Source) blockers(ctx context.Context, olderThan time.Duration, tables []string, scoped bool) ([]postgres.DBSession, error) {
	if olderThan < 0 {
		return nil, fmt.Errorf("remoteclients: %s: negative age %s: %w", OpSourceBlockers, olderThan, ErrBadArgs)
	}
	res, err := s.s.do(ctx, OpSourceBlockers, protocol.TierT0,
		blockersParams{OlderThanMS: olderThan.Milliseconds(), Scoped: scoped, Tables: tables})
	if err != nil {
		return nil, err
	}
	var out []postgres.DBSession
	if err := decode(res, "sessions", &out); err != nil {
		return nil, err
	}
	if scoped {
		// The runner's anonymisation is not trusted alone: a session it
		// marks out of scope never carries an identity here, whatever the
		// runner's build sends.
		for i := range out {
			if !out[i].InScope {
				out[i].User, out[i].App, out[i].Query = "", "", ""
			}
		}
	}
	return out, nil
}

// ReplicationSlots reads the source's replication slots.
func (s *Source) ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error) {
	res, err := s.s.do(ctx, OpSourceReplicationSlots, protocol.TierT0, nil)
	if err != nil {
		return nil, err
	}
	var out []clients.ReplicationSlot
	if err := decode(res, "slots", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// RecentDDL reads the DDL audit rows since the given time, newest first.
func (s *Source) RecentDDL(ctx context.Context, since time.Time, limit int) ([]clients.DDLChange, error) {
	if limit < 0 {
		return nil, fmt.Errorf("remoteclients: %s: negative limit %d: %w", OpSourceRecentDDL, limit, ErrBadArgs)
	}
	res, err := s.s.do(ctx, OpSourceRecentDDL, protocol.TierT0, recentDDLParams{Since: since, Limit: limit})
	if err != nil {
		return nil, err
	}
	var out []clients.DDLChange
	if err := decode(res, "changes", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// LongTransactions lists open transactions older than olderThan.
func (s *Source) LongTransactions(ctx context.Context, olderThan time.Duration) ([]clients.Transaction, error) {
	if olderThan < 0 {
		return nil, fmt.Errorf("remoteclients: %s: negative age %s: %w", OpSourceLongTransactions, olderThan, ErrBadArgs)
	}
	res, err := s.s.do(ctx, OpSourceLongTransactions, protocol.TierT0, olderThanParams{OlderThanMS: olderThan.Milliseconds()})
	if err != nil {
		return nil, err
	}
	var out []clients.Transaction
	if err := decode(res, "transactions", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Row reads one row by primary key. table is "schema.table".
func (s *Source) Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error) {
	if err := requireArg(OpSourceRow, "table", table); err != nil {
		return nil, false, err
	}
	if err := requireArg(OpSourceRow, "pk_column", pkColumn); err != nil {
		return nil, false, err
	}
	res, err := s.s.do(ctx, OpSourceRow, protocol.TierT0, rowParams{Table: table, PKColumn: pkColumn, PK: pk})
	if err != nil {
		return nil, false, err
	}
	rawFound, err := field(res, "found")
	if err != nil {
		return nil, false, err
	}
	var found bool
	if err := json.Unmarshal(rawFound, &found); err != nil {
		return nil, false, fmt.Errorf("remoteclients: %s: decoding found: %w: %w", OpSourceRow, ErrBadResult, err)
	}
	if !found {
		return nil, false, nil
	}
	var row map[string]any
	if err := decode(res, "row", &row); err != nil {
		return nil, false, err
	}
	return row, true, nil
}

// ---- admin writes (Q2b) ----

// SetReplicaIdentity runs ALTER TABLE ... REPLICA IDENTITY for table
// ("schema.table"); mode is full, default or index, and only index takes an
// index name. T2.
func (s *Source) SetReplicaIdentity(ctx context.Context, table, mode, index string) error {
	if err := requireArg(OpSourceSetReplicaIdentity, "table", table); err != nil {
		return err
	}
	switch mode {
	case "full", "default":
		if index != "" {
			return fmt.Errorf("remoteclients: %s: table %s: mode %q takes no index: %w", OpSourceSetReplicaIdentity, table, mode, ErrBadArgs)
		}
	case "index":
		if index == "" {
			return fmt.Errorf("remoteclients: %s: table %s: mode index needs an index name: %w", OpSourceSetReplicaIdentity, table, ErrBadArgs)
		}
	default:
		return fmt.Errorf("remoteclients: %s: table %s: unknown mode %q: %w", OpSourceSetReplicaIdentity, table, mode, ErrBadArgs)
	}
	_, err := s.s.doStep(ctx, OpSourceSetReplicaIdentity, protocol.TierT2,
		replicaIdentityParams{Table: table, Mode: mode, Index: index})
	return err
}

// GrantSelect grants SELECT and nothing else on tables to role. T2.
func (s *Source) GrantSelect(ctx context.Context, role string, tables []string) error {
	if err := requireArg(OpSourceGrantSelect, "role", role); err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("remoteclients: %s: no tables: %w", OpSourceGrantSelect, ErrBadArgs)
	}
	_, err := s.s.doStep(ctx, OpSourceGrantSelect, protocol.TierT2, roleTablesParams{Role: role, Tables: tables})
	return err
}

// CreateHeartbeatTable creates the Debezium heartbeat table if it does not
// exist. T2.
func (s *Source) CreateHeartbeatTable(ctx context.Context, table string) error {
	if err := requireArg(OpSourceCreateHeartbeatTable, "table", table); err != nil {
		return err
	}
	_, err := s.s.doStep(ctx, OpSourceCreateHeartbeatTable, protocol.TierT2, heartbeatTableParams{Table: table})
	return err
}

// EndSession ends the backend pid after the runner re-reads it; a pid that no
// longer names the session that started at startedAt is postgres.ErrSessionChanged.
// T2.
func (s *Source) EndSession(ctx context.Context, pid int64, startedAt time.Time) error {
	if pid <= 0 {
		return fmt.Errorf("remoteclients: %s: pid %d is not a session id: %w", OpSourceEndSession, pid, ErrBadArgs)
	}
	if startedAt.IsZero() {
		return fmt.Errorf("remoteclients: %s: startedAt is the session's identity: %w", OpSourceEndSession, ErrBadArgs)
	}
	_, err := s.s.doStep(ctx, OpSourceEndSession, protocol.TierT2, endSessionParams{PID: pid, StartedAt: startedAt})
	return err
}

// ---- setup-only writes (clients.SetupSourceWriter) ----

// CreatePublication creates publication name FOR TABLE tables. T2.
func (s *Source) CreatePublication(ctx context.Context, name string, tables []string) error {
	if err := requireArg(OpSourceCreatePublication, "publication", name); err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("remoteclients: %s: no tables: %w", OpSourceCreatePublication, ErrBadArgs)
	}
	_, err := s.s.doStep(ctx, OpSourceCreatePublication, protocol.TierT2, publicationParams{Publication: name, Tables: tables})
	return err
}

// CreateCanaryTable creates the ladder's canary table (idempotent). T2.
func (s *Source) CreateCanaryTable(ctx context.Context, table string) error {
	if err := requireArg(OpSourceCreateCanaryTable, "table", table); err != nil {
		return err
	}
	_, err := s.s.doStep(ctx, OpSourceCreateCanaryTable, protocol.TierT2, heartbeatTableParams{Table: table})
	return err
}

// InsertCanary inserts one row with nonce into the canary table. T2.
func (s *Source) InsertCanary(ctx context.Context, table, nonce string) error {
	if err := requireArg(OpSourceInsertCanary, "table", table); err != nil {
		return err
	}
	if err := requireArg(OpSourceInsertCanary, "nonce", nonce); err != nil {
		return err
	}
	_, err := s.s.doStep(ctx, OpSourceInsertCanary, protocol.TierT2, canaryInsertParams{Table: table, Nonce: nonce})
	return err
}

// CreateReadRole creates the read role with its grants and a runner-generated
// password, and returns only the secret store's name for it. T2. The password
// is never in the result, this package, or an error.
func (s *Source) CreateReadRole(ctx context.Context, role string, tables []string) (string, error) {
	if err := requireArg(OpSourceCreateReadRole, "role", role); err != nil {
		return "", err
	}
	if len(tables) == 0 {
		return "", fmt.Errorf("remoteclients: %s: no tables: %w", OpSourceCreateReadRole, ErrBadArgs)
	}
	res, err := s.s.doStep(ctx, OpSourceCreateReadRole, protocol.TierT2, roleTablesParams{Role: role, Tables: tables})
	if err != nil {
		return "", err
	}
	var secretName string
	if err := decode(res, "secret_name", &secretName); err != nil {
		return "", err
	}
	if secretName == "" {
		return "", fmt.Errorf("remoteclients: %s: the runner returned no secret name: %w", OpSourceCreateReadRole, ErrBadResult)
	}
	return secretName, nil
}

// AlterSystem sets one allow-listed server setting and reports whether the
// change needs a restart. It is the T3 op of the contract: the room creates it
// only inside a maintenance window and the runner asks for a "y" on its
// terminal.
func (s *Source) AlterSystem(ctx context.Context, name, value string) (bool, error) {
	if err := requireArg(OpSourceAlterSystem, "setting", name); err != nil {
		return false, err
	}
	res, err := s.s.doStep(ctx, OpSourceAlterSystem, protocol.TierT3, alterSystemParams{Name: name, Value: value})
	if err != nil {
		return false, err
	}
	var pending bool
	if err := decode(res, "pending_restart", &pending); err != nil {
		return false, err
	}
	return pending, nil
}
