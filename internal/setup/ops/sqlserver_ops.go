package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"piperoom/internal/clients"
	sqlserver "piperoom/internal/clients/sqlserver/sqlservertypes"
	"piperoom/internal/events"
	"piperoom/internal/setup/errcode"
	"piperoom/internal/setup/protocol"
)

// The SQL Server source ops (row U4e): the reads U4b's prerequisite driver
// (internal/setup/sources/sqlserver) checks with, over a *sqlserver.Client,
// and the four setup-only source writes over a sqlserver.SetupWriter. They
// carry the names remoteclients sends and the U4b driver names as its ReadOp
// and FixOp, so a SQL Server room's driver runs through a paired runner
// unchanged.
//
// They are registered by RegisterSQLServerSource, not by New: the read op
// source_role_info and the four write ops share their names with the other
// sources' ops (the wire is source-neutral), and a runner serves one source.
//
// The target kind is "postgres": events.SetupTarget has no per-source kind
// yet, and the room registers the source database of every source kind under
// that one (internal/room setupTargetKinds), so requirePostgresTarget is the
// "the room registered a source database" check here too.

// Names of the SQL Server read ops. The write ops keep the names of
// plan's setup-only source ops (source_create_publication, ...).
const (
	OpSourceServerInfo  = "source_server_info"
	OpSourceAgentState  = "source_agent_state"
	OpSourceDatabaseCDC = "source_database_cdc"
	OpSourceTableCDC    = "source_table_cdc"
	OpSourceRoleInfo    = "source_role_info"

	OpSourceCreatePublication = "source_create_publication"
	OpSourceCreateCanaryTable = "source_create_canary_table"
	OpSourceInsertCanary      = "source_insert_canary"
	OpSourceCreateReadRole    = "source_create_read_role"
)

// sqlserverLayer is the owners layer of a SQL Server source
// (owners.LayerSQLServer).
const sqlserverLayer = "sqlserver"

// maxSysnameBytes is SQL Server's sysname limit (nvarchar(128)); the U4b
// driver checks the same bound.
const maxSysnameBytes = 128

// maxSQLServerTables caps the tables one request may name: a pipeline's
// source tables, not a catalogue scan.
const maxSQLServerTables = 1000

// maxNonceBytes bounds the ladder's canary nonce.
const maxNonceBytes = 128

// nonceRe is the shape of the ladder's nonce: the server draws it, so it is
// never free text.
var nonceRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// SQLServerSource is the runner's SQL Server source: the read-only client the
// reads call and the setup writer the writes call. A nil field leaves its ops
// registered but answering not_supported, the convention of the Postgres
// source op (ErrNoSource), so a runner with no source connection refuses with
// a reason the driver can act on rather than an unknown op.
type SQLServerSource struct {
	Reader sqlserver.SettingsReader
	Writer clients.SetupSourceWriter
}

// The real client and writer satisfy SettingsReader and
// clients.SetupSourceWriter: sqlserver_ops_test.go asserts it, so this file
// does not import internal/clients/sqlserver (and go-mssqldb with it) into
// every binary that links the catalogue, the server among them.

// RegisterSQLServerSource adds the SQL Server source ops to r: the five T0
// reads and the four T2 setup writes (their U1-2b floors).
func RegisterSQLServerSource(r *Registry, src SQLServerSource) error {
	if r == nil {
		return errors.New("ops: a registry is required")
	}
	for _, op := range []Op{
		mssqlServerInfoOp{src: src}, mssqlAgentStateOp{src: src}, mssqlDatabaseCDCOp{src: src},
		mssqlTableCDCOp{src: src}, mssqlRoleInfoOp{src: src},
		mssqlCreatePublicationOp{src: src}, mssqlCreateCanaryTableOp{src: src},
		mssqlInsertCanaryOp{src: src}, mssqlCreateReadRoleOp{src: src},
	} {
		if err := r.Register(op); err != nil {
			return fmt.Errorf("ops: registering the SQL Server source: %w", err)
		}
	}
	return nil
}

// ---- shared params and checks ----

type mssqlNoParams struct{}

type mssqlDatabaseParams struct {
	Database string `json:"database"`
}

type mssqlTablesParams struct {
	Tables []string `json:"tables"`
}

type mssqlRoleParams struct {
	Role   string   `json:"role"`
	Tables []string `json:"tables"`
}

type mssqlPublicationParams struct {
	Publication string   `json:"publication"`
	Tables      []string `json:"tables"`
}

type mssqlTableParams struct {
	Table string `json:"table"`
}

type mssqlCanaryParams struct {
	Table string `json:"table"`
	Nonce string `json:"nonce"`
}

// checkSysname is checkIdent at SQL Server's sysname bound.
func checkSysname(field, v string) error {
	if v == "" {
		return fmt.Errorf("%w: %s is required", ErrParams, field)
	}
	if len(v) > maxSysnameBytes || !identRe.MatchString(v) {
		return fmt.Errorf("%w: %s %q is not an identifier (%s, at most %d bytes)", ErrParams, field, clip(v), identRe.String(), maxSysnameBytes)
	}
	return nil
}

// checkSQLServerTable checks one "schema.table" name.
func checkSQLServerTable(t string) error {
	schema, table, ok := strings.Cut(t, ".")
	if !ok {
		return fmt.Errorf("%w: table %q is not \"schema.table\"", ErrParams, clip(t))
	}
	if err := checkSysname("table schema", schema); err != nil {
		return err
	}
	return checkSysname("table", table)
}

// checkSQLServerTables checks a table list: bounded, every name an identifier
// pair. required refuses an empty list (a write that names no table).
func checkSQLServerTables(tables []string, required bool) error {
	if required && len(tables) == 0 {
		return fmt.Errorf("%w: tables is required", ErrParams)
	}
	if len(tables) > maxSQLServerTables {
		return fmt.Errorf("%w: %d tables, at most %d", ErrParams, len(tables), maxSQLServerTables)
	}
	for _, t := range tables {
		if err := checkSQLServerTable(t); err != nil {
			return err
		}
	}
	return nil
}

// tablesSchema is the bounded schema.table list of a SQL Server op.
func tablesSchema() map[string]any {
	return map[string]any{
		"type": "array", "maxItems": maxSQLServerTables,
		"items": map[string]any{"type": "string", "pattern": `^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$`},
	}
}

func sysnameSchema() map[string]any {
	return map[string]any{"type": "string", "pattern": identRe.String(), "maxLength": maxSysnameBytes}
}

func closedSchema(props map[string]any, required ...string) map[string]any {
	if props == nil {
		props = map[string]any{}
	}
	if required == nil {
		required = []string{}
	}
	return map[string]any{"type": "object", "additionalProperties": false, "required": required, "properties": props}
}

// noSQLServer is the answer of an op whose half of the source is not
// configured on this runner.
func noSQLServer(what string) (Result, error) {
	return Result{
		Summary: "no SQL Server " + what + " on this runner",
		Data:    map[string]string{"error_code": errcode.NotSupported},
	}, ErrNoSource
}

// mssqlFailure maps a client error to the op's result: a missing object
// carries error_code not_found (the driver reads it as the check's failure),
// an unsupported call not_supported; anything else is a plain failure.
func mssqlFailure(name string, err error) (Result, error) {
	switch {
	case errors.Is(err, clients.ErrNotFound):
		return Result{Summary: name + ": not found", Data: map[string]string{"error_code": errcode.NotFound}}, fmt.Errorf("ops: %s: %w", name, err)
	case errors.Is(err, clients.ErrNotSupported):
		return Result{Summary: name + ": not supported", Data: map[string]string{"error_code": errcode.NotSupported}}, fmt.Errorf("ops: %s: %w", name, err)
	}
	return Result{}, fmt.Errorf("ops: %s: %w", name, err)
}

// sqlReceipt is the receipt of a SQL Server write: kind sql, the object the
// write reached, never a credential.
func sqlReceipt(target string) events.SetupReceipt {
	return events.SetupReceipt{Kind: "sql", Target: target}
}

// sqlserverTarget is the target kind every SQL Server op reaches: the room's
// source database (see the file comment).
const sqlserverTarget = "postgres"

// mssqlBase is the state every SQL Server op shares; each op is a named type
// over it.
type mssqlBase struct{ src SQLServerSource }

// ---- reads (T0) ----

type mssqlServerInfoOp mssqlBase

func (mssqlServerInfoOp) Name() string                 { return OpSourceServerInfo }
func (mssqlServerInfoOp) Target() string               { return sqlserverTarget }
func (mssqlServerInfoOp) Tier() string                 { return protocol.TierT0 }
func (mssqlServerInfoOp) Layer() string                { return sqlserverLayer }
func (mssqlServerInfoOp) Describe() string             { return "read the SQL Server version and edition" }
func (mssqlServerInfoOp) ParamsSchema() map[string]any { return closedSchema(nil) }
func (mssqlServerInfoOp) Validate(params json.RawMessage, _ Env) error {
	return decodeParams(params, &mssqlNoParams{})
}
func (op mssqlServerInfoOp) checkTarget(params json.RawMessage, env Env) error {
	if err := op.Validate(params, env); err != nil {
		return err
	}
	return requirePostgresTarget(env)
}

func (op mssqlServerInfoOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	if err := op.checkTarget(params, env); err != nil {
		return Result{}, err
	}
	if op.src.Reader == nil {
		return noSQLServer("reader")
	}
	info, err := op.src.Reader.ServerInfo(ctx)
	if err != nil {
		return mssqlFailure(op.Name(), err)
	}
	return Result{
		Summary: fmt.Sprintf("SQL Server %s (%s)", clip(info.Version), clip(info.Edition)),
		Data:    map[string]any{"server": info},
	}, nil
}

type mssqlAgentStateOp mssqlBase

func (mssqlAgentStateOp) Name() string                 { return OpSourceAgentState }
func (mssqlAgentStateOp) Target() string               { return sqlserverTarget }
func (mssqlAgentStateOp) Tier() string                 { return protocol.TierT0 }
func (mssqlAgentStateOp) Layer() string                { return sqlserverLayer }
func (mssqlAgentStateOp) Describe() string             { return "read whether SQL Server Agent is running" }
func (mssqlAgentStateOp) ParamsSchema() map[string]any { return closedSchema(nil) }
func (mssqlAgentStateOp) Validate(params json.RawMessage, _ Env) error {
	return decodeParams(params, &mssqlNoParams{})
}
func (op mssqlAgentStateOp) checkTarget(params json.RawMessage, env Env) error {
	if err := op.Validate(params, env); err != nil {
		return err
	}
	return requirePostgresTarget(env)
}

func (op mssqlAgentStateOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	if err := op.checkTarget(params, env); err != nil {
		return Result{}, err
	}
	if op.src.Reader == nil {
		return noSQLServer("reader")
	}
	st, err := op.src.Reader.AgentState(ctx)
	if err != nil {
		return mssqlFailure(op.Name(), err)
	}
	return Result{
		Summary: "SQL Server Agent: " + clip(st.Status),
		Data:    map[string]any{"agent": st},
	}, nil
}

type mssqlDatabaseCDCOp mssqlBase

func (mssqlDatabaseCDCOp) Name() string   { return OpSourceDatabaseCDC }
func (mssqlDatabaseCDCOp) Target() string { return sqlserverTarget }
func (mssqlDatabaseCDCOp) Tier() string   { return protocol.TierT0 }
func (mssqlDatabaseCDCOp) Layer() string  { return sqlserverLayer }
func (mssqlDatabaseCDCOp) Describe() string {
	return "read whether CDC is enabled on one SQL Server database"
}
func (mssqlDatabaseCDCOp) ParamsSchema() map[string]any {
	return closedSchema(map[string]any{"database": sysnameSchema()}, "database")
}
func (mssqlDatabaseCDCOp) decode(params json.RawMessage) (mssqlDatabaseParams, error) {
	var p mssqlDatabaseParams
	if err := decodeParams(params, &p); err != nil {
		return p, err
	}
	return p, checkSysname("database", p.Database)
}
func (op mssqlDatabaseCDCOp) Validate(params json.RawMessage, _ Env) error {
	_, err := op.decode(params)
	return err
}
func (op mssqlDatabaseCDCOp) checkTarget(params json.RawMessage, env Env) error {
	if _, err := op.decode(params); err != nil {
		return err
	}
	return requirePostgresTarget(env)
}

func (op mssqlDatabaseCDCOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	p, err := op.decode(params)
	if err != nil {
		return Result{}, err
	}
	if err := requirePostgresTarget(env); err != nil {
		return Result{}, err
	}
	if op.src.Reader == nil {
		return noSQLServer("reader")
	}
	on, err := op.src.Reader.DatabaseCDC(ctx, p.Database)
	if err != nil {
		return mssqlFailure(op.Name(), err)
	}
	state := "off"
	if on {
		state = "on"
	}
	return Result{
		Summary: fmt.Sprintf("database %s: CDC %s", p.Database, state),
		Data:    map[string]any{"enabled": on},
	}, nil
}

type mssqlTableCDCOp mssqlBase

func (mssqlTableCDCOp) Name() string   { return OpSourceTableCDC }
func (mssqlTableCDCOp) Target() string { return sqlserverTarget }
func (mssqlTableCDCOp) Tier() string   { return protocol.TierT0 }
func (mssqlTableCDCOp) Layer() string  { return sqlserverLayer }
func (mssqlTableCDCOp) Describe() string {
	return "read CDC tracking and the primary key of each SQL Server source table"
}
func (mssqlTableCDCOp) ParamsSchema() map[string]any {
	return closedSchema(map[string]any{"tables": tablesSchema()}, "tables")
}
func (mssqlTableCDCOp) decode(params json.RawMessage) (mssqlTablesParams, error) {
	var p mssqlTablesParams
	if err := decodeParams(params, &p); err != nil {
		return p, err
	}
	return p, checkSQLServerTables(p.Tables, false)
}
func (op mssqlTableCDCOp) Validate(params json.RawMessage, _ Env) error {
	_, err := op.decode(params)
	return err
}
func (op mssqlTableCDCOp) checkTarget(params json.RawMessage, env Env) error {
	if _, err := op.decode(params); err != nil {
		return err
	}
	return requirePostgresTarget(env)
}

func (op mssqlTableCDCOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	p, err := op.decode(params)
	if err != nil {
		return Result{}, err
	}
	if err := requirePostgresTarget(env); err != nil {
		return Result{}, err
	}
	if op.src.Reader == nil {
		return noSQLServer("reader")
	}
	tables, err := op.src.Reader.TableCDC(ctx, p.Tables)
	if err != nil {
		return mssqlFailure(op.Name(), err)
	}
	if tables == nil {
		tables = []sqlserver.TableCDC{}
	}
	tracked := 0
	for _, t := range tables {
		if t.CDCEnabled {
			tracked++
		}
	}
	return Result{
		Summary: fmt.Sprintf("%d tables read, %d tracked by CDC", len(tables), tracked),
		Data:    map[string]any{"tables": tables},
	}, nil
}

type mssqlRoleInfoOp mssqlBase

func (mssqlRoleInfoOp) Name() string   { return OpSourceRoleInfo }
func (mssqlRoleInfoOp) Target() string { return sqlserverTarget }
func (mssqlRoleInfoOp) Tier() string   { return protocol.TierT0 }
func (mssqlRoleInfoOp) Layer() string  { return sqlserverLayer }
func (mssqlRoleInfoOp) Describe() string {
	return "read the SQL Server capture login: whether it exists, its fixed roles, its database user and its grants"
}
func (mssqlRoleInfoOp) ParamsSchema() map[string]any {
	return closedSchema(map[string]any{"role": sysnameSchema(), "tables": tablesSchema()}, "role")
}
func (mssqlRoleInfoOp) decode(params json.RawMessage) (mssqlRoleParams, error) {
	var p mssqlRoleParams
	if err := decodeParams(params, &p); err != nil {
		return p, err
	}
	if err := checkSysname("role", p.Role); err != nil {
		return p, err
	}
	return p, checkSQLServerTables(p.Tables, false)
}
func (op mssqlRoleInfoOp) Validate(params json.RawMessage, _ Env) error {
	_, err := op.decode(params)
	return err
}
func (op mssqlRoleInfoOp) checkTarget(params json.RawMessage, env Env) error {
	if _, err := op.decode(params); err != nil {
		return err
	}
	return requirePostgresTarget(env)
}

func (op mssqlRoleInfoOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	p, err := op.decode(params)
	if err != nil {
		return Result{}, err
	}
	if err := requirePostgresTarget(env); err != nil {
		return Result{}, err
	}
	if op.src.Reader == nil {
		return noSQLServer("reader")
	}
	info, err := op.src.Reader.RoleInfo(ctx, p.Role, p.Tables)
	if err != nil {
		return mssqlFailure(op.Name(), err)
	}
	if info.Selectable == nil {
		info.Selectable = map[string]bool{}
	}
	return Result{
		Summary: fmt.Sprintf("login %s: exists %t, database user %t, cdc select %t, view database state %t", p.Role, info.Exists, info.HasDBUser, info.CDCSelect, info.ViewDBState),
		Data:    map[string]any{"role": info},
	}, nil
}

// ---- writes (T2) ----

type mssqlCreatePublicationOp mssqlBase

func (mssqlCreatePublicationOp) Name() string   { return OpSourceCreatePublication }
func (mssqlCreatePublicationOp) Target() string { return sqlserverTarget }
func (mssqlCreatePublicationOp) Tier() string   { return protocol.TierT2 }
func (mssqlCreatePublicationOp) Layer() string  { return sqlserverLayer }
func (mssqlCreatePublicationOp) Describe() string {
	return "enable CDC on the SQL Server database and the named tables under one capture instance (sp_cdc_enable_db, sp_cdc_enable_table)"
}
func (mssqlCreatePublicationOp) ParamsSchema() map[string]any {
	return closedSchema(map[string]any{"publication": sysnameSchema(), "tables": tablesSchema()}, "publication", "tables")
}
func (mssqlCreatePublicationOp) decode(params json.RawMessage) (mssqlPublicationParams, error) {
	var p mssqlPublicationParams
	if err := decodeParams(params, &p); err != nil {
		return p, err
	}
	if err := checkSysname("publication", p.Publication); err != nil {
		return p, err
	}
	return p, checkSQLServerTables(p.Tables, true)
}
func (op mssqlCreatePublicationOp) Validate(params json.RawMessage, _ Env) error {
	_, err := op.decode(params)
	return err
}
func (op mssqlCreatePublicationOp) checkTarget(params json.RawMessage, env Env) error {
	if _, err := op.decode(params); err != nil {
		return err
	}
	return requirePostgresTarget(env)
}

func (op mssqlCreatePublicationOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	p, err := op.decode(params)
	if err != nil {
		return Result{}, err
	}
	if err := requirePostgresTarget(env); err != nil {
		return Result{}, err
	}
	if op.src.Writer == nil {
		return noSQLServer("writer")
	}
	if err := op.src.Writer.CreatePublication(ctx, p.Publication, p.Tables); err != nil {
		res, ferr := mssqlFailure(op.Name(), err)
		res.Receipt = sqlReceipt(p.Publication)
		return res, ferr
	}
	return Result{
		Summary: fmt.Sprintf("CDC enabled for %d tables under capture instance %s", len(p.Tables), p.Publication),
		Data:    map[string]any{"publication": p.Publication, "tables": p.Tables},
		Receipt: sqlReceipt(p.Publication),
	}, nil
}

type mssqlCreateCanaryTableOp mssqlBase

func (mssqlCreateCanaryTableOp) Name() string   { return OpSourceCreateCanaryTable }
func (mssqlCreateCanaryTableOp) Target() string { return sqlserverTarget }
func (mssqlCreateCanaryTableOp) Tier() string   { return protocol.TierT2 }
func (mssqlCreateCanaryTableOp) Layer() string  { return sqlserverLayer }
func (mssqlCreateCanaryTableOp) Describe() string {
	return "create the verification ladder's canary table on the SQL Server source (idempotent)"
}
func (mssqlCreateCanaryTableOp) ParamsSchema() map[string]any {
	return closedSchema(map[string]any{"table": map[string]any{"type": "string", "pattern": `^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$`}}, "table")
}
func (mssqlCreateCanaryTableOp) decode(params json.RawMessage) (mssqlTableParams, error) {
	var p mssqlTableParams
	if err := decodeParams(params, &p); err != nil {
		return p, err
	}
	return p, checkSQLServerTable(p.Table)
}
func (op mssqlCreateCanaryTableOp) Validate(params json.RawMessage, _ Env) error {
	_, err := op.decode(params)
	return err
}
func (op mssqlCreateCanaryTableOp) checkTarget(params json.RawMessage, env Env) error {
	if _, err := op.decode(params); err != nil {
		return err
	}
	return requirePostgresTarget(env)
}

func (op mssqlCreateCanaryTableOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	p, err := op.decode(params)
	if err != nil {
		return Result{}, err
	}
	if err := requirePostgresTarget(env); err != nil {
		return Result{}, err
	}
	if op.src.Writer == nil {
		return noSQLServer("writer")
	}
	if err := op.src.Writer.CreateCanaryTable(ctx, p.Table); err != nil {
		res, ferr := mssqlFailure(op.Name(), err)
		res.Receipt = sqlReceipt(p.Table)
		return res, ferr
	}
	return Result{Summary: "canary table " + p.Table + " ready", Data: map[string]any{"table": p.Table}, Receipt: sqlReceipt(p.Table)}, nil
}

type mssqlInsertCanaryOp mssqlBase

func (mssqlInsertCanaryOp) Name() string   { return OpSourceInsertCanary }
func (mssqlInsertCanaryOp) Target() string { return sqlserverTarget }
func (mssqlInsertCanaryOp) Tier() string   { return protocol.TierT2 }
func (mssqlInsertCanaryOp) Layer() string  { return sqlserverLayer }
func (mssqlInsertCanaryOp) Describe() string {
	return "insert one row carrying the ladder's nonce into the SQL Server canary table"
}
func (mssqlInsertCanaryOp) ParamsSchema() map[string]any {
	return closedSchema(map[string]any{
		"table": map[string]any{"type": "string", "pattern": `^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$`},
		"nonce": map[string]any{"type": "string", "pattern": nonceRe.String()},
	}, "table", "nonce")
}
func (mssqlInsertCanaryOp) decode(params json.RawMessage) (mssqlCanaryParams, error) {
	var p mssqlCanaryParams
	if err := decodeParams(params, &p); err != nil {
		return p, err
	}
	if err := checkSQLServerTable(p.Table); err != nil {
		return p, err
	}
	if len(p.Nonce) > maxNonceBytes || !nonceRe.MatchString(p.Nonce) {
		return p, fmt.Errorf("%w: nonce is not the ladder's nonce shape", ErrParams)
	}
	return p, nil
}
func (op mssqlInsertCanaryOp) Validate(params json.RawMessage, _ Env) error {
	_, err := op.decode(params)
	return err
}
func (op mssqlInsertCanaryOp) checkTarget(params json.RawMessage, env Env) error {
	if _, err := op.decode(params); err != nil {
		return err
	}
	return requirePostgresTarget(env)
}

func (op mssqlInsertCanaryOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	p, err := op.decode(params)
	if err != nil {
		return Result{}, err
	}
	if err := requirePostgresTarget(env); err != nil {
		return Result{}, err
	}
	if op.src.Writer == nil {
		return noSQLServer("writer")
	}
	if err := op.src.Writer.InsertCanary(ctx, p.Table, p.Nonce); err != nil {
		res, ferr := mssqlFailure(op.Name(), err)
		res.Receipt = sqlReceipt(p.Table)
		return res, ferr
	}
	return Result{Summary: "canary row inserted into " + p.Table, Data: map[string]any{"table": p.Table}, Receipt: sqlReceipt(p.Table)}, nil
}

type mssqlCreateReadRoleOp mssqlBase

func (mssqlCreateReadRoleOp) Name() string   { return OpSourceCreateReadRole }
func (mssqlCreateReadRoleOp) Target() string { return sqlserverTarget }
func (mssqlCreateReadRoleOp) Tier() string   { return protocol.TierT2 }
func (mssqlCreateReadRoleOp) Layer() string  { return sqlserverLayer }
func (mssqlCreateReadRoleOp) Describe() string {
	return "create the SQL Server capture login and user with its grants and a runner-generated password kept in the runner's secret store"
}
func (mssqlCreateReadRoleOp) ParamsSchema() map[string]any {
	return closedSchema(map[string]any{"role": sysnameSchema(), "tables": tablesSchema()}, "role", "tables")
}
func (mssqlCreateReadRoleOp) decode(params json.RawMessage) (mssqlRoleParams, error) {
	var p mssqlRoleParams
	if err := decodeParams(params, &p); err != nil {
		return p, err
	}
	if err := checkSysname("role", p.Role); err != nil {
		return p, err
	}
	return p, checkSQLServerTables(p.Tables, true)
}
func (op mssqlCreateReadRoleOp) Validate(params json.RawMessage, _ Env) error {
	_, err := op.decode(params)
	return err
}
func (op mssqlCreateReadRoleOp) checkTarget(params json.RawMessage, env Env) error {
	if _, err := op.decode(params); err != nil {
		return err
	}
	return requirePostgresTarget(env)
}

func (op mssqlCreateReadRoleOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	p, err := op.decode(params)
	if err != nil {
		return Result{}, err
	}
	if err := requirePostgresTarget(env); err != nil {
		return Result{}, err
	}
	if op.src.Writer == nil {
		return noSQLServer("writer")
	}
	// Only the secret store's name for the password comes back; the
	// password itself never leaves the writer.
	name, err := op.src.Writer.CreateReadRole(ctx, p.Role, p.Tables)
	if err != nil {
		res, ferr := mssqlFailure(op.Name(), err)
		res.Receipt = sqlReceipt(p.Role)
		return res, ferr
	}
	return Result{
		Summary: fmt.Sprintf("login %s created with SELECT on %d tables", p.Role, len(p.Tables)),
		Data:    map[string]any{"secret_name": name},
		Receipt: sqlReceipt(p.Role),
	}, nil
}
