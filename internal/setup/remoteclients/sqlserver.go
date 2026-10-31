package remoteclients

import (
	"context"
	"fmt"

	"piperoom/internal/clients"
	sqlserver "piperoom/internal/clients/sqlserver/sqlservertypes"
	"piperoom/internal/setup/protocol"
)

// SQLServer is the SQL Server source client over the runner (row U4e): the
// reads U4b's prerequisite driver checks with, sqlserver.SettingsReader (its
// Reader), each one op the runner's SQL Server source answers
// (ops.RegisterSQLServerSource over a *sqlserver.Client).
//
// Its writes are the setup-only source writes of Source
// (clients.SetupSourceWriter through Writer): the ops are source-neutral on
// the wire (source_create_publication, ...), and the runner of a SQL Server
// room executes them against its sqlserver.SetupWriter, with the T2 floors of
// U1-2b.
//
// Result shapes (the runner's OpResult.Data; each is pinned by a test):
//
//	source_server_info   {"server": sqlserver.ServerInfo}
//	source_agent_state   {"agent": sqlserver.AgentState}
//	source_database_cdc  {"enabled": true}; a missing database is error_code not_found (clients.ErrNotFound)
//	source_table_cdc     {"tables": [sqlserver.TableCDC, ...]}
//	source_role_info     {"role": sqlserver.RoleInfo}
type SQLServer struct{ s *Session }

var _ sqlserver.SettingsReader = (*SQLServer)(nil)

// SQLServer is the SQL Server source client.
func (s *Session) SQLServer() *SQLServer { return &SQLServer{s: s} }

type databaseParams struct {
	Database string `json:"database"`
}

// ServerInfo reads the server version and edition.
func (c *SQLServer) ServerInfo(ctx context.Context) (sqlserver.ServerInfo, error) {
	res, err := c.s.do(ctx, OpSourceServerInfo, protocol.TierT0, nil)
	if err != nil {
		return sqlserver.ServerInfo{}, err
	}
	var out sqlserver.ServerInfo
	if err := decode(res, "server", &out); err != nil {
		return sqlserver.ServerInfo{}, err
	}
	return out, nil
}

// AgentState reads whether SQL Server Agent is running.
func (c *SQLServer) AgentState(ctx context.Context) (sqlserver.AgentState, error) {
	res, err := c.s.do(ctx, OpSourceAgentState, protocol.TierT0, nil)
	if err != nil {
		return sqlserver.AgentState{}, err
	}
	var out sqlserver.AgentState
	if err := decode(res, "agent", &out); err != nil {
		return sqlserver.AgentState{}, err
	}
	return out, nil
}

// DatabaseCDC reads whether CDC is enabled on database. A missing database
// is clients.ErrNotFound, which the driver reads as the check's failure.
func (c *SQLServer) DatabaseCDC(ctx context.Context, database string) (bool, error) {
	if err := requireArg(OpSourceDatabaseCDC, "database", database); err != nil {
		return false, err
	}
	res, err := c.s.do(ctx, OpSourceDatabaseCDC, protocol.TierT0, databaseParams{Database: database})
	if err != nil {
		return false, err
	}
	var on bool
	if err := decode(res, "enabled", &on); err != nil {
		return false, err
	}
	return on, nil
}

// TableCDC reads CDC tracking and the primary key of each table.
func (c *SQLServer) TableCDC(ctx context.Context, tables []string) ([]sqlserver.TableCDC, error) {
	if tables == nil {
		tables = []string{}
	}
	res, err := c.s.do(ctx, OpSourceTableCDC, protocol.TierT0, tablesParams{Tables: tables})
	if err != nil {
		return nil, err
	}
	var out []sqlserver.TableCDC
	if err := decode(res, "tables", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// RoleInfo reads the capture login: whether it exists, its fixed roles, its
// database user, its cdc grants and its SELECT on each table. A missing login
// is not an error (Exists false).
func (c *SQLServer) RoleInfo(ctx context.Context, login string, tables []string) (sqlserver.RoleInfo, error) {
	if err := requireArg(OpSourceRoleInfo, "role", login); err != nil {
		return sqlserver.RoleInfo{}, err
	}
	if tables == nil {
		tables = []string{}
	}
	res, err := c.s.do(ctx, OpSourceRoleInfo, protocol.TierT0, roleTablesParams{Role: login, Tables: tables})
	if err != nil {
		return sqlserver.RoleInfo{}, err
	}
	var out sqlserver.RoleInfo
	if err := decode(res, "role", &out); err != nil {
		return sqlserver.RoleInfo{}, err
	}
	if out.Login != "" && out.Login != login {
		// The runner answered for another login: never read one login's
		// grants as another's.
		return sqlserver.RoleInfo{}, fmt.Errorf("remoteclients: %s: the result names another login: %w", OpSourceRoleInfo, ErrBadResult)
	}
	return out, nil
}

// Writer is the SQL Server room's setup-only source writer: Source's writes,
// which the runner executes against its sqlserver.SetupWriter.
func (c *SQLServer) Writer() clients.SetupSourceWriter { return c.s.Source() }
