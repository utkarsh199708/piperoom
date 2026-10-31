// Package sqlservertypes holds the SQL Server read client's result types and
// its SettingsReader interface (internal/clients/sqlserver aliases them). It
// imports no database driver, so code that only decodes or passes these
// values (the server's remote client over the runner, the runner's op
// adapters) does not link go-mssqldb.
package sqlservertypes

import "context"

// ServerInfo is the server version and edition.
type ServerInfo struct {
	Version      string `json:"version"`       // SERVERPROPERTY('ProductVersion')
	Edition      string `json:"edition"`       // SERVERPROPERTY('Edition')
	ProductLevel string `json:"product_level"` // SERVERPROPERTY('ProductLevel'); "" when the server reports none
}

// AgentState reports whether SQL Server Agent is running.
type AgentState struct {
	Running bool   `json:"running"`
	Status  string `json:"status"` // sys.dm_server_services.status_desc; "Unknown" when neither the service row nor an agent session is seen
}

// TableCDC reports CDC enablement and the primary key for one table.
type TableCDC struct {
	Table         string `json:"table"` // "schema.table" as passed in
	Exists        bool   `json:"exists"`
	CDCEnabled    bool   `json:"cdc_enabled"`     // sys.tables.is_tracked_by_cdc
	HasPrimaryKey bool   `json:"has_primary_key"` // some index has is_primary_key = 1
}

// RoleInfo reports the capture login's grants in the client's database.
type RoleInfo struct {
	Login      string `json:"login"`
	Exists     bool   `json:"exists"`        // a server principal (login) of that name exists
	Disabled   bool   `json:"disabled"`      // the login exists but is disabled
	DBOwner    bool   `json:"db_owner"`      // the login's database user is a member of db_owner
	DataReader bool   `json:"db_datareader"` // the login's database user is a member of db_datareader
	Sysadmin   bool   `json:"sysadmin"`      // the login is a member of sysadmin
	// Selectable maps each table to whether the login's database user has SELECT
	// (a direct grant, a schema-level grant, or inherited from
	// db_owner/db_datareader/sysadmin). SELECT granted only through a custom
	// role is not traced.
	Selectable map[string]bool `json:"selectable"`
	// HasDBUser: the login has a database user in the client's database (a
	// sys.database_principals row mapped by sid). Without one no database
	// grant can hold; a sysadmin login reaches the database as dbo without
	// one, which the caller reads from Sysadmin. (U4g)
	HasDBUser bool `json:"has_db_user"`
	// CDCSelect: the user can read the cdc schema — GRANT SELECT ON
	// SCHEMA::cdc, or inherited from db_owner/db_datareader/sysadmin.
	// Debezium reads the capture tables there. (U4g)
	CDCSelect bool `json:"cdc_select"`
	// ViewDBState: VIEW DATABASE STATE is granted, directly or inherited
	// from db_owner/sysadmin (db_datareader does not imply it). Debezium's
	// lsn and snapshot reads need it. (U4g)
	ViewDBState bool `json:"view_database_state"`
}

// SettingsReader is the read-only SQL Server client the setup driver (U4b)
// uses. It is the SQL Server subset of the planned clients.SourceSettingsReader:
// version and edition, SQL Server Agent state, CDC enablement per database and
// table, and the capture login's grants.
type SettingsReader interface {
	ServerInfo(ctx context.Context) (ServerInfo, error)
	AgentState(ctx context.Context) (AgentState, error)
	DatabaseCDC(ctx context.Context, database string) (bool, error)
	TableCDC(ctx context.Context, tables []string) ([]TableCDC, error)
	RoleInfo(ctx context.Context, login string, tables []string) (RoleInfo, error)
}
