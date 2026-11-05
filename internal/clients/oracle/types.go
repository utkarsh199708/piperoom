package oracle

import "context"

// Reader is the read-only surface of the Oracle client: every method is a
// SELECT against V$ / DBA dictionary views, none writes. It is the Oracle
// subset of the contract's source-settings reader; the contract does not yet
// express Oracle-specific prerequisites (see the PR's "Contract change
// request"), so this package defines the shape locally until clients.go gains
// it.
type Reader interface {
	// Database returns the database identity and prerequisites held in
	// V$DATABASE and V$VERSION: the CDB name, whether it is a CDB, the current
	// container, the PDB names, archive log mode, force logging, database-level
	// supplemental logging, and the version and edition.
	Database(ctx context.Context) (DatabaseInfo, error)
	// TableSupplementalLogging reports per-table supplemental logging for each
	// of tables ("owner.table").
	TableSupplementalLogging(ctx context.Context, tables []string) ([]TableSupplementalLogging, error)
	// LogMinerGrants reports the roles, system privileges and table privileges
	// granted to user (the LogMiner account), restricted to the given tables.
	LogMinerGrants(ctx context.Context, user string, tables []string) (LogMinerGrants, error)
}

// DatabaseInfo is what Database reads from the dictionary. All values are text
// or booleans derived from "YES"/"NO"; nothing here is a secret.
type DatabaseInfo struct {
	Name         string              `json:"name"`          // V$DATABASE.NAME: the CDB name when IsCDB, else the database name
	IsCDB        bool                `json:"is_cdb"`        // V$DATABASE.CDB = YES
	Container    string              `json:"container"`     // SYS_CONTEXT('USERENV','CON_NAME'): the container the session is in
	PDBs         []string            `json:"pdbs"`          // PDB names, sorted by CON_ID; empty when not a CDB
	LogMode      string              `json:"log_mode"`      // ARCHIVELOG | NOARCHIVELOG
	ForceLog     string              `json:"force_logging"` // YES | NO
	Supplemental SupplementalLogging `json:"supplemental_logging"`
	Version      string              `json:"version"` // first database product banner from V$VERSION ("Oracle Database …" or "Oracle AI Database …")
	Edition      string              `json:"edition"` // edition keyword parsed from the banner; "" when unknown
}

// SupplementalLogging is the database-level supplemental logging of V$DATABASE.
type SupplementalLogging struct {
	Minimal     bool `json:"minimal"`      // SUPPLEMENTAL_LOG_DATA_MIN
	PrimaryKey  bool `json:"primary_key"`  // SUPPLEMENTAL_LOG_DATA_PK
	All         bool `json:"all"`          // SUPPLEMENTAL_LOG_DATA_ALL
	ForeignKey  bool `json:"foreign_key"`  // SUPPLEMENTAL_LOG_DATA_FK
	UniqueIndex bool `json:"unique_index"` // SUPPLEMENTAL_LOG_DATA_UI
}

// TableSupplementalLogging is one table's supplemental logging from
// DBA_LOG_GROUPS: whether an "ALL COLUMN LOGGING" log group exists (the form
// the Debezium Oracle connector needs) and whether it always logs. Debezium
// also accepts database-level SUPPLEMENTAL_LOG_DATA_ALL = YES instead of
// per-table ALL COLUMN groups; the U9b driver should treat either as
// satisfying the requirement (see SupplementalLogging.All).
type TableSupplementalLogging struct {
	Table      string `json:"table"`
	AllColumns bool   `json:"all_columns"` // an ALL COLUMN LOGGING group exists
	Always     bool   `json:"always"`      // that group's ALWAYS column is "ALWAYS"
}

// LogMinerGrants is the grant state of the LogMiner user.
type LogMinerGrants struct {
	User       string           `json:"user"`
	Roles      []string         `json:"roles,omitempty"`             // DBA_ROLE_PRIVS.GRANTED_ROLE
	SysPrivs   []string         `json:"system_privileges,omitempty"` // DBA_SYS_PRIVS.PRIVILEGE
	TablePrivs []TablePrivilege `json:"table_privileges,omitempty"`  // DBA_TAB_PRIVS on the requested tables
}

// TablePrivilege is one object privilege the LogMiner user holds on a table
// (e.g. SELECT).
type TablePrivilege struct {
	Table     string `json:"table"`
	Privilege string `json:"privilege"`
	Grantable bool   `json:"grantable"` // DBA_TAB_PRIVS.GRANTABLE = YES
}
