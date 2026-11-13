package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"piperoom/internal/clients"
)

// identRe is the AGENTS.md rule 6 identifier shape every caller-supplied
// identifier must match before it is used.
var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validateIdent checks name against identRe and returns it unchanged.
func validateIdent(name string) (string, error) {
	if !identRe.MatchString(name) {
		return "", fmt.Errorf("sqlserver: invalid identifier %q", name)
	}
	return name, nil
}

// quoteIdent validates name and returns it as a SQL Server bracketed identifier
// ([name]). The read methods pass names as parameters (values), so none of them
// interpolates an identifier; the write half (U4a-2) interpolates identifiers
// and uses this. It lives here, with its tests, so the rule lands with the
// client.
//
// The identRe check is repeated inline (not only through validateIdent) so
// the guard sits in the same function as the concatenation: identRe admits
// no "]", so the bracketing cannot be closed early. The quoted text is
// copied out of identChars byte by byte rather than from name, so the
// statement is built only from this package's constants: static analysis
// (CodeQL's go/sql-injection) does not count a regexp match as a sanitiser,
// and before this it traced the caller's string into ExecContext.
func quoteIdent(name string) (string, error) {
	if !identRe.MatchString(name) {
		return "", fmt.Errorf("sqlserver: invalid identifier %q", name)
	}
	var b strings.Builder
	b.Grow(len(name) + 2)
	b.WriteByte('[')
	for i := 0; i < len(name); i++ {
		j := strings.IndexByte(identChars, name[i])
		if j < 0 {
			return "", fmt.Errorf("sqlserver: invalid identifier %q", name)
		}
		b.WriteByte(identChars[j])
	}
	b.WriteByte(']')
	return b.String(), nil
}

// identChars is every byte identRe admits.
const identChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_"

// splitTable validates a "schema.table" identifier and returns the raw parts.
func splitTable(table string) (string, string, error) {
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("sqlserver: table must be \"schema.table\", got %q", table)
	}
	schema, err := validateIdent(parts[0])
	if err != nil {
		return "", "", err
	}
	tbl, err := validateIdent(parts[1])
	if err != nil {
		return "", "", err
	}
	return schema, tbl, nil
}

// splitTables validates a list of "schema.table" identifiers, dropping
// duplicates and returning the unique schema/table pairs in order.
func splitTables(tables []string) ([][2]string, error) {
	if len(tables) > MaxTables {
		return nil, fmt.Errorf("sqlserver: %d tables, at most %d per call", len(tables), MaxTables)
	}
	pairs := make([][2]string, 0, len(tables))
	seen := make(map[string]bool, len(tables))
	for _, t := range tables {
		schema, tbl, err := splitTable(t)
		if err != nil {
			return nil, err
		}
		key := schema + "." + tbl
		if seen[key] {
			continue
		}
		seen[key] = true
		pairs = append(pairs, [2]string{schema, tbl})
	}
	return pairs, nil
}

// MaxTables caps the tables one call may name: two parameters per table
// must stay under SQL Server's 2100-parameter limit for one request, and a
// pipeline's source tables are never a catalogue scan (the runner's ops cap
// requests at the same bound).
const MaxTables = 1000

// tablePredicate builds "(s.name = @p<n> AND t.name = @p<n+1>) OR ..." for the
// given pairs, with the positional parameters starting at start (1-based, as
// go-mssqldb's @pN). The query it is used in must alias the schema as s and
// the table as t.
//
// More than MaxTables pairs is an error: the bound is checked here, next to
// the allocation, as well as in splitTables.
func tablePredicate(start int, pairs [][2]string) (string, []any, error) {
	n := len(pairs)
	if n > MaxTables {
		return "", nil, fmt.Errorf("sqlserver: %d tables, at most %d per call", n, MaxTables)
	}
	var b strings.Builder
	args := make([]any, 0, 2*n)
	for i, p := range pairs {
		if i > 0 {
			b.WriteString(" OR ")
		}
		fmt.Fprintf(&b, "(s.name = @p%d AND t.name = @p%d)", start+2*i, start+2*i+1)
		args = append(args, p[0], p[1])
	}
	return b.String(), args, nil
}

// ServerInfo reads the server version, edition and product level. It needs no
// special permission: SERVERPROPERTY is public.
func (c *Client) ServerInfo(ctx context.Context) (ServerInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	const q = `
		SELECT COALESCE(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)), ''),
		       COALESCE(CAST(SERVERPROPERTY('Edition') AS nvarchar(128)), ''),
		       COALESCE(CAST(SERVERPROPERTY('ProductLevel') AS nvarchar(128)), '')`
	var info ServerInfo
	if err := c.db.QueryRow(ctx, q).Scan(&info.Version, &info.Edition, &info.ProductLevel); err != nil {
		return ServerInfo{}, fmt.Errorf("sqlserver: read server info: %w", err)
	}
	return info, nil
}

// AgentState reads whether SQL Server Agent is running. sys.dm_server_services
// (VIEW SERVER STATE) holds the agent's display name in servicename, e.g.
// "SQL Server Agent (MSSQLSERVER)", so it is matched with a LIKE prefix. On
// Linux containers that DMV has no agent row, so it falls back to counting
// active agent sessions in sys.dm_exec_sessions (also VIEW SERVER STATE). A
// caller without VIEW SERVER STATE gets the wrapped error, never a false
// "stopped"; Status is "Unknown" only when neither read sees the agent.
func (c *Client) AgentState(ctx context.Context) (AgentState, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	const q = `
		SELECT COALESCE(status_desc, '')
		FROM sys.dm_server_services
		WHERE servicename LIKE N'SQL Server Agent%'`
	var status string
	if err := c.db.QueryRow(ctx, q).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return c.agentStateFromSessions(ctx)
		}
		return AgentState{}, fmt.Errorf("sqlserver: read SQL Server Agent state: %w", err)
	}
	return AgentState{Running: status == "Running", Status: status}, nil
}

// agentStateFromSessions is the Linux-container fallback for AgentState: no
// sys.dm_server_services row exists there, so it counts sessions whose
// program_name starts with "SQLAgent - ". Zero sessions means the agent is not
// running, with Status "Unknown".
func (c *Client) agentStateFromSessions(ctx context.Context) (AgentState, error) {
	const q = `SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE program_name LIKE N'SQLAgent - %'`
	var n int64
	if err := c.db.QueryRow(ctx, q).Scan(&n); err != nil {
		return AgentState{}, fmt.Errorf("sqlserver: read SQL Server Agent sessions: %w", err)
	}
	if n == 0 {
		return AgentState{Running: false, Status: "Unknown"}, nil
	}
	return AgentState{Running: true, Status: "Running"}, nil
}

// DatabaseCDC reports whether CDC is enabled on one database. The database name
// is validated but passed as a parameter, so it is never interpolated. A
// database that does not exist (or that the login cannot see) is
// clients.ErrNotFound.
func (c *Client) DatabaseCDC(ctx context.Context, database string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	if _, err := validateIdent(database); err != nil {
		return false, err
	}
	const q = `SELECT is_cdc_enabled FROM sys.databases WHERE name = @p1`
	var enabled bool
	if err := c.db.QueryRow(ctx, q, database).Scan(&enabled); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("sqlserver: database %q: %w", database, clients.ErrNotFound)
		}
		return false, fmt.Errorf("sqlserver: read CDC state of database %q: %w", database, err)
	}
	return enabled, nil
}

// TableCDC reports, for each "schema.table" in tables, whether it exists,
// whether CDC is enabled on it (sys.tables.is_tracked_by_cdc) and whether it
// has a primary key. A table that is not found is returned with Exists false
// and no error; an invalid table name is an error before any query.
func (c *Client) TableCDC(ctx context.Context, tables []string) ([]TableCDC, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	pairs, err := splitTables(tables)
	if err != nil {
		return nil, err
	}
	if len(pairs) == 0 {
		return nil, nil
	}

	pred, args, err := tablePredicate(1, pairs)
	if err != nil {
		return nil, err
	}
	q := `
		SELECT s.name, t.name, t.is_tracked_by_cdc,
		       CAST(CASE WHEN EXISTS (SELECT 1 FROM sys.indexes i WHERE i.object_id = t.object_id AND i.is_primary_key = 1) THEN 1 ELSE 0 END AS bit)
		FROM sys.tables t
		JOIN sys.schemas s ON s.schema_id = t.schema_id
		WHERE ` + pred

	rws, err := c.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read table CDC state: %w", err)
	}
	defer rws.Close()

	found := make(map[string]TableCDC, len(pairs))
	for rws.Next() {
		var schema, table string
		var enabled, hasPK bool
		if err := rws.Scan(&schema, &table, &enabled, &hasPK); err != nil {
			return nil, fmt.Errorf("sqlserver: scan table CDC state: %w", err)
		}
		key := schema + "." + table
		found[key] = TableCDC{Table: key, Exists: true, CDCEnabled: enabled, HasPrimaryKey: hasPK}
	}
	if err := rws.Err(); err != nil {
		return nil, fmt.Errorf("sqlserver: read table CDC state: %w", err)
	}

	out := make([]TableCDC, 0, len(pairs))
	for _, p := range pairs {
		key := p[0] + "." + p[1]
		if tc, ok := found[key]; ok {
			out = append(out, tc)
			continue
		}
		out = append(out, TableCDC{Table: key})
	}
	return out, nil
}

// RoleInfo reports the capture login's grants. Exists, Disabled, DBOwner,
// DataReader and Sysadmin come from one read; Selectable overlays direct and
// schema-level SELECT grants from sys.database_permissions (permission_name
// 'SELECT', state granted) onto the fixed-role membership. The login name is a
// value and is passed as a parameter, never interpolated.
func (c *Client) RoleInfo(ctx context.Context, login string, tables []string) (RoleInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	pairs, err := splitTables(tables)
	if err != nil {
		return RoleInfo{}, err
	}

	const infoQ = `
		SELECT CAST(CASE WHEN EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @p1 AND type IN ('S','U','G')) THEN 1 ELSE 0 END AS int),
		       CAST(ISNULL((SELECT is_disabled FROM sys.server_principals WHERE name = @p1 AND type IN ('S','U','G')), 0) AS int),
		       ISNULL(IS_SRVROLEMEMBER('sysadmin', @p1), 0),
		       CAST(CASE WHEN EXISTS (
		           SELECT 1
		           FROM sys.database_role_members rm
		           JOIN sys.database_principals r ON r.principal_id = rm.role_principal_id AND r.name = N'db_owner'
		           JOIN sys.database_principals u ON u.principal_id = rm.member_principal_id
		           JOIN sys.server_principals sp ON sp.sid = u.sid
		           WHERE sp.name = @p1
		       ) THEN 1 ELSE 0 END AS int),
		       CAST(CASE WHEN EXISTS (
		           SELECT 1
		           FROM sys.database_role_members rm
		           JOIN sys.database_principals r ON r.principal_id = rm.role_principal_id AND r.name = N'db_datareader'
		           JOIN sys.database_principals u ON u.principal_id = rm.member_principal_id
		           JOIN sys.server_principals sp ON sp.sid = u.sid
		           WHERE sp.name = @p1
		       ) THEN 1 ELSE 0 END AS int),
		       CAST(CASE WHEN EXISTS (
		           SELECT 1
		           FROM sys.database_principals u
		           JOIN sys.server_principals sp ON sp.sid = u.sid
		           WHERE sp.name = @p1
		       ) THEN 1 ELSE 0 END AS int),
		       CAST(CASE WHEN EXISTS (
		           SELECT 1
		           FROM sys.database_permissions p
		           JOIN sys.database_principals u ON u.principal_id = p.grantee_principal_id
		           JOIN sys.server_principals sp ON sp.sid = u.sid
		           WHERE sp.name = @p1
		             AND p.class = 0
		             AND p.permission_name = N'VIEW DATABASE STATE'
		             AND p.state IN (N'G', N'W')
		       ) THEN 1 ELSE 0 END AS int),
		       CAST(CASE WHEN EXISTS (
		           SELECT 1
		           FROM sys.database_permissions p
		           JOIN sys.database_principals u ON u.principal_id = p.grantee_principal_id
		           JOIN sys.server_principals sp ON sp.sid = u.sid
		           JOIN sys.schemas s ON p.class = 3 AND s.schema_id = p.major_id
		           WHERE sp.name = @p1
		             AND s.name = N'cdc'
		             AND p.permission_name = N'SELECT'
		             AND p.state IN (N'G', N'W')
		       ) THEN 1 ELSE 0 END AS int)`
	var exists, disabled, sysadmin, dbOwner, dataReader, hasUser, viewState, cdcSelect int64
	if err := c.db.QueryRow(ctx, infoQ, login).Scan(&exists, &disabled, &sysadmin, &dbOwner, &dataReader, &hasUser, &viewState, &cdcSelect); err != nil {
		return RoleInfo{}, fmt.Errorf("sqlserver: read role info for %q: %w", login, err)
	}

	info := RoleInfo{
		Login:      login,
		Exists:     exists == 1,
		Disabled:   disabled == 1,
		Sysadmin:   sysadmin == 1,
		DBOwner:    dbOwner == 1,
		DataReader: dataReader == 1,
		HasDBUser:  hasUser == 1,
		Selectable: make(map[string]bool, len(pairs)),
	}
	// Fixed roles imply the cdc-schema read; VIEW DATABASE STATE is implied
	// by db_owner and sysadmin only (db_datareader reads data, not state).
	info.CDCSelect = cdcSelect == 1 || info.DBOwner || info.DataReader || info.Sysadmin
	info.ViewDBState = viewState == 1 || info.DBOwner || info.Sysadmin
	// A missing login has no database user to hold grants, so there is nothing
	// to read and no grant query to run.
	if !info.Exists {
		return info, nil
	}
	// Fixed-role members can SELECT anything; direct and schema grants add the
	// rest.
	for _, p := range pairs {
		key := p[0] + "." + p[1]
		info.Selectable[key] = info.DBOwner || info.DataReader || info.Sysadmin
	}
	if len(pairs) > 0 {
		direct, err := c.directSelectGrants(ctx, login, pairs)
		if err != nil {
			return RoleInfo{}, err
		}
		for k := range direct {
			info.Selectable[k] = true
		}
	}
	return info, nil
}

// directSelectGrants returns the "schema.table" set on which the login's
// database user holds a direct SELECT grant, either on the object
// (permission class 1) or on the schema (class 3, GRANT SELECT ON SCHEMA::x,
// which covers every table in that schema). A schema grant has no table, so
// its table column comes back empty and is expanded to every requested pair in
// that schema.
func (c *Client) directSelectGrants(ctx context.Context, login string, pairs [][2]string) (map[string]bool, error) {
	pred, targs, err := tablePredicate(2, pairs)
	if err != nil {
		return nil, err
	}
	args := append([]any{login}, targs...)
	q := `
		SELECT s.name, COALESCE(t.name, '')
		FROM sys.database_permissions p
		JOIN sys.database_principals u ON u.principal_id = p.grantee_principal_id
		JOIN sys.server_principals sp ON sp.sid = u.sid
		LEFT JOIN sys.tables t ON p.class = 1 AND t.object_id = p.major_id
		JOIN sys.schemas s ON s.schema_id = COALESCE(t.schema_id, p.major_id)
		WHERE sp.name = @p1
		  AND p.permission_name = N'SELECT'
		  AND p.state IN (N'G', N'W')
		  AND (
		      (p.class = 1 AND p.major_id = t.object_id AND (` + pred + `))
		      OR (p.class = 3 AND p.major_id = s.schema_id)
		  )`

	rws, err := c.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read SELECT grants for %q: %w", login, err)
	}
	defer rws.Close()

	out := make(map[string]bool)
	schemaGrants := make(map[string]bool)
	for rws.Next() {
		var schema, table string
		if err := rws.Scan(&schema, &table); err != nil {
			return nil, fmt.Errorf("sqlserver: scan SELECT grant for %q: %w", login, err)
		}
		if table == "" {
			schemaGrants[schema] = true
			continue
		}
		out[schema+"."+table] = true
	}
	if err := rws.Err(); err != nil {
		return nil, fmt.Errorf("sqlserver: read SELECT grants for %q: %w", login, err)
	}
	for schema := range schemaGrants {
		for _, p := range pairs {
			if p[0] == schema {
				out[p[0]+"."+p[1]] = true
			}
		}
	}
	return out, nil
}
