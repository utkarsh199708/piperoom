package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"piperoom/internal/clients"
)

// settingNames is the fixed list Settings reads, in report order (design
// §6.2). A variable the server does not have (binlog_expire_logs_auto_purge
// before 8.0.29) is simply absent from the result.
var settingNames = []string{
	"version",
	"log_bin",
	"binlog_format",
	"binlog_row_image",
	"gtid_mode",
	"enforce_gtid_consistency",
	"server_id",
	"binlog_expire_logs_seconds",
	"binlog_expire_logs_auto_purge",
	"lower_case_table_names",
}

// rdsRetentionSetting is the RDS/Aurora binlog retention, read from
// mysql.rds_show_configuration when the server has it.
const rdsRetentionSetting = "rds.binlog_retention_hours"

// settingUnits gives the unit of the settings that have one.
var settingUnits = map[string]string{"binlog_expire_logs_seconds": "s", rdsRetentionSetting: "h"}

// SettingNames returns a copy of the settings a MySQL source reports, without
// the RDS-only one. The tools use it to say which ones the server lacks.
func SettingNames() []string { return append([]string(nil), settingNames...) }

// settingsSQL reads the running value of each setting and its persisted value
// (SET PERSIST / PERSIST_ONLY, mysqld-auto.cnf), if any.
var settingsSQL = `
		SELECT g.VARIABLE_NAME, COALESCE(g.VARIABLE_VALUE, ''), p.VARIABLE_VALUE
		FROM performance_schema.global_variables g
		LEFT JOIN performance_schema.persisted_variables p ON p.VARIABLE_NAME = g.VARIABLE_NAME
		WHERE g.VARIABLE_NAME IN (` + strings.TrimSuffix(strings.Repeat("?, ", len(settingNames)), ", ") + `)`

const needSettings = "SELECT on performance_schema"

// Settings reads the fixed CDC settings of design §6.2. PendingRestart is
// true when the persisted value differs from the running one: a SET
// PERSIST_ONLY that takes effect at the next restart. On RDS/Aurora the
// binlog retention hours come last (best effort: absent when the procedure
// is missing or not executable).
func (d *DB) Settings(ctx context.Context) ([]DBSetting, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	args := make([]any, len(settingNames))
	for i, n := range settingNames {
		args[i] = n
	}
	rows, err := d.query(ctx, settingsSQL, args...)
	if err != nil {
		return nil, fail("read settings", needSettings, err)
	}
	defer rows.Close()
	got := map[string]DBSetting{}
	for rows.Next() {
		var (
			name, value string
			persisted   sql.NullString
		)
		if err := rows.Scan(&name, &value, &persisted); err != nil {
			return nil, fmt.Errorf("mysql: scan setting: %w", err)
		}
		name = strings.ToLower(name)
		got[name] = DBSetting{
			Name:           name,
			Value:          cutText(value, 256),
			Unit:           settingUnits[name],
			PendingRestart: persisted.Valid && !strings.EqualFold(persisted.String, value),
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fail("read settings", needSettings, err)
	}
	out := make([]DBSetting, 0, len(settingNames)+1)
	for _, n := range settingNames {
		if s, ok := got[n]; ok {
			out = append(out, s)
		}
	}
	if h, ok := d.rdsRetentionHours(ctx); ok {
		v := ""
		if h != nil {
			v = strconv.FormatInt(*h, 10)
		}
		out = append(out, DBSetting{Name: rdsRetentionSetting, Value: v, Unit: settingUnits[rdsRetentionSetting]})
	}
	return out, nil
}

// rdsRetentionHours calls mysql.rds_show_configuration, which exists only on
// RDS and Aurora. ok is false when the call fails (not RDS, or no EXECUTE):
// the read is best effort and never fails its caller. hours is nil when the
// setting is present but unset (NULL: binlogs are purged as soon as no
// replica needs them).
func (d *DB) rdsRetentionHours(ctx context.Context) (hours *int64, ok bool) {
	rows, err := d.query(ctx, `CALL mysql.rds_show_configuration`)
	if err != nil {
		return nil, false
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, false
	}
	nameCol, valueCol := -1, -1
	for i, c := range cols {
		switch strings.ToLower(c) {
		case "name":
			nameCol = i
		case "value":
			valueCol = i
		}
	}
	if nameCol < 0 || valueCol < 0 {
		return nil, false
	}
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, false
		}
		if !strings.EqualFold(strings.TrimSpace(vals[nameCol].String), "binlog retention hours") {
			continue
		}
		if !vals[valueCol].Valid || strings.EqualFold(vals[valueCol].String, "NULL") || vals[valueCol].String == "" {
			return nil, true
		}
		n, err := strconv.ParseInt(strings.TrimSpace(vals[valueCol].String), 10, 64)
		if err != nil {
			return nil, false
		}
		return &n, true
	}
	return nil, false
}

// tableReplicationSQL reads, for the requested (schema, name) pairs, whether
// each base table exists, its engine and whether it has a primary key. Only
// information_schema is read; a table the read user cannot see is absent.
func tableReplicationSQL(n int) string {
	return `
		SELECT t.TABLE_SCHEMA, t.TABLE_NAME, COALESCE(t.ENGINE, ''),
		       EXISTS (SELECT 1 FROM information_schema.TABLE_CONSTRAINTS c
		               WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
		                 AND c.CONSTRAINT_TYPE = 'PRIMARY KEY')
		FROM information_schema.TABLES t
		WHERE t.TABLE_TYPE = 'BASE TABLE' AND (t.TABLE_SCHEMA, t.TABLE_NAME) IN (` + pairsIn(n) + `)`
}

// TableReplication reads the primary key presence and storage engine of each
// "db.table", in the order given. ReplicaIdentity is "" (MySQL has none). A
// table that is missing, or not visible to the read user, is
// clients.ErrNotFound, named by table only.
func (d *DB) TableReplication(ctx context.Context, tables []string) ([]TableReplication, error) {
	dbs, names, err := splitTables(tables)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	rows, err := d.query(ctx, tableReplicationSQL(len(tables)), pairArgs(dbs, names)...)
	if err != nil {
		return nil, fail("read table replication", "SELECT on the tables", err)
	}
	defer rows.Close()
	type info struct {
		engine string
		pk     bool
	}
	found := map[string]info{}
	for rows.Next() {
		var (
			schema, name string
			i            info
		)
		if err := rows.Scan(&schema, &name, &i.engine, &i.pk); err != nil {
			return nil, fmt.Errorf("mysql: scan table replication: %w", err)
		}
		found[schema+"."+name] = i
	}
	if err := rows.Err(); err != nil {
		return nil, fail("read table replication", "SELECT on the tables", err)
	}
	out := make([]TableReplication, len(tables))
	for k, t := range tables {
		i, ok := lookupFold(found, t)
		if !ok {
			return nil, fmt.Errorf("mysql: table replication of %s: missing, or not visible to the read user: %w", t, clients.ErrNotFound)
		}
		out[k] = TableReplication{Table: t, HasPrimaryKey: i.pk, Engine: cutText(i.engine, 64)}
	}
	return out, nil
}

// lookupFold finds key exactly, else case-insensitively: with
// lower_case_table_names = 1 or 2 the server reports names in its own case.
func lookupFold[V any](m map[string]V, key string) (V, bool) {
	if v, ok := m[key]; ok {
		return v, true
	}
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	var zero V
	return zero, false
}

// account is one mysql.user row of the looked-up user (named columns only:
// never authentication_string).
type account struct {
	host                                         string
	locked, expired                              bool
	replSlave, replClient, sel, reload, lock, db bool
}

// userColumnsSQL is the only read of mysql.user: the design's column-level
// grant gives the read user exactly these columns, so it cannot read a
// password hash.
const userColumnsSQL = `
		SELECT User, Host, account_locked = 'Y', password_expired = 'Y',
		       Repl_slave_priv = 'Y', Repl_client_priv = 'Y', Select_priv = 'Y',
		       Reload_priv = 'Y', Lock_tables_priv = 'Y', Show_db_priv = 'Y'
		FROM mysql.user`

const needGrants = "SELECT on mysql.user (the design's columns), mysql.db, mysql.tables_priv, mysql.role_edges and mysql.default_roles"

// Privilege names of RoleInfo.Privileges.
const (
	PrivReplicationSlave  = "REPLICATION SLAVE"
	PrivReplicationClient = "REPLICATION CLIENT"
	PrivReload            = "RELOAD"
	PrivLockTables        = "LOCK TABLES"
	PrivShowDatabases     = "SHOW DATABASES"
)

// grantee is a (user, host) pair whose grants count for an account: the
// account itself and the roles active at its login.
type grantee struct{ user, host string }

// RoleInfo looks the connector's user up in the grant tables (design §6.4).
// role is a user name, bound as a parameter; every account with that name is
// read, and several combine with AND. Roles granted to an account count when
// they are active at login (mysql.default_roles, or activate_all_roles_on_login
// ON); one level, no partial revokes, no mandatory_roles. A user that does not
// exist is not an error: Exists is false and every table is false.
func (d *DB) RoleInfo(ctx context.Context, role string, tables []string) (RoleInfo, error) {
	if role == "" || len(role) > 32 {
		return RoleInfo{}, errors.New("mysql: role info: user name must be 1 to 32 characters")
	}
	dbs, names, err := splitTables(tables)
	if err != nil {
		return RoleInfo{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	info := RoleInfo{Role: role, Selectable: make(map[string]bool, len(tables))}
	for _, t := range tables {
		info.Selectable[t] = false
	}
	accts, err := d.accounts(ctx, `WHERE User = ? ORDER BY Host`, role)
	if err != nil {
		return RoleInfo{}, err
	}
	if len(accts) == 0 {
		return info, nil
	}
	info.Exists = true

	activeRoles, err := d.activeRoles(ctx, role)
	if err != nil {
		return RoleInfo{}, err
	}
	roleAccts := map[grantee]account{}
	for _, rs := range activeRoles {
		for _, r := range rs {
			roleAccts[r] = account{}
		}
	}
	if len(roleAccts) > 0 {
		keys := make([]grantee, 0, len(roleAccts))
		for g := range roleAccts {
			keys = append(keys, g)
		}
		args := make([]any, 0, 2*len(keys))
		for _, g := range keys {
			args = append(args, g.user, g.host)
		}
		ras, err := d.accounts(ctx, `WHERE (User, Host) IN (`+pairsIn(len(keys))+`)`, args...)
		if err != nil {
			return RoleInfo{}, err
		}
		for _, a := range ras {
			roleAccts[grantee{a.user, a.host}] = a.account
		}
	}

	var all []grantee
	for _, a := range accts {
		all = append(all, grantee{role, a.host})
		all = append(all, activeRoles[a.host]...)
	}
	tableSel, err := d.tableSelect(ctx, all, dbs, names)
	if err != nil {
		return RoleInfo{}, err
	}

	info.CanLogin = true
	info.Replication = true
	info.Privileges = map[string]bool{
		PrivReplicationSlave: true, PrivReplicationClient: true, PrivReload: true,
		PrivLockTables: true, PrivShowDatabases: true,
	}
	sel := make([]bool, len(tables))
	for i := range sel {
		sel[i] = true
	}
	for _, a := range accts {
		info.Accounts = append(info.Accounts, role+"@"+a.host)
		eff := a.account
		gs := append([]grantee{{role, a.host}}, activeRoles[a.host]...)
		for _, r := range activeRoles[a.host] {
			ra := roleAccts[r]
			eff.replSlave = eff.replSlave || ra.replSlave
			eff.replClient = eff.replClient || ra.replClient
			eff.sel = eff.sel || ra.sel
			eff.reload = eff.reload || ra.reload
			eff.lock = eff.lock || ra.lock
			eff.db = eff.db || ra.db
		}
		info.CanLogin = info.CanLogin && !a.locked && !a.expired
		info.Replication = info.Replication && eff.replSlave && eff.replClient
		info.Privileges[PrivReplicationSlave] = info.Privileges[PrivReplicationSlave] && eff.replSlave
		info.Privileges[PrivReplicationClient] = info.Privileges[PrivReplicationClient] && eff.replClient
		info.Privileges[PrivReload] = info.Privileges[PrivReload] && eff.reload
		info.Privileges[PrivLockTables] = info.Privileges[PrivLockTables] && eff.lock
		info.Privileges[PrivShowDatabases] = info.Privileges[PrivShowDatabases] && eff.db
		for i := range tables {
			ok := eff.sel
			for _, g := range gs {
				ok = ok || tableSel.allows(g, dbs[i], names[i])
			}
			sel[i] = sel[i] && ok
		}
	}
	for i, t := range tables {
		info.Selectable[t] = sel[i]
	}
	return info, nil
}

type userAccount struct {
	user string
	account
}

func (d *DB) accounts(ctx context.Context, where string, args ...any) ([]userAccount, error) {
	q := userColumnsSQL + "\n\t\t" + where
	rows, err := d.query(ctx, q, args...)
	if err != nil {
		return nil, fail("read accounts", needGrants, err)
	}
	defer rows.Close()
	var out []userAccount
	for rows.Next() {
		var a userAccount
		if err := rows.Scan(&a.user, &a.host, &a.locked, &a.expired, &a.replSlave, &a.replClient, &a.sel, &a.reload, &a.lock, &a.db); err != nil {
			return nil, fmt.Errorf("mysql: scan account: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fail("read accounts", needGrants, err)
	}
	return out, nil
}

// activeRoles returns, per host of user's accounts, the roles granted to that
// account that are active at login.
func (d *DB) activeRoles(ctx context.Context, user string) (map[string][]grantee, error) {
	var allOn string
	if err := d.queryRow(ctx, `SELECT @@global.activate_all_roles_on_login`).Scan(&allOn); err != nil {
		return nil, fail("read activate_all_roles_on_login", "", err)
	}
	all := allOn == "1" || strings.EqualFold(allOn, "ON")

	rows, err := d.query(ctx, `
		SELECT e.TO_HOST, e.FROM_USER, e.FROM_HOST,
		       EXISTS (SELECT 1 FROM mysql.default_roles r
		               WHERE r.HOST = e.TO_HOST AND r.USER = e.TO_USER
		                 AND r.DEFAULT_ROLE_HOST = e.FROM_HOST AND r.DEFAULT_ROLE_USER = e.FROM_USER)
		FROM mysql.role_edges e
		WHERE e.TO_USER = ?
		ORDER BY e.TO_HOST, e.FROM_USER, e.FROM_HOST`, user)
	if err != nil {
		return nil, fail("read role grants", needGrants, err)
	}
	defer rows.Close()
	out := map[string][]grantee{}
	for rows.Next() {
		var (
			toHost string
			r      grantee
			def    bool
		)
		if err := rows.Scan(&toHost, &r.user, &r.host, &def); err != nil {
			return nil, fmt.Errorf("mysql: scan role grant: %w", err)
		}
		if all || def {
			out[toHost] = append(out[toHost], r)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fail("read role grants", needGrants, err)
	}
	return out, nil
}

// selectGrants are the database- and table-level SELECT grants of a set of
// grantees.
type selectGrants struct {
	db    map[grantee][]string        // grantee -> Db patterns (LIKE syntax) with Select_priv
	table map[grantee]map[string]bool // grantee -> "db.table" with Select in Table_priv
}

func (s selectGrants) allows(g grantee, db, table string) bool {
	for _, p := range s.db[g] {
		if likeMatch(p, db) {
			return true
		}
	}
	return s.table[g][db+"."+table]
}

// tableSelect reads the SELECT grants on databases and tables of gs.
func (d *DB) tableSelect(ctx context.Context, gs []grantee, dbs, names []string) (selectGrants, error) {
	out := selectGrants{db: map[grantee][]string{}, table: map[grantee]map[string]bool{}}
	if len(gs) == 0 || len(dbs) == 0 {
		return out, nil
	}
	gargs := make([]any, 0, 2*len(gs))
	for _, g := range gs {
		gargs = append(gargs, g.user, g.host)
	}
	rows, err := d.query(ctx, `
		SELECT User, Host, Db FROM mysql.db
		WHERE Select_priv = 'Y' AND (User, Host) IN (`+pairsIn(len(gs))+`)`, gargs...)
	if err != nil {
		return out, fail("read database grants", needGrants, err)
	}
	for rows.Next() {
		var g grantee
		var pattern string
		if err := rows.Scan(&g.user, &g.host, &pattern); err != nil {
			rows.Close()
			return out, fmt.Errorf("mysql: scan database grant: %w", err)
		}
		out.db[g] = append(out.db[g], pattern)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, fail("read database grants", needGrants, err)
	}

	args := append(gargs, pairArgs(dbs, names)...)
	rows, err = d.query(ctx, `
		SELECT User, Host, Db, Table_name FROM mysql.tables_priv
		WHERE FIND_IN_SET('Select', Table_priv) > 0
		  AND (User, Host) IN (`+pairsIn(len(gs))+`)
		  AND (Db, Table_name) IN (`+pairsIn(len(dbs))+`)`, args...)
	if err != nil {
		return out, fail("read table grants", needGrants, err)
	}
	defer rows.Close()
	for rows.Next() {
		var g grantee
		var db, table string
		if err := rows.Scan(&g.user, &g.host, &db, &table); err != nil {
			return out, fmt.Errorf("mysql: scan table grant: %w", err)
		}
		if out.table[g] == nil {
			out.table[g] = map[string]bool{}
		}
		out.table[g][db+"."+table] = true
	}
	if err := rows.Err(); err != nil {
		return out, fail("read table grants", needGrants, err)
	}
	return out, nil
}

// likeMatch matches s against a mysql.db Db pattern as the server does: %
// is any run, _ any one character, a backslash escapes the next character.
// Case-sensitive, as on a server with lower_case_table_names = 0.
func likeMatch(pattern, s string) bool {
	p, t := []rune(pattern), []rune(s)
	// memo[i][j]: p[i:] matches t[j:]; bounded by 64-character names.
	memo := map[[2]int]bool{}
	var match func(i, j int) bool
	match = func(i, j int) bool {
		key := [2]int{i, j}
		if v, ok := memo[key]; ok {
			return v
		}
		var r bool
		switch {
		case i == len(p):
			r = j == len(t)
		case p[i] == '%':
			r = match(i+1, j) || (j < len(t) && match(i, j+1))
		case p[i] == '\\' && i+1 < len(p):
			r = j < len(t) && t[j] == p[i+1] && match(i+2, j+1)
		case p[i] == '_':
			r = j < len(t) && match(i+1, j+1)
		default:
			r = j < len(t) && t[j] == p[i] && match(i+1, j+1)
		}
		memo[key] = r
		return r
	}
	if len(p) > 256 || len(t) > 256 {
		return false
	}
	return match(0, 0)
}
