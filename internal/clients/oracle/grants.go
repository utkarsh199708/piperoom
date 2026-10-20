package oracle

import (
	"context"
	"database/sql"
	"fmt"
)

// LogMinerGrants reports the grant state of the LogMiner account: its directly
// granted roles (DBA_ROLE_PRIVS), its system privileges (DBA_SYS_PRIVS) and its
// object privileges on each of tables (DBA_TAB_PRIVS, e.g. LOGMINING and
// SELECT). The user and table names are identifiers and are quoted after the
// rule 6 check before they reach a statement.
func (c *Client) LogMinerGrants(ctx context.Context, user string, tables []string) (LogMinerGrants, error) {
	u, err := quoteIdent(user)
	if err != nil {
		return LogMinerGrants{}, err
	}

	out := LogMinerGrants{User: user}

	roles, err := c.stringColumn(ctx,
		fmt.Sprintf(`SELECT GRANTED_ROLE FROM DBA_ROLE_PRIVS WHERE GRANTEE = %s ORDER BY GRANTED_ROLE`, u),
		"roles")
	if err != nil {
		return LogMinerGrants{}, err
	}
	out.Roles = roles

	privs, err := c.stringColumn(ctx,
		fmt.Sprintf(`SELECT PRIVILEGE FROM DBA_SYS_PRIVS WHERE GRANTEE = %s ORDER BY PRIVILEGE`, u),
		"system privileges")
	if err != nil {
		return LogMinerGrants{}, err
	}
	out.SysPrivs = privs

	for _, table := range tables {
		owner, name, err := quoteTableParts(table)
		if err != nil {
			return LogMinerGrants{}, err
		}
		rows, err := c.query(ctx, fmt.Sprintf(`
			SELECT PRIVILEGE, GRANTABLE
			  FROM DBA_TAB_PRIVS
			 WHERE GRANTEE = %s AND OWNER = %s AND TABLE_NAME = %s
			 ORDER BY PRIVILEGE`, u, owner, name))
		if err != nil {
			return LogMinerGrants{}, fmt.Errorf("oracle: table privileges %q: %w", table, err)
		}
		for rows.Next() {
			var priv, grantable sql.NullString
			if err := rows.Scan(&priv, &grantable); err != nil {
				rows.Close()
				return LogMinerGrants{}, fmt.Errorf("oracle: scan table privilege %q: %w", table, err)
			}
			out.TablePrivs = append(out.TablePrivs, TablePrivilege{
				Table:     table,
				Privilege: nullStr(priv),
				Grantable: yesNo(grantable),
			})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return LogMinerGrants{}, fmt.Errorf("oracle: read table privileges %q: %w", table, err)
		}
		rows.Close()
	}
	return out, nil
}

// stringColumn reads one text column into a slice of strings, newest of the
// query's own ORDER BY.
func (c *Client) stringColumn(ctx context.Context, q, what string) ([]string, error) {
	rows, err := c.query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("oracle: %s: %w", what, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var v sql.NullString
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("oracle: scan %s: %w", what, err)
		}
		out = append(out, nullStr(v))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("oracle: read %s: %w", what, err)
	}
	return out, nil
}
