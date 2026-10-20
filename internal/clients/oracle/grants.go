package oracle

import (
	"context"
	"database/sql"
	"fmt"
)

// LogMinerGrants reports the grant state of the LogMiner account: its directly
// granted roles (DBA_ROLE_PRIVS), its system privileges (DBA_SYS_PRIVS) and its
// object privileges on each of tables (DBA_TAB_PRIVS, e.g. SELECT). The user
// and table names pass the rule 6 check, then are bound as values rather than
// spliced into the statement.
func (c *Client) LogMinerGrants(ctx context.Context, user string, tables []string) (LogMinerGrants, error) {
	if _, err := validateIdent(user); err != nil {
		return LogMinerGrants{}, err
	}

	out := LogMinerGrants{User: user}

	roles, err := c.stringColumn(ctx,
		`SELECT GRANTED_ROLE FROM DBA_ROLE_PRIVS WHERE GRANTEE = :1 ORDER BY GRANTED_ROLE`,
		"roles", user)
	if err != nil {
		return LogMinerGrants{}, err
	}
	out.Roles = roles

	privs, err := c.stringColumn(ctx,
		`SELECT PRIVILEGE FROM DBA_SYS_PRIVS WHERE GRANTEE = :1 ORDER BY PRIVILEGE`,
		"system privileges", user)
	if err != nil {
		return LogMinerGrants{}, err
	}
	out.SysPrivs = privs

	for _, table := range tables {
		owner, name, err := tableParts(table)
		if err != nil {
			return LogMinerGrants{}, err
		}
		err = c.each(ctx, `
			SELECT PRIVILEGE, GRANTABLE
			  FROM DBA_TAB_PRIVS
			 WHERE GRANTEE = :1 AND OWNER = :2 AND TABLE_NAME = :3
			 ORDER BY PRIVILEGE`, []any{user, owner, name}, func(rows sqlRows) error {
			var priv, grantable sql.NullString
			if err := rows.Scan(&priv, &grantable); err != nil {
				return fmt.Errorf("oracle: scan: %w", err)
			}
			out.TablePrivs = append(out.TablePrivs, TablePrivilege{
				Table:     table,
				Privilege: nullStr(priv),
				Grantable: yesNo(grantable),
			})
			return nil
		})
		if err != nil {
			return LogMinerGrants{}, fmt.Errorf("oracle: table privileges %q: %w", table, err)
		}
	}
	return out, nil
}

// stringColumn reads one text column into a slice of strings, newest of the
// query's own ORDER BY.
func (c *Client) stringColumn(ctx context.Context, q, what string, args ...any) ([]string, error) {
	var out []string
	err := c.each(ctx, q, args, func(rows sqlRows) error {
		var v sql.NullString
		if err := rows.Scan(&v); err != nil {
			return fmt.Errorf("oracle: scan: %w", err)
		}
		out = append(out, nullStr(v))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("oracle: %s: %w", what, err)
	}
	return out, nil
}
