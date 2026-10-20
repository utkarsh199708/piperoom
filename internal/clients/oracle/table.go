package oracle

import (
	"context"
	"database/sql"
	"fmt"
)

// tableLogGroupsQuery returns the log groups of one table, with the table
// identifier quoted after the rule 6 check. The query is per-table so no
// statement is ever built from a list of untrusted names beyond quoted
// identifiers.
const tableLogGroupsQuery = `
		SELECT LOG_GROUP_TYPE, ALWAYS
		  FROM DBA_LOG_GROUPS
		 WHERE OWNER = %s AND TABLE_NAME = %s`

// TableSupplementalLogging reports, for each of tables ("owner.table"), whether
// it has an "ALL COLUMN LOGGING" supplemental log group (the form the Debezium
// Oracle connector requires) and whether that group always logs. An empty
// tables list returns nil without querying.
func (c *Client) TableSupplementalLogging(ctx context.Context, tables []string) ([]TableSupplementalLogging, error) {
	out := make([]TableSupplementalLogging, 0, len(tables))
	for _, table := range tables {
		owner, name, err := quoteTableParts(table)
		if err != nil {
			return nil, err
		}
		rows, err := c.query(ctx, fmt.Sprintf(tableLogGroupsQuery, owner, name))
		if err != nil {
			return nil, fmt.Errorf("oracle: table supplemental logging %q: %w", table, err)
		}

		tl := TableSupplementalLogging{Table: table}
		for rows.Next() {
			var typ, always sql.NullString
			if err := rows.Scan(&typ, &always); err != nil {
				rows.Close()
				return nil, fmt.Errorf("oracle: scan log group %q: %w", table, err)
			}
			if typ.Valid && typ.String == "ALL COLUMN LOGGING" {
				tl.AllColumns = true
				// An unconditional group (ALWAYS) is what the connector needs;
				// OR across groups so a conditional group cannot mask it.
				if always.Valid && always.String == "ALWAYS" {
					tl.Always = true
				}
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("oracle: read log groups %q: %w", table, err)
		}
		rows.Close()
		out = append(out, tl)
	}
	return out, nil
}
