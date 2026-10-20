package oracle

import (
	"context"
	"database/sql"
	"fmt"
)

// tableLogGroupsQuery returns the log groups of one table. The owner and table
// names are bound as values (:1, :2) after the rule 6 check; they are never
// spliced into the statement as identifiers.
const tableLogGroupsQuery = `
		SELECT LOG_GROUP_TYPE, ALWAYS
		  FROM DBA_LOG_GROUPS
		 WHERE OWNER = :1 AND TABLE_NAME = :2`

// TableSupplementalLogging reports, for each of tables ("owner.table"), whether
// it has an "ALL COLUMN LOGGING" supplemental log group (the form the Debezium
// Oracle connector requires) and whether that group always logs. An empty
// tables list returns nil without querying.
func (c *Client) TableSupplementalLogging(ctx context.Context, tables []string) ([]TableSupplementalLogging, error) {
	out := make([]TableSupplementalLogging, 0, len(tables))
	for _, table := range tables {
		owner, name, err := tableParts(table)
		if err != nil {
			return nil, err
		}

		tl := TableSupplementalLogging{Table: table}
		err = c.each(ctx, tableLogGroupsQuery, []any{owner, name}, func(rows sqlRows) error {
			var typ, always sql.NullString
			if err := rows.Scan(&typ, &always); err != nil {
				return fmt.Errorf("oracle: scan: %w", err)
			}
			if typ.Valid && typ.String == "ALL COLUMN LOGGING" {
				tl.AllColumns = true
				// An unconditional group (ALWAYS) is what the connector needs;
				// OR across groups so a conditional group cannot mask it.
				if always.Valid && always.String == "ALWAYS" {
					tl.Always = true
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("oracle: table supplemental logging %q: %w", table, err)
		}
		out = append(out, tl)
	}
	return out, nil
}
