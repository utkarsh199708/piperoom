package bigquery

import (
	"fmt"
	"regexp"
	"strings"
)

// identRe is AGENTS.md §4 rule 6: one dataset, table or column name. It is
// stricter than BigQuery (which allows hyphens and Unicode in table names),
// so a name that needs more cannot be reached through this client.
var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func validateIdent(name string) error {
	if !identRe.MatchString(name) {
		return fmt.Errorf("bigquery: invalid identifier %q", name)
	}
	return nil
}

// quote wraps a validated identifier part in backticks. The regex rules out
// backticks and backslashes in the input, so no escaping is needed.
func quote(part string) string { return "`" + part + "`" }

// tableRef is a validated "dataset.table" in the DSN's project.
type tableRef struct {
	project, dataset, table string
}

// parseTable validates "dataset.table". A project prefix is not accepted:
// every table is read in the DSN project, so a tool cannot point the client
// at another project the credential happens to see.
func parseTable(project, name string) (tableRef, error) {
	parts := strings.Split(name, ".")
	if len(parts) != 2 {
		return tableRef{}, fmt.Errorf("bigquery: table must be \"dataset.table\", got %q", name)
	}
	for _, p := range parts {
		if err := validateIdent(p); err != nil {
			return tableRef{}, fmt.Errorf("bigquery: invalid table %q: %w", name, err)
		}
	}
	return tableRef{project: project, dataset: parts[0], table: parts[1]}, nil
}

// sql is the fully qualified, quoted table: `project`.`dataset`.`table`.
// The project was checked by ParseDSN (letters, digits, hyphens).
func (t tableRef) sql() string {
	return quote(t.project) + "." + quote(t.dataset) + "." + quote(t.table)
}

// datasetSQL is `project`.`dataset`, the prefix of the dataset's
// INFORMATION_SCHEMA views.
func (t tableRef) datasetSQL() string {
	return quote(t.project) + "." + quote(t.dataset)
}
