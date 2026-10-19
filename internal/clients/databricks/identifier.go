package databricks

import (
	"fmt"
	"regexp"
	"strings"
)

// identRe is AGENTS.md §4 rule 6: one catalog, schema, table or column
// name. It is stricter than Databricks (which allows more inside backticks),
// so a name that needs more cannot be reached through this client.
var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func validateIdent(name string) error {
	if !identRe.MatchString(name) {
		return fmt.Errorf("databricks: invalid identifier %q", name)
	}
	return nil
}

// quote wraps a validated identifier in backticks, Databricks SQL's
// delimiter. The regex rules out backticks in the input, so no escaping is
// needed.
func quote(part string) string { return "`" + part + "`" }

// tableRef is a validated "schema.table" in the DSN's catalog.
type tableRef struct {
	catalog, schema, table string
}

// parseTable validates "schema.table". A catalog prefix is not accepted:
// every table is read in the DSN catalog, so a tool cannot point the client
// at another catalog the credential happens to see.
func parseTable(catalog, name string) (tableRef, error) {
	parts := strings.Split(name, ".")
	if len(parts) != 2 {
		return tableRef{}, fmt.Errorf("databricks: table must be \"schema.table\", got %q", name)
	}
	for _, p := range parts {
		if err := validateIdent(p); err != nil {
			return tableRef{}, fmt.Errorf("databricks: invalid table %q: %w", name, err)
		}
	}
	return tableRef{catalog: catalog, schema: parts[0], table: parts[1]}, nil
}

// sql is the fully qualified, quoted table: `catalog`.`schema`.`table`. The
// catalog was checked by ParseDSN.
func (t tableRef) sql() string {
	return quote(t.catalog) + "." + quote(t.schema) + "." + quote(t.table)
}
