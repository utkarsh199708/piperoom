package dbtools

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
)

// plainTableEntry is a schema.table entry of table.include.list with no regex
// metacharacters. It names one table; every other entry is a pattern.
var plainTableEntry = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$`)

// ConnectorInclude is a Debezium Postgres source connector's table filter:
// table.include.list and table.exclude.list. Plain entries name one table;
// every other entry is a regular expression, as Debezium documents.
type ConnectorInclude struct {
	// Literal are the included schema.table entries that name one table and
	// are not excluded.
	Literal []string
	// Patterns are include entries that are regular expressions. They are
	// printed for the reader, but a pattern cannot be expanded into table
	// names, so gaps behind a pattern stay undiscovered.
	Patterns []string
	// Invalid are include or exclude entries that do not compile as RE2.
	Invalid []string
	// Unbounded is true when table.include.list is absent or empty: Debezium
	// then captures every table in the schema, so no gap can be computed.
	Unbounded bool

	include []*regexp.Regexp
	exclude []*regexp.Regexp
	// signal is signal.data.collection, the Debezium signal table. It is
	// captured like a source table but carries control rows, not pipeline data,
	// so it is never reported as an outside-scope source table.
	signal string
}

// ParseConnectorInclude reads the include/exclude table filters of a Debezium
// connector config. Nothing else in the config is read.
func ParseConnectorInclude(cfg map[string]string) ConnectorInclude {
	var c ConnectorInclude
	c.signal = strings.TrimSpace(cfg["signal.data.collection"])
	includeEntries := splitTableList(cfg["table.include.list"])
	excludeEntries := splitTableList(cfg["table.exclude.list"])
	c.Unbounded = len(includeEntries) == 0
	for _, e := range excludeEntries {
		if re, ok := compileTablePattern(e); ok {
			c.exclude = append(c.exclude, re)
		} else {
			c.Invalid = append(c.Invalid, e)
		}
	}
	for _, e := range includeEntries {
		if c.excluded(e) {
			continue
		}
		if plainTableEntry.MatchString(e) {
			if e != c.signal && !slices.Contains(c.Literal, e) {
				c.Literal = append(c.Literal, e)
			}
			continue
		}
		if re, ok := compileTablePattern(e); ok {
			c.include = append(c.include, re)
			if !slices.Contains(c.Patterns, e) {
				c.Patterns = append(c.Patterns, e)
			}
			continue
		}
		c.Invalid = append(c.Invalid, e)
	}
	return c
}

// splitTableList splits a comma-separated Debezium list, dropping empty
// entries and surrounding spaces.
func splitTableList(s string) []string {
	var out []string
	for _, e := range strings.Split(s, ",") {
		if e = strings.TrimSpace(e); e != "" {
			out = append(out, e)
		}
	}
	return out
}

// compileTablePattern anchors a Debezium list entry the way its filter
// matches: the whole schema.table name, not a substring.
func compileTablePattern(entry string) (*regexp.Regexp, bool) {
	re, err := regexp.Compile("^(?:" + entry + ")$")
	if err != nil {
		return nil, false
	}
	return re, true
}

// excluded reports whether an include entry is filtered out by an exclude
// entry (plain exact match or a matching pattern).
func (c ConnectorInclude) excluded(table string) bool {
	for _, re := range c.exclude {
		if re.MatchString(table) {
			return true
		}
	}
	return false
}

// Includes reports whether the connector captures table. For a connector with
// no include list it answers true: Debezium then captures every table.
func (c ConnectorInclude) Includes(table string) bool {
	if c.excluded(table) {
		return false
	}
	if c.signal != "" && table == c.signal {
		return true
	}
	if slices.Contains(c.Literal, table) {
		return true
	}
	for _, re := range c.include {
		if re.MatchString(table) {
			return true
		}
	}
	return c.Unbounded
}

// OutsideDeclared returns the connector's literal tables that owners.yaml does
// not declare, in sorted order. These are the tables the pipeline's own
// connector streams but the agent's scope does not include, so a fault on them
// is invisible.
func (c ConnectorInclude) OutsideDeclared(declared []string) []string {
	var out []string
	for _, t := range c.Literal {
		if !slices.Contains(declared, t) {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out
}

// scopeTable is one row of the connector_scope_gap result.
type scopeTable struct {
	Table         string `json:"table"`
	Declared      bool   `json:"declared"`
	InConnector   bool   `json:"in_connector"`
	InPublication bool   `json:"in_publication"`
}

// connectorScopeGapTool compares the pipeline's connector table filter with
// owners.yaml's source_tables and the publication's members. It never lists a
// table that is neither declared nor in this pipeline's connector.
func connectorScopeGapTool(d Deps) tools.Tool {
	return &tool{
		name: "connector_scope_gap",
		desc: "Compare a pipeline's Debezium connector table.include.list with owners.yaml source_tables and the publication. Lists tables the connector captures but owners.yaml does not declare (the agent is blind to faults on them), and declared tables missing from the connector or publication. Use when a table's changes never reach Kafka but nothing fails.",
		schema: objSchema(map[string]any{
			"pipeline_id": strProp("Pipeline id from owners.yaml."),
		}, "pipeline_id"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				PipelineID string `json:"pipeline_id"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if input.PipelineID == "" {
				return tools.Result{}, fmt.Errorf("%w: pipeline_id is required", tools.ErrBadInput)
			}
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			p, err := d.resolve(sc, input.PipelineID)
			if err != nil {
				return tools.Result{}, err
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			conn, err := resolveConnector(ctx, p, d.Connect)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: connector_scope_gap: %w", err)
			}
			inc := ParseConnectorInclude(conn.cfg)
			// The union of what the pipeline declares and what its connector
			// names literally. Nothing else is ever asked of the database.
			tables := append([]string(nil), p.SourceTables...)
			for _, t := range inc.Literal {
				if !slices.Contains(tables, t) {
					tables = append(tables, t)
				}
			}
			var membership []postgres.PublicationTable
			other := 0
			if len(tables) > 0 {
				read, err := d.publicationReader()
				if err != nil {
					return tools.Result{}, err
				}
				membership, other, err = read(ctx, conn.publication, conn.role, tables)
				if err != nil {
					return tools.Result{}, fmt.Errorf("dbtools: connector_scope_gap: %w", err)
				}
			}
			inPub := map[string]bool{}
			for _, t := range membership {
				inPub[t.Schema+"."+t.Table] = t.InPublication
			}

			rows := make([]scopeTable, 0, len(tables))
			gaps := inc.OutsideDeclared(p.SourceTables)
			var unlisted, unpublished []string
			for _, t := range tables {
				row := scopeTable{
					Table:         t,
					Declared:      slices.Contains(p.SourceTables, t),
					InConnector:   inc.Includes(t),
					InPublication: inPub[t],
				}
				if row.Declared && !row.InConnector {
					unlisted = append(unlisted, t)
				}
				if row.Declared && !row.InPublication {
					unpublished = append(unpublished, t)
				}
				rows = append(rows, row)
			}

			text := connectorScopeSummary(conn, inc, rows, gaps)
			if !sc.Unscoped() && other > 0 {
				if line := tools.NotShown(other, "other table in publication "+conn.publication, "other tables in publication "+conn.publication); line != "" {
					text += line + "\n"
				}
			}
			summary, truncated := tools.Truncate(text)
			return tools.Result{
				Summary:   summary,
				Data:      connectorScopeData(p.ID, conn, inc, rows, gaps, unlisted, unpublished),
				Truncated: truncated,
			}, nil
		},
	}
}

// connectorScopeSummary renders the header, one line per table and the
// follow-up for every table the connector streams outside declared scope.
func connectorScopeSummary(conn connectorConfig, inc ConnectorInclude, rows []scopeTable, gaps []string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "connector %s, publication %s (connector user %s)\n", conn.name, conn.publication, conn.role)
	for _, r := range rows {
		fmt.Fprintf(&sb, "%s: declared=%s, in_connector=%s, in_publication=%s", r.Table, yesNo(r.Declared), yesNo(r.InConnector), yesNo(r.InPublication))
		if !r.Declared {
			sb.WriteString(" (in connector, outside declared scope)")
		}
		sb.WriteString("\n")
	}
	switch {
	case inc.Unbounded:
		sb.WriteString("table.include.list is not set: the connector captures every table in the schema, so its scope cannot be compared with owners.yaml.\n")
	case len(inc.Patterns) > 0:
		fmt.Fprintf(&sb, "table.include.list has patterns that cannot be expanded: %s; check their tables by hand.\n", strings.Join(inc.Patterns, ", "))
	}
	if len(inc.Invalid) > 0 {
		fmt.Fprintf(&sb, "ignored unreadable include/exclude entries: %s.\n", strings.Join(inc.Invalid, ", "))
	}
	for _, t := range gaps {
		fmt.Fprintf(&sb, "%s is streamed by the connector but owners.yaml does not declare it: ask the pipeline owner to add it to source_tables before proposing a fix for it.\n", t)
	}
	return sb.String()
}

// connectorScopeData is the structured result: the connector and publication
// identity plus the per-table view and the three gap lists.
func connectorScopeData(pipelineID string, conn connectorConfig, inc ConnectorInclude, rows []scopeTable, gaps, unlisted, unpublished []string) map[string]any {
	return map[string]any{
		"pipeline_id":                 pipelineID,
		"connector":                   conn.name,
		"publication":                 conn.publication,
		"publication_source":          conn.publicationSource,
		"connector_user":              conn.role,
		"tables":                      rows,
		"outside_declared_scope":      nonNilStrings(gaps),
		"declared_not_in_connector":   nonNilStrings(unlisted),
		"declared_not_in_publication": nonNilStrings(unpublished),
		"include_patterns":            nonNilStrings(inc.Patterns),
		"include_unbounded":           inc.Unbounded,
	}
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
