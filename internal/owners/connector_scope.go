package owners

import (
	"regexp"
	"slices"
	"sort"
	"strings"
)

// This file holds the table filter of a Debezium Postgres source connector
// (KAN-404): table.include.list and table.exclude.list, read the way Debezium
// matches them. It lives here, in a leaf package next to the pipeline
// ownership rules, so callers that only need the parser (the watcher's
// config_risk, for example) do not import the DB tools and their clients.

// plainTableEntry is a schema.table entry of table.include.list with no regex
// metacharacters. Every entry is a Debezium regex, so public.orders also
// matches publicXorders; we treat this shape as a literal on purpose, because
// it is the form every owners.yaml source table is written in and the only one
// a gap can be computed for.
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
