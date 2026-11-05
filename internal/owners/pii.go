package owners

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"piperoom/internal/redact"
)

// PIISpec is the optional per-pipeline `pii:` block (X7c,
// docs/design/lineage.md section 4): overrides of the column-name classifier
// (redact.ClassifyColumn). Include adds a column as personal data, exclude
// removes a classifier hit. Entries are "<table>.<column>", where <table> is
// one of the pipeline's source_tables or sink_tables, or a bare "<column>"
// for that column in every table of the pipeline; an include entry may end
// in ":<class>" (default PIIClassOther).
type PIISpec struct {
	Include []string `yaml:"include"`
	Exclude []string `yaml:"exclude"`
}

// MaxPIIEntries bounds each of pii.include and pii.exclude.
const MaxPIIEntries = 500

// PIIClassOther is the class of an include entry without one; it is
// topology.PIIOther (this package cannot import topology).
const PIIClassOther = "other"

// PIIOverride is what a pipeline's pii: block says about one column.
type PIIOverride int

// The three answers of PIIOverrideFor.
const (
	PIINoOverride PIIOverride = iota // the classifier decides
	PIIIncluded                      // personal data, with the entry's class
	PIIExcluded                      // not personal data, whatever the classifier says
)

// piiColumnRE is the column part of an entry: the catalog identifier rule
// the clients use, which also keeps ',' and ':' out of the TagPII value.
var piiColumnRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,127}$`)

// piiEntry is one parsed include or exclude entry; table "" = every table.
type piiEntry struct {
	table, column, class string
}

// piiRules is the validated pii: block, keyed by table "|" lower-case column.
type piiRules struct {
	include map[string]string // key -> class
	exclude map[string]bool
}

// validatePII parses and checks the pii: block (a load error names the
// entry); validate prefixes the pipeline.
func (p *Pipeline) validatePII() error {
	p.pii = nil
	if p.PII == nil {
		return nil
	}
	tables := append(slices.Clone(p.SourceTables), p.SinkTableNames()...)
	r := &piiRules{include: map[string]string{}, exclude: map[string]bool{}}
	for _, l := range []struct {
		name    string
		entries []string
		include bool
	}{{"pii.include", p.PII.Include, true}, {"pii.exclude", p.PII.Exclude, false}} {
		if len(l.entries) > MaxPIIEntries {
			return fmt.Errorf("%s: %d entries, at most %d", l.name, len(l.entries), MaxPIIEntries)
		}
		for _, raw := range l.entries {
			e, err := parsePIIEntry(raw, tables, l.include)
			if err != nil {
				return fmt.Errorf("%s entry %q: %w", l.name, raw, err)
			}
			key := e.table + "|" + strings.ToLower(e.column)
			_, inc := r.include[key]
			switch {
			case l.include && inc, !l.include && r.exclude[key]:
				return fmt.Errorf("%s entry %q: the column is listed twice", l.name, raw)
			case inc: // include is read first
				return fmt.Errorf("%s entry %q: the column is also in pii.include", l.name, raw)
			case l.include:
				r.include[key] = e.class
			default:
				r.exclude[key] = true
			}
		}
	}
	// A bare entry in one list and a table entry in the other name the same
	// column of that table: the intent is unclear, so it is refused.
	for key := range r.exclude {
		table, col, _ := strings.Cut(key, "|")
		for _, other := range overlapKeys(table, col, tables) {
			if _, ok := r.include[other]; ok {
				return fmt.Errorf("pii: column %q of %s is in both include and exclude", col, tableOrAll(table))
			}
		}
	}
	p.pii = r
	return nil
}

// overlapKeys are the include keys that would apply to the same column as
// the exclude key table|col.
func overlapKeys(table, col string, tables []string) []string {
	if table != "" {
		return []string{"|" + col}
	}
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		out = append(out, t+"|"+col)
	}
	return out
}

func tableOrAll(t string) string {
	if t == "" {
		return "every table"
	}
	return t
}

// parsePIIEntry reads "<table>.<column>[:<class>]" or "<column>[:<class>]".
func parsePIIEntry(raw string, tables []string, include bool) (piiEntry, error) {
	s := strings.TrimSpace(raw)
	var e piiEntry
	if body, class, ok := strings.Cut(s, ":"); ok {
		if !include {
			return e, fmt.Errorf("an exclude entry takes no class")
		}
		if class != PIIClassOther && !slices.Contains(redact.ColumnClasses(), class) {
			return e, fmt.Errorf("unknown class %q (want one of %s, %s)", class, strings.Join(redact.ColumnClasses(), ", "), PIIClassOther)
		}
		s, e.class = body, class
	} else if include {
		e.class = PIIClassOther
	}
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		e.table, e.column = s[:i], s[i+1:]
		if !slices.Contains(tables, e.table) {
			if !strings.Contains(e.table, ".") {
				return e, fmt.Errorf("want <schema>.<table>.<column> or a bare <column>, not %q", s)
			}
			return e, fmt.Errorf("table %q is not one of the pipeline's source_tables or sink_tables", e.table)
		}
	} else {
		e.column = s
	}
	if !piiColumnRE.MatchString(e.column) {
		return e, fmt.Errorf("column %q: want a catalog name (letters, digits, _ and $, at most 128 characters)", e.column)
	}
	return e, nil
}

// PIIOverrideFor returns what the pii: block says about column of table
// ("schema.table" as the pipeline lists it). The column compares without
// case (Snowflake upper-cases names); an entry naming the table wins over a
// bare one. The class is set only for PIIIncluded.
func (p Pipeline) PIIOverrideFor(table, column string) (PIIOverride, string) {
	if p.pii == nil {
		return PIINoOverride, ""
	}
	col := strings.ToLower(column)
	for _, key := range []string{table + "|" + col, "|" + col} {
		if class, ok := p.pii.include[key]; ok {
			return PIIIncluded, class
		}
		if p.pii.exclude[key] {
			return PIIExcluded, ""
		}
	}
	return PIINoOverride, ""
}
