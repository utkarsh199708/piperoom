package actions

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"piperoom/internal/dlq"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// The two Debezium heartbeat keys connect_update_connector_config admits
// (Q2c follow-up). source_create_heartbeat_table creates the table and names
// this action as the next step. Unlike the other allow-listed keys their
// values are not a fixed set: heartbeatValueCheck checks them.
const (
	keyHeartbeatInterval = "heartbeat.interval.ms"
	keyHeartbeatQuery    = "heartbeat.action.query"
)

// Debezium's heartbeat.interval.ms: 0 (its default) turns heartbeats off.
// Below one second the connector writes to the source more often than any
// quiet-table WAL problem needs; above an hour the slot still holds WAL for
// that long, which defeats the point.
const (
	minHeartbeatIntervalMS = 1000
	maxHeartbeatIntervalMS = 3600000
)

// Caps of the heartbeat query's parts. A heartbeat row needs few columns and
// short literals; anything larger is not a heartbeat.
const (
	maxHeartbeatColumns = 4
	maxHeartbeatLiteral = 64
)

// heartbeatIdentRe is rule 6 (AGENTS.md §4) with Postgres's 63-byte cap.
var heartbeatIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

// heartbeatIntervalRe admits a plain decimal: no sign, no leading zero, no
// unit, so "1e4", "+1000" and "01000" are refused before any number parse.
var heartbeatIntervalRe = regexp.MustCompile(`^(0|[1-9][0-9]{0,6})$`)

// heartbeatLiteralRe is the inside of a string literal the query may carry:
// no quote, no backslash, nothing an escape could hide in.
var heartbeatLiteralRe = regexp.MustCompile(`^[A-Za-z0-9 _:.-]*$`)

// isHeartbeatKey reports whether key is one of the two heartbeat keys.
func isHeartbeatKey(key string) bool {
	return key == keyHeartbeatInterval || key == keyHeartbeatQuery
}

// heartbeatErr is a refusal of a heartbeat value: tools.ErrBadInput, so the
// room answers it as the proposer's mistake.
func heartbeatErr(key, format string, args ...any) error {
	return fmt.Errorf("actions: %s: %w: %s", key, tools.ErrBadInput, fmt.Sprintf(format, args...))
}

// heartbeatValueCheck checks the value of a heartbeat key without I/O. The
// query's scope (its table against the pipeline) is checked apart, where a
// pipeline is known (heartbeatQueryInScope).
func heartbeatValueCheck(key, value string) error {
	switch key {
	case keyHeartbeatInterval:
		return checkHeartbeatInterval(value)
	case keyHeartbeatQuery:
		_, err := parseHeartbeatQuery(value)
		return err
	}
	return fmt.Errorf("actions: %s is not a heartbeat key", key)
}

func checkHeartbeatInterval(v string) error {
	if !heartbeatIntervalRe.MatchString(v) {
		return heartbeatErr(keyHeartbeatInterval, "value %q is not a whole number of milliseconds; allowed: 0 (heartbeats off) or %d..%d", shortText(v), minHeartbeatIntervalMS, maxHeartbeatIntervalMS)
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return heartbeatErr(keyHeartbeatInterval, "value %q: %v", v, err)
	}
	if n != 0 && (n < minHeartbeatIntervalMS || n > maxHeartbeatIntervalMS) {
		return heartbeatErr(keyHeartbeatInterval, "value %d is out of range; allowed: 0 (heartbeats off) or %d..%d", n, minHeartbeatIntervalMS, maxHeartbeatIntervalMS)
	}
	return nil
}

// heartbeatQuery is a parsed heartbeat.action.query: its target table
// ("schema.table", unquoted) and its string literals (for the secret check).
type heartbeatQuery struct {
	table    string
	literals []string
}

// hbTok is one token of a heartbeat query.
type hbTok struct {
	kind byte // 'w' bare word, 'q' quoted identifier, 'n' number, 's' string literal, 'p' punctuation
	text string
}

// parseHeartbeatQuery admits exactly one statement of these shapes, keywords
// in any case, and nothing else:
//
//	INSERT INTO s.t (c, ...) VALUES (v, ...)
//	    [ON CONFLICT (c, ...) DO NOTHING | ON CONFLICT (c, ...) DO UPDATE SET c = v|EXCLUDED.c, ...
//	     | ON DUPLICATE KEY UPDATE c = v, ...]
//	UPDATE s.t SET c = v, ... [WHERE c = v]
//
// where v is an integer, a short string literal, NOW(), CLOCK_TIMESTAMP() or
// CURRENT_TIMESTAMP, and an optional trailing ";". Debezium runs the text on
// the source database with the connector's credentials, so this is a
// grammar of what a heartbeat needs, not a deny list: no sub-query, no other
// function, no comment, no second statement can be written in it.
func parseHeartbeatQuery(q string) (heartbeatQuery, error) {
	fail := func(format string, args ...any) (heartbeatQuery, error) {
		return heartbeatQuery{}, heartbeatErr(keyHeartbeatQuery, format, args...)
	}
	if strings.Contains(q, "--") || strings.Contains(q, "/*") || strings.Contains(q, "*/") || strings.Contains(q, "#") {
		return fail("comments are not allowed")
	}
	body := strings.TrimRight(q, " \t\r\n")
	body = strings.TrimSuffix(body, ";")
	if strings.Contains(body, ";") {
		return fail("one statement only (a single trailing ';' is allowed)")
	}
	toks, err := lexHeartbeatQuery(body)
	if err != nil {
		return heartbeatQuery{}, err
	}
	p := &hbParser{toks: toks}
	var out heartbeatQuery
	switch {
	case p.word("INSERT"):
		out, err = p.insert()
	case p.word("UPDATE"):
		out, err = p.update()
	default:
		return fail("must start with INSERT INTO or UPDATE")
	}
	if err != nil {
		return heartbeatQuery{}, err
	}
	if !p.done() {
		return fail("unexpected %q after the statement", shortText(p.peek().text))
	}
	return out, nil
}

// lexHeartbeatQuery splits q into tokens and refuses any character outside
// the grammar's alphabet.
func lexHeartbeatQuery(q string) ([]hbTok, error) {
	var toks []hbTok
	for i := 0; i < len(q); {
		c := q[i]
		switch {
		case c == ' ' || c == '\t' || c == '\r' || c == '\n':
			i++
		case c == '(' || c == ')' || c == ',' || c == '=' || c == '.':
			toks = append(toks, hbTok{'p', string(c)})
			i++
		case isHBWordStart(c):
			j := i + 1
			for j < len(q) && isHBWordPart(q[j]) {
				j++
			}
			toks = append(toks, hbTok{'w', q[i:j]})
			i = j
		case c >= '0' && c <= '9':
			j := i + 1
			for j < len(q) && q[j] >= '0' && q[j] <= '9' {
				j++
			}
			if j-i > 18 || (j < len(q) && isHBWordPart(q[j])) {
				return nil, heartbeatErr(keyHeartbeatQuery, "bad number %q", shortText(q[i:min(j+1, len(q))]))
			}
			toks = append(toks, hbTok{'n', q[i:j]})
			i = j
		case c == '"' || c == '`':
			end := strings.IndexByte(q[i+1:], c)
			if end < 0 {
				return nil, heartbeatErr(keyHeartbeatQuery, "unterminated quoted identifier")
			}
			id := q[i+1 : i+1+end]
			if !heartbeatIdentRe.MatchString(id) {
				return nil, heartbeatErr(keyHeartbeatQuery, "quoted identifier %q: letters, digits and _ only, not starting with a digit, at most 63", shortText(id))
			}
			toks = append(toks, hbTok{'q', id})
			i += end + 2
		case c == '\'':
			end := strings.IndexByte(q[i+1:], '\'')
			if end < 0 {
				return nil, heartbeatErr(keyHeartbeatQuery, "unterminated string literal")
			}
			lit := q[i+1 : i+1+end]
			if len(lit) > maxHeartbeatLiteral || !heartbeatLiteralRe.MatchString(lit) {
				return nil, heartbeatErr(keyHeartbeatQuery, "string literal: letters, digits, space and _ : . - only, at most %d bytes", maxHeartbeatLiteral)
			}
			// A quote right after the closing one is an escaped quote ('').
			if i+end+2 < len(q) && q[i+end+2] == '\'' {
				return nil, heartbeatErr(keyHeartbeatQuery, "escaped quotes are not allowed in a string literal")
			}
			toks = append(toks, hbTok{'s', lit})
			i += end + 2
		default:
			return nil, heartbeatErr(keyHeartbeatQuery, "character %q is not allowed", shortText(string([]rune(q[i:])[:1])))
		}
	}
	return toks, nil
}

func isHBWordStart(c byte) bool { return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }
func isHBWordPart(c byte) bool  { return isHBWordStart(c) || c >= '0' && c <= '9' }

// hbParser walks the tokens of one heartbeat query.
type hbParser struct {
	toks []hbTok
	i    int
	lits []string
}

func (p *hbParser) done() bool { return p.i >= len(p.toks) }

func (p *hbParser) peek() hbTok {
	if p.done() {
		return hbTok{}
	}
	return p.toks[p.i]
}

// word consumes the bare keyword w (any case) if it is next.
func (p *hbParser) word(w string) bool {
	if t := p.peek(); t.kind == 'w' && strings.EqualFold(t.text, w) {
		p.i++
		return true
	}
	return false
}

// punct consumes the punctuation s if it is next.
func (p *hbParser) punct(s string) bool {
	if t := p.peek(); t.kind == 'p' && t.text == s {
		p.i++
		return true
	}
	return false
}

func (p *hbParser) expectWord(w string) error {
	if !p.word(w) {
		return heartbeatErr(keyHeartbeatQuery, "expected %s, got %q", w, shortText(p.peek().text))
	}
	return nil
}

func (p *hbParser) expectPunct(s string) error {
	if !p.punct(s) {
		return heartbeatErr(keyHeartbeatQuery, "expected %q, got %q", s, shortText(p.peek().text))
	}
	return nil
}

// ident consumes one identifier, bare or quoted, checked with rule 6.
func (p *hbParser) ident() (string, error) {
	t := p.peek()
	if (t.kind != 'w' && t.kind != 'q') || !heartbeatIdentRe.MatchString(t.text) {
		return "", heartbeatErr(keyHeartbeatQuery, "expected an identifier, got %q", shortText(t.text))
	}
	p.i++
	return t.text, nil
}

// table consumes "schema.table". An unqualified table is refused: its schema
// would come from the connector user's search_path, which the scope check
// cannot see.
func (p *hbParser) table() (string, error) {
	s, err := p.ident()
	if err != nil {
		return "", err
	}
	if !p.punct(".") {
		return "", heartbeatErr(keyHeartbeatQuery, "the table must be schema-qualified (schema.table)")
	}
	t, err := p.ident()
	if err != nil {
		return "", err
	}
	return s + "." + t, nil
}

// columns consumes "(c, ...)" and returns how many there were.
func (p *hbParser) columns() (int, error) {
	if err := p.expectPunct("("); err != nil {
		return 0, err
	}
	n := 0
	for {
		if _, err := p.ident(); err != nil {
			return 0, err
		}
		n++
		if n > maxHeartbeatColumns {
			return 0, heartbeatErr(keyHeartbeatQuery, "at most %d columns", maxHeartbeatColumns)
		}
		if p.punct(")") {
			return n, nil
		}
		if err := p.expectPunct(","); err != nil {
			return 0, err
		}
	}
}

// value consumes one value: an integer, a string literal, NOW(),
// CLOCK_TIMESTAMP() or CURRENT_TIMESTAMP. With excluded, EXCLUDED.c is a
// value too (ON CONFLICT ... DO UPDATE only).
func (p *hbParser) value(excluded bool) error {
	t := p.peek()
	switch t.kind {
	case 'n':
		p.i++
		return nil
	case 's':
		p.i++
		p.lits = append(p.lits, t.text)
		return nil
	case 'w':
		switch {
		case strings.EqualFold(t.text, "NOW"), strings.EqualFold(t.text, "CLOCK_TIMESTAMP"):
			p.i++
			if err := p.expectPunct("("); err != nil {
				return err
			}
			return p.expectPunct(")")
		case strings.EqualFold(t.text, "CURRENT_TIMESTAMP"):
			p.i++
			return nil
		case excluded && strings.EqualFold(t.text, "EXCLUDED"):
			p.i++
			if err := p.expectPunct("."); err != nil {
				return err
			}
			_, err := p.ident()
			return err
		}
	}
	return heartbeatErr(keyHeartbeatQuery, "value %q is not allowed: an integer, a string literal, NOW(), CLOCK_TIMESTAMP() or CURRENT_TIMESTAMP", shortText(t.text))
}

// assignments consumes "c = v, ...".
func (p *hbParser) assignments(excluded bool) error {
	for n := 1; ; n++ {
		if n > maxHeartbeatColumns {
			return heartbeatErr(keyHeartbeatQuery, "at most %d assignments", maxHeartbeatColumns)
		}
		if _, err := p.ident(); err != nil {
			return err
		}
		if err := p.expectPunct("="); err != nil {
			return err
		}
		if err := p.value(excluded); err != nil {
			return err
		}
		if !p.punct(",") {
			return nil
		}
	}
}

func (p *hbParser) insert() (heartbeatQuery, error) {
	if err := p.expectWord("INTO"); err != nil {
		return heartbeatQuery{}, err
	}
	table, err := p.table()
	if err != nil {
		return heartbeatQuery{}, err
	}
	cols, err := p.columns()
	if err != nil {
		return heartbeatQuery{}, err
	}
	if err := p.expectWord("VALUES"); err != nil {
		return heartbeatQuery{}, err
	}
	if err := p.expectPunct("("); err != nil {
		return heartbeatQuery{}, err
	}
	vals := 0
	for {
		if err := p.value(false); err != nil {
			return heartbeatQuery{}, err
		}
		vals++
		if p.punct(")") {
			break
		}
		if err := p.expectPunct(","); err != nil {
			return heartbeatQuery{}, err
		}
	}
	if vals != cols {
		return heartbeatQuery{}, heartbeatErr(keyHeartbeatQuery, "%d columns but %d values", cols, vals)
	}
	if p.word("ON") {
		switch {
		case p.word("CONFLICT"):
			if _, err := p.columns(); err != nil {
				return heartbeatQuery{}, err
			}
			if err := p.expectWord("DO"); err != nil {
				return heartbeatQuery{}, err
			}
			if !p.word("NOTHING") {
				if err := p.expectWord("UPDATE"); err != nil {
					return heartbeatQuery{}, err
				}
				if err := p.expectWord("SET"); err != nil {
					return heartbeatQuery{}, err
				}
				if err := p.assignments(true); err != nil {
					return heartbeatQuery{}, err
				}
			}
		case p.word("DUPLICATE"):
			for _, w := range []string{"KEY", "UPDATE"} {
				if err := p.expectWord(w); err != nil {
					return heartbeatQuery{}, err
				}
			}
			if err := p.assignments(false); err != nil {
				return heartbeatQuery{}, err
			}
		default:
			return heartbeatQuery{}, heartbeatErr(keyHeartbeatQuery, "after ON: CONFLICT or DUPLICATE KEY UPDATE only")
		}
	}
	return heartbeatQuery{table: table, literals: p.lits}, nil
}

func (p *hbParser) update() (heartbeatQuery, error) {
	table, err := p.table()
	if err != nil {
		return heartbeatQuery{}, err
	}
	if err := p.expectWord("SET"); err != nil {
		return heartbeatQuery{}, err
	}
	if err := p.assignments(false); err != nil {
		return heartbeatQuery{}, err
	}
	if p.word("WHERE") {
		if _, err := p.ident(); err != nil {
			return heartbeatQuery{}, err
		}
		if err := p.expectPunct("="); err != nil {
			return heartbeatQuery{}, err
		}
		if err := p.value(false); err != nil {
			return heartbeatQuery{}, err
		}
	}
	return heartbeatQuery{table: table, literals: p.lits}, nil
}

// heartbeatQueryInScope refuses a query whose table is outside pipeline pl:
// the table must be one of pl's source_tables or sit in a schema that holds
// one (source_create_heartbeat_table's rule: a heartbeat table is created
// next to the captured tables, and joins source_tables when a filtered
// publication needs it). The match is exact: an unquoted name Postgres would
// fold to lower case must already be written in lower case.
func heartbeatQueryInScope(pl owners.Pipeline, query string) error {
	hq, err := parseHeartbeatQuery(query)
	if err != nil {
		return err
	}
	if slices.Contains(pl.SourceTables, hq.table) {
		return nil
	}
	schema, _, _ := strings.Cut(hq.table, ".")
	for _, t := range pl.SourceTables {
		if s, _, ok := strings.Cut(t, "."); ok && s == schema {
			return nil
		}
	}
	return fmt.Errorf("actions: %s: %w: table %s is not in a schema of pipeline %q's source_tables", keyHeartbeatQuery, tools.ErrOutOfScope, hq.table, pl.ID)
}

// checkScope checks the set's heartbeat query, if any, against pl.
func (s configChangeSet) checkScope(pl owners.Pipeline) error {
	if q, ok := s.changes[keyHeartbeatQuery]; ok {
		return heartbeatQueryInScope(pl, q)
	}
	return nil
}

// ctxHeartbeatScope checks the set's heartbeat query against the pipeline ctx
// carries. With require, a query without a pipeline in ctx is refused
// (tools.ErrNoScope): Execute and Stage run SQL-bearing config only for a
// known pipeline. Without require (Validate, Preview) it passes, as
// ctxConnectorScope does, and CheckScope is the check.
func ctxHeartbeatScope(ctx context.Context, s configChangeSet, require bool) error {
	if _, ok := s.changes[keyHeartbeatQuery]; !ok {
		return nil
	}
	pl, ok := tools.PipelineFrom(ctx)
	if !ok {
		if require {
			return fmt.Errorf("actions: %s: %w: needs the room's pipeline in ctx to check its table", keyHeartbeatQuery, tools.ErrNoScope)
		}
		return nil
	}
	return s.checkScope(pl)
}

// checkHeartbeatGuard refuses a heartbeat key on a connector that is not a
// Debezium source: a sink or another vendor's connector does not read the
// keys, and a MongoDB connector has no SQL to run a query with.
func checkHeartbeatGuard(connector, key string, cfg map[string]string) error {
	class := strings.TrimSpace(cfg["connector.class"])
	if !strings.HasPrefix(class, "io.debezium.connector.") || dlq.Parse(cfg).IsSink {
		return fmt.Errorf("actions: %s only applies to a Debezium source connector; %s is not one (connector.class %q)", key, connector, shortText(class))
	}
	if key == keyHeartbeatQuery && strings.HasPrefix(class, "io.debezium.connector.mongodb.") {
		return fmt.Errorf("actions: %s does not apply to %s: a MongoDB connector runs no SQL heartbeat query", key, connector)
	}
	return nil
}

// heartbeatValueLooksSecret is valueLooksSecret for a heartbeat value. The
// query is SQL, whose keywords trip the key-name rule ("ON DUPLICATE KEY"), so
// for it the redactor reads the whole text and the key-name rule reads only
// the string literals: the one place a credential could be pasted.
func heartbeatValueLooksSecret(key, value string) bool {
	if key != keyHeartbeatQuery {
		return valueLooksSecret(value)
	}
	if _, st := redact.Text(value); st.Total > 0 {
		return true
	}
	hq, err := parseHeartbeatQuery(value)
	if err != nil {
		return false // refused by the grammar anyway
	}
	for _, l := range hq.literals {
		if valueLooksSecret(l) {
			return true
		}
	}
	return false
}

// heartbeatSchemaText is the params-schema description of a heartbeat key.
func heartbeatSchemaText(key string) string {
	if key == keyHeartbeatInterval {
		return fmt.Sprintf("Debezium source connectors only: milliseconds between heartbeats, 0 (off) or %d..%d", minHeartbeatIntervalMS, maxHeartbeatIntervalMS)
	}
	return "Debezium source connectors only: one INSERT INTO or UPDATE of a schema.table in a schema of the pipeline's source_tables; values: integers, short string literals, NOW(), CLOCK_TIMESTAMP(), CURRENT_TIMESTAMP"
}

// heartbeatRestorable reports whether value, read from the connector's
// current config, can be written back by this action on undo.
func heartbeatRestorable(key, value string, pl *owners.Pipeline) bool {
	if heartbeatValueCheck(key, value) != nil || heartbeatValueLooksSecret(key, value) {
		return false
	}
	if key == keyHeartbeatQuery && pl != nil {
		return heartbeatQueryInScope(*pl, value) == nil
	}
	return true
}
