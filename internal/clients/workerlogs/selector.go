package workerlogs

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// maxSelectorBytes bounds WORKER_LOGS_SELECTOR. A stream selector names a
// few labels; anything longer is a mistake.
const maxSelectorBytes = 1024

var labelNameRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*`)

// CheckSelector reports whether sel is a LogQL stream selector and nothing
// else: `{` one or more `name op "value"` matchers separated by commas `}`,
// with op one of = != =~ !~ and the value a double-quoted or backquoted
// string. A pipeline stage (`|= "x"`, `| json`) or a metric query is
// refused: the operator chooses which streams are read, the tool adds the
// filters, and a selector that already filtered would change what "the
// newest lines" means without the tool knowing.
func CheckSelector(sel string) error {
	s := strings.TrimSpace(sel)
	if s == "" {
		return fmt.Errorf("selector is empty")
	}
	if len(s) > maxSelectorBytes {
		return fmt.Errorf("selector is longer than %d bytes", maxSelectorBytes)
	}
	if s[0] != '{' {
		return fmt.Errorf("selector must start with '{'")
	}
	s = s[1:]
	matchers := 0
	for {
		s = strings.TrimLeft(s, " \t")
		if strings.HasPrefix(s, "}") {
			if matchers == 0 {
				return fmt.Errorf("selector has no label matcher")
			}
			if rest := strings.TrimSpace(s[1:]); rest != "" {
				return fmt.Errorf("selector has text after '}' (pipeline stages are not allowed)")
			}
			return nil
		}
		name := labelNameRE.FindString(s)
		if name == "" {
			return fmt.Errorf("selector matcher %d: bad label name", matchers+1)
		}
		s = strings.TrimLeft(s[len(name):], " \t")
		switch {
		case strings.HasPrefix(s, "=~"), strings.HasPrefix(s, "!~"), strings.HasPrefix(s, "!="):
			s = s[2:]
		case strings.HasPrefix(s, "="):
			s = s[1:]
		default:
			return fmt.Errorf("selector matcher %d (%s): want = != =~ or !~", matchers+1, name)
		}
		s = strings.TrimLeft(s, " \t")
		q, err := strconv.QuotedPrefix(s)
		if err != nil || q[0] == '\'' {
			return fmt.Errorf("selector matcher %d (%s): value is not a quoted string", matchers+1, name)
		}
		s = strings.TrimLeft(s[len(q):], " \t")
		matchers++
		switch {
		case strings.HasPrefix(s, ","):
			s = s[1:]
		case strings.HasPrefix(s, "}"):
		default:
			return fmt.Errorf("selector matcher %d (%s): want ',' or '}' after the value", matchers, name)
		}
	}
}

// buildQuery appends the line filters for q to the operator's selector.
// Every value from the model goes through strconv.Quote, which is the
// inverse of the strconv.Unquote Loki's LogQL lexer applies to string
// literals, so a value can never close its literal and add a stage; the
// regex filters additionally pass the connector name through
// regexp.QuoteMeta so it matches literally.
func buildQuery(selector string, q LogQuery) string {
	var b strings.Builder
	b.WriteString(strings.TrimSpace(selector))
	if q.Connector != "" {
		if q.Task >= 0 {
			// Kafka Connect names a task "<connector>-<n>" (WorkerSourceTask{id=x-0})
			// and its log context "[<connector>|task-<n>]". \b keeps task 1 from
			// matching task 10.
			n := strconv.Itoa(q.Task)
			re := regexp.QuoteMeta(q.Connector) + `(-` + n + `\b|\|task-` + n + `\])`
			b.WriteString(" |~ ")
			b.WriteString(strconv.Quote(re))
		} else {
			b.WriteString(" |= ")
			b.WriteString(strconv.Quote(q.Connector))
		}
	}
	if lv := levelFilter(q.MinLevel); lv != "" {
		b.WriteString(" |~ ")
		b.WriteString(strconv.Quote(lv))
	}
	return b.String()
}

// levelFilter is a coarse server-side regex for lines at min or above, so
// the line limit is not spent on INFO lines; WorkerLogs then drops lines
// whose own level is lower (a WARN word inside an INFO message). Below WARN
// no server-side filter is worth its cost.
func levelFilter(min string) string {
	switch min {
	case "WARN":
		return `\b(WARN|WARNING|ERROR|SEVERE|FATAL)\b`
	case "ERROR":
		return `\b(ERROR|SEVERE|FATAL)\b`
	case "FATAL":
		return `\bFATAL\b`
	}
	return ""
}
