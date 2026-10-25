// Package logql holds the LogQL stream selector check shared by the Loki
// reader (internal/clients/workerlogs, WORKER_LOGS_SELECTOR) and the owners
// config (internal/owners, a pipeline's consumer_logs.selector). It is a leaf
// package: internal/owners cannot import workerlogs, which imports
// internal/tools, which imports internal/owners.
package logql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// MaxSelectorBytes bounds a stream selector. A stream selector names a few
// labels; anything longer is a mistake.
const MaxSelectorBytes = 1024

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
	if len(s) > MaxSelectorBytes {
		return fmt.Errorf("selector is longer than %d bytes", MaxSelectorBytes)
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
