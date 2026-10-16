package dlq

import "strings"

// #440: PostgreSQL prints the key of a unique, foreign-key or exclusion
// violation as "Key (full_name)=(Jane Doe) already exists.". Behind a
// "Detail:" label the tail rule drops it, but some drivers print it without
// the label. maskKeyValues runs on RedactMessage's output (it only adds
// masks): the column list stays, which the agent needs, and the values go.

// keyValueSep is what PostgreSQL prints between a key's columns and its values.
const keyValueSep = ")=("

// keyEnds are the phrases PostgreSQL prints after the values, from the ')'
// that closes them.
var keyEnds = [][]rune{
	[]rune(") already exists"),
	[]rune(") is not present in table"),
	[]rune(") is still referenced from table"),
	[]rune(") conflicts with existing key"),
	[]rune(") conflicts with key"),
	[]rune(") is duplicated"),
}

// maskKeyValues replaces the values of every "(cols)=(values)" with Redacted.
// The values are unquoted and may hold ")" or ", ", so each tuple ends at the
// last keyEnds phrase before the next ")=(" (the next detail's columns stay
// visible). Without such a phrase it runs to the next ")=(", or to the end of
// the text, as in the "Failing row contains" rule.
func maskKeyValues(s string) string {
	if !strings.Contains(s, keyValueSep) {
		return s
	}
	rs := []rune(s)
	var out []rune
	last := 0
	for i := nextKeySep(rs, 0); i >= 0; {
		open := i + len(sepRunes) - 1
		out = append(append(out, rs[last:open]...), redactedRunes...)
		next := nextKeySep(rs, open)
		window := len(rs)
		if next >= 0 {
			window = next
		}
		last = window
		if end := lastKeyEnd(rs[:window], open+1); end >= 0 {
			last = end + 1 // the phrase's ")" closes the masked tuple
		}
		i = next
	}
	return string(append(out, rs[last:]...))
}

var sepRunes = []rune(keyValueSep)

// nextKeySep returns the index of the next ")=(" at or after from, or -1.
func nextKeySep(rs []rune, from int) int {
	for k := from; k+len(sepRunes) <= len(rs); k++ {
		if rs[k] == sepRunes[0] && rs[k+1] == sepRunes[1] && rs[k+2] == sepRunes[2] {
			return k
		}
	}
	return -1
}

// lastKeyEnd returns the index of the ')' that starts the last keyEnds phrase
// in rs at or after from, or -1.
func lastKeyEnd(rs []rune, from int) int {
	for k := len(rs) - 1; k >= from; k-- {
		if rs[k] != ')' {
			continue
		}
		for _, p := range keyEnds {
			if hasPrefixFold(rs[k:], p) {
				return k
			}
		}
	}
	return -1
}
