package dlq

// #440: PostgreSQL prints the key of a unique, foreign-key or exclusion
// violation as "Key (full_name)=(Jane Doe) already exists.". Behind a
// "Detail:" label the tail rule drops it, but some drivers print it without
// the label. maskKeyValues runs on RedactMessage's output (it only adds
// masks): the column list stays, which the agent needs, and the values go.

// keyValueSep is what PostgreSQL prints between a key's columns and its values.
var keyValueSep = []rune(")=(")

// keyEnds are the phrases PostgreSQL prints after the values, from the ')'
// that closes them.
var keyEnds = [][]rune{
	[]rune(") already exists"),
	[]rune(") is not present in table"),
	[]rune(") is still referenced from table"),
	[]rune(") conflicts with existing key"),
}

// maskKeyValues replaces the values of every "(cols)=(values)" with Redacted.
// The values are unquoted and may hold ")" or ", ", so they end at the last
// keyEnds phrase after them; without one they run to the end of the text, as
// in the "Failing row contains" rule. A second detail before that last phrase
// is masked with the first (over-mask, never a leak).
func maskKeyValues(s string) string {
	rs := []rune(s)
	var out []rune
	last := 0
	for i := 0; i+len(keyValueSep) <= len(rs); i++ {
		if string(rs[i:i+len(keyValueSep)]) != string(keyValueSep) {
			continue
		}
		open := i + len(keyValueSep) - 1
		out = append(append(out, rs[last:open]...), redactedRunes...)
		end := lastKeyEnd(rs, open+1)
		if end < 0 {
			return string(out)
		}
		last, i = end+1, end
	}
	if out == nil {
		return s
	}
	return string(append(out, rs[last:]...))
}

// lastKeyEnd returns the index of the ')' that starts the last keyEnds phrase
// at or after from, or -1.
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
