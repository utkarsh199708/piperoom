package dlq

import (
	"strings"
	"unicode"
)

// #544: key details that maskKeyValues does not read. MySQL prints
// "Duplicate entry 'v' for key 'users.ux_login'", SQL Server "The duplicate
// key value is (v).", and some drivers print PostgreSQL's detail with white
// space around "=" or with fullwidth "＝（". maskKeyForms runs on
// maskKeyValues' output and only adds masks (docs/agents/org.md §12): every
// pass replaces spans of its input with Redacted and copies the rest, so
// column and key names stay as main shows them.
func maskKeyForms(s string) string {
	return mergeSplitKeyValues(maskMySQLEntry(maskSQLServerKey(maskKeyVariants(s))))
}

// nextVariantSep returns, for the next variant separator at or after from
// (white space around '=', or '＝' or '（') behind a case-insensitive
// "Key (cols)" whose '(' lies after floor, the index of the '(' that opens its
// values, or -1. A bare "(cols) = (" is only a boundary (nextDetail): "f(x) = (y)" is
// prose as often as it is a key.
func nextVariantSep(rs []rune, from, floor int) int {
	for k := max(from, 0); k < len(rs); k++ {
		if rs[k] != ')' {
			continue
		}
		j := skipSpace(rs, k+1)
		if j >= len(rs) || (rs[j] != '=' && rs[j] != '＝') {
			continue
		}
		j = skipSpace(rs, j+1)
		if j >= len(rs) || (rs[j] != '(' && rs[j] != '（') {
			continue
		}
		if paren := columnParen(rs, floor, k); paren >= 0 && hasKeyWord(rs, paren) {
			return j
		}
	}
	return -1
}

// nextDetail returns where the next key detail at or after from starts, so
// that the tuple before it never runs into it: the column list (with its
// "Key ") of any ")=", ") = " or ")＝" whose '(' is at or after from, also a
// cut one with no values left; a "Key (cols" the cut left without its "=";
// a MySQL or SQL Server phrase (also one glued to the word before it or whose
// value is masked already); and, when forKey is set, MySQL's "for key", whose
// key name main shows. It returns len(rs) when there is none.
func nextDetail(rs []rune, from int, forKey bool) int {
	for k := max(from, 0); k < len(rs); k++ {
		if hasPrefixFold(rs[k:], mySQLEntryPhrase) || hasPrefixFold(rs[k:], sqlServerKeyPhrase) {
			return k
		}
		if wordStart(rs, k) && forKey && hasPrefixFold(rs[k:], forKeyWords) &&
			(k+len(forKeyWords) == len(rs) || !isWordRune(rs[k+len(forKeyWords)])) {
			return k
		}
		if wordStart(rs, k) && hasPrefixFold(rs[k:], keyWord) && k+len(keyWord) < len(rs) &&
			rs[k+len(keyWord)] == '(' && cutColumnList(rs, k+len(keyWord)) {
			return k
		}
		if rs[k] != ')' {
			continue
		}
		if j := skipSpace(rs, k+1); j >= len(rs) || (rs[j] != '=' && rs[j] != '＝') {
			continue
		}
		if p := columnParen(rs, from-1, k); p >= 0 {
			if p-len(keyWord) >= from && hasKeyWord(rs, p) {
				return p - len(keyWord)
			}
			return p
		}
	}
	return len(rs)
}

var forKeyWords = []rune("for key")

// cutColumnList reports whether the '(' at i is a column list the cap cut
// before its "=": it has no ')' that balances it, or that ')' ends the text.
func cutColumnList(rs []rune, i int) bool {
	depth := 0
	for k := i; k < len(rs); k++ {
		switch rs[k] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return skipSpace(rs, k+1) == len(rs)
			}
		}
	}
	return true
}

// maskKeyVariants replaces the values of every variant "Key (cols) = (values)"
// with Redacted, by maskKeyValues' rule: a tuple ends at the last keyEnds
// phrase before the next detail, else just before that detail, else at the
// end of the text.
func maskKeyVariants(s string) string {
	if !strings.ContainsAny(s, "=＝") {
		return s
	}
	rs := []rune(s)
	var out []rune
	last := 0
	for open := nextVariantSep(rs, 0, -1); open >= 0; open = nextVariantSep(rs, last, last-1) {
		out = append(append(out, rs[last:open]...), redactedRunes...)
		window := nextDetail(rs, open+1, true)
		last = window
		if end := lastKeyEndWide(rs[:window], open+1); end >= 0 {
			last = end + 1
		} else if window < len(rs) && window > 0 && unicode.IsSpace(rs[window-1]) {
			out = append(out, ' ')
		}
	}
	return string(append(out, rs[last:]...))
}

// columnParen returns the index of the '(' that balances the ')' at at, if it
// lies after floor, or -1.
func columnParen(rs []rune, floor, at int) int {
	depth := 0
	for k := at; k > floor && k >= 0; k-- {
		switch rs[k] {
		case ')':
			depth++
		case '(':
			depth--
		}
		if depth == 0 {
			return k
		}
	}
	return -1
}

// hasKeyWord reports whether "key " (any case) ends just before index p.
func hasKeyWord(rs []rune, p int) bool {
	return p-len(keyWord) >= 0 && hasPrefixFold(rs[p-len(keyWord):], keyWord)
}

// lastKeyEndWide is lastKeyEnd that also takes a fullwidth '）' as the
// phrase's closing paren.
func lastKeyEndWide(rs []rune, from int) int {
	for k := len(rs) - 1; k >= from; k-- {
		if rs[k] != ')' && rs[k] != '）' {
			continue
		}
		for _, p := range keyEnds {
			if hasPrefixFold(rs[k+1:], p[1:]) {
				return k
			}
		}
	}
	return -1
}

var (
	sqlServerKeyPhrase = []rune("duplicate key value is")
	sqlServerEndPhrase = []rune("the statement has been terminated")
)

// maskSQLServerKey replaces the tuple after SQL Server's "The duplicate key
// value is (v1, v2)." The values are unquoted and may hold ')' or ", ", so the
// tuple ends at the last ')' before "The statement has been terminated", the
// next detail (nextDetail) or the end of the text; without a ')' (a cut), the mask
// runs to there.
func maskSQLServerKey(s string) string {
	if !strings.Contains(strings.ToLower(s), string(sqlServerKeyPhrase)) {
		return s
	}
	rs := []rune(s)
	var out []rune
	last := 0
	for i := 0; i < len(rs); i++ {
		if !wordStart(rs, i) || !startsSQLServerKey(rs, i) {
			continue
		}
		j := skipSpace(rs, i+len(sqlServerKeyPhrase))
		window := nextDetail(rs, j+1, true)
		for k := j + 1; k < window; k++ {
			if wordStart(rs, k) && hasPrefixFold(rs[k:], sqlServerEndPhrase) {
				window = k
				break
			}
		}
		end := window
		for k := window - 1; k > j; k-- {
			if rs[k] == ')' || rs[k] == '）' {
				end = k + 1
				break
			}
		}
		out = append(append(out, rs[last:j]...), redactedRunes...)
		if end == window && window < len(rs) && window > j && unicode.IsSpace(rs[window-1]) {
			out = append(out, ' ')
		}
		last = end
		i = end - 1
	}
	return string(append(out, rs[last:]...))
}

var (
	mySQLEntryPhrase = []rune("duplicate entry")
	mySQLForKey      = []rune("' for key")
)

// maskMySQLEntry replaces the value after MySQL's "Duplicate entry 'v' for
// key 'name'", also when it looks like an identifier (main's quote rule keeps
// 'jdoe_smith'). The key name stays.
func maskMySQLEntry(s string) string {
	if !strings.Contains(strings.ToLower(s), string(mySQLEntryPhrase)) {
		return s
	}
	rs := []rune(s)
	var out []rune
	last := 0
	for i := 0; i < len(rs); i++ {
		if !wordStart(rs, i) || !startsMySQLEntry(rs, i) {
			continue
		}
		j := skipSpace(rs, i+len(mySQLEntryPhrase))
		window := nextDetail(rs, j+1, false)
		end := mySQLValueEnd(rs, j, window)
		out = append(append(out, rs[last:j]...), redactedRunes...)
		if end > j && end < len(rs) && unicode.IsSpace(rs[end-1]) {
			out = append(out, ' ')
		}
		last = end
		i = end - 1
	}
	return string(append(out, rs[last:]...))
}

// mySQLValueEnd returns where the value whose quote opens at j ends, before
// window. MySQL does not escape it, so it ends at the last "' for key"; else
// (main masked the closing quote, or a cut) just before the first "for key";
// else after the last quote that ends a word; else at window.
func mySQLValueEnd(rs []rune, j, window int) int {
	for k := window - len(mySQLForKey); k > j; k-- {
		if hasPrefixFold(rs[k:], mySQLForKey) {
			return k + 1 // the closing quote
		}
	}
	if k := nextDetail(rs[:window], j+1, true); k < window {
		return k
	}
	for k := window - 1; k > j; k-- {
		if rs[k] == '\'' && (k+1 == len(rs) || !isWordRune(rs[k+1])) {
			return k + 1
		}
	}
	return window
}

// mergeSplitKeyValues joins a masked key value with a ")=<redacted>" that
// follows it when the text between them holds no '(' for that ')': there is no
// column list there, so the text was part of the value. "Key (a)=(x) already
// exists y)=(z) already exists." reads in maskKeyValues as two details and
// keeps "y"; here it becomes "Key (a)=<redacted> already exists.". Text that
// holds the word "key" may name a column list printed without its '('
// ("Key Cq2)=(...)"), and text that holds another detail (nextDetail) is
// not a value, so both stay as main shows them.
func mergeSplitKeyValues(s string) string {
	if strings.Count(s, Redacted) < 2 {
		return s
	}
	rs := []rune(s)
	for from := 0; ; {
		m1 := indexRunes(rs, redactedRunes, from)
		if m1 < 0 {
			return string(rs)
		}
		end1 := m1 + len(redactedRunes)
		m2 := indexRunes(rs, redactedRunes, end1)
		if m2 < 0 {
			return string(rs)
		}
		if at := sepBefore(rs, m2); at >= end1 && sepBefore(rs, m1) >= 0 && columnParen(rs, end1-1, at) < 0 &&
			!hasKeyWordIn(rs[end1:at]) && nextDetail(rs[:at], end1, true) == at {
			rs = append(rs[:end1:end1], rs[m2+len(redactedRunes):]...)
			continue // the joined value may join the next one too
		}
		from = end1
	}
}

// sepBefore returns the index of the ')' of a ")=", ") = " or ")＝" that ends
// just before index m, or -1.
func sepBefore(rs []rune, m int) int {
	k := m - 1
	if k >= 0 && rs[k] == ' ' {
		k--
	}
	if k < 0 || (rs[k] != '=' && rs[k] != '＝') {
		return -1
	}
	k--
	if k >= 0 && rs[k] == ' ' {
		k--
	}
	if k < 0 || rs[k] != ')' {
		return -1
	}
	return k
}

// hasKeyWordIn reports whether rs holds the word "key" (any case).
func hasKeyWordIn(rs []rune) bool {
	for k := 0; k+3 <= len(rs); k++ {
		if wordStart(rs, k) && hasPrefixFold(rs[k:], keyWord[:3]) && (k+3 == len(rs) || !isWordRune(rs[k+3])) {
			return true
		}
	}
	return false
}

// startsSQLServerKey reports whether "duplicate key value is (" starts at i.
func startsSQLServerKey(rs []rune, i int) bool {
	if !hasPrefixFold(rs[i:], sqlServerKeyPhrase) {
		return false
	}
	j := skipSpace(rs, i+len(sqlServerKeyPhrase))
	return j < len(rs) && (rs[j] == '(' || rs[j] == '（')
}

// startsMySQLEntry reports whether "duplicate entry '" starts at i.
func startsMySQLEntry(rs []rune, i int) bool {
	if !hasPrefixFold(rs[i:], mySQLEntryPhrase) {
		return false
	}
	j := skipSpace(rs, i+len(mySQLEntryPhrase))
	return j < len(rs) && rs[j] == '\''
}

func skipSpace(rs []rune, i int) int {
	for i < len(rs) && unicode.IsSpace(rs[i]) {
		i++
	}
	return i
}

func hasPrefixRunes(rs, p []rune) bool {
	if len(rs) < len(p) {
		return false
	}
	for i := range p {
		if rs[i] != p[i] {
			return false
		}
	}
	return true
}

// indexRunes returns the index of the first p in rs at or after from, or -1.
func indexRunes(rs, p []rune, from int) int {
	for k := from; k+len(p) <= len(rs); k++ {
		if hasPrefixRunes(rs[k:], p) {
			return k
		}
	}
	return -1
}
