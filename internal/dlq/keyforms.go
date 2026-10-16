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

// keySep is a "(cols) = (" separator: at is the ')' that closes the columns,
// open the '(' that opens the values (or the start of the Redacted that
// already replaced them, when masked).
type keySep struct {
	at, open int
	masked   bool
}

// nextVariantSep returns the next separator at or after from whose column
// list opens after floor: a variant one (white space around '=', or '＝' or
// '（') behind a case-insensitive "Key (cols)", or one whose values a pass
// already masked. The masked ones are only boundaries: they keep the tuple
// before them from running into their column list. at is -1 when there is none.
func nextVariantSep(rs []rune, from, floor int) keySep {
	for k := max(from, 0); k < len(rs); k++ {
		if rs[k] != ')' {
			continue
		}
		j := skipSpace(rs, k+1)
		if j >= len(rs) || (rs[j] != '=' && rs[j] != '＝') {
			continue
		}
		j = skipSpace(rs, j+1)
		masked := hasPrefixRunes(rs[j:], redactedRunes)
		if !masked && (j >= len(rs) || (rs[j] != '(' && rs[j] != '（')) {
			continue
		}
		paren := columnParen(rs, floor, k)
		if paren < 0 || (!masked && !hasKeyWord(rs, paren)) {
			continue
		}
		return keySep{at: k, open: j, masked: masked}
	}
	return keySep{at: -1}
}

// maskKeyVariants replaces the values of every variant "Key (cols) = (values)"
// with Redacted, by maskKeyValues' rule: a tuple ends at the last keyEnds
// phrase before the next detail's column list, else just before that list,
// else at the end of the text.
func maskKeyVariants(s string) string {
	if !strings.ContainsAny(s, "=＝") {
		return s
	}
	rs := []rune(s)
	var out []rune
	last := 0
	for sep := nextVariantSep(rs, 0, -1); sep.at >= 0; {
		if sep.masked {
			end := sep.open + len(redactedRunes)
			out = append(out, rs[last:end]...)
			last = end
			sep = nextVariantSep(rs, end, end-1)
			continue
		}
		out = append(append(out, rs[last:sep.open]...), redactedRunes...)
		next := nextVariantSep(rs, sep.open+1, sep.open)
		window := len(rs)
		if next.at >= 0 {
			window = columnListStart(rs, sep.open, next.at)
		}
		last = window
		if end := lastKeyEndWide(rs[:window], sep.open+1); end >= 0 {
			last = end + 1
		} else if next.at >= 0 && window > 0 && unicode.IsSpace(rs[window-1]) {
			out = append(out, ' ')
		}
		sep = next
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

// columnListStart is where the column list closed at at starts: its '(' and
// the "Key " before it, if any.
func columnListStart(rs []rune, floor, at int) int {
	p := columnParen(rs, floor, at)
	if p < 0 {
		return at
	}
	if p-len(keyWord) > floor && hasKeyWord(rs, p) {
		return p - len(keyWord)
	}
	return p
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
// next such detail or the end of the text; without a ')' (a cut), the mask
// runs to there.
func maskSQLServerKey(s string) string {
	if !strings.Contains(strings.ToLower(s), string(sqlServerKeyPhrase)) {
		return s
	}
	rs := []rune(s)
	var out []rune
	last := 0
	for i := 0; i < len(rs); i++ {
		if !wordStart(rs, i) || !hasPrefixFold(rs[i:], sqlServerKeyPhrase) {
			continue
		}
		j := skipSpace(rs, i+len(sqlServerKeyPhrase))
		if j >= len(rs) || (rs[j] != '(' && rs[j] != '（') {
			continue
		}
		window := len(rs)
		for k := j + 1; k < len(rs); k++ {
			if wordStart(rs, k) && (hasPrefixFold(rs[k:], sqlServerKeyPhrase) || hasPrefixFold(rs[k:], sqlServerEndPhrase)) {
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
// 'jdoe_smith'). MySQL does not escape the value, so it ends at the last
// "' for key" before the next such detail; without one (a cut), the mask runs
// to the next detail or the end of the text. The key name stays.
func maskMySQLEntry(s string) string {
	if !strings.Contains(strings.ToLower(s), string(mySQLEntryPhrase)) {
		return s
	}
	rs := []rune(s)
	var out []rune
	last := 0
	for i := 0; i < len(rs); i++ {
		if !wordStart(rs, i) || !hasPrefixFold(rs[i:], mySQLEntryPhrase) {
			continue
		}
		j := skipSpace(rs, i+len(mySQLEntryPhrase))
		if j >= len(rs) || rs[j] != '\'' {
			continue
		}
		window := len(rs)
		for k := j + 1; k < len(rs); k++ {
			if wordStart(rs, k) && hasPrefixFold(rs[k:], mySQLEntryPhrase) {
				window = k
				break
			}
		}
		end := window
		for k := window - len(mySQLForKey); k > j; k-- {
			if hasPrefixFold(rs[k:], mySQLForKey) {
				end = k + 1 // the closing quote
				break
			}
		}
		out = append(append(out, rs[last:j]...), redactedRunes...)
		if end == window && window < len(rs) && unicode.IsSpace(rs[window-1]) {
			out = append(out, ' ')
		}
		last = end
		i = end - 1
	}
	return string(append(out, rs[last:]...))
}

// mergeSplitKeyValues joins a masked key value with a ")=<redacted>" that
// follows it when the text between them holds no '(' for that ')': there is no
// column list there, so the text was part of the value. "Key (a)=(x) already
// exists y)=(z) already exists." reads in maskKeyValues as two details and
// keeps "y"; here it becomes "Key (a)=<redacted> already exists.". Text that
// holds the word "key" may name a column list printed without its '('
// ("Key Cq2)=(...)"), so it stays as main shows it.
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
		if at := sepBefore(rs, m2); at >= end1 && sepBefore(rs, m1) >= 0 && columnParen(rs, end1-1, at) < 0 && !hasKeyWordIn(rs[end1:at]) {
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
