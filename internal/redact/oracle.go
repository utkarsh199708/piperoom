package redact

import (
	"regexp"
	"strings"
)

// oracleCreds matches an Oracle JDBC URL with credentials,
// jdbc:oracle:<driver>:user/password@host..., which DriverManager quotes
// whole in "No suitable driver" and listener errors (#449). The user may be
// quoted, and a quote may be escaped (\", a trace echoing JSON config) or
// doubled inside ("", SQL style). Group 1 is a quoted password, group 2 the
// rest of the token, where oracleFind looks for the password. A URL with no
// credentials (jdbc:oracle:thin:@host...) does not match: its user part
// would start with '@'.
var oracleCreds = regexp.MustCompile(`(?i)jdbc:oracle:[a-z0-9]+:(?:\\?"(?:[^"\n]|"")*"|[^\s/@:;"'\\]+)/(?:(\\?"(?:[^"\n]|"")*")|(\S+))`)

// oracleRule masks the password of an Oracle JDBC URL. It runs after every
// other rule (scan), on their output, and can only add masks: its password
// runs to the last '@' of a whitespace-free token, since the password may
// itself hold '@' and '/', and in compact JSON or URL parameters that token
// reaches a sibling credential ("connection.password":"Sec@ret..."). Run
// first, it would eat that credential's key and leave its tail; run last,
// the sibling is already masked. Glued text up to a later '@' is masked with
// the password (an over-mask, never a leak). The mask keeps no length class.
var oracleRule = rule{
	kind:   KindURLPass,
	groups: []int{1},
	style:  styleBare,
	pre:    func(_, lower string) bool { return strings.Contains(lower, "jdbc:oracle:") },
	find:   oracleFind,
}

// oracleFind returns oracleCreds matches with group 1 set to the password:
// a quoted password whole; else the bare token up to its last '@', or the
// whole token when no host follows. A token that starts with a mask was
// masked by an earlier pass, and the cut must not move.
func oracleFind(s, _ string) [][]int {
	var out [][]int
	for _, m := range oracleCreds.FindAllStringSubmatchIndex(s, -1) {
		if m[2] >= 0 {
			out = append(out, []int{m[0], m[1], m[2], m[3]})
			continue
		}
		tok := s[m[4]:m[5]]
		if i := strings.Index(tok, marker); i >= 0 {
			tok = tok[:i]
		}
		end := len(tok)
		if at := strings.LastIndexByte(tok, '@'); at >= 0 {
			end = at
		}
		if end == 0 {
			continue // empty password or already masked
		}
		out = append(out, []int{m[0], m[1], m[4], m[4] + end})
	}
	return out
}

// maskOracle runs oracleRule over s.
func maskOracle(s string, st *Stats) string {
	return applyCounted(&oracleRule, s, asciiLower(s), st)
}
