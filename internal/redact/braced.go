package redact

import "strings"

// bracedWords end a key whose braced value is a secret.
var bracedWords = []string{"password", "passwd", "pwd", "secret", "passphrase"}

// bracedSpans finds, in the original text, an MS SQL Server ODBC, ADO.NET
// or JDBC value in braces after a secret key, password={Br@ce;Pw42x}, where
// the braces let the value hold ';', '@' and blanks, and '}}' is an escaped
// '}' (#534). A value left open runs to the end of the line. The span is
// the value with its braces. It reads what the local bracedPassword of
// internal/tools/kafkatools (#430) reads, with blanks around '=' kept on one
// line. The rules before it take password={Br@ as the value and stop at ';'
// or '@', and in a JDBC URL the URL rule may eat the key, so the value is
// found before them and masked whole by project.
func bracedSpans(s string) []span {
	var out []span
	for i := 0; i < len(s); {
		k := strings.IndexByte(s[i:], '{')
		if k < 0 {
			break
		}
		open := i + k
		i = open + 1
		if !bracedKeyBefore(s, open) {
			continue
		}
		e := open + 1
		closed := false
		for e < len(s) && s[e] != '\n' {
			if s[e] == '}' {
				if e+1 < len(s) && s[e+1] == '}' {
					e += 2
					continue
				}
				closed = true
				break
			}
			e++
		}
		body := s[open+1 : e]
		if closed {
			e++
		} else {
			body = strings.TrimRight(body, "\r")
			e = open + 1 + len(body)
		}
		i = max(i, e)
		if strings.TrimSpace(body) == "" || heldMask(body, true) {
			continue
		}
		out = append(out, span{open, e})
	}
	return out
}

// bracedKeyBefore reports whether the '{' at open follows key=, blanks
// allowed around '=', where the key's word characters end in a bracedWord.
func bracedKeyBefore(s string, open int) bool {
	j := open
	for j > 0 && (s[j-1] == ' ' || s[j-1] == '\t') {
		j--
	}
	if j == 0 || s[j-1] != '=' {
		return false
	}
	j--
	for j > 0 && (s[j-1] == ' ' || s[j-1] == '\t') {
		j--
	}
	ke := j
	for j > 0 && j > ke-64 && isWordChar(s[j-1]) {
		j--
	}
	key := asciiLower(s[j:ke])
	for _, w := range bracedWords {
		if strings.HasSuffix(key, w) {
			return true
		}
	}
	return false
}

func isWordChar(c byte) bool { return isAlnum(c) || c == '_' }
