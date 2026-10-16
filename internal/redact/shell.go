package redact

import (
	"regexp"
	"strings"
)

// shellSpans finds, in the original text, the secret arguments of
// command-line flags written in shell forms the flag rules do not read
// (#511): ANSI-C quoting ($'Zx9 hunter2'), a backslash-escaped blank in a
// bare word (Zx9\ hunter2), quote runs glued into one argument
// ('Zx9'\”hunter2', "Zx9"'hunter2') and typographic quotes from chat
// (“Zx9 hunter2”). Arguments in the plain forms are left to those rules.
// It also finds the rest of the line after Windows "set NAME=" with a
// secret NAME, which cmd reads up to the end of the line.
func shellSpans(s string) []span {
	var out []span
	for start := 0; start <= len(s); {
		end := strings.IndexByte(s[start:], '\n')
		if end < 0 {
			end = len(s)
		} else {
			end += start
		}
		line := s[start:end]
		lower := asciiLower(line)
		if containsAny("pass", "secret", "token")(line, lower) {
			for _, sp := range shellFlagSpans(line) {
				out = append(out, span{start + sp.a, start + sp.b})
			}
			if sp, ok := winSetSpan(line); ok {
				out = append(out, span{start + sp.a, start + sp.b})
			}
		}
		start = end + 1
	}
	return out
}

// shellFlagSpans finds the shell-form arguments of secret flags in one
// line. Each step moves past the argument it read, so a line costs time
// linear in its length.
func shellFlagSpans(s string) []span {
	var out []span
	for i := 0; i < len(s); {
		j := strings.IndexByte(s[i:], '-')
		if j < 0 {
			break
		}
		p := i + j
		i = p + 1
		if !flagBefore(s, p) {
			continue
		}
		ns := p + 1
		for ns < len(s) && s[ns] == '-' {
			ns++
		}
		ne := ns
		for ne < len(s) && isKeyChar(s[ne]) {
			ne++
		}
		i = max(i, ne)
		if ne == ns || !isLetter(s[ns]) {
			continue
		}
		lname := asciiLower(s[ns:ne])
		if strings.HasPrefix(lname, "no-") || strings.HasPrefix(lname, "no_") ||
			strings.TrimRight(lname, "._-") != lname || !endsInFlagWord(lname) {
			continue
		}
		v, listForm := flagSep(s, p, ne)
		if v < 0 || listForm {
			continue
		}
		w := readShellWord(s, v)
		i = max(i, w.end)
		if !w.special || w.end <= v {
			continue
		}
		val := w.val.String()
		if strings.TrimSpace(val) == "" || heldMask(val, true) || isPlaceholder(val) ||
			!w.quoted && (plainWord(val) || isReference(val, false)) {
			continue
		}
		out = append(out, span{v, w.end})
	}
	return out
}

// shellWord is one shell argument: where it ends, its text with quotes and
// escapes removed, whether any part was quoted, and whether it uses a form
// the flag rules do not read.
type shellWord struct {
	end             int
	val             strings.Builder
	quoted, special bool
}

// Typographic quotes, as chat clients rewrite straight ones.
const (
	ldquo = "“"
	rdquo = "”"
	lsquo = "‘"
	rsquo = "’"
)

// readShellWord reads the shell argument at v up to an unescaped blank. A
// quote left open runs to the end of the line (a truncated summary). Quote
// runs count as glued when two closed ones make up the word, or a closed
// one follows bare text directly (Zx9'hunter2'); a quote that is never
// closed, or punctuation after a closing quote ("X".), does not glue.
func readShellWord(s string, v int) *shellWord {
	w := &shellWord{}
	k := v
	// closeAt reads a quoted run from k up to the closer; esc allows
	// backslash escapes inside it. It reports whether the closer came.
	closeAt := func(closer string, esc bool) bool {
		for k < len(s) && s[k] != '\r' {
			switch {
			case esc && s[k] == '\\' && k+1 < len(s):
				w.val.WriteByte(s[k+1])
				k += 2
				continue
			case strings.HasPrefix(s[k:], closer):
				k += len(closer)
				return true
			}
			w.val.WriteByte(s[k])
			k++
		}
		return false
	}
	// bare is set while the last run read is bare text.
	bare, closedRuns := false, 0
	quote := func(closer string, esc, special bool) {
		w.quoted = true
		w.special = w.special || special
		if closeAt(closer, esc) {
			closedRuns++
			w.special = w.special || bare || closedRuns > 1
		}
		bare = false
	}
	for k < len(s) {
		c := s[k]
		switch {
		case c == ' ' || c == '\t' || c == '\r':
			w.end = k
			return w
		case c == '$' && k+1 < len(s) && s[k+1] == '\'':
			k += 2
			quote("'", true, true)
		case c == '\'':
			k++
			quote("'", false, false)
		case c == '"':
			k++
			quote(`"`, true, false)
		case strings.HasPrefix(s[k:], ldquo):
			k += len(ldquo)
			quote(rdquo, false, true)
		case strings.HasPrefix(s[k:], lsquo):
			k += len(lsquo)
			quote(rsquo, false, true)
		case c == '\\' && k+1 < len(s) && s[k+1] != '\r':
			if s[k+1] == ' ' || s[k+1] == '\t' {
				w.special = true
			}
			w.val.WriteByte(s[k+1])
			k += 2
			bare = true
		default:
			w.val.WriteByte(c)
			k++
			bare = true
		}
	}
	w.end = k
	return w
}

// winSet matches cmd's "set NAME=value" (or set "NAME=value") at the start
// of a line.
var winSet = regexp.MustCompile(`(?i)^[ \t]*set[ \t]+("?)([A-Za-z_][A-Za-z0-9_.]*)=`)

// winSetSpan returns the value of a Windows set command whose name ends in
// a secret word: the rest of the line, or up to the closing quote of the
// quoted form. A value without a blank is left to the key/value rule.
func winSetSpan(line string) (span, bool) {
	m := winSet.FindStringSubmatchIndex(line)
	if m == nil || !endsInFlagWord(asciiLower(line[m[4]:m[5]])) {
		return span{}, false
	}
	vs, ve := m[1], len(line)
	if m[3] > m[2] {
		if q := strings.LastIndexByte(line, '"'); q >= vs {
			ve = q
		}
	}
	ve = vs + len(strings.TrimRight(line[vs:ve], " \t\r"))
	val := line[vs:ve]
	if !strings.ContainsAny(val, " \t") || heldMask(val, true) || isPlaceholder(val) {
		return span{}, false
	}
	return span{vs, ve}, true
}
