package redact

import "strings"

// addXMLAttrRule masks a secret named by an attribute rather than by the
// element (#510): Java Properties XML <entry key="jdbc.password">X</entry>,
// .NET <add key="DbPassword" value="X"/>, Hibernate and Spring <property
// name="...password" value="X"/>, Tomcat <Parameter name="password"
// value="X"/>. When a key= or name= attribute ends in a secret word, the
// value= attribute is the secret, or else the element's own text, read as
// xmlContent reads a secret-named element. It runs after every other rule,
// on its output, and only adds masks.
var addXMLAttrRule = rule{
	kind:   KindKeyValue,
	find:   xmlAttrFind,
	groups: []int{1},
	style:  styleSecret,
	pre:    func(s, _ string) bool { return strings.Contains(s, "<") && strings.Contains(s, "=") },
	remask: true,
}

// xmlAttr is one attribute of a start tag: its name and the bounds of its
// value inside the quotes.
type xmlAttr struct {
	name   string
	vs, ve int
}

// xmlAttrFind finds the secrets that attributes name. Each start tag is
// read once and the search moves past it; the text scan stops at the next
// '<', as in xmlFind, so hostile text costs time linear in its length.
func xmlAttrFind(s, _ string) [][]int {
	var out [][]int
	cdata := cdataEnds{s: s, next: -1}
	var attrs []xmlAttr
	for i := 0; i < len(s); {
		j := strings.IndexByte(s[i:], '<')
		if j < 0 {
			break
		}
		p := i + j
		ns := p + 1
		ne := xmlName(s, ns)
		i = max(ne, p+1)
		if ne == ns || !(isLetter(s[ns]) || s[ns] == '_') {
			continue
		}
		var k int
		var selfClosing, ok bool
		attrs, k, selfClosing, ok = readAttrs(s, ne, attrs[:0])
		i = max(i, k)
		if !ok || !namedSecret(s, attrs) {
			continue
		}
		if a, found := attrNamed(attrs, "value"); found {
			if a.ve > a.vs && !heldMask(s[a.vs:a.ve], true) {
				out = append(out, []int{a.vs, a.ve, a.vs, a.ve})
			}
			continue
		}
		if selfClosing {
			continue
		}
		ms, end := xmlContent(s, k, s[ns:ne], &cdata)
		schema := schemaElements[asciiLower(localName(s[ns:ne]))] && !keyNamed(s, attrs)
		for _, m := range ms {
			// <column name="password">VARCHAR</column> holds the column's
			// type, not its value.
			if !schema || !typeLike(s[m[2]:m[3]]) {
				out = append(out, m)
			}
		}
		i = max(i, end)
	}
	return out
}

// schemaElements name a column or field with name=: their text may be the
// column's type rather than a value.
var schemaElements = map[string]bool{
	"column": true, "col": true, "field": true, "attribute": true, "attr": true, "element": true, "member": true,
}

// namedSecret reports whether a key= or name= attribute ends in a secret
// word, or in pwd (.NET appSettings keys such as DbPwd).
func namedSecret(s string, attrs []xmlAttr) bool {
	for _, n := range []string{"key", "name"} {
		if a, ok := attrNamed(attrs, n); ok && secretAttrValue(s[a.vs:a.ve]) {
			return true
		}
	}
	return false
}

// keyNamed reports whether a key= attribute ends in a secret word.
func keyNamed(s string, attrs []xmlAttr) bool {
	a, ok := attrNamed(attrs, "key")
	return ok && secretAttrValue(s[a.vs:a.ve])
}

func secretAttrValue(v string) bool {
	l := asciiLower(v)
	return endsInFlagWord(l) || strings.HasSuffix(l, "pwd")
}

// columnTypes are SQL and schema type names that a schema element's text
// may hold.
var columnTypes = map[string]bool{
	"varchar": true, "nvarchar": true, "char": true, "nchar": true, "character": true, "text": true, "clob": true,
	"blob": true, "int": true, "integer": true, "bigint": true, "smallint": true, "tinyint": true, "boolean": true,
	"bool": true, "bit": true, "date": true, "time": true, "timestamp": true, "timestamptz": true, "datetime": true,
	"uuid": true, "bytea": true, "binary": true, "varbinary": true, "numeric": true, "decimal": true, "float": true,
	"double": true, "real": true, "json": true, "jsonb": true, "string": true, "number": true, "long": true,
	"short": true, "byte": true,
}

// knownErrors are JVM error classes a schema element may name.
var knownErrors = map[string]bool{
	"OutOfMemoryError": true, "StackOverflowError": true, "NoClassDefFoundError": true, "AssertionError": true,
	"LinkageError": true, "InternalError": true, "UnsatisfiedLinkError": true, "ExceptionInInitializerError": true,
}

// typeLike reports text that is, as a whole, a column type or an exception
// class rather than a value:
//   - a type word, in capitals when it stands alone (VARCHAR, TEXT; a lone
//     "real" or "text" is a value), then an optional (n) or (n,m), then
//     only constraints: NOT NULL, NULL, PRIMARY KEY, UNIQUE, DEFAULT and a
//     literal (varchar(255) NOT NULL);
//   - one Java or .NET class name ending in Exception, maybe dotted, with
//     letters only in its last part (SerializationException), or a known
//     JVM error (OutOfMemoryError).
//
// Anything else in a schema element named by a secret word is masked.
func typeLike(t string) bool {
	t = strings.TrimSpace(t)
	if exceptionName(t) || knownErrors[t[strings.LastIndexByte(t, '.')+1:]] && !strings.ContainsAny(t, " \t") {
		return true
	}
	w := t
	rest := ""
	if i := strings.IndexAny(t, " \t("); i >= 0 {
		w, rest = t[:i], t[i:]
	}
	if !columnTypes[asciiLower(w)] {
		return false
	}
	rest = strings.TrimLeft(rest, " \t")
	if strings.HasPrefix(rest, "(") {
		e := strings.IndexByte(rest, ')')
		if e < 0 || !sizeArgs(rest[1:e]) {
			return false
		}
		rest = rest[e+1:]
	} else if rest == "" && w != strings.ToUpper(w) {
		return false
	}
	return constraintsOnly(strings.Fields(rest))
}

// sizeArgs reports "n" or "n,m" with digits and optional blanks.
func sizeArgs(a string) bool {
	parts := strings.Split(a, ",")
	if len(parts) > 2 {
		return false
	}
	for _, p := range parts {
		if p = strings.TrimSpace(p); p == "" || !allDigits(p) {
			return false
		}
	}
	return true
}

// constraintsOnly reports words that are only SQL column constraints.
func constraintsOnly(ws []string) bool {
	for i := 0; i < len(ws); i++ {
		switch asciiLower(ws[i]) {
		case "null", "unique":
		case "not":
			if i+1 >= len(ws) || asciiLower(ws[i+1]) != "null" {
				return false
			}
			i++
		case "primary":
			if i+1 >= len(ws) || asciiLower(ws[i+1]) != "key" {
				return false
			}
			i++
		case "default":
			if i+1 >= len(ws) || !sqlLiteral(ws[i+1]) {
				return false
			}
			i++
		default:
			return false
		}
	}
	return true
}

// sqlLiteral reports a DEFAULT literal: digits, an empty or short quoted
// string, true, false, null or current_timestamp.
func sqlLiteral(v string) bool {
	switch asciiLower(v) {
	case "true", "false", "null", "current_timestamp", "now()":
		return true
	}
	if allDigits(strings.TrimPrefix(v, "-")) && v != "-" {
		return true
	}
	return len(v) >= 2 && len(v) <= 16 && v[0] == '\'' && v[len(v)-1] == '\''
}

// exceptionName reports one class name ending in Exception, maybe dotted
// (org.apache.kafka.common.errors.SerializationException), whose last part
// is letters only and starts with a capital.
func exceptionName(t string) bool {
	last := t[strings.LastIndexByte(t, '.')+1:]
	if !strings.HasSuffix(last, "Exception") || last[0] < 'A' || last[0] > 'Z' {
		return false
	}
	for i := 0; i < len(last); i++ {
		if !isLetter(last[i]) {
			return false
		}
	}
	for _, part := range strings.Split(t, ".") {
		if part == "" {
			return false
		}
		for i := 0; i < len(part); i++ {
			if !isKeyChar(part[i]) || part[i] == '-' || part[i] == '.' {
				return false
			}
		}
	}
	return true
}

func attrNamed(attrs []xmlAttr, name string) (xmlAttr, bool) {
	for _, a := range attrs {
		if strings.EqualFold(localName(a.name), name) {
			return a, true
		}
	}
	return xmlAttr{}, false
}

// readAttrs reads the attributes of a start tag whose name ends at i, up to
// its '>', and keeps those named key, name or value. It returns them, the
// index after the '>' (or where the scan stopped), whether the tag closes
// itself, and whether the tag was read whole. A value may be quoted or bare
// (key=DbPassword); none may hold '<', so the scan stops there. Each step
// moves forward, so a tag costs time linear in its length.
func readAttrs(s string, i int, attrs []xmlAttr) ([]xmlAttr, int, bool, bool) {
	for {
		i = skipXMLSpace(s, i)
		switch {
		case i >= len(s):
			return attrs, i, false, false
		case s[i] == '>':
			return attrs, i + 1, false, true
		case strings.HasPrefix(s[i:], "/>"):
			return attrs, i + 2, true, true
		}
		ns := i
		i = xmlName(s, i)
		if i == ns {
			return attrs, i, false, false
		}
		name := s[ns:i]
		i = skipXMLSpace(s, i)
		if i >= len(s) || s[i] != '=' {
			return attrs, i, false, false
		}
		i = skipXMLSpace(s, i+1)
		if i >= len(s) {
			return attrs, i, false, false
		}
		var vs, ve int
		if q := s[i]; q == '"' || q == '\'' {
			vs = i + 1
			ve = vs
			for ve < len(s) && s[ve] != q && s[ve] != '<' {
				ve++
			}
			if ve >= len(s) || s[ve] != q {
				return attrs, ve, false, false
			}
			i = ve + 1
		} else {
			vs = i
			ve = vs
			for ve < len(s) && !isXMLSpace(s[ve]) && s[ve] != '>' && s[ve] != '<' && s[ve] != '"' && s[ve] != '\'' &&
				!strings.HasPrefix(s[ve:], "/>") {
				ve++
			}
			if ve == vs {
				return attrs, ve, false, false
			}
			i = ve
		}
		switch asciiLower(localName(name)) {
		case "key", "name", "value":
			attrs = append(attrs, xmlAttr{name: name, vs: vs, ve: ve})
		}
	}
}

// openTagCut moves a scan cut back before a secret-named XML element whose
// start tag is in the last 4 KiB before the cut and whose end tag is not
// (#510): its text on the next lines would otherwise pass in the head
// without the end tag that lets addXMLRule read it. The cut goes to the
// start of the start tag's line, so every line left in the head is whole,
// or to the tag itself when that line starts before the window. The text
// it drops from the head lands in the overflow mask. Prose that names a
// secret element ("no <password> given") moves the cut too: an over-mask of
// at most 4 KiB, never a leak.
func openTagCut(s string, cut int) int {
	lo := max(0, cut-4096)
	win := s[lo:cut]
	type startTag struct {
		at   int
		name string
	}
	var starts []startTag
	// ends holds the last end tag of each element in the window, by
	// lower-case local name.
	ends := map[string]int{}
	var attrs []xmlAttr
	for i := 0; i < len(win); {
		j := strings.IndexByte(win[i:], '<')
		if j < 0 {
			break
		}
		p := i + j
		ns := p + 1
		if strings.HasPrefix(win[ns:], "/") || strings.HasPrefix(win[ns:], `\/`) {
			ns = strings.IndexByte(win[ns:], '/') + ns + 1
			ne := xmlName(win, ns)
			ends[localName(asciiLower(win[ns:ne]))] = p
			i = max(ne, p+1)
			continue
		}
		ne := xmlName(win, ns)
		i = max(ne, p+1)
		if ne == ns || !(isLetter(win[ns]) || win[ns] == '_') {
			continue
		}
		var k int
		var selfClosing, ok bool
		attrs, k, selfClosing, ok = readAttrs(win, ne, attrs[:0])
		i = max(i, k)
		if ok && !selfClosing && (endsInFlagWord(asciiLower(win[ns:ne])) || namedSecret(win, attrs)) {
			starts = append(starts, startTag{p, localName(asciiLower(win[ns:ne]))})
		}
	}
	for _, st := range starts {
		if e, ok := ends[st.name]; ok && e > st.at {
			continue
		}
		if nl := strings.LastIndexByte(win[:st.at], '\n'); nl >= 0 {
			return lo + nl + 1
		}
		return lo + st.at
	}
	return cut
}
