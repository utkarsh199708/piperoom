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

// maxXMLAttrs bounds how many attributes of one start tag are read.
const maxXMLAttrs = 16

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
		out = append(out, ms...)
		i = max(i, end)
	}
	return out
}

// namedSecret reports whether a key= or name= attribute ends in a secret
// word.
func namedSecret(s string, attrs []xmlAttr) bool {
	for _, n := range []string{"key", "name"} {
		if a, ok := attrNamed(attrs, n); ok && endsInFlagWord(asciiLower(s[a.vs:a.ve])) {
			return true
		}
	}
	return false
}

func attrNamed(attrs []xmlAttr, name string) (xmlAttr, bool) {
	for _, a := range attrs {
		if strings.EqualFold(localName(a.name), name) {
			return a, true
		}
	}
	return xmlAttr{}, false
}

// readAttrs reads the quoted attributes of a start tag whose name ends at
// i, up to its '>'. It returns them, the index after the '>' (or where the
// scan stopped), whether the tag closes itself, and whether the tag was
// read whole. An attribute value may not hold '<', so the scan stops there.
func readAttrs(s string, i int, attrs []xmlAttr) ([]xmlAttr, int, bool, bool) {
	for range maxXMLAttrs + 1 {
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
		if i >= len(s) || s[i] != '"' && s[i] != '\'' {
			return attrs, i, false, false
		}
		q := s[i]
		vs := i + 1
		ve := vs
		for ve < len(s) && s[ve] != q && s[ve] != '<' {
			ve++
		}
		if ve >= len(s) || s[ve] != q {
			return attrs, ve, false, false
		}
		attrs = append(attrs, xmlAttr{name: name, vs: vs, ve: ve})
		i = ve + 1
	}
	return attrs, i, false, false
}

// openTagCut moves a scan cut back before a secret-named XML element whose
// start tag is in the last 4 KiB before the cut and whose end tag is not
// (#510): its text on the next lines would otherwise pass in the head
// without the end tag that lets addXMLRule read it. The cut goes to the
// start of the start tag's line, so every line left in the head is whole,
// or to the tag itself when that line starts before the window. The text
// it drops from the head lands in the overflow mask.
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
