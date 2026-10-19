// Package osroot is the U1-0 spike on file confinement (design §2.3): which
// path forms os.Root refuses by itself, and which need an explicit check in
// internal/setup/ops (U1-2). Test files only; delete after U1-2 lands.
//
// explicitRefusal is the reference for U1-2's lexical rule. It runs on every
// OS, because on Linux and macOS os.Root accepts "C:foo", "a.yaml:x" and
// "CON" as ordinary file names (root_other_test.go), and the design wants the
// same refusals on all three CI legs.
package osroot

import (
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// walkRefusal is the per-component walk U1-2 runs after explicitRefusal: for
// EVERY component of name (each directory on the way, then the leaf) it
// lstats the prefix and refuses a symlink or reparse point and anything that
// is the same file as one of denied (the root's .git and .piperoom, stat'ed
// once at start). Checking only the opened file or its parent would let
// "<alias of .git>/hooks/pre-commit" through. lstat is Root.Lstat and same
// is os.SameFile in production; the tests inject them.
func walkRefusal(name string, lstat func(string) (fs.FileInfo, error), denied []fs.FileInfo, same func(a, b fs.FileInfo) bool) string {
	parts := strings.FieldsFunc(name, func(c rune) bool { return c == '/' || c == '\\' })
	for i := range parts {
		fi, err := lstat(strings.Join(parts[:i+1], "/"))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return "" // the rest does not exist yet: a create, under checked parents
			}
			return "lstat: " + err.Error()
		}
		if fi.Mode()&(fs.ModeSymlink|fs.ModeIrregular) != 0 {
			return "symlink, junction or reparse point"
		}
		for _, d := range denied {
			if same(fi, d) {
				return "alias of a denied directory"
			}
		}
	}
	return ""
}

// fakeInfo is a FileInfo whose identity is id, compared by sameID: an alias
// of a directory (a bind mount, an HFS+ ignorable-character twin, an 8.3
// name) cannot be built portably without privileges, so the nested-alias
// test fakes the file system and the real-FS test below checks the wiring
// with os.SameFile.
type fakeInfo struct {
	id   int
	mode fs.FileMode
}

func (f fakeInfo) Name() string       { return "" }
func (f fakeInfo) Size() int64        { return 0 }
func (f fakeInfo) Mode() fs.FileMode  { return f.mode }
func (f fakeInfo) ModTime() time.Time { return time.Time{} }
func (f fakeInfo) IsDir() bool        { return f.mode.IsDir() }
func (f fakeInfo) Sys() any           { return f.id }

func sameID(a, b fs.FileInfo) bool { return a.Sys() == b.Sys() }

// A nested alias of .git or .piperoom is refused at whichever component it
// appears, not only as the leaf or its parent.
func TestWalkRefusesNestedAlias(t *testing.T) {
	git, pr := fakeInfo{id: 1, mode: fs.ModeDir}, fakeInfo{id: 2, mode: fs.ModeDir}
	files := map[string]fakeInfo{
		".git": git, ".piperoom": pr,
		"a": {id: 10, mode: fs.ModeDir}, "a/gitalias": git, "a/gitalias/hooks": {id: 11, mode: fs.ModeDir},
		"a/gitalias/hooks/pre-commit": {id: 12},
		"a/b":                         {id: 13, mode: fs.ModeDir}, "a/b/c.yaml": {id: 14},
		"a/b/sec": pr, "a/link": {id: 15, mode: fs.ModeSymlink}, "a/junction": {id: 16, mode: fs.ModeIrregular},
	}
	lstat := func(p string) (fs.FileInfo, error) {
		if fi, ok := files[p]; ok {
			return fi, nil
		}
		return nil, fs.ErrNotExist
	}
	denied := []fs.FileInfo{git, pr}
	cases := []struct{ name, want string }{
		{"a/b/c.yaml", ""},
		{"a/b/new.yaml", ""},
		{`a\b\c.yaml`, ""},
		{"a/gitalias/hooks/pre-commit", "alias of a denied directory"},
		{"a/gitalias/hooks/new-hook", "alias of a denied directory"},
		{"a/gitalias", "alias of a denied directory"},
		{"a/b/sec/secrets.json", "alias of a denied directory"},
		{"a/link/x", "symlink, junction or reparse point"},
		{"a/junction/x", "symlink, junction or reparse point"},
	}
	for _, c := range cases {
		if got := walkRefusal(c.name, lstat, denied, sameID); got != c.want {
			t.Errorf("walkRefusal(%q) = %q, want %q", c.name, got, c.want)
		}
	}
}

// The same walk on a real directory through os.Root and os.SameFile: a
// directory registered as denied is refused as a middle component.
func TestWalkRealFS(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{".git", "sub/deep"} {
		if err := os.MkdirAll(dir+"/"+d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	r, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	git, err := r.Lstat(".git")
	if err != nil {
		t.Fatal(err)
	}
	if got := walkRefusal("sub/deep/x.yaml", r.Lstat, []fs.FileInfo{git}, os.SameFile); got != "" {
		t.Errorf("clean path refused: %q", got)
	}
	sub, err := r.Lstat("sub")
	if err != nil {
		t.Fatal(err)
	}
	if got := walkRefusal("sub/deep/x.yaml", r.Lstat, []fs.FileInfo{git, sub}, os.SameFile); got != "alias of a denied directory" {
		t.Errorf("denied middle component: %q", got)
	}
}

// lookAlikes are NFKC-normal runes that read as, or that an ANSI best-fit
// code page maps to, "/", "\", ":" or "." (the WorstFit class). Fullwidth
// forms such as U+FF0F need no entry: NFKC folds them, so the NFKC rule
// refuses them. The table is defence in depth only: no op passes a name to
// argv or an ANSI API (design §2.3), and the list cannot be complete.
const lookAlikes = "\u2215\u2044\u29f8\u2216\u29f5\u29f9\u00a5\u20a9\u2236\u02d0"

// formatChar is a Unicode format character (category Cf: zero-width space,
// joiner and non-joiner, BOM, soft hyphen, bidi controls, word joiner) or the
// combining grapheme joiner U+034F (category Mn). macOS HFS+ ignores these
// when it compares names, so ".g\u200cit" is ".git" there (git
// CVE-2014-9390).
func formatChar(r rune) bool { return unicode.Is(unicode.Cf, r) || r == 0x034f }

// explicitRefusal returns why name is refused before any open, or "".
func explicitRefusal(name string) string {
	switch {
	case name == "":
		return "empty"
	case strings.ContainsFunc(name, func(r rune) bool { return r < 0x20 || r == 0x7f }):
		return "control character"
	case strings.ContainsFunc(name, formatChar):
		return "format character"
	case strings.ContainsAny(name, "?*<>\"|"):
		return "wildcard"
	case strings.ContainsAny(name, lookAlikes):
		return "slash, colon or dot look-alike"
	case !norm.NFKC.IsNormalString(name):
		return "not NFKC-normal" // fullwidth ．／＼：, ligatures, the Kelvin sign, ...
	case name[0] == '/' || name[0] == '\\':
		return "absolute, rooted or UNC" // \x, \\host\share, \\?\C:\, \\.\NUL, //host
	case strings.ContainsRune(name, ':'):
		return "drive letter or alternate data stream" // C:foo, C:\x, a.yaml:x, a::$DATA
	}
	for _, part := range strings.FieldsFunc(name, func(r rune) bool { return r == '/' || r == '\\' }) {
		switch {
		case part == "..":
			return "dot-dot"
		case part != "." && (strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ")):
			return "trailing dot or space" // Win32 strips them: ".git." is ".git"
		case reservedDevice(part):
			return "reserved device name"
		case shortNameAlias(part):
			return "8.3 short-name alias" // PIPERO~1 is .piperoom on a volume with 8.3 names
		case deniedDir(part):
			return "denied directory"
		}
	}
	return ""
}

// deniedDir compares by simple fold and by upper case: strings.EqualFold
// misses the dotless ı (U+0131), whose upper case is "I", so ".gıt" must be
// caught by strings.ToUpper; NTFS's upcase table may fold it the same way.
// Trailing dots and spaces are refused before this runs.
func deniedDir(part string) bool {
	up := strings.ToUpper(part)
	return strings.EqualFold(part, ".git") || strings.EqualFold(part, ".piperoom") ||
		up == ".GIT" || up == ".PIPEROOM"
}

// reservedDevice matches the device names with any extension, as Windows 10
// does; Windows 11 frees "CON.txt", so os.Root alone differs by OS version.
func reservedDevice(part string) bool {
	base := part
	if i := strings.IndexByte(base, '.'); i >= 0 {
		base = base[:i]
	}
	base = strings.ToUpper(strings.TrimRight(base, " "))
	switch base {
	case "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$":
		return true
	}
	if len(base) >= 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) {
		switch base[3:] {
		case "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "\u00b9", "\u00b2", "\u00b3":
			return true
		}
	}
	return false
}

func shortNameAlias(part string) bool {
	i := strings.IndexByte(part, '~')
	return i >= 0 && i+1 < len(part) && part[i+1] >= '0' && part[i+1] <= '9'
}

// The design §2.3 rows, every OS. "" means allowed.
func TestExplicitRefusal(t *testing.T) {
	cases := []struct{ name, want string }{
		{"a.yaml", ""},
		{"connect/worker.properties", ""},
		{`connect\worker.properties`, ""},
		{"./a.yaml", ""},
		{"plugins/debezium-2.7/x.jar", ""},
		{"", "empty"},
		{"..", "dot-dot"},
		{"a/../../x", "dot-dot"},
		{`a\..\a.yaml`, "dot-dot"},
		{"/etc/passwd", "absolute, rooted or UNC"},
		{`\Windows\win.ini`, "absolute, rooted or UNC"},
		{`C:foo`, "drive letter or alternate data stream"},
		{`C:\Windows\win.ini`, "drive letter or alternate data stream"},
		{`\\host\share\x`, "absolute, rooted or UNC"},
		{`//host/share/x`, "absolute, rooted or UNC"},
		{`\\?\C:\Windows\win.ini`, "wildcard"},
		{`\\.\NUL`, "absolute, rooted or UNC"},
		{"a.yaml:x", "drive letter or alternate data stream"},
		{"a.yaml::$DATA", "drive letter or alternate data stream"},
		{"CON", "reserved device name"},
		{"nul", "reserved device name"},
		{"COM1", "reserved device name"},
		{"LPT9", "reserved device name"},
		{"COM\u00b9", "not NFKC-normal"}, // \u00b9 folds to 1; reservedDevice also catches it
		{"COM\u00b9.txt", "not NFKC-normal"},
		{"CONIN$", "reserved device name"},
		{"NUL ", "trailing dot or space"},
		{"CON.txt", "reserved device name"},
		{"sub/aux.yaml", "reserved device name"},
		{"console.txt", ""},
		{"COM10", ""},
		{".git/config", "denied directory"},
		{".GIT/config", "denied directory"},
		{`.git.\config`, "trailing dot or space"},
		{".git /config", "trailing dot or space"},
		{".piperoom/secrets.json", "denied directory"},
		{".Piperoom/ops.jsonl", "denied directory"},
		{`PIPERO~1\secrets.json`, "8.3 short-name alias"},
		{"GIT~1/config", "8.3 short-name alias"},
		{"a~b.txt", ""},
		{"a\x00b", "control character"},
		{"a\x01b", "control character"},
		{"a\tb.yaml", "control character"},
		{"a\nb", "control character"},
		{"a\x1fb", "control character"},
		{"a\x7fb", "control character"},
		{"*.yaml", "wildcard"},
		// Format characters inside ".git": HFS+ ignores them (CVE-2014-9390).
		{".g\u200bit/config", "format character"},            // zero-width space
		{".g\u200cit/config", "format character"},            // zero-width non-joiner
		{".g\u200dit/config", "format character"},            // zero-width joiner
		{".g\u202eit/config", "format character"},            // right-to-left override
		{"\ufeff.git/config", "format character"},            // BOM
		{".g\u00adit/config", "format character"},            // soft hyphen
		{".g\u2060it/config", "format character"},            // word joiner
		{".g\u034fit/config", "format character"},            // combining grapheme joiner (Mn)
		{".pipe\u200croom/secrets.json", "format character"}, // and inside ".piperoom"
		// Unicode: the dotless ı upper-cases to I (EqualFold misses it).
		{".g\u0131t/config", "denied directory"},
		{".G\u0131T/config", "denied directory"},
		{".p\u0131peroom/secrets.json", "denied directory"},
		// NFKC folds these to ".", "/", "\" or ":".
		{"\uff0egit/config", "not NFKC-normal"},      // fullwidth full stop
		{"a\uff0fb", "not NFKC-normal"},              // fullwidth solidus
		{"a\uff3cb", "not NFKC-normal"},              // fullwidth reverse solidus
		{"a.yaml\uff1ax", "not NFKC-normal"},         // fullwidth colon
		{"\ufe52git/config", "not NFKC-normal"},      // small full stop
		{"\u212aafka.properties", "not NFKC-normal"}, // Kelvin sign
		{"co\ufb01g.yaml", "not NFKC-normal"},        // fi ligature
		// NFKC-normal look-alikes and ANSI best-fit targets (WorstFit).
		{"a\u2215b", "slash, colon or dot look-alike"}, // division slash
		{"a\u2044b", "slash, colon or dot look-alike"}, // fraction slash
		{"a\u00a5b", "slash, colon or dot look-alike"}, // yen sign: "\" in code page 932
		{"a\u20a9b", "slash, colon or dot look-alike"}, // won sign: "\" in code page 949
		{"a\u2236b", "slash, colon or dot look-alike"}, // ratio
		{"caf\u00e9.yaml", ""},                         // NFC-composed, NFKC-normal: allowed
	}
	for _, c := range cases {
		if got := explicitRefusal(c.name); got != c.want {
			t.Errorf("explicitRefusal(%q) = %q, want %q", c.name, got, c.want)
		}
	}
}
