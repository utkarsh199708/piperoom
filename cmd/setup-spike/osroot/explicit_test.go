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
	"strings"
	"testing"

	"golang.org/x/text/unicode/norm"
)

// lookAlikes are NFKC-normal runes that read as, or that an ANSI best-fit
// code page maps to, "/", "\", ":" or "." (the WorstFit class). Fullwidth
// forms such as U+FF0F need no entry: NFKC folds them, so the NFKC rule
// refuses them.
const lookAlikes = "∕⁄⧸∖⧵⧹¥₩∶ː"

// explicitRefusal returns why name is refused before any open, or "".
func explicitRefusal(name string) string {
	switch {
	case name == "":
		return "empty"
	case strings.ContainsFunc(name, func(r rune) bool { return r < 0x20 || r == 0x7f }):
		return "control character"
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
		// Unicode: the dotless ı upper-cases to I (EqualFold misses it).
		{".gıt/config", "denied directory"},
		{".GıT/config", "denied directory"},
		{".pıperoom/secrets.json", "denied directory"},
		// NFKC folds these to ".", "/", "\" or ":".
		{"．git/config", "not NFKC-normal"},      // fullwidth full stop
		{"a／b", "not NFKC-normal"},              // fullwidth solidus
		{"a＼b", "not NFKC-normal"},              // fullwidth reverse solidus
		{"a.yaml：x", "not NFKC-normal"},         // fullwidth colon
		{"﹒git/config", "not NFKC-normal"},      // small full stop
		{"Kafka.properties", "not NFKC-normal"}, // Kelvin sign
		{"coﬁg.yaml", "not NFKC-normal"},        // fi ligature
		// NFKC-normal look-alikes and ANSI best-fit targets (WorstFit).
		{"a∕b", "slash, colon or dot look-alike"}, // division slash
		{"a⁄b", "slash, colon or dot look-alike"}, // fraction slash
		{"a¥b", "slash, colon or dot look-alike"}, // yen sign: "\" in code page 932
		{"a₩b", "slash, colon or dot look-alike"}, // won sign: "\" in code page 949
		{"a∶b", "slash, colon or dot look-alike"}, // ratio
		{"café.yaml", ""},                         // NFC-composed, NFKC-normal: allowed
	}
	for _, c := range cases {
		if got := explicitRefusal(c.name); got != c.want {
			t.Errorf("explicitRefusal(%q) = %q, want %q", c.name, got, c.want)
		}
	}
}
