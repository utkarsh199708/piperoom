package ops

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// absPath turns dir into an absolute directory path and refuses a file or a
// missing directory: the project root must exist before any op runs.
func absPath(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("ops: resolving project dir %q: %w", clip(dir), err)
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return "", fmt.Errorf("ops: project dir %s: %w", abs, err)
	}
	if !fi.IsDir() {
		return "", fmt.Errorf("ops: project dir %s is not a directory", abs)
	}
	return abs, nil
}

// The file confinement rule (docs/design/setup-agent.md §2.3): every file op
// opens through an os.Root rooted at the project directory, and any path
// component that is a symlink, junction or other reparse point is refused.
// There is no check-then-open: os.Root resolves inside the root at open time,
// so a swapped symlink cannot escape it.
//
// os.Root alone is not enough (design §2.3, U1-0's spike in
// cmd/setup-spike/osroot): it accepts ".." that stays inside the root, follows
// a relative symlink that stays inside the root, and on non-Windows accepts
// every Windows form as an ordinary name. So every name is first checked
// lexically on all three OSes (lexicalRefusal, in the design's order), then
// every existing prefix of the path is Lstat'ed through the root and refused
// for a symlink or reparse point, and finally the opened handle is compared
// with os.SameFile against the root's .git and .piperoom, so a nested alias
// such as "<alias of .git>/hooks/pre-commit" is caught at the alias component.

// Confiner confines file access to one project directory.
type Confiner struct {
	dir  string
	root *os.Root
}

// OpenConfiner opens dir as the root every file op is confined to.
func OpenConfiner(dir string) (*Confiner, error) {
	abs, err := absPath(dir)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("ops: opening project root %s: %w", abs, err)
	}
	return &Confiner{dir: abs, root: root}, nil
}

// Dir is the absolute project directory.
func (c *Confiner) Dir() string { return c.dir }

// Close releases the root handle.
func (c *Confiner) Close() error {
	if c == nil || c.root == nil {
		return nil
	}
	return c.root.Close()
}

// CheckName runs the lexical §2.3 rule without I/O and returns an error
// wrapping ErrPath. Drivers that render a template use it before a write.
func CheckName(name string) error {
	if why := lexicalRefusal(name); why != "" {
		return fmt.Errorf("%w: %s: %s", ErrPath, why, clip(name))
	}
	return nil
}

// Check is the full path check for a caller outside this package: the lexical
// rule and the walk, returning the slash-separated name to pass to os.Root.
func (c *Confiner) Check(name string) (string, error) {
	clean, _, err := c.prepare(name)
	return clean, err
}

// lexicalRefusal returns why name is refused before any open, or "". It runs
// on every OS because os.Root accepts the Windows forms as plain names off
// Windows. The order is the design's §2.3 list.
func lexicalRefusal(name string) string {
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
		return "not NFKC-normal"
	case name[0] == '/' || name[0] == '\\':
		return "absolute, rooted or UNC"
	case strings.ContainsRune(name, ':'):
		return "drive letter or alternate data stream"
	}
	for _, part := range pathParts(name) {
		switch {
		case part == "..":
			return "dot-dot"
		case part != "." && (strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ")):
			return "trailing dot or space"
		case reservedDevice(part):
			return "reserved device name"
		case shortNameAlias(part):
			return "8.3 short-name alias"
		case deniedDir(part):
			return "denied directory"
		}
	}
	return ""
}

// formatChar is a Unicode format character (category Cf: zero-width space,
// joiner and non-joiner, BOM, soft hyphen, bidi controls, word joiner) or the
// combining grapheme joiner U+034F (category Mn). macOS HFS+ ignores these
// when it compares names, so ".g\u200cit" is ".git" there (git CVE-2014-9390).
func formatChar(r rune) bool { return unicode.Is(unicode.Cf, r) || r == 0x034f }

// lookAlikes are NFKC-normal runes that read as, or that an ANSI best-fit code
// page maps to, "/", "\", ":" or "." (the WorstFit class). Fullwidth forms
// need no entry: NFKC folds them, so the NFKC rule refuses them. The table is
// defence in depth only: no op passes a confined name to argv or an ANSI API.
const lookAlikes = "\u2215\u2044\u29f8\u2216\u29f5\u29f9\u00a5\u20a9\u2236\u02d0"

// pathParts splits on both separators, on every OS.
func pathParts(name string) []string {
	return strings.FieldsFunc(name, func(r rune) bool { return r == '/' || r == '\\' })
}

// deniedDir compares by simple fold and by upper case: strings.EqualFold
// misses the dotless ı (U+0131), whose upper case is "I", so ".gıt" must be
// caught by strings.ToUpper.
func deniedDir(part string) bool {
	up := strings.ToUpper(part)
	return strings.EqualFold(part, ".git") || strings.EqualFold(part, ".piperoom") ||
		up == ".GIT" || up == ".PIPEROOM"
}

// reservedDevice matches the Windows device names with any extension, as
// Windows 10 does; Windows 11 frees "CON.txt", so os.Root alone differs by OS
// version.
func reservedDevice(part string) bool {
	base := part
	if i := strings.IndexByte(base, '.'); i >= 0 {
		base = base[:i]
	}
	base = strings.ToUpper(strings.TrimRight(base, " "))
	switch base {
	case "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$", "CLOCK$":
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

// shortNameAlias matches an 8.3 short name such as PIPERO~1, which opens
// .piperoom on a volume with short names.
func shortNameAlias(part string) bool {
	i := strings.IndexByte(part, '~')
	return i >= 0 && i+1 < len(part) && part[i+1] >= '0' && part[i+1] <= '9'
}

// prepare checks name lexically, walks every existing prefix through the
// root, and returns the slash-separated name to pass to os.Root together with
// the denied directory identities the walk used (so the caller can re-check
// the opened handle with the same identities).
func (c *Confiner) prepare(name string) (string, []fs.FileInfo, error) {
	if why := lexicalRefusal(name); why != "" {
		return "", nil, fmt.Errorf("%w: %s: %s", ErrPath, why, clip(name))
	}
	clean := strings.ReplaceAll(name, "\\", "/")
	denied := c.denied()
	if why := walkRefusal(clean, c.root.Lstat, denied, os.SameFile); why != "" {
		return "", denied, fmt.Errorf("%w: %s: %s", ErrPath, why, clip(name))
	}
	return clean, denied, nil
}

// denied returns the identities of the root's .git and .piperoom. It runs on
// every op, so a .piperoom the runner created after start is re-resolved.
func (c *Confiner) denied() []fs.FileInfo {
	var out []fs.FileInfo
	for _, name := range []string{".git", ".piperoom"} {
		if fi, err := c.root.Lstat(name); err == nil {
			out = append(out, fi)
		}
	}
	return out
}

// walkRefusal is the per-component walk: for every component of name (each
// directory on the way, then the leaf, as far as it exists) it lstats the
// prefix and refuses a symlink or reparse point and anything that is the same
// file as a denied directory. Checking only the opened file or its parent
// would let "<alias of .git>/hooks/pre-commit" through. lstat and same are
// injectable so the nested-alias case, which needs no portable unprivileged
// way to build, is table-tested with a fake file system.
func walkRefusal(name string, lstat func(string) (fs.FileInfo, error), denied []fs.FileInfo, same func(a, b fs.FileInfo) bool) string {
	var prefix []string
	for _, part := range pathParts(name) {
		if part == "." || part == "" {
			continue
		}
		prefix = append(prefix, part)
		p := strings.Join(prefix, "/")
		fi, err := lstat(p)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return "" // the rest does not exist yet: a create under checked parents
			}
			return "lstat: " + clip(err.Error())
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

// verifyHandle re-checks an opened file: it must be a regular file and must
// not be the same file as a denied directory. os.SameFile is re-run on the
// opened handle, so a swap between the walk and the open is caught.
func (c *Confiner) verifyHandle(f *os.File, denied []fs.FileInfo) error {
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("ops: stat opened file: %w", err)
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("%w: the opened handle is not a regular file", ErrNotRegular)
	}
	for _, d := range denied {
		if os.SameFile(fi, d) {
			return fmt.Errorf("%w: the opened handle is an alias of a denied directory", ErrPath)
		}
	}
	return nil
}

// underAny reports whether the slash-separated path is under one of dirs.
func underAny(path string, dirs []string) bool {
	parts := pathParts(path)
	for _, dir := range dirs {
		dp := pathParts(strings.TrimPrefix(dir, "./"))
		if len(dp) == 0 {
			return true // the project root itself is mounted: no write is safe
		}
		if len(dp) > len(parts) {
			continue
		}
		ok := true
		for i := range dp {
			if !strings.EqualFold(dp[i], parts[i]) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}
