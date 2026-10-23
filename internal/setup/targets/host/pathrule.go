package host

import (
	"fmt"
	"path"
	"regexp"
	"strings"
)

// The path rule of a rendered artifact (a systemd unit, a Windows service
// definition) exists once for both shapes, because this package renders for the
// host its build runs on and its tests exercise both shapes on every OS (row
// U8b: "the properties and plugin path with Windows paths"). A value is
// refused, never escaped: systemd expands %-specifiers, $VARIABLES, quotes and
// backslash escapes, and a Windows command line has its own quoting, so a
// refusal has no escaping bug.

// maxTemplatePath bounds one path in a rendered template.
const maxTemplatePath = 512

// windowsComponent RE is one component of a drive-absolute path: no space, %,
// $, quote, *, ?, <, >, |, ;, = or control character. A comma and a tilde are
// legal in a path and harmless in a service definition, so a project directory
// that holds them is served rather than refused.
var windowsComponent = regexp.MustCompile(`^[A-Za-z0-9._+@~,-]+$`)

// CheckTemplatePath refuses a path a rendered unit or service definition cannot
// carry verbatim. Two shapes are accepted:
//
//   - POSIX: absolute, clean, and built from safeToken only;
//   - Windows: drive-absolute (C:\ or C:/), every component a plain name, no
//     dot or dot-dot component, no trailing dot or space, no reserved device
//     name.
//
// A space is refused in both shapes: a systemd unit cannot carry one, and one
// rule keeps a plan that previews on one target portable to the other. A
// relative, rooted or UNC path is refused, and so is a drive-relative path
// (C:foo), which resolves against the process's current directory rather than
// naming one.
func CheckTemplatePath(p string) error {
	if safeToken.MatchString(p) {
		if !strings.HasPrefix(p, "/") || path.Clean(p) != p {
			return fmt.Errorf("%w: %s is not an absolute clean path", ErrDetect, q(p))
		}
		return nil
	}
	if why := windowsPathRefusal(p); why == "" {
		return nil
	} else if strings.HasPrefix(p, "/") || strings.HasPrefix(p, `\`) || isDrivePath(p) {
		return fmt.Errorf("%w: %s: %s", ErrDetect, q(p), why)
	}
	return fmt.Errorf("%w: %s holds a character a unit or service definition would interpret (space, %%, $, quote, backslash, control) or is not an absolute path", ErrDetect, q(p))
}

// CheckTemplateToken is CheckTemplatePath for a value that may also be a plain
// token (a JAVA_HOME value, one ExecStart argument): a safe POSIX token, a
// drive-absolute Windows path, a -Dkey=/abs/path argument, or an absolute path
// ending in a /* glob (the plugin dir in a classpath).
func CheckTemplateToken(v string) error {
	if safeToken.MatchString(v) && !strings.Contains(v, "/") {
		return nil
	}
	if why := windowsPathRefusal(v); why == "" {
		return nil
	}
	if key, val, ok := strings.Cut(v, "="); ok && key != "" && safeToken.MatchString(key) && tokenPathOK(val) {
		return nil
	}
	if tokenPathOK(v) {
		return nil
	}
	return fmt.Errorf("%w: %s is neither a plain token nor a drive-absolute path", ErrDetect, q(v))
}

// tokenPathOK reports whether v is an absolute POSIX path, optionally with a
// trailing /* glob. A relative path, a space, %, $, a quote or a backslash is
// refused, as CheckTemplatePath refuses them.
func tokenPathOK(v string) bool {
	if v == "" || strings.ContainsAny(v, " \t\n\r\"'%$\\") {
		return false
	}
	base := strings.TrimSuffix(v, "/*")
	if base != v {
		return strings.HasPrefix(base, "/") && path.Clean(base) == base && safeToken.MatchString(base)
	}
	if !strings.HasPrefix(v, "/") || path.Clean(v) != v {
		return false
	}
	return safeToken.MatchString(v)
}

// checkDefinitionValue refuses a value a rendered service definition cannot
// carry: the record is one key=value per line, so a newline or another control
// character would forge a line. The paths are checked by CheckTemplatePath;
// this is the rule for the arguments between them (one Java argument, for
// example, is -Dkafka.logs.dir=<path>, which is neither a token nor a path).
func checkDefinitionValue(v string) error {
	if v == "" || len(v) > maxTemplatePath {
		return fmt.Errorf("%w: an empty or over-long service definition value", ErrDetect)
	}
	if strings.ContainsFunc(v, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return fmt.Errorf("%w: %s carries a control character", ErrDetect, q(v))
	}
	return nil
}

// pathSeparator is the separator of a path's shape: the last separator the path
// itself uses, or "\" for a drive-absolute path, "/" otherwise. It keeps a
// value in the shape its caller named (C:/p stays forward slashed, C:\p stays
// back slashed), which nativeJoin and the JVM classpath need.
func pathSeparator(p string) string {
	if i := strings.LastIndexAny(p, `/\`); i >= 0 {
		if p[i] == '\\' {
			return `\`
		}
		return "/"
	}
	if isDrivePath(p) {
		return `\`
	}
	return "/"
}

// isDrivePath reports whether p starts with a drive letter and a separator
// (C:\ or C:/). A drive-relative "C:foo" is not one.
func isDrivePath(p string) bool {
	return len(p) >= 3 && asciiLetter(p[0]) && p[1] == ':' && (p[2] == '\\' || p[2] == '/')
}

// windowsPathRefusal returns why p is not a safe drive-absolute Windows path,
// or "" when it is one.
func windowsPathRefusal(p string) string {
	if strings.HasPrefix(p, `\\`) || strings.HasPrefix(p, "//") {
		return "UNC or device path"
	}
	if !isDrivePath(p) {
		return "not a drive-absolute Windows path (C:\\...)"
	}
	body := p[2:]
	if body == `\` || body == "/" {
		return "" // a drive root
	}
	if len(p) > maxTemplatePath {
		return "too long"
	}
	prevSep := false
	for _, r := range body {
		if r == '\\' || r == '/' {
			if prevSep {
				return "empty path component"
			}
			prevSep = true
			continue
		}
		prevSep = false
	}
	parts := strings.FieldsFunc(body, func(r rune) bool { return r == '\\' || r == '/' })
	if len(parts) == 0 || prevSep {
		return "empty path component"
	}
	for _, part := range parts {
		switch {
		case part == "." || part == "..":
			return "dot or dot-dot component"
		case strings.HasSuffix(part, ".") || strings.HasSuffix(part, " "):
			return "trailing dot or space"
		case reservedDeviceName(part):
			return "reserved device name"
		case !windowsComponent.MatchString(part):
			return "a character a Windows path cannot carry (space, %, $, quote, *, ?, <, >, |, ;, =, control)"
		}
	}
	return ""
}

// reservedDeviceName matches the Windows device names with any extension, as
// Windows 10 does (Windows 11 frees CON.txt), so a path a Windows 10 host
// cannot create is refused on both.
func reservedDeviceName(part string) bool {
	base := part
	if i := strings.IndexByte(base, '.'); i >= 0 {
		base = base[:i]
	}
	base = strings.ToUpper(base)
	switch base {
	case "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$", "CLOCK$":
		return true
	}
	if len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) {
		return base[3] >= '0' && base[3] <= '9'
	}
	return false
}

func asciiLetter(b byte) bool { return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') }
