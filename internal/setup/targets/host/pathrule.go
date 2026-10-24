package host

import (
	"fmt"
	"path"
	"regexp"
	"runtime"
	"strings"
)

// The path rule of a rendered artifact exists in two shapes, because the two
// artifacts are interpreted differently (row U8b). A systemd unit is text:
// systemd splits ExecStart on spaces, expands %-specifiers and $VARIABLES, and
// honours quotes and backslash escapes, so every value must be a bare token.
// A Windows service definition is not text: mgr.CreateService builds the
// command line with syscall.EscapeArg from the exe path and an argv slice, the
// service host starts the JVM with an argv slice, and the definition record is
// one key=value per line, so a space or a parenthesis in a path is harmless
// there and common (`C:\Program Files\Eclipse Adoptium\jdk-21`). Sharing one
// rule refused every default JDK install; the two rules below are the shapes.
//
// Both rules refuse rather than escape: a refusal has no escaping bug, and the
// values come from detection (the project directory, the JDK, the distribution)
// rather than from anything a person or the agent typed.

// maxTemplatePath bounds one path in a rendered template.
const maxTemplatePath = 512

var (
	// windowsComponent is one component of a drive-absolute path in a systemd
	// unit or a record that is read back line by line: no space, %, $, quote,
	// *, ?, <, >, |, ;, = or control character. A comma and a tilde are legal
	// and harmless, so a project directory that holds them is served.
	windowsComponent = regexp.MustCompile(`^[A-Za-z0-9._+@~,-]+$`)
	// serviceComponent is one component of a path a Windows service definition
	// carries: windowsComponent plus a space and parentheses, which
	// CreateService and the argv slice pass through unchanged. A quote, %, $, *,
	// ?, <, >, |, ; or = is still refused: nothing in a detected path needs one,
	// and a definition line stays a single line.
	serviceComponent = regexp.MustCompile(`^[A-Za-z0-9._+@~,() -]+$`)
)

// CheckServicePath refuses a path a Windows service definition cannot carry.
// Two shapes are accepted:
//
//   - POSIX: absolute, clean, and built from safeToken only (the shape the
//     tests use for the Windows definition, so one plan renders both);
//   - Windows: drive-absolute (C:\ or C:/), every component a plain name
//     (serviceComponent: a space and parentheses are allowed, because
//     CreateService escapes each argument and the JVM gets an argv slice), no
//     dot or dot-dot component, no trailing dot or space, no reserved device
//     name.
//
// UNC, device (`\\?\`), rooted (`\x`) and drive-relative (`C:foo`) paths are
// refused: the first two name another host or namespace, and a drive-relative
// path resolves against the service's current directory rather than naming one.
func CheckServicePath(p string) error {
	if p == "" {
		return fmt.Errorf("%w: an empty path", ErrDetect)
	}
	if posixPathOK(p) {
		return nil
	}
	if why := windowsPathRefusal(p, serviceComponent, "a Windows path cannot carry (%, $, quote, *, ?, <, >, |, ;, =, control)"); why == "" {
		return nil
	} else if strings.HasPrefix(p, "/") || strings.HasPrefix(p, `\`) || isDrivePath(p) || isDriveRelative(p) {
		return fmt.Errorf("%w: %s: %s", ErrDetect, q(p), why)
	}
	return fmt.Errorf("%w: %s is not an absolute path of a shape a service definition can carry (a POSIX path of safe characters, or a drive-absolute Windows path)", ErrDetect, q(p))
}

// CheckServiceToken is CheckServicePath for a value that may also be a plain
// token or an argument carrying a path: the subcommand, a flag (-c, -cp), a
// -Dkey=<path> argument (the log directory, the log4j configuration), a main
// class, or a classpath entry ending in the JVM's /* or \* glob. A plain token
// is safeToken with no separator; every value that carries a path goes through
// CheckServicePath, so the one path rule stays in one place.
func CheckServiceToken(v string) error {
	if plainToken(v) {
		return nil
	}
	if CheckServicePath(v) == nil {
		return nil
	}
	if base, ok := trimClasspathGlob(v); ok && CheckServicePath(base) == nil {
		return nil
	}
	if key, val, ok := strings.Cut(v, "="); ok && plainToken(key) && CheckServicePath(val) == nil {
		return nil
	}
	return fmt.Errorf("%w: %s is neither a plain token nor a path a service definition can carry", ErrDetect, q(v))
}

// checkUnitToken is the systemd shape of CheckServiceToken: a plain token (a
// program name, JAVA_HOME=/opt/jdk, -Xmx2G), an absolute clean POSIX path, a
// -Dkey=/abs/path argument, or an absolute path ending in /* (the plugin
// directory in a classpath). A space, %, $, a quote, a backslash or a control
// character is refused: systemd would split or expand it. A drive-absolute
// path is accepted only in a Windows build, for the cross-OS tests (see
// CheckUnitPath in unit.go).
func checkUnitToken(v string) error {
	if plainToken(v) {
		return nil
	}
	if key, val, ok := strings.Cut(v, "="); ok && key != "" && safeToken.MatchString(key) && unitPathOK(val) {
		return nil
	}
	if unitPathOK(v) {
		return nil
	}
	return fmt.Errorf("%w: %s is neither a plain token nor an absolute POSIX path", ErrDetect, q(v))
}

// plainToken reports whether v is a single systemd- or command-line-safe word:
// safeToken, no path separator, and not the dot or dot-dot shorthand. "-Dkafka.
// logs.dir=/x" is not one (the "=" and the separator stay out); "-Xmx2G",
// "run-service" and a Java class name are.
func plainToken(v string) bool {
	return v != "" && v != "." && v != ".." && safeToken.MatchString(v) && !strings.ContainsAny(v, `/\`)
}

// unitPathOK reports whether v is an absolute clean POSIX path, optionally
// with a trailing /* glob, and in a Windows build also a drive-absolute path
// of unit-safe components (no space: see CheckUnitPath).
func unitPathOK(v string) bool {
	if base, ok := trimClasspathGlob(v); ok {
		return unitPathOK(base)
	}
	if posixPathOK(v) {
		return true
	}
	if runtime.GOOS != "windows" {
		return false
	}
	return isDrivePath(v) && windowsPathRefusal(v, windowsComponent, "a character a unit file would interpret") == ""
}

// posixPathOK reports whether p is an absolute, clean POSIX path of safe
// characters only.
func posixPathOK(p string) bool {
	return safeToken.MatchString(p) && strings.HasPrefix(p, "/") && path.Clean(p) == p
}

// trimClasspathGlob strips the JVM's /* or \* classpath glob from v, and the
// separator before it, so the rest is the directory the glob expands in.
func trimClasspathGlob(v string) (string, bool) {
	for _, glob := range []string{"/*", `\*`} {
		if strings.HasSuffix(v, glob) {
			return strings.TrimRight(strings.TrimSuffix(v, "*"), `/\`), true
		}
	}
	return "", false
}

// checkDefinitionValue refuses a value a rendered service definition cannot
// carry: the record is one key=value per line, so a newline or another control
// character would forge a line. The paths are checked by CheckServicePath;
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

// isDriveRelative reports whether p is drive-relative (C:foo): it names a
// directory on drive C: relative to that drive's current directory, which a
// service does not have.
func isDriveRelative(p string) bool {
	return len(p) >= 2 && asciiLetter(p[0]) && p[1] == ':' && (len(p) == 2 || (p[2] != '\\' && p[2] != '/'))
}

// windowsPathRefusal returns why p is not a safe drive-absolute Windows path
// whose components match component, or "" when it is one. badChars names what
// component's alphabet refuses, for the error.
func windowsPathRefusal(p string, component *regexp.Regexp, badChars string) string {
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
		case !component.MatchString(part):
			return badChars
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
