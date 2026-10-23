package host

import (
	"fmt"
	"net"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"piperoom/internal/redact"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// Limits of a worker properties file.
const (
	maxEntries     = 64
	maxValueBytes  = 1024
	maxParsedLines = 512
)

// workerFiles are the files properties_write writes, at the project root,
// each the worker file of one mode (the file the unit's ExecStart passes).
var workerFiles = map[string]string{
	"connect-distributed.properties": ModeDistributed,
	"connect-standalone.properties":  ModeStandalone,
}

// sampleFiles are the distribution's own worker files: properties_read may
// read them (to compare with what the person had), nothing writes them.
var sampleFiles = map[string]string{
	DistDir + "/config/connect-distributed.properties": ModeDistributed,
	DistDir + "/config/connect-standalone.properties":  ModeStandalone,
}

// keyRule is the format a value must have.
type keyRule int

const (
	ruleHostPorts keyRule = iota
	ruleClass
	ruleBool
	ruleInt
	ruleName
	ruleHostname
	ruleListeners
	rulePathList
	rulePath
)

// commonKeys is the allow-list both modes share: joining a broker, the
// converters, where plugins load from, and the REST listener. No key here
// loads code from a value except plugin.path, which must name a directory
// under the project; config.providers, *.jaas.config and the like have no
// entry, so they cannot be written.
var commonKeys = map[string]keyRule{
	"bootstrap.servers":              ruleHostPorts,
	"key.converter":                  ruleClass,
	"value.converter":                ruleClass,
	"key.converter.schemas.enable":   ruleBool,
	"value.converter.schemas.enable": ruleBool,
	"plugin.path":                    rulePathList,
	"listeners":                      ruleListeners,
	"rest.advertised.host.name":      ruleHostname,
	"rest.advertised.port":           ruleInt,
	"offset.flush.interval.ms":       ruleInt,
	"topic.creation.enable":          ruleBool,
}

// modeKeys are the keys only one mode reads. A distributed key in a
// standalone file (or the reverse) is the classic wrong-mode mistake, so it is
// refused rather than written and ignored.
var modeKeys = map[string]map[string]keyRule{
	ModeDistributed: {
		"group.id":                          ruleName,
		"config.storage.topic":              ruleName,
		"offset.storage.topic":              ruleName,
		"status.storage.topic":              ruleName,
		"config.storage.replication.factor": ruleInt,
		"offset.storage.replication.factor": ruleInt,
		"status.storage.replication.factor": ruleInt,
	},
	ModeStandalone: {
		"offset.storage.file.filename": rulePath,
	},
}

// requiredKeys must be present (after the defaults) for the worker to start.
var requiredKeys = map[string][]string{
	ModeDistributed: {"bootstrap.servers", "group.id", "config.storage.topic", "offset.storage.topic", "status.storage.topic", "key.converter", "value.converter"},
	ModeStandalone:  {"bootstrap.servers", "key.converter", "value.converter", "offset.storage.file.filename"},
}

var (
	// valueRE is every character a worker value may hold: no space, $, %,
	// quote, backslash (a trailing one would join the next line), # or !.
	valueRE = regexp.MustCompile(`^[A-Za-z0-9._:/,+\[\]-]+$`)
	// pathValueRE is valueRE for a path value: a Windows path carries
	// backslashes, and this driver rewrites every path value to the forward
	// slash form Java reads on either OS (a backslash in a .properties value
	// is an escape), so one more character set covers both shapes.
	pathValueRE = regexp.MustCompile(`^[A-Za-z0-9._:/\\,+\[\]~@-]+$`)
	classRE     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+$`)
	intRE       = regexp.MustCompile(`^-?[0-9]{1,9}$`)
	nameRE      = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)
	hostnameRE  = regexp.MustCompile(`^[A-Za-z0-9.-]{1,253}$`)
	listenerRE  = regexp.MustCompile(`^(https?)://([^/]*)$`)
	offsetsRE   = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]{0,63}\.offsets$`)
)

// ruleFor returns the rule of key in mode, and whether the key belongs to the
// other mode instead.
func ruleFor(mode, key string) (rule keyRule, ok, otherMode bool) {
	if r, ok := commonKeys[key]; ok {
		return r, true, false
	}
	if r, ok := modeKeys[mode][key]; ok {
		return r, true, false
	}
	for m, keys := range modeKeys {
		if _, ok := keys[key]; ok && m != mode {
			return 0, false, true
		}
	}
	return 0, false, false
}

// workerProps is a validated worker file ready to render.
type workerProps struct {
	mode     string
	entries  map[string]string
	keys     []string // sorted
	defaults []string // keys the driver added
}

// prepareProps validates entries for mode, rewrites path values to absolute
// paths under dir and adds the defaults a safe worker needs: plugin.path (the
// project plugin directory; an unset plugin.path is the classic "connector
// class not found"), a loopback REST listener on every class (Connect's own
// default binds every interface, and its REST API is unauthenticated; a
// shared or production worker that must be reachable names its listeners
// explicitly), and a standalone offset file.
func prepareProps(mode string, entries map[string]string, dir, class string) (workerProps, error) {
	if len(entries) == 0 {
		return workerProps{}, fmt.Errorf("%w: entries is empty", ErrProperties)
	}
	if len(entries) > maxEntries {
		return workerProps{}, fmt.Errorf("%w: %d entries; the cap is %d", ErrProperties, len(entries), maxEntries)
	}
	wp := workerProps{mode: mode, entries: make(map[string]string, len(entries)+3)}
	for key, value := range entries {
		rule, ok, other := ruleFor(mode, key)
		switch {
		case other:
			return workerProps{}, fmt.Errorf("%w: key %s belongs to the other worker mode, not %s", ErrProperties, q(key), mode)
		case !ok:
			return workerProps{}, fmt.Errorf("%w: key %s is not in the %s worker allow-list", ErrProperties, q(key), mode)
		}
		v, err := checkValue(key, value, rule, dir, class)
		if err != nil {
			return workerProps{}, err
		}
		wp.entries[key] = v
	}
	add := func(key, value string) {
		if _, set := wp.entries[key]; !set {
			wp.entries[key] = value
			wp.defaults = append(wp.defaults, key)
		}
	}
	add("plugin.path", projectValue(dir, PluginDir))
	add("listeners", "http://127.0.0.1:8083")
	if mode == ModeStandalone {
		add("offset.storage.file.filename", projectValue(dir, OffsetsFile))
	}
	var missing []string
	for _, key := range requiredKeys[mode] {
		if _, set := wp.entries[key]; !set {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return workerProps{}, fmt.Errorf("%w: a %s worker needs %s", ErrProperties, mode, strings.Join(missing, ", "))
	}
	for key := range wp.entries {
		wp.keys = append(wp.keys, key)
	}
	sort.Strings(wp.keys)
	sort.Strings(wp.defaults)
	return wp, nil
}

// checkValue validates one allow-listed value and returns it, path values
// rewritten to absolute paths under dir in the forward slash form.
func checkValue(key, value string, rule keyRule, dir, class string) (string, error) {
	bad := func(what string) error {
		return fmt.Errorf("%w: key %s: %s", ErrProperties, q(key), what)
	}
	switch rule {
	case rulePath, rulePathList:
		if len(value) > maxValueBytes || !pathValueRE.MatchString(value) {
			return "", bad("the value is empty, too long or holds a character a path cannot carry (no space, $, %, quote, # or line break)")
		}
	default:
		if len(value) > maxValueBytes || !valueRE.MatchString(value) {
			return "", bad("the value is empty, too long or holds a character outside [A-Za-z0-9._:/,+[]-] (no space, $, %, quote, backslash or line break)")
		}
	}
	if masked, st := redact.Text(value); st.Total > 0 || masked != value {
		return "", fmt.Errorf("%w: key %s: the value looks like a credential", ErrSecretValue, q(key))
	}
	switch rule {
	case ruleClass:
		if !classRE.MatchString(value) {
			return "", bad("not a Java class name")
		}
	case ruleBool:
		if value != "true" && value != "false" {
			return "", bad("not true or false")
		}
	case ruleInt:
		if !intRE.MatchString(value) {
			return "", bad("not an integer")
		}
	case ruleName:
		if !nameRE.MatchString(value) {
			return "", bad("not a group or topic name")
		}
	case ruleHostname:
		if !hostnameRE.MatchString(value) {
			return "", bad("not a host name")
		}
	case ruleHostPorts:
		for _, hp := range strings.Split(value, ",") {
			if host, ok := splitHostPort(hp); !ok || host == "" {
				return "", bad("not a host:port list")
			}
		}
	case ruleListeners:
		for _, l := range strings.Split(value, ",") {
			m := listenerRE.FindStringSubmatch(l)
			host, ok := "", m != nil
			if ok {
				host, ok = splitHostPort(m[2])
			}
			if !ok {
				return "", bad("not an http(s)://host:port list")
			}
			if loopbackOnly(class) && !loopbackHost(host) {
				return "", bad("a local target's REST listener must bind 127.0.0.1, localhost or [::1]")
			}
		}
	case rulePath, rulePathList:
		parts := []string{value}
		if rule == rulePathList {
			parts = strings.Split(value, ",")
		}
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			abs, rel, err := projectPath(dir, p)
			if err != nil {
				return "", fmt.Errorf("key %s: %w", q(key), err)
			}
			// plugin.path names only the plugin directory or a directory
			// under it: plugin_install (digest-pinned) is the one writer
			// there, and it is a mount from Register on, so no generic file
			// op can plant an archive the worker would load. The offset file
			// is a root-level *.offsets name, so the worker cannot be pointed
			// at a project file and overwrite it.
			if rule == rulePathList && rel != PluginDir && !strings.HasPrefix(rel, PluginDir+"/") {
				return "", bad("plugin.path entries must be " + PluginDir + "/ or a directory under it (plugin_install fills it)")
			}
			if rule == rulePath && !offsetsRE.MatchString(rel) {
				return "", bad("the offset file must be a *.offsets file at the project root")
			}
			out = append(out, abs)
		}
		return strings.Join(out, ","), nil
	}
	return value, nil
}

// projectValue is dir/rel in the form a Java properties value carries: forward
// slashes, which Java reads on every OS and which need no escaping (a
// backslash in a .properties value is an escape, so C:\x would read as C:x).
func projectValue(dir string, rel ...string) string {
	return strings.ReplaceAll(filepath.Join(append([]string{dir}, rel...)...), "\\", "/")
}

// projectPath accepts an absolute path under dir or a project-relative one,
// and returns both forms with forward slashes. Both path shapes are handled
// explicitly rather than through filepath.Rel, so a Windows project directory
// is checked on any OS (the tests run the Windows shape on Linux too); on a
// Windows host dir is C:\... and a value may be written either way. The
// project root itself is refused: a plugin path or offset file there would make
// the whole project something the worker loads.
func projectPath(dir, p string) (abs, rel string, err error) {
	base := strings.ReplaceAll(dir, "\\", "/")
	value := strings.ReplaceAll(p, "\\", "/")
	rel = value
	if isDrivePath(value) || strings.HasPrefix(value, "/") {
		r, rerr := relUnder(base, value)
		if rerr != nil {
			return "", "", rerr
		}
		rel = r
	}
	rel = strings.TrimPrefix(rel, "./")
	if rel == "." || rel == "" {
		return "", "", fmt.Errorf("%w: %s is the project root, not a path under it", ErrProperties, q(p))
	}
	if err := ops.CheckName(rel); err != nil {
		return "", "", err
	}
	// A trailing slash ("connect-plugins/") names the same directory; the
	// unit-safe check below wants the clean form.
	rel = path.Clean(rel)
	if rel == "." || rel == "" || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", "", fmt.Errorf("%w: %s is the project root or outside it, not a path under it", ErrProperties, q(p))
	}
	abs = base + "/" + rel
	if err := CheckTemplatePath(abs); err != nil {
		return "", "", fmt.Errorf("%w: %w", ErrProperties, err)
	}
	return abs, rel, nil
}

// relUnder returns p relative to dir when p names dir or a path under it. A
// drive path is compared case-insensitively (Windows paths are), a POSIX path
// exactly; either separator counts. Both are already in forward slash form.
func relUnder(dir, p string) (string, error) {
	fold := func(s string) string {
		if isDrivePath(dir) {
			return strings.ToLower(s)
		}
		return s
	}
	d, v := fold(dir), fold(p)
	switch {
	case v == d:
		return "", nil
	case len(v) > len(d) && strings.HasPrefix(v, d) && v[len(d)] == '/':
		return p[len(d)+1:], nil
	}
	return "", fmt.Errorf("%w: %s is outside the project directory", ErrProperties, q(p))
}

// splitHostPort splits host:port (host may be [v6] or empty) and checks the
// port.
func splitHostPort(hp string) (string, bool) {
	host, port, err := net.SplitHostPort(hp)
	if err != nil {
		return "", false
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", false
	}
	if host != "" && !hostnameRE.MatchString(host) && net.ParseIP(host) == nil {
		return "", false
	}
	return host, true
}

// loopbackHost reports whether a listener host binds loopback only. "" binds
// every interface.
func loopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// loopbackOnly reports whether the class refuses a non-loopback REST
// listener (a local target does; a class the person raised may name one
// explicitly, though its default is loopback too).
func loopbackOnly(class string) bool {
	return class != protocol.ClassShared && class != protocol.ClassProduction
}

// render is the file text: a fixed header, then the keys in lexical order.
// Every value passed valueRE, so none can add a line or continue one.
func (wp workerProps) render() []byte {
	var b strings.Builder
	b.WriteString("# Generated by piperoom setup (properties_write, " + wp.mode + " worker); properties_write rewrites this file.\n")
	for _, key := range wp.keys {
		b.WriteString(key + "=" + wp.entries[key] + "\n")
	}
	return []byte(b.String())
}

// parseProperties reads the Java properties forms a worker file uses: # and !
// comments, key=value, key:value and key value, and backslash continuations.
// Values are kept as written; the caller treats them as host text.
func parseProperties(text string) (map[string]string, []string) {
	entries := map[string]string{}
	var order []string
	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	for i := 0; i < len(lines) && len(order) < maxParsedLines; i++ {
		line := strings.TrimLeft(lines[i], " \t\f")
		if line == "" || line[0] == '#' || line[0] == '!' {
			continue
		}
		for continued(line) && i+1 < len(lines) {
			i++
			line = line[:len(line)-1] + strings.TrimLeft(lines[i], " \t\f")
		}
		end := strings.IndexAny(line, "=: \t")
		key, value := line, ""
		if end >= 0 {
			key = line[:end]
			value = strings.TrimLeft(line[end:], " \t\f")
			if value != "" && (value[0] == '=' || value[0] == ':') {
				value = strings.TrimLeft(value[1:], " \t\f")
			}
		}
		if _, seen := entries[key]; !seen {
			order = append(order, key)
		}
		entries[key] = value
	}
	return entries, order
}

// continued reports whether a line ends in an odd number of backslashes.
func continued(line string) bool {
	n := 0
	for i := len(line) - 1; i >= 0 && line[i] == '\\'; i-- {
		n++
	}
	return n%2 == 1
}
