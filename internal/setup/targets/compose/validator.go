package compose

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"piperoom/internal/setup/knowledge"
	"piperoom/internal/setup/ops"
)

// Limits of a file the validator parses. A compose file handed to the runner
// is peer text even when compose_write rendered it, so it is bounded before
// the YAML parser sees it.
const (
	maxComposeBytes    = 256 << 10
	maxComposeServices = 64
	maxComposeEntries  = 256 // ports, volumes and environment entries per service
)

// deniedServiceKeys are the service fields the S1 allow-list (design §2.5)
// refuses outright, whatever their value: the templates render none of them,
// so their presence means the file was not rendered by this driver.
var deniedServiceKeys = map[string]struct{ rule, why string }{
	"privileged":          {RulePrivilege, "runs the container with host privileges"},
	"cap_add":             {RulePrivilege, "adds a Linux capability"},
	"devices":             {RulePrivilege, "passes a host device into the container"},
	"security_opt":        {RulePrivilege, "changes the container's security profile"},
	"pid":                 {RulePrivilege, "shares a host namespace"},
	"ipc":                 {RulePrivilege, "shares a host namespace"},
	"userns_mode":         {RulePrivilege, "changes the user namespace"},
	"device_cgroup_rules": {RulePrivilege, "adds device cgroup access"},
	"cgroup":              {RulePrivilege, "joins a host cgroup"},
	"cgroup_parent":       {RulePrivilege, "joins a host cgroup"},
	"sysctls":             {RulePrivilege, "changes kernel settings from the container"},
	"runtime":             {RulePrivilege, "selects a container runtime outside the templates"},
	"volumes_from":        {RulePrivilege, "inherits another container's volumes"},
	"extends":             {RuleExtends, "inherits another service or file"},
	"build":               {RuleBuild, "builds an image; no template in this driver builds"},
	"env_file":            {RuleMount, "adds an env file the pinned compose argv does not pass"},
	"secrets":             {RuleMount, "mounts a secret file outside the validated document"},
	"configs":             {RuleMount, "mounts a config file outside the validated document"},
}

// interpolationRE is a compose variable substitution from the env file
// (${VAR}, ${VAR:-def}, ${VAR-def}, ${VAR:?err}, ${VAR?err}). It is the only
// form a credential-valued service environment entry may take.
var interpolationRE = regexp.MustCompile(`^\$\{[A-Za-z_][A-Za-z0-9_]*(?::?[-?][^}]*)?\}$`)

// ValidateYAML parses doc and validates it against the S1 rules of design
// §2.5. class is the class of the Compose target: a local target must bind
// every published port to 127.0.0.1. A non-nil error is a *RuleError whose
// Rule the plan layer turns into the T2 preview's reason.
func ValidateYAML(doc []byte, class string) error {
	if len(doc) == 0 {
		return ruleError(RuleYAML, "", "the document is empty")
	}
	if len(doc) > maxComposeBytes {
		return ruleError(RuleYAML, "", "the document is %d bytes; the cap is %d", len(doc), maxComposeBytes)
	}
	dec := yaml.NewDecoder(bytes.NewReader(doc))
	var root map[string]any
	if err := dec.Decode(&root); err != nil {
		return ruleError(RuleYAML, "", "parsing: %s", clip(err.Error()))
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return ruleError(RuleYAML, "", "the document has more than one YAML document")
	}
	if len(root) == 0 {
		return ruleError(RuleYAML, "", "the document is not a mapping")
	}
	for _, key := range sortedKeys(root) {
		switch key {
		case "name", "services":
		case "volumes":
			if err := checkTopVolumes(root[key]); err != nil {
				return err
			}
		case "networks":
			if err := checkTopNetworks(root[key]); err != nil {
				return err
			}
		default:
			return ruleError(RuleTopLevel, "", "%s is not a key this driver validates", q(key))
		}
	}
	raw, ok := root["services"]
	if !ok {
		return ruleError(RuleTopLevel, "", "the document has no services")
	}
	services, ok := raw.(map[string]any)
	if !ok || len(services) == 0 {
		return ruleError(RuleTopLevel, "", "services is not a non-empty mapping")
	}
	if len(services) > maxComposeServices {
		return ruleError(RuleTopLevel, "", "the document has %d services; the cap is %d", len(services), maxComposeServices)
	}
	local := loopbackOnly(class)
	for _, name := range sortedKeys(services) {
		if !serviceNameRE.MatchString(name) {
			return ruleError(RuleTopLevel, "", "service name %s is not a compose service name", q(name))
		}
		if err := checkService(name, services[name], local); err != nil {
			return err
		}
	}
	return nil
}

// checkService validates one service.
func checkService(name string, raw any, local bool) error {
	svc, ok := raw.(map[string]any)
	if !ok {
		return ruleError(RuleYAML, name, "is not a mapping")
	}
	if len(svc) > 64 {
		return ruleError(RuleYAML, name, "has %d fields", len(svc))
	}
	for _, key := range sortedKeys(svc) {
		if denied, ok := deniedServiceKeys[key]; ok {
			return ruleError(denied.rule, name, "%s %s", q(key), denied.why)
		}
		switch key {
		case "network_mode":
			mode, _ := svc[key].(string)
			if mode != "bridge" && mode != "none" {
				return ruleError(RuleNetwork, name, "network_mode %s is not allowed", q(mode))
			}
		}
	}
	image, ok := svc["image"].(string)
	if !ok || image == "" {
		return ruleError(RuleImage, name, "has no image")
	}
	if err := checkImage(name, image); err != nil {
		return err
	}
	if v, ok := svc["environment"]; ok {
		if err := checkEnvironment(name, v); err != nil {
			return err
		}
	}
	if v, ok := svc["ports"]; ok {
		if err := checkPorts(name, v, local); err != nil {
			return err
		}
	}
	if v, ok := svc["volumes"]; ok {
		if err := checkVolumes(name, v); err != nil {
			return err
		}
	}
	return nil
}

// checkImage requires an image in the knowledge allow-list, with a tag and the
// digest the index pins (design §2.5: no floating tags, no unlisted images).
func checkImage(service, ref string) error {
	registry, repo, tag, digest, err := parseImageRef(ref)
	if err != nil {
		return ruleError(RuleImage, service, "image %s is malformed", q(ref))
	}
	if tag == "" {
		return ruleError(RuleImage, service, "image %s has no tag", q(ref))
	}
	if digest == "" {
		return ruleError(RuleImage, service, "image %s is not pinned to a digest", q(ref))
	}
	im, verr := knowledge.VerifyImage(repo, tag, digest)
	switch {
	case errors.Is(verr, knowledge.ErrUnknownImage):
		return ruleError(RuleImage, service, "image %s is not in the knowledge allow-list", q(ref))
	case errors.Is(verr, knowledge.ErrDigestMismatch):
		return ruleError(RuleImage, service, "image %s does not carry the pinned digest of %s", q(ref), q(repo+":"+tag))
	case verr != nil:
		return ruleError(RuleImage, service, "image %s: %s", q(ref), clip(verr.Error()))
	}
	if registry != "" && !sameRegistry(registry, im.Registry) {
		return ruleError(RuleImage, service, "image %s names registry %s; the allow-list pins it on %s", q(ref), q(registry), q(im.Registry))
	}
	return nil
}

// parseImageRef splits "[registry/]repo[:tag][@digest]". A registry is the
// first component when it looks like a hostname (a dot or a port) or is
// localhost.
func parseImageRef(ref string) (registry, repo, tag, digest string, err error) {
	name := strings.TrimSpace(ref)
	if name == "" || strings.ContainsAny(name, " \t\r\n") {
		return "", "", "", "", errors.New("empty or spaced reference")
	}
	if i := strings.LastIndex(name, "@"); i >= 0 {
		name, digest = name[:i], name[i+1:]
		if digest == "" {
			return "", "", "", "", errors.New("empty digest")
		}
	}
	slash := strings.LastIndex(name, "/")
	if colon := strings.LastIndex(name, ":"); colon > slash {
		name, tag = name[:colon], name[colon+1:]
	}
	parts := strings.SplitN(name, "/", 2)
	if len(parts) == 2 && (strings.ContainsAny(parts[0], ".:") || parts[0] == "localhost") {
		registry, repo = parts[0], parts[1]
	} else {
		repo = name
	}
	if repo == "" {
		return "", "", "", "", errors.New("empty repository")
	}
	return registry, repo, tag, digest, nil
}

// sameRegistry accepts docker.io's aliases as the same registry.
func sameRegistry(a, b string) bool {
	if a == b {
		return true
	}
	return a == "index.docker.io" && b == "docker.io" || a == "docker.io" && b == "index.docker.io"
}

// checkEnvironment refuses a plain-text credential in a service environment.
// A credential-named key must be an interpolation from the env file, whose
// value properties_write restricts to a ${secret:name} reference.
func checkEnvironment(service string, raw any) error {
	pairs := map[string]string{}
	switch v := raw.(type) {
	case []any:
		if len(v) > maxComposeEntries {
			return ruleError(RuleYAML, service, "has %d environment entries", len(v))
		}
		for _, e := range v {
			entry, ok := e.(string)
			if !ok {
				return ruleError(RuleYAML, service, "has a non-string environment entry")
			}
			key, value, _ := strings.Cut(entry, "=")
			pairs[key] = value
		}
	case map[string]any:
		if len(v) > maxComposeEntries {
			return ruleError(RuleYAML, service, "has %d environment entries", len(v))
		}
		for key, value := range v {
			pairs[key] = fmt.Sprint(value)
		}
	default:
		return ruleError(RuleYAML, service, "environment is not a list or a mapping")
	}
	for _, key := range sortedKeys(pairs) {
		value := pairs[key]
		if !looksCredential(key) || value == "" || interpolationRE.MatchString(value) {
			continue
		}
		return ruleError(RuleSecret, service,
			"environment %s carries a credential value in plain text; use a ${secret:name} reference in the env file", q(key))
	}
	return nil
}

// checkPorts requires every published port to be bound to 127.0.0.1 on a
// local target (design §2.5). A raised class may publish the plain
// host:container form.
func checkPorts(service string, raw any, local bool) error {
	list, ok := raw.([]any)
	if !ok {
		return ruleError(RuleYAML, service, "ports is not a list")
	}
	if len(list) > maxComposeEntries {
		return ruleError(RuleYAML, service, "has %d port entries", len(list))
	}
	for _, entry := range list {
		switch v := entry.(type) {
		case string:
			if err := checkPortString(service, v, local); err != nil {
				return err
			}
		case map[string]any:
			if err := checkPortMap(service, v, local); err != nil {
				return err
			}
		default:
			return ruleError(RuleYAML, service, "has a port entry that is neither a string nor a mapping")
		}
	}
	return nil
}

// checkPortString validates the short port syntax.
func checkPortString(service, entry string, local bool) error {
	s := entry
	if i := strings.IndexByte(s, '/'); i >= 0 {
		switch proto := s[i+1:]; proto {
		case "tcp", "udp", "sctp":
		default:
			return ruleError(RulePort, service, "port %s names protocol %s", q(entry), q(proto))
		}
		s = s[:i]
	}
	parts := strings.Split(s, ":")
	switch len(parts) {
	case 1:
		if !portSpec(parts[0]) {
			return ruleError(RulePort, service, "port %s is malformed", q(entry))
		}
	case 2:
		if !portSpec(parts[0]) || !portSpec(parts[1]) {
			return ruleError(RulePort, service, "port %s is malformed", q(entry))
		}
	case 3:
		if local && !isLoopback(parts[0]) {
			return ruleError(RulePort, service, "port %s binds %s; a local target binds 127.0.0.1", q(entry), q(parts[0]))
		}
		if (parts[1] != "" && !portSpec(parts[1])) || !portSpec(parts[2]) {
			return ruleError(RulePort, service, "port %s is malformed", q(entry))
		}
	default:
		return ruleError(RulePort, service, "port %s is malformed", q(entry))
	}
	if local && len(parts) < 3 {
		return ruleError(RulePort, service, "port %s publishes on every interface; a local target binds 127.0.0.1", q(entry))
	}
	return nil
}

// checkPortMap validates the long port syntax.
func checkPortMap(service string, m map[string]any, local bool) error {
	for _, key := range sortedKeys(m) {
		switch key {
		case "target", "published", "host_ip", "protocol", "mode":
		default:
			return ruleError(RulePort, service, "port field %s is not allowed", q(key))
		}
	}
	if _, ok := m["target"]; !ok {
		return ruleError(RulePort, service, "a port mapping has no target")
	}
	hostIP, _ := m["host_ip"].(string)
	if local && !isLoopback(hostIP) {
		return ruleError(RulePort, service, "a published port binds %s; a local target binds 127.0.0.1", q(hostIP))
	}
	if proto, ok := m["protocol"].(string); ok && proto != "tcp" && proto != "udp" && proto != "sctp" {
		return ruleError(RulePort, service, "port protocol %s is not a transport protocol", q(proto))
	}
	if mode, ok := m["mode"].(string); ok && mode != "ingress" && mode != "host" {
		return ruleError(RulePort, service, "port mode %s is not ingress or host", q(mode))
	}
	return nil
}

// portSpec accepts a port or a range ("8000-8010").
func portSpec(s string) bool {
	lo, hi, rangeForm := strings.Cut(s, "-")
	if !portNumber(lo) {
		return false
	}
	return !rangeForm || portNumber(hi)
}

// portNumber accepts a decimal port inside 1-65535.
func portNumber(s string) bool {
	n, err := strconv.Atoi(s)
	return err == nil && n >= 1 && n <= 65535 && strconv.Itoa(n) == s
}

// isLoopback reports whether hostIP names a loopback address. An empty host_ip
// is every interface, so it is not loopback.
func isLoopback(hostIP string) bool {
	ip := strings.TrimSpace(hostIP)
	if ip == "" {
		return false
	}
	ip = strings.TrimPrefix(strings.TrimSuffix(ip, "]"), "[")
	if ip == "localhost" {
		return true
	}
	parsed := net.ParseIP(ip)
	return parsed != nil && parsed.IsLoopback()
}

// checkVolumes validates short and long volume entries: bind sources stay
// under the project directory, the Docker socket and / are refused, and a
// named or anonymous volume is allowed.
func checkVolumes(service string, raw any) error {
	list, ok := raw.([]any)
	if !ok {
		return ruleError(RuleYAML, service, "volumes is not a list")
	}
	if len(list) > maxComposeEntries {
		return ruleError(RuleYAML, service, "has %d volume entries", len(list))
	}
	for _, entry := range list {
		switch v := entry.(type) {
		case string:
			if err := checkVolumeString(service, v); err != nil {
				return err
			}
		case map[string]any:
			if err := checkVolumeMap(service, v); err != nil {
				return err
			}
		default:
			return ruleError(RuleYAML, service, "has a volume entry that is neither a string nor a mapping")
		}
	}
	return nil
}

// checkVolumeString validates "src:dst[:mode]" and "dst".
func checkVolumeString(service, entry string) error {
	if strings.ContainsAny(entry, "\\") || strings.Contains(entry, "://") {
		return ruleError(RuleMount, service, "volume %s uses a host path form this validator does not accept", q(entry))
	}
	parts := strings.Split(entry, ":")
	switch len(parts) {
	case 1:
		return checkContainerPath(service, parts[0])
	case 2, 3:
	default:
		return ruleError(RuleMount, service, "volume %s is malformed", q(entry))
	}
	src, dst := parts[0], parts[1]
	if len(parts) == 3 && !volumeMode(parts[2]) {
		return ruleError(RuleMount, service, "volume %s names mode %s", q(entry), q(parts[2]))
	}
	if err := checkContainerPath(service, dst); err != nil {
		return err
	}
	if err := checkPluginMount(service, src, dst); err != nil {
		return err
	}
	if src == "" {
		return ruleError(RuleMount, service, "volume %s has an empty source", q(entry))
	}
	if strings.Contains(src, "docker.sock") || reachesDockerPipe(src) {
		return ruleError(RuleMount, service, "volume %s reaches the Docker socket", q(entry))
	}
	if isHostPath(src) {
		if !localProjectPath(src) {
			return ruleError(RuleMount, service, "volume %s binds outside the project directory", q(entry))
		}
		return nil
	}
	if strings.ContainsAny(src, "/\\") {
		return ruleError(RuleMount, service, "volume %s has a malformed source", q(entry))
	}
	return nil // a named volume: Docker keeps it, no host path is involved
}

// checkVolumeMap validates the long volume syntax.
func checkVolumeMap(service string, m map[string]any) error {
	for _, key := range sortedKeys(m) {
		switch key {
		case "type", "source", "target", "read_only", "consistency", "bind", "volume":
		default:
			return ruleError(RuleMount, service, "volume field %s is not allowed", q(key))
		}
	}
	typ, _ := m["type"].(string)
	source, _ := m["source"].(string)
	target, _ := m["target"].(string)
	if err := checkContainerPath(service, target); err != nil {
		return err
	}
	if err := checkPluginMount(service, source, target); err != nil {
		return err
	}
	if strings.Contains(source, "docker.sock") || reachesDockerPipe(source) {
		return ruleError(RuleMount, service, "a volume reaches the Docker socket")
	}
	switch typ {
	case "bind":
		if source == "" {
			return ruleError(RuleMount, service, "a bind mount has no source")
		}
		if !localProjectPath(source) {
			return ruleError(RuleMount, service, "a bind mount outside the project directory is not allowed")
		}
	case "volume":
		if source != "" && (strings.ContainsAny(source, "/\\:") || strings.HasPrefix(source, ".")) {
			return ruleError(RuleMount, service, "volume name %s is not a volume name", q(source))
		}
	default:
		return ruleError(RuleMount, service, "volume type %s is not volume or bind", q(typ))
	}
	return nil
}

// checkPluginMount: a mount at the Connect container's plugin directory must
// come from the project's plugin directory, so plugin_install writes and
// jar_list inspects exactly what the worker loads.
func checkPluginMount(service, source, target string) error {
	if target != PluginDirContainer && !strings.HasPrefix(target, PluginDirContainer+"/") {
		return nil
	}
	clean := strings.TrimPrefix(strings.TrimPrefix(source, "./"), `.\`)
	if clean != PluginDir {
		return ruleError(RuleMount, service,
			"the plugin directory is mounted at %s from %s, not ./%s", q(target), q(source), PluginDir)
	}
	return nil
}

// checkContainerPath requires an absolute container path without dot-dot.
func checkContainerPath(service, p string) error {
	if !strings.HasPrefix(p, "/") {
		return ruleError(RuleMount, service, "container path %s is not absolute", q(p))
	}
	for _, part := range strings.Split(p, "/") {
		if part == ".." {
			return ruleError(RuleMount, service, "container path %s walks out of itself", q(p))
		}
	}
	return nil
}

// volumeMode is compose's short-form mode list.
func volumeMode(s string) bool {
	for _, part := range strings.Split(s, ",") {
		switch part {
		case "ro", "rw", "z", "Z", "cached", "delegated", "consistent", "nocopy":
		default:
			return false
		}
	}
	return true
}

// isHostPath reports whether a short-form volume source is a host path (a
// bind) rather than a named volume.
func isHostPath(src string) bool {
	return strings.HasPrefix(src, "/") || strings.HasPrefix(src, ".") ||
		strings.HasPrefix(src, "~") || filepath.IsAbs(filepath.FromSlash(src))
}

// localProjectPath reports whether a bind source stays under the project
// directory: relative, no dot-dot, no drive, no home directory, and not the
// project root itself (which would hand the container .piperoom).
func localProjectPath(src string) bool {
	s := strings.TrimSpace(src)
	switch {
	case s == "", s == ".", s == "./", s == "..":
		return false
	case strings.HasPrefix(s, "~"):
		return false
	case strings.HasPrefix(s, "/"), strings.HasPrefix(s, "\\"):
		return false
	case !filepath.IsLocal(filepath.FromSlash(s)):
		return false
	}
	return ops.CheckName(strings.TrimPrefix(strings.TrimPrefix(s, "./"), `.\`)) == nil
}

// checkTopVolumes bounds the top-level volumes map: no driver_opts, which can
// bind a host path behind the validator's back.
func checkTopVolumes(raw any) error {
	m, ok := raw.(map[string]any)
	if !ok {
		return ruleError(RuleTopLevel, "", "volumes is not a mapping")
	}
	for _, name := range sortedKeys(m) {
		def, ok := m[name].(map[string]any)
		if !ok {
			continue
		}
		for _, key := range sortedKeys(def) {
			switch key {
			case "name", "labels", "external":
			default:
				return ruleError(RuleTopLevel, "", "volume %s: %s is not allowed", q(name), q(key))
			}
		}
	}
	return nil
}

// checkTopNetworks bounds the top-level networks map: the bridge driver only.
func checkTopNetworks(raw any) error {
	m, ok := raw.(map[string]any)
	if !ok {
		return ruleError(RuleTopLevel, "", "networks is not a mapping")
	}
	for _, name := range sortedKeys(m) {
		def, ok := m[name].(map[string]any)
		if !ok {
			continue
		}
		for _, key := range sortedKeys(def) {
			switch key {
			case "name", "labels", "external":
			case "driver":
				if driver, _ := def[key].(string); driver != "bridge" {
					return ruleError(RuleNetwork, "", "network %s uses driver %s", q(name), q(driver))
				}
			default:
				return ruleError(RuleTopLevel, "", "network %s: %s is not allowed", q(name), q(key))
			}
		}
	}
	return nil
}

// reachesDockerPipe reports whether a source names the Windows Docker pipe.
func reachesDockerPipe(s string) bool {
	return strings.Contains(s, `\\.\pipe`) || strings.Contains(s, `//./pipe`) ||
		strings.Contains(s, `\pipe\docker_engine`)
}
