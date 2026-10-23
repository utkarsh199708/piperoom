package compose

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"path"
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

// allowedServiceKeys is the S1 allow-list (design §2.5) of service fields:
// exactly the fields of the templates' service struct (templates.go: image,
// command, environment, ports, volumes, depends_on, healthcheck), plus
// network_mode restricted to bridge or none, which checkService pins. Any
// other field is refused whatever its value: the templates render none, so
// its presence means the file was not rendered by this driver, and compose
// keeps adding fields that reach the host (use_api_socket, post_start hooks,
// provider, label_file, gpus, ...), which a deny-list can never keep up with.
var allowedServiceKeys = map[string]bool{
	"image":        true,
	"command":      true,
	"environment":  true,
	"ports":        true,
	"volumes":      true,
	"depends_on":   true,
	"healthcheck":  true,
	"network_mode": true,
}

// knownRefusals names the rule of a refused service field compose documents
// as reaching the host, so a refusal carries a specific reason. It is not a
// gate: every field outside allowedServiceKeys is refused, and a field absent
// here is refused under RuleField.
var knownRefusals = map[string]struct{ rule, why string }{
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
	"entrypoint":          {RulePrivilege, "overrides the image's entrypoint; no template renders one"},
	"extends":             {RuleExtends, "inherits another service or file"},
	"build":               {RuleBuild, "builds an image; no template in this driver builds"},
	"env_file":            {RuleMount, "adds an env file the pinned compose argv does not pass"},
	"secrets":             {RuleMount, "mounts a secret file outside the validated document"},
	"configs":             {RuleMount, "mounts a config file outside the validated document"},
}

// Allowed values of the nested fields the templates render.
var (
	dependsConditions = map[string]bool{"service_started": true, "service_healthy": true, "service_completed_successfully": true}
	// volumeNameRE is a named volume: what compose accepts as a volume key,
	// without a path separator, a dot prefix or a substitution.
	volumeNameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`)
	// durationRE is a compose duration ("10s", "1m30s").
	durationRE = regexp.MustCompile(`^([0-9]+(us|ms|s|m|h))+$`)
)

// interpolationRE is a compose variable substitution from the env file: the
// bare ${VAR} form only. The default and error operators (${VAR:-def},
// ${VAR?err}) are refused everywhere, because a default is a plain-text value
// smuggled past the secret rule (${_:-hunter2} ships hunter2), and
// checkInterpolations restricts VAR to the .env allow-list, because any other
// variable reads the compose process's own environment into the stack
// (DEBUG=${AWS_SECRET_ACCESS_KEY}).
var interpolationRE = regexp.MustCompile(`^\$\{[A-Za-z_][A-Za-z0-9_]*\}$`)

// docInterpolationRE matches one allowed substitution at a position in the raw
// document; checkInterpolations anchors it at every '$'.
var docInterpolationRE = regexp.MustCompile(`^\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// checkInterpolations walks the raw document and requires every '$' to start
// an exact ${KEY} whose KEY is in the .env allow-list. Compose interpolates
// ${...} and $VAR anywhere in the file — volume sources, commands, images —
// from the compose process's environment, before any of the checks below see
// the resolved value; `${HOME}:/host` reads to them as a named volume and
// mounts the home directory. No rendered template uses any other form, so the
// gate refuses nothing a template produces. $$ (compose's literal-$ escape) is
// refused too: no template renders one, and a '$' that survives to a value is
// a form this validator cannot judge.
func checkInterpolations(doc []byte) error {
	s := string(doc)
	for i := 0; i < len(s); i++ {
		if s[i] != '$' {
			continue
		}
		m := docInterpolationRE.FindStringSubmatch(s[i:])
		if m == nil {
			return ruleError(RuleYAML, "", "the document uses a $ form other than ${KEY}; compose would interpolate it from the runner's environment")
		}
		if !envKeys[m[1]] {
			return ruleError(RuleYAML, "", "the document interpolates %s, which is not in the .env allow-list", q(m[1]))
		}
		i += len(m[0]) - 1
	}
	return nil
}

// ValidateYAML parses doc and validates it against the S1 rules of design
// §2.5. class is the class of the Compose target: a local target must bind
// every published port to 127.0.0.1. A non-nil error is a *RuleError whose
// Rule the plan layer turns into the T2 preview's reason.
func ValidateYAML(doc []byte, class string) error {
	_, err := validateDoc(doc, class)
	return err
}

// validateDoc is ValidateYAML that also returns the project-relative bind
// sources the document mounts, sorted, so a caller holding the project root
// can check them on disk (checkBindsOnDisk).
func validateDoc(doc []byte, class string) ([]string, error) {
	if len(doc) == 0 {
		return nil, ruleError(RuleYAML, "", "the document is empty")
	}
	if len(doc) > maxComposeBytes {
		return nil, ruleError(RuleYAML, "", "the document is %d bytes; the cap is %d", len(doc), maxComposeBytes)
	}
	if err := checkInterpolations(doc); err != nil {
		return nil, err
	}
	// yaml.v3 resolves anchors, aliases and merge keys (<<) while decoding
	// into a map, so every check below sees the merged service, not the
	// alias that would hide a field.
	dec := yaml.NewDecoder(bytes.NewReader(doc))
	var root map[string]any
	if err := dec.Decode(&root); err != nil {
		return nil, ruleError(RuleYAML, "", "parsing: %s", clip(err.Error()))
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, ruleError(RuleYAML, "", "the document has more than one YAML document")
	}
	if len(root) == 0 {
		return nil, ruleError(RuleYAML, "", "the document is not a mapping")
	}
	// The top-level allow-list: the templates render name and services only
	// (templates.go composeDoc). Top-level volumes are accepted only as plain
	// local declarations, for the named volumes a service may use; networks
	// are refused outright, because a service joins none in a render and a
	// declared network can alias the host's (name: host, external: true).
	for _, key := range sortedKeys(root) {
		switch key {
		case "services":
		case "name":
			name, ok := root[key].(string)
			if !ok || !projectNameRE.MatchString(name) {
				return nil, ruleError(RuleTopLevel, "", "name is not a %s compose project name", ProjectPrefix)
			}
		case "volumes":
			if err := checkTopVolumes(root[key]); err != nil {
				return nil, err
			}
		case "networks":
			return nil, ruleError(RuleNetwork, "", "top-level networks are not rendered by this driver; the stack uses the project's default network")
		default:
			return nil, ruleError(RuleTopLevel, "", "%s is not a key this driver validates", q(key))
		}
	}
	raw, ok := root["services"]
	if !ok {
		return nil, ruleError(RuleTopLevel, "", "the document has no services")
	}
	services, ok := raw.(map[string]any)
	if !ok || len(services) == 0 {
		return nil, ruleError(RuleTopLevel, "", "services is not a non-empty mapping")
	}
	if len(services) > maxComposeServices {
		return nil, ruleError(RuleTopLevel, "", "the document has %d services; the cap is %d", len(services), maxComposeServices)
	}
	local := loopbackOnly(class)
	binds := map[string]bool{}
	for _, name := range sortedKeys(services) {
		if !serviceNameRE.MatchString(name) {
			return nil, ruleError(RuleTopLevel, "", "service name %s is not a compose service name", q(name))
		}
		if err := checkService(name, services[name], local, binds); err != nil {
			return nil, err
		}
	}
	return sortedKeys(binds), nil
}

// checkService validates one service against the allow-list, then the value
// of every field it carries. binds collects the project-relative bind sources.
func checkService(name string, raw any, local bool, binds map[string]bool) error {
	svc, ok := raw.(map[string]any)
	if !ok {
		return ruleError(RuleYAML, name, "is not a mapping")
	}
	if len(svc) > 64 {
		return ruleError(RuleYAML, name, "has %d fields", len(svc))
	}
	image, ok := svc["image"].(string)
	if !ok || image == "" {
		return ruleError(RuleImage, name, "has no image")
	}
	for _, key := range sortedKeys(svc) {
		if allowedServiceKeys[key] {
			continue
		}
		if known, ok := knownRefusals[key]; ok {
			return ruleError(known.rule, name, "%s %s", q(key), known.why)
		}
		return ruleError(RuleField, name, "%s is not a field this driver renders", q(key))
	}
	// Only environment values and the healthcheck test carry ${KEY}: the
	// templates interpolate nowhere else, and a substitution anywhere else
	// is a value this validator would judge before compose replaces it (a
	// volume source ${POSTGRES_USER} reads as a named volume and binds
	// whatever .env says).
	for _, key := range sortedKeys(svc) {
		if key == "environment" || key == "healthcheck" {
			continue
		}
		if hasDollar(svc[key]) {
			return ruleError(RuleYAML, name, "%s carries a ${...} substitution; only environment values and the healthcheck test may", q(key))
		}
	}
	if err := checkImage(name, image); err != nil {
		return err
	}
	if v, ok := svc["network_mode"]; ok {
		mode, _ := v.(string)
		if mode != "bridge" && mode != "none" {
			return ruleError(RuleNetwork, name, "network_mode %s is not allowed", q(mode))
		}
	}
	if v, ok := svc["command"]; ok {
		if err := checkStrings(name, "command", v); err != nil {
			return err
		}
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
		if err := checkVolumes(name, v, binds); err != nil {
			return err
		}
	}
	if v, ok := svc["depends_on"]; ok {
		if err := checkDependsOn(name, v); err != nil {
			return err
		}
	}
	if v, ok := svc["healthcheck"]; ok {
		if err := checkHealthcheck(name, v); err != nil {
			return err
		}
	}
	return nil
}

// hasDollar reports whether any key or string value under v carries a '$'.
func hasDollar(v any) bool {
	switch t := v.(type) {
	case string:
		return strings.Contains(t, "$")
	case []any:
		for _, e := range t {
			if hasDollar(e) {
				return true
			}
		}
	case map[string]any:
		for k, e := range t {
			if strings.Contains(k, "$") || hasDollar(e) {
				return true
			}
		}
	}
	return false
}

// checkStrings accepts a string or a bounded list of strings (the command and
// healthcheck test forms the templates render).
func checkStrings(service, field string, raw any) error {
	switch v := raw.(type) {
	case string:
		return nil
	case []any:
		if len(v) > maxComposeEntries {
			return ruleError(RuleYAML, service, "%s has %d entries", field, len(v))
		}
		for _, e := range v {
			if _, ok := e.(string); !ok {
				return ruleError(RuleYAML, service, "%s has a non-string entry", field)
			}
		}
		return nil
	}
	return ruleError(RuleYAML, service, "%s is not a string or a list of strings", field)
}

// checkDependsOn accepts the list form and the mapping form the templates
// render ({kafka: {condition: service_healthy}}), with condition as the only
// field.
func checkDependsOn(service string, raw any) error {
	switch v := raw.(type) {
	case []any:
		if len(v) > maxComposeServices {
			return ruleError(RuleYAML, service, "depends_on has %d entries", len(v))
		}
		for _, e := range v {
			dep, ok := e.(string)
			if !ok || !serviceNameRE.MatchString(dep) {
				return ruleError(RuleYAML, service, "depends_on names something that is not a service name")
			}
		}
		return nil
	case map[string]any:
		if len(v) > maxComposeServices {
			return ruleError(RuleYAML, service, "depends_on has %d entries", len(v))
		}
		for _, dep := range sortedKeys(v) {
			if !serviceNameRE.MatchString(dep) {
				return ruleError(RuleYAML, service, "depends_on names %s, which is not a service name", q(dep))
			}
			cond, ok := v[dep].(map[string]any)
			if !ok {
				return ruleError(RuleYAML, service, "depends_on %s is not a mapping", q(dep))
			}
			for _, key := range sortedKeys(cond) {
				if key != "condition" {
					return ruleError(RuleField, service, "depends_on field %s is not a field this driver renders", q(key))
				}
			}
			if c, _ := cond["condition"].(string); !dependsConditions[c] {
				return ruleError(RuleYAML, service, "depends_on %s has condition %s", q(dep), q(c))
			}
		}
		return nil
	}
	return ruleError(RuleYAML, service, "depends_on is not a list or a mapping")
}

// checkHealthcheck accepts the fields the templates render: test, interval
// and retries.
func checkHealthcheck(service string, raw any) error {
	hc, ok := raw.(map[string]any)
	if !ok {
		return ruleError(RuleYAML, service, "healthcheck is not a mapping")
	}
	for _, key := range sortedKeys(hc) {
		switch key {
		case "test":
			if err := checkStrings(service, "healthcheck test", hc[key]); err != nil {
				return err
			}
		case "interval":
			if d, ok := hc[key].(string); !ok || !durationRE.MatchString(d) {
				return ruleError(RuleYAML, service, "healthcheck interval is not a duration")
			}
		case "retries":
			if n, ok := hc[key].(int); !ok || n < 0 || n > 1000 {
				return ruleError(RuleYAML, service, "healthcheck retries is not a small count")
			}
		default:
			return ruleError(RuleField, service, "healthcheck field %s is not a field this driver renders", q(key))
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
			if strings.Contains(key, "$") {
				return ruleError(RuleYAML, service, "an environment key carries a ${...} substitution")
			}
			pairs[key] = value
		}
	case map[string]any:
		if len(v) > maxComposeEntries {
			return ruleError(RuleYAML, service, "has %d environment entries", len(v))
		}
		for key, value := range v {
			if strings.Contains(key, "$") {
				return ruleError(RuleYAML, service, "an environment key carries a ${...} substitution")
			}
			switch value.(type) {
			case nil, string, int, int64, uint64, float64, bool:
			default:
				return ruleError(RuleYAML, service, "environment %s is not a scalar", q(key))
			}
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
func checkVolumes(service string, raw any, binds map[string]bool) error {
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
			if err := checkVolumeString(service, v, binds); err != nil {
				return err
			}
		case map[string]any:
			if err := checkVolumeMap(service, v, binds); err != nil {
				return err
			}
		default:
			return ruleError(RuleYAML, service, "has a volume entry that is neither a string nor a mapping")
		}
	}
	return nil
}

// checkVolumeString validates "src:dst[:mode]" and "dst".
func checkVolumeString(service, entry string, binds map[string]bool) error {
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
		if !pluginTarget(dst) {
			mode := ""
			if len(parts) == 3 {
				mode = parts[2]
			}
			if !modeReadOnly(mode) {
				return ruleError(RuleMount, service,
					"volume %s binds a project directory writable; every bind except ./%s is read-only (:ro)", q(entry), PluginDir)
			}
		}
		binds[bindPath(src)] = true
		return nil
	}
	if !volumeNameRE.MatchString(src) {
		return ruleError(RuleMount, service, "volume %s has a malformed source", q(entry))
	}
	return nil // a named volume: Docker keeps it, no host path is involved
}

// checkVolumeMap validates the long volume syntax.
func checkVolumeMap(service string, m map[string]any, binds map[string]bool) error {
	// type, source, target and read_only only: the bind and volume option
	// blocks (propagation, create_host_path, subpath, nocopy) are not
	// rendered, and volume.subpath walks a named volume's tree.
	for _, key := range sortedKeys(m) {
		switch key {
		case "type", "source", "target", "read_only":
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
		if ro, _ := m["read_only"].(bool); !ro && !pluginTarget(target) {
			return ruleError(RuleMount, service,
				"a bind mount of %s is writable; every bind except ./%s is read-only", q(source), PluginDir)
		}
		binds[bindPath(source)] = true
	case "volume":
		if source != "" && !volumeNameRE.MatchString(source) {
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

// pluginTarget reports whether a mount target is the Connect plugin directory,
// the one bind that stays writable: plugin_install fills it from the host, and
// a container writing its own plugin directory changes only what that same
// container loads. Every other writable bind would let an allow-listed image's
// shell plant files (git hooks, manifests, scripts) that the write ops exist
// to refuse.
func pluginTarget(target string) bool {
	return target == PluginDirContainer || strings.HasPrefix(target, PluginDirContainer+"/")
}

// modeReadOnly reports whether a short-form mode list carries "ro".
func modeReadOnly(mode string) bool {
	for _, part := range strings.Split(mode, ",") {
		if part == "ro" {
			return true
		}
	}
	return false
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

// bindPath is a bind source as a clean project-relative slash path.
func bindPath(src string) string {
	return path.Clean(strings.ReplaceAll(strings.TrimSpace(src), "\\", "/"))
}

// checkTopVolumes allows a plain local volume declaration only: an entry with
// no fields. The templates declare no volumes; a declaration is what a named
// service volume needs, and every field is a way out of the project: name
// and external reuse another project's (or any) volume, driver and
// driver_opts can bind a host path behind the validator's back.
func checkTopVolumes(raw any) error {
	m, ok := raw.(map[string]any)
	if !ok {
		return ruleError(RuleTopLevel, "", "volumes is not a mapping")
	}
	if len(m) > maxComposeEntries {
		return ruleError(RuleTopLevel, "", "volumes has %d entries", len(m))
	}
	for _, name := range sortedKeys(m) {
		if !volumeNameRE.MatchString(name) {
			return ruleError(RuleTopLevel, "", "volume %s is not a volume name", q(name))
		}
		switch def := m[name].(type) {
		case nil:
		case map[string]any:
			for _, key := range sortedKeys(def) {
				return ruleError(RuleTopLevel, "", "volume %s: %s is not allowed; a volume declaration carries no fields", q(name), q(key))
			}
		default:
			return ruleError(RuleTopLevel, "", "volume %s is not a mapping", q(name))
		}
	}
	return nil
}

// reachesDockerPipe reports whether a source names the Windows Docker pipe.
func reachesDockerPipe(s string) bool {
	return strings.Contains(s, `\\.\pipe`) || strings.Contains(s, `//./pipe`) ||
		strings.Contains(s, `\pipe\docker_engine`)
}

// docProjectName is the project a validated document names, DefaultProject
// when it names none. validateDoc has already refused a name outside the
// piperoom- namespace; this re-checks it, since the name decides which stack
// docker acts on.
func docProjectName(doc []byte) (string, error) {
	var head struct {
		Name *string `yaml:"name"`
	}
	if err := yaml.Unmarshal(doc, &head); err != nil {
		return "", ruleError(RuleYAML, "", "the document is not valid YAML")
	}
	if head.Name == nil {
		return DefaultProject, nil
	}
	if !projectNameRE.MatchString(*head.Name) {
		return "", ruleError(RuleTopLevel, "", "name is not a %s compose project name", ProjectPrefix)
	}
	return *head.Name, nil
}
