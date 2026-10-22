package compose

import (
	"path"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

// Limits of a properties file.
const (
	maxPropertiesEntries = 64
	maxPropertiesBytes   = 32 << 10
)

// propsKind is what a properties file is for; the key allow-list depends on it.
type propsKind string

const (
	// kindEnv is the .env file the pinned compose argv passes with
	// --env-file. Its values are interpolated into the rendered services.
	kindEnv propsKind = "env"
	// kindWorker is a Kafka Connect worker properties file (distributed
	// mode), written next to the compose file.
	kindWorker propsKind = "worker"
)

// envKeys is the .env allow-list: exactly the keys the rendered templates
// interpolate. COMPOSE_* and DOCKER_* are refused with their own reason below
// (design §2.5: they change what compose does, not what the stack is).
var envKeys = map[string]bool{
	"POSTGRES_USER":     true,
	"POSTGRES_DB":       true,
	"POSTGRES_PASSWORD": true,
}

// workerKeys is the Connect worker properties allow-list: the keys a
// distributed worker needs to join a broker and store its state. A key not in
// this list has no business in a file this driver writes (design §2.5).
var workerKeys = map[string]bool{
	"bootstrap.servers":                         true,
	"group.id":                                  true,
	"key.converter":                             true,
	"value.converter":                           true,
	"key.converter.schemas.enable":              true,
	"value.converter.schemas.enable":            true,
	"config.storage.topic":                      true,
	"offset.storage.topic":                      true,
	"status.storage.topic":                      true,
	"config.storage.replication.factor":         true,
	"offset.storage.replication.factor":         true,
	"status.storage.replication.factor":         true,
	"plugin.path":                               true,
	"listeners":                                 true,
	"rest.advertised.host.name":                 true,
	"rest.advertised.port":                      true,
	"offset.flush.interval.ms":                  true,
	"topic.creation.enable":                     true,
	"topic.creation.default.partitions":         true,
	"topic.creation.default.replication.factor": true,
}

// secretRefRE matches a ${secret:name} reference; secretRefOnlyRE matches a
// value that is exactly one. Nothing else may carry a "$".
var (
	secretRefRE     = regexp.MustCompile(`\$\{secret:[A-Za-z_][A-Za-z0-9_]*\}`)
	secretRefOnlyRE = regexp.MustCompile(`^\$\{secret:[A-Za-z_][A-Za-z0-9_]*\}$`)
)

// credentialWords are the credential words of actions.LooksSecret
// (internal/actions) without its bare "key" segment rule: compose environment
// names and Java properties both have legitimate non-credential keys that
// carry that segment (KEY_CONVERTER, key.converter), and refusing those would
// reject a valid stack. The list is deliberately a copy; the shared rule is
// the connector-config rule, this is the artifact-validator rule.
var credentialWords = []string{
	"password", "passwd", "secret", "token", "credential", "jaas", "apikey", "private",
}

// looksCredential reports whether key names a credential.
func looksCredential(key string) bool {
	k := strings.ToLower(key)
	for _, w := range credentialWords {
		if strings.Contains(k, w) {
			return true
		}
	}
	return false
}

// kindOf classifies the file name. Only the three exact names this driver
// writes are accepted, at the project root and in the exact case the stack
// reads them: "sub/.env" or "WORKER.PROPERTIES" would be written where the
// pinned argv and the worker never look, so a stack would silently run against
// a stale file. compose.yaml and every other name are refused, so
// properties_write can never bypass compose_write's validator.
func kindOf(file string) (propsKind, string, error) {
	clean := strings.TrimPrefix(strings.ReplaceAll(file, "\\", "/"), "./")
	switch clean {
	case EnvFileName:
		return kindEnv, clean, nil
	case "worker.properties", "connect-distributed.properties":
		return kindWorker, clean, nil
	}
	return "", path.Base(clean), ruleError(RuleProperties, "",
		"file %s is not a properties file this driver writes at the project root (.env, worker.properties, connect-distributed.properties)", q(file))
}

// validateEntries checks the allow-list and every value of one properties
// file. It returns the keys sorted, so the write and its summary agree.
func validateEntries(kind propsKind, entries map[string]string) ([]string, error) {
	if len(entries) == 0 {
		return nil, ruleError(RuleProperties, "", "entries is empty")
	}
	if len(entries) > maxPropertiesEntries {
		return nil, ruleError(RuleProperties, "", "entries has %d keys; the cap is %d", len(entries), maxPropertiesEntries)
	}
	allow := workerKeys
	if kind == kindEnv {
		allow = envKeys
	}
	keys := make([]string, 0, len(entries))
	total := 0
	for key, value := range entries {
		keys = append(keys, key)
		total += len(key) + len(value)
	}
	if total > maxPropertiesBytes {
		return nil, ruleError(RuleProperties, "", "the file would be %d bytes; the cap is %d", total, maxPropertiesBytes)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if kind == kindEnv {
			upper := strings.ToUpper(key)
			if strings.HasPrefix(upper, "COMPOSE_") || strings.HasPrefix(upper, "DOCKER_") {
				return nil, ruleError(RuleProperties, "", "key %s changes what compose does, not what the stack is", q(key))
			}
		}
		if !allow[key] {
			return nil, ruleError(RuleProperties, "", "key %s is not in the %s allow-list", q(key), kind)
		}
		if err := checkValue(kind, key, entries[key]); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

// checkValue validates one allow-listed value: no line injection, no $-form
// other than ${secret:name}, no plain-text credential, and a plugin.path under
// the project directory (or the container's plugin directory).
func checkValue(kind propsKind, key, value string) error {
	switch {
	case value == "":
		return ruleError(RuleProperties, "", "key %s has an empty value", q(key))
	case strings.ContainsFunc(value, unicode.IsControl):
		return ruleError(RuleProperties, "", "key %s carries a control character", q(key))
	case strings.TrimSpace(value) != value:
		return ruleError(RuleProperties, "", "key %s has a leading or trailing space", q(key))
	case strings.HasPrefix(value, "#"):
		return ruleError(RuleProperties, "", "key %s starts with #, which a properties parser reads as a comment", q(key))
	}
	rest := secretRefRE.ReplaceAllString(value, "")
	if strings.Contains(rest, "$") {
		return ruleError(RuleProperties, "", "key %s uses a $ form other than ${secret:name}", q(key))
	}
	if looksCredential(key) && !secretRefOnlyRE.MatchString(value) {
		return ruleError(RuleSecret, "", "key %s is a credential; its value must be a ${secret:name} reference", q(key))
	}
	if kind == kindWorker && key == "plugin.path" {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				return ruleError(RuleProperties, "", "plugin.path has an empty entry")
			}
			if part == PluginDirContainer {
				continue // the path inside the Connect container
			}
			if !localProjectPath(part) {
				return ruleError(RuleProperties, "", "plugin.path entry %s is outside the project directory", q(part))
			}
		}
	}
	return nil
}

// renderProperties is the file text: a fixed header, then the keys in lexical
// order. Values were validated for control characters, so no entry can add a
// line of its own.
func renderProperties(keys []string, entries map[string]string) []byte {
	var b strings.Builder
	b.WriteString("# Generated by piperoom setup; properties_write regenerates this file.\n")
	for _, key := range keys {
		b.WriteString(key)
		b.WriteString("=")
		b.WriteString(entries[key])
		b.WriteString("\n")
	}
	return []byte(b.String())
}

// allowedKeys is the allow-list of kind, for a summary or a schema.
func allowedKeys(kind propsKind) []string {
	allow := workerKeys
	if kind == kindEnv {
		allow = envKeys
	}
	return sortedKeys(allow)
}
