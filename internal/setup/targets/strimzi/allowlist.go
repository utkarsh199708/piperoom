package strimzi

import (
	"fmt"
	"piperoom/internal/setup/kafkatopics"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

// The field allow-list of design §6. Validate runs with no I/O on every
// rendered object before any request: a field outside the list is refused
// even when no template can produce it, and the error names its path. A
// template change is a PR here with its test, never a param.

// refuse is a field refusal naming the path.
func refuse(path, why string) error {
	return fmt.Errorf("%w: %s: %s", ErrField, path, why)
}

// keysOnly refuses every key of m outside allowed, naming the first (sorted).
func keysOnly(prefix string, m map[string]any, allowed ...string) error {
	ok := map[string]bool{}
	for _, a := range allowed {
		ok[a] = true
	}
	var bad []string
	for k := range m {
		if !ok[k] {
			bad = append(bad, k)
		}
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		return refuse(prefix+"."+bad[0], "not in the allow-list")
	}
	return nil
}

func asMap(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

func asString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case float64:
		if n == float64(int(n)) {
			return int(n), true
		}
	}
	return 0, false
}

// alwaysRefusedConnect are the KafkaConnect spec fields §6 refuses whatever
// their value, with the reason the refusal names.
var alwaysRefusedConnect = map[string]string{
	"image":                 "a user image is an unvalidated artifact; the operator picks the image for spec.version",
	"template":              "pod and container overrides (securityContext, hostPath, hostNetwork, serviceAccountName, initContainers, volumes, env) are never set by a setup",
	"externalConfiguration": "deprecated, and it mounts Secrets as volumes or env",
	"tracing":               "not part of a setup",
	"rack":                  "not part of a setup",
	"clientRackInitImage":   "an image the validator cannot judge",
	"jmxOptions":            "not part of a setup",
	"livenessProbe":         "not part of a setup",
	"readinessProbe":        "not part of a setup",
}

// connectConfigKeys is the KafkaConnect spec.config allow-list.
var connectConfigKeys = map[string]bool{
	"group.id": true, "offset.storage.topic": true, "config.storage.topic": true, "status.storage.topic": true,
	"offset.storage.replication.factor": true, "config.storage.replication.factor": true, "status.storage.replication.factor": true,
	"key.converter": true, "value.converter": true, "key.converter.schemas.enable": true, "value.converter.schemas.enable": true,
	"key.converter.schema.registry.url": true, "value.converter.schema.registry.url": true,
	"config.providers": true, "config.providers.secrets.class": true,
}

const secretsProviderClass = "io.strimzi.kafka.KubernetesSecretConfigProvider"

// deepRefusals finds the pod-level fields §6 names anywhere in a subtree:
// whatever key wraps them, a hostPath, hostNetwork, serviceAccountName,
// initContainers or volumes entry is refused.
var deepRefused = []string{"hostPath", "hostNetwork", "serviceAccountName", "initContainers", "volumes", "volumeMounts", "securityContext", "hostPID", "hostIPC", "privileged"}

func findDeep(prefix string, v any) error {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			for _, bad := range deepRefused {
				if k == bad {
					return refuse(prefix+"."+k, "a pod-level override is never set by a setup")
				}
			}
			if err := findDeep(prefix+"."+k, t[k]); err != nil {
				return err
			}
		}
	case []any:
		for i, e := range t {
			if err := findDeep(fmt.Sprintf("%s[%d]", prefix, i), e); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateConnect validates a KafkaConnect spec for pipeline (allowed names
// derive from it) and kafka.
func (d *Driver) ValidateConnect(spec map[string]any, kafka, ns string) error {
	if err := findDeep("spec", spec); err != nil {
		return err
	}
	for k, why := range alwaysRefusedConnect {
		if _, ok := spec[k]; ok {
			return refuse("spec."+k, why)
		}
	}
	if err := keysOnly("spec", spec, "replicas", "version", "bootstrapServers", "tls", "authentication", "config", "build", "resources", "logging", "metricsConfig", "jvmOptions"); err != nil {
		return err
	}
	if r, ok := spec["replicas"]; ok {
		n, isInt := asInt(r)
		if !isInt || n < 1 || n > maxReplicas {
			return refuse("spec.replicas", fmt.Sprintf("must be 1 to %d", maxReplicas))
		}
	}
	if b, ok := spec["bootstrapServers"]; ok {
		// The admission policy pins the same form: the cluster's in-cluster
		// bootstrap Service only, never an external listener's address.
		if s, isStr := asString(b); !isStr || !kafkaBootstrapRE(kafka, ns).MatchString(s) {
			return refuse("spec.bootstrapServers", "the Kafka cluster's bootstrap Service "+kafka+"-kafka-bootstrap[."+ns+"[.svc[.cluster.local]]]:<port> from the Kafka CR's listener status")
		}
	}
	if t, ok := spec["tls"]; ok {
		if err := d.validateTLS(t, kafka); err != nil {
			return err
		}
	}
	if a, ok := spec["authentication"]; ok {
		if err := d.validateAuth(a); err != nil {
			return err
		}
	}
	if c, ok := spec["config"]; ok {
		cm, isMap := asMap(c)
		if !isMap {
			return refuse("spec.config", "must be an object")
		}
		for k, v := range cm {
			if !connectConfigKeys[k] {
				return refuse("spec.config."+k, "not in the worker key allow-list")
			}
			s := fmt.Sprint(v)
			if strings.TrimSpace(s) != s || strings.Contains(s, "${") || strings.IndexFunc(s, unicode.IsControl) >= 0 {
				return refuse("spec.config."+k, "a plain value: no reference, no control character, no whitespace at either end")
			}
			switch k {
			case "key.converter", "value.converter":
				if !allowedConverters[s] {
					return refuse("spec.config."+k, "a converter class from the allow-list only")
				}
			case "key.converter.schema.registry.url", "value.converter.schema.registry.url":
				if _, _, err := registryURL(s); err != nil {
					return refuse("spec.config."+k, err.Error())
				}
			case "config.providers":
				if s != "secrets" {
					return refuse("spec.config.config.providers", "only the secrets provider is allowed")
				}
			case "config.providers.secrets.class":
				if s != secretsProviderClass {
					return refuse("spec.config."+k, "only "+secretsProviderClass+" is allowed")
				}
			}
		}
	}
	if b, ok := spec["build"]; ok {
		if err := d.validateBuild(b); err != nil {
			return err
		}
	}
	if l, ok := spec["logging"]; ok {
		lm, _ := asMap(l)
		if err := keysOnly("spec.logging", lm, "type", "loggers", "valueFrom"); err != nil {
			return err
		}
		if vf, ok := lm["valueFrom"]; ok {
			if err := d.configMapRef("spec.logging.valueFrom", vf, d.loggingName()); err != nil {
				return err
			}
		}
	}
	if m, ok := spec["metricsConfig"]; ok {
		mm, _ := asMap(m)
		if err := keysOnly("spec.metricsConfig", mm, "type", "valueFrom"); err != nil {
			return err
		}
		if t, _ := asString(mm["type"]); t != "jmxPrometheusExporter" {
			return refuse("spec.metricsConfig.type", "must be jmxPrometheusExporter")
		}
		if err := d.configMapRef("spec.metricsConfig.valueFrom", mm["valueFrom"], d.metricsName()); err != nil {
			return err
		}
	}
	if j, ok := spec["jvmOptions"]; ok {
		jm, _ := asMap(j)
		if err := keysOnly("spec.jvmOptions", jm, "-Xms", "-Xmx"); err != nil {
			return err
		}
	}
	return nil
}

var bootstrapRE = regexp.MustCompile(`^[a-z0-9.-]{1,253}:[0-9]{1,5}(,[a-z0-9.-]{1,253}:[0-9]{1,5})*$`)

func (d *Driver) validateTLS(v any, kafka string) error {
	m, _ := asMap(v)
	if err := keysOnly("spec.tls", m, "trustedCertificates"); err != nil {
		return err
	}
	certs, _ := m["trustedCertificates"].([]any)
	if len(certs) != 1 {
		return refuse("spec.tls.trustedCertificates", "exactly the cluster CA")
	}
	c, _ := asMap(certs[0])
	if err := keysOnly("spec.tls.trustedCertificates[0]", c, "secretName", "pattern"); err != nil {
		return err
	}
	if s, _ := asString(c["secretName"]); s != kafka+"-cluster-ca-cert" {
		return refuse("spec.tls.trustedCertificates[0].secretName", "only the Kafka cluster CA Secret")
	}
	return nil
}

func (d *Driver) validateAuth(v any) error {
	m, _ := asMap(v)
	t, _ := asString(m["type"])
	switch t {
	case "tls":
		if err := keysOnly("spec.authentication", m, "type", "certificateAndKey"); err != nil {
			return err
		}
		ck, _ := asMap(m["certificateAndKey"])
		if s, _ := asString(ck["secretName"]); s != d.connectName() {
			return refuse("spec.authentication.certificateAndKey.secretName", "only the Connect KafkaUser's Secret")
		}
	case "scram-sha-512":
		if err := keysOnly("spec.authentication", m, "type", "username", "passwordSecret"); err != nil {
			return err
		}
		ps, _ := asMap(m["passwordSecret"])
		if s, _ := asString(ps["secretName"]); s != d.connectName() {
			return refuse("spec.authentication.passwordSecret.secretName", "only the Connect KafkaUser's Secret")
		}
	default:
		return refuse("spec.authentication.type", "must be tls or scram-sha-512")
	}
	return nil
}

func (d *Driver) configMapRef(path string, v any, want string) error {
	m, _ := asMap(v)
	if err := keysOnly(path, m, "configMapKeyRef"); err != nil {
		return err
	}
	ref, _ := asMap(m["configMapKeyRef"])
	if s, _ := asString(ref["name"]); s != want {
		return refuse(path+".configMapKeyRef.name", "only the chart's "+want)
	}
	return nil
}

// imageRefRE is the build output image the design fixes:
// <registry>/<repo>:piperoom-<pipeline>-<digest>.
var imageRefRE = regexp.MustCompile(`^([a-z0-9.-]+(:[0-9]{1,5})?)/[a-z0-9._/-]{1,200}:piperoom-[a-z0-9-]{1,60}$`)

func (d *Driver) validateBuild(v any) error {
	m, _ := asMap(v)
	if err := keysOnly("spec.build", m, "output", "plugins"); err != nil {
		return err
	}
	out, _ := asMap(m["output"])
	if err := keysOnly("spec.build.output", out, "type", "image", "pushSecret"); err != nil {
		return err
	}
	if t, _ := asString(out["type"]); t != "docker" {
		return refuse("spec.build.output.type", "must be docker")
	}
	if s, _ := asString(out["pushSecret"]); s != d.pushSecretName() {
		return refuse("spec.build.output.pushSecret", "only "+d.pushSecretName())
	}
	if s, _ := asString(out["image"]); !imageRefRE.MatchString(s) || !strings.Contains(s, ":piperoom-"+d.pipeline+"-") {
		return refuse("spec.build.output.image", "must be <registry>/<repo>:piperoom-"+d.pipeline+"-<digest>")
	}
	plugins, _ := m["plugins"].([]any)
	if len(plugins) == 0 {
		return refuse("spec.build.plugins", "at least one plugin from the knowledge index")
	}
	for i, p := range plugins {
		pm, _ := asMap(p)
		path := fmt.Sprintf("spec.build.plugins[%d]", i)
		if err := keysOnly(path, pm, "name", "artifacts"); err != nil {
			return err
		}
		arts, _ := pm["artifacts"].([]any)
		if len(arts) == 0 {
			return refuse(path+".artifacts", "at least one artifact")
		}
		for j, a := range arts {
			am, _ := asMap(a)
			ap := fmt.Sprintf("%s.artifacts[%d]", path, j)
			if err := keysOnly(ap, am, "type", "url", "sha512sum"); err != nil {
				return err
			}
			switch t, _ := asString(am["type"]); t {
			case "tgz", "jar", "zip":
			default:
				return refuse(ap+".type", "must be tgz, jar or zip (maven carries no checksum)")
			}
			if s, _ := asString(am["sha512sum"]); !sha512HexRE.MatchString(s) {
				return refuse(ap+".sha512sum", "the knowledge index's digest is required")
			}
		}
	}
	return nil
}

var sha512HexRE = regexp.MustCompile(`^[0-9a-f]{128}$`)

// credentialKeys are the only config keys whose value may be a reference to
// the db-credentials Secret. Anywhere else a reference would carry the
// credential to wherever that key points: a JAAS line to another listener,
// a URL to another host, a transform into the records.
// Each names the Secret key it may reference: a user key the username, a
// password key the password. Debezium logs a user key's value at start, so a
// password referenced from a user key would land in the pod log.
var credentialKeys = map[string]string{
	"database.user": "username", "database.password": "password",
	"connection.username": "username", "connection.password": "password",
	"mongodb.user": "username", "mongodb.password": "password",
}

// ValidateConnector validates a KafkaConnector spec for the pipeline's topic
// prefix (the op's topic_prefix; "" falls back to a source's topic.prefix)
// in namespace ns on Kafka cluster kafka: §6's provider rule (a reference is
// exactly ${secrets:<ns>/<pipeline>-db-credentials:<key>}, and only in a
// credentialKeys key; Strimzi's store password only in the schema history
// store keys), no literal credential, no value with whitespace at either end,
// no lifecycle or offsets field, and every config key in the plugin's
// allow-list (connector_config.go).
func (d *Driver) ValidateConnector(spec map[string]any, ns, kafka, topicPrefix string) error {
	if err := keysOnly("spec", spec, "class", "tasksMax", "config", "autoRestart"); err != nil {
		return err
	}
	cls, _ := asString(spec["class"])
	plugin, ok := pluginOf(cls)
	if !ok {
		return refuse("spec.class", "must be a Debezium connector class from the knowledge index")
	}
	if n, ok := asInt(spec["tasksMax"]); ok && n != 1 {
		return refuse("spec.tasksMax", "a Debezium source runs one task")
	}
	cfg, _ := asMap(spec["config"])
	ref := func(key string) string { return "${secrets:" + ns + "/" + d.dbSecretName() + ":" + key + "}" }
	keys := make([]string, 0, len(cfg))
	for k := range cfg {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := fmt.Sprint(cfg[k])
		path := "spec.config." + k
		if k == "connector.class" {
			return refuse(path, "the class is spec.class")
		}
		if strings.IndexFunc(k+s, unicode.IsControl) >= 0 {
			// Java's trim() strips every character up to U+0020, so a
			// control character at an end is whitespace to the plugin.
			return refuse(path, "no control character in a key or value")
		}
		if strings.TrimSpace(k) != k || strings.TrimSpace(s) != s {
			// A connector trims some values and not others: " disable" may
			// pass a deny-list here and still turn TLS off there.
			return refuse(path, "no whitespace at either end of a key or value")
		}
		switch {
		case storePasswordKey(k) && s == strimziStorePassword:
			// Strimzi's own store password (connector_config.go); the
			// per-plugin rule pins the key to it.
			continue
		case strings.Contains(s, "${"):
			want, ok := credentialKeys[k]
			if !ok {
				return refuse(path, "a ${...} reference only in a user or password key (database.password, connection.password, ...), never here")
			}
			if s != ref(want) {
				return refuse(path, "exactly "+ref(want)+": a user key references the username, a password key the password")
			}
		case looksSecretKey(k):
			return refuse(path, "a credential is a ${secrets:} reference in a user or password key, never a value")
		}
	}
	// The template's topic_prefix param is the pipeline's (the server
	// refuses a connector step without it or with another). There is no
	// fallback to the config's topic.prefix: that would let a step that
	// left the param out pick the prefix its own topics are checked against.
	if topicPrefix == "" {
		return refuse("params.topic_prefix", topicErr(kafkatopics.ErrNoPrefix))
	}
	if src, _ := asString(cfg["topic.prefix"]); plugin != "jdbc" && src != "" && src != topicPrefix {
		return refuse("spec.config.topic.prefix", "the pipeline's topic prefix "+topicPrefix)
	}
	if err := validateConnectorConfig(plugin, cfg, kafka, ns, topicPrefix); err != nil {
		return err
	}
	if a, ok := spec["autoRestart"]; ok {
		am, _ := asMap(a)
		if err := keysOnly("spec.autoRestart", am, "enabled", "maxRestarts"); err != nil {
			return err
		}
	}
	return nil
}

func looksSecretKey(k string) bool {
	l := strings.ToLower(k)
	for _, w := range []string{"password", "secret", "token", "credential", "jaas", "apikey", "private"} {
		if strings.Contains(l, w) {
			return true
		}
	}
	return false
}

// userACLs is the fixed ACL table of §6 per KafkaUser template.
func (d *Driver) userACLs(template, topicPrefix string) []map[string]any {
	acl := func(typ, name, pattern string, ops ...string) map[string]any {
		o := make([]any, len(ops))
		for i, op := range ops {
			o[i] = op
		}
		return map[string]any{"resource": map[string]any{"type": typ, "name": name, "patternType": pattern}, "operations": o, "host": "*"}
	}
	group := d.connectName()
	if template == "read-user" {
		return []map[string]any{
			acl("topic", topicPrefix+".", "prefix", "Describe", "Read", "DescribeConfigs"),
			acl("group", group, "literal", "Describe"),
		}
	}
	internal := d.connectName() + "-connect-"
	out := []map[string]any{acl("topic", topicPrefix+".", "prefix", "Create", "Describe", "Read", "Write")}
	for _, t := range []string{"configs", "offsets", "status"} {
		out = append(out, acl("topic", internal+t, "literal", "Create", "Describe", "Read", "Write"))
	}
	return append(out, acl("group", group, "literal", "Describe", "Read"))
}

// ValidateUser validates a KafkaUser spec: no password source, simple
// authorization with exactly the template's ACL table (no other resource, no
// cluster or transactionalId ACL, no other operation).
func (d *Driver) ValidateUser(spec map[string]any, template, topicPrefix string) error {
	if err := keysOnly("spec", spec, "authentication", "authorization"); err != nil {
		return err
	}
	auth, _ := asMap(spec["authentication"])
	if _, ok := auth["password"]; ok {
		return refuse("spec.authentication.password", "the User Operator generates the password; a setup never chooses it")
	}
	if err := keysOnly("spec.authentication", auth, "type"); err != nil {
		return err
	}
	switch t, _ := asString(auth["type"]); t {
	case "tls", "scram-sha-512":
	default:
		return refuse("spec.authentication.type", "must be tls or scram-sha-512")
	}
	az, _ := asMap(spec["authorization"])
	if err := keysOnly("spec.authorization", az, "type", "acls"); err != nil {
		return err
	}
	if t, _ := asString(az["type"]); t != "simple" {
		return refuse("spec.authorization.type", "must be simple")
	}
	allowed := map[string]map[string]bool{}
	for _, a := range d.userACLs(template, topicPrefix) {
		r := a["resource"].(map[string]any)
		key := r["type"].(string) + "|" + r["name"].(string) + "|" + r["patternType"].(string)
		allowed[key] = map[string]bool{}
		for _, op := range a["operations"].([]any) {
			allowed[key][op.(string)] = true
		}
	}
	acls, _ := az["acls"].([]any)
	for i, a := range acls {
		am, _ := asMap(a)
		path := fmt.Sprintf("spec.authorization.acls[%d]", i)
		r, _ := asMap(am["resource"])
		typ, _ := asString(r["type"])
		if typ == "cluster" || typ == "transactionalId" {
			return refuse(path+".resource.type", typ+" ACLs are never granted by a setup")
		}
		name, _ := asString(r["name"])
		pat, _ := asString(r["patternType"])
		ops, ok := allowed[typ+"|"+name+"|"+pat]
		if !ok {
			return refuse(path+".resource", "not one of the pipeline's own resources")
		}
		list, _ := am["operations"].([]any)
		for _, op := range list {
			if s, _ := asString(op); !ops[s] {
				return refuse(path+".operations", fmt.Sprintf("%v is outside the table", op))
			}
		}
	}
	return nil
}

// ValidateTopic validates the canary KafkaTopic spec: its topic is exactly
// "<topicPrefix>.<schema>.piperoom_canary", never another topic of the prefix.
func (d *Driver) ValidateTopic(spec map[string]any, topicPrefix string) error {
	if err := keysOnly("spec", spec, "topicName", "partitions", "replicas", "config"); err != nil {
		return err
	}
	n, _ := asString(spec["topicName"])
	rest, ok := strings.CutPrefix(n, topicPrefix+".")
	schema, table, cut := strings.Cut(rest, ".")
	if topicPrefix == "" || !ok || !cut || !identRE.MatchString(schema) || table != CanaryTable {
		return refuse("spec.topicName", "must be the canary topic <topic_prefix>.<schema>."+CanaryTable)
	}
	if n, ok := asInt(spec["partitions"]); !ok || n != 1 {
		return refuse("spec.partitions", "the canary topic has one partition")
	}
	if n, ok := asInt(spec["replicas"]); !ok || n < 1 || n > 3 {
		return refuse("spec.replicas", "1 to 3")
	}
	cfg, _ := asMap(spec["config"])
	return keysOnly("spec.config", cfg, "cleanup.policy", "retention.ms")
}
