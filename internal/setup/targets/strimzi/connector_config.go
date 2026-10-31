package strimzi

import (
	"net"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"

	"piperoom/internal/redact"
	"piperoom/internal/setup/knowledge"
)

// The KafkaConnector config allow-list (design §6). A connector config is the
// model's text, and Connect hands every key to the plugin, so a key outside
// the plugin's table is refused, never passed through: a client override
// (producer.override.*), a second Kafka cluster (*bootstrap.servers), SQL the
// connector runs on connect (*initial.statements, heartbeat.action.query), a
// config provider, or a converter or transform class nobody reviewed. The
// tables are the keys a setup needs per plugin, plus the connector-scope keys
// of the knowledge index's tuning entries; a key the agent needs that is
// missing here is a PR to this file with its test, never a param.

// commonSourceKeys are the Debezium source keys every source plugin shares.
var commonSourceKeys = []string{
	"topic.prefix", "table.include.list", "table.exclude.list", "column.include.list", "column.exclude.list",
	"schema.include.list", "schema.exclude.list", "database.include.list", "database.exclude.list",
	"snapshot.mode", "snapshot.include.collection.list", "tombstones.on.delete", "decimal.handling.mode",
	"time.precision.mode", "binary.handling.mode", "include.schema.changes", "skipped.operations",
	"provide.transaction.metadata", "signal.data.collection", "signal.enabled.channels", "event.processing.failure.handling.mode",
	"message.key.columns", "column.propagate.source.type", "datatype.propagate.source.type",
	"topic.creation.default.replication.factor", "topic.creation.default.partitions",
	"topic.creation.default.cleanup.policy", "topic.creation.default.compression.type",
	"errors.tolerance", "errors.log.enable", "errors.log.include.messages", "errors.retry.timeout", "errors.retry.delay.max.ms",
	"key.converter", "value.converter", "key.converter.schemas.enable", "value.converter.schemas.enable",
	"key.converter.schema.registry.url", "value.converter.schema.registry.url",
	"transforms", "predicates",
}

// schemaHistoryKeys are the schema history keys of the sources that keep one
// (MySQL, SQL Server, Oracle). Its bootstrap must be the room's Kafka cluster
// (resolveFacts checks it against the Kafka CR's listeners).
var schemaHistoryKeys = []string{
	"schema.history.internal.kafka.topic", "schema.history.internal.kafka.bootstrap.servers",
	"schema.history.internal.store.only.captured.tables.ddl", "schema.history.internal.store.only.captured.databases.ddl",
	"schema.history.internal.skip.unparseable.ddl",
	"schema.history.internal.producer.security.protocol", "schema.history.internal.consumer.security.protocol",
	"schema.history.internal.producer.sasl.mechanism", "schema.history.internal.consumer.sasl.mechanism",
	"schema.history.internal.producer.sasl.jaas.config", "schema.history.internal.consumer.sasl.jaas.config",
}

// databaseKeys are the connection keys of the relational sources: the host
// and port are bound to the room's source target (bindSourceHosts).
var databaseKeys = []string{"database.hostname", "database.port", "database.user", "database.password"}

// pluginKeys are each plugin's own keys.
var pluginKeys = map[string][]string{
	"postgres": {
		"database.dbname", "database.sslmode", "plugin.name", "slot.name", "publication.name", "publication.autocreate.mode",
		"slot.drop.on.stop", "slot.max.retries", "slot.retry.delay.ms", "status.update.interval.ms", "xmin.fetch.interval.ms",
		"hstore.handling.mode", "interval.handling.mode", "money.fraction.digits", "schema.refresh.mode",
		"replica.identity.autoset.values", "snapshot.isolation.mode", "lsn.flush.mode",
	},
	"mysql": {
		"database.server.id", "database.ssl.mode", "gtid.source.includes", "gtid.source.excludes", "include.query",
		"snapshot.locking.mode", "connect.timeout.ms", "bigint.unsigned.handling.mode", "enable.time.adjuster",
		"inconsistent.schema.handling.mode", "connect.keep.alive",
	},
	"sqlserver": {
		"database.names", "database.encrypt", "database.trustServerCertificate", "snapshot.isolation.mode",
		"data.query.mode", "incremental.snapshot.option.recompile",
	},
	"oracle": {
		"database.dbname", "database.pdb.name", "log.mining.strategy", "log.mining.continuous.mine",
		"log.mining.archive.log.only.mode", "log.mining.batch.size.default", "log.mining.batch.size.min", "log.mining.batch.size.max",
		"log.mining.query.filter.mode", "lob.enabled", "archive.log.hours",
	},
	"mongodb": {
		"mongodb.connection.string", "mongodb.user", "mongodb.password", "mongodb.authsource", "mongodb.ssl.enabled",
		"mongodb.connection.mode", "capture.mode", "collection.include.list", "collection.exclude.list", "field.exclude.list",
		"field.renames",
	},
}

// jdbcSinkExtraKeys are the JDBC sink's keys beyond its index entry's.
var jdbcSinkExtraKeys = []string{
	"topics", "errors.tolerance", "errors.log.enable", "errors.log.include.messages",
	"errors.deadletterqueue.topic.name", "errors.deadletterqueue.topic.replication.factor", "errors.deadletterqueue.context.headers.enable",
	"key.converter", "value.converter", "key.converter.schemas.enable", "value.converter.schemas.enable",
	"key.converter.schema.registry.url", "value.converter.schema.registry.url", "transforms", "predicates",
}

// neverKeys are refused even when an index entry lists them: SQL the
// connector runs against the source on its own schedule, and the task count
// (spec.tasksMax carries it).
var neverKeys = map[string]string{
	"heartbeat.action.query": "SQL the connector runs on the source is never set by a setup",
	"tasks.max":              "the task count is spec.tasksMax",
	"connector.class":        "the class is spec.class",
}

// refusedKeyRE are key shapes refused whatever the plugin (the why names the
// rule): another cluster, client overrides, SQL run on connect, providers.
var refusedKeyRules = []struct {
	re  *regexp.Regexp
	why string
}{
	{regexp.MustCompile(`(^|\.)override\.`), "a client override (producer/consumer/admin.override.*) is never set by a setup"},
	{regexp.MustCompile(`bootstrap\.servers$`), "a Kafka bootstrap is the room's cluster, never a param"},
	{regexp.MustCompile(`initial\.statements$`), "SQL run on connect is never set by a setup"},
	{regexp.MustCompile(`^config\.providers`), "the config providers are the Connect worker's (the secrets provider only)"},
	{regexp.MustCompile(`(^|\.)(ssl\.(keystore|truststore)\.location|sslrootcert|sslcert|sslkey)$`), "a file path in the Connect pod is never set by a setup"},
}

// allowedConverters are the converter classes a connector may name.
var allowedConverters = map[string]bool{
	"org.apache.kafka.connect.json.JsonConverter":        true,
	"org.apache.kafka.connect.storage.StringConverter":   true,
	"io.confluent.connect.avro.AvroConverter":            true,
	"io.apicurio.registry.utils.converter.AvroConverter": true,
}

var (
	transformKeyRE   = regexp.MustCompile(`^(transforms|predicates)\.([A-Za-z][A-Za-z0-9_-]{0,31})\.([A-Za-z0-9_.-]{1,64})$`)
	transformNamesRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,31}(,[A-Za-z][A-Za-z0-9_-]{0,31}){0,15}$`)
)

var (
	indexOnce    sync.Once
	indexClasses map[string]bool            // SMT and predicate classes (and their variants)
	indexTuning  map[string]map[string]bool // plugin -> connector-scope tuning keys
	indexJDBC    map[string]bool            // the JDBC sink entry's keys
	indexErr     error
)

// loadIndexTables reads the knowledge index's SMT and predicate classes, the
// connector-scope tuning keys per plugin and the JDBC sink's keys once.
func loadIndexTables() error {
	indexOnce.Do(func() {
		ix, err := knowledge.LoadIndex()
		if err != nil {
			indexErr = err
			return
		}
		indexClasses = map[string]bool{}
		indexTuning = map[string]map[string]bool{}
		indexJDBC = map[string]bool{}
		for _, e := range ix.Entries() {
			switch {
			case (e.Kind == knowledge.KindSMT || e.Kind == knowledge.KindPredicate) && e.Runtime == knowledge.RuntimeConnect && e.Class != "":
				if len(e.Variants) == 0 {
					indexClasses[e.Class] = true
				}
				for _, v := range e.Variants {
					indexClasses[e.Class+"$"+v] = true
				}
			case e.Kind == knowledge.KindTuning && e.Runtime == knowledge.RuntimeConnect && e.Scope == "connector":
				for plugin := range pluginKeys {
					if e.Connector != "" && e.Connector != plugin {
						continue
					}
					if indexTuning[plugin] == nil {
						indexTuning[plugin] = map[string]bool{}
					}
					for _, k := range e.Keys {
						indexTuning[plugin][k.Name] = true
					}
				}
			case e.Name == "debezium-jdbc-sink":
				for _, k := range e.Keys {
					indexJDBC[k.Name] = true
					for _, f := range k.Formerly {
						indexJDBC[f] = true
					}
				}
			}
		}
	})
	return indexErr
}

// pluginOf is the plugin short name of a connector class.
func pluginOf(class string) (string, bool) {
	for p, c := range sourceClasses {
		if c == class {
			return p, true
		}
	}
	return "", false
}

// allowedKey reports whether key is in plugin's table.
func allowedKey(plugin, key string) bool {
	if plugin == "jdbc" {
		return indexJDBC[key] || containsKey(jdbcSinkExtraKeys, key)
	}
	if containsKey(commonSourceKeys, key) || indexTuning[plugin][key] || containsKey(pluginKeys[plugin], key) {
		return true
	}
	if plugin != "mongodb" && containsKey(databaseKeys, key) {
		return true
	}
	switch plugin {
	case "mysql", "sqlserver", "oracle":
		return containsKey(schemaHistoryKeys, key)
	}
	return false
}

func containsKey(list []string, k string) bool {
	for _, v := range list {
		if v == k {
			return true
		}
	}
	return false
}

// validateConnectorConfig is the per-plugin rule over spec.config: every key
// in the plugin's table, the shapes of refusedKeyRules refused, converters and
// transform classes from the allow-list and the knowledge index, TLS never
// turned off, and no credential inside a connection URL.
func validateConnectorConfig(plugin string, cfg map[string]any) error {
	if err := loadIndexTables(); err != nil {
		return refuse("spec.config", "the knowledge index does not load: "+err.Error())
	}
	keys := make([]string, 0, len(cfg))
	for k := range cfg {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	transforms := map[string]bool{}
	predicates := map[string]bool{}
	for _, list := range []struct {
		key string
		set map[string]bool
	}{{"transforms", transforms}, {"predicates", predicates}} {
		v, ok := cfg[list.key]
		if !ok {
			continue
		}
		s, _ := asString(v)
		if !transformNamesRE.MatchString(s) {
			return refuse("spec.config."+list.key, "a comma list of up to 16 names")
		}
		for _, n := range strings.Split(s, ",") {
			list.set[n] = true
		}
	}
	for _, k := range keys {
		path := "spec.config." + k
		v, _ := asString(cfg[k])
		if why, ok := neverKeys[k]; ok {
			return refuse(path, why)
		}
		for _, r := range refusedKeyRules {
			if r.re.MatchString(k) && !(k == "schema.history.internal.kafka.bootstrap.servers" && allowedKey(plugin, k)) {
				return refuse(path, r.why)
			}
		}
		if m := transformKeyRE.FindStringSubmatch(k); m != nil {
			set := transforms
			if m[1] == "predicates" {
				set = predicates
			}
			if !set[m[2]] {
				return refuse(path, m[2]+" is not listed in "+m[1])
			}
			if m[3] == "type" && !indexClasses[v] {
				return refuse(path, "a transform or predicate class from the knowledge index only")
			}
			if strings.HasSuffix(m[3], ".class") || strings.HasSuffix(m[3], "converter") {
				return refuse(path, "a class inside a transform is never set by a setup")
			}
			continue
		}
		if !allowedKey(plugin, k) {
			return refuse(path, "not in the "+plugin+" connector's key allow-list")
		}
		switch k {
		case "key.converter", "value.converter":
			if !allowedConverters[v] {
				return refuse(path, "a converter class from the allow-list only (JsonConverter, StringConverter, the Avro converters)")
			}
		case "database.sslmode":
			if strings.EqualFold(v, "disable") || strings.EqualFold(v, "allow") || strings.EqualFold(v, "prefer") {
				return refuse(path, "TLS to the source is never turned off or made optional by a setup")
			}
		case "database.ssl.mode":
			if strings.EqualFold(v, "disabled") || strings.EqualFold(v, "preferred") {
				return refuse(path, "TLS to the source is never turned off or made optional by a setup")
			}
		case "database.encrypt":
			if strings.EqualFold(v, "false") {
				return refuse(path, "TLS to the source is never turned off by a setup")
			}
		case "mongodb.ssl.enabled":
			// false is the plugin's default; the rule refuses only what turns
			// a check off, and this key has none to turn off.
		case "connection.url", "mongodb.connection.string":
			if credentialsInURL(v) {
				return refuse(path, "credentials belong in the user and password keys as ${secrets:} references, never in the URL")
			}
		case "schema.history.internal.kafka.bootstrap.servers":
			if !bootstrapRE.MatchString(v) {
				return refuse(path, "must be host:port entries of the room's Kafka listener")
			}
		}
	}
	return nil
}

// credentialsInURL reports whether a URL or DSN carries a user, a password or
// a password-like query parameter, or a secret the redactor masks.
func credentialsInURL(s string) bool {
	if strings.Contains(s, "${") {
		// A ${secrets:} reference inside a URL still puts a credential in it.
		return true
	}
	if u, err := url.Parse(s); err == nil && u.User != nil {
		return true
	}
	rest := s
	if i := strings.Index(rest, "://"); i >= 0 {
		rest = rest[i+3:]
		if at := strings.LastIndex(strings.SplitN(rest, "/", 2)[0], "@"); at >= 0 {
			return true
		}
	}
	l := strings.ToLower(s)
	for _, w := range []string{"password=", "pwd=", "passwd=", "user=", "username=", "secret=", "token="} {
		if strings.Contains(l, w) {
			return true
		}
	}
	if red, _ := redact.Text(s); red != s {
		return true
	}
	return false
}

// sourceHostPort is the host and port of a source target address: a URL's,
// host:port's, or the bare host with no port.
func sourceHostPort(addr string) (string, string) {
	a := strings.TrimSpace(addr)
	if strings.Contains(a, "://") {
		if u, err := url.Parse(a); err == nil {
			return strings.ToLower(u.Hostname()), u.Port()
		}
		return "", ""
	}
	if h, p, err := net.SplitHostPort(a); err == nil {
		return strings.ToLower(h), p
	}
	return strings.ToLower(a), ""
}

// mongoHosts are the host:port entries of a mongodb:// connection string.
func mongoHosts(s string) []string {
	rest, ok := strings.CutPrefix(s, "mongodb://")
	if !ok {
		rest, ok = strings.CutPrefix(s, "mongodb+srv://")
		if !ok {
			return nil
		}
	}
	if i := strings.IndexAny(rest, "/?"); i >= 0 {
		rest = rest[:i]
	}
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		rest = rest[at+1:]
	}
	return strings.Split(rest, ",")
}
