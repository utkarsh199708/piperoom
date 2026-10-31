package strimzi

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"

	"piperoom/internal/redact"
	"piperoom/internal/setup/kafkatopics"
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
// (MySQL, SQL Server, Oracle). No sasl.jaas.config: a reference to the
// credentials Secret is allowed only in credentialKeys, so a JAAS line can
// never carry it to a listener (SASL schema history is a documented gap). Its bootstrap must be the room's Kafka cluster
// (resolveFacts checks it against the Kafka CR's listeners), and its topic is
// under the connector's topic.prefix. A TLS listener needs the clients'
// stores: only the ones Strimzi itself writes in the Connect pod
// (strimziTrustStore, strimziKeyStore) with the store password Strimzi keeps
// in the pod's environment (strimziStorePassword).
var schemaHistoryKeys = []string{
	"schema.history.internal.kafka.topic", "schema.history.internal.kafka.bootstrap.servers",
	"schema.history.internal.store.only.captured.tables.ddl", "schema.history.internal.store.only.captured.databases.ddl",
	"schema.history.internal.skip.unparseable.ddl",
	"schema.history.internal.producer.security.protocol", "schema.history.internal.consumer.security.protocol",
	"schema.history.internal.producer.sasl.mechanism", "schema.history.internal.consumer.sasl.mechanism",
	"schema.history.internal.producer.ssl.truststore.location", "schema.history.internal.consumer.ssl.truststore.location",
	"schema.history.internal.producer.ssl.truststore.type", "schema.history.internal.consumer.ssl.truststore.type",
	"schema.history.internal.producer.ssl.truststore.password", "schema.history.internal.consumer.ssl.truststore.password",
	"schema.history.internal.producer.ssl.keystore.location", "schema.history.internal.consumer.ssl.keystore.location",
	"schema.history.internal.producer.ssl.keystore.type", "schema.history.internal.consumer.ssl.keystore.type",
	"schema.history.internal.producer.ssl.keystore.password", "schema.history.internal.consumer.ssl.keystore.password",
}

// The stores Strimzi writes in every Connect pod when spec.tls or TLS client
// authentication is set (kafka_connect_tls_prepare_certificates.sh: the
// cluster CA as a PKCS12 truststore, the KafkaUser's certificate and key as
// a PKCS12 keystore), and the reference to the random store password it
// keeps in the pod's environment through its own strimzienv provider (the
// worker's config.providers always carries it). These are the only store
// paths and the only non-secrets reference a connector may name.
const (
	strimziTrustStore    = "/tmp/kafka/cluster.truststore.p12"
	strimziKeyStore      = "/tmp/kafka/cluster.keystore.p12"
	strimziStorePassword = "${strimzienv:CERTS_STORE_PASSWORD}"
)

// schemaHistoryTLS is the fixed value, or the value set, of each schema
// history client's TLS key (producer and consumer alike).
var schemaHistoryTLS = map[string][]string{
	"security.protocol":       {"SSL", "SASL_SSL", "PLAINTEXT", "SASL_PLAINTEXT"},
	"ssl.truststore.location": {strimziTrustStore},
	"ssl.keystore.location":   {strimziKeyStore},
	"ssl.truststore.type":     {"PKCS12", "JKS"},
	"ssl.keystore.type":       {"PKCS12", "JKS"},
}

// historyClientKey is the client key of a schema history producer or
// consumer key ("ssl.truststore.location"), or "".
func historyClientKey(k string) string {
	for _, c := range []string{"schema.history.internal.producer.", "schema.history.internal.consumer."} {
		if rest, ok := strings.CutPrefix(k, c); ok {
			return rest
		}
	}
	return ""
}

// storePasswordKey reports whether k is a schema history client's store
// password, the one place strimziStorePassword may appear.
func storePasswordKey(k string) bool {
	switch historyClientKey(k) {
	case "ssl.truststore.password", "ssl.keystore.password":
		return true
	}
	return false
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

// jdbcClassKeys are the JDBC sink keys whose value is a class Connect loads:
// only the sink's own classes, never one a config names.
var jdbcClassKeys = map[string][]string{
	"connection.provider": {"org.hibernate.c3p0.internal.C3P0ConnectionProvider"},
	"collection.naming.strategy": {"io.debezium.sink.naming.DefaultCollectionNamingStrategy",
		"io.debezium.connector.jdbc.naming.DefaultCollectionNamingStrategy", "io.debezium.connector.jdbc.naming.DefaultTableNamingStrategy"},
	"table.naming.strategy": {"io.debezium.connector.jdbc.naming.DefaultTableNamingStrategy",
		"io.debezium.sink.naming.DefaultCollectionNamingStrategy"},
	"column.naming.strategy": {"io.debezium.connector.jdbc.naming.DefaultColumnNamingStrategy",
		"io.debezium.sink.naming.DefaultColumnNamingStrategy"},
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
	{regexp.MustCompile(`(^|\.)(ssl\.(keystore|truststore)\.location|sslrootcert|sslcert|sslkey|sslfactory|socketFactory)$`), "a file path or a class in the Connect pod is never set by a setup (but Strimzi's own stores for the schema history clients)"},
}

// The SMT rules (design §6). A transform runs inside the Connect worker on
// every record, so its class and every key it reads are reviewed here:
// deniedSMTClasses are index SMTs a setup never names, and every other key
// of a transform or predicate must be one of its index entry's keys.
var deniedSMTClasses = map[string]string{
	"io.debezium.transforms.Filter":                                       "a scripting SMT runs code (jsr223, wasm) the validator cannot read",
	"io.debezium.transforms.ContentBasedRouter":                           "a scripting SMT runs code (jsr223, wasm) the validator cannot read",
	"io.debezium.transforms.ByLogicalTableRouter":                         "a router can send records to a topic outside the pipeline's prefix",
	"io.debezium.transforms.outbox.EventRouter":                           "a router can send records to a topic outside the pipeline's prefix",
	"io.debezium.connector.mongodb.transforms.outbox.MongoEventRouter":    "a router can send records to a topic outside the pipeline's prefix",
	"org.apache.kafka.connect.transforms.RegexRouter":                     "a router can send records to a topic outside the pipeline's prefix",
	"org.apache.kafka.connect.transforms.TimestampRouter":                 "a router can send records to a topic outside the pipeline's prefix",
	"io.debezium.connector.postgresql.transforms.timescaledb.TimescaleDb": "it routes records to its own target.topic.prefix",
}

// deniedSMTKeys are index keys of an allowed SMT a setup never sets.
var deniedSMTKeys = map[string]map[string]string{
	"io.debezium.transforms.ExtractNewRecordState": {"route.by.field": "it routes each record to the topic a field names"},
}

// smtValueRE is the shape of a transform or predicate value: no control
// character, bounded. A "$" is fine (a predicate's ^...$, a RegexRouter's
// $1 replacement); a "${" reference is refused for every key outside
// credentialKeys before this rule runs.
var smtValueRE = regexp.MustCompile(`^[^\x00-\x1f]{0,256}$`)

// sinkOnlySMTs are routers a sink may use: on a sink a router only renames
// the topic it maps to a table (the sink writes no topic), so the router
// refusal is a source's. Scripting SMTs stay refused everywhere.
var sinkOnlySMTs = map[string]bool{"org.apache.kafka.connect.transforms.RegexRouter": true}

// smtBaseClass is a class without its $Key or $Value variant.
func smtBaseClass(c string) string {
	if i := strings.IndexByte(c, '$'); i >= 0 {
		return c[:i]
	}
	return c
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
	indexClasses map[string]bool                     // SMT and predicate classes (and their variants)
	indexSMTKeys map[string]map[string]knowledge.Key // SMT or predicate class (and variants) -> its keys
	indexTuning  map[string]map[string]bool          // plugin -> connector-scope tuning keys
	indexJDBC    map[string]bool                     // the JDBC sink entry's keys
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
		indexSMTKeys = map[string]map[string]knowledge.Key{}
		indexTuning = map[string]map[string]bool{}
		indexJDBC = map[string]bool{}
		for _, e := range ix.Entries() {
			switch {
			case (e.Kind == knowledge.KindSMT || e.Kind == knowledge.KindPredicate) && e.Runtime == knowledge.RuntimeConnect && e.Class != "":
				keys := map[string]knowledge.Key{}
				for _, k := range e.Keys {
					keys[k.Name] = k
				}
				if len(e.Variants) == 0 {
					indexClasses[e.Class] = true
					indexSMTKeys[e.Class] = keys
				}
				for _, v := range e.Variants {
					indexClasses[e.Class+"$"+v] = true
					indexSMTKeys[e.Class+"$"+v] = keys
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
func validateConnectorConfig(plugin string, cfg map[string]any, kafka, ns, prefix string) error {
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
		historyClient := historyClientKey(k) != "" && allowedKey(plugin, k)
		for _, r := range refusedKeyRules {
			if r.re.MatchString(k) && !(k == "schema.history.internal.kafka.bootstrap.servers" && allowedKey(plugin, k)) && !historyClient {
				return refuse(path, r.why)
			}
		}
		if historyClient {
			if want, ok := schemaHistoryTLS[historyClientKey(k)]; ok && !containsKey(want, v) {
				return refuse(path, "one of "+strings.Join(want, ", ")+" (Strimzi's own stores in the Connect pod)")
			}
			if storePasswordKey(k) && v != strimziStorePassword {
				return refuse(path, "the store password is "+strimziStorePassword+", the one Strimzi keeps for its own stores")
			}
		}
		if m := transformKeyRE.FindStringSubmatch(k); m != nil {
			if err := checkTransformKey(plugin, cfg, path, m[1], m[2], m[3], v, transforms, predicates); err != nil {
				return err
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
			if !containsFold([]string{"require", "verify-ca", "verify-full"}, v) {
				return refuse(path, "require, verify-ca or verify-full: TLS to the source is never turned off or made optional by a setup")
			}
		case "database.ssl.mode":
			if !containsFold([]string{"required", "verify_ca", "verify_identity"}, v) {
				return refuse(path, "required, verify_ca or verify_identity: TLS to the source is never turned off or made optional by a setup")
			}
		case "database.encrypt":
			if !strings.EqualFold(v, "true") {
				return refuse(path, "true: TLS to the source is never turned off by a setup")
			}
		case "database.trustServerCertificate":
			if !strings.EqualFold(v, "false") {
				return refuse(path, "the source's certificate is always verified")
			}
		case "database.dbname", "database.pdb.name":
			if !dbNameRE.MatchString(v) {
				return refuse(path, "a database name: letters, digits, _ and -, up to 63")
			}
		case "database.names":
			for _, n := range strings.Split(v, ",") {
				if !dbNameRE.MatchString(strings.TrimSpace(n)) {
					return refuse(path, "a comma list of database names: letters, digits, _ and -, up to 63 each")
				}
			}
		case "mongodb.ssl.enabled":
			// false is the plugin's default; the rule refuses only what turns
			// a check off, and this key has none to turn off.
		case "connection.url":
			if credentialsInURL(v) {
				return refuse(path, "credentials belong in the user and password keys as ${secrets:} references, never in the URL")
			}
			if _, err := parseJDBCURL(v); err != nil {
				return refuse(path, err.Error())
			}
		case "mongodb.connection.string":
			if credentialsInURL(v) {
				return refuse(path, "credentials belong in the user and password keys as ${secrets:} references, never in the URL")
			}
			if err := checkMongoParams(v); err != nil {
				return refuse(path, err.Error())
			}
		case "schema.history.internal.kafka.bootstrap.servers":
			if !kafkaBootstrapRE(kafka, ns).MatchString(v) {
				return refuse(path, "the Kafka cluster's bootstrap Service ("+kafka+"-kafka-bootstrap[."+ns+"[.svc[.cluster.local]]]:<port>), as the room's listener reports it")
			}
		case "schema.history.internal.kafka.topic":
			if err := kafkatopics.SchemaHistory(prefix, v); err != nil {
				return refuse(path, err.Error())
			}
		case "errors.deadletterqueue.topic.name":
			ts, _ := asString(cfg["topics"])
			if err := kafkatopics.DeadLetter(prefix, v, ts); err != nil {
				return refuse(path, err.Error())
			}
		case "key.converter.schema.registry.url", "value.converter.schema.registry.url":
			if _, _, err := registryURL(v); err != nil {
				return refuse(path, err.Error())
			}
		}
		if want, ok := jdbcClassKeys[k]; ok && plugin == "jdbc" && !containsKey(want, v) {
			return refuse(path, "only the sink's own class ("+strings.Join(want, ", ")+")")
		}
	}
	return nil
}

var (
	dbNameRE = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_-]{0,62}$`)
)

// checkTransformKey is the rule over one transforms.<name>.<sub> or
// predicates.<name>.<sub> key: the name is declared, the type is an index
// class a setup may name, and every other key is one of that class's index
// keys with a plain value (one of the key's values when the index lists
// them). A reference in a value would copy a credential into the records.
func checkTransformKey(plugin string, cfg map[string]any, path, list, name, sub, v string, transforms, predicates map[string]bool) error {
	set := transforms
	if list == "predicates" {
		set = predicates
	}
	if !set[name] {
		return refuse(path, name+" is not listed in "+list)
	}
	cls, _ := asString(cfg[list+"."+name+".type"])
	if sub != "type" && !smtValueRE.MatchString(v) {
		return refuse(path, "a plain value: no reference, no control character, up to 256")
	}
	if sub == "type" {
		if !indexClasses[v] {
			return refuse(path, "a transform or predicate class from the knowledge index only")
		}
		if why, ok := deniedSMTClasses[smtBaseClass(v)]; ok && !(plugin == "jdbc" && sinkOnlySMTs[v]) {
			return refuse(path, why)
		}
		return nil
	}
	if list == "transforms" {
		switch sub {
		case "predicate":
			if !predicates[v] {
				return refuse(path, "a predicate listed in predicates")
			}
			return nil
		case "negate":
			if v != "true" && v != "false" {
				return refuse(path, "true or false")
			}
			return nil
		}
	}
	if cls == "" || !indexClasses[cls] {
		return refuse(path, "set "+list+"."+name+".type to an index class first")
	}
	key, ok := indexSMTKeys[cls][sub]
	if !ok {
		return refuse(path, "not a key of "+smtBaseClass(cls)+" in the knowledge index")
	}
	if why, ok := deniedSMTKeys[smtBaseClass(cls)][sub]; ok && v != "" {
		return refuse(path, why)
	}
	if len(key.Values) > 0 && !containsKey(key.Values, v) {
		return refuse(path, "one of "+strings.Join(key.Values, ", "))
	}
	return nil
}

// kafkaBootstrapRE is the Kafka cluster's in-cluster bootstrap Service, the
// only bootstrap a KafkaConnect or a schema history client may name (the
// admission policy pins the same form): <kafka>-kafka-bootstrap, optionally
// qualified by the namespace, with a port. An external listener's address
// is refused.
func kafkaBootstrapRE(kafka, ns string) *regexp.Regexp {
	return regexp.MustCompile(`^` + regexp.QuoteMeta(kafka) + `-kafka-bootstrap(\.` + regexp.QuoteMeta(ns) + `(\.svc(\.cluster\.local)?)?)?:[0-9]{1,5}$`)
}

// registryURL is a Schema Registry URL's host and port: http(s)://host[:port]
// with an optional trailing "/", and nothing else (no user, path, query or
// fragment). connectorFacts binds it to a registry target of the room.
func registryURL(s string) (string, string, error) {
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || strings.Contains(s, "%") {
		return "", "", errors.New("a Schema Registry URL http(s)://host[:port] of the room's registry target")
	}
	port := u.Port()
	if port == "" {
		port = map[string]string{"http": "80", "https": "443"}[u.Scheme]
	}
	return strings.ToLower(u.Hostname()), port, nil
}

// jdbcURL is a parsed JDBC sink connection.url.
type jdbcURL struct {
	dialect string
	hosts   []string // host:port, the dialect's default port filled in
	params  map[string]string
}

// jdbcDialects are the JDBC sink's databases: their default port and their
// URL parameter allow-list (lower-cased name -> allowed values, nil meaning a
// plain bounded value). A parameter outside the list is refused: drivers
// load classes and files from parameters (socketFactory, sslfactory,
// autoDeserialize, allowLoadLocalInfile, loggerFile), and the TLS ones are
// pinned to modes that encrypt (the plugin defaults that do not verify are
// a documented gap: the room's targets say nothing of TLS).
var jdbcDialects = map[string]struct {
	port   string
	params map[string][]string
}{
	"postgresql": {"5432", map[string][]string{
		"sslmode": {"require", "verify-ca", "verify-full"}, "ssl": {"true"},
		"connecttimeout": nil, "sockettimeout": nil, "logintimeout": nil, "applicationname": nil, "currentschema": nil,
		"rewritebatchedinserts": {"true", "false"}, "tcpkeepalive": {"true", "false"}, "stringtype": {"unspecified", "varchar"},
		"targetservertype": {"any", "primary", "master", "secondary", "preferSecondary", "preferPrimary"}, "preparethreshold": nil,
	}},
	"mysql": {"3306", map[string][]string{
		"sslmode": {"required", "verify_ca", "verify_identity"}, "usessl": {"true"}, "requiressl": {"true"}, "verifyservercertificate": {"true"},
		"connecttimeout": nil, "sockettimeout": nil, "servertimezone": nil, "characterencoding": nil, "useunicode": {"true", "false"},
		"rewritebatchedstatements": {"true", "false"}, "zerodatetimebehavior": {"EXCEPTION", "ROUND", "CONVERT_TO_NULL", "convertToNull"},
		// true only with a verified TLS mode (parseJDBCURL): the RSA key is
		// then the verified server's.
		"allowpublickeyretrieval": {"true", "false"},
	}},
	"mariadb": {"3306", map[string][]string{
		"sslmode": {"verify-ca", "verify-full"}, "connecttimeout": nil, "sockettimeout": nil, "rewritebatchedstatements": {"true", "false"},
	}},
	// No hostNameInCertificate: it would let the certificate name another
	// host than the one dialed.
	"sqlserver": {"1433", map[string][]string{
		"databasename": nil, "encrypt": {"true", "strict"}, "trustservercertificate": {"false"},
		"logintimeout": nil, "applicationname": nil,
	}},
	"oracle": {"1521", map[string][]string{}},
	"db2":    {"50000", map[string][]string{}},
}

// jdbcLabelParams are the parameters whose value is a free label: a space,
// written as itself or %20, is allowed inside it (never at an end), and no
// other percent escape.
var jdbcLabelParams = map[string]bool{"applicationname": true}

var (
	jdbcValueRE  = regexp.MustCompile(`^[A-Za-z0-9._/+:-]{1,128}$`)
	jdbcLabelRE  = regexp.MustCompile(`^[A-Za-z0-9._/+:-]([A-Za-z0-9._/+: -]{0,126}[A-Za-z0-9._/+:-])?$`)
	oracleNameRE = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
	jdbcHostRE   = regexp.MustCompile(`^(\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9.-]{1,253})(:[0-9]{1,5})?$`)
)

// parseJDBCURL parses a JDBC sink URL of one of jdbcDialects:
// jdbc:<postgresql|mysql|mariadb>://h[:p][,h[:p]]/db[?k=v&...],
// jdbc:sqlserver://h[:p][;k=v;...], jdbc:db2://h[:p]/db, or Oracle thin as
// jdbc:oracle:thin:@[//]h[:p]/service (EZConnect) or @h:p:SID. Every
// parameter is in the dialect's allow-list; a percent escape is refused in
// every name and every value but a label's %20.
func parseJDBCURL(s string) (jdbcURL, error) {
	var u jdbcURL
	rest, ok := strings.CutPrefix(s, "jdbc:")
	if !ok {
		return u, errors.New("a jdbc: URL")
	}
	var hostPart, query string
	sep := "&"
	if strings.ContainsAny(s, " \t\r\n") {
		return u, errors.New("a URL has no whitespace (a label's space is %20)")
	}
	switch {
	case strings.HasPrefix(rest, "oracle:thin:@"):
		u.dialect = "oracle"
		r := strings.TrimPrefix(strings.TrimPrefix(rest, "oracle:thin:@"), "//")
		if strings.ContainsAny(r, "?;()%@") {
			return u, errors.New("an Oracle URL is jdbc:oracle:thin:@[//]host[:port]/service or @host:port:SID, with no parameters")
		}
		var name string
		if h, svc, ok := strings.Cut(r, "/"); ok {
			hostPart, name = h, svc
		} else if i := strings.LastIndex(r, ":"); i > 0 && strings.Count(r, ":") == 2 {
			hostPart, name = r[:i], r[i+1:]
		}
		if !oracleNameRE.MatchString(name) {
			return u, errors.New("an Oracle URL names a service or SID: jdbc:oracle:thin:@[//]host[:port]/service or @host:port:SID")
		}
	case strings.HasPrefix(rest, "sqlserver://"):
		u.dialect = "sqlserver"
		hostPart, query, _ = strings.Cut(strings.TrimPrefix(rest, "sqlserver://"), ";")
		sep = ";"
	default:
		d, r, ok := strings.Cut(rest, "://")
		if _, known := jdbcDialects[d]; !ok || !known || d == "sqlserver" || d == "oracle" {
			return u, errors.New("a PostgreSQL, MySQL, MariaDB, SQL Server, Db2 or Oracle thin JDBC URL")
		}
		u.dialect = d
		var path string
		hostPart, path, _ = strings.Cut(r, "/")
		path, query, _ = strings.Cut(path, "?")
		if path != "" && !dbNameRE.MatchString(path) {
			return u, errors.New("the database in the URL path is a database name")
		}
		if d == "db2" && query != "" {
			return u, errors.New("a Db2 URL is jdbc:db2://host[:port]/database, with no parameters")
		}
	}
	dia := jdbcDialects[u.dialect]
	if hostPart == "" {
		return u, errors.New("the URL names no host")
	}
	for _, h := range strings.Split(hostPart, ",") {
		if !jdbcHostRE.MatchString(h) {
			return u, errors.New("hosts are host[:port] entries")
		}
		if _, _, err := net.SplitHostPort(h); err != nil {
			h = net.JoinHostPort(strings.Trim(h, "[]"), dia.port)
		}
		u.hosts = append(u.hosts, h)
	}
	u.params = map[string]string{}
	if query != "" {
		for _, kv := range strings.Split(query, sep) {
			if kv == "" {
				continue
			}
			k, v, _ := strings.Cut(kv, "=")
			lk := strings.ToLower(k)
			allowed, ok := dia.params[lk]
			if !ok || strings.Contains(k, "%") {
				return u, fmt.Errorf("URL parameter %s is not in the %s allow-list", clipText(k, 40), u.dialect)
			}
			if jdbcLabelParams[lk] {
				if !jdbcLabelRE.MatchString(strings.ReplaceAll(v, "%20", " ")) {
					return u, fmt.Errorf("URL parameter %s: a label (letters, digits, ._/+:- and inner spaces as %%20)", clipText(k, 40))
				}
			} else if !jdbcValueRE.MatchString(v) {
				return u, fmt.Errorf("URL parameter %s: a plain value", clipText(k, 40))
			}
			if allowed != nil && !containsFold(allowed, v) {
				return u, fmt.Errorf("URL parameter %s: one of %s (TLS is never turned off or left unverified where the driver can verify)", clipText(k, 40), strings.Join(allowed, ", "))
			}
			if _, dup := u.params[lk]; dup {
				return u, fmt.Errorf("URL parameter %s is set twice", clipText(k, 40))
			}
			u.params[lk] = v
		}
	}
	if strings.EqualFold(u.params["allowpublickeyretrieval"], "true") && !containsFold([]string{"verify_ca", "verify_identity"}, u.params["sslmode"]) {
		return u, errors.New("allowPublicKeyRetrieval=true needs sslMode=VERIFY_CA or VERIFY_IDENTITY: the key is then the verified server's")
	}
	return u, nil
}

func containsFold(list []string, v string) bool {
	for _, x := range list {
		if strings.EqualFold(x, v) {
			return true
		}
	}
	return false
}

// mongoParams is the mongodb:// option allow-list (lower-cased); nil means a
// plain bounded value. The TLS options only turn TLS on: tls=false,
// tlsInsecure and the tlsAllowInvalid* options are not in the list, and a
// file option (tlsCAFile, tlsCertificateKeyFile) is never a param.
var mongoParams = map[string][]string{
	"replicaset": nil, "authsource": nil, "authmechanism": {"SCRAM-SHA-256", "SCRAM-SHA-1"}, "tls": {"true"}, "ssl": {"true"},
	"retrywrites": {"true", "false"}, "w": nil, "readpreference": nil, "connecttimeoutms": nil, "sockettimeoutms": nil,
	"serverselectiontimeoutms": nil, "maxpoolsize": nil, "appname": nil, "directconnection": {"true", "false"},
}

// checkMongoParams checks the options of a mongodb:// connection string.
func checkMongoParams(s string) error {
	_, q, ok := strings.Cut(s, "?")
	if !ok || q == "" {
		return nil
	}
	for _, kv := range strings.Split(q, "&") {
		if kv == "" {
			continue // a trailing or doubled &
		}
		k, v, _ := strings.Cut(kv, "=")
		allowed, ok := mongoParams[strings.ToLower(k)]
		if !ok {
			return fmt.Errorf("option %s is not in the mongodb option allow-list (TLS is never turned off or left unverified)", clipText(k, 40))
		}
		if !jdbcValueRE.MatchString(v) || (allowed != nil && !containsFold(allowed, v)) {
			return fmt.Errorf("option %s: a plain value from its allow-list", clipText(k, 40))
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
