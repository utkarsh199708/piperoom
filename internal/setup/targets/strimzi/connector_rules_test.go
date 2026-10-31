package strimzi

import (
	"errors"
	"strings"
	"testing"
)

// checkConfigs runs ValidateConnector over each named config (the plugin's
// topic.prefix "pg" added) and wants it accepted or refused with ErrField.
func checkConfigs(t *testing.T, plugin string, ok bool, cases map[string]map[string]any) {
	t.Helper()
	h := newHarness(t, "", nil)
	for name, cfg := range cases {
		full := map[string]any{"topic.prefix": "pg"}
		if plugin == "jdbc" {
			full = map[string]any{"topics": "pg.public.orders"}
		}
		for k, v := range cfg {
			full[k] = v
		}
		err := h.d.ValidateConnector(connectorSpec(plugin, full), testNS)
		switch {
		case ok && err != nil:
			t.Errorf("%s: refused: %v", name, err)
		case !ok && !errors.Is(err, ErrField):
			t.Errorf("%s: err = %v, want ErrField", name, err)
		}
	}
}

// Scripting SMTs and routers are refused whatever their keys; every other
// transform or predicate key is one of its class's index keys, with a plain
// value from the key's values when the index lists them.
func TestTransformRules(t *testing.T) {
	tr := func(cls string, kv ...string) map[string]any {
		m := map[string]any{"transforms": "t", "transforms.t.type": cls}
		for i := 0; i+1 < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return m
	}
	checkConfigs(t, "postgres", true, map[string]map[string]any{
		"unwrap":            tr("io.debezium.transforms.ExtractNewRecordState", "transforms.t.drop.tombstones", "false", "transforms.t.delete.handling.mode", "rewrite"),
		"a variant":         tr("org.apache.kafka.connect.transforms.InsertField$Value", "transforms.t.static.field", "origin", "transforms.t.static.value", "pg"),
		"a predicate":       {"transforms": "t", "transforms.t.type": "org.apache.kafka.connect.transforms.Filter", "transforms.t.predicate": "p", "transforms.t.negate": "true", "predicates": "p", "predicates.p.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches", "predicates.p.pattern": "pg[.].*"},
		"partition routing": tr("io.debezium.transforms.partitions.PartitionRouting", "transforms.t.partition.payload.fields", "id", "transforms.t.partition.topic.num", "3"),
	})
	checkConfigs(t, "postgres", false, map[string]map[string]any{
		"debezium filter":               tr("io.debezium.transforms.Filter", "transforms.t.language", "jsr223.groovy", "transforms.t.condition", "true"),
		"content based router":          tr("io.debezium.transforms.ContentBasedRouter", "transforms.t.language", "jsr223.groovy", "transforms.t.topic.expression", "'evil'"),
		"regex router":                  tr("org.apache.kafka.connect.transforms.RegexRouter", "transforms.t.regex", ".*", "transforms.t.replacement", "evil"),
		"logical table router":          tr("io.debezium.transforms.ByLogicalTableRouter", "transforms.t.topic.regex", ".*", "transforms.t.topic.replacement", "evil"),
		"outbox router":                 tr("io.debezium.transforms.outbox.EventRouter"),
		"timestamp router":              tr("org.apache.kafka.connect.transforms.TimestampRouter", "transforms.t.topic.format", "evil"),
		"timescaledb":                   tr("io.debezium.connector.postgresql.transforms.timescaledb.TimescaleDb"),
		"route by field":                tr("io.debezium.transforms.ExtractNewRecordState", "transforms.t.route.by.field", "table"),
		"a key off the class":           tr("io.debezium.transforms.ExtractNewRecordState", "transforms.t.language", "jsr223.groovy"),
		"a value off its values":        tr("io.debezium.transforms.ExtractNewRecordState", "transforms.t.delete.handling.mode", "evil"),
		"a reference in a value":        tr("org.apache.kafka.connect.transforms.InsertField$Value", "transforms.t.static.field", "x", "transforms.t.static.value", "${secrets:"+testNS+"/orders-db-credentials:password}"),
		"an undeclared predicate":       tr("org.apache.kafka.connect.transforms.Filter", "transforms.t.predicate", "nope"),
		"a key with no type":            {"transforms": "t", "transforms.t.drop.tombstones": "false"},
		"a predicate key off its class": {"predicates": "p", "predicates.p.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches", "predicates.p.evil": "x"},
	})
}

// The JDBC sink's URL: a known dialect, parameters from its allow-list with
// TLS never off, the class keys pinned to the sink's own classes, and a dead
// letter queue under the topics' prefix.
func TestJDBCSinkConfig(t *testing.T) {
	url := func(u string) map[string]any { return map[string]any{"connection.url": u} }
	checkConfigs(t, "jdbc", true, map[string]map[string]any{
		"postgres verify-full":   url("jdbc:postgresql://pg:5432/warehouse?sslmode=verify-full&ApplicationName=piperoom"),
		"postgres no params":     url("jdbc:postgresql://pg/warehouse"),
		"mysql verify":           url("jdbc:mysql://pg:3306/warehouse?sslMode=VERIFY_IDENTITY"),
		"sqlserver encrypted":    url("jdbc:sqlserver://pg:1433;databaseName=warehouse;encrypt=true;trustServerCertificate=false"),
		"oracle thin":            url("jdbc:oracle:thin:@//pg:1521/ORCLPDB1"),
		"the default provider":   {"connection.url": "jdbc:postgresql://pg:5432/w", "connection.provider": "org.hibernate.c3p0.internal.C3P0ConnectionProvider"},
		"the default naming":     {"connection.url": "jdbc:postgresql://pg:5432/w", "collection.naming.strategy": "io.debezium.sink.naming.DefaultCollectionNamingStrategy"},
		"a dlq under the prefix": {"connection.url": "jdbc:postgresql://pg:5432/w", "errors.deadletterqueue.topic.name": "pg.dlq"},
	})
	checkConfigs(t, "jdbc", false, map[string]map[string]any{
		"sslmode disable":           url("jdbc:postgresql://pg:5432/w?sslmode=disable"),
		"sslmode allow":             url("jdbc:postgresql://pg:5432/w?sslmode=allow"),
		"sslmode prefer":            url("jdbc:postgresql://pg:5432/w?sslmode=prefer"),
		"an ssl factory":            url("jdbc:postgresql://pg:5432/w?sslfactory=org.postgresql.ssl.NonValidatingFactory"),
		"a socket factory":          url("jdbc:postgresql://pg:5432/w?socketFactory=com.evil.F"),
		"a logger file":             url("jdbc:postgresql://pg:5432/w?loggerFile=/tmp/x"),
		"mysql sslMode disabled":    url("jdbc:mysql://pg:3306/w?sslMode=DISABLED"),
		"mysql sslMode preferred":   url("jdbc:mysql://pg:3306/w?sslMode=PREFERRED"),
		"mysql useSSL false":        url("jdbc:mysql://pg:3306/w?useSSL=false"),
		"mysql autoDeserialize":     url("jdbc:mysql://pg:3306/w?autoDeserialize=true"),
		"mysql local infile":        url("jdbc:mysql://pg:3306/w?allowLoadLocalInfile=true"),
		"mysql url in local infile": url("jdbc:mysql://pg:3306/w?allowUrlInLocalInfile=true"),
		"sqlserver encrypt false":   url("jdbc:sqlserver://pg:1433;encrypt=false"),
		"sqlserver trusts any cert": url("jdbc:sqlserver://pg:1433;encrypt=true;trustServerCertificate=true"),
		"mariadb trust":             url("jdbc:mariadb://pg:3306/w?sslMode=trust"),
		"a param set twice":         url("jdbc:postgresql://pg:5432/w?sslmode=verify-full&sslmode=require"),
		"an unknown dialect":        url("jdbc:h2:mem:x"),
		"oracle with params":        url("jdbc:oracle:thin:@//pg:1521/s?x=1"),
		"an oracle descriptor":      url("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(HOST=evil)))"),
		"no host":                   url("jdbc:postgresql:///w"),
		"another provider":          {"connection.url": "jdbc:postgresql://pg:5432/w", "connection.provider": "com.evil.Provider"},
		"another naming strategy":   {"connection.url": "jdbc:postgresql://pg:5432/w", "collection.naming.strategy": "com.evil.Naming"},
		"another table strategy":    {"connection.url": "jdbc:postgresql://pg:5432/w", "table.naming.strategy": "com.evil.Naming"},
		"a dlq off the prefix":      {"connection.url": "jdbc:postgresql://pg:5432/w", "errors.deadletterqueue.topic.name": "payments.dlq"},
		"a dlq that is a topic":     {"connection.url": "jdbc:postgresql://pg:5432/w", "errors.deadletterqueue.topic.name": "pg.public.orders"},
	})
}

// A source's TLS stays on and verified where the key can say so, and the
// database names are names.
func TestSourceTLSAndNames(t *testing.T) {
	checkConfigs(t, "sqlserver", false, map[string]map[string]any{
		"trusts any certificate": {"database.trustServerCertificate": "true"},
		"bad database names":     {"database.names": "orders,x;drop"},
	})
	checkConfigs(t, "sqlserver", true, map[string]map[string]any{
		"verified": {"database.encrypt": "true", "database.trustServerCertificate": "false", "database.names": "orders, sales"},
	})
	checkConfigs(t, "postgres", false, map[string]map[string]any{
		"a dbname with a quote": {"database.dbname": "orders'"},
		"a dbname with a space": {"database.dbname": "a b"},
	})
	ms := func(q string) map[string]any {
		return map[string]any{"mongodb.connection.string": "mongodb://pg:27017/?" + q}
	}
	checkConfigs(t, "mongodb", true, map[string]map[string]any{
		"tls on": ms("replicaSet=rs0&tls=true&authSource=admin"),
	})
	checkConfigs(t, "mongodb", false, map[string]map[string]any{
		"tls off":              ms("tls=false"),
		"ssl off":              ms("ssl=false"),
		"tls insecure":         ms("tlsInsecure=true"),
		"invalid certificates": ms("tlsAllowInvalidCertificates=true"),
		"invalid hostnames":    ms("tlsAllowInvalidHostnames=true"),
		"a CA file":            ms("tlsCAFile=/etc/passwd"),
		"an unknown option":    ms("evil=1"),
	})
}

// The schema history clients reach a TLS listener with Strimzi's own stores
// and store password, and nothing else: another path, another password, or
// the Strimzi reference on any other key is refused. The history topic is
// under the connector's topic.prefix.
func TestSchemaHistoryTLS(t *testing.T) {
	base := func(kv ...string) map[string]any {
		m := map[string]any{"database.hostname": "pg",
			"schema.history.internal.kafka.topic":             "pg.history",
			"schema.history.internal.kafka.bootstrap.servers": "prod-kafka-kafka-bootstrap.data.svc:9093"}
		for i := 0; i+1 < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return m
	}
	var tls []string
	for _, c := range []string{"producer", "consumer"} {
		p := "schema.history.internal." + c + "."
		tls = append(tls, p+"security.protocol", "SSL",
			p+"ssl.truststore.location", strimziTrustStore, p+"ssl.truststore.type", "PKCS12", p+"ssl.truststore.password", strimziStorePassword,
			p+"ssl.keystore.location", strimziKeyStore, p+"ssl.keystore.type", "PKCS12", p+"ssl.keystore.password", strimziStorePassword)
	}
	for _, plugin := range []string{"mysql", "sqlserver", "oracle"} {
		checkConfigs(t, plugin, true, map[string]map[string]any{"tls": base(tls...)})
	}
	checkConfigs(t, "mysql", false, map[string]map[string]any{
		"another truststore":        base("schema.history.internal.producer.ssl.truststore.location", "/etc/ssl/evil.p12"),
		"another keystore":          base("schema.history.internal.consumer.ssl.keystore.location", "/opt/kafka/connect-password/x"),
		"a literal store password":  base("schema.history.internal.producer.ssl.keystore.password", "changeit"),
		"another env var":           base("schema.history.internal.producer.ssl.keystore.password", "${strimzienv:KAFKA_CONNECT_SASL_PASSWORD}"),
		"a strimzi ref elsewhere":   base("database.password", strimziStorePassword),
		"a store type":              base("schema.history.internal.producer.ssl.truststore.type", "PEM"),
		"a protocol":                base("schema.history.internal.producer.security.protocol", "EVIL"),
		"a history topic elsewhere": base("schema.history.internal.kafka.topic", "payments.history"),
		"the bare prefix":           base("schema.history.internal.kafka.topic", "pg"),
	})
	checkConfigs(t, "postgres", false, map[string]map[string]any{
		"a store on postgres": {"schema.history.internal.producer.ssl.truststore.location": strimziTrustStore},
	})
}

// The JDBC sink's database is a target of the room, checked at preview and
// write like a source's.
func TestJDBCSinkBoundToRoomTargets(t *testing.T) {
	for u, ok := range map[string]bool{
		"jdbc:postgresql://pg:5432/w?sslmode=verify-full": true,
		"jdbc:postgresql://pg/w":                          true,
		"jdbc:postgresql://evil.example:5432/w":           false,
		"jdbc:postgresql://pg:6432/w":                     false,
		"jdbc:postgresql://pg:5432,evil:5432/w":           false,
	} {
		h := newHarness(t, "", nil)
		withConnectFacts(h)
		h.shell("KafkaConnector", "orders-src", map[string]any{})
		_, err := run(t, h, "k8s_apply", map[string]any{"kind": "KafkaConnector", "template": "connector", "params": map[string]any{
			"connector": "orders-src", "plugin": "jdbc", "config": map[string]string{"connection.url": u, "topics": "pg.public.orders"},
		}})
		if ok != (err == nil) {
			t.Errorf("%s: err = %v, want ok %v", u, err, ok)
		}
		if !ok {
			for _, r := range h.api.requests {
				if strings.HasPrefix(r, "PATCH") {
					t.Errorf("%s: a write reached the API: %s", u, r)
				}
			}
		}
	}
}
