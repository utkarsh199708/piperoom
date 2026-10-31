package strimzi

import (
	"errors"
	"testing"

	"piperoom/internal/events"
)

// Review round 3: values the runner refuses whatever key they sit in, and
// the keys a secret reference may sit in.
func TestConnectorValueShape(t *testing.T) {
	ref := "${secrets:" + testNS + "/orders-db-credentials:password}"
	checkConfigs(t, "postgres", false, map[string]map[string]any{
		// Whitespace at either end is refused on every key: a plugin that
		// trims would read " disable" as disable.
		"a leading space":           {"database.sslmode": " disable"},
		"a trailing space":          {"database.sslmode": "require "},
		"a free value with a space": {"slot.name": "s "},
		"a key with a space":        {"slot.name ": "s"},
		"a class with a space":      {"transforms": "t", "transforms.t.type": " io.debezium.transforms.ExtractNewRecordState"},
		"a converter with a space":  {"key.converter": "org.apache.kafka.connect.json.JsonConverter "},
		// References sit in credential keys only, and are the exact form.
		"a reference in the hostname":  {"database.hostname": ref},
		"a reference in a free key":    {"slot.name": ref},
		"a reference in a transform":   {"transforms": "t", "transforms.t.type": "org.apache.kafka.connect.transforms.InsertField$Value", "transforms.t.static.field": "x", "transforms.t.static.value": ref},
		"a reference with a suffix":    {"database.password": ref + "x"},
		"a reference with a prefix":    {"database.password": "x" + ref},
		"another provider in a cred":   {"database.password": "${file:/etc/passwd:x}"},
		"a reference in the user name": {"database.user": "${secrets:" + testNS + "/other:user}"},
	})
	checkConfigs(t, "postgres", true, map[string]map[string]any{
		"exact references": {"database.user": "${secrets:" + testNS + "/orders-db-credentials:username}", "database.password": ref},
	})
	checkConfigs(t, "mysql", false, map[string]map[string]any{
		"a reference in the history bootstrap": {"schema.history.internal.kafka.bootstrap.servers": ref},
		"a history bootstrap elsewhere":        {"schema.history.internal.kafka.bootstrap.servers": "evil.example:9093"},
		"another cluster's bootstrap":          {"schema.history.internal.kafka.bootstrap.servers": "other-kafka-bootstrap.data.svc:9093"},
		"another namespace's bootstrap":        {"schema.history.internal.kafka.bootstrap.servers": "prod-kafka-kafka-bootstrap.evil.svc:9093"},
		"a bootstrap list":                     {"schema.history.internal.kafka.bootstrap.servers": "prod-kafka-kafka-bootstrap.data.svc:9093,evil:9093"},
		"a jaas config":                        {"schema.history.internal.producer.sasl.jaas.config": "x"},
	})
	checkConfigs(t, "mysql", true, map[string]map[string]any{
		"the bootstrap Service":             {"schema.history.internal.kafka.bootstrap.servers": "prod-kafka-kafka-bootstrap.data.svc:9093"},
		"the short bootstrap Service":       {"schema.history.internal.kafka.bootstrap.servers": "prod-kafka-kafka-bootstrap:9093"},
		"schemahistory.<prefix>":            {"schema.history.internal.kafka.topic": "schemahistory.pg"},
		"schemahistory.<prefix>.<database>": {"schema.history.internal.kafka.topic": "schemahistory.pg.inventory"},
	})
}

// The JDBC sink's URL parameters from review round 3: the class and file
// loading ones stay refused however they are spelled, and the operational
// ones review asked for pass.
func TestJDBCSinkParamsRound3(t *testing.T) {
	url := func(u string) map[string]any { return map[string]any{"connection.url": u} }
	checkConfigs(t, "jdbc", true, map[string]map[string]any{
		"pg targetServerType":          url("jdbc:postgresql://pg:5432/w?sslmode=verify-full&targetServerType=primary"),
		"pg prepareThreshold":          url("jdbc:postgresql://pg:5432/w?prepareThreshold=0"),
		"an application name space":    url("jdbc:postgresql://pg:5432/w?ApplicationName=pipe%20room"),
		"mysql zeroDateTimeBehavior":   url("jdbc:mysql://pg:3306/w?sslMode=VERIFY_IDENTITY&zeroDateTimeBehavior=CONVERT_TO_NULL"),
		"mysql public key with verify": url("jdbc:mysql://pg:3306/w?sslMode=VERIFY_CA&allowPublicKeyRetrieval=true"),
		"oracle ezconnect without //":  url("jdbc:oracle:thin:@pg:1521/ORCLPDB1"),
		"oracle sid":                   url("jdbc:oracle:thin:@pg:1521:ORCL"),
		"db2":                          url("jdbc:db2://pg:50000/WAREHSE"),
		"a digit-led db name":          url("jdbc:postgresql://pg:5432/2024sales"),
	})
	checkConfigs(t, "jdbc", false, map[string]map[string]any{
		"a percent-encoded param name":    url("jdbc:mysql://pg:3306/w?allow%4CoadLocalInfile=true"),
		"a percent in a value":            url("jdbc:postgresql://pg:5432/w?ApplicationName=a%2Fb"),
		"a percent in a non-label value":  url("jdbc:postgresql://pg:5432/w?targetServerType=primary%20"),
		"a space in the url":              url("jdbc:postgresql://pg:5432/w?sslmode=verify-full &x=1"),
		"sqlserver hostNameInCertificate": url("jdbc:sqlserver://pg:1433;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.evil"),
		"mysql public key without verify": url("jdbc:mysql://pg:3306/w?sslMode=REQUIRED&allowPublicKeyRetrieval=true"),
		"mysql public key no sslMode":     url("jdbc:mysql://pg:3306/w?allowPublicKeyRetrieval=true"),
		"pg ssl password callback":        url("jdbc:postgresql://pg:5432/w?sslpasswordcallback=com.evil.C"),
		"pg ssl hostname verifier":        url("jdbc:postgresql://pg:5432/w?sslhostnameverifier=com.evil.V"),
		"pg auth plugin class":            url("jdbc:postgresql://pg:5432/w?authenticationPluginClassName=com.evil.A"),
		"db2 with params":                 url("jdbc:db2://pg:50000/W:securityMechanism=9;"),
		"oracle sid with a slash":         url("jdbc:oracle:thin:@pg:1521:OR/CL"),
	})
	checkConfigs(t, "mongodb", true, map[string]map[string]any{
		"a trailing &": {"mongodb.connection.string": "mongodb://pg:27017/?tls=true&"},
	})
	checkConfigs(t, "postgres", true, map[string]map[string]any{
		"a digit-led dbname": {"database.dbname": "2024sales"},
	})
}

// RegexRouter renames a sink's topics into table names, which is how a
// JDBC sink maps <prefix>.<schema>.<table> to <table>: allowed there, still
// refused on sources (which would route records off the prefix).
func TestRegexRouterOnSinksOnly(t *testing.T) {
	rr := map[string]any{"transforms": "route", "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
		"transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)", "transforms.route.replacement": "$3"}
	checkConfigs(t, "jdbc", true, map[string]map[string]any{"regex router": rr})
	checkConfigs(t, "postgres", false, map[string]map[string]any{"regex router": rr})
	checkConfigs(t, "jdbc", false, map[string]map[string]any{
		"a reference in the replacement": {"transforms": "route", "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
			"transforms.route.regex": ".*", "transforms.route.replacement": "${secrets:" + testNS + "/orders-db-credentials:password}"},
	})
	checkConfigs(t, "postgres", true, map[string]map[string]any{
		"a $ in a predicate pattern": {"transforms": "t", "transforms.t.type": "org.apache.kafka.connect.transforms.Filter", "transforms.t.predicate": "p",
			"predicates": "p", "predicates.p.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches", "predicates.p.pattern": "pg[.]public[.]orders$"},
	})
}

// The Schema Registry URL keys name the room's registry target (L4).
func TestRegistryURLBoundToTheRoom(t *testing.T) {
	checkConfigs(t, "postgres", false, map[string]map[string]any{
		"a path":        {"value.converter.schema.registry.url": "http://registry.data.svc:8081/x"},
		"a query":       {"value.converter.schema.registry.url": "http://registry.data.svc:8081?x=1"},
		"user info":     {"value.converter.schema.registry.url": "http://u:p@registry.data.svc:8081"},
		"a file scheme": {"value.converter.schema.registry.url": "file:///etc/passwd"},
		"a list":        {"value.converter.schema.registry.url": "http://registry.data.svc:8081,http://evil:8081"},
		"percent":       {"value.converter.schema.registry.url": "http://registry%2edata.svc:8081"},
	})
	for _, c := range []struct {
		name, url      string
		withTarget, ok bool
	}{
		{"the room's registry", "http://registry.data.svc:8081", true, true},
		{"the room's registry, no target", "http://registry.data.svc:8081", false, false},
		{"another host", "http://169.254.169.254:80", true, false},
		{"another port", "http://registry.data.svc:8082", true, false},
	} {
		h := newHarness(t, "", nil)
		if c.withTarget {
			h.env.Targets = append(h.env.Targets, events.SetupTarget{Kind: targetSchemaRegistry, Address: "registry.data.svc:8081"})
		}
		h.shell("KafkaConnector", "orders-src", map[string]any{})
		_, err := run(t, h, "k8s_apply", map[string]any{"kind": "KafkaConnector", "template": "connector", "params": map[string]any{
			"connector": "orders-src", "plugin": "postgres", "config": map[string]string{
				"database.hostname": "pg", "database.port": "5432", "topic.prefix": "pg",
				"value.converter": "io.confluent.connect.avro.AvroConverter", "value.converter.schema.registry.url": c.url},
		}})
		if c.ok != (err == nil) {
			t.Errorf("%s: err = %v, want ok %v", c.name, err, c.ok)
		}
		if !c.ok && err != nil && !errors.Is(err, ErrTarget) && !errors.Is(err, ErrField) {
			t.Errorf("%s: err = %v, want ErrTarget or ErrField", c.name, err)
		}
	}
}

// A JDBC sink may write to any database target of the room, or a host
// target, not only the source (correctness 3).
func TestJDBCSinkToAHostTarget(t *testing.T) {
	h := newHarness(t, "", nil)
	h.env.Targets = append(h.env.Targets, events.SetupTarget{Kind: targetHost, Address: "warehouse.example:5432"})
	h.shell("KafkaConnector", "orders-src", map[string]any{})
	for u, ok := range map[string]bool{
		"jdbc:postgresql://warehouse.example:5432/w": true,
		"jdbc:postgresql://warehouse.example:6432/w": false,
	} {
		_, err := run(t, h, "k8s_apply", map[string]any{"kind": "KafkaConnector", "template": "connector", "params": map[string]any{
			"connector": "orders-src", "plugin": "jdbc", "topic_prefix": "pg", "config": map[string]string{"connection.url": u, "topics": "pg.public.orders"},
		}})
		if ok != (err == nil) {
			t.Errorf("%s: err = %v, want ok %v", u, err, ok)
		}
	}
}

// The worker bootstrap is the Kafka cluster's bootstrap Service, the same
// rule as the schema history bootstrap and the admission policy (correctness 8).
func TestConnectBootstrapIsTheService(t *testing.T) {
	h := newHarness(t, "", nil)
	for b, ok := range map[string]bool{
		"prod-kafka-kafka-bootstrap.data.svc:9093":               true,
		"prod-kafka-kafka-bootstrap.data.svc.cluster.local:9093": true,
		"prod-kafka-kafka-bootstrap:9093":                        true,
		"prod-kafka-kafka-bootstrap.data.svc.evil.example:9093":  false,
		"prod-kafka-kafka-bootstrap.other.svc:9093":              false,
		"evil.example:9093":                                      false,
		"prod-kafka-kafka-bootstrap.data.svc:9093,evil:9093":     false,
		" prod-kafka-kafka-bootstrap.data.svc:9093":              false,
	} {
		err := h.d.ValidateConnect(map[string]any{"bootstrapServers": b}, testKafka, testNS)
		if ok != (err == nil) {
			t.Errorf("%q: err = %v, want ok %v", b, err, ok)
		}
	}
	for v, ok := range map[string]bool{
		"org.apache.kafka.connect.json.JsonConverter":  true,
		" org.apache.kafka.connect.json.JsonConverter": false,
		"${secrets:data/x:y}":                          false,
	} {
		err := h.d.ValidateConnect(map[string]any{"config": map[string]any{"key.converter": v}}, testKafka, testNS)
		if ok != (err == nil) {
			t.Errorf("worker converter %q: err = %v, want ok %v", v, err, ok)
		}
	}
	if err := h.d.ValidateConnect(map[string]any{"config": map[string]any{"header.converter": "com.evil.H"}}, testKafka, testNS); err == nil {
		t.Error("header.converter accepted")
	}
}

// ExtractNewRecordState's drop.fields keys are in the knowledge index, so a
// setup may name them (correctness 6).
func TestUnwrapDropFieldsKeys(t *testing.T) {
	checkConfigs(t, "postgres", true, map[string]map[string]any{
		"drop fields": {"transforms": "unwrap", "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
			"transforms.unwrap.drop.fields.header.name": "drop", "transforms.unwrap.drop.fields.from.key": "true",
			"transforms.unwrap.drop.fields.keep.schema.compatible": "false"},
	})
}
