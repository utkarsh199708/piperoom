package strimzi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
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
	ssl := func(k, v string) map[string]any {
		return map[string]any{k: v, "schema.history.internal.producer.security.protocol": "SSL", "schema.history.internal.consumer.security.protocol": "SSL"}
	}
	checkConfigs(t, "mysql", true, map[string]map[string]any{
		"the bootstrap Service":             ssl("schema.history.internal.kafka.bootstrap.servers", "prod-kafka-kafka-bootstrap.data.svc:9093"),
		"the short bootstrap Service":       ssl("schema.history.internal.kafka.bootstrap.servers", "prod-kafka-kafka-bootstrap:9093"),
		"schemahistory.<prefix>":            ssl("schema.history.internal.kafka.topic", "schemahistory.pg"),
		"schemahistory.<prefix>.<database>": ssl("schema.history.internal.kafka.topic", "schemahistory.pg.inventory"),
	})
}

// The JDBC sink's URL parameters from review round 3: the class and file
// loading ones stay refused however they are spelled, and the operational
// ones review asked for pass.
func TestJDBCSinkParamsRound3(t *testing.T) {
	url := func(u string) map[string]any { return map[string]any{"connection.url": u} }
	checkConfigs(t, "jdbc", true, map[string]map[string]any{
		"pg targetServerType":          url("jdbc:postgresql://pg:5432/w?sslmode=verify-full&targetServerType=primary"),
		"pg prepareThreshold":          url("jdbc:postgresql://pg:5432/w?sslmode=verify-full&prepareThreshold=0"),
		"an application name space":    url("jdbc:postgresql://pg:5432/w?sslmode=verify-full&ApplicationName=pipe%20room"),
		"mysql zeroDateTimeBehavior":   url("jdbc:mysql://pg:3306/w?sslMode=VERIFY_IDENTITY&zeroDateTimeBehavior=CONVERT_TO_NULL"),
		"mysql public key with verify": url("jdbc:mysql://pg:3306/w?sslMode=VERIFY_CA&allowPublicKeyRetrieval=true"),
		"oracle ezconnect without //":  url("jdbc:oracle:thin:@pg:1521/ORCLPDB1"),
		"oracle sid":                   url("jdbc:oracle:thin:@pg:1521:ORCL"),
		"db2":                          url("jdbc:db2://pg:50000/WAREHSE"),
		"a digit-led db name":          url("jdbc:postgresql://pg:5432/2024sales?sslmode=verify-full"),
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
			"topic_prefix": "pg", "connector": "orders-src", "plugin": "postgres", "config": map[string]string{
				"database.hostname": "pg", "database.port": "5432", "topic.prefix": "pg", "database.sslmode": "verify-full",
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
		"jdbc:postgresql://warehouse.example:5432/w?sslmode=verify-full": true,
		"jdbc:postgresql://warehouse.example:6432/w?sslmode=verify-full": false,
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

// A user key may reference only the username and a password key only the
// password: Debezium logs a user key's value, so a password behind it would
// reach the pod log (round 4, item 6).
func TestCredentialReferencesMatchTheirKey(t *testing.T) {
	r := func(k string) string { return "${secrets:" + testNS + "/orders-db-credentials:" + k + "}" }
	checkConfigs(t, "postgres", false, map[string]map[string]any{
		"the password behind a user key": {"database.user": r("password")},
		"the username behind a password": {"database.password": r("username")},
		"another secret key":             {"database.password": r("token")},
	})
	checkConfigs(t, "jdbc", false, map[string]map[string]any{
		"the password behind connection.username": {"connection.username": r("password")},
	})
	checkConfigs(t, "postgres", true, map[string]map[string]any{
		"each key its own": {"database.user": r("username"), "database.password": r("password")},
	})
}

// Pod logs mask the credential values the runner wrote and the value of any
// credential key's config line, which the generic redactor leaves alone
// (round 4, item 6). A user key's line is kept (round 6).
func TestPodLogMasksCredentials(t *testing.T) {
	h := newHarness(t, "", nil)
	h.d.rememberWritten([]string{"Xk9pQ2mL7vRt"})
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = strings.Join([]string{
		"2026-10-31 INFO     database.user = Xk9pQ2mL7vRt",
		"     database.password = hunter2-Summer2026!",
		"     connection.password = Tr0ub4dor&3",
		"     connection.username = cdc",
		"     database.password = [hidden]",
		"connected as Xk9pQ2mL7vRt to pg",
		"     database.hostname = pg",
	}, "\n") + "\n"
	res, err := run(t, h, "k8s_logs", map[string]any{"pod": "orders-connect-connect-0"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	for _, leak := range []string{"Xk9pQ2mL7vRt", "hunter2-Summer2026", "Tr0ub4dor"} {
		if strings.Contains(string(b), leak) {
			t.Errorf("the log returns %q: %s", leak, b)
		}
	}
	for _, keep := range []string{"database.hostname = pg", "connection.username = cdc"} {
		if !strings.Contains(string(b), keep) {
			t.Errorf("a plain line was masked, %q lost: %s", keep, b)
		}
	}
}

// A sink's topics and dead letter queue need the pipeline's topic prefix:
// without the topic_prefix param the runner refuses and names it (round 4
// review, correctness 1; the server always sets it).
func TestSinkWithoutTopicPrefixNamesTheParam(t *testing.T) {
	h := newHarness(t, "", nil)
	for name, cfg := range map[string]map[string]any{
		"topics": {"topics": "pg.public.orders", "connection.url": "jdbc:postgresql://pg:5432/w?sslmode=verify-full"},
		"a dlq":  {"errors.deadletterqueue.topic.name": "pg.dlq", "connection.url": "jdbc:postgresql://pg:5432/w?sslmode=verify-full"},
	} {
		err := h.d.ValidateConnector(connectorSpec("jdbc", cfg), testNS, testKafka, "")
		if !errors.Is(err, ErrField) || !strings.Contains(err.Error(), "topic_prefix param") {
			t.Errorf("%s: err = %v, want a refusal naming topic_prefix", name, err)
		}
	}
	checkConfigs(t, "jdbc", false, map[string]map[string]any{
		"a topic off the prefix": {"topics": "orders-connect-connect-configs"},
	})
}

// Every read masks credentials (round 5): a restarted runner reads the
// values back from the db-credentials Secret, the JSON-escaped and
// URL-encoded copies of a value are masked, the line rule masks the whole
// rest of a key=value or key: value line and the value of a JSON
// "key":"value" (quoted, spaced, comma values), a user name is not masked
// (round 6), and k8s_get, k8s_events and strimzi_build_status mask a bare
// written value as k8s_logs does.
func TestReadsMaskCredentials(t *testing.T) {
	h := newHarness(t, "", nil) // a fresh driver: nothing remembered
	secret := `Re/st"rt Pw1`
	sp := coreObjectPath(testNS, "secrets", h.d.dbSecretName())
	h.api.objects[sp] = map[string]any{"metadata": map[string]any{"name": h.d.dbSecretName()}, "data": map[string]any{
		"password": base64.StdEncoding.EncodeToString([]byte(secret)), "username": base64.StdEncoding.EncodeToString([]byte("ab"))}}
	h.api.allow(sp)
	j, _ := json.Marshal(secret)
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = strings.Join([]string{
		"connected with " + secret + " to pg",
		`{"config":{"database.password":` + string(j) + `,"database.hostname":"pg"}}`,
		"jdbc:postgresql://pg/w?password=" + url.QueryEscape(secret),
		"path /" + url.PathEscape(secret),
		`     database.password = "two words, and a comma"`,
		`{"connection.password": "x y"}`,
		"     database.user: ab",
		"     database.password = [hidden]",
		"     database.hostname = pg",
	}, "\n") + "\n"
	res, err := run(t, h, "k8s_logs", map[string]any{"pod": "orders-connect-connect-0"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	for _, leak := range []string{"st\\\"rt", "Re%2Fst", "two words", "and a comma", "x y"} {
		if strings.Contains(string(b), leak) {
			t.Errorf("the log returns %q: %s", leak, b)
		}
	}
	for _, keep := range []string{"database.hostname = pg", "database.user: ab"} {
		if !strings.Contains(string(b), keep) {
			t.Errorf("the log lost %q: %s", keep, b)
		}
	}

	h.api.allowed["events"] = true
	h.api.objects["events"] = map[string]any{"items": []any{
		map[string]any{"type": "Warning", "reason": "Failed", "message": "login failed with " + secret, "lastTimestamp": "2026-10-30T10:00:00Z"},
	}}
	cp := h.shell("KafkaConnect", "orders-connect", map[string]any{})
	h.api.allow(cp)
	h.api.allow("/api/v1/namespaces/data/pods/orders-connect-connect-build")
	obj := h.api.objects[cp]
	obj["status"] = map[string]any{"conditions": []any{map[string]any{"type": "NotReady", "status": "True", "message": "login with " + secret + " refused"}}}
	for op, params := range map[string]map[string]any{
		"k8s_events":           {"involved": "orders-connect-connect-0"},
		"k8s_get":              {"kind": "KafkaConnect", "name": "orders-connect"},
		"strimzi_build_status": {},
	} {
		res, err := run(t, h, op, params)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		if b, _ := json.Marshal(res.Data); strings.Contains(string(b), "st\\\"rt") || strings.Contains(string(b), "Re/st") {
			t.Errorf("%s returns the credential: %s", op, b)
		}
	}
}

// Round 6 masking: Java's URLEncoder form (~ as %7E, space as +) and
// lower-case percent hex, Java's JSON escaping (only " and \ escaped), a
// short password as a whole token only, and a log cut at limitBytes whose
// last line ends inside a credential.
func TestPodLogMasksRound6(t *testing.T) {
	h := newHarness(t, "", nil)
	pw := `Ab~c d<e>&"f`
	h.d.rememberSecret(map[string]string{"password": pw, "pin": "x7q"})
	lines := []string{
		"url password=Ab%7Ec+d%3Ce%3E%26%22f done",
		"url lower Ab%7ec+d%3ce%3e%26%22f done",
		`log {"msg":"login with Ab~c d<e>&\"f failed"}`,
		`FATAL: login with "x7q" refused`,
		"connecting with x7q.",
		"x7qx and ax7q stay",
	}
	// The kept tail is the last 200 lines: the cases come last, after
	// enough filler for the body to reach limitBytes.
	tail := strings.Join(lines, "\n") + "\n" + "a cut line ending with Ab~c d<"
	body := ""
	for len(body)+len(tail) < maxLogBytes {
		body += "filler line of the connect log\n"
	}
	body += tail
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = body
	res, err := run(t, h, "k8s_logs", map[string]any{"pod": "orders-connect-connect-0"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	got := string(b)
	for _, leak := range []string{"Ab%7Ec", "Ab%7ec", `Ab~c`, `"x7q"`, "with x7q.", "a cut line"} {
		if strings.Contains(got, leak) {
			t.Errorf("the log returns %q", leak)
		}
	}
	if !strings.Contains(got, "x7qx and ax7q stay") {
		t.Errorf("a short value was masked inside a longer word: %s", got[len(got)-600:])
	}
}

// A user name is never masked by value, at any length: "debezium" would
// garble io.debezium.connector in every line. A short password is masked
// as a whole token (round 6, A).
func TestUserNameIsNotMaskedByValue(t *testing.T) {
	h := newHarness(t, "", nil)
	h.d.rememberSecret(map[string]string{"username": "debezium", "password": "q9z"})
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = strings.Join([]string{
		"INFO io.debezium.connector.postgresql.PostgresConnectorTask started as debezium",
		"login with q9z refused",
	}, "\n") + "\n"
	res, err := run(t, h, "k8s_logs", map[string]any{"pod": "orders-connect-connect-0"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	got := string(b)
	if !strings.Contains(got, "io.debezium.connector.postgresql.PostgresConnectorTask started as debezium") {
		t.Errorf("the user name was masked: %s", got)
	}
	if strings.Contains(got, "q9z") {
		t.Errorf("the short password is returned: %s", got)
	}
}

// The line rule masks only a known credential key's value (round 6, B): a
// JSON log keeps its other fields and a user key, MySQL's "(using
// password: YES)", a ${secrets:...} reference, a Java frame and a token's
// state stay; a later key on a kept line is still masked.
func TestCredentialLineRule(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{`{"user":"cdc","password":"p w","message":"connector started"}`, `{"user":"cdc","password":"[masked:credential]","message":"connector started"}`},
		{`{"database.password": "a\"b", "level":"INFO"}`, `{"database.password": "[masked:credential]", "level":"INFO"}`},
		{`msg="{\"sasl.jaas.config\":\"org.apache.kafka.common.security.scram.ScramLoginModule required password=x;\",\"k\":\"v\"}"`, `msg="{\"sasl.jaas.config\":\"[masked:credential]\",\"k\":\"v\"}"`},
		{`Access denied for user 'cdc'@'10.0.0.4' (using password: YES)`, ""},
		{`Access denied for user 'cdc'@'10.0.0.4' (using password: NO)`, ""},
		{`database.password = ${secrets:data/orders-db-credentials:password}`, ""},
		{`database.password=${secrets:data/x:password}, ssl.key.password=hunter22`, `database.password=${secrets:data/x:password}, ssl.key.password=[masked:credential]`},
		{`at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1899)`, ""},
		{`Token: expired`, ""},
		{`database.user: cdc`, ""},
		{`     database.password = two words, and a comma`, `     database.password = [masked:credential]`},
		{`client_secret: abc`, `client_secret: [masked:credential]`},
		{`passwordless login is off`, ""},
	} {
		want := c.want
		if want == "" {
			want = c.in
		}
		if got := maskCredentials(c.in, nil); got != want {
			t.Errorf("maskCredentials(%q)\n got %q\nwant %q", c.in, got, want)
		}
	}
}
