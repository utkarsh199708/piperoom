package strimzi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/redact"
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
// lower-case percent hex, Java's JSON escaping (only " and \ escaped), and
// a log cut at limitBytes whose last line ends inside a credential.
func TestPodLogMasksRound6(t *testing.T) {
	h := newHarness(t, "", nil)
	pw := `Ab~c d<e>&"f`
	h.d.rememberSecret(map[string]string{"password": pw})
	lines := []string{
		"url password=Ab%7Ec+d%3Ce%3E%26%22f done",
		"url lower Ab%7ec+d%3ce%3e%26%22f done",
		`log {"msg":"login with Ab~c d<e>&\"f failed"}`,
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
	for _, leak := range []string{"Ab%7Ec", "Ab%7ec", `Ab~c`, "a cut line"} {
		if strings.Contains(got, leak) {
			t.Errorf("the log returns %q", leak)
		}
	}
}

// A user name is never masked by value, at any length: "debezium" would
// garble io.debezium.connector in every line (round 6, A).
func TestUserNameIsNotMaskedByValue(t *testing.T) {
	h := newHarness(t, "", nil)
	h.d.rememberSecret(map[string]string{"username": "debezium", "password": "q9zLongPw8"})
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = strings.Join([]string{
		"INFO io.debezium.connector.postgresql.PostgresConnectorTask started as debezium",
		"login with q9zLongPw8 refused",
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
	if strings.Contains(got, "q9zLongPw8") {
		t.Errorf("the password is returned: %s", got)
	}
}

// The line rule masks only a credential key's value (round 6, B): a JSON
// log keeps its other fields and a user key, MySQL's "(using password:
// YES)", a whole ${secrets:...} reference and a Java frame stay. Since round
// 7 a token key is a credential key again and an exemption holds only for
// the exact whole value; since round 8 a whole value of expired (Token:
// expired) is kept, as null, none, failed and rejected are.
func TestCredentialLineRule(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{`{"user":"cdc","password":"p w","message":"connector started"}`, `{"user":"cdc","password":"[masked:credential]","message":"connector started"}`},
		{`{"database.password": "a\"b", "level":"INFO"}`, `{"database.password": "[masked:credential]", "level":"INFO"}`},
		{`msg="{\"sasl.jaas.config\":\"org.apache.kafka.common.security.scram.ScramLoginModule required password=x;\",\"k\":\"v\"}"`, `msg="{\"sasl.jaas.config\":\"[masked:credential]\",\"k\":\"v\"}"`},
		{`Access denied for user 'cdc'@'10.0.0.4' (using password: YES)`, ""},
		{`Access denied for user 'cdc'@'10.0.0.4' (using password: NO)`, ""},
		{`database.password = ${secrets:data/orders-db-credentials:password}`, ""},
		{`database.password=${secrets:data/x:password}, ssl.key.password=hunter22`, `database.password=${secrets:data/x:password}, ssl.key.password=[masked:credential]`}, // round 11: the scan goes on after a kept value
		{`at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1899)`, ""},
		{`Token: expired`, ""}, // round 8: a whole "expired" says only the token's state
		{`database.user: cdc`, ""},
		{`     database.password = two words, and a comma`, `     database.password = [masked:credential]`},
		{`client_secret: abc`, `client_secret: [masked:credential]`},
		{`passwordless login is off`, ""},
	} {
		want := c.want
		if want == "" {
			want = c.in
		}
		if got := maskOwn(c.in, nil); got != want {
			t.Errorf("maskOwn(%q)\n got %q\nwant %q", c.in, got, want)
		}
	}
}

// pgjdbc parses targetServerType with an enum's valueOf, so its value must
// be the exact camelCase spelling; useUnicode is not a Connector/J 8 or 9
// property (round 6, D and F).
func TestJDBCParamsRound6(t *testing.T) {
	url := func(u string) map[string]any { return map[string]any{"connection.url": u} }
	pg := "jdbc:postgresql://pg:5432/w?sslmode=verify-full&targetServerType="
	checkConfigs(t, "jdbc", true, map[string]map[string]any{
		"preferSecondary": url(pg + "preferSecondary"),
		"primary":         url(pg + "primary"),
	})
	checkConfigs(t, "jdbc", false, map[string]map[string]any{
		"PRIMARY":                url(pg + "PRIMARY"),
		"prefersecondary":        url(pg + "prefersecondary"),
		"mysql useUnicode":       url("jdbc:mysql://pg:3306/w?sslMode=VERIFY_IDENTITY&useUnicode=true"),
		"mysql useUnicode false": url("jdbc:mysql://pg:3306/w?sslMode=VERIFY_IDENTITY&useUnicode=false"),
	})
}

// Round 7 masking (security re-review of round 6): every form and decoded
// copy of a written value, no value masking under 4 characters (C5),
// token-family and other credential keys, non-string JSON values, exact
// exemptions, and no masking of the runner's Secret name or PWD= (C4).
func TestMaskCredentialsRound7(t *testing.T) {
	written := []string{"a+b!c#d", "ab%1", "ab!xy", "uni\u00e9code", "1", "ab"}
	for _, c := range []struct{ in, want string }{
		// 1: a path-escaped value with a literal +.
		{"path a+b%21c%23d end", "path [masked:credential] end"},
		{"path a%2Bb%21c%23d end", "path [masked:credential] end"},
		// 2: a stray % ahead of an encoded value.
		{"pct %ab%21xy end", "pct %[masked:credential] end"},
		{"x ab%1ab%251 y", "x [masked:credential] y"}, // adjacent masks merge (round 9)
		// Round 8, S5: a value under 4 characters (read back from an old
		// Secret) is masked as a whole token only.
		{"task 1 of 3 at ab", "task [masked:credential] of 3 at [masked:credential]"},
		{"task 12 of 31 at abc", "task 12 of 31 at abc"},
		{"login with uni\u00e9code and uni%C3%A9code", "login with [masked:credential] and [masked:credential]"},
	} {
		if got := maskOwn(c.in, written); got != c.want {
			t.Errorf("maskOwn(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
	}
	for _, c := range []struct{ in, want string }{
		// 4: token and the other credential keys.
		{"token=zz1", "token=[masked:credential]"},
		{"refresh_token: zz1", "refresh_token: [masked:credential]"},
		{"X-Auth-Token: zz1", "X-Auth-Token: [masked:credential]"},
		{"access_token=zz1", "access_token=[masked:credential]"},
		{"sessionToken=zz1", "sessionToken=[masked:credential]"},
		{"aws_session_token = zz1", "aws_session_token = [masked:credential]"},
		{"api_key=zz1 apikey=zz2", "api_key=[masked:credential]"},
		{"x-api-key: zz1", "x-api-key: [masked:credential]"},
		{"Authorization: Basic zz1", "Authorization: [masked:credential]"},
		{"bearer: zz1", "bearer: [masked:credential]"},
		{"credentials=zz1", "credentials=[masked:credential]"},
		{"private_key: zz1", "private_key: [masked:credential]"},
		{"privateKey: zz1", "privateKey: [masked:credential]"},
		{"database.user=cdc username: cdc", "database.user=cdc username: cdc"},
		{"tokenizer=standard", "tokenizer=[masked:credential]"}, // round 10: an exempt name keeps only a boolean, number, none or null
		// 5: a credential key's non-string JSON value.
		{`{"password": ["hunter2xyz"], "m":"ok"}`, `{"password": [masked:credential], "m":"ok"}`},
		{`{"password" : 12345678, "m":"ok"}`, `{"password" : [masked:credential], "m":"ok"}`},
		{`{"token":{"v":"zz1"},"m":"ok"}`, `{"token":[masked:credential],"m":"ok"}`},
		{`m="{\"password\":12345678}"`, `m="{\"password\":[masked:credential]}"`},
		// 8: an exemption holds only for the exact whole value.
		{"password: YES)hunter2xyz", "password: [masked:credential]"},
		{"(using password: YES)", "(using password: YES)"},
		{"(using password: NO) at login", "(using password: [masked:credential]"}, // round 9, F3: only a whole YES)/NO) is kept
		{"password=${a}hunter2xyz", "password=[masked:credential]"},
		{"password=${secrets:data/x:password} hunter2xyz", "password=[masked:credential]"},
		{"password = [hidden]zz1", "password = [masked:credential]"},
		{"password = [hidden]", "password = [hidden]"},
		// C4: the runner's own Secret object's name, and the shell's PWD.
		{"secret: orders-db-credentials", "secret: orders-db-credentials"},
		{`{"secret":"orders-db-credentials","m":"ok"}`, `{"secret":"orders-db-credentials","m":"ok"}`},
		{"secret: orders-db-credentials2", "secret: [masked:credential]"},
		{"PWD=/opt/kafka", "PWD=/opt/kafka"},
		{"pwd=zz1 dbPwd=zz2", "pwd=[masked:credential]"},
		{"dbPwd=zz2", "dbPwd=[masked:credential]"},
	} {
		if got := maskOwn(c.in, nil); got != c.want {
			t.Errorf("maskOwn(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}

// A redacted token straddling a line's 1000th byte is redacted before the
// line is clipped (round 7, 3), and a last line without a newline is
// dropped even when the body is under maxLogBytes (round 7, 7).
func TestPodLogRound7(t *testing.T) {
	h := newHarness(t, "", nil)
	h.d.rememberWritten([]string{"hunter2xyz"})
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = strings.Repeat("x", 990) + " " + fakeKeyID + " tail\n" +
		"a cut line ending with hunter2"
	res, err := run(t, h, "k8s_logs", map[string]any{"pod": "orders-connect-connect-0"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	got := string(b)
	if strings.Contains(got, "AKIAABCD") {
		t.Errorf("the clipped line keeps the key's prefix: %s", got[len(got)-200:])
	}
	if strings.Contains(got, "a cut line") {
		t.Errorf("the partial last line is returned: %s", got[len(got)-200:])
	}
}

// The whole pod log is redacted before the credential masks lengthen it
// (round 7, C1): a PEM private key early in a ~51 KB log whose later lines
// hold many masked values came back with its base64 body when the redact
// window, chosen after the masks, started inside the block.
func TestPodLogRedactsAPEMBlockBeforeTheMasks(t *testing.T) {
	if maxLogBytes > redact.MaxScan {
		t.Fatalf("maxLogBytes %d > redact.MaxScan %d: a whole log no longer fits one redact scan", maxLogBytes, redact.MaxScan)
	}
	h := newHarness(t, "", nil)
	pw := "Wq7xPw"
	h.d.rememberWritten([]string{pw})
	var pem []string
	pem = append(pem, "-----BEGIN PRIVATE KEY-----")
	for i := 0; i < 26; i++ {
		pem = append(pem, base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("private key body line %02d of the test key, 48 bytes", i)))[:64])
	}
	pem = append(pem, "-----END PRIVATE KEY-----")
	head := "2026-10-31 INFO starting the connect worker with its key material from the mounted file\n"
	block := strings.Join(pem, "\n") + "\n"
	line := "2026-10-31 WARN task retry" + strings.Repeat(" login "+pw+" refused, retrying", 6) + strings.Repeat(".", 120) + "\n"
	masked := len(line) + 6*(len("[masked:credential]")-len(pw))
	// Enough masked lines that the masked tail alone nearly fills the old
	// window (redact.MaxScan-1024), so it started inside the PEM block.
	n := 0
	for (n+1)*masked < redact.MaxScan-1024-len(block)/2 {
		n++
	}
	body := head + block + strings.Repeat(line, n)
	if len(body) > maxLogBytes || strings.Count(body, "\n") >= maxLogLines {
		t.Fatalf("the test log is %d bytes, %d lines: over the caps", len(body), strings.Count(body, "\n"))
	}
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = body
	res, err := run(t, h, "k8s_logs", map[string]any{"pod": "orders-connect-connect-0"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	got := string(b)
	for _, l := range pem[1 : len(pem)-1] {
		if strings.Contains(got, l[:24]) {
			t.Fatalf("the PEM body is returned (%d-byte log, %d lines): %q", len(body), strings.Count(body, "\n"), l)
		}
	}
	if strings.Contains(got, pw) {
		t.Errorf("the password is returned")
	}
}

// Written values are masked before the whole log is redacted, without
// lengthening it (round 8): masks that lengthened a log near maxLogBytes
// pushed its tail past redact's scan limit, which masks it whole. The PEM
// block in it is still redacted.
func TestPodLogMasksWithoutLengthening(t *testing.T) {
	if maxLogBytes > redact.MaxScan {
		t.Fatalf("maxLogBytes %d > redact.MaxScan %d: a whole log no longer fits one redact scan", maxLogBytes, redact.MaxScan)
	}
	h := newHarness(t, "", nil)
	pw := "Wq7xPw"
	h.d.rememberWritten([]string{pw})
	var pem []string
	pem = append(pem, "-----BEGIN PRIVATE KEY-----")
	for i := 0; i < 26; i++ {
		pem = append(pem, base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("private key body line %02d of the test key, 48 bytes", i)))[:64])
	}
	pem = append(pem, "-----END PRIVATE KEY-----")
	head := "2026-10-31 INFO starting the connect worker with its key material from the mounted file\n"
	block := strings.Join(pem, "\n") + "\n"
	line := "2026-10-31 WARN task retry" + strings.Repeat(" login "+pw+" refused, retrying", 6) + strings.Repeat(".", 400) + "\n"
	masked := len(line) + 6*(len("[masked:credential]")-len(pw))
	// Enough lines that the body is within maxLogBytes but would pass
	// redact.MaxScan if the masks lengthened it before redact (round 8),
	// and, with the masks after redact, that the old window (the last
	// redact.MaxScan-1024 bytes of the masked text) started inside the
	// PEM block (round 7, C1).
	n := (maxLogBytes - len(head) - len(block)) / len(line)
	body := head + block + strings.Repeat(line, n)
	if len(body) > maxLogBytes || strings.Count(body, "\n") >= maxLogLines || len(head)+len(block)+n*masked <= redact.MaxScan {
		t.Fatalf("the test log is %d bytes, %d lines, %d masked: not the shape", len(body), strings.Count(body, "\n"), len(head)+len(block)+n*masked)
	}
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = body
	res, err := run(t, h, "k8s_logs", map[string]any{"pod": "orders-connect-connect-0"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	got := string(b)
	for _, l := range pem[1 : len(pem)-1] {
		if strings.Contains(got, l[:24]) {
			t.Fatalf("the PEM body is returned (%d-byte log, %d lines): %q", len(body), strings.Count(body, "\n"), l)
		}
	}
	if strings.Contains(got, pw) {
		t.Errorf("the password is returned")
	}
	// The masks must not lengthen the body before redact (round 8): past
	// redact's scan limit the tail, the part a reader wants, is masked
	// whole.
	var d struct{ Lines []string }
	_ = json.Unmarshal(b, &d)
	if len(d.Lines) == 0 || !strings.HasPrefix(d.Lines[len(d.Lines)-1], "2026-10-31 WARN task retry login [masked:credential] refused") {
		t.Errorf("the log's tail is lost: %q", d.Lines[max(0, len(d.Lines)-1):])
	}
}

// A log of one line without a newline (64 KiB or more) is not returned as
// zero lines with no word why (round 7, C3).
func TestPodLogMarksADroppedLine(t *testing.T) {
	h := newHarness(t, "", nil)
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = strings.Repeat("y", maxLogBytes+10)
	res, err := run(t, h, "k8s_logs", map[string]any{"pod": "orders-connect-connect-0"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	if !strings.Contains(string(b), logTruncatedMarker) {
		t.Errorf("no truncation marker: %s", b)
	}
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = "one\ntwo\n"
	res, _ = run(t, h, "k8s_logs", map[string]any{"pod": "orders-connect-connect-0"})
	if b, _ := json.Marshal(res.Data); strings.Contains(string(b), logTruncatedMarker) {
		t.Errorf("a marker on a whole log: %s", b)
	}
}

// k8s_secret_apply refuses a credential under 8 characters before it
// remembers or writes anything; a short user name is fine (round 7, C5).
func TestSecretApplyRefusesAShortCredential(t *testing.T) {
	h := newHarness(t, "", map[string]string{"short": "1234567", "ok": "12345678", "u": "cdc"})
	path := coreObjectPath(testNS, "secrets", "orders-db-credentials")
	h.api.put(path, map[string]any{"kind": "Secret", "type": "Opaque", "metadata": map[string]any{"name": "orders-db-credentials"}})
	_, err := run(t, h, "k8s_secret_apply", map[string]any{"keys": map[string]string{"password": "${secret:short}", "username": "${secret:u}"}})
	if !errors.Is(err, ErrParams) || !strings.Contains(err.Error(), "at least 8") || strings.Contains(err.Error(), "1234567") {
		t.Fatalf("a 7-character password: err = %v", err)
	}
	for _, r := range h.api.requests {
		if strings.HasPrefix(r, "PATCH") {
			t.Fatalf("a write reached the API: %s", r)
		}
	}
	if _, err := run(t, h, "k8s_secret_apply", map[string]any{"keys": map[string]string{"password": "${secret:ok}", "username": "${secret:u}"}}); err != nil {
		t.Fatalf("an 8-character password with a short user name: %v", err)
	}
}

// Round 8 key rule: no bare auth or upper-case camelCase tail, Debezium's
// resume token kept, whole null/none/failed/expired/rejected values kept,
// later keys after YES) still masked (S2), MYSQL_PWD and all-lower-case
// compounds and a quoted key with = masked (S3), doubly escaped JSON (S4).
func TestCredentialKeysRound8(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"NOAUTH Authentication required", ""},
		{"OAUTH: enabled", ""}, // round 11, F1: enabled and disabled are kept
		{"OAUTH: true", ""},
		{"useBasicAuth=true requireAuth: true auth=x", "useBasicAuth=true requireAuth: true auth=[masked:credential]"}, // round 11, F1: each value is judged on its own
		{"useBasicAuth=true", ""},
		{"resumeToken=8263A1B2C3", ""},
		{"resume_token: 8263A1B2C3", ""},
		{"Debezium resume token: 8263A1B2C3", ""},
		{`{"resumeToken":"8263A1B2C3","m":"ok"}`, ""},
		{`{"password": null, "m":"ok"}`, ""},
		{"password=none", ""},
		{"token: failed", ""},
		{`secret: "rejected"`, ""},
		{"token: expired yesterday", "token: [masked:credential]"},
		{"(using password: YES) ssl.key.password=hunter2xyz", "(using password: YES) ssl.key.password=[masked:credential]"}, // round 9, F3; round 11: the next key is judged on its own
		{"MYSQL_PWD=hunter2xyz", "MYSQL_PWD=[masked:credential]"},
		{"DB_PWD: hunter2xyz", "DB_PWD: [masked:credential]"},
		{"PWD=/opt/kafka", ""},
		{"dbpassword=hunter2xyz", "dbpassword=[masked:credential]"},
		{"sslpassword: hunter2xyz", "sslpassword: [masked:credential]"},
		{"accesstoken=hunter2xyz", "accesstoken=[masked:credential]"},
		{`"password" = "hunter2xyz"`, `"password" = [masked:credential]`},
		{`m="{\"password\":\"hun\\\"ter2xyz\",\"k\":\"v\"}"`, `m="{\"password\":\"[masked:credential]\",\"k\":\"v\"}"`},
		{`m="{\"password\":\"a\\\\\",\"k\":\"v\"}"`, `m="{\"password\":\"[masked:credential]\",\"k\":\"v\"}"`},
		{`{"token":["a",{"b":"]"}],"m":"ok"}`, `{"token":[masked:credential],"m":"ok"}`},
	} {
		want := c.want
		if want == "" {
			want = c.in
		}
		if got := maskOwn(c.in, nil); got != want {
			t.Errorf("maskOwn(%q)\n got %q\nwant %q", c.in, got, want)
		}
	}
}

// The JSON key scan is one pass (round 8, item 4): a long line of
// unclosed credential values took quadratic time.
// The check compares how the time grows from 64 KiB to 256 KiB rather than
// a wall-clock ceiling: the race detector slows the scan about tenfold, and
// a fixed ceiling flaked there at 2.4s while the scan stayed linear. Linear
// work grows about 4x over a 4x longer line and quadratic work about 16x, so
// a ratio above 10 fails; each size takes the best of three runs to damp
// scheduler noise. The absolute bound only guards against a hang.
func TestCredentialKeysLinear(t *testing.T) {
	best := func(line string) time.Duration {
		var min time.Duration
		for i := 0; i < 3; i++ {
			start := time.Now()
			maskCredentials(line, nil)
			if d := time.Since(start); i == 0 || d < min {
				min = d
			}
		}
		return min
	}
	for _, unit := range []string{`"token":[`, `"token":1,`, `\"token\":\"x`} {
		small := best(strings.Repeat(unit, (64<<10)/len(unit)))
		large := best(strings.Repeat(unit, (256<<10)/len(unit)))
		if small < time.Millisecond {
			small = time.Millisecond
		}
		if ratio := float64(large) / float64(small); ratio > 10 || large > 30*time.Second {
			t.Errorf("%q: 64 KiB %v, 256 KiB %v (x%.1f): not linear", unit, small, large, ratio)
		}
	}
}

// Each decoded copy catches a form no other copy or literal form does
// (round 8, item 5): mixed-case percent hex next to a literal + (%XX
// copy), '+' with
// mixed-case hex (%XX and + copy), and JSON escapes Go and Java never write
// (JSON copy); and \u escapes are decoded by hand, surrogate pairs too.
func TestCredentialValueCopies(t *testing.T) {
	written := []string{"uni\u00e9code", "two words\u00e9x", "a/b/c/d", "Ab12x", "x\U0001F600yzab", "a+b\u00e9xy"}
	for _, c := range []struct{ in, want string }{
		{"pct uni%c3%A9code end", "pct [masked:credential] end"},
		{"q two+words%c3%A9x end", "q [masked:credential] end"},
		{"p a+b%c3%A9xy end", "p [masked:credential] end"},
		{`{"m":"a\/b\/c\/d"}`, `{"m":"[masked:credential]"}`},
		{`{"m":"\u0041b12x"}`, `{"m":"[masked:credential]"}`},
		{`{"m":"x\ud83d\ude00yzab"}`, `{"m":"[masked:credential]"}`},
		{`{"m":"\u00G1b12x \u12"}`, `{"m":"\u00G1b12x \u12"}`},
	} {
		if got := maskOwn(c.in, written); got != c.want {
			t.Errorf("maskOwn(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}

// Written values are masked before redaction in every read (round 8, item
// 2 and S1): redact keeps the first and last characters of a token, so a
// password holding an AWS-key-shaped part leaked "Kx7orders-AKIA…" around
// the mask when redact ran first. The PEM test pins the other half of the
// order: the whole body is still redacted at once.
func TestReadsMaskBeforeRedact(t *testing.T) {
	pws := []string{"Kx7orders-" + fakeKeyID + "-x", "Zq3x-" + fakeKeyID + "-9fTr2LmQ8vWk1s", fakeSecretKey}
	for _, pw := range pws {
		h := newHarness(t, "", nil)
		h.d.rememberWritten([]string{pw})
		h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = "login with " + pw + " refused\n"
		cp := h.shell("KafkaConnect", "orders-connect", map[string]any{})
		h.api.allow(cp)
		h.api.objects[cp]["status"] = map[string]any{"conditions": []any{map[string]any{"type": "NotReady", "status": "True", "message": "login with " + pw + " refused"}}}
		for op, params := range map[string]map[string]any{
			"k8s_logs": {"pod": "orders-connect-connect-0"},
			"k8s_get":  {"kind": "KafkaConnect", "name": "orders-connect"},
		} {
			res, err := run(t, h, op, params)
			if err != nil {
				t.Fatalf("%s: %v", op, err)
			}
			b, _ := json.Marshal(res.Data)
			got := string(b)
			for _, part := range []string{pw[:6], pw[len(pw)-6:], "AKIA", "redacted"} {
				if strings.Contains(got, part) {
					t.Errorf("%s with %q returns %q: %s", op, pw, part, got)
				}
			}
		}
	}
}

// A short credential read back from an existing Secret is masked as a
// whole token, and the reads' summaries say to rotate it (round 8, S5).
func TestShortReadBackCredential(t *testing.T) {
	h := newHarness(t, "", nil)
	sp := coreObjectPath(testNS, "secrets", h.d.dbSecretName())
	h.api.objects[sp] = map[string]any{"metadata": map[string]any{"name": h.d.dbSecretName()}, "data": map[string]any{
		"password": base64.StdEncoding.EncodeToString([]byte("q9z")), "username": base64.StdEncoding.EncodeToString([]byte("cdc"))}}
	h.api.allow(sp)
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = "login%20q9z%20failed for cdc; q9zz stays\n"
	res, err := run(t, h, "k8s_logs", map[string]any{"pod": "orders-connect-connect-0"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	if got := string(b); strings.Contains(got, "%20q9z%20") || !strings.Contains(got, "q9zz stays") || !strings.Contains(got, "for cdc") {
		t.Errorf("k8s_logs: %s", got)
	}
	if !strings.Contains(res.Summary, "rotate it") {
		t.Errorf("no warning in the summary: %q", res.Summary)
	}
}

// preview_k8s_secret_apply refuses a short credential as k8s_secret_apply
// does (round 8, S5).
func TestPreviewRefusesAShortCredential(t *testing.T) {
	h := newHarness(t, "", map[string]string{"short": "1234567"})
	path := coreObjectPath(testNS, "secrets", "orders-db-credentials")
	h.api.put(path, map[string]any{"kind": "Secret", "type": "Opaque", "metadata": map[string]any{"name": "orders-db-credentials"}})
	_, err := run(t, h, "preview_k8s_secret_apply", map[string]any{"keys": map[string]string{"password": "${secret:short}"}})
	if !errors.Is(err, ErrParams) || !strings.Contains(err.Error(), "at least 8") || strings.Contains(err.Error(), "1234567") {
		t.Fatalf("preview of a 7-character password: err = %v", err)
	}
}

// fakeKeyID and fakeSecretKey have the shapes redact masks (an AWS access
// key id; a 40-character secret key), built at run time so no key-shaped
// literal sits in the source for secret scanners. Neither is a real key.
var (
	fakeKeyID     = "AK" + "IA" + "ABCDEFGHIJKLMNOP"
	fakeSecretKey = "wJalrXUtnF" + "EMI/K7MDENG/" + "bPxRfiCYzZ9TESTKEY"
)

// readsOf runs line through every read that masks: k8s_logs (a pod log
// line), k8s_get (a KafkaConnect condition), k8s_events (an event message)
// and strimzi_build_status (a build condition and the build pod's log),
// each on a fresh driver that remembers written. It returns each op's
// Data as JSON, and its Summary.
func readsOf(t *testing.T, written []string, line string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, op := range []string{"k8s_logs", "k8s_get", "k8s_events", "strimzi_build_status"} {
		h := newHarness(t, "", nil)
		h.d.rememberWritten(written)
		h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = line + "\n"
		h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-build/log"] = line + "\n"
		h.api.allowed["events"] = true
		h.api.objects["events"] = map[string]any{"items": []any{
			map[string]any{"type": "Warning", "reason": "Failed", "message": line, "lastTimestamp": "2026-10-30T10:00:00Z"},
		}}
		cp := h.shell("KafkaConnect", "orders-connect", map[string]any{})
		h.api.allow(cp)
		h.api.allow("/api/v1/namespaces/data/pods/orders-connect-connect-build")
		h.api.objects[cp]["status"] = map[string]any{"conditions": []any{map[string]any{"type": "NotReady", "status": "True", "reason": "BuildFailed", "message": "build: " + line}}}
		params := map[string]map[string]any{
			"k8s_logs":             {"pod": "orders-connect-connect-0"},
			"k8s_get":              {"kind": "KafkaConnect", "name": "orders-connect"},
			"k8s_events":           {"involved": "orders-connect-connect-0"},
			"strimzi_build_status": {},
		}[op]
		res, err := run(t, h, op, params)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		b, _ := json.Marshal(res.Data)
		out[op] = string(b)
		out[op+" summary"] = res.Summary
	}
	return out
}

// Round 9 key rule, through every read: credential keys by a
// case-insensitive suffix of the last segment (F1, M1), the ...Auth keys
// (F2), a YES)/NO) kept only whole (F3), doubly escaped JSON in pod logs
// and build logs too (F4), PWD after - or ., pagination cursors kept (C4).
func TestReadsKeyRuleRound9(t *testing.T) {
	const v = "Zq9pLm2x Vb7Kd"
	for _, k := range []string{"APIToken", "DBPassword", "PGPASSWORD", "MYSQLPASSWORD", "SASLPassword", "LDAPPassword", "AWSSecret", "OAUTHTOKEN", "GITHUBTOKEN", "JWTToken",
		"csrftoken", "httpAuth", "basicAuth", "proxyAuth", "MYSQL-PWD", "MYSQL.PWD", "MYSQL_PWD", "dbPwd", "DBPWD"} {
		for _, sep := range []string{"=", ": "} {
			for op, got := range readsOf(t, nil, "cfg "+k+sep+v) {
				if strings.Contains(got, "Zq9pLm2x") || strings.Contains(got, "Vb7Kd") {
					t.Errorf("%s: %s%s%s leaks: %s", op, k, sep, v, got)
				}
			}
		}
	}
	for _, line := range []string{"password: NO) Zq9pLm2xVb7Kd", `password: "YES) Zq9pLm2xVb7Kd"`, `m="{\"password\":\"Zq9pLm2x\\\"Vb7Kd\",\"k\":\"v\"}"`} {
		for op, got := range readsOf(t, nil, line) {
			if strings.Contains(got, "Zq9pLm2x") || strings.Contains(got, "Vb7Kd") {
				t.Errorf("%s: %q leaks: %s", op, line, got)
			}
		}
	}
	// PWD=/opt/kafka is kept by the key rule (TestMaskCredentialsRound7);
	// redact's own rule masks it after, so it is not checked here.
	for _, keep := range []string{"OAUTH: true", "tokenizer=1", "resumeToken=8263A1", "resume_token: 8263A1", "Debezium resume token: 8263A1",
		"nexttoken=AbC123", "pagetoken: AbC123", "continuationtoken=AbC123", "next_page_token: AbC123", "NEXTTOKEN=AbC123", "nextPageToken=AbC123",
		"useBasicAuth=true", "requireAuth: false", "httpAuth=false", "httpAuth: none", "basicAuth: null", "Token: expired"} {
		// "(using password: YES)" is kept by the key rule
		// (TestCredentialLineRule) but redact's own rule masks it after.
		for op, got := range readsOf(t, nil, keep) {
			if strings.HasSuffix(op, " summary") {
				continue
			}
			j, _ := json.Marshal(keep)
			if !strings.Contains(got, strings.Trim(string(j), `"`)) {
				t.Errorf("%s: %q is masked: %s", op, keep, got)
			}
		}
	}
}

// A written value holding the placeholder byte or invalid UTF-8 (read
// back from an old Secret) is still masked: the value is sanitized as the
// text is (round 9, F5); a 0x01 in a read's text is neither masked nor
// passed through (C5).
func TestReadsSanitizePlaceholder(t *testing.T) {
	for _, w := range []string{"ab\x01cdefgh", "ab\xffcdefgh"} {
		for op, got := range readsOf(t, []string{w}, "login with "+w+" refused") {
			if strings.Contains(got, "cdefgh") {
				t.Errorf("%s: written %q leaks: %s", op, w, got)
			}
		}
	}
	for op, got := range readsOf(t, nil, "a\x01\x01b stays") {
		if strings.HasSuffix(op, " summary") {
			continue
		}
		if strings.Contains(got, "masked") || strings.Contains(got, "\\u0001") || !strings.Contains(got, "a__b stays") {
			t.Errorf("%s: a 0x01 in the text: %s", op, got)
		}
	}
}

// k8s_secret_apply and its preview refuse a value holding a control
// character or invalid UTF-8, which no read could match (round 9, F5).
func TestSecretApplyRefusesControlCharacters(t *testing.T) {
	for _, v := range []string{"long\x01enough", "long\tenough", "long\xffenough"} {
		for _, op := range []string{"k8s_secret_apply", "preview_k8s_secret_apply"} {
			h := newHarness(t, "", map[string]string{"pw": v})
			path := coreObjectPath(testNS, "secrets", "orders-db-credentials")
			h.api.put(path, map[string]any{"kind": "Secret", "type": "Opaque", "metadata": map[string]any{"name": "orders-db-credentials"}})
			_, err := run(t, h, op, map[string]any{"keys": map[string]string{"password": "${secret:pw}"}})
			if !errors.Is(err, ErrParams) || !strings.Contains(err.Error(), "control character") {
				t.Errorf("%s %q: err = %v", op, v, err)
			}
		}
	}
}

// The rotate warning says what the reads do (round 9, C2): 4 to 7
// characters are masked wherever they appear, counted in runes.
func TestShortCredentialWarningRound9(t *testing.T) {
	got := readsOf(t, []string{"\u00e9\u00e9\u00e9\u00e9x"}, "login with \u00e9\u00e9\u00e9\u00e9x refused (and xx\u00e9\u00e9\u00e9\u00e9xx)")
	for _, op := range []string{"k8s_logs", "k8s_get", "k8s_events", "strimzi_build_status"} {
		if strings.Contains(got[op], "\u00e9\u00e9\u00e9\u00e9x") || strings.Contains(got[op], "\\u00e9\\u00e9") {
			t.Errorf("%s: a 5-rune value leaks: %s", op, got[op])
		}
		if s := got[op+" summary"]; !strings.Contains(s, "rotate it") || strings.Contains(s, "only as a whole word)") {
			t.Errorf("%s summary: %q", op, s)
		}
	}
	// Round 10, C7: a 3-rune value holding a non-ASCII rune is masked
	// wherever it appears; a 3-character ASCII one only as a token.
	if got := readsOf(t, []string{"\u00e9\u00e9\u00e9"}, "task \u00e9\u00e9\u00e9 and x\u00e9\u00e9\u00e9x")["k8s_logs"]; strings.Contains(got, "\u00e9") {
		t.Errorf("a 3-rune non-ASCII value leaks: %s", got)
	}
	if got := readsOf(t, []string{"q9z"}, "task q9z and xq9zx")["k8s_logs"]; !strings.Contains(got, "xq9zx") || strings.Contains(got, "task q9z") {
		t.Errorf("a 3-character ASCII value is not masked as a whole token only: %s", got)
	}
}
