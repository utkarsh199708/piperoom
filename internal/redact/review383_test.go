package redact

import (
	"strings"
	"testing"
	"time"
)

// TestReview383 covers #383: the command-line flag and XML element rules of
// #372 missed several real shapes, and masked plain prose.
func TestReview383(t *testing.T) {
	const sec = "Zx9hunter2Qw7Pa55"
	// Each row holds "hunter2Qw7", which must not survive.
	leaks := []struct{ name, in string }{
		{"quoted value with space", `psql --password "` + sec + ` x"`},
		{"single-quoted value with space", `psql --password '` + sec + ` x'`},
		{"underscore flag", "run --db_password " + sec},
		{"dotted flag", "kafka-console-consumer --sasl.password " + sec},
		{"upper-case flag", "psql --PASSWORD " + sec},
		{"camel-case flag", "tool --sslKeyPassword " + sec},
		{"equals separator", "psql --password=" + sec},
		{"equals and quotes", `psql --password="` + sec + ` x"`},
		{"escaped quote inside", `psql --password "Zx9\"hunter2Qw7Pa55 x"`},
		{"escaped quotes in JSON", `{"cmd":"psql --password \"Zx9 hunter2Qw7Pa55\""}`},
		{"repeated flag", "psql --password Ab1Cd2Ef3Gh --password " + sec},
		{"unclosed quote", `psql --password "Zx9 hunter2Qw7Pa55`},
		{"CRLF line", "psql --password " + sec + "\r\nnext line"},
		{"args array", `args: ["--password", "` + sec + `"]`},
		{"random token flag", "confluent --api-token " + sec},
		{"single-dash flag", "tool -password " + sec},
		{"curl quoted user", "curl -u 'admin:" + sec + "' http://connect:8083/"},
		{"curl quoted user with space", `curl -u "admin:Zx9 hunter2Qw7Pa55" http://connect:8083/`},
		{"curl user equals", "curl --user=admin:" + sec + " http://connect:8083/"},
		{"CDATA", "<password><![CDATA[" + sec + "]]></password>"},
		{"CDATA as in #383", "<password><[CDATA[" + sec + "]]></password>"},
		{"CDATA with spaces", "<password> <![CDATA[Zx9 hunter2Qw7Pa55]]> </password>"},
		{"attribute", `<password type="plain">` + sec + "</password>"},
		{"attribute single quotes", "<password type='plain'>" + sec + "</password>"},
		{"attribute holding >", `<password hint="a>b">` + sec + "</password>"},
		{"leading space", "<password> " + sec + " </password>"},
		{"own line", "<password>\n  " + sec + "\n</password>"},
		{"own line CRLF", "<password>\r\n  " + sec + "\r\n</password>"},
		{"own line JSON-escaped", `<password>\n  ` + sec + `\n</password>`},
		{"value with spaces", "<password>Zx9 hunter2Qw7Pa55</password>"},
		{"nested in parents", "<config><db><password>" + sec + "</password></db></config>"},
		{"nested child", "<password><value>" + sec + "</value></password>"},
		{"namespace prefix", "<ns:password>" + sec + "</ns:password>"},
		{"escaped slash", `<password>` + sec + `<\/password>`},
		{"after other XML on line", "<user>debezium</user><password>" + sec + "</password>"},
		// Round 2 of the #383 review.
		{"curl proxy user", "curl -U proxyuser:" + sec + " http://x/"},
		{"upper-case --USER", "curl --USER admin:" + sec},
		{"curl --proxy-user", "curl --proxy-user proxy:" + sec + " http://x/"},
		{"key/value children", "<secret><key>name</key><value>" + sec + "</value></secret>"},
		{"end tag with prefix", "<ns:password>" + sec + "</password>"},
		{"after a backtick", "`--password " + sec + "`"},
		{"after a JSON-escaped line break", `{"cmd":"run\n--password ` + sec + `"}`},
		{"after a semicolon", "cmd;--password " + sec},
		{"after a pipe", "x|--password " + sec},
	}
	for _, tc := range leaks {
		got, _ := Text(tc.in)
		if strings.Contains(got, "hunter2Qw7") {
			t.Errorf("%s: leak: %q -> %q", tc.name, tc.in, got)
		}
		if again, _ := Text(got); again != got {
			t.Errorf("%s: not idempotent: %q -> %q", tc.name, got, again)
		}
	}

	// No secret in any of these: they must pass unchanged.
	keep := []string{
		"psql: error: --password requires an argument",
		"psql --no-password orders",
		"pg_dump --no_password orders",
		"Unknown option --api-token used",
		"docker exec -u postgres:postgres pg psql",
		"docker run -u 1000:1000 debezium/connect",
		"the --token flag is deprecated, use --secret-name instead.",
		"pass --password to prompt for it",
		"--password-file /etc/pg/pass and --token-ttl 3600",
		"psql -U postgres orders",
		"<password/>",
		"<password></password>",
		`<password type="env"/>`,
		`<passwordPolicy type="x">strict</passwordPolicy>`,
		"<password>",
		"<user>debezium</user>",
		"connector pg-orders-connector task 0 RUNNING",
		// Round 2 of the #383 review.
		"I ran psql with --password. Still failing with FATAL",
		"run --password. Connection refused afterwards.",
		"1. Put the new value in `<token>`\n2. Restart pg-orders-connector\n3. Check lag on orders.public.orders at lsn 0/16B3748\n</details>",
		"WARN no <password> given\nsome evidence orders_v2 lsn 0/16B6C50\n</pre>",
		"<password>Zx9hunter2Qw7Pa55</other>",
		"usage: tool [-h] [--password PASSWORD] [--db-password DB_PASSWORD]",
		"psql --password $PGPASSWORD orders",
		"psql --password ${PGPASSWORD} orders",
		"docker exec -u www-data:www-data app sh",
		"psql -U postgres -h db orders",
	}
	for _, in := range keep {
		if got, _ := Text(in); got != in {
			t.Errorf("changed evidence: %q -> %q", in, got)
		}
	}

	// The mask keeps the surrounding structure.
	exact := []struct{ in, want string }{
		{`<password type="plain">` + sec + "</password>", `<password type="plain">[redacted 24]</password>`},
		{"<password>\n  " + sec + "\n</password>", "<password>\n  [redacted 24]\n</password>"},
		{"<password><![CDATA[" + sec + "]]></password>", "<password><![CDATA[[redacted 24]]]></password>"},
		{`psql --password "` + sec + ` x" orders`, `psql --password "[redacted 24]" orders`},
		{"run --db_password " + sec + " -h db", "run --db_password [redacted 24] -h db"},
		{"curl -u admin:" + sec + " http://c/", "curl -u admin:[redacted 24] http://c/"},
		{"<secret><key>name</key><value>" + sec + "</value></secret>", "<secret><key>name</key><value>[redacted 24]</value></secret>"},
		{"(--password " + sec + ")", "(--password [redacted 24])"},
		{"[--password " + sec + "]", "[--password [redacted 24]]"},
		{"`psql --password " + sec + "`", "`psql --password [redacted 24]`"},
		{"curl -U proxyuser:" + sec + " http://x/", "curl -U proxyuser:[redacted 24] http://x/"},
	}
	for _, tc := range exact {
		if got, _ := Text(tc.in); got != tc.want {
			t.Errorf("Text(%q)\n got  %q\n want %q", tc.in, got, tc.want)
		}
	}
}

// slowLines383 are hostile inputs for the flag and XML finders: open quotes,
// open start tags and CDATA sections that never close, and repeated flags
// whose arguments are rejected.
var slowLines383 = map[string]func(n int) string{
	"open-quotes":     func(n int) string { return strings.Repeat(`--password "a `, n/14) },
	"rejected-flags":  func(n int) string { return strings.Repeat("--password word ", n/16) },
	"bare-dashes":     func(n int) string { return strings.Repeat(" -", n/2) },
	"user-flags":      func(n int) string { return strings.Repeat("-u a:b ", n/7) },
	"open-tags":       func(n int) string { return strings.Repeat(`<password a="`, n/13) },
	"open-cdata":      func(n int) string { return strings.Repeat("<password><![CDATA[x", n/20) },
	"unclosed-elems":  func(n int) string { return strings.Repeat("<password>x\n", n/12) },
	"nested-children": func(n int) string { return strings.Repeat("<password><value>", n/17) },
	// Round 2 of the #383 review: rejected flags whose value scan ran to
	// the end of the line, and end tags that close another element.
	"user-equals":     func(n int) string { return strings.Repeat("-u=x=", n/5) },
	"token-equals":    func(n int) string { return strings.Repeat("--token=x=", n/10) },
	"token-dquotes":   func(n int) string { return strings.Repeat(`--token "`, n/9) },
	"token-squotes":   func(n int) string { return strings.Repeat("--token '", n/9) },
	"user-colons":     func(n int) string { return strings.Repeat("-u a:b:", n/7) },
	"mismatched-ends": func(n int) string { return strings.Repeat("<password>x</other>", n/19) },
	"key-children":    func(n int) string { return strings.Repeat("<secret><key>a</key>", n/20) },
	// Round 3: secret children handed back after a failed parent, and the
	// two lines closest to the bound in round 2.
	"secret-siblings": func(n int) string { return strings.Repeat("<secret><password>x</password><a/>", n/34) },
	"eq-token":        func(n int) string { return strings.Repeat("=-token=1", n/9) },
	"user-pairs":      func(n int) string { return strings.Repeat("-u a:b ", n/7) },
}

// TestReview383LinearTime holds the new finders to TestReviewLinearTime's
// ceiling.
func TestReview383LinearTime(t *testing.T) {
	if raceEnabled {
		t.Skip("timing check; runs without -race")
	}
	const ceiling = 200 * time.Millisecond
	for name, mk := range slowLines383 {
		s := mk(MaxScan)
		best := time.Duration(1 << 62)
		for range 5 {
			start := time.Now()
			Text(s)
			best = min(best, time.Since(start))
			if best < ceiling/4 {
				break
			}
		}
		if best > ceiling {
			t.Errorf("%s: best %v for %d KiB, want under %v: superlinear?", name, best, len(s)>>10, ceiling)
		}
	}
}

// TestReview383Round3 holds the rows of the round-2 review of #415. S is the
// secret; CD is the CDATA opener.
func TestReview383Round3(t *testing.T) {
	const S = "Zx9hunter2Qw7Pa55"
	const CD = "<![CDATA["
	leaks := []string{
		// Item 1: a failed child scan of a secret element hid complete
		// secret children.
		"<secret><name>db</name><password>" + S + "</password><user>deb",
		"<secret><password>" + S + "</password>",
		"<secret><token>" + S + "</token> (truncated)",
		"expected <password><token>" + S + "</token> pair",
		"<token><password>" + S + "</password> trailing text",
		"<secret><password>" + S + "</password> trailing</secret>",
		"<secret><password>" + S + "</password></secrets>",
		`<secret><name>db</name><password>` + S + `</password><rotation enabled="true"/></secret>`,
		"<secret><password>" + S + "</password><!-- rotated --></secret>",
		"<secret><password>" + S + "</password><value>" + CD + "x]]></value></secret>",
		"<secret><password>" + S + "</password><note>" + CD + "x]]></note></secret>",
		"<secret><password>" + S + "</password><meta><x>1</x></meta></secret>",
		"<token><access_token>" + S + "</access_token><scope><item>read</item></scope></token>",
		"<secret><password>" + S + "</password>" + strings.Repeat("<a>1</a>", 16) + "</secret>",
		"<secret>\n  <name>db</name>\n  <password>" + S + "</password>\n  <user>debezium</user>\n",
		"<token>\n  <access_token>" + S + "</access_token>\n  <token_type>Bearer</token_type>\n  <expires_in>36",
		// Item 3: long names, a leading digit, a third dash.
		"<io.confluent.connect.jdbc.sink.JdbcSinkConnectorConfig.connection.password>" + S +
			"</io.confluent.connect.jdbc.sink.JdbcSinkConnectorConfig.connection.password>",
		"--2fa-token " + S,
		"--1password-secret " + S,
		"---password " + S,
		// Item 11 (flipped): a flag after {, < or &.
		"{--password " + S + "}",
		"<--password " + S + ">",
		"true&&--password " + S,
	}
	for _, in := range leaks {
		got, _ := Text(in)
		if strings.Contains(got, "hunter2Qw7") {
			t.Errorf("leak: %q -> %q", in, got)
		}
		if again, _ := Text(got); again != got {
			t.Errorf("not idempotent: %q -> %q", got, again)
		}
	}

	keep := []string{
		// Item 12: in the list form, the next flag or a plain word is no value.
		`command: ["psql", "--password", "-h", "db", "orders"]`,
		`["psql", "--password", "orders"]`,
		`["psql", "--password", "--", "orders"]`,
		// Item 13: element text must sit on one line.
		"Step 1: replace <token>\nconnector pg-orders-connector FAILED lsn 0/16B3748\nStep 2: </token> closes it",
		// Item 14: a CDATA counts only when its element's end tag follows.
		"<password>" + CD + "\nconnector pg-orders-connector FAILED lsn 0/16B3748\n<script>" + CD + "x]]></script>",
		// Item 15: only value children and secret-named children are values.
		"<token><type>bearer</type><expires_in>3600</expires_in></token>",
		// Item 16: a closer belongs to the flag only after its opener.
		"usage: curl [-u user:password] url",
		"curl (-u user:password) url",
		"pass `-u user:password` to curl",
		// Item 5's keep row, and item 9's.
		`exec: "psql" "--password" "-h" "db"`,
		"psql -p 5432 -U app orders",
		"docker run -p 8080:80 app",
	}
	for _, in := range keep {
		if got, _ := Text(in); got != in {
			t.Errorf("changed evidence: %q -> %q", in, got)
		}
	}

	exact := []struct{ in, want string }{
		// Item 2: weak flag names mask any value that is not prose.
		{"tool --token 8f3a2c1d9e", "tool --token [redacted 16]"},
		{"tool --token 123456789012345678", "tool --token [redacted 24]"},
		{"tool --api-token ABCDEFGHIJKLMNOPQRST", "tool --api-token [redacted 24]"},
		// Item 16: a closer that is not paired stays in the secret.
		{"psql --password Zx9hunter2Qw7Pa5)", "psql --password [redacted 24]"},
		{"<secret><key>db</key><value>" + S + "</value></secret>", "<secret><key>db</key><value>[redacted 24]</value></secret>"},
	}
	for _, tc := range exact {
		if got, _ := Text(tc.in); got != tc.want {
			t.Errorf("Text(%q)\n got  %q\n want %q", tc.in, got, tc.want)
		}
	}
}

// TestReview383Gaps pins shapes that still pass unmasked (main leaks them
// too), or that are still over-masked, to today's output. Each row names its
// gap; with the gap marker removed, the row checks the ideal instead (no
// leak, or the ideal output) and fails. #455 takes them up.
func TestReview383Gaps(t *testing.T) {
	const S = "Zx9hunter2Qw7Pa55"
	bs := "\\"
	rows := []struct{ gap, in, now, ideal string }{
		{"item 4: a whole quoted --flag=value argument ends at the first blank",
			"\"args\":[\"--password=Zx9 hunter2Qw7Pa55\",\"--db=orders\"]",
			"\"args\":[\"--password=[redacted 8] hunter2Qw7Pa55\",\"--db=orders\"]", ""},
		{"item 4: a whole quoted --flag=value argument ends at the first blank",
			"  - \"--password=Zx9 hunter2Qw7Pa55\"",
			"  - \"--password=[redacted 8] hunter2Qw7Pa55\"", ""},
		{"item 4: a whole quoted --flag=value argument ends at the first blank",
			"  - '--sasl.password=Zx9 hunter2Qw7Pa55'",
			"  - '--sasl.password=[redacted 8] hunter2Qw7Pa55'", ""},
		{"item 5: JSON-escaped or %q argument lists are not read",
			"{\"args\":\"[\\\"--password\\\", \\\"Zx9hunter2Qw7Pa55\\\"]\"}",
			"{\"args\":\"[\\\"--password\\\", \\\"Zx9hunter2Qw7Pa55\\\"]\"}", ""},
		{"item 5: JSON-escaped or %q argument lists are not read",
			"{\"spec\":\"[\\\"--password\\\",\\\"Zx9hunter2Qw7Pa55\\\"]\"}",
			"{\"spec\":\"[\\\"--password\\\",\\\"Zx9hunter2Qw7Pa55\\\"]\"}", ""},
		{"item 5: JSON-escaped or %q argument lists are not read",
			"\"[\\\"--password\\\",\\\"Zx9hunter2Qw7Pa55\\\"]\"",
			"\"[\\\"--password\\\",\\\"Zx9hunter2Qw7Pa55\\\"]\"", ""},
		{"item 5: JSON-escaped or %q argument lists are not read",
			"\"[\\\"-u\\\", \\\"admin:Zx9hunter2Qw7Pa55\\\"]\"",
			"\"[\\\"-u\\\", \\\"admin:Zx9hunter2Qw7Pa55\\\"]\"", ""},
		{"item 5: JSON-escaped or %q argument lists are not read",
			"running [\"psql\" \"-h\" \"db\" \"--password\" \"Zx9hunter2Qw7Pa55\" \"orders\"]",
			"running [\"psql\" \"-h\" \"db\" \"--password\" \"Zx9hunter2Qw7Pa55\" \"orders\"]", ""},
		{"item 5: JSON-escaped or %q argument lists are not read",
			"exec: \"psql\" \"--password\" \"Zx9hunter2Qw7Pa55\": exit status 2",
			"exec: \"psql\" \"--password\" \"Zx9hunter2Qw7Pa55\": exit status 2", ""},
		{"item 6: CDATA, comments or text around a child are not read",
			"<password><value><![CDATA[Zx9hunter2Qw7Pa55]]></value></password>",
			"<password><value><![CDATA[Zx9hunter2Qw7Pa55]]></value></password>", ""},
		{"item 6: CDATA, comments or text around a child are not read",
			"<secret><key>db</key><value><![CDATA[Zx9hunter2Qw7Pa55]]></value></secret>",
			"<secret><key>db</key><value><![CDATA[Zx9hunter2Qw7Pa55]]></value></secret>", ""},
		{"item 6: CDATA, comments or text around a child are not read",
			"\"<secret><value><![CDATA[Zx9hunter2Qw7Pa55]]><\\/value><\\/secret>\"",
			"\"<secret><value><![CDATA[Zx9hunter2Qw7Pa55]]><\\/value><\\/secret>\"", ""},
		{"item 6: CDATA, comments or text around a child are not read",
			"<password><![CDATA[Zx9]]]]><![CDATA[>hunter2Qw7Pa55]]></password>",
			"<password><![CDATA[Zx9]]]]><![CDATA[>hunter2Qw7Pa55]]></password>", ""},
		{"item 6: CDATA, comments or text around a child are not read",
			"<password><!-- env -->Zx9hunter2Qw7Pa55</password>",
			"<password><!-- env -->Zx9hunter2Qw7Pa55</password>", ""},
		{"item 6: CDATA, comments or text around a child are not read",
			"<password>Zx9hunter2Qw7Pa55<!-- x --></password>",
			"<password>Zx9hunter2Qw7Pa55<!-- x --></password>", ""},
		{"item 6: CDATA, comments or text around a child are not read",
			"<password><value>Zx9hunter2Qw7Pa55</value> (masked by vault)</password>",
			"<password><value>Zx9hunter2Qw7Pa55</value> (masked by vault)</password>", ""},
		{"item 7: -u with a quoted part, an empty user, or glued to the flag",
			"curl -u admin:\"Zx9hunter2Qw7Pa55\" http://c/",
			"curl -u admin:\"Zx9hunter2Qw7Pa55\" http://c/", ""},
		{"item 7: -u with a quoted part, an empty user, or glued to the flag",
			"curl -u admin:'Zx9 hunter2Qw7Pa55' http://c/",
			"curl -u admin:'Zx9 hunter2Qw7Pa55' http://c/", ""},
		{"item 7: -u with a quoted part, an empty user, or glued to the flag",
			"curl --user \"admin\":\"Zx9hunter2Qw7Pa55\"",
			"curl --user \"admin\":\"Zx9hunter2Qw7Pa55\"", ""},
		{"item 7: -u with a quoted part, an empty user, or glued to the flag",
			"curl -u \"svc user:Zx9hunter2Qw7Pa55\" http://c/",
			"curl -u \"svc user:Zx9hunter2Qw7Pa55\" http://c/", ""},
		{"item 7: -u with a quoted part, an empty user, or glued to the flag",
			"curl --user 'svc user:Zx9hunter2Qw7Pa55' http://c/",
			"curl --user 'svc user:Zx9hunter2Qw7Pa55' http://c/", ""},
		{"item 7: -u with a quoted part, an empty user, or glued to the flag",
			"curl -u :Zx9hunter2Qw7Pa55 http://c/",
			"curl -u :Zx9hunter2Qw7Pa55 http://c/", ""},
		{"item 7: -u with a quoted part, an empty user, or glued to the flag",
			"curl -uadmin:Zx9hunter2Qw7Pa55",
			"curl -uadmin:Zx9hunter2Qw7Pa55", ""},
		{"item 8: a doubly escaped \\\\\\\" opener is not read",
			"{\"cmd\":\"psql --password \\\\\\\"Zx9hunter2Qw7Pa55\\\\\\\"\"}",
			"{\"cmd\":\"psql --password [redacted 8]\"Zx9hunter2Qw7Pa55\\\\\\\"\"}", ""},
		{"item 9: names ending in pass, and docker login -p, are not read",
			"keytool -importcert -alias ca -keystore kafka.truststore.jks -storepass Zx9hunter2Qw7Pa55 -noprompt",
			"keytool -importcert -alias ca -keystore kafka.truststore.jks -storepass Zx9hunter2Qw7Pa55 -noprompt", ""},
		{"item 9: names ending in pass, and docker login -p, are not read",
			"keytool -genkey -keypass Zx9hunter2Qw7Pa55",
			"keytool -genkey -keypass Zx9hunter2Qw7Pa55", ""},
		{"item 9: names ending in pass, and docker login -p, are not read",
			"openssl enc -aes-256-cbc -pass Zx9hunter2Qw7Pa55",
			"openssl enc -aes-256-cbc -pass Zx9hunter2Qw7Pa55", ""},
		{"item 9: names ending in pass, and docker login -p, are not read",
			"tool --pass Zx9hunter2Qw7Pa55",
			"tool --pass Zx9hunter2Qw7Pa55", ""},
		{"item 9: names ending in pass, and docker login -p, are not read",
			"docker login -u admin -p Zx9hunter2Qw7Pa55",
			"docker login -u admin -p Zx9hunter2Qw7Pa55", ""},
		{"item 10: Hadoop name/value pairs, Maven <arg> lists and \\u003c-escaped XML are not read",
			"<property><name>javax.jdo.option.ConnectionPassword</name><value>Zx9hunter2Qw7Pa55</value></property>",
			"<property><name>javax.jdo.option.ConnectionPassword</name><value>Zx9hunter2Qw7Pa55</value></property>", ""},
		{"item 10: Hadoop name/value pairs, Maven <arg> lists and \\u003c-escaped XML are not read",
			"<property>\n  <name>javax.jdo.option.ConnectionPassword</name>\n  <value>Zx9hunter2Qw7Pa55</value>\n</property>",
			"<property>\n  <name>javax.jdo.option.ConnectionPassword</name>\n  <value>Zx9hunter2Qw7Pa55</value>\n</property>", ""},
		{"item 10: Hadoop name/value pairs, Maven <arg> lists and \\u003c-escaped XML are not read",
			"<arg>--password</arg><arg>Zx9hunter2Qw7Pa55</arg>",
			"<arg>--password</arg><arg>Zx9hunter2Qw7Pa55</arg>", ""},
		{"item 10: Hadoop name/value pairs, Maven <arg> lists and \\u003c-escaped XML are not read",
			"{\"xml\":\"" + bs + "u003cpassword" + bs + "u003e" + S + bs + "u003c/password" + bs + "u003e\"}",
			"{\"xml\":\"" + bs + "u003cpassword" + bs + "u003e" + S + bs + "u003c/password" + bs + "u003e\"}", ""},
		{"item 11: a value holding a quote ends there",
			"psql --password Zx9\"hunter2Qw7Pa55",
			"psql --password [redacted 8]\"hunter2Qw7Pa55", ""},
		{"item 11: a value starting with - reads as the next flag",
			"psql --password -Zx9hunter2Qw7Pa55",
			"psql --password -Zx9hunter2Qw7Pa55", ""},
		{"nit B: a malformed or mismatched end tag is not this element's",
			"<password>Zx9hunter2Qw7Pa55</ password>",
			"<password>Zx9hunter2Qw7Pa55</ password>", ""},
		{"nit B: a malformed or mismatched end tag is not this element's",
			"<password>Zx9hunter2Qw7Pa55</passwd>",
			"<password>Zx9hunter2Qw7Pa55</passwd>", ""},
		{"item 12: psql's --password takes no argument; orders_v2 is not a plain word",
			"[\"pg_dump\", \"--password\", \"orders_v2\"]",
			"[\"pg_dump\", \"--password\", \"[redacted 16]\"]", "[\"pg_dump\", \"--password\", \"orders_v2\"]"},
		{"item 13: a plain word between a secret element's tags reads as its value",
			"Put the value between <password> and </password> tags",
			"Put the value between <password> [redacted 8] </password> tags", "Put the value between <password> and </password> tags"},
		{"item 16: a ) closing a bracket before the command, not the flag, stays in the value",
			"(curl -u admin:Zx9hunter2Qw7Pa55)",
			"(curl -u admin:[redacted 24]", "(curl -u admin:" + S + ")"},
	}
	for _, r := range rows {
		got, _ := Text(r.in)
		switch {
		case r.gap != "":
			if got != r.now {
				t.Errorf("gap %q moved: Text(%q)\n got  %q\n pinned %q", r.gap, r.in, got, r.now)
			}
		case r.ideal != "":
			if got != r.ideal {
				t.Errorf("Text(%q)\n got  %q\n want %q", r.in, got, r.ideal)
			}
		case strings.Contains(got, "hunter2Qw7"):
			t.Errorf("leak: %q -> %q", r.in, got)
		}
	}
}
