package redact

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
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
		"pg_dump --no_password orders",
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
		"usage: tool [-h] [--password PASSWORD] [--db-password DB_PASSWORD]",
		"psql --password ${PGPASSWORD} orders",
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
	// Round 5: quotes glued to more of the word, in rejected arguments.
	"glued-quotes":  func(n int) string { return strings.Repeat(`--token "{{x"y `, n/15) },
	"quote-letters": func(n int) string { return "--password \"" + strings.Repeat(`a"b`, n/3) },
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

// TestReview383Gaps pins, to today's output, shapes that origin/main handles
// no better: secrets it leaks too, and over-masks it has too (the #372
// rules run unchanged ahead of this PR's). Each row names its gap; with the
// gap marker removed, the row checks the ideal instead (no leak, or the
// ideal output) and fails. No ideal holds a raw secret. #455 takes them up.
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
		{"item 16: a ) closing a bracket before the command, not the flag, stays in the value",
			"(curl -u admin:Zx9hunter2Qw7Pa55)",
			"(curl -u admin:[redacted 24]", "(curl -u admin:[redacted 24])"},
		{"#372 rule: a bare $VAR is read as the value",
			"psql --password $PGPASSWORD orders",
			"psql --password [redacted 16] orders", "psql --password $PGPASSWORD orders"},
		{"#372 rule: a bare value runs over a closing backtick",
			"`psql --password " + S + "`",
			"`psql --password [redacted 24]", "`psql --password [redacted 24]`"},
		{"a quoted flag followed by a quoted value is not read (main too)",
			"psql '--password' 'Zx9hunter2Qw7Pa55'",
			"psql '--password' 'Zx9hunter2Qw7Pa55'", ""},
		{"a flag inside element text is not read (main too)",
			"<arg>--password Zx9hunter2Qw7Pa55</arg>",
			"<arg>--password Zx9hunter2Qw7Pa55</arg>", ""},
		{"a flag after ':' is not read (main too)",
			"cmd:--password Zx9hunter2Qw7Pa55",
			"cmd:--password Zx9hunter2Qw7Pa55", ""},
		{"a value after a CR is not followed (main too)",
			"psql --password\rZx9hunter2Qw7Pa55",
			"psql --password\rZx9hunter2Qw7Pa55", ""},
		{"a single-quoted ${...} is kept as a placeholder (main too)",
			"psql --password '${Zx9hunter2Qw7Pa55}'",
			"psql --password '${Zx9hunter2Qw7Pa55}'", ""},
		{"an argument after -- is not read (main too)",
			"psql --password -- Zx9hunter2Qw7Pa55",
			"psql --password -- Zx9hunter2Qw7Pa55", ""},
		{"shell-escaped quotes are not read (main too)",
			"psql --password \\'Zx9hunter2Qw7Pa55\\'",
			"psql --password \\'Zx9hunter2Qw7Pa55\\'", ""},
		{"a self-closing attribute ends the start tag early (main too)",
			"<password a=/>Zx9hunter2Qw7Pa55</password>",
			"<password a=/>Zx9hunter2Qw7Pa55</password>", ""},
		{"an attribute value holding <x> ends the start tag (main too)",
			"<password hint=\"<x>\">Zx9hunter2Qw7Pa55</password>",
			"<password hint=\"<x>\">Zx9hunter2Qw7Pa55</password>", ""},
		{"a flag after a non-ASCII blank is not read (main too)",
			"x\u00a0--password Zx9hunter2Qw7Pa55",
			"x\u00a0--password Zx9hunter2Qw7Pa55", ""},
		{"an em dash is not a flag (main too)",
			"x —password Zx9hunter2Qw7Pa55",
			"x —password Zx9hunter2Qw7Pa55", ""},
		{"element text that spans lines is not read (main too)",
			"<password>Zx9hunter2Qw7Pa55\nmore\n</password>",
			"<password>Zx9hunter2Qw7Pa55\nmore\n</password>", ""},
		{"an escaped end tag of another element is not this element's (main too)",
			"<password>Zx9hunter2Qw7Pa55<\\/passwd>",
			"<password>Zx9hunter2Qw7Pa55<\\/passwd>", ""},
		{"a child's text that spans lines is not read (main too)",
			"<secret><value>Zx9\nhunter2Qw7Pa55</value></secret>",
			"<secret><value>Zx9\nhunter2Qw7Pa55</value></secret>", ""},
		{"#372 rule: a bare value runs over a closing tag",
			"<cmd>psql --password Zx9hunter2Qw7Pa55</cmd>",
			"<cmd>psql --password [redacted 24]", "<cmd>psql --password [redacted 24]</cmd>"},
		{"#372 rule: an unquoted command substitution is read as the value",
			"psql --password $(cat /run/secrets/pw) orders",
			"psql --password [redacted 8] /run/secrets/pw) orders", "psql --password $(cat /run/secrets/pw) orders"},
		{"#372 rule: a Windows %VAR% is read as the value",
			"psql --password %PGPASSWORD% orders",
			"psql --password [redacted 16] orders", "psql --password %PGPASSWORD% orders"},
		{"#372 rule: a user:gid pair with a named user is read as user:password",
			"docker run -u postgres:999 img",
			"docker run -u postgres:[redacted 8] img", "docker run -u postgres:999 img"},
		{"#490, split out: the #372 rule masks the word after this flag",
			"psql: error: --password requires an argument",
			"psql: error: --password [redacted 8] an argument", "psql: error: --password requires an argument"},
		{"#490, split out: the #372 rule masks the word after this flag",
			"psql --no-password orders",
			"psql --no-password [redacted 8]", "psql --no-password orders"},
		{"#490, split out: the #372 rule masks the word after this flag",
			"Unknown option --api-token used",
			"Unknown option --api-token [redacted 8]", "Unknown option --api-token used"},
		{"#490, split out: the #372 rule masks the word after this flag",
			"docker exec -u postgres:postgres pg psql",
			"docker exec -u postgres:[redacted 8] pg psql", "docker exec -u postgres:postgres pg psql"},
		{"#490, split out: the #372 rule masks the word after this flag",
			"docker run -u 1000:1000 debezium/connect",
			"docker run -u 1000:[redacted 8] debezium/connect", "docker run -u 1000:1000 debezium/connect"},
		{"#490, split out: the #372 rule masks the word after this flag",
			"the --token flag is deprecated, use --secret-name instead.",
			"the --token [redacted 8] is deprecated, use --secret-name instead.", "the --token flag is deprecated, use --secret-name instead."},
		{"#490, split out: the #372 rule masks the word after this flag",
			"pass --password to prompt for it",
			"pass --password [redacted 8] prompt for it", "pass --password to prompt for it"},
		{"#490, split out: the #372 rule masks the word after this flag",
			"docker exec -u www-data:www-data app sh",
			"docker exec -u www-data:[redacted 8] app sh", "docker exec -u www-data:www-data app sh"},
		// Round 4, item 4: generated lower-case tokens after a flag in an
		// argument list or usage brackets read as names (main leaks them too).
		{"#455: a lower-case token in list form reads as a name (main too)",
			`args: ["--token", "d41d8cd98f00b204e9800998ecf8427e"]`,
			`args: ["--token", "d41d8cd98f00b204e9800998ecf8427e"]`, `args: ["--token", "[redacted 32]"]`},
		{"#455: a lower-case token in list form reads as a name (main too)",
			`args: ["--password", "s3cr3tpassw0rd"]`,
			`args: ["--password", "s3cr3tpassw0rd"]`, `args: ["--password", "[redacted 16]"]`},
		{"#455: a lower-case token in list form reads as a name (main too)",
			`command: ["kafka-acls", "--sasl-password", "a3f9c2e7b1d48e6f"]`,
			`command: ["kafka-acls", "--sasl-password", "a3f9c2e7b1d48e6f"]`, `command: ["kafka-acls", "--sasl-password", "[redacted 16]"]`},
		{"#455: a lower-case token after [ reads as a name (main too)",
			"[--token d41d8cd98f00b204e9800998ecf8427e]",
			"[--token d41d8cd98f00b204e9800998ecf8427e]", "[--token [redacted 32]]"},
	}
	for _, r := range rows {
		if strings.Contains(r.ideal, "hunter2Qw7") {
			t.Errorf("gap %q: the ideal holds the raw secret", r.gap)
		}
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

// TestReview383Round4 holds the rows of the round-3 review of #415, under
// the additive design: the #372 rules run first and the added rules only
// mask more. Exact rows give main's output wherever main masks.
func TestReview383Round4(t *testing.T) {
	leaks := []string{
		"<password>Zx9hunter2Qw7Pa55</ password>",                                                   // i1
		"<password>Zx9hunter2Qw7Pa55</passwd>",                                                      // i1
		"<password>Zx9hunter2Qw7Pa55</other>",                                                       // i1
		"<password>Zx9hunter2Qw7Pa55</password",                                                     // i1
		"<password>Zx9hunter2Qw7Pa55</>",                                                            // i1
		"<password>Zx9hunter2Qw7Pa55</passwords>",                                                   // i1
		"<password>Zx9hunter2Qw7Pa55</passw",                                                        // i1
		"<password>Zx9 hunter2Qw7Pa55</pas",                                                         // i1
		"<password>Zx9hunter2Qw7Pa55</ password >",                                                  // i1
		"<token>Zx9hunter2Qw7Pa55</secret>",                                                         // i1
		"<token>Zx9hunter2Qw7Pa55</token-x>",                                                        // i1
		"<ssl.keystore.password>Zx9hunter2Qw7Pa55</password>",                                       // i1
		"<db:password>Zx9hunter2Qw7Pa55</db:passwd>",                                                // i1
		"<password>Zx9\\nhunter2Qw7Pa55</password>",                                                 // i2
		"<password>Zx9\\rhunter2Qw7Pa55</password>",                                                 // i2
		"<password>Zx9\\\\nhunter2Qw7Pa55</password>",                                               // i2
		"<token>ab\\rcdEF12hunter2Qw7Pa55</token>",                                                  // i2
		"{\"xml\":\"<password>Zx9\\nhunter2Qw7Pa55</password>\"}",                                   // i2
		"{\"xml\":\"<secret>\\nMIIBUzCCAQ2gAwIBAgIUZx9hunter2Qw7Pa55\\nABCDEFGHIJKL\\n</secret>\"}", // i2
		"progress 100%\r--password Zx9hunter2Qw7Pa55",                                               // i3
		"line\r--token Zx9hunter2Qw7Pa55",                                                           // i3
		"a\r--api-secret Zx9hunter2Qw7Pa55 b",                                                       // i3
		"x\r-u admin:Zx9hunter2Qw7Pa55",                                                             // i3
		"page\f--password Zx9hunter2Qw7Pa55",                                                        // i3
		"gpg --sslpassphrase Zx9hunter2Qw7Pa55",                                                     // i4
		"tool --keypassphrase Zx9hunter2Qw7Pa55",                                                    // i4
		"tool --mypassphrase Zx9hunter2Qw7Pa55",                                                     // i4
		"tool -dbpassphrase Zx9hunter2Qw7Pa55",                                                      // i4
		"psql --password $Zx9hunter2Qw7Pa55",                                                        // i5
		"psql --password '$Zx9hunter2Qw7Pa55'",                                                      // i5
		"psql --password \"$Zx9hunter2Qw7Pa55\"",                                                    // i5
		"psql --password $ecretHunter2Qw7Pa55",                                                      // i5
		"tool --api-secret '$Ab12hunter2Qw7Pa55'",                                                   // i5
		"x --password $PGPASSWORDZx9hunter2Qw7Pa55",                                                 // i5
		"curl -u admin:$Zx9hunter2Qw7Pa55 http://x/",                                                // i5
		"curl -u 'admin:$Zx9hunter2Qw7Pa55' http://x/",                                              // i5
		"[\"psql\",\"--password\",\"$Zx9hunter2Qw7Pa55\"]",                                          // i16
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
		"{\"error\":\"flag needs an argument: --password\",\"exit_code\":2}",                                // i7
		"{\"level\":\"fatal\",\"msg\":\"flag needs an argument: -password\",\"service_name\":\"piperoom\"}", // i7
		"{\"msg\":\"unknown flag: --token\",\"connectorName\":\"pg-orders-connector\"}",                     // i7
		"{'error': 'missing --password', 'retry_count': 3}",                                                 // i7
		"[\"pg_dump\", \"--password\", \"orders_v2\"]",                                                      // i8
		"command: [\"pg_dump\", \"--password\", \"orders_db\"]",                                             // i8
		"command: [\"pg_dump\", \"--password\", \"orders-db\"]",                                             // i8
		"args: [\"--token\", \"{{ .Values.token }}\"]",                                                      // i8
		"Put the value between <password> and </password> tags",                                             // i9
		"Set <token> to </token> in the template",                                                           // i9
		"the <password> tag and </password> close",                                                          // i9
		"Replace <secret> with </secret>",                                                                   // i9
		"Put the new token between <token> or </token>",                                                     // i9
		"<password> TODO </password>",                                                                       // i9
		"usage: tool {--password PASSWORD}",                                                                 // i10
		"usage: tool <--token TOKEN>",                                                                       // i10
		"usage: tool {-u user:password}",                                                                    // i10
		"<--password PASSWORD>",                                                                             // i10
		"usage: tool [-h] [--api-token TOKEN] [--sasl.password SASL_PASSWORD]",                              // i11
		"usage: psql [OPTION]... [--password PASSWD]",                                                       // i11
		"(--token TOKEN_FILE)",                                                                              // i11
		"psql -W|--password orders_v2",                                                                      // i11
		"tool --token=v2",                                                                                   // i12
		"tool --token=orders_v2",                                                                            // i12
		"tool --token=ab12",                                                                                 // i12
		"tool --auth-token=3600",                                                                            // i12
		"tool --session-token=abc_def_1",                                                                    // i12
		"tool --page-token=next_page_2",                                                                     // i12
		"tool --api-token=TOKEN_FROM_ENV",                                                                   // i12
		"tool --max-token=4096 orders",                                                                      // i12
		"Error: --token=Bearer is not allowed",                                                              // i12
		"tool --token \"$(gcloud auth print-access-token)\" orders",                                         // i13
		"psql --password \"$(cat /run/secrets/pg)\" orders",                                                 // i13
		"tool --token=$(cat /run/token) orders",                                                             // i13
	}
	for _, in := range keep {
		if got, _ := Text(in); got != in {
			t.Errorf("changed evidence: %q -> %q", in, got)
		}
	}
	exact := []struct{ in, want string }{
		{"curl -u admin:123456789012345678 http://x/", "curl -u admin:[redacted 24] http://x/"},                                                  // i6
		{"curl --user api:9876543210987 https://x/", "curl --user api:[redacted 16] https://x/"},                                                 // i6
		{"curl -U admin:9999999999 http://x/", "curl -U admin:[redacted 16] http://x/"},                                                          // i6
		{"curl -U admin:9876543210 http://proxy/", "curl -U admin:[redacted 16] http://proxy/"},                                                  // i6
		{"curl -u admin:20231015 http://x/", "curl -u admin:[redacted 8] http://x/"},                                                             // i6
		{"curl -u admin:12345678 http://connect:8083/connectors", "curl -u admin:[redacted 8] http://connect:8083/connectors"},                   // i6
		{"curl -u admin:12 http://x/", "curl -u admin:[redacted 8] http://x/"},                                                                   // i6
		{"curl -u admin:correct-horse-battery-staple http://x/", "curl -u admin:[redacted 32] http://x/"},                                        // i6
		{"curl -u svc_kafka:blue_ocean_tiger_lamp http://connect:8083/", "curl -u svc_kafka:[redacted 24] http://connect:8083/"},                 // i6
		{"curl -u admin:hunter-two http://x/", "curl -u admin:[redacted 16] http://x/"},                                                          // i6
		{"curl -u admin:pass-word http://x/", "curl -u admin:[redacted 16] http://x/"},                                                           // i6
		{"curl -u svc:secret_key_prod http://x/", "curl -u svc:[redacted 16] http://x/"},                                                         // i6
		{"{\"msg\":\"bad --password\",\"db_password\":\"Zx9hunter2Qw7Pa55\"}", "{\"msg\":\"bad --password\",\"db_password\":\"[redacted 24]\"}"}, // i7
		{"{--password Zx9hunter2Qw7Pa55}", "{--password [redacted 24]}"},                                                                         // i10
		{"<--password Zx9hunter2Qw7Pa55>", "<--password [redacted 24]>"},                                                                         // i10
		{"curl --user=admin:12345678 http://x/", "curl --user=admin:[redacted 8] http://x/"},                                                     // i16
		{"x --password=Zx9hunter2Qw7Pa55] y", "x --password=[redacted 24]] y"},                                                                   // i16
		{"psql --password correcthorsebatterystaple orders", "psql --password [redacted 32] orders"},                                             // nits
		{"tool --token qwertyuiopasdfghjklzxcvb", "tool --token [redacted 24]"},                                                                  // nits
		{"psql --password letmein! orders", "psql --password [redacted 8] orders"},                                                               // nits
		{"curl -u admin:letmein http://x/", "curl -u admin:[redacted 8] http://x/"},                                                              // nits
		{"<password>\\tZx9hunter2Qw7Pa55</password>", "<password>[redacted 24]</password>"},                                                      // nits
	}
	for _, tc := range exact {
		if got, _ := Text(tc.in); got != tc.want {
			t.Errorf("Text(%q)\n got  %q\n want %q", tc.in, got, tc.want)
		}
	}
}

// idempotenceSeeds383 are shapes where a quoted value's run-on once broke
// Text(Text(x)) == Text(x); FuzzText starts from them too.
var idempotenceSeeds383 = []string{
	"I ran `psql --password \"Zx9 hunter2Qw7Pa55\"` and it failed",
	"tool --token \"abc def\"/v1 --url \"http://x\"",
	"psql -h db --password \"Zx9 hunter2Qw7Pa55\"x -c \"select 1\"",
	"psql --password \"Zx9 hunter2Qw7Pa55\"\" orders",
	"Run `mongosh --password \"Zx9hunter2Qw7Pa55\"` then check pg-orders-connector lag",
	"--password \"Zx9hunter2Qw7Pa55\".",
}

// TestReview383Round5 holds the rows of the round-4 review of #415.
func TestReview383Round5(t *testing.T) {
	// Item 1: a mask the #372 rule closed with a quote is not run on.
	// Items 2 and 3: values main masks, and secrets in padded XML.
	exact := []struct{ in, want string }{
		{"Run `mongosh --password \"Zx9hunter2Qw7Pa55\"` then check pg-orders-connector lag on orders.public.orders",
			"Run `mongosh --password \"[redacted 24]\"` then check pg-orders-connector lag on orders.public.orders"},
		{"Use --password \"Zx9hunter2Qw7Pa55\". Then restart pg-orders-connector at lsn 0/16B3748.",
			"Use --password \"[redacted 24]\". Then restart pg-orders-connector at lsn 0/16B3748."},
		{"I ran docker login --password \"Zx9hunter2Qw7Pa55\": it failed with 401 on registry.example.com",
			"I ran docker login --password \"[redacted 24]\": it failed with 401 on registry.example.com"},
		{"--password \"Zx9hunter2Qw7Pa55\".", "--password \"[redacted 24]\"."},
		{"mongosh --username debezium --password dbz --authenticationDatabase admin",
			"mongosh --username debezium --password [redacted 8] --authenticationDatabase admin"},
		{"kafka-tool --keystore-password changeit --truststore-password changeit",
			"kafka-tool --keystore-password [redacted 8] --truststore-password [redacted 8]"},
		{"rpk cluster info --user superuser --password redpanda --sasl-mechanism SCRAM-SHA-256",
			"rpk cluster info --user superuser --password [redacted 8] --sasl-mechanism SCRAM-SHA-256"},
		{"docker login -u ci --password letmein registry.example.com", "docker login -u ci --password [redacted 8] registry.example.com"},
		{"docker login -u admin --password changeme registry.example.com", "docker login -u admin --password [redacted 8] registry.example.com"},
		{"influx -username admin -password supersecret -host db", "influx -username admin -password [redacted 16] -host db"},
		{"confluent kafka topic produce orders --api-key ABC --api-secret mysecret",
			"confluent kafka topic produce orders --api-key ABC --api-secret [redacted 8]"},
		{"mongosh --username root --password example", "mongosh --username root --password [redacted 8]"},
		{"clickhouse-client --password secret --query \"select 1\"", "clickhouse-client --password [redacted 8] --query \"select 1\""},
		{"tool --passphrase hello", "tool --passphrase [redacted 8]"},
		{"tool --password correcthorse", "tool --password [redacted 16]"},
		{"helm repo add charts https://x --username u --password qwertyuiopas",
			"helm repo add charts https://x --username u --password [redacted 16]"},
	}
	for _, tc := range exact {
		if got, _ := Text(tc.in); got != tc.want {
			t.Errorf("Text(%q)\n got  %q\n want %q", tc.in, got, tc.want)
		}
	}
	leaks := []string{
		"I ran `psql --password \"Zx9 hunter2Qw7Pa55\"` and it failed",
		"psql -h db --password \"Zx9 hunter2Qw7Pa55\"x -c \"select 1\"",
		"psql --password \"Zx9 hunter2Qw7Pa55\"\" orders",
		// Item 3: upper-case and digit secrets in padded or own-line XML.
		"<password>\n  JBSWY3DPEHPK3PXP\n</password>",
		"<token>\n  A94A8FE5CCB19BA61C4C0873D391E987982FBBD3\n</token>",
		"<sasl.jaas.password>\n  K7Q2M9X4P8R3T6W1\n</sasl.jaas.password>",
		"<password> JBSWY3DPEHPK3PXP </password>",
		"<ns:password>\r\n  JBSWY3DPEHPK3PXP\r\n</ns:password>",
		"<password type=\"plain\">\n  ORACLEPW2024\n</password>",
	}
	for _, in := range leaks {
		got, _ := Text(in)
		for _, sec := range []string{"hunter2Qw7", "JBSWY3DPEHPK3PXP", "A94A8FE5CCB1", "K7Q2M9X4P8R3T6W1", "ORACLEPW2024"} {
			if strings.Contains(got, sec) {
				t.Errorf("leak: %q -> %q", in, got)
			}
		}
	}
	keep := []string{
		"<password> TODO </password>",
		"<password>\n  TODO\n</password>",
		"<password> CHANGE_ME </password>",
	}
	for _, in := range keep {
		if got, _ := Text(in); got != in {
			t.Errorf("changed evidence: %q -> %q", in, got)
		}
	}
	for _, in := range append(append([]string{}, idempotenceSeeds383...), leaks...) {
		once, _ := Text(in)
		if twice, _ := Text(once); twice != once {
			t.Errorf("not idempotent: %q\n once  %q\n twice %q", in, once, twice)
		}
	}
}

// TestReview383Idempotent checks Text(Text(x)) == Text(x) over every string
// literal in this file, alone and with a secret appended in three ways.
func TestReview383Idempotent(t *testing.T) {
	fs := token.NewFileSet()
	f, err := parser.ParseFile(fs, "review383_test.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	const sec = "Zx9hunter2Qw7Pa55"
	n := 0
	ast.Inspect(f, func(node ast.Node) bool {
		lit, ok := node.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		v, err := strconv.Unquote(lit.Value)
		if err != nil {
			return true
		}
		for _, in := range []string{v, v + sec, v + " " + sec + " tail", v + "\"" + sec + "\" x"} {
			n++
			once, _ := Text(in)
			if twice, _ := Text(once); twice != once {
				t.Errorf("not idempotent: %q\n once  %q\n twice %q", in, once, twice)
			}
		}
		return true
	})
	if n < 1000 {
		t.Fatalf("only %d inputs", n)
	}
}

// secretRow is one leak row: the parts of the secret listed must not survive.
type secretRow struct {
	name, in string
	secrets  []string
}

// checkSecretRows runs leak rows through Text and checks idempotence.
func checkSecretRows(t *testing.T, rows []secretRow) {
	t.Helper()
	for _, tc := range rows {
		got, _ := Text(tc.in)
		for _, s := range tc.secrets {
			if strings.Contains(got, s) {
				t.Errorf("%s: leak of %q: %q -> %q", tc.name, s, tc.in, got)
			}
		}
		if again, _ := Text(got); again != got {
			t.Errorf("%s: not idempotent: %q -> %q -> %q", tc.name, tc.in, got, again)
		}
	}
}

// checkExact pins whole outputs, so a fix cannot pass by masking far more
// than the secret.
func checkExact(t *testing.T, rows []struct{ in, want string }) {
	t.Helper()
	for _, tc := range rows {
		if got, _ := Text(tc.in); got != tc.want {
			t.Errorf("Text(%q)\n got  %q\n want %q", tc.in, got, tc.want)
		}
	}
}

// checkKeep holds evidence that only looks like a secret form unchanged.
func checkKeep(t *testing.T, rows []string) {
	t.Helper()
	for _, in := range rows {
		if got, _ := Text(in); got != in {
			t.Errorf("changed evidence: %q -> %q", in, got)
		}
	}
}

// review510Leaks are the texts of #510 and variants of them: XML secrets
// padded or on their own line with a plain-word value, and secrets named by
// a key= or name= attribute. debezium and postgres are this stack's default
// passwords.
var review510Leaks = []secretRow{
	// Item 1: padded or own-line plain words and short capitals.
	{"own line debezium", "<password>\n  debezium\n</password>", []string{"debezium"}},
	{"own line postgres", "<password>\n    postgres\n  </password>", []string{"postgres"}},
	{"own line correcthorse", "<password>\n  correcthorse\n</password>", []string{"correcthorse"}},
	{"own line capitals", "<password>\n  ORACLE19C\n</password>", []string{"ORACLE19C"}},
	{"padded capitals", "<password> HUNTER2 </password>", []string{"HUNTER2"}},
	{"own line tab", "<password>\n\tdebezium\n</password>", []string{"debezium"}},
	{"own line CRLF", "<password>\r\n  debezium\r\n</password>", []string{"debezium"}},
	{"own line JSON-escaped (guard: main masks it too)", `<password>\n  debezium\n</password>`, []string{"debezium"}},
	{"own line dotted name", "<db.password>\n  postgres\n</db.password>", []string{"postgres"}},
	{"own line namespace", "<ns:password>\n  debezium\n</ns:password>", []string{"debezium"}},
	{"own line attribute", "<password type=\"plain\">\n  debezium\n</password>", []string{"debezium"}},
	{"own line token capitals", "<token>\n  ABCXYZ\n</token>", []string{"ABCXYZ"}},
	{"own line two words", "<password>\n  correct horse\n</password>", []string{"correct horse"}},
	{"padded capitals with digits", "<secret> QW7PA55 </secret>", []string{"QW7PA55"}},
	{"break before only", "<password>\n  debezium</password>", []string{"debezium"}},
	{"break after only", "<password>debezium\n</password>", []string{"debezium"}},
	// Item 2: a secret named by an attribute.
	{"properties entry", `<entry key="jdbc.password">Zx9hunter2Qw7Pa55</entry>`, []string{"hunter2Qw7"}},
	{"appSettings add", `<add key="DbPassword" value="Zx9hunter2Qw7Pa55"/>`, []string{"hunter2Qw7"}},
	{"hibernate property", `<property name="hibernate.connection.password" value="Zx9hunter2Qw7Pa55"/>`, []string{"hunter2Qw7"}},
	{"tomcat parameter", `<Parameter name="password" value="Zx9hunter2Qw7Pa55" override="false"/>`, []string{"hunter2Qw7"}},
	{"single quotes", `<add key='DbPassword' value='Zx9hunter2Qw7Pa55'/>`, []string{"hunter2Qw7"}},
	{"value before key", `<add value="Zx9hunter2Qw7Pa55" key="DbPassword"/>`, []string{"hunter2Qw7"}},
	{"blanks around =", `<add key = "DbPassword" value = "Zx9hunter2Qw7Pa55" />`, []string{"hunter2Qw7"}},
	{"upper-case attribute names", `<add KEY="DbPassword" VALUE="Zx9hunter2Qw7Pa55"/>`, []string{"hunter2Qw7"}},
	{"entry padded", `<entry key="sasl.token"> Zx9hunter2Qw7Pa55 </entry>`, []string{"hunter2Qw7"}},
	{"entry plain word", `<entry key="db.password">debezium</entry>`, []string{"debezium"}},
	{"property text", `<property name="password">Zx9hunter2Qw7Pa55</property>`, []string{"hunter2Qw7"}},
	{"client secret", `<add key="ClientSecret" value="Zx9hunter2Qw7Pa55"/>`, []string{"hunter2Qw7"}},
	{"param token", `<param name="api.token" value="Zx9hunter2Qw7Pa55"/>`, []string{"hunter2Qw7"}},
	{"value with blanks", `<add key="DbPassword" value="Zx9 hunter2Qw7Pa55"/>`, []string{"hunter2Qw7"}},
	{"attributes over lines", "<add key=\"DbPassword\"\n     value=\"Zx9hunter2Qw7Pa55\"/>", []string{"hunter2Qw7"}},
	{"after other XML", `<appSettings><add key="DbPassword" value="Zx9hunter2Qw7Pa55"/></appSettings>`, []string{"hunter2Qw7"}},
	{"entry own line", "<entry key=\"jdbc.password\">\n  postgres\n</entry>", []string{"postgres"}},
}

var review510Keep = []string{
	`<entry key="jdbc.url">jdbc:postgresql://db:5432/orders</entry>`,
	`<add key="DbPassword" value=""/>`,
	`<add key="DbPassword" value="${DB_PASSWORD}"/>`,
	`<property name="hibernate.connection.password" value="${db.password}"/>`,
	`<property name="password.policy" value="strict"/>`,
	`<entry key="jdbc.password"/>`,
	`<entry key="topics">orders.public.orders</entry>`,
	`<add key="TokenTtl" value="3600"/>`,
	"<password>\n  TODO\n</password>",
	"<password>\n  CHANGE_ME\n</password>",
	"<password>\n  changeme\n</password>",
	"<password> CHANGE_ME </password>",
	"<password> YOUR_PASSWORD </password>",
	"<password> and </password>",
	"between <password> and </password>",
	"<token>\n  replace with the vault token\n  and restart\n</token>",
}

var review510Exact = []struct{ in, want string }{
	{"<password>\n  debezium\n</password>", "<password>\n  [redacted 8]\n</password>"},
	{"<password>\n    postgres\n  </password>", "<password>\n    [redacted 8]\n  </password>"},
	{"<password> HUNTER2 </password>", "<password> [redacted 8] </password>"},
	{`<entry key="jdbc.password">Zx9hunter2Qw7Pa55</entry>`, `<entry key="jdbc.password">[redacted 24]</entry>`},
	{`<add key="DbPassword" value="Zx9hunter2Qw7Pa55"/>`, `<add key="DbPassword" value="[redacted 24]"/>`},
	{`<property name="hibernate.connection.password" value="Zx9hunter2Qw7Pa55"/>`, `<property name="hibernate.connection.password" value="[redacted 24]"/>`},
	{`<Parameter name="password" value="Zx9hunter2Qw7Pa55" override="false"/>`, `<Parameter name="password" value="[redacted 24]" override="false"/>`},
	{`<add key="DbPwd" value="Zx9hunter2Qw7Pa55"/>`, `<add key="DbPwd" value="[redacted 24]"/>`},
	{`<entry key="jdbc.password"><![CDATA[Zx9hunter2Qw7Pa55]]></entry>`, `<entry key="jdbc.password"><![CDATA[[redacted 24]]]></entry>`},
	{`<entry key="jdbc.password">Zx9hunter2Qw7Pa55</entry> <entry key="jdbc.user">loader</entry>`, `<entry key="jdbc.password">[redacted 24]</entry> <entry key="jdbc.user">loader</entry>`},
}

// TestReview510 covers #510 items 1 and 2.
func TestReview510(t *testing.T) {
	checkSecretRows(t, review510Leaks)
	checkKeep(t, review510Keep)
	checkExact(t, review510Exact)
}

// review510Cut are the #510 item 3 shapes: an element whose text sits on
// its own line, placed so that it straddles the MaxScan cut.
var review510Cut = []string{
	"<password>\n  Zx9hunter2Qw7Pa55\n</password>",
	"<password><![CDATA[\nZx9hunter2Qw7Pa55\n]]></password>",
	"<secret>\n<value>Zx9hunter2Qw7Pa55</value>\n</secret>",
	"<entry key=\"jdbc.password\">\n  Zx9hunter2Qw7Pa55\n</entry>",
	"<password>\n  debezium\n</password>",
}

// TestReview510ScanCut: no part of an element's secret passes in the head
// when the element straddles the scan limit, at every offset around it.
func TestReview510ScanCut(t *testing.T) {
	line := "evidence lsn 0/16B3748 topic orders.public.orders\n"
	for _, el := range review510Cut {
		for d := 1; d <= len(el)+8; d++ {
			fill := strings.Repeat(line, (MaxScan-d-1)/len(line))
			fill += strings.Repeat("x", MaxScan-d-len(fill)-1) + "\n"
			in := fill + el + "\nafter\n"
			got, _ := Text(in)
			for _, sec := range []string{"hunter2Qw7", "debezium"} {
				if strings.Contains(got[len(fill)-1:], sec) {
					t.Errorf("offset %d: %q leaks %q: tail %q", d, el, sec, got[len(fill)-1:])
				}
			}
			// A mask longer than its value can push an input of exactly
			// MaxScan past the limit, and the second call then caps it; main
			// does the same for any value masked there.
			if again, _ := Text(got); again != got && len(in) > MaxScan {
				t.Errorf("offset %d: %q not idempotent", d, el)
			}
		}
	}
}

// review511Leaks are the texts of #511 and variants: shell-quoted flag
// values.
var review511Leaks = []secretRow{
	{"ansi-c quoted with blank", "psql --password $'Zx9 hunter2Qw7Pa55' orders", []string{"hunter2Qw7", "Zx9"}},
	{"ansi-c quoted", "kcat --sasl.password $'Zx9hunter2Qw7Pa55'", []string{"hunter2Qw7"}},
	{"backslash-escaped blank", "psql --password Zx9\\ hunter2Qw7Pa55 orders", []string{"hunter2Qw7"}},
	{"glued quote runs", "psql --password 'Zx9'\\''hunter2Qw7Pa55'", []string{"hunter2Qw7"}},
	{"double then single quotes", "psql --password \"Zx9\"'hunter2Qw7Pa55'", []string{"hunter2Qw7"}},
	{"typographic double quotes", "psql --password “Zx9 hunter2Qw7Pa55”", []string{"hunter2Qw7"}},
	{"ansi-c escaped quote", "psql --password $'Zx9\\'hunter2Qw7Pa55' orders", []string{"hunter2Qw7"}},
	{"ansi-c after =", "psql --password=$'Zx9hunter2Qw7Pa55'", []string{"hunter2Qw7"}},
	{"ansi-c single dash", "tool -password $'Zx9 hunter2Qw7Pa55'", []string{"hunter2Qw7"}},
	{"two escaped blanks", "kcat --sasl.password Zx9\\ hunter2Qw7Pa55\\ x -b k:9092", []string{"hunter2Qw7"}},
	{"three glued runs", "psql --password \"Zx9\"'hunter2Qw7Pa55'\"x\"", []string{"hunter2Qw7"}},
	{"single then double quotes", "psql --password 'Zx9'\"hunter2Qw7Pa55\"", []string{"hunter2Qw7"}},
	{"bare then quoted", "psql --password Zx9'hunter2Qw7Pa55'", []string{"hunter2Qw7"}},
	{"typographic single quotes", "psql --password ‘Zx9 hunter2Qw7Pa55’", []string{"hunter2Qw7"}},
	{"typographic after =", "psql --password=“Zx9 hunter2Qw7Pa55”", []string{"hunter2Qw7"}},
	{"typographic in prose", "I ran psql --password “Zx9 hunter2Qw7Pa55” and it failed", []string{"hunter2Qw7"}},
	{"windows set", "set PGPASSWORD=Zx9 hunter2Qw7Pa55", []string{"hunter2Qw7"}},
	{"windows SET", "SET DB_PASSWORD=Zx9 hunter2Qw7Pa55", []string{"hunter2Qw7"}},
}

var review511Keep = []string{
	"psql --password ${PGPASSWORD} orders",
	"echo $'\\n' --password-file x",
	"set PATH=C:\\tools",
	"pg_dump --password-file /etc/pg/pass orders",
	"run --secret-name orders-db",
	"psql -U postgres -h db orders",
	"it’s the “orders” topic, see --token-ttl",
	"set PGHOST=db",
}

var review511Exact = []struct{ in, want string }{
	{"psql --password $'Zx9 hunter2Qw7Pa55' orders", "psql --password [redacted 24] orders"},
	{"psql --password Zx9\\ hunter2Qw7Pa55 orders", "psql --password [redacted 24] orders"},
	{"psql --password 'Zx9'\\''hunter2Qw7Pa55' orders", "psql --password [redacted 24] orders"},
	{"psql --password \"Zx9\"'hunter2Qw7Pa55' orders", "psql --password [redacted 24] orders"},
	{"psql --password “Zx9 hunter2Qw7Pa55” orders", "psql --password [redacted 24] orders"},
	{"set PGPASSWORD=Zx9 hunter2Qw7Pa55", "set PGPASSWORD=[redacted 24]"},
	{`set "PGPASSWORD=Zx9 hunter2Qw7Pa55"`, `set "PGPASSWORD=[redacted 24]"`},
	{`C:\> set PGPASSWORD=Zx9 hunter2Qw7Pa55`, `C:\> set PGPASSWORD=[redacted 24]`},
	// A quote that never closes after the value belongs to the JSON around
	// the command, not to the value.
	{`{"cmd":"psql --password “Zx9 hunter2Qw7Pa55”"}`, `{"cmd":"psql --password [redacted 24]"}`},
	{`{"cmd":"psql --password 'Zx9'\''hunter2Qw7Pa55'"}`, `{"cmd":"psql --password [redacted 24]"}`},
}

// TestReview511Gaps pins shapes next to #511 that stay open; each row
// gives today's output.
func TestReview511Gaps(t *testing.T) {
	rows := []struct{ gap, in, now string }{
		{"an environment assignment (not a flag) with ANSI-C quoting (main too)",
			"export PGPASSWORD=$'Zx9 hunter2Qw7Pa55'",
			"export PGPASSWORD=$'Zx9 hunter2Qw7Pa55'"},
		{"a braced SQL Server value split across lines (main too)",
			"password = {Br@ce\n;Pw42x}",
			"password = [redacted 8]\n;Pw42x}"},
	}
	for _, r := range rows {
		if got, _ := Text(r.in); got != r.now {
			t.Errorf("gap %q moved: Text(%q)\n got  %q\n pinned %q", r.gap, r.in, got, r.now)
		}
	}
}

// TestReview511 covers #511.
func TestReview511(t *testing.T) {
	checkSecretRows(t, review511Leaks)
	checkKeep(t, review511Keep)
	checkExact(t, review511Exact)
}

// slowLines510 are hostile inputs for the passes of #510, #511 and #534.
// Each is checked at 64 KiB, and the cut ones past MaxScan.
var slowLines510 = map[string]func(n int) string{
	"attr-open-values":  func(n int) string { return strings.Repeat(`<add key="DbPassword" value="`, n/30) },
	"attr-named-tags":   func(n int) string { return strings.Repeat(`<entry key="jdbc.password">`, n/27) },
	"attr-many":         func(n int) string { return "<a " + strings.Repeat(`b="c" `, n/6) },
	"attr-no-close":     func(n int) string { return strings.Repeat(`<add key="token" `, n/17) },
	"attr-entry-text":   func(n int) string { return strings.Repeat("<entry key=\"x.token\">\n  a\n", n/26) },
	"own-line-elements": func(n int) string { return strings.Repeat("<password>\n  debezium\n", n/22) },
	"cut-open-tags":     func(n int) string { return strings.Repeat("<password>\n", n/11+1000) },
	"cut-attr-tags":     func(n int) string { return strings.Repeat("<entry key=\"a.password\">\n", n/25+1000) },
	"cut-end-tags":      func(n int) string { return strings.Repeat("<password></password>", n/21+1000) },
	// #511: shell words that never close or glue many runs, and many
	// masks for project to place.
	"shell-ansi-open":    func(n int) string { return strings.Repeat("--password $'", n/13) },
	"shell-blanks":       func(n int) string { return "--password " + strings.Repeat(`a\ `, n/3) },
	"shell-glued":        func(n int) string { return "--password " + strings.Repeat(`'a'"b"`, n/6) },
	"shell-flags-glued":  func(n int) string { return strings.Repeat(`--password 'a''b' `, n/18) },
	"shell-typographic":  func(n int) string { return strings.Repeat("--password “", n/14) },
	"shell-set-lines":    func(n int) string { return strings.Repeat("set PGPASSWORD=Zx9 hunter2Qw7\n", n/30) },
	"shell-many-masks":   func(n int) string { return strings.Repeat("password=Zx9hunter2Qw7 --password $'a b' ", n/41) },
	"shell-masks-repeat": func(n int) string { return strings.Repeat("password=Zx9hunter2Qw7 x ", n/25) + "--password $'a b'" },
	// #534: braces that never close, escapes, and many short values.
	"braced-open":    func(n int) string { return strings.Repeat("password={", n/10) },
	"braced-escapes": func(n int) string { return "pwd={" + strings.Repeat("}}", n/2) },
	"braced-many":    func(n int) string { return strings.Repeat("Pwd={a;b};", n/10) },
	"braced-url":     func(n int) string { return strings.Repeat("jdbc:sqlserver://db:1433;password={B@c;d};", n/43) },
	// Round 1 of the #547 review: unbounded and bare attributes, more quote
	// styles, apostrophes, and quoted braced keys.
	"attr-bare-many":  func(n int) string { return "<add " + strings.Repeat("b=c ", n/4) },
	"attr-key-bare":   func(n int) string { return strings.Repeat("<add key=DbPassword value=", n/26) },
	"typo-open":       func(n int) string { return strings.Repeat("--password „", n/14) },
	"typo-apostrophe": func(n int) string { return "--password ‘" + strings.Repeat("it’s ", n/7) },
	"braced-quoted":   func(n int) string { return strings.Repeat("\"Password\"={", n/13) },
	"padded-words":    func(n int) string { return strings.Repeat("<password> debezium </password>", n/31) },
}

// TestReview510LinearTime: every hostile 64 KiB line runs in under 50 ms.
func TestReview510LinearTime(t *testing.T) {
	if raceEnabled {
		t.Skip("timing check; runs without -race")
	}
	const ceiling = 50 * time.Millisecond
	for name, mk := range slowLines510 {
		s := mk(MaxScan)
		best := time.Duration(1 << 62)
		for range 5 {
			start := time.Now()
			Text(s)
			best = min(best, time.Since(start))
		}
		if best > ceiling {
			t.Errorf("%s: best %v for %d KiB, want under %v", name, best, len(s)>>10, ceiling)
		}
	}
}

// review547Leaks are the texts of the round-1 review of #547 (LEAD-REVIEW,
// 0a4b8f8): own-line or padded values that only contain a placeholder word,
// one-line plain words padded with blanks or tabs (must-fix 1 and 2), and
// the shapes of nits 3 to 5.
var review547Leaks = []secretRow{
	// Must-fix 1: a placeholder word inside a real value is not a placeholder.
	{"own line changeit", "<password>\n  changeit\n</password>", []string{"changeit"}},
	{"entry own line changeit", "<entry key=\"ssl.truststore.password\">\n  changeit\n</entry>", []string{"changeit"}},
	{"own line supersecret", "<password>\n  supersecret\n</password>", []string{"supersecret"}},
	{"own line mypassword", "<password>\n  mypassword\n</password>", []string{"mypassword"}},
	{"own line yourmom", "<password>\n  yourmom\n</password>", []string{"yourmom"}},
	{"own line tokenring", "<password>\n  tokenring\n</password>", []string{"tokenring"}},
	{"padded CHANGEIT", "<password> CHANGEIT </password>", []string{"CHANGEIT"}},
	{"padded MYSECRET1", "<password> MYSECRET1 </password>", []string{"MYSECRET1"}},
	{"padded XXXHUNTER", "<password> XXXHUNTER </password>", []string{"XXXHUNTER"}},
	{"padded NULLHUNTER9", "<password> NULLHUNTER9 </password>", []string{"NULLHUNTER9"}},
	// Must-fix 2: a one-line plain word padded with blanks or tabs.
	{"padded debezium", "<password> debezium </password>", []string{"debezium"}},
	{"tab-padded postgres", "<password>\tpostgres\t</password>", []string{"postgres"}},
	{"padded secret element", "<secret> debezium </secret>", []string{"debezium"}},
	{"entry padded debezium", "<entry key=\"jdbc.password\">  debezium  </entry>", []string{"debezium"}},
	// Nit 3: more quote styles, and an apostrophe inside typographic quotes.
	{"low-high quotes", "psql --password „Zx9 hunter2Qw7Pa55“", []string{"hunter2Qw7"}},
	{"guillemets", "psql --password «Zx9 hunter2Qw7Pa55»", []string{"hunter2Qw7"}},
	{"fullwidth quotes", "psql --password ＂Zx9 hunter2Qw7Pa55＂", []string{"hunter2Qw7"}},
	{"apostrophe inside typographic quotes", "psql --password ‘Zx9 it’s hunter2Qw7Pa55’", []string{"hunter2Qw7"}},
	// Nit 4: a quoted key before a braced value.
	{"quoted braced key", "Server=db;\"Password\"={Br@ce;Pw42x};", []string{"Pw42x"}},
	// Nit 5: value= after many attributes, and unquoted attributes.
	{"value after 16 attributes", "<add a1=\"1\" a2=\"2\" a3=\"3\" a4=\"4\" a5=\"5\" a6=\"6\" a7=\"7\" a8=\"8\" a9=\"9\" a10=\"10\" a11=\"11\" a12=\"12\" a13=\"13\" a14=\"14\" a15=\"15\" a16=\"16\" key=\"DbPassword\" value=\"Zx9hunter2Qw7Pa55\"/>", []string{"hunter2Qw7"}},
	{"unquoted attributes", "<add key=DbPassword value=Zx9hunter2Qw7Pa55 />", []string{"hunter2Qw7"}},
}

var review547Keep = []string{
	"<password>\n  TODO\n</password>",
	"<password> TODO </password>",
	"<password>\n  CHANGE_ME\n</password>",
	"<password> CHANGE_ME </password>",
	"<password>\n  ${DB_PASSWORD}\n</password>",
	"<password> ${DB_PASSWORD} </password>",
	"<password> YOUR_PASSWORD </password>",
	"<password>\n  changeme\n</password>",
	"<password> <REPLACE_ME> </password>",
	"<password> xxx </password>",
	"<password> and </password>",
	"<password>\tand\t</password>",
	"<password> see docs </password>",
	// Nit 6: a name= attribute on a schema element names a column or field,
	// not a secret.
	"<column name=\"password\">\n  VARCHAR\n</column>",
	"<field name=\"secret\">\n  SerializationException\n</field>",
	"<field name=\"password\">varchar(255) NOT NULL</field>",
	`<attribute name="token" type="string"/>`,
}

var review547Exact = []struct{ in, want string }{
	{"<password> debezium </password>", "<password> [redacted 8] </password>"},
	{"<password>\tpostgres\t</password>", "<password>\t[redacted 8]\t</password>"},
	{"<password>\n  changeit\n</password>", "<password>\n  [redacted 8]\n</password>"},
	{"psql --password „Zx9 hunter2Qw7Pa55“ orders", "psql --password [redacted 24] orders"},
	{"Server=db;\"Password\"={Br@ce;Pw42x};", "Server=db;\"Password\"=[redacted 16];"},
	// Nit 6: elements other than schema ones take a name= secret, so a
	// constant in a param is masked too: <param name="password"> is a real
	// config shape.
	{"<param name=\"token\">\n  TOPIC_ORDERS\n</param>", "<param name=\"token\">\n  [redacted 16]\n</param>"},
	// Nit 7: cmd's set takes the whole rest of the line, so the mask does.
	{"set API_TOKEN=see the vault entry for orders.public.orders, then SerializationException",
		"set API_TOKEN=[redacted 128]"},
}

// TestReview547Round1 holds the rows of the round-1 review of #547.
func TestReview547Round1(t *testing.T) {
	checkSecretRows(t, review547Leaks)
	checkKeep(t, review547Keep)
	checkExact(t, review547Exact)
	rows := []struct{ gap, in, now string }{
		{"a password glued to its flag (mysql -p'X') is not read (main too)",
			"mysql -p'Zx9'\"hunter2Qw7Pa55\"",
			"mysql -p'Zx9'\"hunter2Qw7Pa55\""},
	}
	for _, r := range rows {
		if got, _ := Text(r.in); got != r.now {
			t.Errorf("gap %q moved: Text(%q)\n got  %q\n pinned %q", r.gap, r.in, got, r.now)
		}
	}
}

// TestReview547CutProse: prose that names a secret element near the scan
// limit does not move the cut (nit 8); a start tag that opens its line does.
func TestReview547CutProse(t *testing.T) {
	line := "evidence lsn 0/16B3748 topic orders.public.orders\n"
	tail := "WARN no <password> given for pg-orders-connector\n"
	fill := strings.Repeat(line, (MaxScan-2000)/len(line))
	in := fill + tail + strings.Repeat(line, 4000/len(line))
	if got := scanCut(in); got < len(fill)+len(tail) {
		t.Errorf("cut moved to %d, before the prose line ends at %d", got, len(fill)+len(tail))
	}
}
