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
