package strimzi

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// round14Secret marks the round 14 inputs' secrets.
const round14Secret = "S3cr3tPw9"

// round14Cases are the round 14 security re-review's leaks: every read
// must mask them. They are in the corpus.
func round14Cases() []corpusCase {
	s := round14Secret
	var cs []corpusCase
	add := func(line string, markers ...string) {
		if len(markers) == 0 {
			markers = []string{s}
		}
		cs = append(cs, corpusCase{name: "round 14 " + strings.ReplaceAll(line, "\n", `\n`), line: line, markers: markers})
	}
	for _, l := range []string{
		// 1: a CSV column under any line naming a credential key.
		"username,password,created_by\nalice," + s + ",root", "first_name,password,host\nalice," + s + ",db1",
		"user,password,host,port,tls\nalice," + s + ",db1,5432,true", "timestamp,user,password\n2026,alice," + s,
		"user, password, host\nalice, " + s + ", db1", "2026-10-31 INFO user,password,host\nalice," + s + ",db1",
		"admin,password,user\nx," + s + ",y", "password,token\nAn0therPw7," + s,
		"user;password;host\nalice;" + s + ";db1", "user|password|host\nalice|" + s + "|db1",
		"user\tpassword\thost\nalice\t" + s + "\tdb1", "user,password,host\n\nalice," + s + ",db1",
		// 3: mysql's -p in exec forms, quotes and escapes; mariadb; long
		// option prefixes.
		`command: ["mysql", "-uroot", "-p` + s + `", "db"]`, `"command":["mysql","-uroot","-p` + s + `"]`,
		"command:\n- mysql\n- -uroot\n- -p" + s, `mysql "-p` + s + `"`, "mysql -p''" + s, `mysql -p""` + s,
		`mysql -p"` + s + " rest", "mariadb -p" + s, "mariadb-dump -u app -p" + s, "mysqladmin -p" + s,
		"mysqlimport -p" + s, "mysqlpump -p" + s, "mysql --pas=" + s, "mysql --passw=" + s, "mysql --passwo=" + s,
		"mysql --passwor=" + s,
		// 4: session cookies and keys.
		"PHPSESSID=" + s, "JSESSIONID " + s, "session_key=" + s, "sessionId=" + s + ", epoch=INITIAL",
		// 5: a block indicator with a comment, tag or anchor.
		"password: | # c\n  " + s, "password: >- # c\n  " + s, "password: !!str |\n  " + s, "password: !!binary |\n  " + s,
		"password: &anchor |\n  " + s, "password: !!str &a | # c\n  " + s,
		// 7: a null or true that goes on, an empty value that goes on.
		"password=null&x=" + s, "password=null x=" + s, `password=null"x=` + s, "password=null'x:" + s,
		"password=true x=" + s, "password=," + s,
	} {
		add(l)
	}
	add("user,password,api_key\nalice,An0therKey7,"+s, s, "An0therKey7")
	add("mysql -pS3cr3t'Pw9'", "S3cr3t", "Pw9")
	add(`mysql -pS3cr3t\ Pw9`, "S3cr3t", "Pw9")
	// 4: a session id past an int32, or with a format character in it.
	add("sessionid=12345678901", "12345678901")
	add("sessionid=1234​5678", "5678")
	// 6: any blank between pass and its number.
	add("pass\t482193", "482193")
	add("pass \t482193", "482193")
	return cs
}

// round14KeepCases are Kafka's and ZooKeeper's session diagnostics:
// every read returns them unmasked. They are in the corpus.
func round14KeepCases() []corpusCase {
	var cs []corpusCase
	for _, l := range []string{
		"Error sending fetch request (sessionId=INVALID, epoch=INITIAL) to node 2: (org.apache.kafka.clients.FetchSessionHandler)",
		"processing sessionid:0x100000b3ca50000 type:create cxid:0x1 zxid:0x2",
		"Node 1 sent an invalid full fetch response with sessionId=2147483647, epoch=INITIAL",
	} {
		cs = append(cs, corpusCase{name: "round 14 keep " + l, line: l, keep: l})
	}
	return cs
}

// Round 14: every read masks the security re-review's inputs, and keeps
// the session diagnostics, the kept words before a credential key and a
// struct's null.
func TestRound14Regressions(t *testing.T) {
	for _, c := range round14Cases() {
		got := readsOf(t, nil, c.line)
		for _, m := range c.markers {
			if bad := leaksIn(got, m); len(bad) > 0 {
				t.Errorf("%q: %q leaks in %v", c.line, m, bad)
			}
		}
	}
	keeps := []string{"useBasicAuth=true requireAuth: true", "Struct{password=null, user=bob}", "pass: 3 fail: 0",
		"sessionId=1234567, epoch=INITIAL"}
	for _, c := range round14KeepCases() {
		keeps = append(keeps, c.keep)
	}
	for _, keep := range keeps {
		if got := readsOf(t, nil, keep)["k8s_logs"]; strings.Contains(got, "masked") {
			t.Errorf("%q is masked: %s", keep, got)
		}
	}
	// The header's names stay readable; its column is masked below.
	if got := readsOf(t, nil, "user;password;host\nalice;"+round14Secret+";db1")["k8s_logs"]; !strings.Contains(got, "user;password;") || !strings.Contains(got, "alice;[masked:credential];db1") {
		t.Errorf("semicolon table: %s", got)
	}
}

// Round 14, item 2: Jackson's message shape is matched once a line, not
// once a token, so a line of many skipped tokens costs linear time (the
// reviewer's inputs at a quarter and at full size: x4 is linear, x16
// quadratic). Ratios, so the race detector's slowdown does not matter.
// A loaded CI machine can stall one sample (x10.6 seen while the integration
// suite ran alongside), so the ratio is measured up to three times and fails
// only if every attempt is above the bound: quadratic code stays near x16 on
// every attempt, noise does not.
func TestRound14JacksonCostIsLinear(t *testing.T) {
	best := func(s string) time.Duration {
		var min time.Duration
		for i := 0; i < 5; i++ {
			start := time.Now()
			maskCredentials(s, nil)
			if d := time.Since(start); i == 0 || d < min {
				min = d
			}
		}
		return max(min, time.Millisecond)
	}
	for _, in := range []struct{ unit, tail string }{
		{"Unrecognized token 'a' x", "': was expecting"},
		{"Unexpected token '}' ", "at [Source: x]"},
	} {
		var tries []string
		for attempt := 0; attempt < 3; attempt++ {
			small := best(strings.Repeat(in.unit, 700) + in.tail)
			full := best(strings.Repeat(in.unit, 2800) + in.tail)
			r := float64(full) / float64(small)
			if r <= 9 {
				tries = nil
				break
			}
			tries = append(tries, fmt.Sprintf("x700 %v, x2800 %v (x%.1f)", small, full, r))
		}
		if tries != nil {
			t.Errorf("%q: not linear on every attempt: %s", in.unit, strings.Join(tries, "; "))
		}
	}
}
