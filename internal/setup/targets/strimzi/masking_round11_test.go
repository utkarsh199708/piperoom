package strimzi

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// leaksIn reports which of the four reads (and their summaries) hold
// marker.
func leaksIn(got map[string]string, marker string) []string {
	var bad []string
	for op, s := range got {
		if strings.Contains(s, marker) {
			bad = append(bad, op)
		}
	}
	return bad
}

// Round 11, item 1: a kept value (a cursor, a path, a switch, a resume
// position, a placeholder) is one token, cut at the first separator or
// credential key, and the scan goes on after it: a key after it is judged
// on its own, through every read.
func TestKeptValueNeverHidesKey(t *testing.T) {
	for _, kept := range []string{"nextToken=abc", "pageToken=a", "next_page_token: x", "PWD=/opt", "pwd=/home/u", "OLDPWD=/x",
		"resume token: abc", "httpAuth=true", "NOAUTH=true", "password=[hidden]", "secret=orders-db-credentials", "password=${file:/x:y}"} {
		for _, sep := range []string{"&", ",", ";", "|", `\`, "'", `"`, ")", " ", "#", "/", ":", "+", "?"} {
			line := kept + sep + "masterauth=" + corpusMarker
			if bad := leaksIn(readsOf(t, nil, line), corpusMarker); len(bad) > 0 {
				t.Errorf("%q leaks in %v", line, bad)
			}
		}
	}
	// What the kept value keeps, through k8s_logs.
	for _, c := range []struct{ in, want string }{
		{"nextToken=AbC123&masterauth=" + corpusMarker, "nextToken=AbC123\\u0026masterauth=[masked:credential]"},
		{"resume token: abc,masterauth=" + corpusMarker, "resume token: abc[masked:credential]"},
		{"PWD=/opt,masterauth=" + corpusMarker, "PWD=/opt,masterauth=[masked:credential]"},
	} {
		got := readsOf(t, nil, c.in)["k8s_logs"]
		// redact masks PWD's path on its own; only the key after it matters.
		if strings.Contains(got, corpusMarker) || !strings.Contains(got, "masterauth=[masked:credential]") && !strings.Contains(got, c.want) {
			t.Errorf("%q: %s", c.in, got)
		}
	}
}

// Round 11, F1: a pagination cursor or an exempt switch keeps or masks
// its own first token and nothing more; the rest of the line is judged on
// its own (enabled and disabled are kept for the switches).
func TestExemptValuesJudgeOneToken(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"OAUTH: true, listener=tls", ""},
		{"nextToken=AbC123, count=10", ""},
		{"?pageToken=AbC123&limit=10 took 12ms", ""},
		{"OAUTH: enabled", ""},
		{"requireAuth=disabled, mode=x", ""},
		{"NOAUTH=" + corpusMarker + ", listener=tls", "NOAUTH=[masked:credential], listener=tls"},
		{"nextToken=" + strings.Repeat("a", 70) + "&count=10", "nextToken=[masked:credential]&count=10"},
		{"requireAuth=-1.5 " + corpusMarker, "requireAuth=[masked:credential]"},
	} {
		want := c.want
		if want == "" {
			want = c.in
		}
		got := readsOf(t, nil, c.in)
		for _, op := range []string{"k8s_logs", "k8s_events", "strimzi_build_status"} {
			j, _ := json.Marshal(want)
			if !strings.Contains(got[op], strings.Trim(string(j), `"`)) {
				t.Errorf("%s: %q\n got %s\nwant %s", op, c.in, got[op], want)
			}
		}
	}
}

// Round 11, item 2: the substitute for 0x01 and invalid UTF-8 is '_', no
// delimiter: a '?' in the userinfo read as a URL query hid the password
// from redact's userinfo rule.
func TestSanitizedByteIsNoDelimiter(t *testing.T) {
	for _, b := range []string{"\x01", "\xff", "\xc3", "\xe2\x82"} {
		for _, line := range []string{
			"postgres://u" + b + ":" + corpusMarker + "@h/x",
			"postgres://u:" + corpusMarker + b + "@h/x",
			"Authorization: Bearer ab" + b + corpusMarker,
		} {
			if bad := leaksIn(readsOf(t, nil, line), corpusMarker); len(bad) > 0 {
				t.Errorf("%q leaks in %v", line, bad)
			}
		}
	}
}

// Round 11, F5: a written value with an invalid byte and a quote or
// backslash is found only by its per-byte U+FFFD form, in k8s_get's
// decoded strings (Java's escaping writes the quote escaped, Go's the
// byte as \ufffd).
func TestWrittenInvalidByteWithQuote(t *testing.T) {
	for _, w := range []string{"ab\xff\"" + corpusMarker, "ab\xff\\" + corpusMarker} {
		h := newHarness(t, "", nil)
		h.d.rememberWritten([]string{w})
		cp := h.shell("KafkaConnect", "orders-connect", map[string]any{"config": map[string]any{"note": "login with " + w + " refused"}})
		h.api.allow(cp)
		res, err := run(t, h, "k8s_get", map[string]any{"kind": "KafkaConnect", "name": "orders-connect"})
		if err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(res.Data)
		if strings.Contains(string(b), corpusMarker[:8]) {
			t.Errorf("written %q leaks: %s", w, b)
		}
	}
}

// Round 11, items 3 and 4: a quote opened inside a value is followed onto
// the next lines, and a value still open after maxContinuation lines is
// masked to the end of the read.
func TestContinuedValuesRound11(t *testing.T) {
	lines := []string{
		"password = x \"y\n" + corpusMarker + "\"",
		"token: a \"b c\nd " + corpusMarker + "\" done",
	}
	for _, n := range []int{maxContinuation - 1, maxContinuation, maxContinuation + 1, 100} {
		mid := strings.Repeat("line\n", n)
		lines = append(lines,
			"password=\"start\n"+mid+corpusMarker+"\"",
			"secret: 'start\n"+mid+corpusMarker+"'",
			"token=a\\\n"+strings.Repeat("b\\\n", n)+corpusMarker,
			"password=a%\n"+strings.Repeat("%\n", n)+corpusMarker,
			"password=\"a\r\n"+strings.Repeat("x\r\n", n)+corpusMarker+"\"\r")
	}
	for _, line := range lines {
		if bad := leaksIn(readsOf(t, nil, line), corpusMarker); len(bad) > 0 {
			t.Errorf("%.40q... (%d lines) leaks in %v", line, strings.Count(line, "\n"), bad)
		}
	}
	// A quote closing an outer string (msg="... token=abc") opens nothing.
	got := readsOf(t, nil, "msg=\"set token=abc\" level=info\nnext line stays")["k8s_logs"]
	if !strings.Contains(got, "next line stays") {
		t.Errorf("an outer string's closing quote continued the value: %s", got)
	}
}

// Round 11, item 6: the forms and keys that leaked before, through every
// read, and the messages around them that stay. (A CSV header,
// user,password,host, reads as a row: its next field is masked.)
func TestCredentialFormsRound11(t *testing.T) {
	m := corpusMarker
	for _, line := range []string{
		"password [" + m + "]", "password is " + m, "password\t" + m, "u,password," + m, "ALTER USER app WITH password '" + m + "';",
		"password: |\n  " + m + "\n", "password:\n  " + m + "\n", "password: >-\n  a\n\n  " + m + "\nnext: 1",
		"{'user': 'u', 'password': '" + m + "'}", "{'sasl.jaas.config': '" + m + "'}",
		`{"name":"DB_PASSWORD","value":"` + m + `"}`, `env=[{"name":"SECRET_KEY", "value": "` + m + `"}]`,
		`m="{\"name\":\"API_TOKEN\",\"value\":\"` + m + `\"}"`,
		"- name: DB_PASSWORD\n  value: " + m, "name=JWT value=" + m,
		"passphrase=" + m, "ssl.key.passphrase: " + m, "SECRET_KEY=" + m, "secret_key: " + m, "aws_secret_access_key=" + m,
		"CONNECT_PASS=" + m, "pass: " + m, "jwt=" + m, "session=" + m, "Cookie: a=" + m, "Set-Cookie: s=" + m,
		"password=>" + m, "password => " + m, "password := " + m, "> passphrase: Basic " + m, "--passphrase " + m, "-Dpass=" + m,
		"pass\u200bword=" + m, "pass\ufeffword=" + m, "\u200dsecret_key=" + m, "pass\u202eword=" + m,
		"AccountKey=" + m, "basic.auth.user.info=" + m, "mysql -uroot -p" + m + " db",
	} {
		if bad := leaksIn(readsOf(t, nil, line), m); len(bad) > 0 {
			t.Errorf("%q leaks in %v", line, bad)
		}
	}
	for _, keep := range []string{
		"password is required", "password is too short (min 8)", "password authentication failed for user app",
		"bypass=on", "session.timeout.ms=45000", "- name: LOG_LEVEL\n  value: debug",
		"credentials:\nnext: 1",
	} {
		got := readsOf(t, nil, keep)["k8s_logs"]
		if strings.Contains(got, "masked") {
			t.Errorf("%q is masked: %s", keep, got)
		}
	}
}

// Round 11, item 6: k8s_get masks a container env entry whose name is a
// credential key, and keeps one whose name is not.
func TestGetMasksEnvCredentials(t *testing.T) {
	h := newHarness(t, "", nil)
	cp := h.shell("KafkaConnect", "orders-connect", map[string]any{
		"template": map[string]any{"connectContainer": map[string]any{"env": []any{
			map[string]any{"name": "DB_PASSWORD", "value": corpusMarker + "a"},
			map[string]any{"name": "CONNECT_PASS", "value": corpusMarker + "b"},
			map[string]any{"name": "JWT", "value": corpusMarker + "c"},
			map[string]any{"name": "LOG_LEVEL", "value": "debug"},
			map[string]any{"name": "PWD", "value": "/opt/kafka"},
		}}},
	})
	h.api.allow(cp)
	res, err := run(t, h, "k8s_get", map[string]any{"kind": "KafkaConnect", "name": "orders-connect"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	if strings.Contains(string(b), corpusMarker) || !strings.Contains(string(b), `"debug"`) {
		t.Errorf("k8s_get env: %s", b)
	}
}

// Round 11, item 5: masking is linear in a string up to redact.MaxScan
// and flat past it (the rest is masked whole), for adversarial log lines
// and for one huge string in k8s_get. Ratios, not absolute times, so the
// race detector's slowdown does not matter.
func TestMaskingCostIsBounded(t *testing.T) {
	best := func(f func()) time.Duration {
		var min time.Duration
		for i := 0; i < 3; i++ {
			start := time.Now()
			f()
			if d := time.Since(start); i == 0 || d < min {
				min = d
			}
		}
		return max(min, time.Millisecond)
	}
	fill := func(unit string, n int) string { return strings.Repeat(unit, n/len(unit)+1)[:n] }
	for _, g := range []struct {
		unit    string
		written []string
	}{
		{"a=", nil}, {"pw=", nil}, {"nextToken=a&", nil}, {"password=x\\\n", nil}, {"password=\"x\n", nil},
		{"password: |\n  x\n", nil}, {"u,password,", nil}, {"password is x ", nil}, {"pass\u200b", nil},
		{"\xff", []string{"\xff\xff\xff\xff\xff\xff\xff\xff"}}, {"a", []string{"aaaaaaaa"}}, {`A`, []string{"AAAAAAAA"}},
	} {
		small := best(func() { maskCredentials(fill(g.unit, 16<<10), g.written) })
		full := best(func() { maskCredentials(fill(g.unit, 64<<10), g.written) })
		huge := best(func() { maskCredentials(fill(g.unit, 1<<20), g.written) })
		if r := float64(full) / float64(small); r > 12 {
			t.Errorf("%q: 16 KiB %v, 64 KiB %v (x%.1f): not linear", g.unit, small, full, r)
		}
		if r := float64(huge) / float64(full); r > 6 {
			t.Errorf("%q: 64 KiB %v, 1 MiB %v (x%.1f): not capped", g.unit, full, huge, r)
		}
	}
	// One 1 MiB string in a k8s_get object costs about what 64 KiB does,
	// and what is past the cap is masked.
	get := func(n int) (time.Duration, string) {
		h := newHarness(t, "", nil)
		cp := h.shell("KafkaConnect", "orders-connect", map[string]any{"config": map[string]any{"note": fill("pw=a=", n) + corpusMarker}})
		h.api.allow(cp)
		var out []byte
		d := best(func() {
			res, err := run(t, h, "k8s_get", map[string]any{"kind": "KafkaConnect", "name": "orders-connect"})
			if err != nil {
				t.Fatal(err)
			}
			out, _ = json.Marshal(res.Data)
		})
		return d, string(out)
	}
	full, _ := get(60 << 10)
	huge, out := get(1 << 20)
	if r := float64(huge) / float64(full); r > 8 || strings.Contains(out, corpusMarker) {
		t.Errorf("k8s_get: 60 KiB %v, 1 MiB %v (x%.1f), marker leaks %v", full, huge, r, strings.Contains(out, corpusMarker))
	}
}

// Round 11, F3: the kept values of TestKeptValuesRound10 through the
// reads. The key rule keeps them; redact still masks a path or a
// mechanism word under an auth or pwd key on its own (its rules do not
// know these keys' kinds), which is accepted: such a value is shown as
// redact's mask, never as this package's, and "[hidden]" is kept whole.
func TestKeptValuesThroughReads(t *testing.T) {
	for _, c := range []struct {
		in         string
		redactMask bool // redact masks the kept value on its own
	}{
		{"httpAuth: SCRAM-SHA-512", true},
		{"sasl.client.auth=required", false},
		{"masterauth: kerberos", false},
		{"proxyAuth=basic", false},
		{"OLDPWD=/opt/kafka", false},
		{"pwd: /home/kafka", true},
		{"PWD=/opt/kafka", true},
		{"password=[hidden]", false},
		{"ssl.key.password=[hidden], ssl.truststore.password=[hidden]", false},
		{"nextToken=AbC123", false},
		{"OAUTH: true", false},
	} {
		got := readsOf(t, nil, c.in)
		for _, op := range []string{"k8s_logs", "k8s_events", "strimzi_build_status"} {
			s := got[op]
			j, _ := json.Marshal(c.in)
			whole := strings.Contains(s, strings.Trim(string(j), `"`))
			switch {
			case strings.Contains(s, credentialMask):
				t.Errorf("%s: %q: the key rule masks a kept value: %s", op, c.in, s)
			case strings.Contains(s, "]]"):
				t.Errorf("%s: %q: a stray bracket: %s", op, c.in, s)
			case !c.redactMask && !whole:
				t.Errorf("%s: %q is not kept: %s", op, c.in, s)
			case c.redactMask && whole:
				t.Errorf("%s: %q: redact no longer masks it; make it a plain keep: %s", op, c.in, s)
			}
		}
	}
}
