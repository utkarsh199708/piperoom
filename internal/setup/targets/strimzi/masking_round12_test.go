package strimzi

import (
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/redact"
)

// round12Secret is the secret the round 12 inputs carry.
const round12Secret = "Zq9hunterXK7"

// round12Cases are the round 12 security review's inputs: each must be
// masked by every read. They are also in the corpus (corpusCases).
func round12Cases() []corpusCase {
	s := round12Secret
	long := strings.Repeat("x", redact.MaxScan-20)
	var cs []corpusCase
	add := func(name, line string, written ...string) {
		cs = append(cs, corpusCase{name: "round 12 " + name, line: line, written: written, markers: []string{s}})
	}
	// F1: a written value or a key's value straddling redact's cut.
	add("F1 written straddles the cut", long+s+" Pq8ladderWM3 tail", s+" Pq8ladderWM3")
	add("F1 written straddles MaxScan", strings.Repeat("x", redact.MaxScan-5)+s+" tail", s)
	add("F1 key value straddles the cut", strings.Repeat("x", redact.MaxScan-30)+" masterauth="+s+"Pq8ladderWM3 tail")
	// F2: a kept value, a separator, then more of the value.
	for _, kept := range []string{"password=null", "password=[hidden]", "password=${x}", `password="null"`, "token=expired", "masterauth=plain", "NOAUTH=1", "nextToken=abc", "PWD=/opt"} {
		for _, sep := range []string{",", ";", "&", "|", ", ", ",\u200b"} {
			add("F2 "+kept+" "+sep, kept+sep+s)
		}
	}
	// F4: blanks past a search window between a key and its value.
	for _, pre := range []string{"", "x ", strings.Repeat("y", 500) + " "} {
		for _, k := range []string{"password=", "masterauth=", "OAUTH=", "pwd="} {
			add("F4 "+k+" after "+pre[:min(len(pre), 2)], pre+k+strings.Repeat(" ", 520)+s+"%")
		}
	}
	// F5: a long key's tail is no exempt name.
	for _, k := range []string{"nexttoken", "oauth", "pwd", "resumetoken"} {
		add("F5 dashes "+k, strings.Repeat("-", 600)+k+"=/"+s)
	}
	// F6: a format character inside an exempt name makes it a plain key.
	for _, l := range []string{"page\u200btoken=" + s, "next\ufeffToken: " + s, "NO\u200dAUTH=12345678", "P\u200bWD=/" + s, "resume tok\u200ben: " + s, "http\u200bAuth: plain"} {
		add("F6 "+l, l)
	}
	return cs
}

// Round 12: every read masks the security review's inputs.
func TestRound12Regressions(t *testing.T) {
	for _, c := range round12Cases() {
		marker := c.markers[0]
		if strings.HasPrefix(c.line, "NO\u200dAUTH") {
			marker = "12345678"
		} else if strings.HasPrefix(c.line, "http\u200bAuth") {
			marker = "plain"
		}
		h := newHarness(t, "", nil)
		h.d.rememberWritten(c.written)
		cp := h.shell("KafkaConnect", "orders-connect", map[string]any{"config": map[string]any{"note": c.line}})
		h.api.allow(cp)
		res, err := run(t, h, "k8s_get", map[string]any{"kind": "KafkaConnect", "name": "orders-connect"})
		if err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(res.Data)
		if strings.Contains(string(b), marker) {
			t.Errorf("%s: k8s_get leaks: %.300s", c.name, b[max(0, len(b)-300):])
		}
		if len(c.line) > 4096 {
			continue // past what the other reads keep of a line
		}
		if bad := leaksIn(readsOf(t, c.written, c.line), marker); len(bad) > 0 {
			t.Errorf("%s: %q leaks in %v", c.name, c.line, bad)
		}
	}
}

// round12KeepCases are the round 12 correctness review's over-masking
// inputs: every read must return them unmasked. They are in the corpus.
func round12KeepCases() []corpusCase {
	var cs []corpusCase
	for _, l := range []string{
		// 1: Kafka's fetch-session diagnostics.
		"Node 1 sent an invalid full fetch response with sessionId=1234567, epoch=INITIAL",
		"session.id=1234567 epoch=3", "FetchSessionHandler: sessionid: 9876543",
		"password is closed",
		// 2: Jackson's parse errors.
		"Unrecognized token 'abc': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')",
		`Unexpected token "}" at line 3`,
		// 3: a stack trace under a message ending in token:.
		"Failed to refresh token:\n\tat org.example.Auth.refresh(Auth.java:42)\n\tat org.example.Main.run(Main.java:7)",
		"could not read token:\n    at org.example.Auth.read(Auth.java:9)",
		// 4: mysql options that start with p.
		"mysqlbinlog -partition=3 -position=154 binlog.000001",
		"mysql client: -port 3306",
		// 5: a CSV header.
		"user,password,host",
		// 6: test runner counters.
		"pass: 3 fail: 0", // (redact masks "pass=12" on its own)
		// 7: placeholders already in the text.
		"password [masked:credential]", "password=[masked:credential]", "password=[redacted 8]", "token [hidden]",
	} {
		cs = append(cs, corpusCase{name: "round 12 keep " + strings.SplitN(l, "\n", 2)[0], line: l, keep: strings.SplitN(l, "\n", 2)[0]})
	}
	return cs
}

// Round 12, correctness items 1-7: the over-masking cases stay as they
// are through every read, and the secrets next to them are still masked.
func TestRound12Keeps(t *testing.T) {
	for _, c := range round12KeepCases() {
		got := readsOf(t, nil, c.line)
		for _, op := range []string{"k8s_logs", "k8s_events", "strimzi_build_status"} {
			j, _ := json.Marshal(c.keep)
			if !strings.Contains(got[op], strings.Trim(string(j), `"`)) {
				t.Errorf("%s: %q is masked: %s", op, c.keep, got[op])
			}
		}
	}
	// The stack trace's frames stay too.
	if got := readsOf(t, nil, "Failed to refresh token:\n\tat org.example.Auth.refresh(Auth.java:42)")["k8s_logs"]; !strings.Contains(got, "Auth.java:42") {
		t.Errorf("a stack frame is masked: %s", got)
	}
	s := round12Secret
	for _, line := range []string{
		"user,password,host\napp," + s + ",db", "id,db_password\n1," + s,
		"password: |\n  " + s, "  password:\n    " + s, "- password: >-\n    " + s,
		"mysql -uroot -p" + s + " db", "mysql -p'" + s + "'", "pass: " + s, "token 'x" + s + "'",
		"password [" + s + "]",
	} {
		if bad := leaksIn(readsOf(t, nil, line), s); len(bad) > 0 {
			t.Errorf("%q leaks in %v", line, bad)
		}
	}
	// The CSV header itself is kept; only the column's cells are masked.
	if got := readsOf(t, nil, "user,password,host\napp,"+s+",db")["k8s_logs"]; !strings.Contains(got, "user,password,host") || !strings.Contains(got, "app,[masked:credential],db") {
		t.Errorf("CSV: %s", got)
	}
}

// Round 12, correctness item 8: the format-character map's capacity is
// bounded by redact.MaxScan, not by the text.
func TestStripFormatCharsBounded(t *testing.T) {
	s, pos := stripFormatChars(strings.Repeat("a\u200b", redact.MaxScan))
	if len(pos) != len(s)+1 || cap(pos) > len(pos)*2+redact.MaxScan {
		t.Errorf("len %d, pos len %d cap %d", len(s), len(pos), cap(pos))
	}
}
