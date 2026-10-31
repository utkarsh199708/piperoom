package strimzi

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
)

// The masking regression corpus (round 10): every input carries a secret
// marker, runs through k8s_logs, k8s_get, k8s_events and
// strimzi_build_status, and no decoded output may hold the marker. It is
// generated here, not committed: the PEM keys are made at test time.

// corpusMarker is the secret most inputs carry.
const corpusMarker = "Zq9pLm2xVb7Kd"

// corpusCase is one input: its line, the values the runner remembers, and
// the markers that must not come back.
type corpusCase struct {
	name    string
	line    string
	written []string
	markers []string
}

// corpusKeys are the credential key spellings the corpus writes values
// under.
func corpusKeys() []string {
	keys := []string{
		"password", "passwd", "secret", "token", "apikey", "api_key", "api-key", "apiKey", "ApiKey", "APIKEY", "X-API-Key",
		"authorization", "Authorization", "Proxy-Authorization", "bearer", "Bearer", "credential", "credentials",
		"privatekey", "private_key", "privateKey", "private-key", "sasl.jaas.config", "requirepass", "masterauth",
		"httpAuth", "basicAuth", "proxyAuth", "auth", "pw", "db_pw", "dbPw", "pwd", "MYSQL_PWD", "MYSQL-PWD", "MYSQL.PWD", "dbPwd", "DBPWD", "OLDPWD",
		"pass_word", "pass.word", "PASS_WORD", "p_wd", "se_cret", "to_ken", "bear_er", "cred_entials", "auth_orization",
		"database.password", "ssl.key.password", "ssl.truststore.password", "ssl.keystore.password", "spring.datasource.password",
		"connection.password", "schema.history.internal.producer.ssl.key.password", "x-auth-token", "X-Auth-Token",
		"access_token", "refresh_token", "aws_session_token", "client_secret", "sessionToken", "accessToken", "csrftoken",
		"PGPASSWORD", "MYSQLPASSWORD", "SASLPassword", "LDAPPassword", "AWSSecret", "APIToken", "OAUTHTOKEN", "GITHUBTOKEN",
		"JWTToken", "DBPassword", "dbpassword", "sslpassword", "accesstoken", "DB_PASSWORD", "POSTGRES_PASSWORD", "MYSQL_ROOT_PASSWORD",
	}
	for _, kw := range []string{"Password", "PASSWORD", "password", "Secret", "SECRET", "secret", "Token", "TOKEN", "token"} {
		for _, p := range []string{"db", "DB", "ssl", "aws", "AWS", "pg", "jwt", "github", "GITHUB", "app_", "app.", "x-"} {
			keys = append(keys, p+kw)
		}
	}
	sort.Strings(keys)
	out := keys[:0]
	for i, k := range keys {
		if i == 0 || k != keys[i-1] {
			out = append(out, k)
		}
	}
	return out
}

// jsonInner is s as it sits inside a JSON string.
func jsonInner(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1])
}

// corpusCases builds the corpus. pems are generated keys (PEM text).
func corpusCases(pems []string) []corpusCase {
	var cs []corpusCase
	add := func(name, line string, markers ...string) {
		cs = append(cs, corpusCase{name: name, line: line, markers: markers})
	}
	templates := []struct {
		name string
		f    func(k, v string) string
	}{
		{"eq", func(k, v string) string { return k + "=" + v }},
		{"colon", func(k, v string) string { return k + ": " + v }},
		{"spaced", func(k, v string) string { return k + " = " + v }},
		{"quoted-eq", func(k, v string) string { return `"` + k + `"="` + v + `"` }},
		{"json", func(k, v string) string { return `{"` + k + `":"` + jsonInner(v) + `","m":"ok"}` }},
		{"json-array", func(k, v string) string { return `{"` + k + `": ["` + jsonInner(v) + `"], "m":"ok"}` }},
		{"json-escaped", func(k, v string) string { return `m="` + jsonInner(`{"`+k+`":"`+jsonInner(v)+`","n":1}`) + `"` }},
		{"json-double", func(k, v string) string {
			return `m="` + jsonInner(jsonInner(`{"`+k+`":"`+jsonInner(v)+`"}`)) + `"`
		}},
		{"yaml", func(k, v string) string { return "  " + k + ": " + v }},
		{"export", func(k, v string) string { return "export " + k + "=" + v }},
		{"query", func(k, v string) string { return "GET /x?" + k + "=" + url.QueryEscape(v) + "&y=1" }},
		{"colon-quoted", func(k, v string) string { return k + `: "` + v + `"` }},
	}
	for _, k := range corpusKeys() {
		for i, tp := range templates {
			v := corpusMarker
			if i%2 == 0 {
				v = "w1 " + corpusMarker + " w3" // a multi-word value
			}
			add("key "+k+" "+tp.name, tp.f(k, v), corpusMarker)
		}
	}
	// Multi-line quoted and continued values.
	for _, l := range []string{
		"password=\"a \\\n" + corpusMarker + "\"",
		"PGPASSWORD=\"x\n" + corpusMarker + "\"",
		"password=abc\\\n" + corpusMarker,
		"secret: 'x\ny " + corpusMarker + "'",
		"token=ab%\n3D" + corpusMarker,
		"token=ab%3\nD" + corpusMarker,
		"DB_PASSWORD='line1\nline2\nline3 " + corpusMarker + "'",
		"connection.password = \"first \\\n second \\\n " + corpusMarker + "\"",
	} {
		add("multi-line "+strings.SplitN(l, "\n", 2)[0], l, corpusMarker)
	}
	// Generated PEM keys after a credential key, after another key, alone.
	for i, p := range pems {
		var body []string
		for _, l := range strings.Split(strings.TrimSpace(p), "\n") {
			if !strings.HasPrefix(l, "-----") && len(l) >= 24 {
				body = append(body, l[:24])
			}
		}
		for _, pre := range []string{"privateKey: ", "password: ", "secret=", "private_key=", "tls.key: ", "key: ", "", "log line\n"} {
			add(fmt.Sprintf("pem %d after %q", i, pre), pre+p+"after", body...)
		}
	}
	// Kept values followed by more.
	for _, l := range []string{
		"password: YES) " + corpusMarker, "password=${a}" + corpusMarker, "password=${secrets:data/x:password} " + corpusMarker,
		"password=[hidden] " + corpusMarker, "password = [hidden]" + corpusMarker, "token: expired " + corpusMarker,
		"password: none " + corpusMarker, "secret: orders-db-credentials " + corpusMarker, `{"password":"NO) ` + corpusMarker + `"}`,
		"(using password: NO) " + corpusMarker, "httpAuth: true " + corpusMarker, "pwd=/opt " + corpusMarker,
	} {
		add("kept+junk "+l, l, corpusMarker)
	}
	// Exempt names carrying a secret.
	for _, l := range []string{
		"nexttoken=abc " + corpusMarker, "nextPageToken: a " + corpusMarker, "continuationToken=" + strings.Repeat("a", 60) + corpusMarker,
		"next_page_token: x " + corpusMarker, "pagetoken=" + corpusMarker + " more", "useBasicAuth=" + corpusMarker, "requireAuth: " + corpusMarker,
		"oauth=" + corpusMarker, "OAUTH: " + corpusMarker, "NOAUTH: " + corpusMarker, "tokenizer=" + corpusMarker, "resume token: abc " + corpusMarker,
		"presume token: " + corpusMarker, "unable to resume token=" + corpusMarker, "resumeToken=abc " + corpusMarker, "resume_token: abc " + corpusMarker,
		"httpAuth: required " + corpusMarker, "OLDPWD=" + corpusMarker, "pwd: " + corpusMarker, "PWD=/opt " + corpusMarker, "PWD=" + corpusMarker,
		`{"nextPageToken":"a ` + corpusMarker + `"}`, `{"useBasicAuth":"` + corpusMarker + `"}`, `{"tokenizer":"` + corpusMarker + `"}`,
	} {
		add("exempt "+l, l, corpusMarker)
	}
	// Doubly escaped JSON with the header and JAAS keys.
	for _, k := range []string{"Authorization", "Proxy-Authorization", "bearer", "sasl.jaas.config"} {
		for _, v := range []string{"Bearer " + corpusMarker, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"" + corpusMarker + "\";"} {
			add("double "+k, `m="`+jsonInner(jsonInner(`{"`+k+`":"`+jsonInner(v)+`","k":"v"}`))+`"`, corpusMarker)
			add("single "+k, `m="`+jsonInner(`{"`+k+`":"`+jsonInner(v)+`","k":"v"}`)+`"`, corpusMarker)
		}
	}
	// Written values in raw, URL and JSON forms.
	written := []struct{ v, marker string }{
		{"Xr7Kq2pLmNw", "Kq2pLmN"}, {"Mé€Kd9pLqx", "Kd9pLqx"}, {"ab\x01Zq9pLmXw", "Zq9pLmXw"}, {"ab\xffZq9pLmYw", "Zq9pLmYw"},
		{"q9z", "q9z"}, {"ééé", "ééé"}, {"€€€", "€€€"}, {"two words+x!", "two words"},
	}
	for _, w := range written {
		forms := map[string]string{"raw": w.v, "query": url.QueryEscape(w.v), "path": url.PathEscape(w.v), "json": jsonInner(w.v)}
		names := []string{"json", "path", "query", "raw"}
		for _, fn := range names {
			f := forms[fn]
			for ci, c := range []string{"login with %s failed", "url ?p=%s&x=1", "path /%s/x", `{"msg":"%s"}`, "(%s)"} {
				l := fmt.Sprintf(c, f)
				markers := []string{w.marker}
				if fn != "raw" && f != w.v {
					markers = append(markers, f)
				}
				cs = append(cs, corpusCase{name: fmt.Sprintf("written %q %s ctx %d", w.v, fn, ci), line: l, written: []string{w.v}, markers: markers})
			}
		}
	}
	return cs
}

// corpusPEMs generates an RSA and an EC private key in PEM.
func corpusPEMs(t *testing.T) []string {
	rk, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ek, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	eb, err := x509.MarshalECPrivateKey(ek)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := x509.MarshalPKCS8PrivateKey(rk)
	if err != nil {
		t.Fatal(err)
	}
	return []string{
		string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(rk)})),
		string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: eb})),
	}
}

// corpusReads runs one input through the four reads on one driver and
// returns each op's Data as JSON.
func corpusReads(t *testing.T, c corpusCase) map[string]string {
	t.Helper()
	h := newHarness(t, "", nil)
	if len(c.written) > 0 {
		h.d.rememberWritten(c.written)
	}
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = c.line + "\n"
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-build/log"] = c.line + "\n"
	h.api.allowed["events"] = true
	h.api.objects["events"] = map[string]any{"items": []any{
		map[string]any{"type": "Warning", "reason": "Failed", "message": c.line, "lastTimestamp": "2026-10-30T10:00:00Z"},
	}}
	cp := h.shell("KafkaConnect", "orders-connect", map[string]any{})
	h.api.allow(cp)
	h.api.allow("/api/v1/namespaces/data/pods/orders-connect-connect-build")
	h.api.objects[cp]["status"] = map[string]any{"conditions": []any{map[string]any{"type": "NotReady", "status": "True", "reason": "BuildFailed", "message": "build: " + c.line}}}
	out := map[string]string{}
	for op, params := range map[string]map[string]any{
		"k8s_logs":             {"pod": "orders-connect-connect-0"},
		"k8s_get":              {"kind": "KafkaConnect", "name": "orders-connect"},
		"k8s_events":           {"involved": "orders-connect-connect-0"},
		"strimzi_build_status": {},
	} {
		res, err := run(t, h, op, params)
		if err != nil {
			t.Fatalf("%s: %s: %v", c.name, op, err)
		}
		b, _ := json.Marshal(res.Data)
		out[op] = string(b)
	}
	return out
}

// decodedStrings is every string in a JSON document, decoded.
func decodedStrings(doc string) []string {
	var v any
	if json.Unmarshal([]byte(doc), &v) != nil {
		return []string{doc}
	}
	var out []string
	var walk func(any)
	walk = func(x any) {
		switch t := x.(type) {
		case string:
			out = append(out, t)
		case []any:
			for _, e := range t {
				walk(e)
			}
		case map[string]any:
			for _, e := range t {
				walk(e)
			}
		}
	}
	walk(v)
	return out
}

// TestMaskingCorpus runs the corpus through every read. With
// PIPEROOM_MASKING_CORPUS_OUT set it writes one line per leaking case
// and the totals to that file instead of failing (to count leaks at
// older commits).
func TestMaskingCorpus(t *testing.T) {
	cases := corpusCases(corpusPEMs(t))
	if len(cases) < 2000 {
		t.Fatalf("the corpus has %d inputs, want at least 2000", len(cases))
	}
	outPath := os.Getenv("PIPEROOM_MASKING_CORPUS_OUT")
	var report strings.Builder
	leaks := 0
	for _, c := range cases {
		outs := corpusReads(t, c)
		var bad []string
		for op, doc := range outs {
			for _, s := range append(decodedStrings(doc), doc) {
				for _, m := range c.markers {
					if strings.Contains(s, m) {
						bad = append(bad, op+": "+m)
					}
				}
			}
		}
		if len(bad) == 0 {
			continue
		}
		leaks++
		if outPath != "" {
			fmt.Fprintf(&report, "%s\t%q\n", c.name, bad)
			continue
		}
		t.Errorf("%s: %q leaks %v\n  k8s_logs: %s", c.name, c.line, bad, outs["k8s_logs"])
	}
	if outPath != "" {
		fmt.Fprintf(&report, "TOTAL\t%d inputs\t%d leaking\n", len(cases), leaks)
		if err := os.WriteFile(outPath, []byte(report.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
