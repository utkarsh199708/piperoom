package strimzi

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
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

// Round 11 adds the security review's corpus (reviewCases).

// corpusMarker is the secret most inputs carry.
const corpusMarker = "Zq9pLm2xVb7Kd"

// corpusCase is one input: its line, the values the runner remembers, and
// the markers that must not come back.
type corpusCase struct {
	name    string
	line    string
	written []string
	markers []string
	// keep, when set, must come back from every read as it is (round
	// 12: an over-masking case).
	keep string
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

// corpusRunner runs inputs through the four reads. Inputs without
// written values share one driver (round 11: the setup cost more than the
// reads); one with written values gets its own, so no input's values
// mask another's.
type corpusRunner struct {
	t      *testing.T
	shared *harness
}

func newCorpusRunner(t *testing.T) *corpusRunner {
	return &corpusRunner{t: t, shared: corpusHarness(t, nil)}
}

func corpusHarness(t *testing.T, written []string) *harness {
	h := newHarness(t, "", nil)
	if len(written) > 0 {
		h.d.rememberWritten(written)
	}
	h.api.allowed["events"] = true
	h.api.allow(h.shell("KafkaConnect", "orders-connect", map[string]any{}))
	h.api.allow("/api/v1/namespaces/data/pods/orders-connect-connect-build")
	return h
}

// reads runs one input through the four reads and returns each op's Data
// as JSON.
func (r *corpusRunner) reads(c corpusCase) map[string]string {
	t := r.t
	t.Helper()
	h := r.shared
	if len(c.written) > 0 {
		h = corpusHarness(t, c.written)
	}
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = c.line + "\n"
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-build/log"] = c.line + "\n"
	h.api.objects["events"] = map[string]any{"items": []any{
		map[string]any{"type": "Warning", "reason": "Failed", "message": c.line, "lastTimestamp": "2026-10-30T10:00:00Z"},
	}}
	cp := objectPath(testNS, "KafkaConnect", "orders-connect")
	h.api.objects[cp]["status"] = map[string]any{"conditions": []any{map[string]any{"type": "NotReady", "status": "True", "reason": "BuildFailed", "message": "build: " + c.line}}}
	out := map[string]string{}
	for _, op := range []string{"k8s_logs", "k8s_get", "k8s_events", "strimzi_build_status"} {
		params := map[string]map[string]any{
			"k8s_logs":             {"pod": "orders-connect-connect-0"},
			"k8s_get":              {"kind": "KafkaConnect", "name": "orders-connect"},
			"k8s_events":           {"involved": "orders-connect-connect-0"},
			"strimzi_build_status": {},
		}[op]
		res, err := run(t, h, op, params)
		if err != nil {
			t.Fatalf("%s: %s: %v", c.name, op, err)
		}
		b, _ := json.Marshal(res.Data)
		out[op] = string(b)
	}
	return out
}

// corpusBatch is how many inputs share one k8s_get and one k8s_events.
const corpusBatch = 50

// batchReads runs the inputs without written values on the shared
// driver: k8s_get and k8s_events once per corpusBatch inputs (each input
// one config value and one event message, which the reads mask one
// string at a time), k8s_logs and strimzi_build_status once per input (a
// log is masked whole). The result holds, for each such input, the
// reads' output for it; an input with written values is left nil.
func (r *corpusRunner) batchReads(cases []corpusCase) []map[string]string {
	t, h := r.t, r.shared
	outs := make([]map[string]string, len(cases))
	var idx []int
	for i, c := range cases {
		if len(c.written) == 0 {
			idx = append(idx, i)
		}
	}
	cp := objectPath(testNS, "KafkaConnect", "orders-connect")
	for len(idx) > 0 {
		chunk := idx[:min(corpusBatch, len(idx))]
		idx = idx[len(chunk):]
		config := map[string]any{}
		var items []any
		for j, i := range chunk {
			config[fmt.Sprintf("c%03d", j)] = cases[i].line
			items = append(items, map[string]any{"type": "Warning", "reason": "Failed", "message": cases[i].line, "lastTimestamp": fmt.Sprintf("2026-10-30T10:00:%03dZ", j)})
		}
		h.api.objects[cp]["spec"] = map[string]any{"config": config}
		delete(h.api.objects[cp], "status")
		h.api.objects["events"] = map[string]any{"items": items}
		var got struct {
			Spec struct {
				Config map[string]any `json:"config"`
			} `json:"spec"`
		}
		var evs struct {
			Events []struct {
				Message string `json:"message"`
			} `json:"events"`
		}
		for op, v := range map[string]any{"k8s_get": &got, "k8s_events": &evs} {
			params := map[string]any{"kind": "KafkaConnect", "name": "orders-connect"}
			if op == "k8s_events" {
				params = map[string]any{"involved": "orders-connect-connect-0"}
			}
			res, err := run(t, h, op, params)
			if err != nil {
				t.Fatalf("%s: %v", op, err)
			}
			b, _ := json.Marshal(res.Data)
			if err := json.Unmarshal(b, v); err != nil {
				t.Fatalf("%s: %v", op, err)
			}
		}
		if len(evs.Events) != len(chunk) || len(got.Spec.Config) != len(chunk) {
			t.Fatalf("batch of %d: %d events, %d config values", len(chunk), len(evs.Events), len(got.Spec.Config))
		}
		h.api.objects[cp]["spec"] = map[string]any{}
		for j, i := range chunk {
			c := cases[i]
			gb, _ := json.Marshal(got.Spec.Config[fmt.Sprintf("c%03d", j)])
			eb, _ := json.Marshal(evs.Events[j].Message)
			out := map[string]string{"k8s_get": string(gb), "k8s_events": string(eb)}
			h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = c.line + "\n"
			h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-build/log"] = c.line + "\n"
			h.api.objects[cp]["status"] = map[string]any{"conditions": []any{map[string]any{"type": "NotReady", "status": "True", "reason": "BuildFailed", "message": "build: " + c.line}}}
			for _, op := range []string{"k8s_logs", "strimzi_build_status"} {
				params := map[string]any{}
				if op == "k8s_logs" {
					params = map[string]any{"pod": "orders-connect-connect-0"}
				}
				res, err := run(t, h, op, params)
				if err != nil {
					t.Fatalf("%s: %s: %v", c.name, op, err)
				}
				b, _ := json.Marshal(res.Data)
				out[op] = string(b)
			}
			outs[i] = out
		}
	}
	return outs
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
	cases := append(append(append(corpusCases(corpusPEMs(t)), reviewCases()...), round12Cases()...), round12KeepCases()...)
	cases = append(cases, round13Cases()...)
	cases = append(append(cases, round14Cases()...), round14KeepCases()...)
	if len(cases) < 5000 {
		t.Fatalf("the corpus has %d inputs, want at least 5000", len(cases))
	}
	runner := newCorpusRunner(t)
	outPath := os.Getenv("PIPEROOM_MASKING_CORPUS_OUT")
	var report strings.Builder
	leaks := 0
	batched := runner.batchReads(cases)
	for i, c := range cases {
		outs := batched[i]
		if outs == nil {
			outs = runner.reads(c)
		}
		var bad []string
		for op, doc := range outs {
			kept := false
			for _, s := range append(decodedStrings(doc), doc) {
				for _, m := range c.markers {
					if strings.Contains(s, m) {
						bad = append(bad, op+": "+m)
					}
				}
				kept = kept || (c.keep != "" && strings.Contains(s, c.keep))
			}
			if c.keep != "" && !kept {
				bad = append(bad, op+": over-masks "+c.keep)
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

// ---- round 11: the security review's corpus ----

// reviewCases is the round 11 security review's adversarial corpus, as it
// was written (one canary per input; any 7 bytes of it count as a leak),
// less the inputs reviewGap lists. Multi-line inputs keep their lines.
func reviewCases() []corpusCase {
	var cs []corpusCase
	n := 0
	canary := func() string {
		n++
		const al = "BCDFGHJKLMNPQRSTVWXZbcdfghjkmnpqrstvwxz"
		s := []byte("Qx")
		x := n*7919 + 104729
		for i := 0; i < 12; i++ {
			s = append(s, al[x%len(al)])
			x = x/len(al) + i*31 + 17
		}
		return string(append(s, fmt.Sprintf("%d", 10+n%89)...))
	}
	windows := func(c string) []string {
		var ws []string
		for i := 0; i+7 <= len(c); i++ {
			ws = append(ws, c[i:i+7])
		}
		return ws
	}
	put := func(name, text string, written []string, c string) {
		if reviewGap(name) == "" {
			cs = append(cs, corpusCase{name: "review " + name, line: strings.TrimSuffix(text, "\n"), written: written, markers: windows(c)})
		}
	}
	add := func(name, tmpl string, w ...string) {
		c := canary()
		var ws []string
		for _, x := range w {
			ws = append(ws, strings.ReplaceAll(x, "@C@", c))
		}
		put(name, strings.ReplaceAll(tmpl, "@C@", c), ws, c)
	}
	keys := []string{
		"password", "Password", "PASSWORD", "db.password", "database.password", "ssl.key.password", "keystorePassword",
		"truststore_password", "passphrase", "ssl.key.passphrase", "snowflake.private.key.passphrase", "secret", "client_secret",
		"clientSecret", "SECRET_KEY", "secret_key", "secretKey", "aws_secret_access_key", "AWS_SECRET_ACCESS_KEY",
		"aws.secret.access.key", "azure.client.secret", "AccountKey", "SharedAccessKey", "SharedAccessSignature", "sig",
		"token", "auth_token", "authToken", "bearerToken", "access-token", "refreshToken", "id_token", "x-amz-security-token",
		"api_key", "apiKey", "X-Api-Key", "x-api-key", "apikey", "private_key", "privateKey", "snowflake.private.key",
		"sasl.jaas.config", "producer.override.sasl.jaas.config", "basic.auth.user.info", "schema.registry.basic.auth.user.info",
		"Authorization", "authorization", "Proxy-Authorization", "cookie", "Cookie", "Set-Cookie", "session", "sessionid",
		"jwt", "JWT", "pass", "PASS", "pwd", "Pwd", "PWD", "MYSQL_PWD", "pw", "passcode", "pin", "credentials", "credential",
		"connection.uri", "connectionString", "DATABASE_URL", "dsn", "mongodb.connection.string", "redis.password",
		"masterauth", "requirepass", "httpAuth", "auth", "Auth", "sslpassword", "pgpass", "ldap.bind.password",
		"key.password", "keystore.key", "ssl.keystore.key", "encryption.key", "hmacKey", "signingKey",
	}
	formats := []struct{ n, f string }{
		{"eq", "%s=@C@"}, {"eq-sp", "%s = @C@"}, {"colon", "%s: @C@"}, {"tab", "%s\t@C@"}, {"arrow", "%s=>@C@"},
		{"walrus", "%s := @C@"}, {"py", "{'user': 'u', '%s': '@C@'}"}, {"json", `{"user":"u","%s":"@C@"}`},
		{"json-sp", `{ "%s" : "@C@" }`}, {"json-esc", `msg="{\"%s\":\"@C@\"}"`}, {"dq", `%s="w1 @C@ w2"`}, {"sq", `%s='@C@'`},
		{"xml", "<%s>@C@</%s>"}, {"xml-entry", `<entry key="%s">@C@</entry>`}, {"xml-prop", `<property name="%s" value="@C@"/>`},
		{"flag", "--%s @C@"}, {"flag-eq", "--%s=@C@"}, {"sysprop", "-D%s=@C@"}, {"map", "{a=1, %s=@C@, b=2}"},
		{"logfmt", `level=info %s="a b @C@" msg=done`}, {"bracket", "%s [@C@]"}, {"is", "%s is @C@"},
		{"yaml-block", "%s: |\n  @C@\n"}, {"yaml-next", "%s:\n  @C@\n"}, {"query", "GET /cb?x=1&%s=@C@&y=2 HTTP/1.1"},
		{"header", "> %s: Basic @C@"}, {"envjson", `{"name":"%s","value":"@C@"}`}, {"sql", "ALTER USER app WITH %s '@C@';"},
		{"csv", "u,%s,@C@"}, {"semi", "Server=h;Uid=u;%s=@C@;Database=d"},
	}
	for _, k := range keys {
		for _, f := range formats {
			args := make([]any, strings.Count(f.f, "%s"))
			for i := range args {
				args[i] = k
			}
			add("kf "+k+" "+f.n, fmt.Sprintf(f.f, args...))
		}
	}
	for _, sch := range []string{"postgres", "postgresql", "mysql", "mongodb", "mongodb+srv", "redis", "rediss", "amqp", "https", "jdbc:postgresql", "jdbc:mysql", "sqlserver", "kafka", "ftp"} {
		for _, f := range []string{"%s://app:@C@@db.example:5432/x", "url=%s://app:@C@@h/x?ssl=1", `"uri":"%s://u:@C@@h"`, "%s://:@C@@h:6379", "connecting to %s://app:p%%40@C@@h"} {
			add("url "+sch+" "+f, fmt.Sprintf(f, sch))
		}
	}
	for _, l := range [][2]string{
		{"jdbc query pw", "jdbc:postgresql://h/db?user=u&password=@C@&ssl=true"},
		{"jdbc sqlserver", "jdbc:sqlserver://h;user=u;password=@C@;encrypt=true"},
		{"odbc braces", "Driver={X};Server=h;PWD={@C@};"},
		{"azure conn", "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=@C@==;EndpointSuffix=core.windows.net"},
		{"bearer header", "Authorization: Bearer @C@"}, {"bearer lc", "authorization: bearer @C@"},
		{"basic b64", "Authorization: Basic dTo=@C@"}, {"curl -u", "curl -u admin:@C@ https://h"},
		{"mysql -p", "mysql -uroot -p@C@ db"}, {"psql PGPASSWORD env", "PGPASSWORD=@C@ psql -h h"},
		{"jaas", `sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="u" password="@C@";`},
		{"jaas json", `{"sasl.jaas.config":"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"@C@\";"}`},
		{"cursor 64", "nextPageToken=" + strings.Repeat("a", 50) + "@C@"}, {"cursor jwtish", "pageToken=eyJhbGciOiJIUzI1NiJ9.@C@.sig"},
		{"resume 2tok", "resume token: abc,password=@C@"}, {"mysql pwd path", "MYSQL_PWD=/@C@"}, {"Pwd path", "Pwd=/@C@"},
		{"auth mech + junk", "auth=PLAIN@C@"}, {"auth mech q", `httpAuth: "basic" @C@`}, {"switch number", "NOAUTH=1@C@"},
		{"switch neg", "requireAuth=-1.5 @C@"},
	} {
		add(l[0], l[1])
	}
	// Item 1: a kept value swallowing a second credential.
	for _, ex := range []string{"nextToken=abc", "pageToken=a", "next_page_token: x", "continuationToken=z", "PWD=/opt", "pwd=/home/u", "OLDPWD=/x",
		"httpAuth=true", "auth=plain", "useBasicAuth=true", "NOAUTH=true", "tokenizer=1", "password=none", "token=expired", "secret=orders-db-credentials",
		"password=${file:/x:y}", "password=[hidden]", "resume token: abc", "pass=null", "requireAuth=false", "oauth=null"} {
		for _, sep := range []string{"&", ",", ";", "|", ":", "#", "/", "\\", "+", "?", "'", "\""} {
			add("exempt "+ex+" sep "+sep, ex+sep+"password=@C@")
			add("exempt "+ex+" sep "+sep+" masterauth", ex+sep+"masterauth=@C@")
		}
		add("exempt "+ex+" sp", ex+" password=@C@")
		add("exempt "+ex+" json", ex+`,{"password":"@C@"}`)
		add("exempt "+ex+" auth", ex+"&Authorization=Bearer%20@C@")
	}
	// Items 3 and 4: continuation.
	for _, k := range []int{1, 10, 62, 63, 64, 65, 66, 100} {
		mid := strings.Repeat("line\n", k)
		add(fmt.Sprintf("cont dq %d", k), "password=\"start\n"+mid+"@C@\"")
		add(fmt.Sprintf("cont sq %d", k), "secret: 'start\n"+mid+"@C@'")
		add(fmt.Sprintf("cont bs %d", k), "token=a\\\n"+strings.Repeat("b\\\n", k)+"@C@")
		add(fmt.Sprintf("cont pct %d", k), "password=a%\n"+strings.Repeat("%\n", k)+"@C@")
		add(fmt.Sprintf("cont crlf %d", k), "password=\"a\r\n"+strings.Repeat("x\r\n", k)+"@C@\"\r")
	}
	for _, l := range [][2]string{
		{"cont escaped quote", `password="a \" b` + "\n@C@\""}, {"cont closes then new", "password=\"a\nb\" c\n@C@"},
		{"cont bs-bs", "password=a\\\\\n@C@"}, {"cont two opens", "password=\"x\nsecret='y\n@C@'\""},
		{"cont quote later", "password = x \"y\n@C@\""}, {"cont json unterminated", `{"password":"abc` + "\n@C@\"}"},
		{"cont pct2", "token=ab%2\n@C@"}, {"cont pct no hex", "token=ab%G\n@C@"}, {"cont key no value", "password=\n@C@"},
		{"heredoc", "password <<EOF\n@C@\nEOF"},
		{"pem enc", "-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIF@C@\n-----END ENCRYPTED PRIVATE KEY-----"},
		{"pem no end", "key:\n-----BEGIN PRIVATE KEY-----\nMIIE@C@abc\nMIIE@C@def"},
		{"pem json", `{"tls.key":"-----BEGIN PRIVATE KEY-----\nMIIE@C@abc\n-----END PRIVATE KEY-----\n"}`},
		{"pem dbl json", `m="{\"k\":\"-----BEGIN PRIVATE KEY-----\\nMIIE@C@abc\\n-----END PRIVATE KEY-----\"}"`},
	} {
		add(l[0], l[1])
	}
	// Mask literals in the input.
	for _, mk := range []string{"[redacted]", "[redacted 8]", "[redacted 8", "[masked:credential]", "…[redacted 9 bytes past the scan limit]", "ab…[redacted 48]…cd", "[redacted private key 64]", "[hidden]", "***", "[redacted 8]]"} {
		add("marker before "+mk, mk+" password=@C@")
		add("marker after "+mk, "password=@C@ "+mk)
		add("marker inside "+mk, "password=x"+mk+"@C@")
		add("marker json "+mk, `{"a":"`+mk+`","secret":"@C@"}`)
		add("marker url "+mk, "postgres://u:@C@@h/"+mk)
	}
	for _, l := range []string{" ", ", ", " password=", "&password=", "\n", "@", ":", "x"} {
		add(fmt.Sprintf("repeat %q", l), "password=\"a"+l+"@C@\""+l+"password=\"b"+l+"@C@\"")
		add(fmt.Sprintf("repeat url %q", l), "postgres://u:p"+l+"@C@@h"+l+"postgres://u:q"+l+"@C@@h")
	}
	// Item 2 and item 6: bytes and characters around and inside keys.
	for _, pre := range []string{"é", "€", "日本", "\U0001F512", "\u200b", "\u202e", "\xff", "\x01", "\xc3", "\xe2\x82", "\ufffd", "\u0301"} {
		add(fmt.Sprintf("mb pre %q", pre), pre+"password="+pre+"@C@"+pre)
		add(fmt.Sprintf("mb json %q", pre), `{"`+pre+`password":"@C@"}`)
		add(fmt.Sprintf("mb url %q", pre), "postgres://u"+pre+":@C@"+pre+"@h")
		add(fmt.Sprintf("mb inkey %q", pre), "pass"+pre+"word=@C@")
		add(fmt.Sprintf("mb written %q", pre), "login "+pre+"@C@"+pre+" failed", pre+"@C@")
	}
	for _, f := range []struct {
		n string
		f func(string) string
	}{
		{"auth", func(c string) string { return "auth " + c + " failed" }},
		{"b64", func(c string) string { return "b64 " + base64.StdEncoding.EncodeToString([]byte(c)) }},
		{"u", func(c string) string { return "u " + strings.ReplaceAll(c, "x", `\u0078`) }},
		{"pct", func(c string) string { return "pct " + url.PathEscape(c[:4]) + "%" + fmt.Sprintf("%02X", c[4]) + c[5:] }},
		{"pct lower", func(c string) string { return "pct lower " + "%" + fmt.Sprintf("%02x", c[0]) + c[1:] }},
		{"hex", func(c string) string { return "hex " + fmt.Sprintf("%x", c) }},
	} {
		c := canary()
		put("written form "+f.n, f.f(c), []string{c}, c)
	}
	for _, w := range []string{"p@ss w0rd!%s", "a+b/c=%s", "%s\"quote", "%s\\back", "<%s>&", "%s~*'()"} {
		c := canary()
		v := fmt.Sprintf(w, c)
		j, _ := json.Marshal(v)
		jj, _ := json.Marshal(string(j))
		var all strings.Builder
		for i := 0; i < len(v); i++ {
			fmt.Fprintf(&all, "%%%02X", v[i])
		}
		for i, enc := range []string{v, url.QueryEscape(v), string(j), string(jj), url.PathEscape(v), all.String()} {
			put(fmt.Sprintf("written %q ctx %d", w, i), "x "+enc, []string{v}, c)
		}
	}
	return cs
}

// reviewGap says why an input of the review's corpus is left out (it is
// an open gap, listed in the PR), or "".
func reviewGap(name string) string {
	f := strings.Fields(name)
	switch {
	case len(f) == 3 && f[0] == "kf" && map[string]bool{"pin": true, "connection.uri": true, "connectionString": true, "DATABASE_URL": true, "dsn": true, "mongodb.connection.string": true, "pgpass": true}[f[1]]:
		return "no credential key: a connection string's userinfo is redact's; pin and pgpass name no secret often enough"
	case len(f) == 3 && f[0] == "kf" && (f[2] == "xml" || f[2] == "xml-entry"):
		return "XML elements are redact's XML rules, which know fewer keys"
	case strings.HasPrefix(name, "mb inkey") && strings.ContainsAny(name, "é€日\U0001F512\u0301"):
		return "a visible character inside a key makes another word"
	case strings.HasPrefix(name, "repeat url"):
		return "a URL whose password holds a blank or a line break is no URL"
	case name == "heredoc", name == "cont closes then new", name == "cont key no value", name == "cont pct no hex":
		return "no value continues onto that line"
	}
	return ""
}
