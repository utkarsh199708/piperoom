package textnorm

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/tools"
)

const schemaTrace = `Task 0: FAILED:
org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler
	at org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execAndHandleError(RetryWithToleranceOperator.java:230)
	at org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execute(RetryWithToleranceOperator.java:156)
	at org.apache.kafka.connect.runtime.WorkerSourceTask.convertTransformedRecord(WorkerSourceTask.java:327)
	at org.apache.kafka.connect.runtime.WorkerSourceTask.sendRecords(WorkerSourceTask.java:355)
	at org.apache.kafka.connect.runtime.WorkerSourceTask.execute(WorkerSourceTask.java:257)
	at org.apache.kafka.connect.runtime.WorkerTask.doRun(WorkerTask.java:188)
Caused by: org.apache.kafka.connect.errors.DataException: Failed to serialize Avro data from topic pg.public.orders :
	at io.confluent.connect.avro.AvroConverter.fromConnectData(AvroConverter.java:93)
	... 13 more
Caused by: org.apache.kafka.common.errors.SerializationException: Error registering Avro schema{"type":"record"}
Caused by: io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Schema being registered is incompatible with an earlier schema for subject "pg.public.orders-value", details: [{oldSchemaVersion: 1}]; error code: 409
	at io.confluent.kafka.schemaregistry.client.rest.RestService.sendHttpRequest(RestService.java:297)
	at io.confluent.kafka.schemaregistry.client.rest.RestService.registerSchema(RestService.java:430)
`

func TestNormalizeSchemaChange(t *testing.T) {
	tpl := Normalize(schemaTrace)
	if len(tpl.Causes) != 5 {
		t.Fatalf("causes = %d, want 5: %s", len(tpl.Causes), tpl)
	}
	root := tpl.Causes[len(tpl.Causes)-1]
	if root.Class != "io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException" {
		t.Errorf("root class = %q", root.Class)
	}
	if !strings.Contains(root.Message, `subject "pg.public.orders-value"`) || !strings.Contains(root.Message, "error code: 409") {
		t.Errorf("root message not masked: %q", root.Message)
	}
	if got := tpl.Causes[1].Frames; len(got) != DefaultFrames || got[0] != "org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execAndHandleError" {
		t.Errorf("frames = %v", got)
	}
}

func TestMasking(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{"lsn", "ERROR: could not start at 0/5A3B2F8", "ERROR: could not start at <lsn>"},
		{"lsn braces", "Streaming from LSN{0/5A3B2F8} failed", "Streaming from <lsn> failed"},
		{"word slash not lsn", "fall back to feed/dead", "fall back to feed/dead"},
		{"offset and partition", "key at pg.public.orders/1@25377154 bad", "key at pg.public.orders/<n>@<n> bad"},
		{"topic partition", "Seek to offset 99 for pg.public.orders-3", "Seek to offset <n> for pg.public.orders-<n>"},
		{"uuid", "id 3f2b8c1e-9a4d-4e0b-8c1a-0123456789ab gone", "id <uuid> gone"},
		{"member keeps group", "Member consumer-order-sync-1-3f2b8c1e-9a4d-4e0b-8c1a-0123456789ab left", "Member consumer-order-sync-<member> left"},
		{"member other group", "Member consumer-billing-1-3f2b8c1e-9a4d-4e0b-8c1a-0123456789ab left", "Member consumer-billing-<member> left"},
		{"ip port", "connect to 172.18.0.6:8083 refused", "connect to <ip> refused"},
		{"host port", "Connection to kafka:29092 refused", "Connection to <host> refused"},
		{"fqdn", "no route to broker-1.kafka.svc.cluster.local", "no route to <host>"},
		{"iso ts", "at 2026-10-11T09:37:00.123Z failed", "at <ts> failed"},
		{"epoch ms", "ts=1760175420123 late", "ts=<n> late"},
		{"quoted ident kept", `column "amount" of relation 'orders' missing`, `column "amount" of relation 'orders' missing`},
		{"quoted value masked", `value "bob smith" bad, token 'a@b' x`, `value "<str>" bad, token '<str>' x`},
		{"json value masked", `{"name":"bob","k": "v"}`, `{"name":"<str>","k": "<str>"}`},
		{"unclosed quote", `say "hello and 'x`, `say "hello and 'x`},
		{"sqlstate 23505", "duplicate key (SQLState: 23505)", "duplicate key (SQLState: 23505)"},
		{"sqlstate 40001", "SQLSTATE 40001 retry 3", "SQLSTATE 40001 retry <n>"},
		{"registry 40401", "schema registry error 40401: Subject 'x y' not found.", "schema registry error 40401: Subject '<str>' not found."},
		{"error code 409", "details []; error code: 409", "details []; error code: 409"},
		{"http 503", "got HTTP/1.1 503 Service Unavailable", "got HTTP/<n>.<n> 503 Service Unavailable"},
		{"status 401", "status=401 after 12 tries", "status=401 after <n> tries"},
		{"email", "notify bob@example.com now", "notify <email> now"},
		{"pg key value", "Detail: Key (email)=(bob@example.com) already exists.", "Detail: Key (email)=(<val>) already exists."},
		{"pg failing row", "Failing row contains (5, bob, 2026-01-01).", "Failing row contains (<row>)."},
		{"debezium struct", "bad Struct{id=5,name=bob,amount=12.50}", "bad Struct{<fields>}"},
		{"brace kv", "WorkerSourceTask{id=pg-orders-0} stopped", "WorkerSourceTask{<fields>} stopped"},
		{"error code host", "error code:40401 now", "error code:40401 now"},
		{"volatile after error", "Error 25377154 records skipped", "Error <n> records skipped"},
		{"volatile after status", "status 1760175420 reached", "status <n> reached"},
		{"volatile after code", "code 77 retry", "code <n> retry"},
		{"value enum", `Could not find value "Bob" in enum`, `Could not find value "<str>" in enum`},
		{"partition suffix", "table orders_2026_10_13 gone", "table orders_<n>_<n>_<n> gone"},
		{"fqdn tld", "no route to broker.example.io", "no route to <host>"},
		{"class not host", "failed javax.net.ssl.SSLHandshakeException here", "failed javax.net.ssl.SSLHandshakeException here"},
		{"setting not host", "request.timeout:30000 exceeded", "request.timeout:<n> exceeded"},
		{"task fraction", "Task 1/3 failed, ab12/cd34", "Task <n>/<n> failed, ab12/cd34"},
		{"lsn after word", "restart lsn 1/22", "restart lsn <lsn>"},
		{"apostrophe kept", "can't read it", "can't read it"},
		{"generation", "Generation 42 is stale", "Generation <n> is stale"},
		{"schema id", "Error retrieving Avro schema for id 100023", "Error retrieving Avro schema for id <n>"},
		{"sqlstate kept", "SQLSTATE 42P01", "SQLSTATE 42P01"},
		{"hex", "handle 0xDEADBEEF", "handle <hex>"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := maskMessage(tc.in); got != tc.want {
				t.Errorf("maskMessage(%q)\n got %q\nwant %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestLogLineForms(t *testing.T) {
	a := `[2026-10-11 09:37:00,123] ERROR WorkerSourceTask{id=pg-orders-0} Task threw an uncaught exception`
	b := `[2026-10-12 11:02:59,001] WARN WorkerSourceTask{id=pg-orders-3} Task threw an uncaught exception`
	if Fingerprint(a) != Fingerprint(b) {
		t.Errorf("log prefix/level/task id changed fingerprint:\n%s\n%s", Normalize(a), Normalize(b))
	}
	x := `level=ERROR msg="devconsumer stopped" err="devconsumer: key at pg.public.orders/1@25377154: not confluent wire format"`
	y := `level=ERROR msg="devconsumer stopped" err="devconsumer: key at pg.public.orders/7@99: not confluent wire format"`
	if Fingerprint(x) != Fingerprint(y) {
		t.Errorf("logfmt differs:\n%s\n%s", Normalize(x), Normalize(y))
	}
	if !strings.Contains(Normalize(x).String(), "not confluent wire format") {
		t.Errorf("logfmt message lost: %s", Normalize(x))
	}
}

func TestVolatileFieldsKeepFingerprint(t *testing.T) {
	variant := strings.NewReplacer(
		":230)", ":999)", ":156)", ":1)", "WorkerSourceTask.java:257", "WorkerSourceTask.java:300",
		"error code: 409", "error code: 409", "oldSchemaVersion: 1", "oldSchemaVersion: 7",
		"topic pg.public.orders :", "topic pg.public.orders :",
	).Replace(schemaTrace)
	if variant == schemaTrace {
		t.Fatal("variant is identical")
	}
	if Fingerprint(variant) != Fingerprint(schemaTrace) {
		t.Errorf("volatile-only difference changed fingerprint:\n%s\n---\n%s", Normalize(schemaTrace), Normalize(variant))
	}
}

func TestDifferentFailuresDiffer(t *testing.T) {
	base := schemaTrace
	otherRoot := strings.Replace(base, "RestClientException", "SSLHandshakeException", 1)
	otherMsg := strings.Replace(base, "is incompatible with an earlier schema", "has an unsupported type", 1)
	fp := Fingerprint(base)
	if Fingerprint(otherRoot) == fp {
		t.Error("different root class gave same fingerprint")
	}
	if Fingerprint(otherMsg) == fp {
		t.Error("different message template gave same fingerprint")
	}
}

func TestTruncatedTraceMatches(t *testing.T) {
	// Deep root-cause frames push the trace over the summary cap, so
	// tools.Truncate cuts inside them.
	full := schemaTrace + strings.Repeat("\tat com.example.Deep.frame(Deep.java:1)\n", 200)
	cut, did := tools.Truncate(full)
	if !did {
		t.Fatal("expected truncation")
	}
	if Fingerprint(full) != Fingerprint(cut) {
		t.Errorf("tools.Truncate output does not match:\n%s\n---\n%s", Normalize(full), Normalize(cut))
	}
	// Sweep every cut after the root cause's first frame line has ended: the
	// partial line before the marker must never become a bogus cause.
	marker := "sendHttpRequest(RestService.java:297)\n"
	start := strings.Index(full, marker) + len(marker)
	want := Fingerprint(full)
	for k := start; k <= len(full); k++ {
		if k > start+400 && k%53 != 0 {
			continue
		}
		c := full[:k] + "\n…[truncated]"
		if got := Fingerprint(c); got != want {
			t.Fatalf("cut at %d (%q) mismatches:\n%s", k, c[max(0, k-30):], Normalize(c))
		}
	}
}

func TestTruncatedSingleLineKept(t *testing.T) {
	a := Normalize("org.x.FooException: " + strings.Repeat("word ", 40) + "\n…[truncated]")
	if len(a.Causes) != 1 || a.Causes[0].Class != "org.x.FooException" {
		t.Errorf("single truncated line lost: %+v", a)
	}
}

func TestRootFramesCount(t *testing.T) {
	npeA := "java.lang.NullPointerException\n\tat com.example.A.run(A.java:10)\n\tat com.example.Main.main(Main.java:5)"
	npeB := "java.lang.NullPointerException\n\tat com.example.B.run(B.java:10)\n\tat com.example.Main.main(Main.java:5)"
	npeA2 := "java.lang.NullPointerException\n\tat com.example.A.run(A.java:99)\n\tat com.example.Main.main(Main.java:8)"
	if Fingerprint(npeA) == Fingerprint(npeB) {
		t.Error("message-less NPEs at different sites share a fingerprint")
	}
	if Fingerprint(npeA) != Fingerprint(npeA2) {
		t.Error("line numbers changed the fingerprint")
	}
}

func TestOuterFramesCount(t *testing.T) {
	a := "a.b.OuterException: boom\n\tat x.One.run(One.java:1)\nCaused by: a.b.RootException: bad\n\tat y.Root.run(Root.java:1)"
	b := strings.Replace(a, "x.One.run", "x.Two.run", 1)
	if Fingerprint(a) == Fingerprint(b) {
		t.Error("outer frame change did not change the fingerprint")
	}
}

func TestModulePrefixStripped(t *testing.T) {
	tpl := Normalize("a.b.E: x\n\tat java.base/java.lang.Thread.run(Thread.java:829)\n\tat app//org.x.Y.z(Y.java:1)")
	got := tpl.Causes[0].Frames
	if len(got) != 2 || got[0] != "java.lang.Thread.run" || got[1] != "org.x.Y.z" {
		t.Errorf("frames = %v", got)
	}
}

func TestLeadingFramesAnonymousCause(t *testing.T) {
	tpl := Normalize("\tat a.B.c(B.java:1)\n\tat a.B.d(B.java:2)\nCaused by: x.YException: z")
	if len(tpl.Causes) != 2 || tpl.Causes[0].Class != "" || len(tpl.Causes[0].Frames) != 2 || tpl.Causes[1].Class != "x.YException" {
		t.Errorf("template = %+v", tpl)
	}
}

func TestMessageClip(t *testing.T) {
	msg := strings.Repeat("abcdefghij ", 100)
	tpl := Normalize("a.b.CException: " + msg)
	if got := len(tpl.Causes[0].Message); got != MaxMessageBytes {
		t.Errorf("message len = %d, want %d", got, MaxMessageBytes)
	}
	// The tail beyond the cap must not change the fingerprint.
	if Fingerprint("a.b.CException: "+msg) != Fingerprint("a.b.CException: "+msg+"different tail") {
		t.Error("text past the cap changed the fingerprint")
	}
}

func TestLongChainKeepsRoot(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 100; i++ {
		b.WriteString("Caused by: a.b.CException: step\n")
	}
	one := b.String() + "Caused by: a.b.RootAException: root\n"
	two := b.String() + "Caused by: a.b.RootBException: root\n"
	tpl := Normalize(one)
	if len(tpl.Causes) > MaxCauses {
		t.Fatalf("causes = %d", len(tpl.Causes))
	}
	if last := tpl.Causes[len(tpl.Causes)-1]; last.Class != "a.b.RootAException" {
		t.Errorf("root lost, last = %q", last.Class)
	}
	if Fingerprint(one) == Fingerprint(two) {
		t.Error("different roots share a fingerprint")
	}
}

func TestEvidenceCodesDiffer(t *testing.T) {
	pairs := [][2]string{
		{"ERROR: duplicate key (SQLState: 23505)", "ERROR: duplicate key (SQLState: 40001)"},
		{"schema registry error 40401: Subject 'a' not found.", "schema registry error 40403: Subject 'a' not found."},
		{"RestClientException: x; error code: 409", "RestClientException: x; error code: 422"},
		{"got HTTP 401", "got HTTP 503"},
	}
	for _, p := range pairs {
		if Fingerprint(p[0]) == Fingerprint(p[1]) {
			t.Errorf("same fingerprint:\n%s\n%s", p[0], p[1])
		}
	}
}

func TestNamesDifferValuesDont(t *testing.T) {
	if Fingerprint(`ERROR: relation "orders" does not exist`) == Fingerprint(`ERROR: relation "payments" does not exist`) {
		t.Error("different relations share a fingerprint")
	}
	if Fingerprint("Member consumer-order-sync-1-3f2b8c1e-9a4d-4e0b-8c1a-0123456789ab left") == Fingerprint("Member consumer-billing-1-3f2b8c1e-9a4d-4e0b-8c1a-0123456789ab left") {
		t.Error("different consumer groups share a fingerprint")
	}
	a := "ERROR: duplicate key\nDetail: Key (email)=(bob@example.com) already exists."
	b := "ERROR: duplicate key\nDetail: Key (email)=(alice@other.org) already exists."
	if Fingerprint(a) != Fingerprint(b) {
		t.Error("record value changed the fingerprint")
	}
	if s := Normalize(a).String(); strings.Contains(s, "bob") || strings.Contains(s, "example.com") {
		t.Errorf("record value leaked into template: %s", s)
	}
}

func TestBounds(t *testing.T) {
	big := strings.Repeat("Caused by: a.b.CException: x 1\n\tat a.B.c(B.java:1)\n", 50000)
	if tpl := Normalize(big); len(tpl.Causes) > MaxCauses {
		t.Errorf("causes = %d", len(tpl.Causes))
	}
	if m := Normalize(strings.Repeat("9", 1<<20)).Causes[0].Message; m != "<hex>" {
		t.Errorf("message = %q", m)
	}
	if m := Normalize(strings.Repeat("a1b ", 1<<18)).Causes[0].Message; len(m) != MaxMessageBytes {
		t.Errorf("message len %d, want %d", len(m), MaxMessageBytes)
	}
	for _, in := range []string{"", "\n\n", "\xff\xfe", "Caused by:", "at (", "at x(", strings.Repeat("(", 5000)} {
		if len(Fingerprint(in)) != FingerprintLen {
			t.Errorf("fingerprint(%q) wrong length", in)
		}
	}
}

// scenarioTexts returns every tool error/summary/message text in the eval
// fixtures, so the normaliser is tested on the strings the agent really sees.
func scenarioTexts(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("../../evals/scenarios/*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no scenario fixtures found: %v", err)
	}
	var out []string
	var walk func(n *yaml.Node)
	walk = func(n *yaml.Node) {
		if n.Kind == yaml.MappingNode {
			for i := 0; i+1 < len(n.Content); i += 2 {
				k, v := n.Content[i].Value, n.Content[i+1]
				if v.Kind == yaml.ScalarNode && (k == "error" || k == "summary" || k == "text") {
					out = append(out, v.Value)
				}
			}
		}
		for _, c := range n.Content {
			walk(c)
		}
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var doc yaml.Node
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		walk(&doc)
	}
	return out
}

var digitRun = regexp.MustCompile(`\b\d+\b`)

// evidencePosition lists, independently of the package code, where the
// scenarios carry evidence codes that must not be bumped.
var evidencePosition = regexp.MustCompile(`(?i)(sqlstate|error[ _]?code|status|http\S*|error)[\s:="'\[(]*$`)

// bumpNumbers changes every standalone number to another of the same shape
// (timestamps stay timestamps), except evidence codes, which are kept on purpose.
func bumpNumbers(s string) string {
	idx := digitRun.FindAllStringIndex(s, -1)
	var b strings.Builder
	last := 0
	for _, m := range idx {
		b.WriteString(s[last:m[0]])
		d := s[m[0]:m[1]]
		if evidencePosition.MatchString(s[:m[0]]) {
			b.WriteString(d)
		} else {
			bs := []byte(d)
			for i := range bs {
				bs[i] = '0' + (bs[i]-'0'+3)%10
			}
			b.Write(bs)
		}
		last = m[1]
	}
	b.WriteString(s[last:])
	return b.String()
}

func TestScenarioTexts(t *testing.T) {
	texts := scenarioTexts(t)
	if len(texts) < 20 {
		t.Fatalf("only %d texts found", len(texts))
	}
	seen := map[string]string{} // fingerprint -> template text
	for _, s := range texts {
		tpl := Normalize(s)
		fp := tpl.Fingerprint()
		if len(fp) != FingerprintLen {
			t.Fatalf("bad fingerprint %q", fp)
		}
		if fp != Fingerprint(s) {
			t.Errorf("unstable fingerprint for %q", s)
		}
		// Volatile-only change: every standalone number becomes another
		// number of the same shape (timestamps stay timestamps).
		mut := bumpNumbers(s)
		if Fingerprint(mut) != fp {
			t.Errorf("numbers changed fingerprint:\n%s\n---\n%s", tpl, Normalize(mut))
		}
		// Different templates must not collide.
		if prev, ok := seen[fp]; ok && prev != tpl.String() {
			t.Errorf("collision %s:\n%s\n---\n%s", fp, prev, tpl)
		}
		seen[fp] = tpl.String()
	}
}

func FuzzNormalize(f *testing.F) {
	f.Add(schemaTrace)
	f.Add("")
	f.Add("Caused by: org.postgresql.util.PSQLException: ERROR: must be owner of table orders")
	f.Add("ERROR: could not start at 0/5A3B2F8 LSN{1/ABC}")
	f.Add(`level=ERROR msg="x" err="y \" 'z' 1.2.3.4:9092"`)
	f.Add("\tat java.base/java.lang.Thread.run(Thread.java:829)\n... 5 more")
	f.Add("consumer-g-1-3f2b8c1e-9a4d-4e0b-8c1a-0123456789ab 2026-10-11T09:37:00Z")
	f.Add("\xff\xfe\x00 at ( at a( 'unterminated \"unterminated")
	for _, s := range scenarioSeeds() {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		tpl := Normalize(s)
		if len(tpl.Causes) > MaxCauses {
			t.Fatalf("causes = %d", len(tpl.Causes))
		}
		for _, c := range tpl.Causes {
			if len(c.Message) > MaxMessageBytes || len(c.Frames) > DefaultFrames {
				t.Fatalf("unbounded cause: %+v", c)
			}
		}
		fp := tpl.Fingerprint()
		if len(fp) != FingerprintLen || fp != Fingerprint(s) {
			t.Fatalf("bad or unstable fingerprint %q", fp)
		}
	})
}

// scenarioSeeds reads fixture texts for the fuzz corpus; it returns nothing
// when the fixtures cannot be read (the fuzz target still runs on its seeds).
func scenarioSeeds() []string {
	files, _ := filepath.Glob("../../evals/scenarios/*.yaml")
	var out []string
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		for _, l := range strings.Split(string(raw), "\n") {
			if strings.Contains(l, "Exception") || strings.Contains(l, "error") {
				out = append(out, l)
			}
		}
	}
	if len(out) > 40 {
		out = out[:40]
	}
	return out
}

func TestNestedBodiesMasked(t *testing.T) {
	tests := []struct{ name, in string }{
		{"nested struct", "bad Struct{id=5,name=bob,addr=Struct{city=Paris}} here"},
		{"nested braces", "bad {id=5, name=bob, addr={city=Paris}} here"},
		{"envelope", "Struct{before=Struct{id=5,name=bob},after=Struct{id=5,name=bobby},op=u}"},
		{"inner parens", "Key (name)=(bob (jr)) already exists."},
		{"failing row", "Failing row contains (5, bob (jr), 2026-01-01)."},
	}
	for _, tc := range tests {
		got := Normalize(tc.in).String()
		for _, leak := range []string{"bob", "Paris", "jr"} {
			if strings.Contains(got, leak) {
				t.Errorf("%s: %q leaked into %q", tc.name, leak, got)
			}
		}
	}
	if got := maskMessage("Key (name)=(bob (jr)) already exists."); got != "Key (name)=(<val>) already exists." {
		t.Errorf("inner parens: %q", got)
	}
}

// A body that is cut by the line cap has no closer; everything after the
// opener is a value and must be masked to the end of the line.
func TestCutBodiesMasked(t *testing.T) {
	pad := strings.Repeat("x", 300)
	openers := []string{"Struct{name=bob,", "{id=5,name=bob,", "Failing row contains (5, bob, ", "Key (email)=(bob"}
	for _, o := range openers {
		in := pad + " " + o + strings.Repeat("secretvalue ", 100) + "}) tail"
		got := Normalize(in).String()
		if strings.Contains(got, "bob") || strings.Contains(got, "secret") {
			t.Errorf("opener %q leaked: %q", o, got)
		}
	}
	// Opener early in the line, closer beyond the line cap: no closer is seen.
	in := "msg Struct{name=bob," + strings.Repeat("a", 500) + "} tail"
	if got := Normalize(in).String(); strings.Contains(got, "bob") || strings.Contains(got, "tail") {
		t.Errorf("line cap not applied before masking: %q", got)
	}
}

func TestLineCapAndBudget(t *testing.T) {
	// maxLineBytes: a line is cut before parsing, so text past it is gone.
	long := "a.b.CException: " + strings.Repeat("a", maxLineBytes) + " Struct{name=bob}"
	if got := Normalize(long).String(); strings.Contains(got, "Struct") {
		t.Errorf("text past maxLineBytes survived: %q", got)
	}
	// budgetBytes: head causes stop once the budget is spent; the final cause stays.
	var b strings.Builder
	for i := 0; i < 100; i++ {
		b.WriteString("a.b.CException: " + strings.Repeat("w", 300) + "\n")
	}
	b.WriteString("a.b.RootException: the end\n")
	tpl := Normalize(b.String())
	if max := budgetBytes/300 + 2; len(tpl.Causes) > max {
		t.Errorf("causes = %d, want <= %d", len(tpl.Causes), max)
	}
	if len(tpl.Causes) < 20 {
		t.Errorf("budget too small: %d causes", len(tpl.Causes))
	}
	if last := tpl.Causes[len(tpl.Causes)-1]; last.Class != "a.b.RootException" {
		t.Errorf("root lost: %q", last.Class)
	}
}

func TestRootIsLastClassCause(t *testing.T) {
	// A trailing log line must not become the root: frames of the real root
	// (the last class-bearing cause) still count only up to RootFrames.
	a := "a.b.OuterException: x\nCaused by: a.b.RootException: y\n\tat r.One.m(One.java:1)\n\tat r.Two.m(Two.java:1)\nWorker stopped"
	b := strings.Replace(a, "r.Two.m", "r.Other.m", 1)
	if Fingerprint(a) != Fingerprint(b) {
		t.Error("second root frame changed the fingerprint with a trailing log line")
	}
	c := strings.Replace(a, "r.One.m", "r.Other.m", 1)
	if Fingerprint(a) == Fingerprint(c) {
		t.Error("first root frame ignored with a trailing log line")
	}
}
