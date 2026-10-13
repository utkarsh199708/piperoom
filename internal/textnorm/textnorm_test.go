package textnorm

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

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
	if !strings.Contains(root.Message, `subject "<str>"`) || !strings.Contains(root.Message, "error code: <n>") {
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
		{"member", "Member consumer-order-sync-1-3f2b8c1e-9a4d-4e0b-8c1a-0123456789ab left", "Member <member> left"},
		{"ip port", "connect to 172.18.0.6:8083 refused", "connect to <ip> refused"},
		{"host port", "Connection to kafka:29092 refused", "Connection to <host> refused"},
		{"fqdn", "no route to broker-1.kafka.svc.cluster.local", "no route to <host>"},
		{"iso ts", "at 2026-10-11T09:37:00.123Z failed", "at <ts> failed"},
		{"epoch ms", "ts=1760175420123 late", "ts=<n> late"},
		{"quoted", `column "amount" of relation 'orders' missing`, `column "<str>" of relation '<str>' missing`},
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
		"pg.public.orders-value", "pg.public.payments-value",
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
	// A long root-cause frame list pushes the trace over the summary cap, so
	// tools.Truncate cuts inside the root cause's frames.
	var b strings.Builder
	b.WriteString(schemaTrace)
	for i := 0; i < 200; i++ {
		b.WriteString("\tat com.example.Deep.frame(Deep.java:1)\n")
	}
	full := b.String()
	cut, did := tools.Truncate(full)
	if !did {
		t.Fatal("expected truncation")
	}
	if !strings.Contains(cut, "RestClientException") {
		t.Fatal("test setup: root cause line did not survive truncation")
	}
	if Fingerprint(full) != Fingerprint(cut) {
		t.Errorf("truncated trace does not match:\n%s\n---\n%s", Normalize(full), Normalize(cut))
	}
}

func TestBounds(t *testing.T) {
	big := strings.Repeat("Caused by: a.b.CException: x 1\n\tat a.B.c(B.java:1)\n", 50000)
	start := time.Now()
	tpl := Normalize(big)
	if len(tpl.Causes) > MaxCauses {
		t.Errorf("causes = %d", len(tpl.Causes))
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("slow: %v", time.Since(start))
	}
	long := strings.Repeat("9", 1<<20)
	if m := Normalize(long).Causes[0].Message; len(m) > MaxMessageBytes {
		t.Errorf("message len %d", len(m))
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

func bumpDigits(d string) string {
	b := []byte(d)
	for i := range b {
		b[i] = '0' + (b[i]-'0'+3)%10
	}
	return string(b)
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
		mut := digitRun.ReplaceAllStringFunc(s, bumpDigits)
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
