package kafkatools

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// Writer-controlled header values that are not what the field should hold
// (digits with dashes, a hex string, base64url, a field of a customer record)
// never reach the summary or Data, in any mode, in any of the four forms.
func TestDLQPeekClassAndStageForms(t *testing.T) {
	const m = "ZZMARK"
	bad := []string{
		"4111-1111:1111-" + m,
		"cust_" + m + "_email",
		hex.EncodeToString([]byte(m)),
		base64.RawURLEncoding.EncodeToString([]byte(m)),
		base64.StdEncoding.EncodeToString([]byte(m)),
		m + "Error", // no package
		"x" + base64.RawURLEncoding.EncodeToString([]byte(m)) + "Exception", // no package
		"com.acme." + m, // a valid class name, but not a throwable's
		strings.Repeat("a", 250) + ".XException",
	}
	var recs []clients.DLQRecord
	for i, v := range bad {
		cn := v
		if strings.HasPrefix(v, "com.acme.") {
			// A dotted, capitalised class name is the accepted residual for
			// class.name (see operatorClassName); it is only refused as a throwable.
			cn = "plain"
		}
		recs = append(recs, clients.DLQRecord{Topic: "orders-dlq", Offset: int64(i), Timestamp: ts(i), Headers: hdr(v, "x",
			"__connect.errors.class.name", cn, "__connect.errors.stage", v)})
	}
	for _, mode := range []string{"counts_only", "classes", "messages"} {
		t.Run(mode, func(t *testing.T) {
			k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), recs: recs}
			r, err := peekTool(t, dlqDeps(t, mode, k, defaultCfgs())).Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink","limit":20}`))
			if err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(r.Data)
			for _, text := range []string{r.Summary, string(data)} {
				for _, needle := range []string{m, hex.EncodeToString([]byte(m)), base64.StdEncoding.EncodeToString([]byte(m)),
					base64.RawURLEncoding.EncodeToString([]byte(m)), "4111", "cust_"} {
					if strings.Contains(text, needle) {
						t.Errorf("%s carries %q:\n%s", mode, needle, text)
					}
				}
			}
			if got := r.Data.(dlqData).ExceptionClassCount["?"]; got != len(bad) {
				t.Errorf("class counts = %v, want %d under \"?\"", r.Data.(dlqData).ExceptionClassCount, len(bad))
			}
		})
	}
}

func TestDLQPeekValidNames(t *testing.T) {
	for _, v := range []string{"org.apache.kafka.connect.errors.DataException", "org.apache.kafka.connect.errors.DataException", "java.lang.StackOverflowError", "a.b.$Inner$Throwable", "org.apache.kafka.connect.errors.SerializationException"} {
		if excClassName(v) != v {
			t.Errorf("excClassName(%q) = %q", v, excClassName(v))
		}
	}
	for _, v := range []string{"", "1Exception", "a..bException", "a.b.", "Foo", "x-Exception", "a b.Exception"} {
		if excClassName(v) != "?" {
			t.Errorf("excClassName(%q) = %q, want ?", v, excClassName(v))
		}
	}
	for s := range connectStages {
		if stageName(s) != s {
			t.Errorf("stage %s", s)
		}
	}
	if stageName("value_converter") != "?" || stageName("VALUE_CONVERTER ") != "?" {
		t.Error("stage must match exactly")
	}
}

func TestDLQPeekTraceCutsAtAnyLineBreak(t *testing.T) {
	for _, sep := range []string{"\n", "\r", "\u2028", "\u2029", "\u0085", "\v", "\f"} {
		h := hdr("org.apache.kafka.connect.errors.DataException", "m", "__connect.errors.exception.stacktrace", "first line"+sep+"SECONDLINEMARK")
		line := recordLine(clients.DLQRecord{Timestamp: ts(1), Headers: h}, "messages", tools.Scope{})
		if strings.Contains(line, "SECONDLINEMARK") || !strings.Contains(line, "first line") {
			t.Errorf("sep %q: %s", sep, line)
		}
	}
}

func TestDLQPeekOriginTopicMustBeInScope(t *testing.T) {
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), recs: []clients.DLQRecord{
		{Topic: "orders-dlq", Offset: 2, Timestamp: ts(2), Headers: hdr("org.apache.kafka.connect.errors.DataException", "x")},
		{Topic: "orders-dlq", Offset: 1, Timestamp: ts(1), Headers: hdr("org.apache.kafka.connect.errors.DataException", "x", "__connect.errors.topic", "bill.public.invoices")},
	}}
	r, err := peekTool(t, dlqDeps(t, "", k, defaultCfgs())).Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Summary, "from pg.public.orders/1@25377154") || !strings.Contains(r.Summary, "from ?/1@25377154") || strings.Contains(r.Summary, "bill.public") {
		t.Fatalf("summary:\n%s", r.Summary)
	}
}

func TestDLQPeekSharedTopicWordingAndMissingConnectorName(t *testing.T) {
	noName := hdr("org.apache.kafka.connect.errors.DataException", "x")
	delete(noName, "__connect.errors.connector.name")
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), recs: []clients.DLQRecord{
		{Topic: "orders-dlq", Offset: 3, Timestamp: ts(3), Headers: noName},
		{Topic: "orders-dlq", Offset: 2, Timestamp: ts(2), Headers: hdr("org.apache.kafka.connect.errors.DataException", "x", "__connect.errors.connector.name", "bill-sink")},
	}}
	r, err := peekTool(t, dlqDeps(t, "", k, defaultCfgs())).Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{"counts above are for the whole topic", "1 shown records have no connector.name header"} {
		if !strings.Contains(r.Summary, w) {
			t.Errorf("summary lacks %q:\n%s", w, r.Summary)
		}
	}
	if strings.Contains(r.Summary, "bill-sink") {
		t.Errorf("foreign connector shown:\n%s", r.Summary)
	}
}

func TestDLQPeekDeclaredFlag(t *testing.T) {
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: map[string][]clients.PartitionOffsets{"anything": {{Partition: 0, Start: 0, End: 1}}}}
	r, err := peekTool(t, Deps{Kafka: k}).Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"topic":"anything"}`))
	if err != nil {
		t.Fatal(err)
	}
	if r.Data.(dlqData).Declared {
		t.Fatal("an unscoped topic is not known to be declared")
	}
}
