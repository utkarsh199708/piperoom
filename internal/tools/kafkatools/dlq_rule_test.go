package kafkatools

import (
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// TestDLQPeekRejectRuleInEveryMode covers KAN-412 TC1/TC2: the value-free
// reject rule is shown in the most restrictive mode as well, and an exception
// message (which may quote the rejected value) appears only in messages mode,
// redacted.
func TestDLQPeekRejectRuleInEveryMode(t *testing.T) {
	const msg = `unknown status "gift" password=hunter2`
	rec := clients.DLQRecord{
		Topic: "orders-dlq", Partition: 1, Offset: 412, Timestamp: ts(58),
		Headers: hdr("org.apache.kafka.connect.errors.DataException", msg, hRejectRule, "status_enum"),
	}
	tests := []struct {
		mode    string
		want    []string
		wantNot []string
	}{
		{"counts_only",
			[]string{"headers: counts_only", "p1@412 2026-10-13T09:58:00Z rule=status_enum"},
			[]string{"gift", "hunter2", "unknown status", "msg=", "stage="}},
		{"classes",
			[]string{"headers: classes", "p1@412 2026-10-13T09:58:00Z rule=status_enum org.apache.kafka.connect.errors.DataException stage=VALUE_CONVERTER"},
			[]string{"gift", "hunter2", "unknown status", "msg="}},
		{"messages",
			[]string{"headers: messages", "rule=status_enum", `msg="unknown status <redacted> password=<redacted>"`},
			[]string{"gift", "hunter2"}},
		{"",
			[]string{"rule=status_enum"},
			[]string{"gift", "hunter2", "msg="}},
	}
	for _, tc := range tests {
		name := tc.mode
		if name == "" {
			name = "default"
		}
		t.Run(name, func(t *testing.T) {
			k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), recs: []clients.DLQRecord{rec}}
			r, err := peekTool(t, dlqDeps(t, tc.mode, k, defaultCfgs())).Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range tc.want {
				if !strings.Contains(r.Summary, w) {
					t.Errorf("summary lacks %q:\n%s", w, r.Summary)
				}
			}
			for _, w := range tc.wantNot {
				if strings.Contains(r.Summary, w) {
					t.Errorf("summary has %q:\n%s", w, r.Summary)
				}
			}
			data, _ := json.Marshal(r.Data)
			if strings.Contains(string(data), "status_enum") {
				t.Errorf("Data carries the rule: %s", data)
			}
			if len(r.Summary) > tools.MaxSummaryBytes {
				t.Fatalf("summary %d bytes", len(r.Summary))
			}
		})
	}
}

// TestDLQPeekRejectRuleHostileValues covers TC3: a writer can put anything in
// the header, so a value that is not a plain rule name is shown as "?", capped
// and never spread over a second line. The hostile bytes reach neither the
// summary nor Data.
func TestDLQPeekRejectRuleHostileValues(t *testing.T) {
	const marker = "ZZRULEMARK"
	tests := []struct {
		name  string
		value string
	}{
		{"html", "<b>" + marker + "</b>"},
		{"10 KB", strings.Repeat("A", 10*1024)},
		{"newlines", "status_enum\n" + marker + "\nrule=forged"},
		{"quote", `status_enum" onload="` + marker},
		{"card number", "4111" + strings.Repeat("1111", 3)},
		{"too long a name", strings.Repeat("A", 65)},
		{"rejection text", `unknown status "gift"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := clients.DLQRecord{
				Topic: "orders-dlq", Partition: 0, Offset: 7, Timestamp: ts(1),
				Headers: hdr("org.apache.kafka.connect.errors.DataException", "m", hRejectRule, tc.value),
			}
			k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), recs: []clients.DLQRecord{rec}}
			r, err := peekTool(t, dlqDeps(t, "counts_only", k, defaultCfgs())).Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(r.Summary, "p0@7 2026-10-13T09:01:00Z rule=?") {
				t.Errorf("summary lacks the \"?\" fallback:\n%s", r.Summary)
			}
			data, _ := json.Marshal(r.Data)
			for _, text := range []string{r.Summary, string(data)} {
				for _, needle := range []string{marker, "<b>", "forged", "gift", "4111"} {
					if strings.Contains(text, needle) {
						t.Errorf("text carries %q:\n%s", needle, text)
					}
				}
			}
			// A hostile value must not add a record line or a second rule field.
			if got := strings.Count(r.Summary, "\n  "); got != 1 {
				t.Errorf("summary has %d record lines, want 1:\n%s", got, r.Summary)
			}
			if got := strings.Count(r.Summary, "rule="); got != 1 {
				t.Errorf("summary has %d rule fields, want 1:\n%s", got, r.Summary)
			}
		})
	}
}

// TestDLQPeekRejectRuleAbsent: a record without the header (a Connect sink
// writes none) gets no rule field and is not an error; the record line is
// byte-for-byte what it was before KAN-412.
func TestDLQPeekRejectRuleAbsent(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want string
	}{
		{"counts_only", "  p0@9 2026-10-13T09:05:00Z"},
		{"classes", "  p0@9 2026-10-13T09:05:00Z org.apache.kafka.connect.errors.DataException stage=VALUE_CONVERTER"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			rec := clients.DLQRecord{Topic: "orders-dlq", Partition: 0, Offset: 9, Timestamp: ts(5),
				Headers: hdr("org.apache.kafka.connect.errors.DataException", "m")}
			k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), recs: []clients.DLQRecord{rec}}
			r, err := peekTool(t, dlqDeps(t, tc.mode, k, defaultCfgs())).Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(r.Summary, "rule=") {
				t.Errorf("summary has a rule without the header:\n%s", r.Summary)
			}
			if !strings.Contains(r.Summary, tc.want) {
				t.Errorf("summary lacks %q:\n%s", tc.want, r.Summary)
			}
		})
	}
	// An empty header value reads as absent.
	k := &fakeDLQ{fakeKafka: &fakeKafka{}, offsets: okOffsets(), recs: []clients.DLQRecord{
		{Topic: "orders-dlq", Partition: 0, Offset: 9, Timestamp: ts(5), Headers: map[string]string{hRejectRule: "   "}},
	}}
	r, err := peekTool(t, dlqDeps(t, "counts_only", k, defaultCfgs())).Run(scoped(), json.RawMessage(`{"connector":"pg-orders-sink"}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(r.Summary, "rule=") {
		t.Errorf("empty rule header produced a rule field:\n%s", r.Summary)
	}
}

func TestRejectRuleShape(t *testing.T) {
	tests := []struct{ in, want string }{
		{"status_enum", "status_enum"},
		{"StatusEnum2", "StatusEnum2"},
		{"  status_enum  ", "status_enum"}, // the client trims; a fake may not
		{"rules.status_enum", "?"},         // a dot is not the grammar
		{"status-enum", "?"},
		{"status enum", "?"},
		{"1status", "?"},
		{"", ""},
		{"   ", ""},
		{"unknown status \"gift\"", "?"},
		{strings.Repeat("a", 64), strings.Repeat("a", 64)},
		{strings.Repeat("a", 65), "?"},
		{"status_enum\nrule=forged", "?"},
		{"<b>rule</b>", "?"},
		{"4111111111111111", "?"},
	}
	for _, tc := range tests {
		got := rejectRule(map[string]string{hRejectRule: tc.in})
		if got != tc.want {
			t.Errorf("rejectRule(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if got := rejectRule(nil); got != "" {
		t.Errorf("rejectRule(nil) = %q, want empty", got)
	}
}
