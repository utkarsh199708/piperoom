package dlq

import (
	"strings"
	"testing"
)

func TestRedactMessageUnquotedPII(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		gone   []string
		intact []string
	}{
		{"email", "failed for jane.doe@example.com at row 5", []string{"jane.doe", "example.com"}, []string{"failed for", "at row 5"}},
		{"card grouped", "card 4111 1111 1111 1111 rejected", []string{"4111 1111"}, []string{"card", "rejected"}},
		{"card dashed", "pan 5500-0000-0000-0004 bad", []string{"5500-0000"}, []string{"pan"}},
		{"card contiguous", "pan=4111111111111111.", []string{"4111111111111111"}, []string{"pan="}},
		{"ssn", "ssn 123-45-6789 invalid", []string{"123-45-6789"}, []string{"ssn", "invalid"}},
		{"phone", "call +1 555-123-4567 now", []string{"555-123-4567"}, []string{"call", "now"}},
		{"password kv", "password=hunter2 user=app", []string{"hunter2"}, []string{"password=", "user=app"}},
		{"short token kv", "token=xyz, retry", []string{"xyz"}, []string{"token=", "retry"}},
		{"api key colon", "api_key: abcdef123456 next", []string{"abcdef123456"}, []string{"api_key:", "next"}},
		{"evidence kept", "offset 1697040000000 partition 3 lsn 0/16B3748 on 2026-10-14 ts 1697040000123", nil,
			[]string{"1697040000000", "0/16B3748", "2026-10-14", "1697040000123", "partition 3"}},
		{"non-luhn long number kept", "offset 4111111111111112", nil, []string{"4111111111111112"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RedactMessage(tt.in)
			for _, g := range tt.gone {
				if strings.Contains(got, g) {
					t.Errorf("RedactMessage(%q) = %q, still has %q", tt.in, got, g)
				}
			}
			for _, k := range tt.intact {
				if !strings.Contains(got, k) {
					t.Errorf("RedactMessage(%q) = %q, lost %q", tt.in, got, k)
				}
			}
			if again := RedactMessage(got); again != got {
				t.Errorf("not idempotent: %q -> %q", got, again)
			}
		})
	}
}

func TestRedactTraceKeepsStructure(t *testing.T) {
	in := `Caused by: RestClientException: Schema being registered is incompatible with an earlier schema for subject "pg.public.orders-value", details: [{errorType:'MISSING_UNION_BRANCH', description:'The new schema is missing a type'}, {oldSchemaVersion: 1}]; error code: 409`
	got := RedactTrace(in)
	for _, want := range []string{`"pg.public.orders-value"`, "MISSING_UNION_BRANCH", "oldSchemaVersion: 1", "error code: 409", "[{"} {
		if !strings.Contains(got, want) {
			t.Fatalf("RedactTrace lost %q: %q", want, got)
		}
	}

	in = `Caused by: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint "users_email_key" Detail: Key (email)=(jane@example.com) already exists.`
	got = RedactTrace(in)
	if strings.Contains(got, "jane") || !strings.Contains(got, `"users_email_key"`) {
		t.Fatalf("RedactTrace = %q", got)
	}

	in = `DataException: Failed to deserialize {"email":"jane@example.com","ssn":"123-45-6789","card":4111111111111111}`
	got = RedactTrace(in)
	for _, leak := range []string{"jane", "123-45", "4111"} {
		if strings.Contains(got, leak) {
			t.Fatalf("RedactTrace leaked %q: %q", leak, got)
		}
	}
	if !strings.Contains(got, `"email"`) {
		t.Fatalf("field name lost: %q", got)
	}
}
