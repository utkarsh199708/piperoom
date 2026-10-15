package dlq

import (
	"strings"
	"testing"
)

// Unquoted values in a Connect message: the quote and group rules never saw
// them, so they went out whole.
func TestRedactMessageUnquotedValues(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		gone   []string
		intact []string
	}{
		{"email", "failed for jane.doe@example.com at row 5", []string{"jane.doe", "example.com"}, []string{"failed for", "at row 5"}},
		{"url password", "Connection to jdbc:postgresql://app:S3cretPw@db:5432/orders refused", []string{"S3cretPw"}, []string{"Connection to", "refused"}},
		{"password kv", "password=hunter2 user=app", []string{"hunter2"}, []string{"password=", "user=app"}},
		{"short token kv", "token=xyz, retry", []string{"xyz"}, []string{"token=", "retry"}},
		{"api key colon", "api_key: abcdef123456 next", []string{"abcdef123456"}, []string{"api_key:", "next"}},
		{"ssn", "ssn 123-45-6789 invalid", []string{"123-45-6789"}, []string{"ssn", "invalid"}},
		{"card grouped", "card 4111 1111 1111 1111 rejected", []string{"4111 1111"}, []string{"card", "rejected"}},
		{"card contiguous", "pan=4111111111111111.", []string{"4111111111111111"}, []string{"pan="}},
		{"phone", "call +1 555-123-4567 now", []string{"555-123-4567"}, []string{"call", "now"}},
		{"evidence kept", "offset 1697040000000 partition 3 lsn 0/16B3748 on 2026-10-14 ts 1697040000123", nil,
			[]string{"1697040000000", "0/16B3748", "2026-10-14", "1697040000123", "partition 3"}},
		{"non-luhn long number kept", "offset 4111111111111112", nil, []string{"4111111111111112"}},
		{"exception class kept", "org.apache.kafka.connect.errors.DataException: Converting byte[] to Kafka Connect data failed", nil,
			[]string{"org.apache.kafka.connect.errors.DataException", "Converting byte"}},
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
			if n := len([]rune(got)); n > MaxMessageRunes {
				t.Errorf("over the cap: %d runes", n)
			}
		})
	}
}
