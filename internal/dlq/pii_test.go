package dlq

import (
	"strings"
	"testing"
)

// fakeStripeKey is built at run time so the source holds no string that
// secret scanners take for a live key.
var fakeStripeKey = "sk_" + "live_" + "51HxAbCdEfGhIjKlMnOpQrStUv"

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
		{"email non-ascii local part", "failed for jané@example.com now", []string{"jané", "example.com"}, []string{"failed for", "now"}},
		{"email non-ascii domain", "failed for jane@exämple.com now", []string{"jane@", "exämple"}, []string{"failed for", "now"}},
		{"bearer after token key", "token: Bearer abcdef123456 next", []string{"Bearer", "abcdef123456"}, []string{"token:", "next"}},
		{"basic after credential key", "credentials=Basic dXNlcjpwYXNz, retry", []string{"dXNlcjpwYXNz"}, []string{"credentials=", "retry"}},
		{"long token keeps no head or tail", "key " + fakeStripeKey + " rejected", []string{"sk_l", "Uv ", "51Hx"}, []string{"key", "rejected"}},
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

// A cut at the cap can drop the closing quote of a value. The open single
// quote then takes the rest of the text instead of reading as an apostrophe.
func TestRedactMessageQuoteCutAtCap(t *testing.T) {
	in := "Failed to convert value for customer 'Jane Q. Doe, 12 Elm St, " + strings.Repeat("filler ", 50) + "' at offset 5"
	got := RedactMessage(in)
	for _, leak := range []string{"Jane", "Elm St", "filler"} {
		if strings.Contains(got, leak) {
			t.Fatalf("RedactMessage kept %q past a cut quote: %q", leak, got)
		}
	}
	if !strings.HasPrefix(got, "Failed to convert value for customer") || !strings.HasSuffix(got, Redacted) {
		t.Fatalf("RedactMessage = %q", got)
	}
	if again := RedactMessage(got); again != got {
		t.Fatalf("not idempotent: %q -> %q", got, again)
	}
	// No cut: an apostrophe without a closer is still an apostrophe.
	if got := RedactMessage("the user's row and 'odd text"); got != "the user's row and 'odd text" {
		t.Fatalf("uncut apostrophe changed: %q", got)
	}
}
