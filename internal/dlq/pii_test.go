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

// A value that straddles the 300-rune cap is cut out whole. Before, the cut
// left "jane.doe@exam" or "123-45-678", which no mask matched (#392).
func TestRedactMessageCapNeverSplitsAValue(t *testing.T) {
	values := []struct{ text, leak string }{
		{"for jane.doe@example.com", "jane.doe"},
		{"ssn 123-45-6789", "123-45"},
		{"phone 555-123-4567", "555-123"},
		{"card 4111 1111 1111 1111", "4111 1111"},
		{"intl +44 20 7946 0958", "7946"},
		{"jdbc:postgresql://app:S3cretPw@db:5432/orders", "S3cr"},
	}
	for _, v := range values {
		for pad := 260; pad <= 300; pad++ {
			in := strings.Repeat("x", pad-1) + " " + v.text + " and the rest of the message"
			got := RedactMessage(in)
			if strings.Contains(got, v.leak) {
				t.Errorf("pad %d: RedactMessage kept %q of %q: %q", pad, v.leak, v.text, got[max(0, len(got)-40):])
			}
			if n := len([]rune(got)); n > MaxMessageRunes {
				t.Errorf("pad %d: over the cap: %d runes", pad, n)
			}
			if again := RedactMessage(got); again != got {
				t.Errorf("pad %d: not idempotent: %q -> %q", pad, got, again)
			}
		}
	}
}

// The safe cut keeps ordinary evidence at the cap: an offset, an LSN and a
// timestamp that straddle it are not taken for a value and lose only the
// word that was split, and a very long word is cut where the cap falls.
func TestRedactMessageCapKeepsEvidence(t *testing.T) {
	head := strings.Repeat("x ", 140) // 280 runes
	for _, tt := range []struct{ tail, keep string }{
		{"partition 3 offset 1697040000000 next", "partition 3"},
		{"lsn 0/16B3748 at 2026-10-14T10:00:00Z", "lsn 0/16B3748"},
		{"ts 1697040000123 and offsets 1234520-1234619", "ts 1697040000123"},
	} {
		got := RedactMessage(head + tt.tail)
		if !strings.Contains(got, tt.keep) || strings.Contains(got, Redacted) {
			t.Errorf("RedactMessage(...%q) = ...%q, want %q kept and nothing masked", tt.tail, got[len(head)-2:], tt.keep)
		}
	}
	if got := RedactMessage(strings.Repeat("a", 1000)); len(got) != MaxMessageRunes {
		t.Errorf("a long word is cut at the cap, got %d runes", len(got))
	}
}

// #426: E.164 and international phone numbers and a bare bearer token are
// masked; offset ranges and positions that pass Luhn or look like a phone
// are not.
func TestRedactMessagePhonesBearerPositions(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		gone   []string
		intact []string
	}{
		{"intl grouped", "phone +44 20 7946 0958 now", []string{"7946", "0958"}, []string{"phone", "now"}},
		{"e164", "phone +15551234567 now", []string{"5551234567"}, []string{"phone", "now"}},
		{"intl dashes", "tel +49-30-1234-5678.", []string{"1234-5678"}, []string{"tel"}},
		{"bare bearer", "auth header bearer abcdef123456 sent", []string{"abcdef123456"}, []string{"auth header bearer", "sent"}},
		{"bearer any case", "got Bearer eyJhbGciOi.x.y here", []string{"eyJhbGciOi"}, []string{"here"}},
		{"luhn offset range", "skipped offsets 1234520-1234619 on orders-0", nil, []string{"1234520-1234619", "orders-0"}},
		{"offset phone shape", "Topic orders-dlq partition 0 offset 555 123 4567", nil, []string{"offset 555 123 4567"}},
		{"partition phone shape", "partitions: 555-123-4567", nil, []string{"555-123-4567"}},
		{"short plus number", "retry +1 2 3 times", nil, []string{"+1 2 3"}},
		{"plus offset delta", "lag +12 at 10:00", nil, []string{"+12"}},
		{"grouped luhn bad first digit", "ids 1234 5678 9012 3452", nil, []string{"1234 5678 9012 3452"}},
		{"grouped amex", "card 3782 822463 10005 ok", []string{"822463"}, []string{"card", "ok"}},
		{"phone not after offset", "call 555 123 4567 now", []string{"555 123 4567"}, []string{"call", "now"}},
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

func TestMaskPIIKeepsShape(t *testing.T) {
	in := "ALTER TABLE public.t ADD CONSTRAINT c CHECK (email <> 'jane.doe@example.com' AND card <> '4111111111111111' AND ssn <> '123-45-6789')"
	got := MaskPII(in)
	for _, leak := range []string{"jane.doe", "example.com", "4111111111111111", "123-45-6789"} {
		if strings.Contains(got, leak) {
			t.Errorf("MaskPII kept %q: %q", leak, got)
		}
	}
	if !strings.HasPrefix(got, "ALTER TABLE public.t ADD CONSTRAINT c CHECK (email <> '") {
		t.Errorf("MaskPII changed the statement: %q", got)
	}
}
