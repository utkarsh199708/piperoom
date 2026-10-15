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
		{"card 2223 0031 2200 3222", "2223 0031"},
		{"card 2223-0031-2200-3222", "2223-0031"},
		{"card 2221 0000 0000 0009", "2221 0000"},
		{"card 2720 9900 0000 0007", "2720 9900"},
		{"card 2200 0000 0000 0004", "2200 0000"},
		{"phone 555\t123\t4567", "555 123"},
		{"phone 555\u00a0123\u00a04567", "555 123"},
		{"phone 555  123  4567", "555 123"},
		{"card 4111  1111  1111  1111", "4111 1111"},
		{"card 4111\t1111\t1111\t1111", "4111 1111"},
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
	// A masked value at the cap is replaced, its key kept: the message is
	// never emptied.
	for in, want := range map[string]string{
		"password=" + strings.Repeat("a", 320) + " failed to write": "password=" + Redacted,
		"jane@" + strings.Repeat("a", 320) + ".com failed":          Redacted,
	} {
		if got := RedactMessage(in); got != want {
			t.Errorf("RedactMessage(%.20q...) = %q, want %q", in, got, want)
		}
	}
	// An offset the phone rule keeps is not cut out at the cap either.
	got := RedactMessage(strings.Repeat("x ", 140) + "y offset 555 123 4567 next")
	if !strings.Contains(got, "offset 555 123") {
		t.Errorf("offset at the cap lost: ...%q", got[len(got)-30:])
	}
}

// TestRedactCorpus is the redaction corpus (docs/agents/org.md §12): every
// text a review quoted is a row. mask: the secret part is gone. keep: the text
// comes back unchanged (main keeps it too). gap: a value main leaks as well,
// pinned at today's output until its follow-up ticket fixes it.
func TestRedactCorpus(t *testing.T) {
	tests := []struct {
		kind, in string
		gone     string // mask: the part that must be gone
		want     string // gap: today's output
	}{
		// #426: international phones and a bare bearer token are masked.
		{kind: "mask", in: "phone +44 20 7946 0958 now", gone: "7946"},
		{kind: "mask", in: "phone +15551234567 now", gone: "5551234567"},
		{kind: "mask", in: "tel +49-30-1234-5678.", gone: "1234-5678"},
		{kind: "mask", in: "phone +91-98765-43210 now", gone: "98765"},
		{kind: "mask", in: "tel +91 98765 43210 now", gone: "98765"},
		{kind: "mask", in: "customer at offset +44 20 7946 0958", gone: "7946"},
		{kind: "mask", in: "phone.offset: 555-123-4567", gone: "555-123"},
		{kind: "mask", in: "call 555 123 4567 now", gone: "555 123"},
		{kind: "mask", in: "call +1 2 3 4 5 555-123-4567 now", gone: "123-4567"},
		{kind: "mask", in: "auth header bearer abcdef123456 sent", gone: "abcdef123456"},
		{kind: "mask", in: "got Bearer eyJhbGciOi.x.y here", gone: "eyJhbGciOi"},
		{kind: "mask", in: "Authorization: Bearer x", gone: " x"},
		{kind: "mask", in: "authorization=Basic dXNlcjpwYXNz retry", gone: "dXNlcjpwYXNz"},
		// Cards main masks: 2-series and Mir, and every Luhn-valid grouping.
		{kind: "mask", in: "card 2223 0031 2200 3222 declined", gone: "2223"},
		{kind: "mask", in: "card 2223-0031-2200-3222 declined", gone: "2223"},
		{kind: "mask", in: "card 2221 0000 0000 0009 rejected", gone: "2221"},
		{kind: "mask", in: "card 2720 9900 0000 0007 declined", gone: "2720"},
		{kind: "mask", in: "card 2200 0000 0000 0004 rejected", gone: "2200"},
		{kind: "mask", in: "card 4111 11111111 1111 declined", gone: "4111"},
		{kind: "mask", in: "card 41111111 11111111 declined", gone: "41111111"},
		{kind: "mask", in: "card 41 11 11 11 11 11 11 11 declined", gone: "41 11"},
		{kind: "mask", in: "card 41 1111 1111 1111 11 rejected", gone: "1111"},
		{kind: "mask", in: "card 3782 8224 6310 005 rejected", gone: "3782"},
		{kind: "mask", in: "card 3782-8224-6310-005 rejected", gone: "3782"},
		{kind: "mask", in: "card 3056 9309 0259 04 rejected", gone: "3056"},
		{kind: "mask", in: "card 4222 2222 2222 2 rejected", gone: "4222"},
		{kind: "mask", in: "card 3782 822463 10005 ok", gone: "822463"},
		{kind: "mask", in: "ids 1234 5678 9012 3452", gone: "1234"},
		// #426: Kafka positions and offset ranges are kept.
		{kind: "keep", in: "skipped offsets 1234520-1234619 on orders-0"},
		{kind: "keep", in: "offsets 123456789-123456999 skipped"},
		{kind: "keep", in: "Topic orders-dlq partition 0 offset 555 123 4567"},
		{kind: "keep", in: "partitions: 555-123-4567"},
		// Prose around "bearer" is kept.
		{kind: "keep", in: "bearer token expired"},
		{kind: "keep", in: "the Bearer scheme is required by the sink"},
		{kind: "keep", in: "HTTP 401 invalid bearer token for sink"},
		{kind: "keep", in: "missing bearer header"},
		{kind: "keep", in: "WWW-Authenticate: Bearer realm=api, error=invalid_token"},
		{kind: "keep", in: "Bearer realm=example rejected"},
		// Counts, builds and zone offsets are not phones.
		{kind: "keep", in: "consumer lag grew +12500000 messages"},
		{kind: "keep", in: "consumer lag grew by +12500000 in 5m"},
		{kind: "keep", in: "lag delta +123456789 records"},
		{kind: "keep", in: "lag delta +1234567890 since 18:00"},
		{kind: "keep", in: "build 2.7.0+20261015.1234 deployed"},
		{kind: "keep", in: "Europe/Paris UTC+1 2026-10-15 18:00:00"},
		{kind: "keep", in: "retry +1 2026-10-15 scheduled"},
		{kind: "keep", in: "retry +1 2 3 times"},
		{kind: "keep", in: "lag +12 at 10:00"},
		// Known gaps: main leaks these too (follow-up ticket in the PR).
		{kind: "gap", in: "tel 0049-30-123456 now", want: "tel 0049-30-123456 now"},
		{kind: "gap", in: "phone +44 (0)20 7946 0958 now", want: "phone +44 (0)20 7946 0958 now"},
		{kind: "gap", in: "phone +49 30 1234567 now", want: "phone +49 30 1234567 now"},
		{kind: "gap", in: "phone +49 30 12345678 now", want: "phone +49 30 12345678 now"},
		{kind: "gap", in: "phone 020 7946 0958 now", want: "phone 020 7946 0958 now"},
		{kind: "gap", in: "phone: 5551234567 now", want: "phone: 5551234567 now"},
		{kind: "gap", in: "header bearer: abc123def456 rejected", want: "header bearer: abc123def456 rejected"},
		{kind: "gap", in: "ssn 123 45 6789 invalid", want: "ssn 123 45 6789 invalid"},
		{kind: "gap", in: "card 2223003122003222 rejected", want: "card 2223003122003222 rejected"},
	}
	for _, tt := range tests {
		t.Run(tt.kind+" "+tt.in, func(t *testing.T) {
			got := RedactMessage(tt.in)
			switch tt.kind {
			case "mask":
				if strings.Contains(got, tt.gone) || !strings.Contains(got, Redacted) {
					t.Errorf("RedactMessage(%q) = %q, %q must be masked", tt.in, got, tt.gone)
				}
			case "keep":
				if got != tt.in {
					t.Errorf("RedactMessage(%q) = %q, want it unchanged", tt.in, got)
				}
			case "gap":
				if got != tt.want {
					t.Errorf("known gap changed: RedactMessage(%q) = %q, want %q (fixed? flip the row to mask)", tt.in, got, tt.want)
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
	// A Postgres cast is not a key and value.
	for _, keep := range []string{
		"ALTER TABLE public.events ALTER COLUMN token TYPE varchar(64) USING token::varchar(64)",
		"ALTER TABLE public.t ALTER COLUMN access_token TYPE text USING access_token::text",
	} {
		if got := MaskPII(keep); got != keep {
			t.Errorf("MaskPII(%q) = %q, want it unchanged", keep, got)
		}
	}
}
