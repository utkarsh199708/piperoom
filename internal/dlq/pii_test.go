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
		{"card 41111111-11111111", "41111111"},
		{"token=:xyz789abc", "xyz789"},
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

// A URL that straddles the cap is cut out whole, whatever its password
// holds ('!', '/', '?', '#', ':'). Cutting at an e-mail-like "<tail>@host"
// inside it kept "postgres://app:S3cr!" or "postgres://app:S3cr/" (rounds 3
// and 4 of #435). The sweep checks that no part of the password survives;
// it starts at pad 266 because from there every URL below reaches the cap,
// and a URL that ends before the cap is the uncut gap pinned in
// TestRedactCorpus (#469).
func TestRedactMessageCapNeverSplitsAURLPassword(t *testing.T) {
	for _, v := range []struct{ text, leak string }{
		{"postgres://app:S3cr!t@db.example.com/x", "S3cr!"},
		{"mysql://root:pa$$w0rd@mysql.example.net:3306/shop", "pa$$"},
		{"postgres://app:S3cr/t9x@db.example.com/orders", "S3cr/"},
		{"mysql://root:pa/ss9@mysql.example.net:3306/shop", "pa/"},
		{"redis://:Abc/def123@cache.example.com:6379", "Abc/"},
		{"postgres://app:Qq?w9x7@db.example.com/orders", "Qq?"},
		{"postgres://app:Hh#k2m8@db.example.com/orders", "Hh#"},
		{"postgres://app:Zz:p4r6@db.example.com/orders", "Zz:"},
		// A scheme glued to '_' or to a long word (round 5 of #435).
		{"my_postgres://app:Zq9/Wk7x@db.example.com/x", "Zq9/"},
		{"x_mysql+srv://app:Zq9/Wk7x@db.example.com/x", "Zq9/"},
		{"jdbcpostgresql://app:Zq9/Wk7x@db.example.com/x", "Zq9/"},
		{"postgres_ro://app:Zq9/Wk7x@db.example.com/x", "Zq9/"},
		{strings.Repeat("w", 40) + "postgres://app:Zq9/Wk7x@db.example.com/x", "Zq9/"},
	} {
		for pad := 266; pad <= 300; pad++ {
			in := strings.Repeat("x", pad-1) + " " + v.text + " and the rest of the message"
			if got := RedactMessage(in); strings.Contains(got, v.leak) {
				t.Errorf("pad %d: RedactMessage kept %q: ...%q", pad, v.leak, got[max(0, len(got)-40):])
			}
		}
	}
}

// The same URL glued to the padding, with no space before it.
func TestRedactMessageCapNeverSplitsAGluedURL(t *testing.T) {
	for pad := 266; pad <= 300; pad++ {
		in := strings.Repeat("x", pad) + "postgres://app:Zq9/Wk7x@db.example.com/x and the rest of it"
		if got := RedactMessage(in); strings.Contains(got, "Zq9/") {
			t.Errorf("pad %d: RedactMessage kept %q: ...%q", pad, "Zq9/", got[max(0, len(got)-40):])
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
}

// TestRedactCorpus is the redaction corpus (docs/agents/org.md §12): every
// text a review quoted is a row. mask: the secret part is gone. keep: the text
// comes back unchanged. gap: main gives the same output; the row pins it, with
// the ticket that will change it, until that ticket flips it.
func TestRedactCorpus(t *testing.T) {
	tests := []struct {
		kind, in string
		gone     string // mask: the part that must be gone
		want     string // mask, gap: the exact output (main's, for mask rows)
		why      string // gap: the ticket
	}{
		{kind: "mask", in: "call 555 123 4567 now", gone: "555 123", want: "call <redacted> now"},
		{kind: "mask", in: "phone.offset: 555-123-4567", gone: "555-123", want: "phone.offset: <redacted>"},
		{kind: "mask", in: "call +1 2 3 4 5 555-123-4567 now", gone: "123-4567", want: "call +1 2 3 4 5 <redacted> now"},
		{kind: "mask", in: "authorization=Basic dXNlcjpwYXNz retry", gone: "dXNlcjpwYXNz", want: "authorization=Basic <redacted> retry"},
		// Cards main masks: 2-series and Mir, and every Luhn-valid grouping,
		// dash-joined halves included.
		{kind: "mask", in: "card 2223 0031 2200 3222 declined", gone: "2223", want: "card <redacted> declined"},
		{kind: "mask", in: "card 2223-0031-2200-3222 declined", gone: "2223", want: "card <redacted> declined"},
		{kind: "mask", in: "card 2221 0000 0000 0009 rejected", gone: "2221", want: "card <redacted> rejected"},
		{kind: "mask", in: "card 2720 9900 0000 0007 declined", gone: "2720", want: "card <redacted> declined"},
		{kind: "mask", in: "card 2200 0000 0000 0004 rejected", gone: "2200", want: "card <redacted> rejected"},
		{kind: "mask", in: "card 4111 11111111 1111 declined", gone: "4111", want: "card <redacted> declined"},
		{kind: "mask", in: "card 41111111 11111111 declined", gone: "41111111", want: "card <redacted> declined"},
		{kind: "mask", in: "card 41 11 11 11 11 11 11 11 declined", gone: "41 11", want: "card <redacted> declined"},
		{kind: "mask", in: "card 41 1111 1111 1111 11 rejected", gone: "1111", want: "card <redacted> rejected"},
		{kind: "mask", in: "card 3782 8224 6310 005 rejected", gone: "3782", want: "card <redacted> rejected"},
		{kind: "mask", in: "card 3782-8224-6310-005 rejected", gone: "3782", want: "card <redacted> rejected"},
		{kind: "mask", in: "card 3056 9309 0259 04 rejected", gone: "3056", want: "card <redacted> rejected"},
		{kind: "mask", in: "card 4222 2222 2222 2 rejected", gone: "4222", want: "card <redacted> rejected"},
		{kind: "mask", in: "card 3782 822463 10005 ok", gone: "822463", want: "card <redacted> ok"},
		{kind: "mask", in: "ids 1234 5678 9012 3452", gone: "1234", want: "ids <redacted>"},
		{kind: "mask", in: "card 41111111-11111111 declined", gone: "41111111-11111111", want: "card <redacted> declined"},
		{kind: "mask", in: "pan=41111111-11111111", gone: "41111111-11111111", want: "pan=<redacted>"},
		{kind: "mask", in: "card 411111-1111111111 declined", gone: "411111-1111111111", want: "card <redacted> declined"},
		{kind: "mask", in: "card 4111111111-111111 declined", gone: "4111111111-111111", want: "card <redacted> declined"},
		{kind: "mask", in: "card 4111111-111111111 declined", gone: "4111111-111111111", want: "card <redacted> declined"},
		{kind: "mask", in: "card 378282-246310005 declined", gone: "378282-246310005", want: "card <redacted> declined"},
		{kind: "mask", in: "pan 378282-246310005 rejected", gone: "378282-246310005", want: "pan <redacted> rejected"},
		{kind: "mask", in: "card 5555555555-554444 declined", gone: "5555555555-554444", want: "card <redacted> declined"},
		{kind: "mask", in: "card 555555-5555554444 declined", gone: "555555-5555554444", want: "card <redacted> declined"},
		{kind: "mask", in: "card 55555555-55554444 declined", gone: "55555555-55554444", want: "card <redacted> declined"},
		{kind: "mask", in: "card 222300-3122003222 declined", gone: "222300-3122003222", want: "card <redacted> declined"},
		{kind: "mask", in: "card 22230031-22003222 x", gone: "22230031-22003222", want: "card <redacted> x"},
		{kind: "mask", in: "card 6011000990-139424 declined", gone: "6011000990-139424", want: "card <redacted> declined"},
		{kind: "mask", in: "card 601111-1111111117 declined", gone: "601111-1111111117", want: "card <redacted> declined"},
		{kind: "mask", in: "pan 4222222-222222 rejected", gone: "4222222-222222", want: "pan <redacted> rejected"},
		// A credential value that starts with ':' is still a value.
		{kind: "mask", in: "token=:xyz789", gone: "xyz789", want: "token=<redacted>"},
		{kind: "mask", in: "token: :xyz789 rejected", gone: "xyz789", want: "token: <redacted> rejected"},
		{kind: "mask", in: "token = :xyz789 rejected", gone: "xyz789", want: "token = <redacted> rejected"},
		{kind: "mask", in: "auth_token=:abc123 rejected", gone: "abc123", want: "auth_token=<redacted> rejected"},
		{kind: "mask", in: "access_token = :Zx9 failed", gone: "Zx9", want: "access_token = <redacted> failed"},
		{kind: "mask", in: "access_token=:s3cr3t", gone: "s3cr3t", want: "access_token=<redacted>"},
		{kind: "mask", in: "api_token: :s3cr3t99", gone: "s3cr3t99", want: "api_token: <redacted>"},
		{kind: "mask", in: "session_token=:a1b2c3 failed", gone: "a1b2c3", want: "session_token=<redacted> failed"},
		{kind: "mask", in: "refresh_token=:abc failed", gone: "abc", want: "refresh_token=<redacted> failed"},
		{kind: "mask", in: "x-api-token=:k3y", gone: "k3y", want: "x-api-token=<redacted>"},
		{kind: "mask", in: "token=::abc123def", gone: "abc123def", want: "token=<redacted>"},
		{kind: "mask", in: "token: ::eyJabc", gone: "eyJabc", want: "token: <redacted>"},
		{kind: "mask", in: "token: Bearer :abc123def", gone: "abc123def", want: "token: <redacted>"},
		{kind: "mask", in: "token=Basic :dXNlcjpw", gone: "dXNlcjpw", want: "token=<redacted>"},
		// Phone formats after a position word.
		{kind: "mask", in: "partition (555) 123-4567", gone: "(555) 123-4567", want: "partition <redacted>"},
		{kind: "mask", in: "partition: (555) 123-4567 owner", gone: "(555) 123-4567", want: "partition: <redacted> owner"},
		{kind: "mask", in: "customer at offset (555) 123-4567", gone: "(555) 123-4567", want: "customer at offset <redacted>"},
		{kind: "mask", in: "offset (555) 123-4567", gone: "(555) 123-4567", want: "offset <redacted>"},
		{kind: "mask", in: "offsets: 555.123.4567", gone: "555.123.4567", want: "offsets: <redacted>"},
		{kind: "mask", in: "offsets 555.123.4567", gone: "555.123.4567", want: "offsets <redacted>"},
		// Prose and counts main keeps.
		{kind: "keep", in: "offsets 123456789-123456999 skipped"},
		{kind: "keep", in: "bearer token expired"},
		{kind: "keep", in: "the Bearer scheme is required by the sink"},
		{kind: "keep", in: "HTTP 401 invalid bearer token for sink"},
		{kind: "keep", in: "missing bearer header"},
		{kind: "keep", in: "WWW-Authenticate: Bearer realm=api, error=invalid_token"},
		{kind: "keep", in: "Bearer realm=example rejected"},
		{kind: "keep", in: "consumer lag grew +12500000 messages"},
		{kind: "keep", in: "consumer lag grew by +12500000 in 5m"},
		{kind: "keep", in: "lag delta +123456789 records"},
		{kind: "keep", in: "lag delta +1234567890 since 18:00"},
		{kind: "keep", in: "build 2.7.0+20261015.1234 deployed"},
		{kind: "keep", in: "Europe/Paris UTC+1 2026-10-15 18:00:00"},
		{kind: "keep", in: "retry +1 2026-10-15 scheduled"},
		{kind: "keep", in: "retry +1 2 3 times"},
		{kind: "keep", in: "lag +12 at 10:00"},
		// #426 is split out of #435 (org.md §12): main's output, pinned.
		{kind: "gap", in: "phone +44 20 7946 0958 now", want: "phone +44 20 7946 0958 now", why: "#426, split out"},
		{kind: "gap", in: "phone +15551234567 now", want: "phone +15551234567 now", why: "#426, split out"},
		{kind: "gap", in: "tel +49-30-1234-5678.", want: "tel +49-30-1234-5678.", why: "#426, split out"},
		{kind: "gap", in: "phone +91-98765-43210 now", want: "phone +91-98765-43210 now", why: "#426, split out"},
		{kind: "gap", in: "tel +91 98765 43210 now", want: "tel +91 98765 43210 now", why: "#426, split out"},
		{kind: "gap", in: "customer at offset +44 20 7946 0958", want: "customer at offset +44 20 7946 0958", why: "#426, split out"},
		{kind: "gap", in: "auth header bearer abcdef123456 sent", want: "auth header bearer abcdef123456 sent", why: "#426, split out"},
		{kind: "gap", in: "got Bearer eyJhbGciOi.x.y here", want: "got Bearer eyJhbGciOi.x.y here", why: "#426, split out"},
		{kind: "gap", in: "Authorization: Bearer x", want: "Authorization: Bearer x", why: "#426, split out"},
		{kind: "gap", in: "skipped offsets 1234520-1234619 on orders-0", want: "skipped offsets <redacted> on orders-0", why: "#426, split out (over-mask)"},
		{kind: "gap", in: "Topic orders-dlq partition 0 offset 555 123 4567", want: "Topic orders-dlq partition 0 offset <redacted>", why: "#426, split out (over-mask)"},
		{kind: "gap", in: "partitions: 555-123-4567", want: "partitions: <redacted>", why: "#426, split out (over-mask)"},
		// Known gaps main shares, tracked in #469.
		{kind: "gap", in: "tel 0049-30-123456 now", want: "tel 0049-30-123456 now", why: "#469"},
		{kind: "gap", in: "phone +44 (0)20 7946 0958 now", want: "phone +44 (0)20 7946 0958 now", why: "#469"},
		{kind: "gap", in: "phone +49 30 1234567 now", want: "phone +49 30 1234567 now", why: "#469"},
		{kind: "gap", in: "phone +49 30 12345678 now", want: "phone +49 30 12345678 now", why: "#469"},
		{kind: "gap", in: "phone 020 7946 0958 now", want: "phone 020 7946 0958 now", why: "#469"},
		{kind: "gap", in: "phone: 5551234567 now", want: "phone: 5551234567 now", why: "#469"},
		{kind: "gap", in: "header bearer: abc123def456 rejected", want: "header bearer: abc123def456 rejected", why: "#469"},
		{kind: "gap", in: "ssn 123 45 6789 invalid", want: "ssn 123 45 6789 invalid", why: "#469"},
		{kind: "gap", in: "card 2223003122003222 rejected", want: "card 2223003122003222 rejected", why: "#469"},
		{kind: "gap", in: "card 4111 1111 1111 1111 2 items", want: "card 4111 1111 1111 1111 2 items", why: "#469"},
		{kind: "gap", in: "phone+44 20 7946 0958", want: "phone+44 20 7946 0958", why: "#469"},
		{kind: "gap", in: "bearer=abc123def456 rejected", want: "bearer=abc123def456 rejected", why: "#469"},
		{kind: "gap", in: "connect to postgres://app:S3cr!t@db.example.com/x failed", want: "connect to postgres://app:S3cr!<redacted>/x failed", why: "#469"},
		{kind: "gap", in: "connect to postgres://app:S3cr/t9x@db.example.com/orders failed", want: "connect to postgres://app:S3cr/<redacted>/orders failed", why: "redact urlFind password with raw '/'; #469"},
	}
	for _, tt := range tests {
		t.Run(tt.kind+" "+tt.in, func(t *testing.T) {
			got := RedactMessage(tt.in)
			switch tt.kind {
			case "mask":
				if strings.Contains(tt.want, tt.gone) || got != tt.want {
					t.Errorf("RedactMessage(%q) = %q, want %q (%q masked)", tt.in, got, tt.want, tt.gone)
				}
			case "keep":
				if got != tt.in {
					t.Errorf("RedactMessage(%q) = %q, want it unchanged", tt.in, got)
				}
			case "gap":
				if got != tt.want {
					t.Errorf("gap (%s) changed: RedactMessage(%q) = %q, want %q (fixed? flip the row)", tt.why, tt.in, got, tt.want)
				}
			}
			if again := RedactMessage(got); again != got {
				t.Errorf("not idempotent: %q -> %q", got, again)
			}
		})
	}
}

// A message over the cap only because of white space keeps main's quote rule:
// the cut is decided on the text as it came, so an open single quote is a
// value that runs past the cut, not an apostrophe.
func TestRedactMessageCutCountsTheOriginalText(t *testing.T) {
	for in, want := range map[string]string{
		"Batch entry 0 INSERT INTO t (a) SELECT 'Jane Doe, 42 Elm St" + strings.Repeat("\"\\n        \"", 40) + "was aborted": "Batch entry 0 INSERT INTO t (a) SELECT " + Redacted,
		"insert failed for value 'jane doe 555" + strings.Repeat(" ", 300) + "x":                                              "insert failed for value " + Redacted,
	} {
		if got := RedactMessage(in); got != want {
			t.Errorf("RedactMessage(%.40q...) = %q, want %q", in, got, want)
		}
	}
}

// A long token that only internal/redact knows is cut where the cap falls:
// main does the same (#469).
func TestRedactMessageCapLongJWTGap(t *testing.T) {
	jwt := "eyJhbGciOiJIUzI1NiJ9." + strings.Repeat("eyJzdWIiOiJqYW5lQGV4YW1wbGUuY29tIn0", 6) + ".c2lnbmF0dXJl"
	in := strings.Repeat("x", 109) + " token " + jwt + " rejected"
	if got, want := RedactMessage(in), in[:MaxMessageRunes]; got != want {
		t.Errorf("gap (#469) changed: RedactMessage(...) = %q, want %q (fixed? flip the row)", got, want)
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
