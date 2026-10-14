package dlq

import (
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestRedactMessage(t *testing.T) {
	const jsonSnippet = `{"email":"a@b.com","card":"4111111111111111"}`
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"message with nothing to hide is unchanged",
			"Failed to deserialize data for topic pg.public.orders to Avro: Unknown magic byte!",
			"Failed to deserialize data for topic pg.public.orders to Avro: Unknown magic byte!"},
		{"Jackson unexpected character (design doc example)",
			"Unexpected character ('x' (code 120)): was expecting double-quote to start field name\n at [Source: (byte[])\"{x\":\"a@b.com\"}\"; line: 1, column: 2]",
			"Unexpected character ('x' (code 120)): was expecting double-quote to start field name at <redacted>"},
		{"Jackson unrecognized token with real text",
			"com.fasterxml.jackson.core.JsonParseException: Unrecognized token 'hello': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')\n at [Source: (byte[])\"hello world\"; line: 1, column: 5]",
			// The operand after "Unrecognized token" is record data even when it
			// looks like an identifier. 'null', 'true', 'false' are keywords and stay.
			"com.fasterxml.jackson.core.JsonParseException: Unrecognized token <redacted>: was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false') at <redacted>"},
		{"Jackson token with an email", "Unrecognized token 'jane@example.com': was expecting JSON", "Unrecognized token <redacted>: was expecting JSON"},
		{"Jackson token bare word", "Unrecognized token abc: was expecting JSON", "Unrecognized token <redacted>: was expecting JSON"},
		{"Jackson token unterminated", "Unrecognized token 'jane", "Unrecognized token <redacted>"},
		{"Jackson token at the end", "Unrecognized token", "Unrecognized token"},
		{"MySQL duplicate entry", "Duplicate entry 'jane@example.com' for key 'users.email'", "Duplicate entry <redacted> for key 'users.email'"},
		{"MySQL incorrect integer", "Incorrect integer value: 'abc 123' for column 'amount' at row 1", "Incorrect integer value: <redacted> for column 'amount' at row 1"},
		{"JDBC batch VALUES", "Batch entry 0 INSERT INTO orders (id, email) VALUES (5,'jane@example.com') was aborted: ERROR: x", "Batch entry 0 INSERT INTO orders (id, email) VALUES <redacted> was aborted: ERROR: x"},
		{"VALUES lower case, several tuples", "insert into t(a,b) values (1, 'x'), (2,'it''s ) ok') returning", "insert into t(a,b) values <redacted> returning"},
		{"VALUES with a close paren in a quoted value", "VALUES (1,'a)b') end", "VALUES <redacted> end"},
		{"VALUES unbalanced", "VALUES (1,2", "VALUES <redacted>"},
		{"values in prose is kept", "the values must be positive", "the values must be positive"},
		{"identifier-shaped single quotes are kept", "Cannot cast field 'amount' of type 'VALUE_CONVERTER' to 'users.order-id' (null 'x')", "Cannot cast field 'amount' of type 'VALUE_CONVERTER' to 'users.order-id' (null 'x')"},
		{"number in single quotes", "bad id '5' here", "bad id <redacted> here"},
		{"empty single quotes", "bad id '' here", "bad id <redacted> here"},
		{"apostrophes in prose are not quotes", "can't convert, don't retry, it's the users' table", "can't convert, don't retry, it's the users' table"},
		{"prose apostrophe then a real span", "can't map 'jane doe' to string", "can't map <redacted> to string"},
		{"single quote span with an inner apostrophe", "bad 'it's mine' value", "bad <redacted> value"},
		{"unterminated single quote is an apostrophe", "bad 'jane", "bad 'jane"},
		{"Det NUL ail tail", "x Det\x00ail: Key (id)=(5)", "x <redacted>"},
		{"Jackson Source tail with a JSON record in it",
			"Unexpected end-of-input at [Source: (byte[])" + `"` + jsonSnippet + `"; line: 1, column: 46]`,
			"Unexpected end-of-input at <redacted>"},
		{"Jackson snippet with Source lower case",
			"bad json at [source: (String)\"{\"a\":1}\"; line: 1, column: 3]",
			"bad json at <redacted>"},
		{"PostgreSQL JDBC duplicate key (real text)",
			"org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint \"orders_pkey\"\n  Detail: Key (id)=(5) already exists.\n  Where: SQL statement \"INSERT INTO orders VALUES (5)\"",
			"org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint <redacted> <redacted>"},
		{"JDBC Detail with the key value",
			"ERROR: null value in column amount violates not-null constraint  Detail: Failing row contains (7, null, jane@example.com).",
			"ERROR: null value in column amount violates not-null constraint <redacted>"},
		{"Where tail alone",
			"PL/pgSQL function fn() failed. Where: PL/pgSQL function fn() line 3 at RAISE",
			"PL/pgSQL function fn() failed. <redacted>"},
		{"Detail at the very start", "Detail: Key (id)=(5) already exists.", "<redacted>"},
		{"the earliest tail wins over a later one",
			"x Where: y Detail: z [Source: q]", "x <redacted>"},
		{"a word that merely ends in where is not a tail",
			"Elsewhere: the broker said no", "Elsewhere: the broker said no"},
		{"nested braces",
			"cannot map {a={b={c=1}}, d=[1,2,[3]]} to struct",
			"cannot map <redacted> to struct"},
		{"brackets with braces inside",
			"bad batch [{id=1},{id=2}] rejected", "bad batch <redacted> rejected"},
		{"two groups", "from {a=1} to {b=2}", "from <redacted> to <redacted>"},
		{"quoted string", `field "email" has value "a@b.com"`, "field <redacted> has value <redacted>"},
		{"quoted string with an escaped quote",
			`bad value "he said \"hi\" and left" in row`, "bad value <redacted> in row"},
		{"quote ending in an escaped backslash",
			`bad "path\\" then "next"`, "bad <redacted> then <redacted>"},
		{"close brace inside a quoted string inside a group",
			`got {"a":"}","b":"secret"} done`, "got <redacted> done"},
		{"unbalanced opener takes the rest", "got {a=1, b=secret and more", "got <redacted>"},
		{"unterminated quote takes the rest", `got "secret and more`, "got <redacted>"},
		{"stray closers are dropped", "a ] b } c", "a b c"},
		{"stray closers do not fuse words", "Det}ail: Key (id)=(5)", "Det ail: Key (id)=(5)"},
		{"control characters and runs of spaces", "a\x00b\tc\r\n  d​e", "ab c de"},
		{"bidi override is dropped", "abc‮def", "abcdef"},
		{"empty quotes and braces", `x "" {} [] y`, "x <redacted> <redacted> <redacted> y"},
		{"invalid UTF-8", "bad \xff\xfe byte", "bad �� byte"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactMessage(tc.in)
			if got != tc.want {
				t.Fatalf("RedactMessage(%q)\n got: %q\nwant: %q", tc.in, got, tc.want)
			}
			if !utf8.ValidString(got) {
				t.Errorf("result is not valid UTF-8: %q", got)
			}
			if again := RedactMessage(got); again != got {
				t.Errorf("not idempotent: %q -> %q", got, again)
			}
		})
	}
}

// The cap comes first: a [Source: tail that the cap cut in the middle is
// still dropped, and nothing past rune 300 survives.
func TestRedactMessageCapThenRedact(t *testing.T) {
	prefix := strings.Repeat("e", 200)

	// The marker is cut in half by the cap: "[Sourc" is no tail, so the filler
	// stays, and the result is still within the cap.
	cut := prefix + " [Sourc" + strings.Repeat("x", 100)
	got := RedactMessage(cut)
	if n := len([]rune(got)); n > MaxMessageRunes {
		t.Fatalf("result has %d runes, want <= %d", n, MaxMessageRunes)
	}

	// The cap lands in the middle of the tail: the tail start is inside the
	// cap, its end is not. The record text after the cap never appears.
	const secret = "TOPSECRET-VALUE"
	in := prefix + " at [Source: (byte[])\"{\\\"k\\\":\\\"" + secret + "\\\"}\"; line: 1, column: 2]"
	got = RedactMessage(in)
	if strings.Contains(got, secret) || strings.Contains(got, "Source") {
		t.Fatalf("tail leaked: %q", got)
	}
	if !strings.HasSuffix(got, " at <redacted>") {
		t.Fatalf("got %q, want the tail replaced by <redacted>", got)
	}

	// A secret that sits only past rune 300 is never seen.
	far := strings.Repeat("a ", 150) + secret
	if got := RedactMessage(far); strings.Contains(got, secret) {
		t.Fatalf("text past the cap survived: %q", got)
	}
}

// Redaction can lengthen the text ("" -> "<redacted>"); the result still obeys the cap.
func TestRedactMessageStaysWithinCap(t *testing.T) {
	for _, in := range []string{
		strings.Repeat(`"" `, 200),
		strings.Repeat(`{} `, 200),
		strings.Repeat(`""`, 500),
		strings.Repeat("a", 1000),
	} {
		got := RedactMessage(in)
		if n := utf8.RuneCountInString(got); n > MaxMessageRunes {
			t.Errorf("%d runes in, %d out, want <= %d", utf8.RuneCountInString(in), n, MaxMessageRunes)
		}
		if again := RedactMessage(got); again != got {
			t.Errorf("not idempotent for input of %d runes", utf8.RuneCountInString(in))
		}
	}
}

// A junk value placed where converters and drivers put record bytes never
// survives, in plain, base64 or hex form (the leak test of the design doc,
// section 6.2, for the redaction half).
func TestRedactMessageJunkValueNeverSurvives(t *testing.T) {
	const junk = "JUNK-VALUE-9c1d4e7a"
	forms := []string{junk, base64.StdEncoding.EncodeToString([]byte(junk)), hex.EncodeToString([]byte(junk))}
	templates := []string{
		`Unexpected character ('x' (code 120)): was expecting double-quote to start field name` + "\n at [Source: (byte[])\"{x:%s}\"; line: 1, column: 2]",
		`Unrecognized token: was expecting JSON` + "\n at [Source: (byte[])\"%s\"; line: 1, column: 5]",
		"ERROR: duplicate key value violates unique constraint \"orders_pkey\"\n  Detail: Key (id)=(%s) already exists.\n  Where: SQL statement \"INSERT\"",
		`cannot convert {name=%s, tags=[a,%s]} to struct`,
		`bad value "%s" for field`,
		`Failing row contains [%s, null]`,
		`row {"id":1,"v":"%s"}`,
		`oops at [Source: (String)"%s"`,
		`unterminated "%s`,
	}
	for _, tpl := range templates {
		for _, form := range forms {
			in := strings.ReplaceAll(tpl, "%s", form)
			got := RedactMessage(in)
			for _, f := range forms {
				if strings.Contains(got, f) {
					t.Errorf("template %q: %q survived in %q", tpl, f, got)
				}
			}
		}
	}
}

// Real Kafka Connect / driver texts, as strings the redaction must handle
// without a panic and without keeping the values they quote.
func TestRedactMessageRealTexts(t *testing.T) {
	tests := []struct {
		name, in string
		hidden   []string // must not appear
		kept     []string // must appear (the message stays useful)
	}{
		{
			"JsonConverter wraps Jackson",
			"org.apache.kafka.connect.errors.DataException: Converting byte[] to Kafka Connect data failed due to serialization error: \n" +
				"Caused by: org.apache.kafka.common.errors.SerializationException: java.io.CharConversionException: Invalid UTF-32 character 0x7b226964 (above 0x0010ffff) at char #1, byte #7)\n" +
				" at [Source: (byte[])\"{\"id\":5,\"email\":\"jane@example.com\"}\"; line: 1, column: 2]",
			[]string{"jane@example.com", "\"id\":5"},
			[]string{"DataException", "serialization error", "Invalid UTF-32 character"},
		},
		{
			"Jackson token error",
			"com.fasterxml.jackson.core.JsonParseException: Unrecognized token 'abc': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')\n at [Source: (byte[])\"abc def\"; line: 1, column: 5]",
			[]string{"abc def"},
			[]string{"JsonParseException", "Unrecognized token"},
		},
		{
			"PostgreSQL JDBC",
			"java.sql.SQLException: Exception chain:\norg.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint \"orders_pkey\"\n  Detail: Key (id)=(5) already exists.\n  Where: SQL statement \"INSERT INTO t VALUES (5)\"",
			[]string{"orders_pkey", "(id)=(5)", "INSERT"},
			[]string{"PSQLException", "duplicate key value violates unique constraint"},
		},
		{
			"JDBC sink value too long",
			"ERROR: value too long for type character varying(10)\n  Where: COPY orders, line 1, column name: \"jane doe 1234567890\"",
			[]string{"jane doe"},
			[]string{"value too long for type character varying(10)"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactMessage(tc.in)
			for _, h := range tc.hidden {
				if strings.Contains(got, h) {
					t.Errorf("%q survived in %q", h, got)
				}
			}
			for _, k := range tc.kept {
				if !strings.Contains(got, k) {
					t.Errorf("%q was lost from %q", k, got)
				}
			}
		})
	}
}

// Known residual: a bare identifier-shaped value in single quotes looks like a
// column name and is kept, and typographic/fullwidth quotes are not
// recognised. This test pins both so a change is a conscious one.
func TestRedactMessageKnownResidual(t *testing.T) {
	if got := RedactMessage("Duplicate entry 'jane' for key 'users.email'"); got != "Duplicate entry 'jane' for key 'users.email'" {
		t.Fatalf("identifier-shaped value changed: %q", got)
	}
	// The e-mail rule masks an address whatever quotes it; other text in
	// typographic quotes stays.
	if got := RedactMessage("bad \u201cjane doe\u201d here"); !strings.Contains(got, "jane doe") {
		t.Fatalf("typographic quotes are now handled: %q (update the PR note)", got)
	}
}

// Cap first: a group opened before rune 300 and closed after it. Without the
// first cap the closing brace is found, the group is removed and SECRETPLAIN
// (outside any quote or group) survives.
func TestRedactMessageCapIsAppliedFirst(t *testing.T) {
	in := strings.Repeat("a", 100) + "{" + strings.Repeat("x", 300) + "} SECRETPLAIN"
	got := RedactMessage(in)
	if strings.Contains(got, "SECRETPLAIN") {
		t.Fatalf("text past the 300-rune cap survived: %q", got)
	}
	if want := strings.Repeat("a", 100) + Redacted; got != want {
		t.Fatalf("got %q, want the group redacted to the end", got)
	}
}

// The cap never leaves half a marker.
func TestRedactMessageNoPartialMarker(t *testing.T) {
	for n := 270; n < 300; n++ {
		got := RedactMessage(strings.Repeat("a", n) + ` "x" "y" "z"`)
		if i := strings.LastIndex(got, "<"); i >= 0 && !strings.HasPrefix(got[i:], Redacted) {
			t.Errorf("n=%d: partial marker at the end: %q", n, got[i:])
		}
	}
}

func FuzzRedactMessage(f *testing.F) {
	seeds := []string{
		"",
		"plain message",
		"Unexpected character ('x' (code 120)) at [Source: (byte[])\"{x\"; line: 1, column: 2]",
		"ERROR: duplicate key\n  Detail: Key (id)=(5) already exists.\n  Where: SQL statement \"X\"",
		`{"a":"}`, `"\"`, `"\`, "[{[{[", "}}]]", `"" "" ""`, "Det}ail:", "[Sou}rce:", `Det"x"ail:`,
		"a\x00b", "\xff\xfe", "‮​", strings.Repeat(`""`, 400), strings.Repeat("[", 400),
		"Where:Where:", "wHeRe: x", "deTAIL:", "[SOURCE:",
		"Unrecognized token 'a': x", "VALUES (1,'a'), (2", "Det\x00ail:", "'a b' 'c d'", "''''", "x 'it's' y",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		got := RedactMessage(s)
		if n := utf8.RuneCountInString(got); n > MaxMessageRunes {
			t.Fatalf("%d runes, want <= %d: %q", n, MaxMessageRunes, got)
		}
		if !utf8.ValidString(got) {
			t.Fatalf("invalid UTF-8: %q", got)
		}
		if again := RedactMessage(got); again != got {
			t.Fatalf("not idempotent:\n first: %q\nsecond: %q", got, again)
		}
		if strings.ContainsAny(got, "\"{}[]") {
			t.Fatalf("a quote, brace or bracket survived: %q", got)
		}
		if r := openSpan(got); r != "" {
			t.Fatalf("single-quoted span with a non-identifier body survived (%q): %q", r, got)
		}
		if strings.ContainsAny(got, "\x00\r\n\t") {
			t.Fatalf("control character survived: %q", got)
		}
	})
}

// openSpan finds a '...' span (opener at a word start, closer not followed by
// a letter or digit) whose body is not identifier-shaped, and returns it.
func openSpan(s string) string {
	rs := []rune(s)
	for i := 0; i < len(rs); i++ {
		if rs[i] != '\'' || !wordStart(rs, i) {
			continue
		}
		end := apostropheEnd(rs, i)
		if end < 0 {
			continue
		}
		if !isIdentifier(rs[i+1 : end]) {
			return string(rs[i : end+1])
		}
		i = end // a kept span: its closer is not the next opener
	}
	return ""
}
