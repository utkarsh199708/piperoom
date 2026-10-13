package room

import (
	"strings"
	"testing"
	"time"
)

func TestClassifyNumber(t *testing.T) {
	for _, tc := range []struct {
		tok  string
		want numberClass
	}{
		{"0", numberExact},
		{"-0", numberExact},
		{"3", numberExact},
		{"0.1", numberExact},
		{"2.50", numberExact},
		{"1e3", numberExact},
		{"-12.5E-2", numberExact},
		{"9007199254740992", numberExact},   // 2^53
		{"9007199254740993", numberInexact}, // 2^53 + 1
		{"12345678901234567890", numberInexact},
		{"0.12345678901234567890123", numberInexact}, // more than 17 significant digits
		{"1e-400", numberInexact},                    // underflows to 0
		{"1e-999999", numberInexact},                 // underflow, must not build 10^999999
		{"123e-99999999999", numberInexact},
		{"0e999999", numberExact}, // zero stays zero whatever the exponent
		{"0.000", numberExact},
		{"5e-324", numberExact},                 // the smallest denormal is a double
		{"1.7976931348623157e308", numberExact}, // largest double
		{"1e309", numberOutOfRange},
		{"1e400", numberOutOfRange},
		{"-1e400", numberOutOfRange},
		{"2E+999", numberOutOfRange},
		{"1e99999999999", numberOutOfRange},
		{"-", numberIgnore},
		{"1e", numberIgnore},
	} {
		if got := classifyNumber(tc.tok); got != tc.want {
			t.Errorf("classifyNumber(%q) = %d, want %d", tc.tok, got, tc.want)
		}
	}
}

func TestHashForm(t *testing.T) {
	for _, tc := range []struct {
		in, out string
		inexact int
	}{
		{`{"a":1,"b":[0.1,2]}`, `{"a":1,"b":[0.1,2]}`, 0},
		{`{"a":1e400}`, `{"a":"1e400"}`, 1},
		{`{"a":[-1e400,1e400,3]}`, `{"a":["-1e400","1e400",3]}`, 2},
		{`{"a":9007199254740993}`, `{"a":9007199254740993}`, 1}, // finite: left for JCS to round, but counted
		{`{"s":"1e400 is text here","t":1e400}`, `{"s":"1e400 is text here","t":"1e400"}`, 1},
		{`{"s":"quote \" then 1e400 inside"}`, `{"s":"quote \" then 1e400 inside"}`, 0},
		{`[]`, `[]`, 0},
	} {
		out, n := hashForm([]byte(tc.in))
		if string(out) != tc.out || n != tc.inexact {
			t.Errorf("hashForm(%s) = %s, %d; want %s, %d", tc.in, out, n, tc.out, tc.inexact)
		}
	}
}

func TestHasOutOfRangeNumberIgnoresStringsAndGarbage(t *testing.T) {
	for in, want := range map[string]bool{
		`{"v":1e400}`:                  true,
		`{"v":[1,{"w":-3e500}]}`:       true,
		`{"v":"1e400"}`:                false,
		`{"k1e400":1}`:                 false,
		`{"v":1e308,"w":1e-400}`:       false,
		`{"v":9007199254740993}`:       false,
		`not json 1e400`:               true, // the scanner is tolerant; callers validate JSON first
		`{"v":"unterminated 1e400`:     false,
		`{"v":"\\" , "w": 1e400}`:      true,
		`{"v":"esc \" 1e400 \" end"}`:  false,
		"":                             false,
		`-`:                            false,
		`{"v":"a\`:                     false,
		`{"v":1e400`:                   true,
		`{"v":0.00000000000000000001}`: false,
	} {
		if got := hasOutOfRangeNumber([]byte(in)); got != want {
			t.Errorf("hasOutOfRangeNumber(%q) = %v, want %v", in, got, want)
		}
	}
}

// The cost of classifying a number is bounded by its length: 1 MB of tokens
// like 1e-999999 (each used to build 10^999999 with big.Rat, about 27 ms) must
// finish at once in both the webhook guard and the hash form.
func TestNumberClassificationIsCheap(t *testing.T) {
	tok := "1e-999999,"
	body := []byte("[" + strings.Repeat(tok, 1<<20/len(tok)) + "0]")
	start := time.Now()
	if hasOutOfRangeNumber(body) {
		t.Fatal("underflow is not out of range")
	}
	if _, n := hashForm(body); n == 0 {
		t.Fatal("hashForm should count the underflowing numbers as inexact")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("1 MB of 1e-999999 took %v, want well under 1 s", d)
	}

	// One huge token (a million digits) is also cheap and is inexact.
	long := []byte(`{"n":1` + strings.Repeat("0", 1<<20) + `}`)
	start = time.Now()
	if _, n := hashForm(long); n != 1 {
		t.Fatalf("inexact = %d, want 1", n)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("one 1 MB token took %v", d)
	}
}
