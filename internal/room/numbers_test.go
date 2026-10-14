package room

import (
	"strings"
	"testing"
	"time"

	"piperoom/internal/jsonnum"
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
		// Exponents at the int64 edge: the sign of the clamp must survive.
		{"10e9223372036854775807", numberOutOfRange},
		{"0.1e-9223372036854775808", numberInexact}, // underflows: inexact, not out of range
		{"10e92233720368547758070", numberOutOfRange},
		{"0.1e-92233720368547758080", numberInexact},
		{"0e9223372036854775807", numberExact},
		{"0.0e-9223372036854775808", numberExact},
		// Long mantissas: padding is not significant, digits are.
		{"1" + strings.Repeat("0", 900000) + "e-899990", numberExact},                  // value 1e10
		{strings.Repeat("7", 900000) + "e-899990", numberInexact},                      // about 7.7e9, 900000 significant digits
		{"1797693134862315808" + strings.Repeat("1", 800) + "e-510", numberOutOfRange}, // above max double
		{"1797693134862315807" + strings.Repeat("1", 800) + "e-510", numberInexact},    // below max double
	} {
		if got := classifyNumber(tc.tok); got != tc.want {
			t.Errorf("classifyNumber(%q) = %d, want %d", tc.tok, got, tc.want)
		}
	}
}

func TestSignificantForm(t *testing.T) {
	for _, tc := range []struct {
		tok    string
		digits string
		exp10  int
		ok     bool
	}{
		{"1", "1", 0, true},
		{"-1", "1", 0, true},
		{"1000", "1", 3, true},
		{"1e3", "1", 3, true},
		{"0.001", "1", -3, true},
		{"1.50", "15", -1, true},
		{"0.0120e+2", "12", -1, true},
		{"12.5E-2", "125", -3, true},
		{"0", "0", 0, true},
		{"0.000e5", "0", 0, true},
		{"1." + strings.Repeat("0", 5000) + "e-320", "1", -320, true}, // trailing fraction zeros are not significant
		{"-", "", 0, false},
		{"1e", "", 0, false},
		{"1e5x", "", 0, false},
		{"e5", "", 0, false},
		{"1e99999999999999999999", "1", 1 << 40, true}, // clamped, sign kept
		{"1e-99999999999999999999", "1", -(1 << 40), true},
	} {
		d, e, ok := jsonnum.Significant(tc.tok)
		if ok != tc.ok || (ok && (d != tc.digits || e != tc.exp10)) {
			t.Errorf("jsonnum.Significant(%.40q) = %q, %d, %v; want %q, %d, %v", tc.tok, d, e, ok, tc.digits, tc.exp10, tc.ok)
		}
	}
}

// The same value must be classified the same whatever its spelling, because
// jsonb keeps the decimal scale and so changes the length of the text:
// "1.<700 zeros>e-320" is 707 characters in and 1022 after the round trip.
func TestClassifyNumberIsStableAcrossSpellings(t *testing.T) {
	zeros := func(n int) string { return strings.Repeat("0", n) }
	for _, group := range [][]string{
		// 1e-320 is a denormal that JCS writes back as 1e-320: exact, in every
		// spelling.
		{"1e-320", "1." + zeros(700) + "e-320", "0." + zeros(319) + "1", "0." + zeros(319) + "1" + zeros(700)},
		// 2^-20 is exact. A long spelling used to be classed inexact only
		// because it was over the length cap.
		{"9.5367431640625e-7", "9.5367431640625" + zeros(1200) + "e-7", "0.00000095367431640625", "0.00000095367431640625" + zeros(1200)},
		// 2^60 as an integer, a long fraction of zeros, and an exponent form.
		{"1152921504606846976", "1152921504606846976." + zeros(1200), "1.152921504606846976e18", "115292150460684697.6e1"},
		{"-0.5", "-0.5" + zeros(1500), "-5e-1", "-0.0000005e6"},
		// More than 17 digits is inexact whatever the spelling.
		{"0.1234567890123456789", "1234567890123456789e-19", "0.1234567890123456789" + zeros(1500)},
	} {
		want := classifyNumber(group[0])
		for _, tok := range group[1:] {
			if got := classifyNumber(tok); got != want {
				t.Errorf("classifyNumber(%.50q) = %d, but %q is %d", tok, got, group[0], want)
			}
		}
	}
	if got := classifyNumber("9.5367431640625" + zeros(1200) + "e-7"); got != numberExact {
		t.Errorf("long spelling of 2^-20 = %d, want exact", got)
	}
	// Exact means JCS writes the same decimal back, so 1e-320 (written as
	// 1e-320) is exact, in the 707-character spelling and in the 1022-character
	// one jsonb returns.
	if got := classifyNumber("1." + zeros(700) + "e-320"); got != numberExact {
		t.Errorf("1.<700 zeros>e-320 = %d, want exact", got)
	}
	if got := classifyNumber("0." + zeros(319) + "1" + zeros(700)); got != numberExact {
		t.Errorf("the jsonb spelling of 1.<700 zeros>e-320 = %d, want exact", got)
	}
	// Over maxExactSigDigits significant digits: inexact, never compared.
	if got := classifyNumber("1" + strings.Repeat("3", maxExactSigDigits) + "e-" + "1000"); got != numberInexact {
		t.Errorf("801 significant digits = %d, want inexact", got)
	}
}

// strconv.ParseFloat stops adding exponent digits at about 10000, so a token
// padded with zeros used to parse as a small number while its value is huge
// (or tiny), and big.Rat then built 10^999899 (about 32 ms a token). The
// decision is now made on the significant form.
func TestClassifyNumberPaddedTokensAreCheapAndRight(t *testing.T) {
	zeros := func(n int) string { return strings.Repeat("0", n) }
	hugePos := "0." + zeros(10098) + "1e1009998" // value 1e999899, beyond every double
	hugeNeg := "1" + zeros(10099) + "e-1009998"  // value 1e-999899, rounds to zero
	for _, tc := range []struct {
		name string
		tok  string
		want numberClass
	}{
		{"huge positive value", hugePos, numberOutOfRange},
		{"negative of the huge positive value", "-" + hugePos, numberOutOfRange},
		{"tiny value (underflow)", hugeNeg, numberInexact},
		{"value 1 with a 99999-zero fraction and e100000", "0." + zeros(99999) + "1e100000", numberExact},
		{"value 0.1 with 99999 integer zeros and e-100000", "1" + zeros(99999) + "e-100000", numberExact},
		{"value 1 in the plain spelling", "1", numberExact},
		{"value 0.1 in the plain spelling", "0.1", numberExact},
	} {
		start := time.Now()
		got := classifyNumber(tc.tok)
		if d := time.Since(start); d > 20*time.Millisecond {
			t.Errorf("%s: took %v, want a few ms at most", tc.name, d)
		}
		if got != tc.want {
			t.Errorf("%s: classifyNumber = %d, want %d", tc.name, got, tc.want)
		}
	}
	// Neither padded 'one' and 'tenth' is out of range for the guard.
	for _, body := range []string{`{"n":0.` + zeros(99999) + `1e100000}`, `{"n":1` + zeros(99999) + `e-100000}`} {
		if hasOutOfRangeNumber([]byte(body)) {
			t.Errorf("hasOutOfRangeNumber(%.30q...) = true, want false", body)
		}
	}
	// Both spellings of one value agree, which is what survives the jsonb round trip.
	if classifyNumber("0."+zeros(99999)+"1e100000") != classifyNumber("1") {
		t.Error("padded and plain spellings of 1 differ")
	}
}

// 1 MiB of the slow tokens must go through the guard and the hash form fast.
func TestPaddedNumberTokensAreCheapInABody(t *testing.T) {
	for name, tok := range map[string]string{
		"huge positive": "0." + strings.Repeat("0", 10098) + "1e1009998",
		"huge negative": "1" + strings.Repeat("0", 10099) + "e-1009998",
	} {
		body := []byte("[" + strings.Repeat(tok+",", (1<<20)/(len(tok)+1)) + "0]")
		start := time.Now()
		got := hasOutOfRangeNumber(body)
		_, inexact := hashForm(body)
		if d := time.Since(start); d > 500*time.Millisecond {
			t.Fatalf("%s: 1 MiB of tokens took %v, want well under 1 s", name, d)
		}
		if got != (name == "huge positive") || inexact == 0 {
			t.Errorf("%s: out of range = %v, inexact = %d", name, got, inexact)
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

	// One 1 MB token with a single significant digit goes through the exact
	// comparison (the cap is on significant digits) and must stay cheap too.
	// 2^53+1 is inexact, 0.5 is exact; both must be found so fast.
	few := []byte(`{"n":9007199254740993.` + strings.Repeat("0", 1<<20) + `,"m":0.5` + strings.Repeat("0", 1<<20) + `}`)
	start = time.Now()
	if _, n := hashForm(few); n != 1 {
		t.Fatalf("inexact = %d, want 1 (2^53+1 is inexact, 0.5 is exact)", n)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("two 1 MB tokens with few digits took %v", d)
	}
}
