package jsonnum

import (
	"strings"
	"testing"
)

func TestOutOfRange(t *testing.T) {
	for tok, want := range map[string]bool{
		"0":                               false,
		"-0":                              false,
		"1":                               false,
		"1.5e10":                          false,
		"0e999999":                        false, // zero stays zero
		"0.000":                           false,
		"5e-324":                          false, // smallest denormal
		"1.7976931348623157e308":          false,
		"1e309":                           true,
		"-1e400":                          true,
		"1e131072":                        true, // jsonb refuses this one
		"1e-400":                          true, // underflows to zero
		"1e-16384":                        true, // jsonb refuses this one
		"1e99999999999":                   true,
		"-":                               false, // not a number
		"1e":                              false,
		"1." + strings.Repeat("0", 20000): true, // scale beyond what jsonb takes
		// ParseFloat cuts a long exponent off at about 10000 digits and would
		// read these as 1 and 1; the values are 1e999899 and 1e-999899.
		"0." + strings.Repeat("0", 10098) + "1e1009998": true,
		"1" + strings.Repeat("0", 10099) + "e-1009998":  true,
		"0." + strings.Repeat("0", 5000) + "1e5001":     false, // value 1
	} {
		if got := OutOfRange(tok); got != want {
			t.Errorf("OutOfRange(%.40q) = %v, want %v", tok, got, want)
		}
	}
}

func TestQuote(t *testing.T) {
	for _, tc := range []struct {
		in, out string
		n       int
	}{
		{`{"a":1,"b":[0.1,2]}`, `{"a":1,"b":[0.1,2]}`, 0},
		{`{"a":1e400}`, `{"a":"1e400"}`, 1},
		{`{"a":[-1e400,1e-16384,3]}`, `{"a":["-1e400","1e-16384",3]}`, 2},
		{`{"s":"1e400 is text","t":1e131072}`, `{"s":"1e400 is text","t":"1e131072"}`, 1},
		{`{"s":"quote \" then 1e400 inside"}`, `{"s":"quote \" then 1e400 inside"}`, 0},
		{`[1e400,1e400]`, `["1e400","1e400"]`, 2},
		{`[]`, `[]`, 0},
		{``, ``, 0},
	} {
		out, n := Quote([]byte(tc.in))
		if string(out) != tc.out || n != tc.n {
			t.Errorf("Quote(%s) = %s, %d; want %s, %d", tc.in, out, n, tc.out, tc.n)
		}
	}
	// The 10109-character token whose true value is 1e999899 is quoted.
	big := "0." + strings.Repeat("0", 10098) + "1e1009998"
	if out, n := Quote([]byte(`{"n":` + big + `}`)); n != 1 || string(out) != `{"n":"`+big+`"}` {
		t.Errorf("Quote of the padded huge token: n = %d, out = %.60s", n, out)
	}
	// No copy when nothing changes.
	in := []byte(`{"a":1}`)
	if out, _ := Quote(in); &out[0] != &in[0] {
		t.Error("Quote copied an input it did not change")
	}
}
