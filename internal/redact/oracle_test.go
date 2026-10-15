package redact

import (
	"strings"
	"testing"
)

// Corpus for #449: Oracle JDBC URLs with credentials,
// jdbc:oracle:<driver>:user/password@host. The credentials are dummies. Rows
// come from the ticket and the #430 reviews of the local mask in
// internal/tools/kafkatools (connect_tools.go maskOracleThin).

// listener is the text DriverManager and the Oracle driver put before a URL.
const listener = "java.sql.SQLException: No suitable driver found for "

var oracleCases = []struct {
	name, in string
	secrets  []string // must not survive
	keep     []string // must survive
}{
	{"sid", listener + "jdbc:oracle:thin:loader/Or4clePw77@wh:1521:ORCL", []string{"Or4clePw77"}, []string{"jdbc:oracle:thin:loader/", "@wh:1521:ORCL"}},
	{"service", listener + "jdbc:oracle:thin:loader/Or4clePw77@//wh:1521/svc", []string{"Or4clePw77"}, []string{"jdbc:oracle:thin:loader/", "@//wh:1521/svc"}},
	{"bang in password", "jdbc:oracle:thin:etl/Svc0racle!9@//wh:1521/svc", []string{"Svc0racle"}, []string{"@//wh:1521/svc"}},
	{"quoted password with blank and at", `jdbc:oracle:thin:ops/"Qu0ted p@ss"@//wh:1521/svc`, []string{"Qu0ted", "p@ss"}, []string{"jdbc:oracle:thin:ops/", "@//wh:1521/svc"}},
	{"at in password, sid", "jdbc:oracle:thin:loader/Pr0d@2024x@wh:1521:ORCL", []string{"Pr0d", "2024x"}, []string{"jdbc:oracle:thin:loader/", "@wh:1521:ORCL"}},
	{"at in password, service", "jdbc:oracle:thin:etl/S3c@Ret99@//wh:1521/svc", []string{"S3c@", "Ret99"}, []string{"@//wh:1521/svc"}},
	{"leading at", "jdbc:oracle:thin:loader/P@ssw0rd99@wh:1521:ORCL", []string{"ssw0rd99"}, []string{"@wh:1521:ORCL"}},
	{"at mid password", "jdbc:oracle:thin:etl/Ab@cD3fGh!@//wh:1521/svc", []string{"Ab@", "cD3fGh"}, []string{"@//wh:1521/svc"}},
	{"two ats", "jdbc:oracle:thin:ops/x@y@Zq9Kp2Lm@wh:1521:ORCL", []string{"x@y", "Zq9Kp2Lm"}, []string{"jdbc:oracle:thin:ops/", "@wh:1521:ORCL"}},
	{"slash and at", "jdbc:oracle:thin:loader/a/B9@x7Qw@//wh:1521/svc", []string{"B9@", "x7Qw"}, []string{"@//wh:1521/svc"}},
	{"quoted user", `jdbc:oracle:thin:"Loader"/QuPw55z@wh:1521:ORCL`, []string{"QuPw55z"}, []string{"@wh:1521:ORCL"}},
	{"quoted upper user", `jdbc:oracle:thin:"LOADER"/QuS3rPw77@wh:1521:ORCL`, []string{"QuS3rPw77"}, []string{"@wh:1521:ORCL"}},
	{"no host", "jdbc:oracle:thin:loader/NoAtPw99 (connection refused)", []string{"NoAtPw99"}, []string{"jdbc:oracle:thin:loader/", "(connection refused)"}},
	{"upper-case scheme", "JDBC:ORACLE:THIN:loader/UpPw42x@wh:1521:ORCL", []string{"UpPw42x"}, []string{"@wh:1521:ORCL"}},
	{"oci driver", "jdbc:oracle:oci:loader/OciPw42x@wh:1521:ORCL", []string{"OciPw42x"}, []string{"@wh:1521:ORCL"}},
	{"escaped quoted user", `jdbc:oracle:thin:\"Loader\"/EscU5erPw9@wh:1521:ORCL`, []string{"EscU5erPw9"}, []string{"@wh:1521:ORCL"}},
	{"escaped quoted password in json", `{\"connection.url\":\"jdbc:oracle:thin:loader/\"p w0rdJs0nTail\"@wh:1521:ORCL\"}`, []string{"w0rdJs0nTail"}, []string{"jdbc:oracle:thin:loader/", "@wh:1521:ORCL"}},
	{"doubled quote in quoted password", `jdbc:oracle:thin:loader/"Dq""Pw0rdTail"@wh:1521:ORCL`, []string{"Pw0rdTail"}, []string{"@wh:1521:ORCL"}},
	{"double-escaped quoted password", `jdbc:oracle:thin:loader/\\"DblEsc9Pw\\"@wh:1521:ORCL`, []string{"DblEsc9Pw"}, []string{"@wh:1521:ORCL"}},
	{"doubled quote in quoted user", `jdbc:oracle:thin:"lo""ader"/NqPw66Zx@wh:1521:ORCL`, []string{"NqPw66Zx"}, []string{"@wh:1521:ORCL"}},
	{"in json config", `{"connection.url":"jdbc:oracle:thin:loader/Js0nPw9x@wh:1521:ORCL","topics":"orders"}`, []string{"Js0nPw9x"}, []string{"@wh:1521:ORCL", `"topics":"orders"`}},
	{"two urls on a line", "jdbc:oracle:thin:a/FirstPw1@h1:1521:A, jdbc:oracle:thin:b/SecondPw2@h2:1521:B", []string{"FirstPw1", "SecondPw2"}, []string{"@h2:1521:B"}},
	// Sibling credentials after the URL on the same line keep their own
	// mask: the Oracle password runs to the last '@' of its token, so it
	// must run after the other rules or it would eat the sibling's key.
	{"json sibling password", `{"connection.url":"jdbc:oracle:thin:loader/Pw9Orc@wh:1521:ORCL","connection.password":"Sec@ret99Tail"}`, []string{"Pw9Orc", "ret99Tail"}, []string{"@wh:1521:ORCL", "connection.password"}},
	{"escaped json sibling password", `{\"connection.url\":\"jdbc:oracle:thin:loader/Pw9Orc@wh:1521:ORCL\",\"connection.password\":\"Sec@ret99Tail\"}`, []string{"Pw9Orc", "ret99Tail"}, []string{"@wh:1521:ORCL", "connection.password"}},
	{"json sibling token", `{"connection.url":"jdbc:oracle:thin:loader/Pw9Orc@wh:1521:ORCL","connection.token":"Tk@n9Zq8Xw7Vu6"}`, []string{"Pw9Orc", "n9Zq8Xw7Vu6"}, []string{"@wh:1521:ORCL"}},
	{"ezconnect wallet password", "jdbc:oracle:thin:loader/Pw9Orc@tcps://wh:2484/svc?wallet_location=/w&oracle.net.wallet_password=W@llT4il99", []string{"Pw9Orc", "llT4il99"}, []string{"@tcps://wh:2484/svc"}},
	{"braced sibling password", "jdbc:oracle:thin:loader/Pw9Orc@wh:1521:ORCL;password={Br@ceOrcT4il}", []string{"Pw9Orc", "ceOrcT4il"}, []string{"@wh:1521:ORCL"}},
	{"password on the next line", "jdbc:oracle:thin:loader/Pw9Orc@wh:1521:ORCL\npassword=Nx7Line9Pw", []string{"Pw9Orc", "Nx7Line9Pw"}, []string{"@wh:1521:ORCL\n"}},
}

func TestOracleThinMasked(t *testing.T) {
	for _, tc := range oracleCases {
		t.Run(tc.name, func(t *testing.T) {
			got, st := Text(tc.in)
			for _, s := range tc.secrets {
				if strings.Contains(got, s) {
					t.Errorf("Text(%q) = %q: %q survives", tc.in, got, s)
				}
			}
			for _, k := range tc.keep {
				if !strings.Contains(got, k) {
					t.Errorf("Text(%q) = %q: %q lost", tc.in, got, k)
				}
			}
			if st.Total == 0 {
				t.Errorf("Text(%q): no masks counted", tc.in)
			}
		})
	}
}

// TestOracleThinExact pins whole outputs, so the rule cannot pass by masking
// far more than the password.
func TestOracleThinExact(t *testing.T) {
	tests := []struct{ in, want string }{
		{"jdbc:oracle:thin:loader/Or4clePw77@wh:1521:ORCL", "jdbc:oracle:thin:loader/[redacted 16]@wh:1521:ORCL"},
		{"jdbc:oracle:thin:loader/Or4clePw77@//wh:1521/svc", "jdbc:oracle:thin:loader/[redacted 16]@//wh:1521/svc"},
		{`jdbc:oracle:thin:ops/"Qu0ted p@ss"@//wh:1521/svc`, `jdbc:oracle:thin:ops/[redacted 16]@//wh:1521/svc`},
		{"jdbc:oracle:thin:loader/NoAtPw99 (connection refused)", "jdbc:oracle:thin:loader/[redacted 8] (connection refused)"},
	}
	for _, tc := range tests {
		if got, _ := Text(tc.in); got != tc.want {
			t.Errorf("Text(%q)\n got  %q\n want %q", tc.in, got, tc.want)
		}
	}
}

// TestOracleThinNearMisses: URLs without credentials and prose about the
// form stay whole.
func TestOracleThinNearMisses(t *testing.T) {
	for _, in := range []string{
		"jdbc:oracle:thin:@//wh:1521/svc",
		"jdbc:oracle:thin:@wh:1521:ORCL",
		"jdbc:oracle:thin:@wh/svc",
		listener + "jdbc:oracle:thin:@//wh:1521/svc",
		"connection.url uses the user/password@host form",
		"Oracle thin URLs look like jdbc:oracle:thin:@host:port:sid",
		"jdbc:oracle:thin:/@wh:1521:ORCL", // wallet login: empty user and password
		"jdbc:oracle:thin:loader/@wh:1521:ORCL",
		"jdbc:oracle:thin:loader/[redacted 16]@wh:1521:ORCL",
	} {
		if got, _ := New(Options{}).Text(in); got != in {
			t.Errorf("Text(%q)\n got  %q\n want it unchanged", in, got)
		}
	}
}

// TestOracleThinIdempotent: a second pass over the output changes nothing,
// so the cut at the last '@' does not move.
func TestOracleThinIdempotent(t *testing.T) {
	for _, tc := range oracleCases {
		once, _ := Text(tc.in)
		twice, st := Text(once)
		if twice != once || st.Total != 0 {
			t.Errorf("not idempotent:\n in:    %q\n once:  %q\n twice: %q", tc.in, once, twice)
		}
	}
}
