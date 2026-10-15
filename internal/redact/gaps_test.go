package redact

import (
	"strings"
	"testing"
)

// gapCases are the leaks the package comment used to list as known gaps,
// plus three found by the executor tests (internal/room). Each names the
// parts of the secret that must not survive.
var gapCases = []struct {
	name, in string
	secrets  []string
}{
	{"url-encoded separator", "password%3DZx9%21hunter2-Qw7", []string{"Zx9", "hunter2", "Qw7"}},
	{"url-encoded in query", "GET /login?user=bob&password%3DZx9%21hunter2-Qw7%26next%3D%2Fhome", []string{"Zx9", "hunter2", "Qw7"}},
	{"bare value wrapped onto next line", "password=Zx9!hun\nter2-Qw7", []string{"Zx9", "ter2-Qw7"}},
	{"bare value wrapped, CRLF", "password=Zx9!hun\r\nter2-Qw7\r\n", []string{"Zx9", "ter2-Qw7"}},
	{"bare value with shell continuation", "export PGPASSWORD=Zx9!hun\\\nter\n", []string{"Zx9", "\nter"}},
	{"quoted value with backslash-newline", "password=\"Zx9!hunter2-\\\nQw7\"", []string{"Zx9", "hunter2", "Qw7"}},
	{"quoted value with two continuations", "password=\"Zx9!\\\nhunter2-\\\nQw7\" next", []string{"Zx9", "hunter2", "Qw7"}},
	{"quoted value wrapped without backslash", "password=\"Zx9!hun\nter2-Qw7\" user=\"bob\"", []string{"Zx9", "ter2-Qw7"}},
	{"single-quoted value wrapped", "secret: 'Zx9!hun\nter2 Qw7'", []string{"Zx9", "ter2 Qw7"}},
	{"escaped-quoted value wrapped", `{"cfg":"password=\"Zx9!hun` + "\n" + `ter2-Qw7\" x"}`, []string{"Zx9", "ter2-Qw7"}},
	{"url password with raw @", "postgres://app:p@ss@db/x", []string{"p@ss"}},
	{"url password with two raw @", "postgres://app:p4ss@word@db:5432/orders", []string{"p4ss", "word@"}},
	{"url password with raw /", "postgres://app:p4ss/w0rd@db/x", []string{"p4ss", "w0rd"}},
	{"url password with raw @ and /", "postgres://app:p@ss/w0rd@db/x", []string{"p@ss", "w0rd"}},
	{"quoted password with spaces", `password="correct horse battery staple" user=bob`, []string{"correct", "horse", "battery", "staple"}},
	{"unterminated quoted password with spaces", `password="correct horse battery staple`, []string{"correct", "horse", "battery", "staple"}},
	{"unterminated json value (truncated)", `{"user":"bob","password":"Zx9!hunter2 Qw7…`, []string{"Zx9", "hunter2", "Qw7"}},
	{"jaas escaped twice", `{"a":"{\"b\":\"sasl.jaas.config=x required username=\\\"svc\\\" password=\\\"Pa55word\\\";\"}"}`, []string{"Pa55word"}},
	{"json key escaped twice", `{\\\"password\\\":\\\"Pa55word\\\"}`, []string{"Pa55word"}},
}

func TestKnownGapsClosed(t *testing.T) {
	for _, tc := range gapCases {
		t.Run(tc.name, func(t *testing.T) {
			got, st := Text(tc.in)
			for _, s := range tc.secrets {
				if strings.Contains(got, s) {
					t.Errorf("Text(%q) = %q: %q survives", tc.in, got, s)
				}
			}
			if st.Total == 0 {
				t.Errorf("Text(%q): no masks counted", tc.in)
			}
		})
	}
}

// TestGapExactOutput pins the output for the main cases, so a fix cannot
// pass by masking far more than the secret.
func TestGapExactOutput(t *testing.T) {
	tests := []struct{ in, want string }{
		{"password%3DZx9%21hunter2-Qw7", "password%3D[redacted 24]"},
		{"a=1&password%3DZx9%21hunter2%26next%3D%2Fhome", "a=1&password%3D[redacted 16]%26next%3D%2Fhome"},
		{"password=Zx9!hun\nter2-Qw7", "password=[redacted 8]\n[redacted 8]"},
		{"password=Zx9!hun\nter2-Qw7 done", "password=[redacted 8]\n[redacted 8] done"},
		{"password=\"Zx9!hunter2-\\\nQw7\"", "password=\"[redacted 16]\\\n[redacted 8]\""},
		{"postgres://app:p@ss@db/x", "postgres://app:[redacted 8]@db/x"},
		{"postgres://app:p@ss/w0rd@db:5432/x", "postgres://app:[redacted 16]@db:5432/x"},
		{"postgres://app:pw12@db/orders?user=a@b.com", "postgres://app:[redacted 8]@db/orders?user=a@b.com"},
		{"https://u:tok12345@registry.example.com/@scope/pkg", "https://u:[redacted 8]@registry.example.com/@scope/pkg"},
		{`password="correct horse battery staple" user=bob`, `password="[redacted 32]" user=bob`},
		{`password="correct horse`, `password="[redacted 16]`},
		{`password=\\\"Pa55word\\\";`, `password=\\\"[redacted 8]\\\";`},
		// Unquoted values stop at the first space: see the package comment.
		{"password=correct horse", "password=[redacted 8] horse"},
	}
	for _, tc := range tests {
		got, _ := New(Options{}).Text(tc.in)
		if got != tc.want {
			t.Errorf("Text(%q)\n got  %q\n want %q", tc.in, got, tc.want)
		}
	}
}

// TestGapNearMisses keeps ordinary text that sits close to the new rules.
func TestGapNearMisses(t *testing.T) {
	for _, in := range []string{
		"password_policy=strict",
		"token_count=3",
		"password_encryption%3Dscram-sha-256",
		"https://db.example.com:5432/orders",
		"https://registry.example.com/@scope/pkg",
		"postgres://db:5432/orders?sslmode=require",
		"kafka://broker-1.internal:9092@v2",
		"git@github.com:org/repo.git",
		"password_policy=strict\nter2-Qw7",
		// The next line after a bare password is evidence unless it reads
		// like the tail of a credential.
		"password=[redacted 8]\nConnection refused",
		"slot=debezium_orders\nERROR: replication slot is active",
		"auth_method=scram\npg-orders-connector FAILED",
		// A plain word, an indented line, a timestamp or a log prefix after
		// a password is never part of it.
		"password=Pa55word\nconnection refused",
		"password=Pa55word\n  at org.postgresql.Driver.connect",
		"password=Pa55word\n2026-10-14T09:41:07Z INFO started",
		"password=Pa55word\n[INFO] connected",
		"password=Pa55word\npublic.orders",
		"password=Pa55word\n10.0.4.12",
		"token_type=bearer\nZx9Qw7aa",
		`message="password reset for user bob"`,
	} {
		got, _ := New(Options{}).Text(in)
		want := in
		// The first line's password is still masked; only the rest must stay.
		if i := strings.Index(in, "password=Pa55word"); i >= 0 {
			want = strings.Replace(in, "Pa55word", "[redacted 8]", 1)
		}
		if got != want {
			t.Errorf("Text(%q)\n got  %q\n want %q", in, got, want)
		}
	}
}

// TestGapIdempotent is a property test: a second pass over the output of the
// new cases, and of random mixes of their pieces, changes nothing.
func TestGapIdempotent(t *testing.T) {
	var ins []string
	for _, tc := range gapCases {
		ins = append(ins, tc.in)
	}
	pieces := []string{
		"password=", "password%3D", "secret: ", `"`, `'`, `\"`, `\\\"`, "\\", "\n", "\r\n", " ", "Zx9!hun", "ter2-Qw7",
		"postgres://app:", "@", "/", "db", "p@ss", "token=", "Ab3xQ9zL7mK2", "…", "[redacted 8]", "&", "%26", ":", "=",
	}
	g := newGen(42)
	for range 4000 {
		var b strings.Builder
		for range 1 + g.r.IntN(10) {
			b.WriteString(pieces[g.r.IntN(len(pieces))])
		}
		ins = append(ins, b.String())
	}
	for _, in := range ins {
		once, _ := Text(in)
		twice, st := Text(once)
		if twice != once || st.Total != 0 {
			t.Fatalf("not idempotent:\n in:    %q\n once:  %q\n twice: %q", in, once, twice)
		}
	}
}
