package pgconv

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRedactDSNString(t *testing.T) {
	cases := []struct {
		name    string
		dsn     string
		msg     string
		gone    []string // must not appear in the result
		kept    []string // must still appear
		wantOut string   // exact result, when set
	}{
		{
			name: "url password and the whole dsn",
			dsn:  "postgres://writer:S3cret-pass@db.internal:5432/orders?sslmode=require",
			msg:  "dial postgres://writer:S3cret-pass@db.internal:5432/orders?sslmode=require failed; pw S3cret-pass",
			gone: []string{"S3cret-pass", "postgres://writer"},
			kept: []string{"dial", "failed"},
		},
		{
			name: "url-escaped password in both forms",
			dsn:  "postgresql://w:p%40ss%2Fword@db/orders",
			msg:  "a p%40ss%2Fword b p@ss/word c",
			gone: []string{"p%40ss%2Fword", "p@ss/word"},
			kept: []string{"a ", " b ", " c"},
		},
		{
			name: "password query parameter",
			dsn:  "postgres://db/orders?user=w&password=q-secret-1&sslpassword=keypass9",
			msg:  "saw q-secret-1 and keypass9",
			gone: []string{"q-secret-1", "keypass9"},
		},
		{
			name: "keyword/value, quoted with a space",
			dsn:  "host=db user=w password='a b secret' dbname=orders",
			msg:  "conn: a b secret",
			gone: []string{"a b secret"},
		},
		{
			name: "keyword/value, plain",
			dsn:  "host=db user=w password=topsecret dbname=orders",
			msg:  "conn: password=topsecret here",
			gone: []string{"topsecret"},
		},
		{
			name:    "short password only in dsn syntax",
			dsn:     "postgres://w:pw@db/orders",
			msg:     "user w:pw@db; the pwd file",
			wantOut: "user w:[redacted]@db; the pwd file",
		},
		{
			name:    "backticked parts go even without a dsn",
			dsn:     "",
			msg:     "failed to connect to `user=w database=orders`: refused",
			wantOut: "failed to connect to `[redacted]`: refused",
		},
		{
			name: "malformed url: password still found by hand",
			dsn:  "postgres://w:hunter2secret@db:notaport/orders",
			msg:  "invalid port near hunter2secret",
			gone: []string{"hunter2secret"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactDSNString(tc.msg, tc.dsn)
			if tc.wantOut != "" && got != tc.wantOut {
				t.Fatalf("got %q, want %q", got, tc.wantOut)
			}
			for _, g := range tc.gone {
				if strings.Contains(got, g) {
					t.Fatalf("%q still in %q", g, got)
				}
			}
			for _, k := range tc.kept {
				if !strings.Contains(got, k) {
					t.Fatalf("%q lost from %q", k, got)
				}
			}
		})
	}
}

// A real pgx parse error of a DSN with a bad port: pgx prints the connection
// string; neither it nor the password survive.
func TestRedactDSNOnPgxParseError(t *testing.T) {
	dsn := "postgres://writer:hunter2secret@db.internal:badport/orders"
	_, err := pgxpool.ParseConfig(dsn)
	if err == nil {
		t.Fatal("expected a parse error")
	}
	got := RedactDSN(err, dsn).Error()
	for _, g := range []string{"hunter2secret", dsn, "writer:", "db.internal"} {
		if strings.Contains(got, g) {
			t.Fatalf("%q still in %q", g, got)
		}
	}
}

func TestRedactDSNKeepsChain(t *testing.T) {
	if RedactDSN(nil, "postgres://a:b@c/d") != nil {
		t.Fatal("nil error must stay nil")
	}
	sentinel := errors.New("boom")
	err := RedactDSN(fmt.Errorf("x %s: %w", "postgres://a:longsecret@c/d", sentinel), "postgres://a:longsecret@c/d")
	if !errors.Is(err, sentinel) {
		t.Fatal("errors.Is lost through RedactDSN")
	}
	if strings.Contains(err.Error(), "longsecret") {
		t.Fatalf("secret in %q", err.Error())
	}
}
