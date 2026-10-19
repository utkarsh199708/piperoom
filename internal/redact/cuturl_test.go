package redact

import (
	"strings"
	"testing"
)

// TestCutURLPassword covers a URL cut before its "@host": a capped log tail
// (Airflow task_log_tail) or a field can end inside the password, and the
// part that is there must still be masked.
func TestCutURLPassword(t *testing.T) {
	tests := []struct {
		name, in, want string
		secret         string // must not survive; "" when nothing is masked
	}{
		{"cut at end of text", "dial postgres://user:secret", "dial postgres://user:[redacted 8]", "secret"},
		{"cut, long password", "postgres://app:Zx9hunter2Qw7", "postgres://app:[redacted 16]", "Zx9hunter2Qw7"},
		{"cut, empty user", "redis://:Zx9hunter2", "redis://:[redacted 16]", "Zx9hunter2"},
		{"followed by newline", "postgres://user:secret\nnext line", "postgres://user:[redacted 8]\nnext line", "secret"},
		{"followed by CRLF", "postgres://user:secret\r\nnext", "postgres://user:[redacted 8]\r\nnext", "secret"},
		{"followed by double quote", `{"dsn":"postgres://user:secret","x":1}`, `{"dsn":"postgres://user:[redacted 8]","x":1}`, "secret"},
		{"followed by single quote", `dsn='amqp://svc:secret' ok`, `dsn='amqp://svc:[redacted 8]' ok`, "secret"},
		{"followed by space", "postgres://user:secret connection refused", "postgres://user:[redacted 8] connection refused", "secret"},
		{"followed by tab", "mysql://root:secret\tfailed", "mysql://root:[redacted 8]\tfailed", "secret"},
		{"password with slash", "postgres://app:p4ss/w0rd", "postgres://app:[redacted 16]", "w0rd"},
		{"password starting with digits", "postgres://app:2024Spring", "postgres://app:[redacted 16]", "Spring"},
		// No password: a port after a host, or after user@host.
		{"host:8080", "GET http://host:8080 timed out", "GET http://host:8080 timed out", ""},
		{"host:8080 at end", "http://connect:8083", "http://connect:8083", ""},
		{"host:port then path", "http://connect:8083/connectors", "http://connect:8083/connectors", ""},
		{"host:port then full stop", "see http://connect:8083.", "see http://connect:8083.", ""},
		{"host list", "SASL_SSL://broker-1:9093,broker-2:9093", "SASL_SSL://broker-1:9093,broker-2:9093", ""},
		{"sql server port", "jdbc:sqlserver://db:1433;databaseName=orders", "jdbc:sqlserver://db:1433;databaseName=orders", ""},
		{"user@host:5432", "postgres://user@host:5432", "postgres://user@host:5432", ""},
		{"user@host:5432/db", "postgres://user@host:5432/orders", "postgres://user@host:5432/orders", ""},
		{"windows file url", "file://C:/Users/ops/dag.py", "file://C:/Users/ops/dag.py", ""},
		// The full form keeps working.
		{"user:pass@host:5432", "postgres://user:secret@host:5432/orders", "postgres://user:[redacted 8]@host:5432/orders", "secret"},
		{"user@server:pass@host:5432", "postgresql://myadmin@srv:Zx9hunter2Qw7@srv.example.com:5432/postgres",
			"postgresql://myadmin@srv:[redacted 16]@srv.example.com:5432/postgres", "Zx9hunter2Qw7"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, st := New(Options{}).Text(tc.in)
			if got != tc.want {
				t.Errorf("Text(%q)\n got  %q\n want %q", tc.in, got, tc.want)
			}
			if tc.secret == "" {
				return
			}
			if strings.Contains(got, tc.secret) {
				t.Errorf("Text(%q) leaks %q: %q", tc.in, tc.secret, got)
			}
			if st.ByKind[KindURLPass] == 0 {
				t.Errorf("Text(%q): no %s mask in %+v", tc.in, KindURLPass, st)
			}
		})
	}
}
