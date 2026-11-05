package clickhouse

import (
	"errors"
	"strings"
	"testing"
)

func TestRedactRemovesFullDSNAndEncodedOrShortSecrets(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dsn     string
		message string
		secrets []string
	}{
		{
			name:    "encoded password",
			dsn:     "clickhouse://reader:my%2Fsecret@host:9000/analytics",
			message: "dial failed: my/secret (clickhouse://reader:my%2Fsecret@host:9000/analytics)",
			secrets: []string{"my/secret", "my%2Fsecret", "clickhouse://reader:"},
		},
		{
			name:    "short password",
			dsn:     "clickhouse://reader:abc@host:9000/analytics",
			message: "authentication rejected for password abc",
			secrets: []string{"abc"},
		},
		{
			name:    "encoded query token",
			dsn:     "clickhouse://host:9000/analytics?access_token=token%2Fvalue",
			message: "invalid token value token/value",
			secrets: []string{"token/value", "token%2Fvalue"},
		},
		{
			name:    "malformed escaped password",
			dsn:     "clickhouse://reader:secret%ZZ@host:9000/analytics",
			message: "authentication rejected for password secret%ZZ",
			secrets: []string{"secret%ZZ"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactString(tc.message, tc.dsn)
			for _, secret := range tc.secrets {
				if strings.Contains(got, secret) {
					t.Errorf("redacted text %q still contains %q", got, secret)
				}
			}
			if !strings.Contains(got, redacted) {
				t.Fatalf("redacted text = %q, want marker", got)
			}
		})
	}
}

func TestRedactPreservesWrappedError(t *testing.T) {
	cause := errors.New("driver error")
	err := Redact(cause, "clickhouse://reader:secret@host/db")
	if !errors.Is(err, cause) {
		t.Fatalf("Redact error %v does not unwrap to cause", err)
	}
}
