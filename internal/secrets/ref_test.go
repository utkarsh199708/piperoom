package secrets

import (
	"errors"
	"strings"
	"testing"
)

func TestParseRef(t *testing.T) {
	tests := []struct {
		in   string
		want Ref
		err  bool
	}{
		{in: "secret://aws-sm-prod/prod/piperoom/orders-db#password",
			want: Ref{Provider: "aws-sm-prod", Path: "prod/piperoom/orders-db", Key: "password"}},
		{in: "secret://vault-corp/database/creds/piperoom-ro#password",
			want: Ref{Provider: "vault-corp", Path: "database/creds/piperoom-ro", Key: "password"}},
		{in: "secret://files/run/secrets/kafka/password",
			want: Ref{Provider: "files", Path: "run/secrets/kafka/password"}},
		{in: "secret://v/secret/app#k?version=3", want: Ref{Provider: "v", Path: "secret/app", Key: "k", Version: "3"}},
		{in: "secret://v/secret/app?version=3#k", want: Ref{Provider: "v", Path: "secret/app", Key: "k", Version: "3"}},
		{in: "secret://v/secret/app?version=AWSPREVIOUS", want: Ref{Provider: "v", Path: "secret/app", Version: "AWSPREVIOUS"}},
		{in: "secret://aws/arn:aws:secretsmanager:eu-west-1:123:secret:db-AbC",
			want: Ref{Provider: "aws", Path: "arn:aws:secretsmanager:eu-west-1:123:secret:db-AbC"}},
		{in: "secret://p/a%20b/c%23d#k%3Fx", want: Ref{Provider: "p", Path: "a b/c#d", Key: "k?x"}},
		// Decoded once only: %252e%252e stays the literal name "%2e%2e".
		{in: "secret://p/a/%252e%252e/b", want: Ref{Provider: "p", Path: "a/%2e%2e/b"}},
		{in: "secret://p_1-x/x", want: Ref{Provider: "p_1-x", Path: "x"}},

		{in: "", err: true},
		{in: "secret:/p/x", err: true},
		{in: "SECRET://p/x", err: true},
		{in: "secret://p", err: true},
		{in: "secret://p/", err: true},
		{in: "secret:///x", err: true},
		{in: "secret://Vault/x", err: true},
		{in: "secret://-p/x", err: true},
		{in: "secret://" + strings.Repeat("a", 64) + "/x", err: true},
		{in: "secret://p/../etc/passwd", err: true},
		{in: "secret://p/a/../b", err: true},
		{in: "secret://p/a/./b", err: true},
		{in: "secret://p/a/%2e%2e/b", err: true},
		{in: "secret://p/a/%2E%2E", err: true},
		{in: "secret://p/a%2f..%2fb", err: true},
		{in: "secret://p/a//b", err: true},
		{in: "secret://p/a/", err: true},
		{in: "secret://p/a%00b", err: true},
		{in: "secret://p/a%5cb", err: true},
		{in: "secret://p/a%zz", err: true},
		{in: "secret://p/a#", err: true},
		{in: "secret://p/a#k#j", err: true},
		{in: "secret://p/a?v=1", err: true},
		{in: "secret://p/a?version=", err: true},
		{in: "secret://p/a?version=1&x=2", err: true},
		{in: "secret://p/a?version=1?version=2", err: true},
		{in: "secret://p/" + strings.Repeat("a", maxRefLen), err: true},
	}
	for _, tt := range tests {
		got, err := ParseRef(tt.in)
		if tt.err {
			if !errors.Is(err, ErrInvalidRef) {
				t.Errorf("ParseRef(%q) err = %v, want ErrInvalidRef", tt.in, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseRef(%q): %v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseRef(%q) = %+v, want %+v", tt.in, got, tt.want)
		}
	}
}

func TestParseRefErrorNeverQuotesInput(t *testing.T) {
	const pasted = "hunter2-PASSWORD"
	for _, in := range []string{pasted, "secret://p/" + pasted + "%zz", "secret://p/x?" + pasted} {
		_, err := ParseRef(in)
		if err == nil || strings.Contains(err.Error(), pasted) {
			t.Errorf("ParseRef error %v quotes the input", err)
		}
	}
}

func TestRefStringRoundTrip(t *testing.T) {
	refs := []Ref{
		{Provider: "p", Path: "a/b"},
		{Provider: "p", Path: "a b/c#d?e%f", Key: "k#?%", Version: "v 1"},
		{Provider: "aws", Path: "arn:aws:secretsmanager:eu-west-1:1:secret:x", Key: "password", Version: "AWSCURRENT"},
		{Provider: "p", Path: "%2e%2e/x"},
	}
	for _, r := range refs {
		s := r.String()
		got, err := ParseRef(s)
		if err != nil {
			t.Errorf("ParseRef(%q): %v", s, err)
			continue
		}
		if got != r {
			t.Errorf("round trip %q = %+v, want %+v", s, got, r)
		}
	}
	if got := (Ref{Provider: "files", Path: "run/secrets/x"}).String(); got != "secret://files/run/secrets/x" {
		t.Errorf("String = %q", got)
	}
}

func TestIsRef(t *testing.T) {
	if !IsRef("secret://x/y") || IsRef("hunter2") || IsRef(" secret://x/y") {
		t.Error("IsRef")
	}
}
