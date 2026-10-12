package snowflake

import (
	"errors"
	"strings"
	"testing"
)

func TestRedact(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		msg  string
		leak []string
	}{
		{
			name: "whole dsn echoed",
			dsn:  "svc:S3cr3t!pw@acme-xy12345/ANALYTICS/PUBLIC?warehouse=WH&role=PIPEROOM_RO",
			msg:  "parse svc:S3cr3t!pw@acme-xy12345/ANALYTICS/PUBLIC?warehouse=WH&role=PIPEROOM_RO failed",
			leak: []string{"S3cr3t!pw", "svc:S3cr3t"},
		},
		{
			name: "password alone",
			dsn:  "svc:S3cr3t!pw@acme-xy12345/ANALYTICS/PUBLIC",
			msg:  "login failed for password S3cr3t!pw",
			leak: []string{"S3cr3t!pw"},
		},
		{
			name: "password with at sign and slash",
			dsn:  "svc:p@ss/word@acme/DB/SCH",
			msg:  "bad credential p@ss/word",
			leak: []string{"p@ss/word"},
		},
		{
			name: "url-escaped password",
			dsn:  "svc:p%40ss@acme/DB/SCH",
			msg:  "bad credential p@ss",
			leak: []string{"p@ss"},
		},
		{
			name: "token param",
			dsn:  "svc@acme/DB/SCH?authenticator=oauth&token=abc123tok",
			msg:  "rejected abc123tok",
			leak: []string{"abc123tok"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := errors.New(tc.msg)
			got := Redact(base, tc.dsn)
			for _, l := range tc.leak {
				if strings.Contains(got.Error(), l) {
					t.Errorf("redacted error %q still contains %q", got, l)
				}
			}
			if !errors.Is(got, base) {
				t.Error("redacted error no longer unwraps to the original")
			}
		})
	}
}

func TestRedactKeepsHarmlessText(t *testing.T) {
	err := Redact(errors.New("snowflake: ping: context deadline exceeded"), "svc:pw@acme/DB/SCH")
	if err.Error() != "snowflake: ping: context deadline exceeded" {
		t.Fatalf("Redact changed a message with no secret: %q", err)
	}
	if Redact(nil, "x") != nil {
		t.Fatal("Redact(nil) must be nil")
	}
	if RedactString("hello", "") != "hello" {
		t.Fatal("empty dsn must leave the string alone")
	}
}
