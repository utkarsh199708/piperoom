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
			name: "unencoded question mark in password",
			dsn:  "svc:p?ss@acme/DB/SCH",
			msg:  "auth failed for p?ss",
			leak: []string{"p?ss"},
		},
		{
			name: "question mark in password with params",
			dsn:  "svc:pa?ssword@acme/DB/SCH?warehouse=WH&role=R",
			msg:  "auth failed for pa?ssword",
			leak: []string{"pa?ssword"},
		},
		{
			name: "plus in password",
			dsn:  "svc:pa+ss9@acme/DB/SCH",
			msg:  "auth failed for pa ss9 and pa+ss9",
			leak: []string{"pa ss9", "pa+ss9"},
		},
		{
			name: "proxyPassword param",
			dsn:  "svc:pw1234@acme/DB/SCH?proxyHost=h&proxyUser=u&proxyPassword=Pr0xySecret",
			msg:  "proxy auth failed with Pr0xySecret",
			leak: []string{"Pr0xySecret"},
		},
		{
			name: "privateKey param",
			dsn:  "svc@acme/DB/SCH?authenticator=snowflake_jwt&privateKey=MIIEvQIBADANBgkqhkiG9w0BAQEFAASC",
			msg:  "bad key MIIEvQIBADANBgkqhkiG9w0BAQEFAASC",
			leak: []string{"MIIEvQIBADANBgkqhkiG9w0BAQEFAASC"},
		},
		{
			name: "passcode param",
			dsn:  "svc:pw1234@acme/DB/SCH?passcode=483920",
			msg:  "mfa rejected 483920",
			leak: []string{"483920"},
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
	// A short password must not blank unrelated text such as the warehouse name.
	short := Redact(errors.New("warehouse WH is suspended for svc:WH@acme/DB/SCH?warehouse=WH"), "svc:WH@acme/DB/SCH?warehouse=WH")
	if !strings.Contains(short.Error(), "warehouse WH is suspended") {
		t.Fatalf("short password blanked unrelated text: %q", short)
	}
	shortCtx := Redact(errors.New("login as svc:WH@acme failed"), "svc:WH@acme/DB/SCH")
	if strings.Contains(shortCtx.Error(), ":WH@") {
		t.Fatalf("short password left in its user:pw@ context: %q", shortCtx)
	}
	if Redact(nil, "x") != nil {
		t.Fatal("Redact(nil) must be nil")
	}
	if RedactString("hello", "") != "hello" {
		t.Fatal("empty dsn must leave the string alone")
	}
}
