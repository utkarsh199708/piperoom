package databricks

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

// testSecret is an obviously fake OAuth secret (see testToken).
const testSecret = "fake-test-oauth-secret-not-real"

func TestParseDSN(t *testing.T) {
	host := "dbc-1234abcd-5678.cloud.databricks.com"
	path := "/sql/1.0/warehouses/abc123"
	for _, c := range []struct {
		name, dsn string
		want      Config
	}{
		{"pat", "token:" + testToken + "@" + host + ":443" + path + "?catalog=main",
			Config{Host: host, Port: 443, HTTPPath: path, Catalog: "main", Token: testToken}},
		{"https prefix, default port, schema", "https://token:" + testToken + "@" + host + path + "?catalog=main&schema=sales",
			Config{Host: host, Port: 443, HTTPPath: path, Catalog: "main", Schema: "sales", Token: testToken}},
		{"other port", "token:" + testToken + "@" + host + ":8443" + path + "?catalog=main",
			Config{Host: host, Port: 8443, HTTPPath: path, Catalog: "main", Token: testToken}},
		{"accessToken param", host + ":443" + path + "?catalog=main&authType=Pat&accessToken=" + testToken,
			Config{Host: host, Port: 443, HTTPPath: path, Catalog: "main", Token: testToken}},
		{"escaped token", "token:" + url.QueryEscape("ab/c+d=e") + "@" + host + path + "?catalog=main",
			Config{Host: host, Port: 443, HTTPPath: path, Catalog: "main", Token: "ab/c+d=e"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseDSN(c.dsn)
			if err != nil {
				t.Fatalf("ParseDSN: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %+v\nwant %+v", got, c.want)
			}
		})
	}
}

func TestParseDSNRefuses(t *testing.T) {
	host := "dbc-1234abcd-5678.cloud.databricks.com"
	path := "/sql/1.0/warehouses/abc123"
	tok := "token:" + testToken + "@"
	for _, c := range []struct{ name, dsn string }{
		{"http", "http://" + tok + host + path + "?catalog=main"},
		{"other scheme", "jdbc://" + tok + host + path + "?catalog=main"},
		{"no catalog", tok + host + path},
		{"bad catalog", tok + host + path + "?catalog=ma-in"},
		{"bad schema", tok + host + path + "?catalog=main&schema=a;b"},
		{"unknown param (session param)", tok + host + path + "?catalog=main&ansi_mode=false"},
		{"useKernel", tok + host + path + "?catalog=main&useKernel=true"},
		{"repeated", tok + host + path + "?catalog=main&catalog=other"},
		{"user not token", "admin:" + testToken + "@" + host + path + "?catalog=main"},
		{"token as user", testToken + "@" + host + path + "?catalog=main"},
		{"empty token", "token:@" + host + path + "?catalog=main"},
		{"no credential", host + path + "?catalog=main"},
		{"u2m", host + path + "?catalog=main&authType=OAuthU2M"},
		{"unknown auth", host + path + "?catalog=main&authType=basic&accessToken=" + testToken},
		{"m2m without secret", host + path + "?catalog=main&authType=OAuthM2M&clientId=app"},
		{"token and m2m", tok + host + path + "?catalog=main&clientId=app&clientSecret=" + testSecret},
		{"token twice", tok + host + path + "?catalog=main&accessToken=" + testToken},
		{"no path", tok + host + "?catalog=main"},
		{"dotdot path", tok + host + "/sql/../x?catalog=main"},
		{"token in path", tok + host + "/sql/1.0/" + testToken + "%00?catalog=main"},
		{"bad port", tok + host + ":99999" + path + "?catalog=main"},
		{"no host", tok + path + "?catalog=main"},
		{"fragment", tok + host + path + "?catalog=main#" + testToken},
		{"garbage", "%%%" + testToken},
		{"secret as key", tok + host + path + "?catalog=main&" + testToken + "=1"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseDSN(c.dsn)
			if err == nil {
				t.Fatal("want error")
			}
			if strings.Contains(err.Error(), testToken) || strings.Contains(err.Error(), testSecret) {
				t.Fatalf("secret in error: %v", err)
			}
		})
	}
}

// OAuth M2M DSNs parse when the client credentials are complete (explicit
// authType or inferred), and are refused without echoing the secret when they
// are incomplete or mixed with a personal access token.
func TestParseDSNM2M(t *testing.T) {
	host := "dbc-1234abcd-5678.cloud.databricks.com"
	path := "/sql/1.0/warehouses/abc123"
	for _, c := range []struct {
		name, dsn string
		want      Config
	}{
		{"explicit", host + path + "?catalog=main&authType=OAuthM2M&clientId=app&clientSecret=" + testSecret,
			Config{Host: host, Port: 443, HTTPPath: path, Catalog: "main", ClientID: "app", ClientSecret: testSecret}},
		{"inferred clientID", host + path + "?catalog=main&clientID=app&clientSecret=" + testSecret,
			Config{Host: host, Port: 443, HTTPPath: path, Catalog: "main", ClientID: "app", ClientSecret: testSecret}},
		{"case-insensitive auth", host + path + "?catalog=main&authType=oauthm2m&clientId=app&clientSecret=" + testSecret,
			Config{Host: host, Port: 443, HTTPPath: path, Catalog: "main", ClientID: "app", ClientSecret: testSecret}},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseDSN(c.dsn)
			if err != nil {
				t.Fatalf("ParseDSN: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %+v\nwant %+v", got, c.want)
			}
			if !got.isM2M() {
				t.Fatal("not marked M2M")
			}
		})
	}

	for _, dsn := range []string{
		host + path + "?catalog=main&authType=OAuthM2M&clientId=app",                                           // no secret
		host + path + "?catalog=main&authType=OAuthM2M",                                                        // neither
		host + path + "?catalog=main&clientSecret=" + testSecret,                                               // no id
		"token:" + testToken + "@" + host + path + "?catalog=main&clientId=app&clientSecret=" + testSecret,     // token + m2m
		host + path + "?catalog=main&authType=OAuthM2M&clientId=app&clientId=app2&clientSecret=" + testSecret,  // repeated id
		host + path + "?catalog=main&authType=OAuthM2M&clientId=app&clientSecret=x&clientSecret=" + testSecret, // repeated secret
	} {
		_, err := ParseDSN(dsn)
		if err == nil {
			t.Errorf("%s: want error", dsn)
			continue
		}
		if strings.Contains(err.Error(), testSecret) || strings.Contains(err.Error(), testToken) {
			t.Errorf("%s: secret in error: %v", dsn, err)
		}
	}
}

func TestConfigStringHidesSecrets(t *testing.T) {
	c := testCfg
	for _, s := range []string{fmt.Sprint(c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c), c.target()} {
		if strings.Contains(s, testToken) || strings.Contains(s, testSecret) {
			t.Fatalf("secret printed: %s", s)
		}
	}
	m2m := Config{Host: "dbc-1234abcd-5678.cloud.databricks.com", Port: 443, HTTPPath: "/sql/1.0/warehouses/a", Catalog: "main", ClientID: "app", ClientSecret: testSecret}
	for _, s := range []string{fmt.Sprint(m2m), fmt.Sprintf("%+v", m2m), fmt.Sprintf("%#v", m2m), m2m.target()} {
		if strings.Contains(s, testSecret) {
			t.Fatalf("m2m secret printed: %s", s)
		}
	}
}

func TestRedact(t *testing.T) {
	host := "dbc-1234abcd-5678.cloud.databricks.com"
	escaped := "ab/cd+ef=gh"
	for _, c := range []struct {
		name, dsn string
		secrets   []string
	}{
		{"pat", "token:" + testToken + "@" + host + "/sql/1.0/warehouses/a?catalog=main", []string{testToken}},
		{"m2m", host + "/sql/1.0/warehouses/a?catalog=main&clientId=app&clientSecret=" + testSecret, []string{testSecret}},
		{"escaped", "token:" + url.QueryEscape(escaped) + "@" + host + "/p?catalog=main", []string{escaped, url.QueryEscape(escaped)}},
		// ParseDSN refuses these, but their secrets are still scrubbed.
		{"refused dsn", "http://token:" + testToken + "@" + host + "/p", []string{testToken}},
		{"token as user", testToken + "@" + host + "/p", []string{testToken}},
	} {
		t.Run(c.name, func(t *testing.T) {
			msg := "dial " + c.dsn + " failed; token=" + strings.Join(c.secrets, " and ")
			err := Redact(errors.New(msg), c.dsn)
			for _, s := range append(c.secrets, c.dsn) {
				if strings.Contains(err.Error(), s) {
					t.Fatalf("%q survives: %s", s, err)
				}
			}
			if !strings.Contains(err.Error(), redacted) {
				t.Fatalf("nothing redacted: %s", err)
			}
		})
	}
	sentinel := errors.New("x " + testToken)
	if err := Redact(sentinel, "token:"+testToken+"@h/p?catalog=c"); !errors.Is(err, sentinel) {
		t.Fatal("Redact lost the wrapped error")
	}
	if Redact(nil, "x") != nil {
		t.Fatal("Redact(nil) != nil")
	}
}
