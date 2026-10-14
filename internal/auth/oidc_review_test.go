package auth_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/auth"
	"piperoom/internal/testenv"
)

func TestOIDCAZP(t *testing.T) {
	f := testenv.NewFakeIdP(t)
	o := newRP(t, f, "groups")
	if err := o.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}
	run := func(azp string) error {
		f.AZP = azp
		verifier := auth.NewVerifier()
		u, _ := o.AuthCodeURL("s", "n", verifier)
		code, _ := authorize(t, u)
		_, err := o.Exchange(context.Background(), code, verifier, "n")
		return err
	}
	if err := run(""); err != nil {
		t.Fatalf("no azp: %v", err)
	}
	if err := run(f.ClientID); err != nil {
		t.Fatalf("azp == client id: %v", err)
	}
	if err := run("another-client"); !errors.Is(err, auth.ErrLoginRefused) {
		t.Fatalf("azp of another client: %v, want ErrLoginRefused", err)
	}
}

// rewrite sends discovery and keys to the real fake IdP and answers the token
// call with a canned response.
type rewrite struct {
	status int
	body   string
}

func (r rewrite) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/token") {
		return &http.Response{
			StatusCode: r.status, Status: http.StatusText(r.status),
			Header: http.Header{"Content-Type": {"application/json"}},
			Body:   io.NopCloser(strings.NewReader(r.body)), Request: req,
		}, nil
	}
	return http.DefaultTransport.RoundTrip(req)
}

// A failed token call keeps only the status and a vetted OAuth error code: the
// body is whatever the IdP wrote and must not reach the error (and so the log).
func TestOIDCTokenErrorsNeverCarryTheBody(t *testing.T) {
	f := testenv.NewFakeIdP(t)
	const leak = "SECRET-DETAIL-FROM-THE-IDP"
	cases := []struct {
		name   string
		status int
		body   string
		want   error
		code   string
	}{
		{"400 with a json error", 400, `{"error":"invalid_grant","error_description":"` + leak + `"}`, auth.ErrLoginRefused, "invalid_grant"},
		{"400 with an html page", 400, "<html>" + leak + "</html>", auth.ErrLoginRefused, ""},
		{"401 with a hostile code", 401, `{"error":"` + leak + `"}`, auth.ErrLoginRefused, ""},
		{"500 with a body", 500, "boom " + leak, auth.ErrIDPUnavailable, ""},
		{"502 with a json error", 502, `{"error":"temporarily_unavailable","error_description":"` + leak + `"}`, auth.ErrIDPUnavailable, "temporarily_unavailable"},
	}
	for _, c := range cases {
		o, err := auth.NewOIDC(auth.OIDCConfig{
			Issuer: f.Issuer, ClientID: f.ClientID, ClientSecret: f.ClientSecret,
			RedirectURL: "https://piperoom.test/auth/callback", Scopes: []string{"openid"},
			HTTPClient: &http.Client{Transport: rewrite{status: c.status, body: c.body}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := o.Discover(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, err = o.Exchange(context.Background(), "code", auth.NewVerifier(), "n")
		if !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", c.name, err, c.want)
			continue
		}
		if strings.Contains(err.Error(), leak) {
			t.Errorf("%s: the error carries the response body: %v", c.name, err)
		}
		if c.code != "" && !strings.Contains(err.Error(), c.code) {
			t.Errorf("%s: the OAuth error code is missing: %v", c.name, err)
		}
	}
}
