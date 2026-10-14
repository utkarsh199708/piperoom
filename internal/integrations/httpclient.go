package integrations

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"piperoom/internal/clients/netsec"
)

// maxBody caps every response body a check or client reads (AGENTS.md §4 rule 7).
const maxBody = 4 << 20

// errBadInput marks a value that cannot work, found before any dial.
var errBadInput = errors.New("integrations: bad input")

// tlsMaterial is PEM text from the form. It is held as secrets and never
// written to disk (integrations.md §4.3).
type tlsMaterial struct {
	CA, Cert, Key netsec.Secret
	ServerName    string
}

func (m tlsMaterial) isZero() bool {
	return m.CA == "" && m.Cert == "" && m.Key == "" && m.ServerName == ""
}

// config builds a *tls.Config that always verifies the server.
func (m tlsMaterial) config() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: m.ServerName}
	if m.CA != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(m.CA.Reveal())) {
			return nil, fmt.Errorf("%w: the CA certificate field holds no PEM certificate", errBadInput)
		}
		cfg.RootCAs = pool
	}
	if (m.Cert == "") != (m.Key == "") {
		return nil, fmt.Errorf("%w: client certificate and key must be set together", errBadInput)
	}
	if m.Cert != "" {
		// X509KeyPair's errors describe the PEM structure, never key bytes.
		pair, err := tls.X509KeyPair([]byte(m.Cert.Reveal()), []byte(m.Key.Reveal()))
		if err != nil {
			return nil, fmt.Errorf("%w: client certificate: %v", errBadInput, err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

// httpOpts configures newHTTPClient.
type httpOpts struct {
	BaseURL  string
	Username string
	Password netsec.Secret
	Bearer   netsec.Secret // sent as Authorization: Bearer; exclusive with Username
	Header   map[string]netsec.Secret
	TLS      tlsMaterial
	Dial     dialFunc
	Timeout  time.Duration
}

// newHTTPClient builds a client that dials through the guard, never uses an
// environment proxy (it would bypass the guard), never follows redirects (a
// redirect could point at an internal address), and sends credentials only
// to the base URL's scheme and host. It refuses credentials over http://,
// like netsec.HTTP.
func newHTTPClient(o httpOpts) (*http.Client, *url.URL, error) {
	u, err := url.Parse(strings.TrimRight(o.BaseURL, "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, nil, fmt.Errorf("%w: URL must be http(s)://host[:port]", errBadInput)
	}
	if u.User != nil {
		return nil, nil, fmt.Errorf("%w: put the login in the username and password fields, not in the URL", errBadInput)
	}
	if (o.Username == "") != (o.Password == "") {
		return nil, nil, fmt.Errorf("%w: basic auth needs both a username and a password", errBadInput)
	}
	hasCreds := o.Username != "" || o.Bearer != "" || len(o.Header) > 0
	if u.Scheme == "http" {
		if hasCreds {
			return nil, nil, fmt.Errorf("%w: a login over http:// sends it in clear text; use https://", errBadInput)
		}
		if !o.TLS.isZero() {
			return nil, nil, fmt.Errorf("%w: TLS certificates are set but the URL is http://", errBadInput)
		}
	}
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, nil, fmt.Errorf("integrations: http.DefaultTransport is %T", http.DefaultTransport)
	}
	tr := base.Clone()
	tr.Proxy = nil
	if o.Dial != nil {
		tr.DialContext = o.Dial
	}
	if u.Scheme == "https" {
		cfg, err := o.TLS.config()
		if err != nil {
			return nil, nil, err
		}
		tr.TLSClientConfig = cfg
	}
	var rt http.RoundTripper = tr
	if hasCreds {
		rt = &authRT{next: tr, scheme: u.Scheme, host: u.Host, o: o}
	}
	timeout := o.Timeout
	if timeout <= 0 {
		timeout = netsec.DefaultHTTPTimeout
	}
	return &http.Client{
		Transport:     rt,
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, u, nil
}

type authRT struct {
	next         http.RoundTripper
	scheme, host string
	o            httpOpts
}

func (a *authRT) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != a.scheme || req.URL.Host != a.host {
		return a.next.RoundTrip(req)
	}
	r := req.Clone(req.Context())
	switch {
	case a.o.Username != "":
		r.SetBasicAuth(a.o.Username, a.o.Password.Reveal())
	case a.o.Bearer != "":
		r.Header.Set("Authorization", "Bearer "+a.o.Bearer.Reveal())
	}
	for k, v := range a.o.Header {
		r.Header.Set(k, v.Reveal())
	}
	return a.next.RoundTrip(r)
}

// getJSON GETs path under base and returns the status and at most maxBody
// bytes of the body. The body is never echoed into a check.
func getBody(ctx context.Context, c *http.Client, base *url.URL, path string, accept string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String()+path, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("integrations: building request: %w", err)
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("integrations: reading response: %w", err)
	}
	return resp.StatusCode, body, nil
}

// isTLSError reports whether err comes from the TLS handshake or certificate
// verification.
func isTLSError(err error) bool {
	var (
		cv  *tls.CertificateVerificationError
		rh  tls.RecordHeaderError
		ua  x509.UnknownAuthorityError
		he  x509.HostnameError
		ci  x509.CertificateInvalidError
		alt tls.AlertError
	)
	return errors.As(err, &cv) || errors.As(err, &rh) || errors.As(err, &ua) ||
		errors.As(err, &he) || errors.As(err, &ci) || errors.As(err, &alt)
}

// httpFailure turns a failed request into a check outcome with a fix.
func httpFailure(err error, what string) outcome {
	switch {
	case errors.Is(err, ErrBlockedHost):
		return fail(errText(err), "Use a host allowed by PIPEROOM_INTEGRATION_ALLOWED_HOSTS, or ask the Piperoom operator to add it.")
	case isTLSError(err):
		return fail("TLS handshake failed: "+errText(err), "Paste the CA certificate that signs "+what+"'s certificate into the CA field, or fix the host name.")
	default:
		return fail("cannot reach "+what+": "+errText(err), "Allow HTTPS from the Piperoom server to "+what+" and check the URL.")
	}
}
