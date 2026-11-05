package connprobe

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"piperoom/internal/clients/netsec"
)

// maxHTTPBody caps every response a probe reads (AGENTS.md rule 7).
const maxHTTPBody = 4 << 20

// errBodyTooLarge is returned when a response exceeds maxHTTPBody.
var errBodyTooLarge = errors.New("connprobe: response larger than 4 MB")

// httpService names one REST service and the settings that configure it, so
// problem and fix texts can name the setting to change.
type httpService struct {
	Name    string // "Kafka Connect", "Schema Registry"
	URLKey  string // CONNECT_URL
	UserKey string // CONNECT_USERNAME
	PassKey string // CONNECT_PASSWORD
	TLSKeys string // "CONNECT_TLS_CA_FILE, CONNECT_TLS_CERT_FILE and CONNECT_TLS_KEY_FILE"
	CAKey   string // CONNECT_TLS_CA_FILE
	Accept  string // Accept header
}

// httpProbe is a throwaway REST client whose every dial goes through the guard.
type httpProbe struct {
	svc    httpService
	base   *url.URL
	sec    netsec.HTTP
	client *http.Client
	tr     *http.Transport
}

// newHTTPProbe validates the settings with netsec's own rules (the ones the
// server's client applies) and builds a client that dials through g. A
// non-empty Check is the failed config check.
func newHTTPProbe(svc httpService, raw string, sec netsec.HTTP, g *Guard) (*httpProbe, *Check) {
	fail := func(problem, fix string) (*httpProbe, *Check) {
		return nil, &Check{ID: "config", Status: StatusFail, Problem: problem, Fix: fix}
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fail(svc.URLKey+" is not set.", "Set "+svc.URLKey+" to the "+svc.Name+" REST URL, e.g. https://host:port.")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fail(svc.URLKey+" is not an http(s)://host:port URL.", "Set "+svc.URLKey+" to the "+svc.Name+" REST URL, e.g. https://host:port.")
	}
	if u.User != nil {
		return fail(svc.URLKey+" carries a user name or password.", "Remove them from the URL and set "+svc.UserKey+" and "+svc.PassKey+" instead.")
	}
	hasUser, hasPass := sec.Username != "", sec.Password != ""
	if hasUser != hasPass {
		return fail("Only one of "+svc.UserKey+" and "+svc.PassKey+" is set.", "Set both or neither.")
	}
	if u.Scheme == "http" && hasUser {
		return fail("A login over http:// would send the password in clear text.", "Use an https:// URL in "+svc.URLKey+".")
	}
	if u.Scheme == "http" && !sec.TLS.IsZero() {
		return fail("TLS files are set but "+svc.URLKey+" is http://.", "Use an https:// URL, or unset "+svc.TLSKeys+".")
	}
	// netsec is the server's own validation; it must accept what we accept.
	if _, err := sec.Client(raw, DefaultStepTimeout); err != nil {
		if u.Scheme == "https" {
			return fail("The "+svc.Name+" TLS settings cannot be used.", "Check "+svc.TLSKeys+": readable PEM files, certificate and key set together.")
		}
		return fail("The "+svc.Name+" settings are refused by the client builder.", "Check "+svc.URLKey+", "+svc.UserKey+" and "+svc.PassKey+".")
	}

	tr := &http.Transport{
		// No proxy: a proxy would dial for us and bypass the guard.
		Proxy:                 nil,
		DialContext:           g.DialContext,
		TLSHandshakeTimeout:   DefaultStepTimeout,
		ResponseHeaderTimeout: DefaultStepTimeout,
		MaxIdleConns:          2,
		IdleConnTimeout:       30 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	if u.Scheme == "https" {
		cfg, err := sec.TLS.Config()
		if err != nil {
			return fail("The "+svc.Name+" TLS settings cannot be used.", "Check "+svc.TLSKeys+": readable PEM files, certificate and key set together.")
		}
		tr.TLSClientConfig = cfg
	}
	return &httpProbe{
		svc:  svc,
		base: u,
		sec:  sec,
		tr:   tr,
		client: &http.Client{
			Transport: tr,
			Timeout:   DefaultStepTimeout,
			// Redirects are not followed: the login goes only to the
			// configured host, and a redirect target would skip CheckURL.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

// close releases idle connections.
func (h *httpProbe) close() { h.tr.CloseIdleConnections() }

// hasLogin reports whether basic auth is configured.
func (h *httpProbe) hasLogin() bool { return h.sec.Username != "" }

// get sends GET path (already escaped) and returns the status and the body,
// read up to maxHTTPBody. A body over the cap is errBodyTooLarge.
func (h *httpProbe) get(ctx context.Context, path string) (int, []byte, error) {
	if err := netsec.CheckPath(path); err != nil {
		return 0, nil, fmt.Errorf("connprobe: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
	defer cancel()
	u := *h.base
	u.Path = strings.TrimSuffix(u.Path, "/")
	u.RawPath = ""
	full := u.String() + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, full, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("connprobe: build request: %w", err)
	}
	req.Header.Set("Accept", h.svc.Accept)
	if h.hasLogin() {
		req.SetBasicAuth(h.sec.Username, h.sec.Password.Reveal())
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("connprobe: GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPBody+1))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("connprobe: read %s: %w", path, err)
	}
	if len(b) > maxHTTPBody {
		return resp.StatusCode, nil, errBodyTooLarge
	}
	return resp.StatusCode, b, nil
}

// reachFail turns a transport error into the failed reach check. The error
// text itself never reaches the result.
func (h *httpProbe) reachFail(err error) Check {
	c := Check{ID: "reach", Status: StatusFail}
	var cve *tls.CertificateVerificationError
	var uae x509.UnknownAuthorityError
	var hne x509.HostnameError
	var cie x509.CertificateInvalidError
	var rhe tls.RecordHeaderError
	switch {
	case errors.Is(err, ErrBlockedAddress):
		c.Problem = "The " + h.svc.Name + " address is not allowed: it is a link-local, metadata or otherwise blocked address."
		c.Fix = "Point " + h.svc.URLKey + " at the " + h.svc.Name + " service itself."
	case errors.Is(err, errBodyTooLarge):
		c.Problem = "The " + h.svc.Name + " answer is larger than 4 MB."
		c.Fix = "Check that " + h.svc.URLKey + " points at the " + h.svc.Name + " REST API."
	case errors.As(err, &cve), errors.As(err, &uae), errors.As(err, &hne), errors.As(err, &cie):
		c.Problem = "The " + h.svc.Name + " TLS certificate is not trusted or does not match the host."
		c.Fix = "Set " + h.svc.CAKey + " to the CA that signed the server certificate, and use the host name the certificate names."
	case errors.As(err, &rhe):
		c.Problem = "The " + h.svc.Name + " server did not answer TLS."
		c.Fix = "Use http:// in " + h.svc.URLKey + " if the server has no TLS, or check the port."
	case isTimeout(err):
		c.Problem = "No answer from " + h.svc.Name + " within 10 s."
		c.Fix = "Check " + h.svc.URLKey + " (host and port) and that the network lets Piperoom reach it."
	default:
		c.Problem = "Cannot connect to " + h.svc.Name + "."
		c.Fix = "Check " + h.svc.URLKey + " (host and port) and that the network lets Piperoom reach it."
	}
	return c
}

// isTimeout reports a context deadline or a network timeout.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}

// checkURL runs the guard on the base URL before the first request.
func (h *httpProbe) checkURL(ctx context.Context, g *Guard) error {
	ctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
	defer cancel()
	return g.CheckURL(ctx, h.base.String())
}

// loginFail is the failed login check for a 401 or 403 answer to path.
func (h *httpProbe) loginFail(status int, path string) Check {
	c := Check{ID: "login", Status: StatusFail}
	switch {
	case status == http.StatusUnauthorized && h.hasLogin():
		c.Problem = h.svc.Name + " refused the user name or password."
		c.Fix = "Check " + h.svc.UserKey + " and " + h.svc.PassKey + "."
	case status == http.StatusUnauthorized:
		c.Problem = h.svc.Name + " needs a login."
		c.Fix = "Set " + h.svc.UserKey + " and " + h.svc.PassKey + " (with an https:// URL)."
	default:
		c.Problem = fmt.Sprintf("The login may not call GET %s (HTTP %d).", path, status)
		c.Fix = "Allow the read-only GET calls listed in docs/pilot/permissions.md for this login, or check " + h.svc.UserKey + " and " + h.svc.PassKey + "."
	}
	return c
}

// statusFail describes an unexpected status for path in one sentence.
func (h *httpProbe) statusFail(id string, status int, path string) Check {
	c := Check{ID: id, Status: StatusFail}
	switch {
	case status >= 500:
		c.Problem = fmt.Sprintf("%s answered GET %s with a server error (HTTP %d).", h.svc.Name, path, status)
		c.Fix = "Check the " + h.svc.Name + " service health and its logs."
	case status == http.StatusNotFound:
		c.Problem = fmt.Sprintf("GET %s answered 404: this URL is not the %s REST API.", path, h.svc.Name)
		c.Fix = "Check " + h.svc.URLKey + " (scheme, host, port, no extra path)."
	default:
		c.Problem = fmt.Sprintf("Unexpected answer to GET %s (HTTP %d).", path, status)
		c.Fix = "Check " + h.svc.URLKey + "; redirects are not followed."
	}
	return c
}

// isAuthStatus reports 401 or 403.
func isAuthStatus(s int) bool { return s == http.StatusUnauthorized || s == http.StatusForbidden }

// loginOK is the login check after a successful authenticated request.
func (h *httpProbe) loginOK() Check {
	if h.hasLogin() {
		return Check{ID: "login", Status: StatusOK, Detail: "basic auth accepted"}
	}
	return Check{ID: "login", Status: StatusOK, Detail: "no login configured; the server asked for none"}
}
