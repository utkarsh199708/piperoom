package databricks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	dbsqlauth "github.com/databricks/databricks-sql-go/auth"
	dbsqloauth "github.com/databricks/databricks-sql-go/auth/oauth"
)

// m2mScopeAllAPIs is the scope a Databricks OAuth M2M token for a SQL
// warehouse needs; the driver's own M2M authenticator adds exactly this one.
const m2mScopeAllAPIs = "all-apis"

// m2mExpirySkew refreshes a cached token this much before it expires, so a
// token minted on one call is not handed to the next one already stale.
const m2mExpirySkew = time.Minute

// m2mDefaultLifetime is the expiry assumed when the token endpoint omits
// expires_in. Databricks always sends it, but an absent field must not force
// a refetch on every query.
const m2mDefaultLifetime = 5 * time.Minute

// oauthM2M is an auth.Authenticator that mints Databricks OAuth M2M
// (client-credentials) bearer tokens for a service principal. The token
// fetch goes over one bounded *http.Client (10 s timeout, 4 MB body cap) and
// the result is cached until close to expiry.
type oauthM2M struct {
	clientID     string
	clientSecret string
	tokenURL     string
	scopes       []string
	hc           *http.Client

	mu    sync.Mutex
	token string
	exp   time.Time
}

// newOAuthM2M builds the authenticator. hc must already be bounded (see
// newM2MClient); tokenURL names the workspace's OAuth token endpoint.
func newOAuthM2M(clientID, clientSecret, tokenURL string, hc *http.Client) *oauthM2M {
	return &oauthM2M{
		clientID:     clientID,
		clientSecret: clientSecret,
		tokenURL:     tokenURL,
		scopes:       []string{m2mScopeAllAPIs},
		hc:           hc,
	}
}

// Authenticate adds a valid bearer token to r, fetching a new one over the
// bounded client when the cached one is missing or close to expiry. The
// request's context bounds the fetch on top of the client's own timeout. The
// client secret and token never appear in an error this returns.
func (a *oauthM2M) Authenticate(r *http.Request) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token == "" || !time.Now().Add(m2mExpirySkew).Before(a.exp) {
		token, exp, err := a.fetch(r.Context())
		if err != nil {
			return err
		}
		a.token, a.exp = token, exp
	}
	r.Header.Set("Authorization", "Bearer "+a.token)
	return nil
}

// fetch runs the client-credentials grant (RFC 6749 §4.4) against tokenURL
// with HTTP basic auth, over the bounded client. A bad status, an oversized
// body, an unreadable body and a missing access_token are all errors; the
// body text is never returned, so a token endpoint echoing the secret cannot
// leak it.
func (a *oauthM2M) fetch(ctx context.Context) (string, time.Time, error) {
	form := url.Values{"grant_type": {"client_credentials"}}
	if len(a.scopes) > 0 {
		form.Set("scope", strings.Join(a.scopes, " "))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("databricks: OAuth M2M token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(a.clientID, a.clientSecret)

	resp, err := a.hc.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("databricks: OAuth M2M token fetch: %w", err)
	}
	defer resp.Body.Close()

	// The transport caps the body at maxResponseBytes; read it all so an
	// oversized response fails here rather than being silently truncated.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		if errors.Is(err, ErrResponseTooLarge) {
			return "", time.Time{}, fmt.Errorf("databricks: OAuth M2M token response too large: %w", ErrResponseTooLarge)
		}
		return "", time.Time{}, fmt.Errorf("databricks: OAuth M2M token body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", time.Time{}, fmt.Errorf("databricks: OAuth M2M token endpoint returned HTTP %d", resp.StatusCode)
	}
	var tk m2mTokenJSON
	if err := json.Unmarshal(body, &tk); err != nil {
		return "", time.Time{}, fmt.Errorf("databricks: OAuth M2M token decode: %w", err)
	}
	if tk.AccessToken == "" {
		return "", time.Time{}, errors.New("databricks: OAuth M2M token endpoint returned no access_token")
	}
	exp := time.Now().Add(m2mDefaultLifetime)
	if tk.ExpiresIn > 0 {
		exp = time.Now().Add(time.Duration(tk.ExpiresIn) * time.Second)
	}
	return tk.AccessToken, exp, nil
}

// m2mTokenJSON is the part of the OAuth token response this client reads.
type m2mTokenJSON struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
}

// newM2MClient is the one bounded client the authenticator uses: a 10 s
// timeout and a 4 MB cap on every response body (AGENTS.md §4 rule 7).
func newM2MClient(base http.RoundTripper) *http.Client {
	return &http.Client{
		Timeout:   defaultTimeout,
		Transport: m2mTransport{base: base},
	}
}

// m2mTransport caps the response body at maxResponseBytes; the client's
// Timeout bounds the request as a whole.
type m2mTransport struct {
	base http.RoundTripper
}

func (t m2mTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = &cappedBody{rc: resp.Body, left: maxResponseBytes}
	return resp, nil
}

// resolveM2MTokenURL finds the workspace's OAuth token endpoint for an M2M
// client-credentials grant. It reuses the driver's endpoint resolution (cloud
// inference, Azure tenants, unified/SPOG account-rooted discovery) so this
// client does not reimplement it; only the token fetch itself runs here, over
// the bounded client.
func resolveM2MTokenURL(ctx context.Context, host string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ep, err := dbsqloauth.GetEndpoint(ctx, host)
	if err != nil {
		return "", fmt.Errorf("databricks: OAuth M2M endpoint discovery: %w", err)
	}
	if ep.TokenURL == "" {
		return "", errors.New("databricks: OAuth M2M endpoint discovery returned no token URL")
	}
	return ep.TokenURL, nil
}

// Compile-time assertion: oauthM2M implements the driver's authenticator.
var _ dbsqlauth.Authenticator = (*oauthM2M)(nil)
