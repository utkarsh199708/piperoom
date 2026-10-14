package azure

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/secrets"
)

// Ways Piperoom logs in to Entra ID (Options.Auth).
const (
	// AuthManagedIdentity asks the instance metadata service (IMDS) of the
	// VM, VM scale set or AKS node pool Piperoom runs on.
	AuthManagedIdentity = "managed_identity"
	// AuthWorkloadIdentity exchanges a federated token file (AKS workload
	// identity, or any OIDC issuer the app registration trusts) for an
	// access token. This is the hosted-mode path: the customer trusts
	// Piperoom's issuer and can revoke it.
	AuthWorkloadIdentity = "workload_identity"
	// AuthClientSecret uses an app registration's client secret, the
	// fallback when no workload identity is available.
	AuthClientSecret = "client_secret"
)

const (
	// DefaultAuthorityHost is the Entra ID host of the public cloud.
	DefaultAuthorityHost = "https://login.microsoftonline.com"
	// defaultIMDSEndpoint is the fixed, link-local managed identity endpoint.
	defaultIMDSEndpoint = "http://169.254.169.254/metadata/identity/oauth2/token"
	imdsAPIVersion      = "2018-02-01"
	// refreshBefore renews a cached token this long before it expires.
	refreshBefore = 2 * time.Minute
)

// tokenSource fetches and caches one Entra ID access token for Key Vault.
type tokenSource struct {
	mu  sync.Mutex
	tok netsec.Secret
	exp time.Time
	// inflight is non-nil while a login runs; it is closed when that
	// login ends, so waiters can give up on their own deadline instead of
	// queueing on the mutex behind another caller's network call.
	inflight chan struct{}
	gen      uint64 // bumped by invalidate, so a stale login is not cached
	now      func() time.Time
	fetch    func(ctx context.Context) (netsec.Secret, time.Time, error)
	// mode names the login method in errors and check fixes.
	mode string
}

// token returns the cached token, fetching a new one when it is missing
// or close to expiry. Concurrent callers share one login; each waiter
// keeps to its own ctx. The login runs on the caller's goroutine under
// the caller's ctx, so no goroutine outlives a request.
func (t *tokenSource) token(ctx context.Context) (netsec.Secret, error) {
	for {
		t.mu.Lock()
		if t.tok != "" && t.now().Before(t.exp.Add(-refreshBefore)) {
			tok := t.tok
			t.mu.Unlock()
			return tok, nil
		}
		if wait := t.inflight; wait != nil {
			t.mu.Unlock()
			select {
			case <-wait:
				// The other login ended. Use its token, or, if it failed
				// (perhaps only because its caller's deadline was
				// shorter), try again ourselves while ctx allows.
				continue
			case <-ctx.Done():
				return "", fmt.Errorf("%w: waiting for login: %w", secrets.ErrUnreachable, ctx.Err())
			}
		}
		if err := ctx.Err(); err != nil {
			t.mu.Unlock()
			return "", fmt.Errorf("%w: before login: %w", secrets.ErrUnreachable, err)
		}
		done := make(chan struct{})
		t.inflight = done
		gen := t.gen
		t.mu.Unlock()

		tok, exp, err := t.fetch(ctx)

		t.mu.Lock()
		t.inflight = nil
		if err == nil && gen == t.gen {
			t.tok, t.exp = tok, exp
		}
		t.mu.Unlock()
		close(done)
		if err != nil {
			return "", err
		}
		return tok, nil
	}
}

// invalidate drops the cached token (Key Vault answered 401 with it).
func (t *tokenSource) invalidate() {
	t.mu.Lock()
	t.tok, t.exp = "", time.Time{}
	t.gen++
	t.mu.Unlock()
}

// tokenResponse covers both Entra ID v2 (expires_in is a number) and IMDS
// (expires_in is a string).
type tokenResponse struct {
	AccessToken string          `json:"access_token"`
	ExpiresIn   json.RawMessage `json:"expires_in"`
}

func (r tokenResponse) expiry(now time.Time) time.Time {
	s := strings.Trim(string(r.ExpiresIn), `"`)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		// Unknown lifetime: Entra ID tokens live at least an hour; renew
		// well before that.
		return now.Add(30 * time.Minute)
	}
	return now.Add(time.Duration(n) * time.Second)
}

// aadErrMsg reads Entra ID's {"error","error_description"} body. The
// description starts with an AADSTS code, which is what the admin needs.
func aadErrMsg(b []byte) (string, string) {
	var e struct {
		Error       string `json:"error"`
		Description string `json:"error_description"`
	}
	if json.Unmarshal(b, &e) != nil {
		return "", ""
	}
	return e.Error, e.Description
}

// tokenFetchers holds what the three login methods need.
type tokenFetchers struct {
	hc            *http.Client
	imdsHC        *http.Client
	authority     string
	tenantID      string
	clientID      string
	scope         string // "<resource>/.default"
	resource      string // for IMDS
	clientSecret  func() (netsec.Secret, error)
	assertionFile string
	imdsEndpoint  string
	now           func() time.Time
}

// clientCredentials posts the client-credentials grant with either a
// client secret or a federated assertion (read fresh each time: projected
// tokens rotate).
func (f tokenFetchers) clientCredentials(useAssertion bool) func(ctx context.Context) (netsec.Secret, time.Time, error) {
	return func(ctx context.Context) (netsec.Secret, time.Time, error) {
		ctx, cancel := withDefaultDeadline(ctx)
		defer cancel()
		form := url.Values{
			"grant_type": {"client_credentials"},
			"client_id":  {f.clientID},
			"scope":      {f.scope},
		}
		if useAssertion {
			a, err := readSmallFile(f.assertionFile)
			if err != nil {
				return "", time.Time{}, fmt.Errorf("%w: reading the federated token file: %w", secrets.ErrLogin, err)
			}
			form.Set("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
			form.Set("client_assertion", a.Reveal())
		} else {
			s, err := f.clientSecret()
			if err != nil {
				return "", time.Time{}, fmt.Errorf("%w: reading the client secret: %w", secrets.ErrLogin, err)
			}
			form.Set("client_secret", s.Reveal())
		}
		u := f.authority + "/" + url.PathEscape(f.tenantID) + "/oauth2/v2.0/token"
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(form.Encode()))
		if err != nil {
			return "", time.Time{}, fmt.Errorf("azure: building token request: %w", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")
		var tr tokenResponse
		if err := do(f.hc, req, &tr, aadErrMsg); err != nil {
			return "", time.Time{}, tokenErr("entra id", err)
		}
		if tr.AccessToken == "" {
			return "", time.Time{}, fmt.Errorf("%w: entra id returned no access token", secrets.ErrLogin)
		}
		return netsec.Secret(tr.AccessToken), tr.expiry(f.now()), nil
	}
}

// managedIdentity asks IMDS. client_id picks a user-assigned identity.
func (f tokenFetchers) managedIdentity(ctx context.Context) (netsec.Secret, time.Time, error) {
	ctx, cancel := withDefaultDeadline(ctx)
	defer cancel()
	q := url.Values{"api-version": {imdsAPIVersion}, "resource": {f.resource}}
	if f.clientID != "" {
		q.Set("client_id", f.clientID)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.imdsEndpoint+"?"+q.Encode(), nil)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("azure: building IMDS request: %w", err)
	}
	req.Header.Set("Metadata", "true")
	req.Header.Set("Accept", "application/json")
	var tr tokenResponse
	if err := do(f.imdsHC, req, &tr, aadErrMsg); err != nil {
		return "", time.Time{}, tokenErr("managed identity endpoint", err)
	}
	if tr.AccessToken == "" {
		return "", time.Time{}, fmt.Errorf("%w: managed identity endpoint returned no access token", secrets.ErrLogin)
	}
	return netsec.Secret(tr.AccessToken), tr.expiry(f.now()), nil
}

// tokenErr marks every refused login (400 invalid_client, 401, 403 from
// the identity endpoint) as secrets.ErrLogin; network trouble stays
// secrets.ErrUnreachable.
func tokenErr(who string, err error) error {
	var se *statusError
	if asStatus(err, &se) && se.Status >= 400 && se.Status < 500 && se.Status != http.StatusTooManyRequests {
		return fmt.Errorf("%w: %s: %s", secrets.ErrLogin, who, se.Error())
	}
	return fmt.Errorf("azure: %s: %w", who, err)
}
