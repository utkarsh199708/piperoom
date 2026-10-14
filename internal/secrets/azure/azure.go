// Package azure is the Azure Key Vault secret provider
// (docs/design/secret-providers.md §3). It speaks the Key Vault REST API
// (api-version 7.4) and Entra ID's token endpoints with net/http, so no
// Azure SDK module is needed for three GET calls and one POST.
//
// References look like secret://<id>/<secret-name>[#key][?version=<32 hex>].
// The path is a Key Vault secret name; the version is a Key Vault version id.
package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
)

// APIVersion is the Key Vault data-plane API version Piperoom speaks.
const APIVersion = "7.4"

// Options configure an Azure Key Vault provider.
type Options struct {
	// VaultURL is the vault, e.g. https://acme-prod.vault.azure.net.
	VaultURL string
	// Auth is one of the Auth* consts; "" picks: client secret when one is
	// given, else workload identity when a federated token file is set (or
	// AZURE_FEDERATED_TOKEN_FILE), else managed identity.
	Auth string
	// TenantID and ClientID default to AZURE_TENANT_ID and AZURE_CLIENT_ID.
	// For managed identity, ClientID picks a user-assigned identity.
	TenantID string
	ClientID string
	// ClientSecret, or ClientSecretFile (re-read on every login), for AuthClientSecret.
	ClientSecret     netsec.Secret
	ClientSecretFile string
	// FederatedTokenFile defaults to AZURE_FEDERATED_TOKEN_FILE.
	FederatedTokenFile string
	// AuthorityHost defaults to AZURE_AUTHORITY_HOST, then DefaultAuthorityHost.
	AuthorityHost string
	// Resource is the token audience; default derived from the vault host
	// (https://vault.azure.net for *.vault.azure.net, the sovereign-cloud
	// equivalent otherwise).
	Resource string
	// TLS and CAPEM verify the vault (a private endpoint with a private CA).
	TLS   netsec.TLS
	CAPEM string
	// HTTPClient, when set, is used for Key Vault calls (for example one
	// whose dialer enforces PIPEROOM_INTEGRATION_ALLOWED_HOSTS). Redirects
	// are refused either way.
	HTTPClient *http.Client
	// IdentityHTTPClient, when set, is used for token calls. The default is
	// a plain client: the guarded integration dialer blocks the metadata
	// address managed identity needs.
	IdentityHTTPClient *http.Client
	// IMDSEndpoint overrides the managed identity endpoint (tests only).
	IMDSEndpoint string
	Timeout      time.Duration
	// Getenv defaults to os.Getenv.
	Getenv func(string) string
	// Now defaults to time.Now (tests).
	Now func() time.Time
}

// Provider reads secrets from one Azure Key Vault. It is safe for
// concurrent use.
type Provider struct {
	base   *url.URL
	hc     *http.Client
	idc    *http.Client
	imdsc  *http.Client
	tokens *tokenSource
	closed atomic.Bool
}

var (
	// secretNameRe is Key Vault's rule for secret names.
	secretNameRe = regexp.MustCompile(`^[0-9a-zA-Z-]{1,127}$`)
	// versionRe matches Key Vault version ids.
	versionRe = regexp.MustCompile(`^[0-9a-fA-F]{32}$`)
	// tenantRe accepts GUIDs and verified domains (contoso.onmicrosoft.com).
	tenantRe = regexp.MustCompile(`^[0-9a-zA-Z][0-9a-zA-Z.-]{0,253}$`)
)

// New validates opts and builds the provider. It does not call Azure.
func New(opts Options) (*Provider, error) {
	getenv := opts.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	base, err := checkAddress(opts.VaultURL, "azure key vault")
	if err != nil {
		return nil, err
	}
	hc := opts.HTTPClient
	if hc == nil {
		if hc, err = newClient(opts.TLS, opts.CAPEM, opts.Timeout); err != nil {
			return nil, err
		}
	} else {
		hc = noRedirects(hc)
	}
	idc := opts.IdentityHTTPClient
	imdsc := idc
	if idc == nil {
		if idc, err = newClient(netsec.TLS{}, "", opts.Timeout); err != nil {
			return nil, err
		}
		// IMDS is link-local: a corporate HTTPS_PROXY must never see it.
		tr := idc.Transport.(*http.Transport).Clone()
		tr.Proxy = nil
		imdsc = &http.Client{Transport: tr, Timeout: idc.Timeout, CheckRedirect: idc.CheckRedirect}
	} else {
		idc = noRedirects(idc)
		imdsc = idc
	}

	resource := strings.TrimRight(opts.Resource, "/")
	if resource == "" {
		resource = resourceFor(base.Hostname())
	} else if _, err := checkAddress(resource, "azure resource"); err != nil {
		return nil, err
	}
	authority := firstNonEmpty(opts.AuthorityHost, getenv("AZURE_AUTHORITY_HOST"), DefaultAuthorityHost)
	au, err := checkAddress(authority, "azure authority host")
	if err != nil {
		return nil, err
	}
	f := tokenFetchers{
		hc:            idc,
		imdsHC:        imdsc,
		authority:     au.String(),
		tenantID:      firstNonEmpty(opts.TenantID, getenv("AZURE_TENANT_ID")),
		clientID:      firstNonEmpty(opts.ClientID, getenv("AZURE_CLIENT_ID")),
		scope:         resource + "/.default",
		resource:      resource,
		assertionFile: firstNonEmpty(opts.FederatedTokenFile, getenv("AZURE_FEDERATED_TOKEN_FILE")),
		imdsEndpoint:  firstNonEmpty(opts.IMDSEndpoint, defaultIMDSEndpoint),
		now:           now,
	}
	mode := opts.Auth
	if mode == "" {
		switch {
		case opts.ClientSecret != "" || opts.ClientSecretFile != "":
			mode = AuthClientSecret
		case f.assertionFile != "":
			mode = AuthWorkloadIdentity
		default:
			mode = AuthManagedIdentity
		}
	}
	ts := &tokenSource{now: now, mode: mode}
	switch mode {
	case AuthManagedIdentity:
		ts.fetch = f.managedIdentity
	case AuthWorkloadIdentity, AuthClientSecret:
		if !tenantRe.MatchString(f.tenantID) || f.clientID == "" {
			return nil, fmt.Errorf("%w: azure %s login needs tenant_id and client_id", secrets.ErrBadProviderConfig, mode)
		}
		if mode == AuthWorkloadIdentity {
			if f.assertionFile == "" {
				return nil, fmt.Errorf("%w: azure workload identity needs federated_token_file or AZURE_FEDERATED_TOKEN_FILE", secrets.ErrBadProviderConfig)
			}
			ts.fetch = f.clientCredentials(true)
			break
		}
		switch {
		case opts.ClientSecret != "" && opts.ClientSecretFile != "":
			return nil, fmt.Errorf("%w: give client_secret or client_secret_file, not both", secrets.ErrBadProviderConfig)
		case opts.ClientSecret != "":
			s := opts.ClientSecret
			f.clientSecret = func() (netsec.Secret, error) { return s, nil }
		case opts.ClientSecretFile != "":
			path := opts.ClientSecretFile
			f.clientSecret = func() (netsec.Secret, error) { return readSmallFile(path) }
		default:
			return nil, fmt.Errorf("%w: azure client_secret login needs client_secret or client_secret_file", secrets.ErrBadProviderConfig)
		}
		ts.fetch = f.clientCredentials(false)
	default:
		return nil, fmt.Errorf("%w: azure auth must be %s, %s or %s", secrets.ErrBadProviderConfig,
			AuthManagedIdentity, AuthWorkloadIdentity, AuthClientSecret)
	}
	return &Provider{base: base, hc: hc, idc: idc, imdsc: imdsc, tokens: ts}, nil
}

// resourceFor maps a vault host to its cloud's Key Vault audience.
func resourceFor(host string) string {
	if _, suffix, ok := strings.Cut(host, ".vault."); ok && suffix != "" {
		return "https://vault." + suffix
	}
	return "https://vault.azure.net"
}

// String names the provider; it holds no secret.
func (p *Provider) String() string { return "azure key vault provider " + p.base.Host }

// GoString keeps %#v to the String form (the cached token is unexported).
func (p *Provider) GoString() string { return p.String() }

// Close drops the cached token and idle connections. Later calls fail with
// secrets.ErrClosed.
func (p *Provider) Close() {
	if p.closed.Swap(true) {
		return
	}
	p.tokens.invalidate()
	p.hc.CloseIdleConnections()
	p.idc.CloseIdleConnections()
	p.imdsc.CloseIdleConnections()
}

// get sends an authenticated GET. After a 401 the cached token is dropped
// and the call is retried once with a fresh one (a revoked or rotated
// credential).
func (p *Provider) get(ctx context.Context, u string, out any) error {
	if p.closed.Load() {
		return secrets.ErrClosed
	}
	for attempt := 0; ; attempt++ {
		tok, err := p.tokens.token(ctx)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return fmt.Errorf("azure: building request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Authorization", "Bearer "+tok.Reveal())
		err = do(p.hc, req, out, kvErrMsg)
		if attempt == 0 && errors.Is(err, secrets.ErrLogin) {
			p.tokens.invalidate()
			continue
		}
		return err
	}
}

func (p *Provider) secretURL(name, version string) string {
	u := p.base.String() + "/secrets/" + url.PathEscape(name)
	if version != "" {
		u += "/" + url.PathEscape(version)
	}
	return u + "?api-version=" + APIVersion
}

// Resolve reads one secret version (latest when ref.Version is "").
func (p *Provider) Resolve(ctx context.Context, ref secrets.Ref) (secrets.Value, error) {
	if !secretNameRe.MatchString(ref.Path) {
		return secrets.Value{}, fmt.Errorf("%w: an azure key vault path is a secret name (letters, digits and '-')", secrets.ErrInvalidRef)
	}
	if ref.Version != "" && !versionRe.MatchString(ref.Version) {
		return secrets.Value{}, fmt.Errorf("%w: an azure key vault version is a 32-character hex id", secrets.ErrInvalidRef)
	}
	ctx, cancel := withDefaultDeadline(ctx)
	defer cancel()
	var out struct {
		Value *string `json:"value"`
		ID    string  `json:"id"`
	}
	if err := p.get(ctx, p.secretURL(ref.Path, ref.Version), &out); err != nil {
		return secrets.Value{}, fmt.Errorf("secrets: azure key vault: reading %s: %w", ref.Path, err)
	}
	if out.Value == nil {
		return secrets.Value{}, fmt.Errorf("%w: %s has no value", secrets.ErrNotFound, ref.Path)
	}
	return secrets.Value{Data: netsec.Secret(*out.Value), Version: lastSegment(out.ID)}, nil
}

// maxListPages bounds List so a huge vault cannot keep a request busy.
const maxListPages = 40

// List returns secret names starting with prefix (case-insensitive, as Key
// Vault names are). It reads metadata pages only, never values.
func (p *Provider) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	ctx, cancel := withDefaultDeadline(ctx)
	defer cancel()
	if limit <= 0 {
		limit = 200
	}
	lp := strings.ToLower(prefix)
	next := p.base.String() + "/secrets?api-version=" + APIVersion + "&maxresults=25"
	out := []string{}
	for page := 0; next != "" && page < maxListPages && len(out) < limit; page++ {
		var body struct {
			Value []struct {
				ID string `json:"id"`
			} `json:"value"`
			NextLink string `json:"nextLink"`
		}
		if err := p.get(ctx, next, &body); err != nil {
			if errors.Is(err, secrets.ErrAccessDenied) {
				return nil, fmt.Errorf("%w: %w", secrets.ErrListUnsupported, err)
			}
			return nil, fmt.Errorf("secrets: azure key vault: listing: %w", err)
		}
		for _, s := range body.Value {
			if name := lastSegment(s.ID); name != "" && strings.HasPrefix(strings.ToLower(name), lp) {
				out = append(out, name)
			}
		}
		var err error
		if next, err = p.sameOrigin(body.NextLink); err != nil {
			return nil, err
		}
	}
	sort.Strings(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// sameOrigin accepts a nextLink only on the vault's own scheme and host,
// so a tampered page cannot send the bearer token elsewhere.
func (p *Provider) sameOrigin(link string) (string, error) {
	if link == "" {
		return "", nil
	}
	u, err := url.Parse(link)
	if err != nil || u.Scheme != p.base.Scheme || u.Host != p.base.Host {
		return "", fmt.Errorf("secrets: azure key vault: listing: nextLink points outside the vault")
	}
	return link, nil
}

// Keys returns the fields of a JSON secret, [] for a plain one.
func (p *Provider) Keys(ctx context.Context, ref secrets.Ref) ([]string, error) {
	ref.Key = ""
	v, err := p.Resolve(ctx, ref)
	if err != nil {
		return nil, err
	}
	return secrets.KeysOf(v.Data), nil
}

// Check runs the card's test: reach the vault, log in, list (for the
// picker), read the sample. Azure cannot say whether an identity has extra
// rights without reading role assignments, so least_privilege is skipped.
func (p *Provider) Check(ctx context.Context, sample *secrets.Ref) []spec.Check {
	const (
		reachTitle = "Can reach Azure Key Vault"
		loginTitle = "Piperoom can log in to Entra ID"
		listTitle  = "Piperoom can list names for the picker"
		tooTitle   = "Piperoom has no more access than it needs"
	)
	ctx, cancelAll := context.WithTimeout(ctx, checkBudget)
	defer cancelAll()
	tooSkipped := skipped(secrets.CheckTooMuch, tooTitle,
		"Azure cannot say whether an identity has extra rights without Microsoft.Authorization/roleAssignments/read")
	if p.closed.Load() {
		return []spec.Check{fail(secrets.CheckReach, reachTitle, secrets.ErrClosed.Error(), "")}
	}

	// Reach: an unauthenticated call. Key Vault answers 401 with a
	// challenge, which proves TLS and the host without a token.
	if err := p.probe(ctx); err != nil {
		return []spec.Check{
			fail(secrets.CheckReach, reachTitle, errorDetail(err), "check the vault URL, its firewall or private endpoint, and outbound HTTPS"),
			skipped(secrets.CheckLogin, loginTitle, "not reachable"),
			skipped(secrets.CheckRead, sampleTitle, "not reachable"),
			tooSkipped,
		}
	}
	out := []spec.Check{pass(secrets.CheckReach, reachTitle, p.base.Host)}

	cctx, cancel := context.WithTimeout(ctx, checkTimeout)
	_, err := p.tokens.token(cctx)
	cancel()
	if err != nil {
		return append(out,
			fail(secrets.CheckLogin, loginTitle, errorDetail(err), p.loginFix()),
			skipped(secrets.CheckRead, sampleTitle, "login failed"),
			tooSkipped)
	}

	cctx, cancel = context.WithTimeout(ctx, checkTimeout)
	err = p.get(cctx, p.base.String()+"/secrets?api-version="+APIVersion+"&maxresults=1", nil)
	cancel()
	switch {
	case err == nil:
		out = append(out, pass(secrets.CheckLogin, loginTitle, p.tokens.mode))
	case errors.Is(err, secrets.ErrLogin):
		return append(out,
			fail(secrets.CheckLogin, loginTitle, "Key Vault rejected the token: "+errorDetail(err),
				"use the vault's own tenant and the https://vault.azure.net audience (or the sovereign-cloud one)"),
			skipped(secrets.CheckRead, sampleTitle, "login failed"),
			tooSkipped)
	case errors.Is(err, secrets.ErrAccessDenied):
		out = append(out, pass(secrets.CheckLogin, loginTitle, p.tokens.mode),
			warn(secrets.CheckListNames, listTitle, "denied: "+errorDetail(err),
				"the Key Vault Secrets User role includes list; with access policies grant List, or type references by hand"))
	default:
		out = append(out, pass(secrets.CheckLogin, loginTitle, p.tokens.mode),
			warn(secrets.CheckListNames, listTitle, errorDetail(err), ""))
	}
	return append(out, sampleCheck(ctx, p, sample), tooSkipped)
}

// probe sends one unauthenticated request; any answer but a 5xx counts.
func (p *Provider) probe(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, p.base.String()+"/secrets?api-version="+APIVersion+"&maxresults=1", nil)
	if err != nil {
		return fmt.Errorf("azure: building request: %w", err)
	}
	resp, err := p.hc.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", secrets.ErrUnreachable, err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("%w: HTTP %d", secrets.ErrUnreachable, resp.StatusCode)
	}
	return nil
}

func (p *Provider) loginFix() string {
	switch p.tokens.mode {
	case AuthManagedIdentity:
		return "assign a managed identity to the VM or node pool (and set client_id for a user-assigned one), or use workload identity"
	case AuthWorkloadIdentity:
		return "add a federated credential for Piperoom's issuer and subject to the app registration, and check tenant_id and client_id"
	default:
		return "check tenant_id, client_id and that the client secret has not expired"
	}
}

func lastSegment(id string) string {
	id = strings.TrimRight(id, "/")
	if i := strings.LastIndexByte(id, '/'); i >= 0 {
		return id[i+1:]
	}
	return id
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s = strings.TrimSpace(s); s != "" {
			return s
		}
	}
	return ""
}

// readSmallFile reads a small credential file (a client secret, a
// federated JWT), trimmed.
func readSmallFile(path string) (netsec.Secret, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("azure: reading %s: %w", path, err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 64<<10))
	if err != nil {
		return "", fmt.Errorf("azure: reading %s: %w", path, err)
	}
	return netsec.Secret(strings.TrimSpace(string(b))), nil
}

func asStatus(err error, target **statusError) bool { return errors.As(err, target) }
