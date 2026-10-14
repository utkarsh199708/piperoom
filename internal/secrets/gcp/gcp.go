// Package gcp is the Google Secret Manager secret provider
// (docs/design/secret-providers.md §3). It speaks the Secret Manager REST
// API v1 with net/http and gets tokens from golang.org/x/oauth2/google
// (application default credentials: an attached service account, GKE
// Workload Identity, or workload identity federation configured by the
// operator through GOOGLE_APPLICATION_CREDENTIALS). The gRPC client
// cloud.google.com/go/secretmanager is not used: two GET calls and one POST
// do not justify it.
//
// References look like secret://<id>/<secret>[#key][?version=<n|alias>] or
// secret://<id>/projects/<project>/secrets/<secret>, for a secret in
// another project. "latest" is used when no version is given.
package gcp

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
)

const (
	// DefaultEndpoint is the public Secret Manager API.
	DefaultEndpoint = "https://secretmanager.googleapis.com"
	// DefaultResourceManagerEndpoint answers project-level testIamPermissions.
	DefaultResourceManagerEndpoint = "https://cloudresourcemanager.googleapis.com"
	// Scope is the OAuth scope Secret Manager requires.
	Scope = "https://www.googleapis.com/auth/cloud-platform"
)

// Options configure a Google Secret Manager provider.
type Options struct {
	// Project is the default project id or number for short paths.
	Project string
	// Endpoint overrides DefaultEndpoint (a regional or Private Service
	// Connect endpoint; tests).
	Endpoint string
	// ResourceManagerEndpoint overrides DefaultResourceManagerEndpoint.
	ResourceManagerEndpoint string
	// CredentialsFile is a service account key file set by the operator.
	// Only the service_account type is accepted: other credential files
	// (external_account, impersonation) can point token requests at any
	// URL, so they are honoured only through GOOGLE_APPLICATION_CREDENTIALS,
	// which the operator controls. Empty = application default credentials.
	CredentialsFile string
	// TokenSource replaces the credentials above with a fixed source
	// (tests). Piperoom cannot rebuild it, so after a 401 the retry gets
	// whatever the source returns next; a caching source returns the same
	// rejected token. Prefer NewTokenSource for real credentials.
	// Token() takes no context: the source must bound its own fetch (an
	// HTTP client with a timeout). At most one fetch per provider runs at
	// a time, so a hanging source holds one goroutine, not one per call.
	TokenSource oauth2.TokenSource
	// NewTokenSource builds the token source on first use and again after
	// Google answers 401, so a revoked or rotated credential is picked up
	// (the hosted per-workspace impersonated source). It overrides
	// TokenSource and the credentials above. The same bounded-fetch rule
	// as for TokenSource applies.
	NewTokenSource func() (oauth2.TokenSource, error)
	// TLS and CAPEM verify the endpoint.
	TLS   netsec.TLS
	CAPEM string
	// HTTPClient, when set, is used for Secret Manager calls (for example
	// a client whose dialer enforces PIPEROOM_INTEGRATION_ALLOWED_HOSTS).
	HTTPClient *http.Client
	// IdentityHTTPClient, when set, fetches tokens. The default is a plain
	// client: the guarded dialer blocks the metadata server.
	IdentityHTTPClient *http.Client
	Timeout            time.Duration
}

// Provider reads one project's (and, by full path, other projects')
// secrets. It is safe for concurrent use.
type Provider struct {
	project   string
	base      string
	rmBase    string
	hc        *http.Client
	idc       *http.Client
	credsFile string
	injected  oauth2.TokenSource
	newTS     func() (oauth2.TokenSource, error)

	// bg is the context token sources keep for refreshes; Close cancels
	// it, which stops any token fetch still running.
	bg       context.Context
	bgCancel context.CancelFunc

	mu     sync.Mutex
	ts     oauth2.TokenSource
	fetch  *tokenCall // the one token fetch in flight, or nil
	closed atomic.Bool
}

var (
	// projectRe accepts project ids, numbers and domain-scoped ids (example.com:proj).
	projectRe = regexp.MustCompile(`^[a-z0-9][a-z0-9.:-]{0,127}$`)
	// secretIDRe is Secret Manager's rule for secret ids.
	secretIDRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,255}$`)
	// versionRe accepts "latest", version numbers and version aliases.
	versionRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,63}$`)
)

// New validates opts and builds the provider. It does not call Google;
// credentials are looked up on first use.
func New(opts Options) (*Provider, error) {
	if !projectRe.MatchString(opts.Project) {
		return nil, fmt.Errorf("%w: google secret manager needs a project id (the provider address)", secrets.ErrBadProviderConfig)
	}
	base, err := checkAddress(firstNonEmpty(opts.Endpoint, DefaultEndpoint), "google secret manager")
	if err != nil {
		return nil, err
	}
	rm, err := checkAddress(firstNonEmpty(opts.ResourceManagerEndpoint, DefaultResourceManagerEndpoint), "google resource manager")
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
	if idc == nil {
		if idc, err = newClient(netsec.TLS{}, "", opts.Timeout); err != nil {
			return nil, err
		}
	} else {
		// A 307/308 would resend the POST body, the signed JWT assertion,
		// to the redirect target.
		idc = noRedirects(idc)
	}
	bg, cancel := context.WithCancel(context.Background())
	return &Provider{
		project: opts.Project, base: base.String(), rmBase: rm.String(), hc: hc, idc: idc,
		credsFile: opts.CredentialsFile, injected: opts.TokenSource, newTS: opts.NewTokenSource, bg: bg, bgCancel: cancel,
	}, nil
}

// String names the provider; it holds no secret.
func (p *Provider) String() string { return "google secret manager provider " + p.project }

// GoString keeps %#v to the String form (the token source is unexported).
func (p *Provider) GoString() string { return p.String() }

// Close stops token refreshes and drops idle connections. Later calls fail
// with secrets.ErrClosed.
func (p *Provider) Close() {
	if p.closed.Swap(true) {
		return
	}
	p.bgCancel()
	p.mu.Lock()
	p.ts = nil
	p.mu.Unlock()
	p.hc.CloseIdleConnections()
	p.idc.CloseIdleConnections()
}

// tokenSource returns the cached source, building it on first use. Only
// runFetch calls it, one fetch at a time, so the build runs without p.mu:
// FindDefaultCredentials can probe the metadata server, and callers that
// only check for an in-flight fetch must not wait behind that.
func (p *Provider) tokenSource() (oauth2.TokenSource, error) {
	if p.newTS == nil && p.injected != nil {
		return p.injected, nil
	}
	p.mu.Lock()
	ts := p.ts
	p.mu.Unlock()
	if ts != nil {
		return ts, nil
	}
	ts, err := p.buildSource()
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	if !p.closed.Load() {
		p.ts = ts
	}
	p.mu.Unlock()
	return ts, nil
}

func (p *Provider) buildSource() (oauth2.TokenSource, error) {
	if p.newTS != nil {
		ts, err := p.newTS()
		if err != nil {
			// The hook's error text is not ours to vouch for; keep its type.
			return nil, fmt.Errorf("%w: building the injected Google token source failed (%T)", secrets.ErrLogin, err)
		}
		if ts == nil {
			return nil, fmt.Errorf("%w: the injected Google token source is nil", secrets.ErrLogin)
		}
		return ts, nil
	}
	// The source keeps this context for later refreshes, so it must not be
	// a request context; oauth2 reads the HTTP client from it.
	cctx := context.WithValue(p.bg, oauth2.HTTPClient, p.idc)
	var creds *google.Credentials
	var err error
	if p.credsFile != "" {
		var b []byte
		if b, err = readSmallFile(p.credsFile); err != nil {
			return nil, fmt.Errorf("%w: %w", secrets.ErrLogin, err)
		}
		creds, err = google.CredentialsFromJSONWithType(cctx, b, google.ServiceAccount, Scope)
	} else {
		creds, err = google.FindDefaultCredentials(cctx, Scope)
	}
	if err != nil {
		// The credentials library's error can quote the file; name the source only.
		return nil, fmt.Errorf("%w: no usable Google credentials (%s)", secrets.ErrLogin, p.credsSource())
	}
	return creds.TokenSource, nil
}

func (p *Provider) credsSource() string {
	switch {
	case p.newTS != nil || p.injected != nil:
		return "injected token source"
	case p.credsFile != "":
		return "service account file " + p.credsFile
	}
	return "application default credentials"
}

// tokenCall is one token fetch shared by every caller that arrives while
// it runs.
type tokenCall struct {
	done chan struct{}
	tok  string
	err  error
}

// token gets an access token within ctx. oauth2.TokenSource takes no
// context, so the fetch runs in a goroutine. Only one fetch per provider
// runs at a time and callers wait on it with their own ctx, so a hanging
// source holds at most one goroutine. The goroutine ends when Token()
// returns: the identity client's timeout bounds ADC and key-file fetches,
// and Close cancels their refresh context; an injected source must bound
// its own fetch (Options.TokenSource).
func (p *Provider) token(ctx context.Context) (netsec.Secret, error) {
	p.mu.Lock()
	c := p.fetch
	if c == nil {
		c = &tokenCall{done: make(chan struct{})}
		p.fetch = c
		go p.runFetch(c)
	}
	p.mu.Unlock()
	select {
	case <-c.done:
		return netsec.Secret(c.tok), c.err
	case <-ctx.Done():
		return "", fmt.Errorf("%w: getting a Google access token: %w", secrets.ErrUnreachable, ctx.Err())
	case <-p.bg.Done():
		return "", secrets.ErrClosed
	}
}

// runFetch performs c and publishes its result. The oauth2 sources cache
// valid tokens themselves, so a fetch is cheap when nothing expired.
func (p *Provider) runFetch(c *tokenCall) {
	defer func() {
		p.mu.Lock()
		if p.fetch == c {
			p.fetch = nil
		}
		p.mu.Unlock()
		close(c.done)
	}()
	ts, err := p.tokenSource()
	if err != nil {
		c.err = err
		return
	}
	t, err := ts.Token()
	switch {
	case err != nil:
		c.err = p.tokenErr(err)
	case t == nil || t.AccessToken == "":
		c.err = fmt.Errorf("%w: google returned no access token", secrets.ErrLogin)
	default:
		c.tok = t.AccessToken
	}
}

// invalidate drops a cached ADC, key-file or NewTokenSource source so the
// next call logs in afresh. A fixed Options.TokenSource cannot be rebuilt.
func (p *Provider) invalidate() {
	p.mu.Lock()
	p.ts = nil
	// A fetch already running may use the rejected source; later callers
	// start a new one instead of joining it.
	p.fetch = nil
	p.mu.Unlock()
}

// tokenErr maps a token failure. A refused grant is ErrLogin; network
// trouble and 5xx are ErrUnreachable. The RetrieveError's own text quotes
// the response body, so only its code and description are kept.
func (p *Provider) tokenErr(err error) error {
	var re *oauth2.RetrieveError
	if errors.As(err, &re) {
		status := 0
		if re.Response != nil {
			status = re.Response.StatusCode
		}
		msg := truncate(strings.TrimSpace(re.ErrorCode+" "+re.ErrorDescription), 200)
		if status >= 500 || status == http.StatusTooManyRequests {
			return fmt.Errorf("%w: google token endpoint: HTTP %d %s", secrets.ErrUnreachable, status, msg)
		}
		return fmt.Errorf("%w: google token endpoint: HTTP %d %s", secrets.ErrLogin, status, msg)
	}
	var ne net.Error
	if errors.As(err, &ne) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: google token endpoint: %w", secrets.ErrUnreachable, err)
	}
	// Other flows (external account STS exchange, metadata server) can put
	// the raw response body in their error text; keep only the source and
	// the error's type.
	return fmt.Errorf("%w: google credentials from %s failed (%T)", secrets.ErrLogin, p.credsSource(), err)
}

// call sends an authenticated request. After a 401 the cached source is
// dropped and the call retried once (revoked or rotated credentials).
func (p *Provider) call(ctx context.Context, method, u string, body string, out any) error {
	if p.closed.Load() {
		return secrets.ErrClosed
	}
	for attempt := 0; ; attempt++ {
		tok, err := p.token(ctx)
		if err != nil {
			return err
		}
		var rd io.Reader
		if body != "" {
			rd = strings.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, u, rd)
		if err != nil {
			return fmt.Errorf("gcp: building request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("Authorization", "Bearer "+tok.Reveal())
		err = do(p.hc, req, out, apiErrMsg)
		if attempt == 0 && errors.Is(err, secrets.ErrLogin) {
			p.invalidate()
			continue
		}
		return err
	}
}

// split maps a reference path to project and secret id.
func (p *Provider) split(path string) (project, secret string, err error) {
	project, secret = p.project, path
	if strings.HasPrefix(path, "projects/") {
		parts := strings.Split(path, "/")
		if len(parts) != 4 || parts[2] != "secrets" {
			return "", "", fmt.Errorf("%w: a google path is <secret> or projects/<project>/secrets/<secret>", secrets.ErrInvalidRef)
		}
		project, secret = parts[1], parts[3]
	}
	if !projectRe.MatchString(project) || !secretIDRe.MatchString(secret) {
		return "", "", fmt.Errorf("%w: a google secret id has letters, digits, '_' and '-' only", secrets.ErrInvalidRef)
	}
	return project, secret, nil
}

func (p *Provider) secretURL(project, secret string) string {
	return p.base + "/v1/projects/" + url.PathEscape(project) + "/secrets/" + url.PathEscape(secret)
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Resolve calls versions:access.
func (p *Provider) Resolve(ctx context.Context, ref secrets.Ref) (secrets.Value, error) {
	project, secret, err := p.split(ref.Path)
	if err != nil {
		return secrets.Value{}, err
	}
	ver := ref.Version
	if ver == "" {
		ver = "latest"
	}
	if !versionRe.MatchString(ver) {
		return secrets.Value{}, fmt.Errorf("%w: a google version is a number, an alias or latest", secrets.ErrInvalidRef)
	}
	ctx, cancel := withDefaultDeadline(ctx)
	defer cancel()
	var out struct {
		Name    string `json:"name"`
		Payload *struct {
			Data       string `json:"data"`
			DataCrc32c string `json:"dataCrc32c"`
		} `json:"payload"`
	}
	u := p.secretURL(project, secret) + "/versions/" + url.PathEscape(ver) + ":access"
	if err := p.call(ctx, http.MethodGet, u, "", &out); err != nil {
		return secrets.Value{}, fmt.Errorf("secrets: google secret manager: reading %s: %w", ref.Path, err)
	}
	if out.Payload == nil {
		return secrets.Value{}, fmt.Errorf("%w: %s has no payload", secrets.ErrNotFound, ref.Path)
	}
	data, err := base64.StdEncoding.DecodeString(out.Payload.Data)
	if err != nil {
		return secrets.Value{}, fmt.Errorf("secrets: google secret manager: reading %s: payload is not base64", ref.Path)
	}
	if out.Payload.DataCrc32c != "" {
		want, err := strconv.ParseUint(out.Payload.DataCrc32c, 10, 32)
		if err != nil || crc32.Checksum(data, castagnoli) != uint32(want) {
			return secrets.Value{}, fmt.Errorf("secrets: google secret manager: reading %s: payload checksum mismatch", ref.Path)
		}
	}
	return secrets.Value{Data: netsec.Secret(data), Version: lastSegment(out.Name)}, nil
}

// maxListPages bounds List (250 names a page).
const maxListPages = 20

// List returns secret ids in the provider's project starting with prefix.
// It reads metadata (secrets.list), never values.
func (p *Provider) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	ctx, cancel := withDefaultDeadline(ctx)
	defer cancel()
	if limit <= 0 {
		limit = 200
	}
	out := []string{}
	pageToken := ""
	for page := 0; page < maxListPages && len(out) < limit; page++ {
		q := url.Values{"pageSize": {"250"}}
		if pageToken != "" {
			q.Set("pageToken", pageToken)
		}
		var body struct {
			Secrets []struct {
				Name string `json:"name"`
			} `json:"secrets"`
			NextPageToken string `json:"nextPageToken"`
		}
		u := p.base + "/v1/projects/" + url.PathEscape(p.project) + "/secrets?" + q.Encode()
		if err := p.call(ctx, http.MethodGet, u, "", &body); err != nil {
			if errors.Is(err, secrets.ErrAccessDenied) {
				return nil, fmt.Errorf("%w: %w", secrets.ErrListUnsupported, err)
			}
			return nil, fmt.Errorf("secrets: google secret manager: listing: %w", err)
		}
		for _, s := range body.Secrets {
			if name := lastSegment(s.Name); name != "" && strings.HasPrefix(name, prefix) {
				out = append(out, name)
			}
		}
		if body.NextPageToken == "" {
			break
		}
		pageToken = body.NextPageToken
	}
	sort.Strings(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
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

// Permissions that let Piperoom change secrets or their access. Piperoom
// needs none of them.
var (
	secretWritePerms = []string{
		"secretmanager.versions.add", "secretmanager.versions.destroy", "secretmanager.versions.disable",
		"secretmanager.secrets.update", "secretmanager.secrets.delete", "secretmanager.secrets.setIamPolicy",
	}
	projectPerms = []string{
		"secretmanager.versions.access", "secretmanager.versions.add", "secretmanager.secrets.create",
		"secretmanager.secrets.delete", "secretmanager.secrets.setIamPolicy",
	}
)

// Check runs the card's test: reach, log in, list (picker), read the
// sample, then testIamPermissions to warn about write access or
// project-wide read access.
func (p *Provider) Check(ctx context.Context, sample *secrets.Ref) []spec.Check {
	const (
		reachTitle = "Can reach Google Secret Manager"
		loginTitle = "Google credentials are accepted"
		listTitle  = "Piperoom can list names for the picker"
		tooTitle   = "Piperoom has no more access than it needs"
	)
	if p.closed.Load() {
		return []spec.Check{fail(secrets.CheckReach, reachTitle, secrets.ErrClosed.Error(), "")}
	}
	// Each step gets min(checkTimeout, what is left of checkBudget).
	ctx, cancelAll := context.WithTimeout(ctx, checkBudget)
	defer cancelAll()
	listURL := p.base + "/v1/projects/" + url.PathEscape(p.project) + "/secrets?pageSize=1"
	if err := p.probe(ctx, listURL); err != nil {
		return []spec.Check{
			fail(secrets.CheckReach, reachTitle, errorDetail(err), "check outbound HTTPS to "+p.base+" (or the Private Service Connect endpoint)"),
			skipped(secrets.CheckLogin, loginTitle, "not reachable"),
			skipped(secrets.CheckRead, sampleTitle, "not reachable"),
			skipped(secrets.CheckTooMuch, tooTitle, "not reachable"),
		}
	}
	out := []spec.Check{pass(secrets.CheckReach, reachTitle, strings.TrimPrefix(p.base, "https://"))}

	cctx, cancel := context.WithTimeout(ctx, checkTimeout)
	_, err := p.token(cctx)
	cancel()
	if err != nil {
		return append(out,
			fail(secrets.CheckLogin, loginTitle, errorDetail(err),
				"run Piperoom with a service account (attached, or GKE Workload Identity), or set GOOGLE_APPLICATION_CREDENTIALS to a workload identity federation config"),
			skipped(secrets.CheckRead, sampleTitle, "login failed"),
			skipped(secrets.CheckTooMuch, tooTitle, "login failed"))
	}

	cctx, cancel = context.WithTimeout(ctx, checkTimeout)
	err = p.call(cctx, http.MethodGet, listURL, "", nil)
	cancel()
	switch {
	case err == nil:
		out = append(out, pass(secrets.CheckLogin, loginTitle, p.credsSourceForDetail()))
	case errors.Is(err, secrets.ErrLogin):
		return append(out,
			fail(secrets.CheckLogin, loginTitle, "Secret Manager rejected the token: "+errorDetail(err), "check the service account is enabled and its key or federation config is current"),
			skipped(secrets.CheckRead, sampleTitle, "login failed"),
			skipped(secrets.CheckTooMuch, tooTitle, "login failed"))
	case errors.Is(err, secrets.ErrAccessDenied):
		out = append(out, pass(secrets.CheckLogin, loginTitle, p.credsSourceForDetail()),
			warn(secrets.CheckListNames, listTitle, "denied: "+errorDetail(err),
				"grant roles/secretmanager.viewer on the project (names and metadata only, never values), or type references by hand"))
	default:
		out = append(out, pass(secrets.CheckLogin, loginTitle, p.credsSourceForDetail()),
			warn(secrets.CheckListNames, listTitle, errorDetail(err), "check the project id and that the Secret Manager API is enabled"))
	}
	out = append(out, sampleCheck(ctx, p, sample))
	if dl, ok := ctx.Deadline(); ctx.Err() != nil || (ok && time.Until(dl) < minStepTime) {
		return append(out, skipped(secrets.CheckTooMuch, tooTitle, "skipped: the time budget for this test is used up"))
	}
	return append(out, p.privilegeCheck(ctx, sample, tooTitle))
}

// minStepTime is the least time left worth starting the privilege check with.
const minStepTime = time.Second

func (p *Provider) credsSourceForDetail() string {
	if p.injected != nil || p.newTS != nil {
		return "workspace credentials"
	}
	return p.credsSource()
}

// privilegeCheck asks testIamPermissions, which needs no extra role and
// changes nothing, whether Piperoom could write or read project-wide.
func (p *Provider) privilegeCheck(ctx context.Context, sample *secrets.Ref, title string) spec.Check {
	// The two testIamPermissions calls are independent; run them in
	// parallel so the check fits the Check budget.
	type answer struct {
		got []string
		err error
	}
	var projAns, secAns answer
	var secretName string
	asked := 1
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cctx, cancel := context.WithTimeout(ctx, checkTimeout)
		defer cancel()
		projAns.got, projAns.err = p.testPermissions(cctx, p.rmBase+"/v1/projects/"+url.PathEscape(p.project)+":testIamPermissions", projectPerms)
	}()
	if sample != nil {
		if project, secret, err := p.split(sample.Path); err == nil {
			asked++
			secretName = secret
			wg.Add(1)
			go func() {
				defer wg.Done()
				cctx, cancel := context.WithTimeout(ctx, checkTimeout)
				defer cancel()
				secAns.got, secAns.err = p.testPermissions(cctx, p.secretURL(project, secret)+":testIamPermissions", secretWritePerms)
			}()
		}
	}
	wg.Wait()

	var granted, errs []string
	projectWideRead := false
	if projAns.err != nil {
		errs = append(errs, "project: "+errorDetail(projAns.err))
	} else {
		for _, g := range projAns.got {
			if g == "secretmanager.versions.access" {
				projectWideRead = true
			} else {
				granted = append(granted, g+" (project)")
			}
		}
	}
	if asked == 2 {
		if secAns.err != nil {
			errs = append(errs, "secret: "+errorDetail(secAns.err))
		} else {
			for _, g := range secAns.got {
				granted = append(granted, g+" ("+secretName+")")
			}
		}
	}
	switch {
	case len(granted) > 0:
		sort.Strings(granted)
		return warn(secrets.CheckTooMuch, title, "Piperoom can change secrets: "+strings.Join(granted, ", "),
			"grant only roles/secretmanager.secretAccessor (and roles/secretmanager.viewer for the picker)")
	case projectWideRead:
		return warn(secrets.CheckTooMuch, title, "Piperoom can read every secret in project "+p.project,
			"bind roles/secretmanager.secretAccessor per secret, or with an IAM condition on the name prefix (see the setup script)")
	case len(errs) == asked:
		return skipped(secrets.CheckTooMuch, title, "testIamPermissions failed: "+truncate(strings.Join(errs, "; "), 300))
	case len(errs) > 0:
		return pass(secrets.CheckTooMuch, title, "no write access found; partial answer: "+truncate(strings.Join(errs, "; "), 200))
	}
	return pass(secrets.CheckTooMuch, title, "read-only, no project-wide read")
}

// testPermissions POSTs {"permissions": perms} and returns those granted.
func (p *Provider) testPermissions(ctx context.Context, u string, perms []string) ([]string, error) {
	var body strings.Builder
	body.WriteString(`{"permissions":[`)
	for i, s := range perms {
		if i > 0 {
			body.WriteByte(',')
		}
		body.WriteString(strconv.Quote(s))
	}
	body.WriteString(`]}`)
	var out struct {
		Permissions []string `json:"permissions"`
	}
	if err := p.call(ctx, http.MethodPost, u, body.String(), &out); err != nil {
		return nil, err
	}
	return out.Permissions, nil
}

// probe sends one unauthenticated request; any answer but a 5xx counts
// (Google answers 401, which proves TLS and the host without a token).
func (p *Provider) probe(ctx context.Context, u string) error {
	cctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("gcp: building request: %w", err)
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

func lastSegment(name string) string {
	name = strings.TrimRight(name, "/")
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s = strings.TrimSpace(s); s != "" {
			return s
		}
	}
	return ""
}

// readSmallFile reads a credentials file (at most 64 KiB).
func readSmallFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("gcp: reading %s: %w", path, err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 64<<10))
	if err != nil {
		return nil, fmt.Errorf("gcp: reading %s: %w", path, err)
	}
	return b, nil
}
