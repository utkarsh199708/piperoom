package secrets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

// Vault auth methods.
const (
	VaultAuthToken      = "token"
	VaultAuthAppRole    = "approle"
	VaultAuthKubernetes = "kubernetes"
	VaultAuthJWT        = "jwt"
)

// DefaultKubernetesTokenFile is the in-cluster service-account token.
const DefaultKubernetesTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// VaultOptions configure a HashiCorp Vault or OpenBao provider. Both speak
// the same HTTP API, so one implementation serves both.
type VaultOptions struct {
	Address   string // https://vault.corp:8200
	Namespace string // Vault Enterprise namespace; sent as X-Vault-Namespace
	// Auth is one of the VaultAuth* methods. Default: kubernetes when a
	// service-account token is mounted, else token.
	Auth      string
	AuthMount string // auth method mount; default = the method name
	Role      string // kubernetes, jwt: the Vault role

	Token     netsec.Secret // token auth (last resort)
	TokenFile string        // token auth: a file the token is re-read from (Vault Agent sink)
	RoleID    string        // approle
	SecretID  netsec.Secret // approle
	// SecretIDFile is re-read on each login (response-wrapped delivery not supported).
	SecretIDFile string
	JWT          netsec.Secret // jwt
	JWTFile      string        // jwt, kubernetes; kubernetes default DefaultKubernetesTokenFile

	// KV2Mounts are the mounts that are KV version 2 (default ["secret"]).
	// Paths in other mounts are read as is: KV v1, the database engine, ...
	KV2Mounts []string
	// Prefix is the path prefix Piperoom is meant to read (e.g.
	// "secret/piperoom/"); it scopes the least-privilege check and is the
	// starting point of List with an empty prefix. "" = not checked.
	Prefix string

	TLS        netsec.TLS
	CAPEM      string       // a CA pasted into the card; replaces TLS.CAFile
	HTTPClient *http.Client // optional: a client with a guarded dialer, or tests
	Timeout    time.Duration
}

// VaultProvider reads from Vault or OpenBao over its HTTP API.
type VaultProvider struct {
	base      *url.URL
	namespace string
	auth      string
	mount     string
	role      string
	opts      VaultOptions // holds secrets; never printed (see String)
	kv2       map[string]bool
	prefix    string
	hc        *http.Client
	now       func() time.Time

	loginMu sync.Mutex // one login at a time
	mu      sync.Mutex
	token   netsec.Secret
	tokRen  *Renewer
	closed  bool
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewVaultProvider checks the options. It does not contact Vault; the first
// call logs in.
func NewVaultProvider(opts VaultOptions) (*VaultProvider, error) {
	base, err := checkAddress(opts.Address, "vault")
	if err != nil {
		return nil, err
	}
	auth := opts.Auth
	if auth == "" {
		auth = VaultAuthToken
		if opts.Token == "" && opts.TokenFile == "" && fileExists(DefaultKubernetesTokenFile) {
			auth = VaultAuthKubernetes
		}
	}
	switch auth {
	case VaultAuthToken:
		if opts.Token == "" && opts.TokenFile == "" {
			return nil, fmt.Errorf("%w: vault token auth needs a token or token_file", ErrBadProviderConfig)
		}
	case VaultAuthAppRole:
		if opts.RoleID == "" || (opts.SecretID == "" && opts.SecretIDFile == "") {
			return nil, fmt.Errorf("%w: vault approle auth needs role_id and secret_id (or secret_id_file)", ErrBadProviderConfig)
		}
	case VaultAuthKubernetes:
		if opts.Role == "" {
			return nil, fmt.Errorf("%w: vault kubernetes auth needs a role", ErrBadProviderConfig)
		}
		if opts.JWTFile == "" && opts.JWT == "" {
			opts.JWTFile = DefaultKubernetesTokenFile
		}
	case VaultAuthJWT:
		if opts.Role == "" || (opts.JWT == "" && opts.JWTFile == "") {
			return nil, fmt.Errorf("%w: vault jwt auth needs a role and a jwt or jwt_file", ErrBadProviderConfig)
		}
	default:
		return nil, fmt.Errorf("%w: unknown vault auth method %q", ErrBadProviderConfig, auth)
	}
	mount := strings.Trim(opts.AuthMount, "/")
	if mount == "" {
		mount = auth
	}
	hc := opts.HTTPClient
	if hc == nil {
		if hc, err = newHTTPClient(opts.TLS, opts.CAPEM, opts.Timeout); err != nil {
			return nil, err
		}
	} else {
		hc = noRedirects(hc)
	}
	kv2 := map[string]bool{}
	mounts := opts.KV2Mounts
	if mounts == nil {
		mounts = []string{"secret"}
	}
	for _, m := range mounts {
		if m = strings.Trim(m, "/ "); m != "" {
			kv2[m] = true
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &VaultProvider{
		base: base, namespace: opts.Namespace, auth: auth, mount: mount, role: opts.Role, opts: opts,
		kv2: kv2, prefix: strings.TrimPrefix(opts.Prefix, "/"), hc: hc, now: time.Now, ctx: ctx, cancel: cancel,
	}, nil
}

// GoString keeps %#v from printing unexported credential fields.
func (p *VaultProvider) GoString() string { return p.String() }

// String names the provider without any credential.
func (p *VaultProvider) String() string {
	return fmt.Sprintf("vault provider %s (auth %s)", p.base.Redacted(), p.auth)
}

// Close stops token renewal.
func (p *VaultProvider) Close() {
	p.mu.Lock()
	p.closed = true
	ren := p.tokRen
	p.tokRen = nil
	p.token = ""
	p.mu.Unlock()
	p.cancel()
	if ren != nil {
		ren.Close()
	}
}

// vaultResponse is the envelope of every Vault read.
type vaultResponse struct {
	LeaseID       string          `json:"lease_id"`
	LeaseDuration int64           `json:"lease_duration"`
	Renewable     bool            `json:"renewable"`
	Data          json.RawMessage `json:"data"`
	Auth          *vaultAuth      `json:"auth"`
}

type vaultAuth struct {
	ClientToken   netsec.Secret `json:"client_token"`
	LeaseDuration int64         `json:"lease_duration"`
	Renewable     bool          `json:"renewable"`
}

func vaultErrMsg(body []byte) string {
	var e struct {
		Errors []string `json:"errors"`
	}
	if json.Unmarshal(body, &e) != nil {
		return ""
	}
	return strings.Join(e.Errors, "; ")
}

// call sends one request with the token, logging in first when needed and
// once more after a 403 (an expired or revoked token answers 403).
func (p *VaultProvider) call(ctx context.Context, method, path string, query url.Values, body, out any) (int, error) {
	tok, err := p.currentToken(ctx)
	if err != nil {
		return 0, err
	}
	status, err := p.send(ctx, method, path, query, body, out, tok)
	if status == http.StatusForbidden && p.canRelogin() {
		p.dropToken(tok)
		if tok, err = p.currentToken(ctx); err != nil {
			return 0, err
		}
		status, err = p.send(ctx, method, path, query, body, out, tok)
	}
	return status, err
}

func (p *VaultProvider) send(ctx context.Context, method, path string, query url.Values, body, out any, tok netsec.Secret) (int, error) {
	ctx, cancel := withDefaultDeadline(ctx)
	defer cancel()
	// path is already escaped segment by segment, so it is appended as text.
	u := strings.TrimRight(p.base.String(), "/") + "/v1/" + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := newJSONRequest(ctx, method, u, body)
	if err != nil {
		return 0, err
	}
	if tok != "" {
		req.Header.Set("X-Vault-Token", tok.Reveal())
	}
	if p.namespace != "" {
		req.Header.Set("X-Vault-Namespace", p.namespace)
	}
	status, err := doJSON(p.hc, req, out, vaultErrMsg)
	if err != nil {
		return status, fmt.Errorf("secrets: vault %s %s: %w", method, path, err)
	}
	return status, nil
}

// canRelogin reports whether a fresh token can be obtained without the
// operator: every method but a static token typed in.
func (p *VaultProvider) canRelogin() bool {
	return p.auth != VaultAuthToken || p.opts.TokenFile != ""
}

func (p *VaultProvider) dropToken(tok netsec.Secret) {
	p.mu.Lock()
	var ren *Renewer
	if p.token == tok {
		p.token = ""
		ren, p.tokRen = p.tokRen, nil
	}
	p.mu.Unlock()
	if ren != nil {
		ren.Close()
	}
}

func (p *VaultProvider) currentToken(ctx context.Context) (netsec.Secret, error) {
	p.mu.Lock()
	tok, closed := p.token, p.closed
	p.mu.Unlock()
	if closed {
		return "", ErrClosed
	}
	if tok != "" {
		return tok, nil
	}
	p.loginMu.Lock()
	defer p.loginMu.Unlock()
	p.mu.Lock()
	tok = p.token
	p.mu.Unlock()
	if tok != "" {
		return tok, nil
	}
	tok, ttl, renewable, err := p.login(ctx)
	if err != nil {
		return "", err
	}
	var ren *Renewer
	if renewable && ttl > 0 {
		ren = NewRenewer(p.renewToken(tok), func(error) { p.dropToken(tok) })
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return "", ErrClosed
	}
	p.token, p.tokRen = tok, ren
	p.mu.Unlock()
	if ren != nil {
		ren.Start(p.ctx, p.now().Add(ttl))
	}
	return tok, nil
}

// login returns a token and its TTL.
func (p *VaultProvider) login(ctx context.Context) (netsec.Secret, time.Duration, bool, error) {
	if p.auth == VaultAuthToken {
		tok := p.opts.Token
		if p.opts.TokenFile != "" {
			t, err := readSecretFile(p.opts.TokenFile)
			if err != nil {
				return "", 0, false, fmt.Errorf("%w: %w", ErrLogin, err)
			}
			tok = t
		}
		var self vaultResponse
		if _, err := p.send(ctx, http.MethodGet, "auth/token/lookup-self", nil, nil, &self, tok); err != nil {
			return "", 0, false, loginErr(err)
		}
		var d struct {
			TTL       int64 `json:"ttl"`
			Renewable bool  `json:"renewable"`
		}
		_ = json.Unmarshal(self.Data, &d) // missing fields mean "not renewable"
		return tok, time.Duration(d.TTL) * time.Second, d.Renewable, nil
	}
	body := map[string]string{}
	switch p.auth {
	case VaultAuthAppRole:
		sid := p.opts.SecretID
		if p.opts.SecretIDFile != "" {
			s, err := readSecretFile(p.opts.SecretIDFile)
			if err != nil {
				return "", 0, false, fmt.Errorf("%w: %w", ErrLogin, err)
			}
			sid = s
		}
		body["role_id"], body["secret_id"] = p.opts.RoleID, sid.Reveal()
	case VaultAuthKubernetes, VaultAuthJWT:
		jwt := p.opts.JWT
		if p.opts.JWTFile != "" {
			// Re-read each login: projected service-account tokens rotate.
			j, err := readSecretFile(p.opts.JWTFile)
			if err != nil {
				return "", 0, false, fmt.Errorf("%w: %w", ErrLogin, err)
			}
			jwt = j
		}
		body["role"], body["jwt"] = p.role, jwt.Reveal()
	}
	var resp vaultResponse
	if _, err := p.send(ctx, http.MethodPost, "auth/"+p.mount+"/login", nil, body, &resp, ""); err != nil {
		return "", 0, false, loginErr(err)
	}
	if resp.Auth == nil || resp.Auth.ClientToken == "" {
		return "", 0, false, fmt.Errorf("%w: vault login returned no token", ErrLogin)
	}
	return resp.Auth.ClientToken, time.Duration(resp.Auth.LeaseDuration) * time.Second, resp.Auth.Renewable, nil
}

// loginErr marks a refused login as ErrLogin; an unreachable Vault stays
// ErrUnreachable.
func loginErr(err error) error {
	if errors.Is(err, ErrUnreachable) || errors.Is(err, ErrLogin) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrLogin, err)
}

func (p *VaultProvider) renewToken(tok netsec.Secret) func(context.Context) (time.Time, error) {
	return func(ctx context.Context) (time.Time, error) {
		ctx, cancel := context.WithTimeout(ctx, DefaultResolveTimeout)
		defer cancel()
		var resp vaultResponse
		if _, err := p.send(ctx, http.MethodPost, "auth/token/renew-self", nil, map[string]any{}, &resp, tok); err != nil {
			return time.Time{}, err
		}
		if resp.Auth == nil || resp.Auth.LeaseDuration <= 0 {
			return time.Time{}, errLeaseEnded
		}
		return p.now().Add(time.Duration(resp.Auth.LeaseDuration) * time.Second), nil
	}
}

// vaultInternalSegments name Vault's own endpoints. A reference may never
// reach them: auth/token/lookup-self would hand Piperoom's own token to
// whoever wrote the reference, and sys/, identity/ and cubbyhole/ hold
// Vault state rather than application secrets.
var vaultInternalSegments = map[string]bool{"auth": true, "sys": true, "identity": true, "cubbyhole": true}

// checkVaultPath refuses paths into Vault's internal endpoints. Outside the
// KV v2 mounts every segment is checked, because Vault Enterprise also
// addresses namespaces by path ("team-a/auth/token/lookup-self").
func (p *VaultProvider) checkVaultPath(path string) error {
	mount, _ := splitMount(path)
	if vaultInternalSegments[mount] {
		return fmt.Errorf("%w: vault %s/ endpoints cannot be read through a reference", ErrAccessDenied, mount)
	}
	if p.kv2[mount] {
		return nil
	}
	for _, seg := range strings.Split(path, "/") {
		if vaultInternalSegments[seg] {
			return fmt.Errorf("%w: vault %s/ endpoints cannot be read through a reference", ErrAccessDenied, seg)
		}
	}
	return nil
}

// split returns the mount (first segment) and the rest of a path.
func splitMount(path string) (string, string) {
	m, rest, _ := strings.Cut(path, "/")
	return m, rest
}

// Resolve reads a KV v2 secret (at a version when pinned), a KV v1 secret,
// or any other read endpoint such as database/creds/<role>, whose lease is
// returned for renewal. Data is the JSON object of the secret's fields.
func (p *VaultProvider) Resolve(ctx context.Context, ref Ref) (Value, error) {
	if err := p.checkVaultPath(ref.Path); err != nil {
		return Value{}, err
	}
	mount, rest := splitMount(ref.Path)
	if p.kv2[mount] {
		if rest == "" {
			return Value{}, fmt.Errorf("%w: vault path needs a secret name after the mount", ErrInvalidRef)
		}
		q := url.Values{}
		if ref.Version != "" {
			if _, err := strconv.ParseUint(ref.Version, 10, 64); err != nil {
				return Value{}, fmt.Errorf("%w: a KV v2 version is a number", ErrInvalidRef)
			}
			q.Set("version", ref.Version)
		}
		var resp vaultResponse
		if _, err := p.call(ctx, http.MethodGet, mount+"/data/"+escapePath(rest), q, nil, &resp); err != nil {
			return Value{}, err
		}
		var kv struct {
			Data     map[string]json.RawMessage `json:"data"`
			Metadata struct {
				Version     int64  `json:"version"`
				DeletedTime string `json:"deletion_time"`
				Destroyed   bool   `json:"destroyed"`
			} `json:"metadata"`
		}
		if json.Unmarshal(resp.Data, &kv) != nil {
			return Value{}, errors.New("secrets: vault: KV v2 response is not the expected JSON")
		}
		if kv.Data == nil {
			// A deleted or destroyed version answers 200 with data null in some versions.
			return Value{}, fmt.Errorf("%w: %s (deleted or destroyed version)", ErrNotFound, ref.Path)
		}
		data, err := json.Marshal(kv.Data)
		if err != nil {
			return Value{}, errors.New("secrets: vault: encoding secret fields failed")
		}
		return Value{Data: netsec.Secret(data), Version: strconv.FormatInt(kv.Metadata.Version, 10)}, nil
	}
	if ref.Version != "" {
		return Value{}, fmt.Errorf("%w: only KV v2 mounts have versions (mount %s is not in kv2_mounts)", ErrInvalidRef, mount)
	}
	var resp vaultResponse
	if _, err := p.call(ctx, http.MethodGet, escapePath(ref.Path), nil, nil, &resp); err != nil {
		return Value{}, err
	}
	// RawMessage keeps each field's bytes: map[string]any would turn a large
	// integer into a float64 and change the secret.
	var fields map[string]json.RawMessage
	if json.Unmarshal(resp.Data, &fields) != nil || fields == nil {
		return Value{}, fmt.Errorf("%w: %s has no data", ErrNotFound, ref.Path)
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return Value{}, errors.New("secrets: vault: encoding secret fields failed")
	}
	v := Value{Data: netsec.Secret(data)}
	if resp.LeaseID != "" && resp.LeaseDuration > 0 {
		v.LeaseID = resp.LeaseID
		v.Renewable = resp.Renewable
		v.ExpiresAt = p.now().Add(time.Duration(resp.LeaseDuration) * time.Second)
	}
	return v, nil
}

// RenewLease implements LeaseRenewer with sys/leases/renew.
func (p *VaultProvider) RenewLease(ctx context.Context, v Value) (time.Time, error) {
	if v.LeaseID == "" {
		return time.Time{}, errors.New("secrets: vault: no lease to renew")
	}
	var resp vaultResponse
	if _, err := p.call(ctx, http.MethodPut, "sys/leases/renew", nil, map[string]any{"lease_id": v.LeaseID}, &resp); err != nil {
		return time.Time{}, err
	}
	if resp.LeaseDuration <= 0 {
		return time.Time{}, errLeaseEnded
	}
	return p.now().Add(time.Duration(resp.LeaseDuration) * time.Second), nil
}

// List lists one level under the directory part of prefix (KV LIST). An
// empty prefix starts at VaultOptions.Prefix; when that is set, a prefix
// outside it is refused, because secret names can be sensitive (§9).
func (p *VaultProvider) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	if prefix == "" {
		prefix = p.prefix
	}
	prefix = strings.TrimPrefix(prefix, "/")
	if strings.Contains(prefix, "..") {
		return nil, fmt.Errorf("%w: '..' in prefix", ErrInvalidRef)
	}
	if p.prefix != "" && !strings.HasPrefix(prefix, p.prefix) {
		return nil, fmt.Errorf("%w: listing is limited to the provider's prefix %s", ErrAccessDenied, p.prefix)
	}
	if err := p.checkVaultPath(prefix); err != nil {
		return nil, err
	}
	i := strings.LastIndexByte(prefix, '/')
	if i < 0 {
		// Listing mounts needs sys/mounts, which a least-privilege policy lacks.
		return []string{}, nil
	}
	dir, namePrefix := prefix[:i], prefix[i+1:]
	mount, rest := splitMount(dir)
	path := escapePath(dir)
	if p.kv2[mount] {
		path = mount + "/metadata"
		if rest != "" {
			path += "/" + escapePath(rest)
		}
	}
	var resp vaultResponse
	status, err := p.call(ctx, "LIST", path, nil, nil, &resp)
	if status == http.StatusNotFound {
		return []string{}, nil
	}
	if errors.Is(err, ErrAccessDenied) {
		return nil, fmt.Errorf("%w: %w", ErrListUnsupported, err)
	}
	if err != nil {
		return nil, err
	}
	var d struct {
		Keys []string `json:"keys"`
	}
	if json.Unmarshal(resp.Data, &d) != nil {
		return nil, errors.New("secrets: vault: LIST response is not the expected JSON")
	}
	out := []string{}
	for _, k := range d.Keys {
		if strings.HasPrefix(k, namePrefix) {
			out = append(out, dir+"/"+k)
		}
	}
	sort.Strings(out)
	return capList(out, limit), nil
}

// Keys returns the field names of a KV secret. For a credentials endpoint
// (a "creds" segment outside the KV mounts) it answers the database
// engine's fields without reading, because each read issues a new login.
func (p *VaultProvider) Keys(ctx context.Context, ref Ref) ([]string, error) {
	// Browsing is limited to the prefix (§9), keys as well as names: the
	// key names and whether a secret exists are themselves information.
	if p.prefix != "" && !strings.HasPrefix(ref.Path, p.prefix) {
		return nil, fmt.Errorf("%w: key listing is limited to the provider's prefix %s", ErrAccessDenied, p.prefix)
	}
	mount, rest := splitMount(ref.Path)
	if !p.kv2[mount] && (rest == "creds" || strings.HasPrefix(rest, "creds/") || strings.Contains(rest, "/creds/")) {
		return []string{"password", "username"}, nil
	}
	v, err := p.Resolve(ctx, ref.secretID())
	if err != nil {
		return nil, err
	}
	return KeysOf(v.Data), nil
}

// Check runs reach, login, sample read, least privilege and lease checks.
func (p *VaultProvider) Check(ctx context.Context, sample *Ref) []spec.Check {
	var out []spec.Check
	reach := p.checkReach(ctx)
	out = append(out, reach)
	const loginTitle = "Piperoom can log in"
	if reach.Status == spec.CheckFail {
		return append(out,
			skipped(CheckLogin, loginTitle, "Vault is not reachable"),
			skipped(CheckRead, "Can read the sample secret", "Vault is not reachable"),
			skipped(CheckTooMuch, "Piperoom has no more access than it needs", "Vault is not reachable"),
			skipped(CheckLease, "Leases are renewable", "Vault is not reachable"))
	}
	lctx, cancel := context.WithTimeout(ctx, checkTimeout)
	_, err := p.currentToken(lctx)
	cancel()
	if err != nil {
		fix := "check the " + p.auth + " auth method at auth/" + p.mount + " and the role binding"
		return append(out, fail(CheckLogin, loginTitle, errorDetail(err), fix),
			skipped(CheckRead, "Can read the sample secret", "login failed"),
			skipped(CheckTooMuch, "Piperoom has no more access than it needs", "login failed"),
			skipped(CheckLease, "Leases are renewable", "login failed"))
	}
	if p.auth == VaultAuthToken {
		out = append(out, warn(CheckLogin, loginTitle, "static token: prefer workload identity",
			"use the kubernetes, jwt or approle auth method so the token is short-lived"))
	} else {
		out = append(out, pass(CheckLogin, loginTitle, "logged in with "+p.auth+" at auth/"+p.mount))
	}
	sc, v, ok := sampleCheck(ctx, p, sample)
	out = append(out, sc)
	out = append(out, p.checkLeastPrivilege(ctx, sample))
	const leaseTitle = "Leases are renewable"
	switch {
	case !ok || v.LeaseID == "":
		out = append(out, skipped(CheckLease, leaseTitle, "the sample has no lease"))
	case v.Renewable:
		out = append(out, pass(CheckLease, leaseTitle, "lease expires "+v.ExpiresAt.UTC().Format(time.RFC3339)))
	default:
		out = append(out, warn(CheckLease, leaseTitle, "the lease cannot be renewed; Piperoom will fetch new credentials before it ends",
			"set a renewable default_ttl/max_ttl on the role"))
	}
	return out
}

func (p *VaultProvider) checkReach(ctx context.Context) spec.Check {
	const title = "Can reach Vault"
	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	q := url.Values{"standbyok": {"true"}, "perfstandbyok": {"true"}}
	var health struct {
		Initialized bool   `json:"initialized"`
		Sealed      bool   `json:"sealed"`
		Version     string `json:"version"`
	}
	status, err := p.send(ctx, http.MethodGet, "sys/health", q, nil, &health, "")
	switch {
	case status == 0 && err != nil:
		return fail(CheckReach, title, errorDetail(err), "check the address, the firewall and the CA certificate")
	case status == http.StatusServiceUnavailable:
		return fail(CheckReach, title, "Vault is sealed", "unseal Vault")
	case status == http.StatusNotImplemented:
		return fail(CheckReach, title, "Vault is not initialized", "initialize Vault")
	}
	detail := "TLS"
	if p.base.Scheme != "https" {
		detail = "plain HTTP to localhost (dev only)"
	}
	if health.Version != "" {
		detail += ", version " + health.Version
	}
	return pass(CheckReach, title, detail)
}

// checkLeastPrivilege asks sys/capabilities-self about a path outside the
// prefix and about writing to the sample, without writing anything.
func (p *VaultProvider) checkLeastPrivilege(ctx context.Context, sample *Ref) spec.Check {
	const title = "Piperoom has no more access than it needs"
	if p.prefix == "" && sample == nil {
		return skipped(CheckTooMuch, title, "set a path prefix (or a sample) to check")
	}
	paths := map[string]string{} // api path → what it stands for
	if p.prefix != "" {
		mount, _ := splitMount(p.prefix)
		probe := mount + "/piperoom-access-probe-outside-prefix"
		if p.kv2[mount] {
			probe = mount + "/data/piperoom-access-probe-outside-prefix"
		}
		paths[probe] = "outside"
	}
	if sample != nil {
		mount, rest := splitMount(sample.Path)
		sp := sample.Path
		if p.kv2[mount] {
			sp = mount + "/data/" + rest
		}
		paths[sp] = "sample"
	}
	list := make([]string, 0, len(paths))
	for k := range paths {
		list = append(list, k)
	}
	sort.Strings(list)
	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	var resp map[string]json.RawMessage
	if _, err := p.call(ctx, http.MethodPost, "sys/capabilities-self", nil, map[string]any{"paths": list}, &resp); err != nil {
		return skipped(CheckTooMuch, title, "sys/capabilities-self not allowed: "+errorDetail(err))
	}
	var problems []string
	for _, path := range list {
		var caps []string
		raw, ok := resp[path]
		if !ok {
			if d, ok2 := resp["data"]; ok2 {
				var inner map[string][]string
				if json.Unmarshal(d, &inner) == nil {
					caps = inner[path]
				}
			}
		} else if json.Unmarshal(raw, &caps) != nil {
			continue
		}
		set := map[string]bool{}
		for _, c := range caps {
			set[c] = true
		}
		switch paths[path] {
		case "outside":
			if set["root"] || set["read"] || set["sudo"] {
				problems = append(problems, "can read "+path+", outside "+p.prefix)
			}
		case "sample":
			if set["root"] || set["create"] || set["update"] || set["delete"] {
				problems = append(problems, "can write "+path)
			}
		}
	}
	if len(problems) > 0 {
		return warn(CheckTooMuch, title, strings.Join(problems, "; "),
			"limit the policy to read on "+p.prefix+"* and list on its metadata")
	}
	// Only these paths were asked about: a policy can still grant other
	// paths or mounts, so the pass says what was checked, not more.
	var checked []string
	for _, path := range list {
		switch paths[path] {
		case "outside":
			checked = append(checked, "no read on "+path+" (one probe path outside "+p.prefix+")")
		case "sample":
			checked = append(checked, "no write on the sample")
		}
	}
	return pass(CheckTooMuch, title, strings.Join(checked, "; ")+"; other paths and mounts are not checked")
}

// escapePath percent-escapes each segment of a Vault path.
func escapePath(p string) string {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}
