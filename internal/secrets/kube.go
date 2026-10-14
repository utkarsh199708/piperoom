package secrets

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

// In-cluster service-account files.
const (
	kubeSADir           = "/var/run/secrets/kubernetes.io/serviceaccount"
	kubeSATokenFile     = kubeSADir + "/token"
	kubeSACAFile        = kubeSADir + "/ca.crt"
	kubeSANamespaceFile = kubeSADir + "/namespace"
)

// KubernetesOptions configure a Kubernetes Secrets provider. Every field
// defaults to the in-cluster service account.
type KubernetesOptions struct {
	// Address is the API server; default https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT.
	Address string
	// Namespace is used for paths without one; default the pod's namespace.
	Namespace string
	// TokenFile is re-read on every request (projected tokens rotate).
	TokenFile string
	CAFile    string
	CAPEM     string
	// HTTPClient replaces the client built from the CA (tests, guarded dialer).
	HTTPClient *http.Client
	Timeout    time.Duration
	// Getenv defaults to os.Getenv.
	Getenv func(string) string
}

// KubernetesProvider reads Secret objects from the API server with plain
// HTTPS (no client-go). Reference paths are "<namespace>/<name>" or "<name>";
// Data is a JSON object of the secret's decoded data.
type KubernetesProvider struct {
	base      string
	namespace string
	tokenFile string
	// pinnedNS is true when the operator set Namespace: List then stays in it.
	pinnedNS bool
	hc       *http.Client
}

// RFC 1123 names, as Kubernetes requires for namespaces and secrets.
var kubeNameRe = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$`)

// NewKubernetesProvider resolves the in-cluster defaults.
func NewKubernetesProvider(opts KubernetesOptions) (*KubernetesProvider, error) {
	getenv := opts.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	addr := opts.Address
	if addr == "" {
		host, port := getenv("KUBERNETES_SERVICE_HOST"), getenv("KUBERNETES_SERVICE_PORT")
		if host == "" || port == "" {
			return nil, fmt.Errorf("%w: kubernetes provider needs an address outside a cluster", ErrBadProviderConfig)
		}
		addr = "https://" + net.JoinHostPort(host, port)
	}
	u, err := checkAddress(addr, "kubernetes")
	if err != nil {
		return nil, err
	}
	p := &KubernetesProvider{base: strings.TrimRight(u.String(), "/"), namespace: opts.Namespace, tokenFile: opts.TokenFile, pinnedNS: opts.Namespace != ""}
	if p.tokenFile == "" {
		p.tokenFile = kubeSATokenFile
	}
	if p.namespace == "" {
		if ns, err := readSecretFile(kubeSANamespaceFile); err == nil {
			p.namespace = ns.Reveal()
		}
	}
	if p.namespace != "" && !kubeNameRe.MatchString(p.namespace) {
		return nil, fmt.Errorf("%w: bad kubernetes namespace", ErrBadProviderConfig)
	}
	p.hc = opts.HTTPClient
	if p.hc == nil {
		caFile := opts.CAFile
		if caFile == "" && opts.CAPEM == "" && fileExists(kubeSACAFile) {
			caFile = kubeSACAFile
		}
		if p.hc, err = newHTTPClient(netsec.TLS{CAFile: caFile}, opts.CAPEM, opts.Timeout); err != nil {
			return nil, err
		}
	} else {
		p.hc = noRedirects(p.hc)
	}
	return p, nil
}

// GoString keeps %#v to the String form.
func (p *KubernetesProvider) GoString() string { return p.String() }

// String names the provider.
func (p *KubernetesProvider) String() string { return "kubernetes provider " + p.base }

// Close does nothing: requests share the client's idle pool only.
func (p *KubernetesProvider) Close() { p.hc.CloseIdleConnections() }

func (p *KubernetesProvider) nsName(path string) (string, string, error) {
	ns, name, ok := strings.Cut(path, "/")
	if !ok {
		ns, name = p.namespace, path
	}
	if ns == "" {
		return "", "", fmt.Errorf("%w: kubernetes path needs <namespace>/<name> (no default namespace)", ErrInvalidRef)
	}
	if !kubeNameRe.MatchString(ns) || !kubeNameRe.MatchString(name) {
		return "", "", fmt.Errorf("%w: kubernetes namespace and secret names are lowercase RFC 1123 names", ErrInvalidRef)
	}
	return ns, name, nil
}

func kubeErrMsg(body []byte) string {
	var st struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &st) != nil {
		return ""
	}
	return st.Message
}

func (p *KubernetesProvider) do(ctx context.Context, method, path string, body any, accept string, out any) (int, error) {
	ctx, cancel := withDefaultDeadline(ctx)
	defer cancel()
	tok, err := readSecretFile(p.tokenFile)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrLogin, err)
	}
	req, err := newJSONRequest(ctx, method, p.base+path, body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+tok.Reveal())
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	status, err := doJSON(p.hc, req, out, kubeErrMsg)
	if err != nil {
		return status, fmt.Errorf("secrets: kubernetes %s %s: %w", method, path, err)
	}
	return status, nil
}

type kubeSecret struct {
	Metadata struct {
		Name            string `json:"name"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Data map[string]string `json:"data"`
}

// Resolve GETs one Secret and decodes its data. Kubernetes keeps no history,
// so a pinned version must equal the current resourceVersion.
func (p *KubernetesProvider) Resolve(ctx context.Context, ref Ref) (Value, error) {
	ns, name, err := p.nsName(ref.Path)
	if err != nil {
		return Value{}, err
	}
	var s kubeSecret
	if _, err := p.do(ctx, http.MethodGet, "/api/v1/namespaces/"+ns+"/secrets/"+name, nil, "", &s); err != nil {
		return Value{}, err
	}
	if ref.Version != "" && ref.Version != s.Metadata.ResourceVersion {
		return Value{}, fmt.Errorf("%w: kubernetes keeps only the current version of %s", ErrNotFound, ref.Path)
	}
	fields := make(map[string]string, len(s.Data))
	for k, b64 := range s.Data {
		raw, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return Value{}, fmt.Errorf("secrets: kubernetes: key %s of %s is not base64", k, ref.Path)
		}
		fields[k] = string(raw)
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return Value{}, errors.New("secrets: kubernetes: encoding secret fields failed")
	}
	return Value{Data: netsec.Secret(data), Version: s.Metadata.ResourceVersion}, nil
}

// List returns "<namespace>/<name>" for the secrets of the prefix's
// namespace whose names start with the rest. It asks for metadata only, so
// no secret values cross the wire. A 403 (RBAC without list) is
// ErrListUnsupported.
func (p *KubernetesProvider) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	ns, namePrefix, ok := strings.Cut(prefix, "/")
	if !ok {
		ns, namePrefix = p.namespace, prefix
	}
	if ns == "" || !kubeNameRe.MatchString(ns) {
		return []string{}, nil
	}
	if p.pinnedNS && ns != p.namespace {
		// Secret names can be sensitive (§9): listing stays in the configured namespace.
		return nil, fmt.Errorf("%w: listing is limited to namespace %s", ErrAccessDenied, p.namespace)
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	q := url.Values{}
	if limit > 0 && namePrefix == "" {
		q.Set("limit", fmt.Sprint(limit))
	}
	path := "/api/v1/namespaces/" + ns + "/secrets"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	const metaOnly = "application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1"
	_, err := p.do(ctx, http.MethodGet, path, nil, metaOnly, &list)
	if errors.Is(err, ErrAccessDenied) {
		return nil, fmt.Errorf("%w: %w", ErrListUnsupported, err)
	}
	if err != nil {
		return nil, err
	}
	out := []string{}
	for _, it := range list.Items {
		if strings.HasPrefix(it.Metadata.Name, namePrefix) {
			out = append(out, ns+"/"+it.Metadata.Name)
		}
	}
	sort.Strings(out)
	return capList(out, limit), nil
}

// Keys returns the data keys of a Secret.
func (p *KubernetesProvider) Keys(ctx context.Context, ref Ref) ([]string, error) {
	v, err := p.Resolve(ctx, ref.secretID())
	if err != nil {
		return nil, err
	}
	return KeysOf(v.Data), nil
}

// Check reaches the API server, checks the identity with a
// SelfSubjectAccessReview, reads the sample and warns about write or list
// access to secrets.
func (p *KubernetesProvider) Check(ctx context.Context, sample *Ref) []spec.Check {
	const reachTitle, loginTitle = "Can reach the Kubernetes API", "Service account is accepted"
	var out []spec.Check
	rctx, cancel := context.WithTimeout(ctx, checkTimeout)
	status, err := p.do(rctx, http.MethodGet, "/version", nil, "", nil)
	cancel()
	if status == 0 && err != nil {
		reason := errorDetail(err)
		return append(out, fail(CheckReach, reachTitle, reason, "check the API server address and its CA"),
			skipped(CheckLogin, loginTitle, "API not reachable"),
			skipped(CheckRead, "Can read the sample secret", "API not reachable"),
			skipped(CheckTooMuch, "Piperoom has no more access than it needs", "API not reachable"))
	}
	out = append(out, pass(CheckReach, reachTitle, p.base))

	ns := p.namespace
	if sample != nil {
		if n, _, err := p.nsName(sample.Path); err == nil {
			ns = n
		}
	}
	can := func(verb, name string) (bool, error) {
		cctx, cancel := context.WithTimeout(ctx, checkTimeout)
		defer cancel()
		attrs := map[string]string{"namespace": ns, "verb": verb, "resource": "secrets"}
		if name != "" {
			attrs["name"] = name
		}
		body := map[string]any{
			"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview",
			"spec": map[string]any{"resourceAttributes": attrs},
		}
		var resp struct {
			Status struct {
				Allowed bool `json:"allowed"`
			} `json:"status"`
		}
		if _, err := p.do(cctx, http.MethodPost, "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", body, "", &resp); err != nil {
			return false, err
		}
		return resp.Status.Allowed, nil
	}
	if _, err := can("get", ""); errors.Is(err, ErrLogin) {
		return append(out, fail(CheckLogin, loginTitle, errorDetail(err), "mount the service-account token (automountServiceAccountToken: true)"),
			skipped(CheckRead, "Can read the sample secret", "login failed"),
			skipped(CheckTooMuch, "Piperoom has no more access than it needs", "login failed"))
	}
	out = append(out, pass(CheckLogin, loginTitle, ""))
	sc, _, _ := sampleCheck(ctx, p, sample)
	out = append(out, sc)

	const tooTitle = "Piperoom has no more access than it needs"
	if ns == "" {
		return append(out, skipped(CheckTooMuch, tooTitle, "no namespace to check"))
	}
	var problems []string
	for _, verb := range []string{"create", "update", "delete"} {
		ok, err := can(verb, "")
		if err != nil {
			return append(out, skipped(CheckTooMuch, tooTitle, "SelfSubjectAccessReview failed: "+errorDetail(err)))
		}
		if ok {
			problems = append(problems, "can "+verb+" secrets in "+ns)
		}
	}
	if ok, err := can("get", ""); err == nil && ok {
		problems = append(problems, "can read every secret in "+ns+" (use resourceNames)")
	}
	if len(problems) > 0 {
		return append(out, warn(CheckTooMuch, tooTitle, strings.Join(problems, "; "),
			"bind a Role with only get on the named secrets (resourceNames)"))
	}
	return append(out, pass(CheckTooMuch, tooTitle, "read-only on named secrets"))
}
