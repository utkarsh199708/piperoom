// Package kubernetes is a small read-only Kubernetes API client over
// net/http for the incident tools that read Strimzi (internal/tools/k8stools).
// It sends GET requests only, and decodes only what those tools need: object
// metadata (labels and the strimzi.io/ annotations), an allow-listed part of
// a custom resource's spec, its status, events, pod status and a Deployment's
// status. A Secret, a pod spec (with its env), a connector's config and
// kubectl's last-applied annotation never leave this package.
//
// It is independent of internal/setup/targets/strimzi (the setup runner's
// client), which the server must not link (cmd/server/deps_test.go).
package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/tools"
)

const (
	// DefaultTimeout bounds one request (AGENTS.md rule 4).
	DefaultTimeout = 10 * time.Second
	// MaxResponseBytes caps a response body (AGENTS.md rule 7).
	MaxResponseBytes = 4 << 20
	// maxTokenFileBytes caps the token file read before each request.
	maxTokenFileBytes = 64 << 10
	// EventLimit is the most events one EventsFor call asks for.
	EventLimit = 50
	// podLimit is the most pods one Pods call asks for.
	podLimit = 100
)

// StrimziGroup is the API group of the Strimzi custom resources.
const StrimziGroup = "kafka.strimzi.io"

// StrimziVersions are the group versions tried in order: v1 (Strimzi 0.49
// and later) first, then v1beta2. A version the API server does not serve
// answers 404 without an object Status, and the next one is tried.
var StrimziVersions = []string{"v1", "v1beta2"}

// Strimzi kinds this client reads.
const (
	KindKafkaConnector = "KafkaConnector"
	KindKafkaConnect   = "KafkaConnect"
	KindKafkaTopic     = "KafkaTopic"
	KindKafka          = "Kafka"
)

// plurals maps each readable custom resource kind to its resource name.
var plurals = map[string]string{
	KindKafkaConnector: "kafkaconnectors",
	KindKafkaConnect:   "kafkaconnects",
	KindKafkaTopic:     "kafkatopics",
	KindKafka:          "kafkas",
}

// specFields are the spec fields kept per kind. Everything else in a spec is
// dropped on decode: a KafkaConnector's config may hold a credential, and a
// KafkaConnect's spec holds its authentication and external configuration.
var specFields = map[string][]string{
	KindKafkaConnector: {"class", "tasksMax", "pause", "state", "autoRestart"},
	KindKafkaConnect:   {"replicas", "version"},
	KindKafkaTopic:     {"partitions", "replicas"},
	KindKafka:          {},
}

// Errors of the client. Compare with errors.Is.
var (
	// ErrUnauthorized: 401, the token is missing, expired or rejected.
	ErrUnauthorized = errors.New("kubernetes: the API server rejected the token (401)")
	// ErrForbidden: 403, RBAC does not grant the read.
	ErrForbidden = errors.New("kubernetes: the ServiceAccount's Role does not allow this read (403)")
	// ErrNotFound: 404, no such object (or the Strimzi group is not served).
	ErrNotFound = errors.New("kubernetes: object not found (404)")
	// ErrTooLarge: a response body over MaxResponseBytes.
	ErrTooLarge = errors.New("kubernetes: API response over 4 MB")
	// ErrAPI: any other non-2xx status, or a body that does not decode.
	ErrAPI = errors.New("kubernetes: API request failed")
	// ErrBadName: a name, namespace, kind or label that is not valid; no
	// request was sent.
	ErrBadName = errors.New("kubernetes: invalid name")
	// ErrConfig: the client settings are not usable.
	ErrConfig = errors.New("kubernetes: invalid client settings")
)

var (
	// dns1123Label is a namespace or a label-sized name.
	dns1123Label = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	// dns1123Subdomain is an object name.
	dns1123Subdomain = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
	// labelValue is a label value (may be empty).
	labelValue = regexp.MustCompile(`^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$`)
	// labelName is the name part of a label key.
	labelName = regexp.MustCompile(`^([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]$`)
)

// ValidNamespace reports whether ns is a DNS-1123 label.
func ValidNamespace(ns string) bool { return len(ns) <= 63 && dns1123Label.MatchString(ns) }

// ValidName reports whether name is a DNS-1123 subdomain (an object name).
func ValidName(name string) bool { return len(name) <= 253 && dns1123Subdomain.MatchString(name) }

func checkNamespace(ns string) error {
	if !ValidNamespace(ns) {
		return fmt.Errorf("%w: namespace is not a DNS-1123 label", ErrBadName)
	}
	return nil
}

func checkName(name string) error {
	if !ValidName(name) {
		return fmt.Errorf("%w: object name is not a DNS-1123 subdomain", ErrBadName)
	}
	return nil
}

// checkLabelKey accepts [prefix/]name with a DNS-1123 subdomain prefix.
func checkLabelKey(k string) error {
	prefix, name, hasPrefix := strings.Cut(k, "/")
	if !hasPrefix {
		name, prefix = prefix, ""
	}
	if hasPrefix && (len(prefix) > 253 || !dns1123Subdomain.MatchString(prefix)) {
		return fmt.Errorf("%w: label key prefix", ErrBadName)
	}
	if len(name) > 63 || !labelName.MatchString(name) {
		return fmt.Errorf("%w: label key", ErrBadName)
	}
	return nil
}

// Config configures New.
type Config struct {
	// BaseURL is https://host[:port] of the API server. Plain http is
	// accepted only to a loopback IP (tests).
	BaseURL string
	// Token is a static bearer token. Set exactly one of Token and TokenFile.
	Token netsec.Secret
	// TokenFile is read before every request, so a projected, rotated
	// ServiceAccount token keeps working.
	TokenFile string
	// CAFile is a PEM bundle of the CAs that sign the API server's
	// certificate; empty means the system roots.
	CAFile string
	// Timeout bounds one request; zero is DefaultTimeout.
	Timeout time.Duration
	// HTTPClient replaces the built client (tests). Its timeout is used as is.
	HTTPClient *http.Client
}

// Client reads the Kubernetes API. Safe for concurrent use.
type Client struct {
	base  *url.URL
	http  *http.Client
	token func() (string, error)

	mu sync.Mutex
	// version is the Strimzi group version that answered for each kind.
	version map[string]string
}

// New builds a client. It sends no request.
func New(c Config) (*Client, error) {
	u, err := checkBaseURL(c.BaseURL)
	if err != nil {
		return nil, err
	}
	switch {
	case c.Token == "" && c.TokenFile == "":
		return nil, fmt.Errorf("%w: a token or a token file is required", ErrConfig)
	case c.Token != "" && c.TokenFile != "":
		return nil, fmt.Errorf("%w: set a token or a token file, not both", ErrConfig)
	}
	cl := &Client{base: u, http: c.HTTPClient, version: map[string]string{}}
	if c.Token != "" {
		tok := c.Token
		cl.token = func() (string, error) { return tok.Reveal(), nil }
	} else {
		file := c.TokenFile
		cl.token = func() (string, error) { return readToken(file) }
	}
	if cl.http == nil {
		if u.Scheme == "http" && c.CAFile != "" {
			return nil, fmt.Errorf("%w: a CA file is set but the URL is http://", ErrConfig)
		}
		tr, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			return nil, fmt.Errorf("%w: http.DefaultTransport is %T", ErrConfig, http.DefaultTransport)
		}
		tr = tr.Clone()
		if u.Scheme == "https" {
			tc, err := netsec.TLS{CAFile: c.CAFile}.Config()
			if err != nil {
				return nil, fmt.Errorf("kubernetes: TLS: %w", err)
			}
			tr.TLSClientConfig = tc
		}
		timeout := c.Timeout
		if timeout <= 0 {
			timeout = DefaultTimeout
		}
		cl.http = &http.Client{
			Transport: tr,
			Timeout:   timeout,
			// The bearer token must never follow a redirect to another host.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return cl, nil
}

// Host is the API server's host:port, for provenance and errors.
func (c *Client) Host() string { return c.base.Host }

// checkBaseURL accepts https://host[:port], and http only to a loopback IP.
// A path, query, fragment or user info is refused.
func checkBaseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") != "" {
		return nil, fmt.Errorf("%w: the API server URL must be https://host[:port]", ErrConfig)
	}
	switch u.Scheme {
	case "https":
	case "http":
		ip, err := netip.ParseAddr(u.Hostname())
		if err != nil || !ip.IsLoopback() {
			return nil, fmt.Errorf("%w: the API server URL must be https (plain http only to a loopback IP)", ErrConfig)
		}
	default:
		return nil, fmt.Errorf("%w: the API server URL must be https://host[:port]", ErrConfig)
	}
	u.Path = ""
	return u, nil
}

// ValidBaseURL reports whether raw is an acceptable API server URL.
func ValidBaseURL(raw string) bool {
	_, err := checkBaseURL(raw)
	return err == nil
}

func readToken(file string) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		// The OS error names the path and the reason only.
		return "", fmt.Errorf("kubernetes: reading the token file: %w", err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxTokenFileBytes+1))
	if err != nil {
		return "", fmt.Errorf("kubernetes: reading the token file: %w", err)
	}
	if len(b) > maxTokenFileBytes {
		return "", fmt.Errorf("%w: the token file is larger than %d bytes", ErrConfig, maxTokenFileBytes)
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("%w: the token file is empty", ErrConfig)
	}
	return tok, nil
}

// status is the metav1.Status body of an error response.
type status struct {
	Kind    string `json:"kind"`
	Reason  string `json:"reason"`
	Details struct {
		Name string `json:"name"`
	} `json:"details"`
}

// errNoVersion marks a 404 that is not an object's: the path itself (the
// group version or the resource) is not served.
var errNoVersion = errors.New("kubernetes: group version not served")

// get sends one GET to path (already escaped) with query q and returns the
// body. Every request is recorded as provenance; the query is shown, the
// token never.
func (c *Client) get(ctx context.Context, path string, q url.Values) ([]byte, error) {
	u := *c.base
	u.RawPath = path
	p, err := url.PathUnescape(path)
	if err != nil {
		return nil, fmt.Errorf("%w: path", ErrBadName)
	}
	u.Path = p
	u.RawQuery = q.Encode()
	text := "GET " + path
	if u.RawQuery != "" {
		text += "?" + u.RawQuery
	}
	start := time.Now()
	body, err := c.do(ctx, u.String())
	tools.Record(ctx, tools.Call{Kind: tools.CallHTTP, Target: c.base.Host, Text: text, At: start, TookMS: time.Since(start).Milliseconds()})
	return body, err
}

func (c *Client) do(ctx context.Context, rawURL string) ([]byte, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultTimeout)
		defer cancel()
	}
	tok, err := c.token()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("kubernetes: building the request: %w", ErrAPI)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			// The url.Error text repeats the URL; keep the reason only.
			err = ue.Err
		}
		return nil, fmt.Errorf("kubernetes: GET %s: %w", c.base.Host, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("kubernetes: reading the response: %w", err)
	}
	if len(body) > MaxResponseBytes {
		return nil, ErrTooLarge
	}
	switch code := resp.StatusCode; {
	case code >= 200 && code < 300:
		return body, nil
	case code == http.StatusUnauthorized:
		return nil, ErrUnauthorized
	case code == http.StatusForbidden:
		return nil, ErrForbidden
	case code == http.StatusNotFound:
		var st status
		if json.Unmarshal(body, &st) == nil && st.Kind == "Status" && st.Reason == "NotFound" && st.Details.Name != "" {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("%w: %w", ErrNotFound, errNoVersion)
	default:
		var st status
		_ = json.Unmarshal(body, &st)
		if st.Reason != "" && labelName.MatchString(st.Reason) {
			return nil, fmt.Errorf("%w: status %d (%s)", ErrAPI, code, st.Reason)
		}
		return nil, fmt.Errorf("%w: status %d", ErrAPI, code)
	}
}

func esc(s string) string { return url.PathEscape(s) }

// ObjectMeta is the part of an object's metadata the tools read. Annotations
// holds only the strimzi.io/ ones.
type ObjectMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	Generation        int64             `json:"generation"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	CreationTimestamp string            `json:"creationTimestamp,omitempty"`
	DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
}

// keepAnnotations drops every annotation outside strimzi.io/: kubectl's
// last-applied-configuration is a full copy of the spec, config included.
func keepAnnotations(in map[string]string) map[string]string {
	var out map[string]string
	for k, v := range in {
		if !strings.HasPrefix(k, "strimzi.io/") {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[k] = v
	}
	return out
}

// rawMeta decodes metadata.
type rawMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	Generation        int64             `json:"generation"`
	Labels            map[string]string `json:"labels"`
	Annotations       map[string]string `json:"annotations"`
	CreationTimestamp string            `json:"creationTimestamp"`
	DeletionTimestamp string            `json:"deletionTimestamp"`
}

func (m rawMeta) meta() ObjectMeta {
	return ObjectMeta{
		Name: m.Name, Namespace: m.Namespace, Generation: m.Generation, Labels: m.Labels,
		Annotations: keepAnnotations(m.Annotations), CreationTimestamp: m.CreationTimestamp, DeletionTimestamp: m.DeletionTimestamp,
	}
}

// Resource is a Strimzi custom resource as the tools see it.
type Resource struct {
	Kind       string `json:"kind"`
	APIVersion string `json:"apiVersion"`
	Metadata   ObjectMeta
	// Spec holds only the allow-listed fields of the kind (specFields).
	Spec map[string]json.RawMessage
	// Status is the resource's status as the operator wrote it.
	Status json.RawMessage
}

// CustomResource reads one Strimzi custom resource of kind (KafkaConnector,
// KafkaConnect, KafkaTopic or Kafka) in ns, trying StrimziVersions in order
// and remembering the one that answered.
func (c *Client) CustomResource(ctx context.Context, kind, ns, name string) (Resource, error) {
	plural, ok := plurals[kind]
	if !ok {
		return Resource{}, fmt.Errorf("%w: kind is not a readable Strimzi kind", ErrBadName)
	}
	if err := checkNamespace(ns); err != nil {
		return Resource{}, err
	}
	if err := checkName(name); err != nil {
		return Resource{}, err
	}
	versions := StrimziVersions
	c.mu.Lock()
	if v, ok := c.version[kind]; ok {
		versions = []string{v}
	}
	c.mu.Unlock()
	var lastErr error
	for _, v := range versions {
		path := "/apis/" + StrimziGroup + "/" + v + "/namespaces/" + esc(ns) + "/" + plural + "/" + esc(name)
		body, err := c.get(ctx, path, nil)
		if errors.Is(err, errNoVersion) {
			lastErr = err
			continue
		}
		if err != nil {
			return Resource{}, err
		}
		c.mu.Lock()
		c.version[kind] = v
		c.mu.Unlock()
		return decodeResource(kind, body)
	}
	return Resource{}, fmt.Errorf("kubernetes: %s.%s is not served in versions %s: %w", plural, StrimziGroup, strings.Join(StrimziVersions, ", "), lastErr)
}

func decodeResource(kind string, body []byte) (Resource, error) {
	var raw struct {
		Kind       string                     `json:"kind"`
		APIVersion string                     `json:"apiVersion"`
		Metadata   rawMeta                    `json:"metadata"`
		Spec       map[string]json.RawMessage `json:"spec"`
		Status     json.RawMessage            `json:"status"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return Resource{}, fmt.Errorf("%w: %s does not decode", ErrAPI, kind)
	}
	r := Resource{Kind: raw.Kind, APIVersion: raw.APIVersion, Metadata: raw.Metadata.meta(), Status: raw.Status}
	for _, f := range specFields[kind] {
		if v, ok := raw.Spec[f]; ok {
			if r.Spec == nil {
				r.Spec = map[string]json.RawMessage{}
			}
			r.Spec[f] = v
		}
	}
	return r, nil
}

// Event is one core/v1 Event.
type Event struct {
	Type           string `json:"type"`
	Reason         string `json:"reason"`
	Message        string `json:"message"`
	Count          int32  `json:"count"`
	FirstTimestamp string `json:"firstTimestamp,omitempty"`
	LastTimestamp  string `json:"lastTimestamp,omitempty"`
	EventTime      string `json:"eventTime,omitempty"`
	Source         string `json:"source,omitempty"`
}

// When returns the event's latest time: lastTimestamp, else eventTime, else
// firstTimestamp (RFC 3339 strings sort by time).
func (e Event) When() string {
	switch {
	case e.LastTimestamp != "":
		return e.LastTimestamp
	case e.EventTime != "":
		return e.EventTime
	}
	return e.FirstTimestamp
}

// EventsFor lists the events of one object in ns (kind and name of the
// involved object), at most EventLimit, newest first.
func (c *Client) EventsFor(ctx context.Context, ns, kind, name string) ([]Event, error) {
	if err := checkNamespace(ns); err != nil {
		return nil, err
	}
	if err := checkName(name); err != nil {
		return nil, err
	}
	if !labelName.MatchString(kind) || len(kind) > 63 {
		return nil, fmt.Errorf("%w: kind", ErrBadName)
	}
	q := url.Values{}
	q.Set("fieldSelector", "involvedObject.name="+name+",involvedObject.kind="+kind)
	q.Set("limit", fmt.Sprint(EventLimit))
	body, err := c.get(ctx, "/api/v1/namespaces/"+esc(ns)+"/events", q)
	if err != nil {
		return nil, err
	}
	var raw struct {
		Items []struct {
			Type           string `json:"type"`
			Reason         string `json:"reason"`
			Message        string `json:"message"`
			Count          int32  `json:"count"`
			FirstTimestamp string `json:"firstTimestamp"`
			LastTimestamp  string `json:"lastTimestamp"`
			EventTime      string `json:"eventTime"`
			Source         struct {
				Component string `json:"component"`
			} `json:"source"`
			ReportingController string `json:"reportingComponent"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("%w: event list does not decode", ErrAPI)
	}
	out := make([]Event, 0, len(raw.Items))
	for _, it := range raw.Items {
		src := it.Source.Component
		if src == "" {
			src = it.ReportingController
		}
		out = append(out, Event{Type: it.Type, Reason: it.Reason, Message: it.Message, Count: it.Count,
			FirstTimestamp: it.FirstTimestamp, LastTimestamp: it.LastTimestamp, EventTime: it.EventTime, Source: src})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].When() > out[j].When() })
	return out, nil
}

// ContainerStatus is one container's state; never its spec or env.
type ContainerStatus struct {
	Name         string `json:"name"`
	Ready        bool   `json:"ready"`
	RestartCount int32  `json:"restart_count"`
	// State is running, waiting or terminated; Reason is the waiting or
	// terminated reason (CrashLoopBackOff, OOMKilled, Error, ...).
	State    string `json:"state"`
	Reason   string `json:"reason,omitempty"`
	ExitCode *int32 `json:"exit_code,omitempty"`
	// LastTermination is the previous run's end: reason, exit code, time.
	LastTerminationReason string `json:"last_termination_reason,omitempty"`
	LastTerminationCode   *int32 `json:"last_termination_exit_code,omitempty"`
	LastTerminationAt     string `json:"last_termination_at,omitempty"`
}

// Pod is a pod's status.
type Pod struct {
	Name       string            `json:"name"`
	Labels     map[string]string `json:"labels,omitempty"`
	Phase      string            `json:"phase"`
	Reason     string            `json:"reason,omitempty"`
	StartTime  string            `json:"start_time,omitempty"`
	Deleting   bool              `json:"deleting,omitempty"`
	Containers []ContainerStatus `json:"containers"`
}

type rawState struct {
	Running *struct {
		StartedAt string `json:"startedAt"`
	} `json:"running"`
	Waiting *struct {
		Reason string `json:"reason"`
	} `json:"waiting"`
	Terminated *struct {
		Reason     string `json:"reason"`
		ExitCode   int32  `json:"exitCode"`
		FinishedAt string `json:"finishedAt"`
	} `json:"terminated"`
}

// Pods lists the pods in ns that match every label of selector (at most
// 100). Only status is decoded: a pod spec holds env values.
func (c *Client) Pods(ctx context.Context, ns string, selector map[string]string) ([]Pod, error) {
	if err := checkNamespace(ns); err != nil {
		return nil, err
	}
	sel, err := labelSelector(selector)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	q.Set("labelSelector", sel)
	q.Set("limit", fmt.Sprint(podLimit))
	body, err := c.get(ctx, "/api/v1/namespaces/"+esc(ns)+"/pods", q)
	if err != nil {
		return nil, err
	}
	var raw struct {
		Items []struct {
			Metadata rawMeta `json:"metadata"`
			Status   struct {
				Phase             string `json:"phase"`
				Reason            string `json:"reason"`
				StartTime         string `json:"startTime"`
				ContainerStatuses []struct {
					Name                 string   `json:"name"`
					Ready                bool     `json:"ready"`
					RestartCount         int32    `json:"restartCount"`
					State                rawState `json:"state"`
					LastTerminationState rawState `json:"lastState"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("%w: pod list does not decode", ErrAPI)
	}
	out := make([]Pod, 0, len(raw.Items))
	for _, it := range raw.Items {
		p := Pod{Name: it.Metadata.Name, Labels: it.Metadata.Labels, Phase: it.Status.Phase, Reason: it.Status.Reason,
			StartTime: it.Status.StartTime, Deleting: it.Metadata.DeletionTimestamp != ""}
		for _, cs := range it.Status.ContainerStatuses {
			s := ContainerStatus{Name: cs.Name, Ready: cs.Ready, RestartCount: cs.RestartCount}
			switch st := cs.State; {
			case st.Terminated != nil:
				code := st.Terminated.ExitCode
				s.State, s.Reason, s.ExitCode = "terminated", st.Terminated.Reason, &code
			case st.Waiting != nil:
				s.State, s.Reason = "waiting", st.Waiting.Reason
			case st.Running != nil:
				s.State = "running"
			}
			if lt := cs.LastTerminationState.Terminated; lt != nil {
				code := lt.ExitCode
				s.LastTerminationReason, s.LastTerminationCode, s.LastTerminationAt = lt.Reason, &code, lt.FinishedAt
			}
			p.Containers = append(p.Containers, s)
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// labelSelector builds "k=v,..." (sorted) after checking every key and value.
func labelSelector(sel map[string]string) (string, error) {
	if len(sel) == 0 {
		return "", fmt.Errorf("%w: empty label selector", ErrBadName)
	}
	keys := make([]string, 0, len(sel))
	for k := range sel {
		if err := checkLabelKey(k); err != nil {
			return "", err
		}
		if v := sel[k]; len(v) > 63 || !labelValue.MatchString(v) {
			return "", fmt.Errorf("%w: label value", ErrBadName)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + sel[k]
	}
	return strings.Join(parts, ","), nil
}

// Condition is a status condition (Deployments and Strimzi resources share
// the shape).
type Condition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

// Deployment is a Deployment's status and its pod selector.
type Deployment struct {
	Name                string            `json:"name"`
	Generation          int64             `json:"generation"`
	ObservedGeneration  int64             `json:"observed_generation"`
	DesiredReplicas     int32             `json:"desired_replicas"`
	Replicas            int32             `json:"replicas"`
	ReadyReplicas       int32             `json:"ready_replicas"`
	AvailableReplicas   int32             `json:"available_replicas"`
	UpdatedReplicas     int32             `json:"updated_replicas"`
	UnavailableReplicas int32             `json:"unavailable_replicas"`
	Conditions          []Condition       `json:"conditions,omitempty"`
	Selector            map[string]string `json:"selector,omitempty"`
}

// Deployment reads one apps/v1 Deployment's status and spec.selector's
// matchLabels; the pod template (with its env) is not decoded.
func (c *Client) Deployment(ctx context.Context, ns, name string) (Deployment, error) {
	if err := checkNamespace(ns); err != nil {
		return Deployment{}, err
	}
	if err := checkName(name); err != nil {
		return Deployment{}, err
	}
	body, err := c.get(ctx, "/apis/apps/v1/namespaces/"+esc(ns)+"/deployments/"+esc(name), nil)
	if err != nil {
		return Deployment{}, err
	}
	var raw struct {
		Metadata rawMeta `json:"metadata"`
		Spec     struct {
			Replicas *int32 `json:"replicas"`
			Selector struct {
				MatchLabels map[string]string `json:"matchLabels"`
			} `json:"selector"`
		} `json:"spec"`
		Status struct {
			ObservedGeneration  int64       `json:"observedGeneration"`
			Replicas            int32       `json:"replicas"`
			ReadyReplicas       int32       `json:"readyReplicas"`
			AvailableReplicas   int32       `json:"availableReplicas"`
			UpdatedReplicas     int32       `json:"updatedReplicas"`
			UnavailableReplicas int32       `json:"unavailableReplicas"`
			Conditions          []Condition `json:"conditions"`
		} `json:"status"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return Deployment{}, fmt.Errorf("%w: deployment does not decode", ErrAPI)
	}
	d := Deployment{
		Name: raw.Metadata.Name, Generation: raw.Metadata.Generation, ObservedGeneration: raw.Status.ObservedGeneration,
		DesiredReplicas: 1, Replicas: raw.Status.Replicas, ReadyReplicas: raw.Status.ReadyReplicas,
		AvailableReplicas: raw.Status.AvailableReplicas, UpdatedReplicas: raw.Status.UpdatedReplicas,
		UnavailableReplicas: raw.Status.UnavailableReplicas, Conditions: raw.Status.Conditions,
		Selector: raw.Spec.Selector.MatchLabels,
	}
	if raw.Spec.Replicas != nil {
		d.DesiredReplicas = *raw.Spec.Replicas
	}
	return d, nil
}
