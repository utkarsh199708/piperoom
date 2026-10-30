package confluentcloud

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/netsec"
)

var _ clients.ConnectClient = (*Client)(nil)

// Plugin is one entry of connector-plugins.
type Plugin struct {
	Class   string `json:"class"`
	Type    string `json:"type"`
	Version string `json:"version,omitempty"`
}

// Plugins lists the managed connector plugins of the cluster.
func (c *Client) Plugins(ctx context.Context) ([]Plugin, error) {
	var out []Plugin
	if err := do(ctx, c.http, c.base, http.MethodGet, c.connectPath("connector-plugins"), "", nil, &out, "list connector plugins"); err != nil {
		return nil, err
	}
	if out == nil {
		out = []Plugin{}
	}
	return out, nil
}

// ConfigError is one config of a validate result that has errors.
type ConfigError struct {
	Name   string   `json:"name"`
	Errors []string `json:"errors"`
}

// Validation is a validate result without any config value.
type Validation struct {
	ErrorCount int           `json:"error_count"`
	Errors     []ConfigError `json:"errors"`
}

// Validate validates cfg against the plugin's definition
// (validateConnectv1ConnectorPlugin). Only the names and the error messages
// are kept: the response echoes every value, the secrets included.
func (c *Client) Validate(ctx context.Context, plugin string, cfg map[string]string) (Validation, error) {
	if err := CheckPlugin(plugin); err != nil {
		return Validation{}, err
	}
	if len(cfg) == 0 {
		return Validation{}, fmt.Errorf("%w: an empty config", ErrInvalid)
	}
	var resp struct {
		ErrorCount int `json:"error_count"`
		Configs    []struct {
			Value struct {
				Name   string   `json:"name"`
				Errors []string `json:"errors"`
			} `json:"value"`
		} `json:"configs"`
	}
	if err := do(ctx, c.http, c.base, http.MethodPut, c.connectPath("connector-plugins", plugin, "config", "validate"), "", cfg, &resp, "validate connector config"); err != nil {
		return Validation{}, err
	}
	v := Validation{ErrorCount: resp.ErrorCount, Errors: []ConfigError{}}
	for _, cf := range resp.Configs {
		if len(cf.Value.Errors) == 0 {
			continue
		}
		errs := make([]string, 0, len(cf.Value.Errors))
		for _, e := range cf.Value.Errors {
			errs = append(errs, clip(e, 400))
		}
		v.Errors = append(v.Errors, ConfigError{Name: clip(cf.Value.Name, 128), Errors: errs})
	}
	return v, nil
}

// Created is a created connector, its config redacted.
type Created struct {
	Name   string            `json:"name"`
	Type   string            `json:"type,omitempty"`
	Config map[string]string `json:"config,omitempty"`
}

// CreateConnector creates a connector (createConnectv1Connector). cfg holds
// materialised secrets; the result's config is redacted.
func (c *Client) CreateConnector(ctx context.Context, name string, cfg map[string]string) (Created, error) {
	if err := CheckName(name); err != nil {
		return Created{}, err
	}
	if len(cfg) == 0 {
		return Created{}, fmt.Errorf("%w: an empty config", ErrInvalid)
	}
	var resp struct {
		Name   string            `json:"name"`
		Type   string            `json:"type"`
		Config map[string]string `json:"config"`
	}
	body := map[string]any{"name": name, "config": cfg}
	if err := do(ctx, c.http, c.base, http.MethodPost, c.connectPath("connectors"), "", body, &resp, "create connector"); err != nil {
		return Created{}, err
	}
	out := Created{Name: resp.Name, Type: resp.Type}
	if resp.Config != nil {
		out.Config = actions.RedactConfig(resp.Config)
	}
	return out, nil
}

// ListConnectors returns the connector names (an array of strings).
func (c *Client) ListConnectors(ctx context.Context) ([]string, error) {
	var out []string
	if err := do(ctx, c.http, c.base, http.MethodGet, c.connectPath("connectors"), "", nil, &out, "list connectors"); err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// Status reads a connector's status (readConnectv1ConnectorStatus): the
// Connect REST shape, with a task's "msg" as its Trace.
func (c *Client) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	if err := CheckName(name); err != nil {
		return clients.ConnectorStatus{}, err
	}
	var resp struct {
		Name      string `json:"name"`
		Type      string `json:"type"`
		Connector struct {
			State    string `json:"state"`
			WorkerID string `json:"worker_id"`
			Trace    string `json:"trace"`
		} `json:"connector"`
		Tasks []struct {
			ID       int    `json:"id"`
			State    string `json:"state"`
			WorkerID string `json:"worker_id"`
			Msg      string `json:"msg"`
		} `json:"tasks"`
	}
	if err := do(ctx, c.http, c.base, http.MethodGet, c.connectPath("connectors", name, "status"), "", nil, &resp, "read connector status"); err != nil {
		return clients.ConnectorStatus{}, err
	}
	st := clients.ConnectorStatus{Name: resp.Name, Type: resp.Type, State: resp.Connector.State, WorkerID: resp.Connector.WorkerID,
		Trace: clip(resp.Connector.Trace, 8192), Tasks: []clients.TaskStatus{}}
	for _, t := range resp.Tasks {
		st.Tasks = append(st.Tasks, clients.TaskStatus{ID: t.ID, State: t.State, WorkerID: t.WorkerID, Trace: clip(t.Msg, 8192)})
	}
	return st, nil
}

// Config reads a connector's config, redacted.
func (c *Client) Config(ctx context.Context, name string) (map[string]string, error) {
	if err := CheckName(name); err != nil {
		return nil, err
	}
	var resp map[string]string
	if err := do(ctx, c.http, c.base, http.MethodGet, c.connectPath("connectors", name, "config"), "", nil, &resp, "read connector config"); err != nil {
		return nil, err
	}
	return actions.RedactConfig(resp), nil
}

// RestartTask is not supported by this client.
func (c *Client) RestartTask(context.Context, string, int) error { return ErrNotSupported }

// RestartConnector is not supported by this client.
func (c *Client) RestartConnector(context.Context, string) error { return ErrNotSupported }

// Cluster is the CMK v2 view of the Kafka cluster.
type Cluster struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	Cloud             string `json:"cloud,omitempty"`
	Region            string `json:"region,omitempty"`
	Availability      string `json:"availability,omitempty"`
	BootstrapEndpoint string `json:"bootstrap_endpoint,omitempty"`
	HTTPEndpoint      string `json:"http_endpoint,omitempty"`
	Phase             string `json:"phase,omitempty"`
}

// Cluster reads the cluster (getCmkV2Cluster with ?environment=).
func (c *Client) Cluster(ctx context.Context) (Cluster, error) {
	var resp struct {
		ID   string `json:"id"`
		Spec struct {
			DisplayName            string `json:"display_name"`
			Availability           string `json:"availability"`
			Cloud                  string `json:"cloud"`
			Region                 string `json:"region"`
			KafkaBootstrapEndpoint string `json:"kafka_bootstrap_endpoint"`
			HTTPEndpoint           string `json:"http_endpoint"`
		} `json:"spec"`
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
	}
	q := url.Values{"environment": {c.env}}.Encode()
	if err := do(ctx, c.http, c.base, http.MethodGet, "/cmk/v2/clusters/"+url.PathEscape(c.cluster), q, nil, &resp, "read cluster"); err != nil {
		return Cluster{}, err
	}
	return Cluster{ID: resp.ID, Name: resp.Spec.DisplayName, Cloud: resp.Spec.Cloud, Region: resp.Spec.Region, Availability: resp.Spec.Availability,
		BootstrapEndpoint: resp.Spec.KafkaBootstrapEndpoint, HTTPEndpoint: resp.Spec.HTTPEndpoint, Phase: resp.Status.Phase}, nil
}

// ACL is one Kafka REST v3 ACL (CreateAclRequestData).
type ACL struct {
	ResourceType string `json:"resource_type"` // TOPIC | GROUP | CLUSTER | TRANSACTIONAL_ID
	ResourceName string `json:"resource_name"`
	PatternType  string `json:"pattern_type"` // LITERAL | PREFIXED
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	Operation    string `json:"operation"`
	Permission   string `json:"permission"` // ALLOW
}

var (
	aclResources   = map[string]bool{"TOPIC": true, "GROUP": true, "CLUSTER": true, "TRANSACTIONAL_ID": true}
	aclPatterns    = map[string]bool{"LITERAL": true, "PREFIXED": true}
	aclOperations  = map[string]bool{"READ": true, "WRITE": true, "CREATE": true, "DESCRIBE": true, "DESCRIBE_CONFIGS": true, "IDEMPOTENT_WRITE": true}
	aclPermissions = map[string]bool{"ALLOW": true}
)

func (a ACL) check() error {
	if !aclResources[a.ResourceType] || !aclPatterns[a.PatternType] || !aclOperations[a.Operation] || !aclPermissions[a.Permission] {
		return fmt.Errorf("%w: ACL %s %s %s %s is outside the allowed set", ErrInvalid, a.ResourceType, a.PatternType, a.Operation, a.Permission)
	}
	if a.Host != "*" {
		return fmt.Errorf("%w: ACL host %q, want *", ErrInvalid, clip(a.Host, 32))
	}
	if a.ResourceType == "CLUSTER" {
		if a.ResourceName != "kafka-cluster" {
			return fmt.Errorf("%w: a CLUSTER ACL names kafka-cluster", ErrInvalid)
		}
	} else if err := CheckResource(a.ResourceName); err != nil {
		return err
	}
	return CheckPrincipal(a.Principal)
}

// ACLClient creates ACLs on the cluster's own REST endpoint with a cluster
// API key.
type ACLClient struct {
	base    *url.URL
	cluster string
	http    *http.Client
}

// ACLConfig builds an ACLClient. Endpoint is the cluster's http_endpoint
// (Cluster.HTTPEndpoint): https and a host under confluent.cloud.
type ACLConfig struct {
	Endpoint   string
	Cluster    string
	Key        string
	Secret     netsec.Secret
	Transport  http.RoundTripper
	baseSuffix string
}

// NewACLClient checks cfg and builds the client.
func NewACLClient(cfg ACLConfig) (*ACLClient, error) {
	if err := CheckCluster(cfg.Cluster); err != nil {
		return nil, err
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" {
		return nil, fmt.Errorf("%w: the cluster REST endpoint is not https://host[:port]", ErrInvalid)
	}
	suffix := cfg.baseSuffix
	if suffix == "" {
		suffix = ".confluent.cloud"
	}
	if !strings.HasSuffix(u.Hostname(), suffix) {
		return nil, fmt.Errorf("%w: the cluster REST endpoint is not under %s", ErrInvalid, suffix)
	}
	hc, base, err := authClient(u.Scheme+"://"+u.Host, cfg.Key, cfg.Secret, cfg.Transport)
	if err != nil {
		return nil, err
	}
	return &ACLClient{base: base, cluster: cfg.Cluster, http: hc}, nil
}

// CreateACL creates one ACL (createKafkaAcls, 201 with no body).
func (a *ACLClient) CreateACL(ctx context.Context, acl ACL) error {
	if err := acl.check(); err != nil {
		return err
	}
	return do(ctx, a.http, a.base, http.MethodPost, "/kafka/v3/clusters/"+url.PathEscape(a.cluster)+"/acls", "", acl, nil, "create ACL")
}
