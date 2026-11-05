package aiven

import (
	"context"
	"fmt"
	"net/http"
	"sort"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
)

var _ clients.ConnectClient = (*Client)(nil)

// Service is what setup reads of a service. Only these fields are decoded:
// the response's secrets never enter a value of this package.
type Service struct {
	Name  string   `json:"name"`
	Type  string   `json:"type"`
	State string   `json:"state"`
	Plan  string   `json:"plan,omitempty"`
	Cloud string   `json:"cloud,omitempty"`
	Users []string `json:"users,omitempty"` // names only
}

// Service reads the client's own service.
func (c *Client) Service(ctx context.Context) (Service, error) {
	return c.ServiceNamed(ctx, c.service)
}

// ServiceNamed reads another service of the project (the Kafka service an
// integration names).
func (c *Client) ServiceNamed(ctx context.Context, name string) (Service, error) {
	if err := CheckService(name); err != nil {
		return Service{}, err
	}
	var resp struct {
		Service struct {
			ServiceName string `json:"service_name"`
			ServiceType string `json:"service_type"`
			State       string `json:"state"`
			Plan        string `json:"plan"`
			CloudName   string `json:"cloud_name"`
			Users       []struct {
				Username string `json:"username"`
			} `json:"users"`
		} `json:"service"`
	}
	e, p := path("project", c.project, "service", name)
	if err := c.do(ctx, http.MethodGet, [2]string{e, p}, nil, &resp, "get service"); err != nil {
		return Service{}, err
	}
	s := Service{Name: resp.Service.ServiceName, Type: resp.Service.ServiceType, State: resp.Service.State, Plan: resp.Service.Plan, Cloud: resp.Service.CloudName}
	for _, u := range resp.Service.Users {
		s.Users = append(s.Users, u.Username)
	}
	return s, nil
}

// Plugin is one available connector plugin.
type Plugin struct {
	Class   string `json:"class"`
	Title   string `json:"title,omitempty"`
	Type    string `json:"type,omitempty"`
	Version string `json:"version,omitempty"`
}

// Plugins lists the service's available connectors (unconfirmed shape:
// "plugins"[]).
func (c *Client) Plugins(ctx context.Context) ([]Plugin, error) {
	var resp struct {
		Plugins []Plugin `json:"plugins"`
	}
	e, p := c.servicePath("available-connectors")
	if err := c.do(ctx, http.MethodGet, [2]string{e, p}, nil, &resp, "list available connectors"); err != nil {
		return nil, err
	}
	if resp.Plugins == nil {
		resp.Plugins = []Plugin{}
	}
	return resp.Plugins, nil
}

type listed struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
}

func (c *Client) list(ctx context.Context) ([]listed, error) {
	var resp struct {
		Connectors []listed `json:"connectors"`
	}
	e, p := c.servicePath("connectors")
	if err := c.do(ctx, http.MethodGet, [2]string{e, p}, nil, &resp, "list connectors"); err != nil {
		return nil, err
	}
	return resp.Connectors, nil
}

// ListConnectors returns the connector names.
func (c *Client) ListConnectors(ctx context.Context) ([]string, error) {
	ls, err := c.list(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ls))
	for _, l := range ls {
		out = append(out, l.Name)
	}
	sort.Strings(out)
	return out, nil
}

// Config returns one connector's config from the list, redacted.
func (c *Client) Config(ctx context.Context, name string) (map[string]string, error) {
	if err := CheckName(name); err != nil {
		return nil, err
	}
	ls, err := c.list(ctx)
	if err != nil {
		return nil, err
	}
	for _, l := range ls {
		if l.Name == name {
			return actions.RedactConfig(l.Config), nil
		}
	}
	return nil, fmt.Errorf("%w: connector %q", ErrNotFound, name)
}

// Status reads a connector's status (unconfirmed shape: "status":
// {"state", "tasks"[{"id","state","trace"}]}).
func (c *Client) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	if err := CheckName(name); err != nil {
		return clients.ConnectorStatus{}, err
	}
	var resp struct {
		Status struct {
			State string `json:"state"`
			Tasks []struct {
				ID    int    `json:"id"`
				State string `json:"state"`
				Trace string `json:"trace"`
			} `json:"tasks"`
		} `json:"status"`
	}
	e, p := c.servicePath("connectors", name, "status")
	if err := c.do(ctx, http.MethodGet, [2]string{e, p}, nil, &resp, "read connector status"); err != nil {
		return clients.ConnectorStatus{}, err
	}
	st := clients.ConnectorStatus{Name: name, State: resp.Status.State, Tasks: []clients.TaskStatus{}}
	for _, t := range resp.Status.Tasks {
		st.Tasks = append(st.Tasks, clients.TaskStatus{ID: t.ID, State: t.State, Trace: clip(t.Trace, 8192)})
	}
	return st, nil
}

// CreateConnector creates a connector; cfg is the whole config (with
// "name" and "connector.class") and holds materialised secrets.
func (c *Client) CreateConnector(ctx context.Context, cfg map[string]string) (string, error) {
	name := cfg["name"]
	if err := CheckName(name); err != nil {
		return "", err
	}
	if cfg["connector.class"] == "" {
		return "", fmt.Errorf("%w: connector.class is required", ErrInvalid)
	}
	var resp struct {
		Connector struct {
			Name string `json:"name"`
		} `json:"connector"`
	}
	e, p := c.servicePath("connectors")
	if err := c.do(ctx, http.MethodPost, [2]string{e, p}, cfg, &resp, "create connector"); err != nil {
		return "", err
	}
	if resp.Connector.Name != "" {
		return resp.Connector.Name, nil
	}
	return name, nil
}

// IntegrationKafkaConnect is the integration type that joins a Kafka
// service to a Kafka Connect service.
const IntegrationKafkaConnect = "kafka_connect"

// CreateIntegration joins source (a Kafka service of the project) to the
// client's Connect service (create_service_integration's body; the
// endpoint ids are null and user_config empty, as the client sends them).
func (c *Client) CreateIntegration(ctx context.Context, source string) (string, error) {
	if err := CheckService(source); err != nil {
		return "", err
	}
	body := map[string]any{
		"source_endpoint_id": nil, "source_service": source,
		"dest_endpoint_id": nil, "dest_service": c.service,
		"integration_type": IntegrationKafkaConnect, "user_config": map[string]any{},
	}
	var resp struct {
		ServiceIntegration struct {
			ID string `json:"service_integration_id"`
		} `json:"service_integration"`
	}
	e, p := path("project", c.project, "integration")
	if err := c.do(ctx, http.MethodPost, [2]string{e, p}, body, &resp, "create integration"); err != nil {
		return "", err
	}
	return resp.ServiceIntegration.ID, nil
}

// RestartTask is not supported by this client.
func (c *Client) RestartTask(context.Context, string, int) error { return ErrNotSupported }

// RestartConnector is not supported by this client.
func (c *Client) RestartConnector(context.Context, string) error { return ErrNotSupported }
