package mskconnect

import (
	"context"
	"errors"

	"piperoom/internal/clients"
)

// The clients.ConnectClient view of MSK Connect. A connector is named by its
// name, as the REST client names it; the ARN is looked up per call (msk.md
// §3a keeps no cache in v1).
var _ clients.ConnectClient = (*Client)(nil)

// ListConnectors returns every connector name in the region.
func (c *Client) ListConnectors(ctx context.Context) ([]string, error) {
	sums, err := c.listConnectors(ctx, "")
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(sums))
	for _, s := range sums {
		out = append(out, str(s.ConnectorName))
	}
	return out, nil
}

// Status maps a connector's state onto clients.ConnectorStatus. MSK Connect
// reports no per-task state and no worker id, so Tasks is empty; CREATING
// and UPDATING read as UNASSIGNED (not running yet), DELETING as STOPPED,
// and a FAILED connector's StateDescription is the Trace (untrusted text).
func (c *Client) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	arn, err := c.FindConnector(ctx, name)
	if err != nil {
		return clients.ConnectorStatus{}, err
	}
	cn, err := c.DescribeConnector(ctx, arn)
	if err != nil {
		return clients.ConnectorStatus{}, err
	}
	st := clients.ConnectorStatus{Name: cn.Name, State: MapState(cn.State), Tasks: []clients.TaskStatus{}}
	if cn.State == "FAILED" && (cn.StateCode != "" || cn.StateMessage != "") {
		st.Trace = cn.StateCode + ": " + cn.StateMessage
	}
	return st, nil
}

// MapState maps an MSK Connect connector state to the Connect REST state
// vocabulary of clients.ConnectorStatus.
func MapState(s string) string {
	switch s {
	case "RUNNING":
		return "RUNNING"
	case "FAILED":
		return "FAILED"
	case "DELETING":
		return "STOPPED"
	case "CREATING", "UPDATING":
		return "UNASSIGNED"
	}
	return "UNASSIGNED"
}

// Config returns the connector's configuration, redacted.
func (c *Client) Config(ctx context.Context, name string) (map[string]string, error) {
	arn, err := c.FindConnector(ctx, name)
	if err != nil {
		return nil, err
	}
	cn, err := c.DescribeConnector(ctx, arn)
	if err != nil {
		return nil, err
	}
	if cn.Config == nil {
		return map[string]string{}, nil
	}
	return cn.Config, nil
}

// RestartTask is not supported by MSK Connect.
func (c *Client) RestartTask(context.Context, string, int) error {
	return ErrNotSupported
}

// RestartConnector is not supported by MSK Connect.
func (c *Client) RestartConnector(context.Context, string) error {
	return ErrNotSupported
}

// ErrorClass returns a stable class name for err: bad_request, forbidden,
// not_found, conflict, throttled, invalid or aws.
func ErrorClass(err error) string {
	switch {
	case errors.Is(err, ErrBadRequest):
		return "bad_request"
	case errors.Is(err, ErrForbidden):
		return "forbidden"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrConflict):
		return "conflict"
	case errors.Is(err, ErrThrottled):
		return "throttled"
	case errors.Is(err, ErrInvalid):
		return "invalid"
	case err == nil:
		return ""
	}
	return "aws"
}
