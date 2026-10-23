package remoteclients

import (
	"context"
	"fmt"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/setup/protocol"
)

// Connect is the Kafka Connect client over the runner: the reads the Q tools
// take (clients.ConnectReader), the Q4a config validator, the Q5a lifecycle
// writer and offsets reader, the Q1c plugin list and Connect's own version.
//
// Result shapes (the runner's OpResult.Data; each is pinned by a test):
//
//	connect_list_connectors     {"connectors": ["name", ...]}
//	connect_status              {"status": clients.ConnectorStatus}
//	connect_config              {"config": {"key": "value", ...}}
//	connect_validate_config     {"validate": {"error_count": n, "errors": {"key": ["msg"]}}}
//	connect_plugin_config       {"definitions": [connect.ConfigKeyDef, ...]}
//	connect_plugins             {"plugins": [connect.Plugin, ...]}
//	connect_source_offsets      {"offsets": [connect.SourceOffset, ...]}
//	connect_server_info         {"server_info": connect.ServerInfo}
//
// A write op returns Data only when it has something to say; the method
// ignores it. A failed result may carry {"error_code": ...} (see ErrorCode*)
// so the Q actions can match their sentinels.
//
// Every write is the write of the proposal step in the call's context, sent
// with the step's frozen params (Session.doStep): the method's arguments are
// checked against those params, never sent in their place.
type Connect struct{ s *Session }

var (
	_ clients.ConnectClient          = (*Connect)(nil)
	_ connect.ConnectLifecycleWriter = (*Connect)(nil)
	_ connect.SourceOffsetReader     = (*Connect)(nil)
	_ connect.ConnectInfoReader      = (*Connect)(nil)
	_ connect.PluginReader           = (*Connect)(nil)
)

// ---- params and result envelopes ----

type connectorParams struct {
	Connector string `json:"connector"`
}

type classParams struct {
	Class string `json:"class"`
}

type validateParams struct {
	Class  string            `json:"class"`
	Config map[string]string `json:"config"`
}

type createConnectorParams struct {
	Connector string            `json:"connector"`
	Config    map[string]string `json:"config"`
}

type restartTaskParams struct {
	Connector string `json:"connector"`
	Task      int    `json:"task"`
}

// resetOffsetsParams is the part of a connect_reset_source_offsets step the
// reset and alter writes check: the connector and the one position the step
// moves to (null for a reset that clears every offset). The step's other
// params (stop_first) travel unchanged with it.
type resetOffsetsParams struct {
	Connector string          `json:"connector"`
	Position  *offsetPosition `json:"position"`
}

// offsetPosition is one partition's position as the reset step names it.
type offsetPosition struct {
	Partition map[string]any `json:"partition"`
	Offset    map[string]any `json:"offset"`
}

// ListConnectors lists every connector name Connect knows
// (GET /connectors). The list is cluster metadata, not filtered by the
// pipeline; the callers scope it.
func (c *Connect) ListConnectors(ctx context.Context) ([]string, error) {
	res, err := c.s.do(ctx, OpConnectListConnectors, protocol.TierT0, nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Connectors []string `json:"connectors"`
	}
	if err := decode(res, "connectors", &out.Connectors); err != nil {
		return nil, err
	}
	return out.Connectors, nil
}

// Status reads one connector's state, worker and task states
// (GET /connectors/{name}/status).
func (c *Connect) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	if err := requireArg(OpConnectStatus, "connector", name); err != nil {
		return clients.ConnectorStatus{}, err
	}
	res, err := c.s.do(ctx, OpConnectStatus, protocol.TierT0, connectorParams{Connector: name})
	if err != nil {
		return clients.ConnectorStatus{}, err
	}
	var out clients.ConnectorStatus
	if err := decode(res, "status", &out); err != nil {
		return clients.ConnectorStatus{}, err
	}
	return out, nil
}

// Config reads one connector's stored configuration. The values can hold
// credentials; the method returns them to the caller that needs them and puts
// none of them in an error.
func (c *Connect) Config(ctx context.Context, name string) (map[string]string, error) {
	if err := requireArg(OpConnectConfig, "connector", name); err != nil {
		return nil, err
	}
	res, err := c.s.do(ctx, OpConnectConfig, protocol.TierT0, connectorParams{Connector: name})
	if err != nil {
		return nil, err
	}
	var out map[string]string
	if err := decode(res, "config", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ValidateConfig asks Connect to validate cfg for class and creates nothing
// (PUT /connector-plugins/{class}/config/validate).
func (c *Connect) ValidateConfig(ctx context.Context, class string, cfg map[string]string) (connect.ValidateResult, error) {
	if err := requireArg(OpConnectValidateConfig, "class", class); err != nil {
		return connect.ValidateResult{}, err
	}
	res, err := c.s.do(ctx, OpConnectValidateConfig, protocol.TierT0, validateParams{Class: class, Config: cfg})
	if err != nil {
		return connect.ValidateResult{}, err
	}
	// connect.ValidateResult has no JSON tags, so the wire shape is decoded
	// locally and copied (coding rule: no new contract, no invented tags).
	var wire struct {
		ErrorCount int                 `json:"error_count"`
		Errors     map[string][]string `json:"errors"`
	}
	if err := decode(res, "validate", &wire); err != nil {
		return connect.ValidateResult{}, err
	}
	out := connect.ValidateResult{ErrorCount: wire.ErrorCount, Errors: map[string][]string{}}
	for k, v := range wire.Errors {
		if len(v) > 0 {
			out.Errors[k] = v
		}
	}
	return out, nil
}

// PluginConfigDef lists a plugin's known configuration keys
// (GET /connector-plugins/{class}/config, Kafka Connect 3.2+). A worker
// without the endpoint answers clients.ErrNotSupported or clients.ErrNotFound,
// exactly as connect.Client does.
func (c *Connect) PluginConfigDef(ctx context.Context, class string) ([]connect.ConfigKeyDef, error) {
	if err := requireArg(OpConnectPluginConfig, "class", class); err != nil {
		return nil, err
	}
	res, err := c.s.do(ctx, OpConnectPluginConfig, protocol.TierT0, classParams{Class: class})
	if err != nil {
		return nil, err
	}
	var out []connect.ConfigKeyDef
	if err := decode(res, "definitions", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Plugins lists the worker's plugins (GET /connector-plugins, Kafka Connect
// 3.2+ with connectorsOnly=false).
func (c *Connect) Plugins(ctx context.Context) ([]connect.Plugin, error) {
	res, err := c.s.do(ctx, OpConnectPlugins, protocol.TierT0, nil)
	if err != nil {
		return nil, err
	}
	var out []connect.Plugin
	if err := decode(res, "plugins", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// SourceOffsets reads a connector's stored offsets (KIP-875,
// GET /connectors/{name}/offsets, Kafka 3.5+). A Connect without the endpoint
// answers clients.ErrNotSupported.
func (c *Connect) SourceOffsets(ctx context.Context, connector string) ([]connect.SourceOffset, error) {
	if err := requireArg(OpConnectSourceOffsets, "connector", connector); err != nil {
		return nil, err
	}
	res, err := c.s.do(ctx, OpConnectSourceOffsets, protocol.TierT0, connectorParams{Connector: connector})
	if err != nil {
		return nil, err
	}
	var out []connect.SourceOffset
	if err := decode(res, "offsets", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ServerInfo reads Connect's own version (GET /): the worker's version,
// commit and Kafka cluster id.
func (c *Connect) ServerInfo(ctx context.Context) (connect.ServerInfo, error) {
	res, err := c.s.do(ctx, OpConnectServerInfo, protocol.TierT0, nil)
	if err != nil {
		return connect.ServerInfo{}, err
	}
	var out connect.ServerInfo
	if err := decode(res, "server_info", &out); err != nil {
		return connect.ServerInfo{}, err
	}
	return out, nil
}

// RestartTask restarts one task of a connector (POST
// /connectors/{name}/tasks/{task}/restart). T2.
func (c *Connect) RestartTask(ctx context.Context, name string, task int) error {
	if err := requireArg(OpConnectRestartTask, "connector", name); err != nil {
		return err
	}
	if task < 0 {
		return fmt.Errorf("remoteclients: %s: task %d is not a task id: %w", OpConnectRestartTask, task, ErrBadArgs)
	}
	_, err := c.s.doStep(ctx, OpConnectRestartTask, protocol.TierT2, restartTaskParams{Connector: name, Task: task})
	return err
}

// RestartConnector restarts a connector and its tasks (POST
// /connectors/{name}/restart). T2.
func (c *Connect) RestartConnector(ctx context.Context, name string) error {
	return c.write(ctx, OpConnectRestartConnector, connectorParams{Connector: name}, name)
}

// CreateConnector registers a new connector (POST /connectors). T2. The config
// is sent in the op's params, never in an error or a log line.
func (c *Connect) CreateConnector(ctx context.Context, name string, cfg map[string]string) error {
	if err := requireArg(OpConnectCreateConnector, "connector", name); err != nil {
		return err
	}
	if len(cfg) == 0 {
		return fmt.Errorf("remoteclients: %s: connector %q has an empty config: %w", OpConnectCreateConnector, name, ErrBadArgs)
	}
	_, err := c.s.doStep(ctx, OpConnectCreateConnector, protocol.TierT2, createConnectorParams{Connector: name, Config: cfg})
	return err
}

// DeleteConnector deletes a connector and stops its tasks (DELETE
// /connectors/{name}). T2.
func (c *Connect) DeleteConnector(ctx context.Context, name string) error {
	return c.write(ctx, OpConnectDeleteConnector, connectorParams{Connector: name}, name)
}

// StopConnector moves a connector to STOPPED (PUT /connectors/{name}/stop,
// Kafka 3.5+). T2.
func (c *Connect) StopConnector(ctx context.Context, name string) error {
	return c.write(ctx, OpConnectStopConnector, connectorParams{Connector: name}, name)
}

// ResetSourceOffsets clears a stopped connector's stored offsets (DELETE
// /connectors/{name}/offsets, Kafka 3.6+). T2. It is the write of a
// connect_reset_source_offsets step with no position.
func (c *Connect) ResetSourceOffsets(ctx context.Context, name string) error {
	return c.write(ctx, OpConnectResetSourceOffsets, resetOffsetsParams{Connector: name}, name)
}

// AlterSourceOffsets replaces a stopped connector's stored offsets (PATCH
// /connectors/{name}/offsets, Kafka 3.6+). T2. Its only proposal step is a
// connect_reset_source_offsets step with a position, so it sends that op (the
// runner binds a request to the step's action) and takes exactly one offset:
// the step's position.
func (c *Connect) AlterSourceOffsets(ctx context.Context, name string, offsets []connect.SourceOffset) error {
	if err := requireArg(OpConnectResetSourceOffsets, "connector", name); err != nil {
		return err
	}
	if len(offsets) == 0 {
		return fmt.Errorf("remoteclients: %s: connector %q: no offsets; reset every offset instead: %w", OpConnectResetSourceOffsets, name, ErrBadArgs)
	}
	if len(offsets) != 1 {
		return fmt.Errorf("remoteclients: %s: connector %q: a reset step moves one partition, not %d: %w", OpConnectResetSourceOffsets, name, len(offsets), ErrStepMismatch)
	}
	pos := &offsetPosition{Partition: offsets[0].Partition, Offset: offsets[0].Offset}
	_, err := c.s.doStep(ctx, OpConnectResetSourceOffsets, protocol.TierT2, resetOffsetsParams{Connector: name, Position: pos})
	return err
}

// write sends one connector-named T2 op and ignores its Data.
func (c *Connect) write(ctx context.Context, op string, params any, name string) error {
	if err := requireArg(op, "connector", name); err != nil {
		return err
	}
	_, err := c.s.doStep(ctx, op, protocol.TierT2, params)
	return err
}

// requireArg refuses an empty argument before any op is built.
func requireArg(op, what, value string) error {
	if value == "" {
		return fmt.Errorf("remoteclients: %s: %s is required: %w", op, what, ErrBadArgs)
	}
	return nil
}
