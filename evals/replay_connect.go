package evals

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/debeziumtools"
	"piperoom/internal/tools/kafkatools"
	"piperoom/internal/tools/logtools"
	"piperoom/internal/tools/nettools"
)

// The step 1 and 3 to 6 readers and the Q5 lifecycle writers in replay (Q6d).
//
// Tools: kafkatools registers connect_plugins (Q1c), connect_worker_info
// (Q3a), connect_validate_config (Q4a) and connect_source_offsets (Q5a) only
// for clients that have the optional reads, so LiveTools hands it the
// definition-only definitionConnect and definitionKafka below, as production
// hands it the real clients. connect_worker_metrics (Q3b), connect_worker_logs
// (Q3c) and net_probe (Q6a) are registered from their own packages
// (registerWorkerToolDefs). Every one of them is answered from the scenario's
// recordings like any other tool; their Run is never called.
//
// Actions: connect_stop_connector, connect_reset_source_offsets,
// connect_create_connector and connect_delete_connector are registered by
// actions.RegisterLifecycle, the call production's wiring makes, after the Q2
// source actions (production's order). Their Validate and Preview read the
// recorded stack through replayLifecycle: connect_status (state and type),
// connect_config, connect_source_offsets and connect_validate_config, each
// answering what the matching tool recording answers. Every lifecycle write
// is a no-op recorded in the run's writeLog.

var (
	_ kafkatools.ConfigValidator     = definitionConnect{}
	_ connect.SourceOffsetReader     = definitionConnect{}
	_ connect.PluginReader           = definitionConnect{}
	_ kafka.ConnectClusterReader     = definitionKafka{}
	_ clients.ConnectClient          = replayLifecycle{}
	_ connect.SourceOffsetReader     = replayLifecycle{}
	_ connect.ConnectLifecycleWriter = replayLifecycle{}
)

// definitionConnect is a Connect client for tool definitions only. It
// implements the optional reads so kafkatools registers their tools; no
// method is ever called, because the harness replays recordings.
type definitionConnect struct{ clients.ConnectClient }

func (definitionConnect) ValidateConfig(context.Context, string, map[string]string) (connect.ValidateResult, error) {
	return connect.ValidateResult{}, errNotReplayed
}

func (definitionConnect) PluginConfigDef(context.Context, string) ([]connect.ConfigKeyDef, error) {
	return nil, errNotReplayed
}

func (definitionConnect) SourceOffsets(context.Context, string) ([]connect.SourceOffset, error) {
	return nil, errNotReplayed
}

func (definitionConnect) Plugins(context.Context) ([]connect.Plugin, error) {
	return nil, errNotReplayed
}

// ConnectWorkerGroups registers connect_worker_info (Q3a), as the production
// Kafka client does.
func (definitionKafka) ConnectWorkerGroups(context.Context) ([]kafka.ConnectWorkerGroup, error) {
	return nil, errNotReplayed
}

func (definitionKafka) InternalTopics(context.Context, []string) ([]kafka.InternalTopic, error) {
	return nil, errNotReplayed
}

// registerWorkerToolDefs adds the definitions of connect_worker_metrics,
// connect_worker_logs and net_probe, with no endpoint, log store or target:
// the harness never runs them. Production registers them in the wiring rows
// (Q3e, Q6b); the evals offer them now so a scenario can record them.
func registerWorkerToolDefs(reg *tools.Registry) error {
	empty := &owners.Owners{}
	if err := debeziumtools.RegisterWorkerMetrics(reg, debeziumtools.Deps{Owners: empty}, ""); err != nil {
		return fmt.Errorf("evals: worker metrics tool definition: %w", err)
	}
	if err := logtools.Register(reg, logtools.Deps{Owners: empty}); err != nil {
		return fmt.Errorf("evals: worker logs tool definition: %w", err)
	}
	if err := nettools.Register(reg, nettools.Deps{Owners: empty}); err != nil {
		return fmt.Errorf("evals: net probe tool definition: %w", err)
	}
	return nil
}

// registerLifecycle adds the Q5 lifecycle actions to reg through
// actions.RegisterLifecycle over replay clients that read res and recs and
// record every write in wl.
func registerLifecycle(reg *actions.Registry, res Resources, recs []Recording, wl *writeLog) error {
	if err := actions.RegisterLifecycle(reg, actions.LifecycleDeps{
		Connect: replayLifecycle{replayConfig: replayConfig{res: res, recs: recs}, log: wl},
		Source:  replaySource{res: res, recs: recs},
	}); err != nil {
		return fmt.Errorf("evals: lifecycle action definitions: %w", err)
	}
	return nil
}

// replayLifecycle is the Connect client of the lifecycle actions: the
// replayConfig reads plus the stored offsets and config validation, and the
// lifecycle writes as recorded no-ops.
type replayLifecycle struct {
	replayConfig
	log *writeLog
}

// Status answers from connect_status, except that a connector this run
// already stopped reads STOPPED: the stop step's own wait and the reset's
// "stopped now" check then see the stop they follow, as on Connect.
func (c replayLifecycle) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	st, err := c.replayConfig.Status(ctx, name)
	if err != nil {
		return st, err
	}
	if c.stopped(name) {
		st.State = "STOPPED"
	}
	return st, nil
}

func (c replayLifecycle) stopped(name string) bool {
	for _, w := range c.log.snapshot() {
		if w.Method == "StopConnector" && w.Args["connector"] == name {
			return true
		}
	}
	return false
}

// sourceOffsetsData is connect_source_offsets' Data.
type sourceOffsetsData struct {
	Connectors []struct {
		Connector string                 `json:"connector"`
		Offsets   []connect.SourceOffset `json:"offsets"`
	} `json:"connectors"`
}

// SourceOffsets answers what connect_source_offsets answers for
// {connector: name} (a recording without input that lists the connector
// serves too). Numbers stay json.Number, as the action decodes its params,
// so a 64-bit LSN compares exactly.
func (c replayLifecycle) SourceOffsets(_ context.Context, name string) ([]connect.SourceOffset, error) {
	b, ok := matchedData(c.recs, "connect_source_offsets", map[string]any{"connector": name})
	if ok {
		var d sourceOffsetsData
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		if dec.Decode(&d) == nil {
			for _, row := range d.Connectors {
				if row.Connector == name {
					return row.Offsets, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("evals: stored offsets of %s: %w (record connect_source_offsets with data for that connector)", name, errNotReplayed)
}

// validateConfigData is the part of connect_validate_config's Data that
// connect_create_connector's Preview reads.
type validateConfigData struct {
	ErrorCount int                 `json:"error_count"`
	Errors     map[string][]string `json:"errors"`
}

// ValidateConfig answers what connect_validate_config answered for
// {class: class}: the recorded verdict, whatever config the step sends.
func (c replayLifecycle) ValidateConfig(_ context.Context, class string, _ map[string]string) (connect.ValidateResult, error) {
	b, ok := matchedData(c.recs, "connect_validate_config", map[string]any{"class": class})
	var d validateConfigData
	if !ok || json.Unmarshal(b, &d) != nil {
		return connect.ValidateResult{}, fmt.Errorf("evals: validate config of %s: %w (record connect_validate_config with data for that class)", class, errNotReplayed)
	}
	return connect.ValidateResult{ErrorCount: d.ErrorCount, Errors: d.Errors}, nil
}

// RestartTask and RestartConnector complete clients.ConnectClient; no
// lifecycle action calls them.
func (c replayLifecycle) RestartTask(_ context.Context, name string, task int) error {
	c.log.record("RestartTask", map[string]any{"connector": name, "task": task})
	return nil
}

func (c replayLifecycle) RestartConnector(_ context.Context, name string) error {
	c.log.record("RestartConnector", map[string]any{"connector": name})
	return nil
}

// CreateConnector records connect_create_connector's write; the config's
// values never reach the log.
func (c replayLifecycle) CreateConnector(_ context.Context, name string, cfg map[string]string) error {
	c.log.record("CreateConnector", map[string]any{"connector": name, "keys": len(cfg)})
	return nil
}

// DeleteConnector records connect_delete_connector's write.
func (c replayLifecycle) DeleteConnector(_ context.Context, name string) error {
	c.log.record("DeleteConnector", map[string]any{"connector": name})
	return nil
}

// StopConnector records connect_stop_connector's write.
func (c replayLifecycle) StopConnector(_ context.Context, name string) error {
	c.log.record("StopConnector", map[string]any{"connector": name})
	return nil
}

// ResetSourceOffsets records connect_reset_source_offsets' full reset.
func (c replayLifecycle) ResetSourceOffsets(_ context.Context, name string) error {
	c.log.record("ResetSourceOffsets", map[string]any{"connector": name})
	return nil
}

// AlterSourceOffsets records connect_reset_source_offsets' position write;
// the positions themselves are not logged.
func (c replayLifecycle) AlterSourceOffsets(_ context.Context, name string, offsets []connect.SourceOffset) error {
	c.log.record("AlterSourceOffsets", map[string]any{"connector": name, "partitions": len(offsets)})
	return nil
}
