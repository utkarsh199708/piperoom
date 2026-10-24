package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// server_properties_write (T1) writes the Debezium Server configuration for
// one index sink and one source connector.
type serverPropertiesWriteOp struct{ d *Driver }

func (serverPropertiesWriteOp) Name() string   { return "server_properties_write" }
func (serverPropertiesWriteOp) Target() string { return targetKind }
func (serverPropertiesWriteOp) Tier() string   { return protocol.TierT1 }
func (serverPropertiesWriteOp) Layer() string  { return layerName }
func (serverPropertiesWriteOp) Describe() string {
	return "write the Debezium Server application.properties for one index sink and one source connector"
}

func (serverPropertiesWriteOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"sink", "source"},
		"properties": map[string]any{
			"sink":   map[string]any{"type": "string", "minLength": 1},
			"source": map[string]any{"type": "string", "minLength": 1},
			"sink_keys": map[string]any{
				"type": "object", "maxProperties": maxPropertiesEntries,
				"additionalProperties": map[string]any{"type": "string"},
			},
			"source_keys": map[string]any{
				"type": "object", "maxProperties": maxPropertiesEntries,
				"additionalProperties": map[string]any{"type": "string"},
			},
			"format":       map[string]any{"type": "string", "enum": valueFormats},
			"key_format":   map[string]any{"type": "string", "enum": keyFormats},
			"offsets_file": map[string]any{"type": "string"},
		},
	}
}

func (o serverPropertiesWriteOp) Validate(params json.RawMessage, _ ops.Env) error {
	var p PropertiesParams
	if err := decodeStrict(params, &p); err != nil {
		return err
	}
	_, err := o.d.BuildProperties(p)
	return err
}

func (o serverPropertiesWriteOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	var p PropertiesParams
	if err := decodeStrict(params, &p); err != nil {
		return ops.Result{}, err
	}
	props, err := o.d.BuildProperties(p)
	if err != nil {
		return ops.Result{}, err
	}
	text := props.render()
	// The Compose image runs as a non-root user that must be able to read the
	// file across a bind mount, so a compose render is 0644; a unit runs as the
	// runner's own user and gets 0600. Either way the file holds ${secret:name}
	// references (names), not values: a container's credentials come from the
	// .env file's environment entry (see serverService).
	var mode fs.FileMode = 0o600
	if o.d.mode == ModeCompose {
		mode = 0o644
	}
	clean, changed, err := writeFile(env, ConfigFile, text, mode)
	if err != nil {
		return ops.Result{}, err
	}
	state := "wrote"
	if !changed {
		state = "unchanged"
	}
	summary := fmt.Sprintf("%s %s (%s -> %s, %d keys)", state, clean, props.Source, props.Sink, len(props.Keys))
	return ops.Result{
		Summary: summary,
		Data: map[string]any{
			"file": clean, "sink": props.Sink, "sink_entry": props.SinkName,
			"source": props.Source, "connector_class": props.ConnectorClass,
			"keys": props.Keys, "aliases": props.Aliases, "checks": props.Checks,
			"doc": props.Doc, "index_source": props.IndexSource, "bytes": len(text), "changed": changed,
		},
		Receipt: events.SetupReceipt{Kind: "file", Target: clean, At: now(env)},
	}, nil
}
