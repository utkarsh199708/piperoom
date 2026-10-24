package setupactions

import (
	"encoding/json"
	"errors"
	"fmt"

	"piperoom/internal/actions"
)

// EnvironmentSpecs is the setup-only action catalogue of this package: the
// environment ops (docs/design/setup-agent.md §2.5) with their tiers, layers,
// bounded params schemas and preview ops. The runner owns the ops themselves
// (internal/setup/ops and internal/setup/targets/*); this table is the server's
// model-facing view of them, and the runner re-validates every param before it
// executes.
//
// Every entry's scope is ScopeFree: these ops write only under the runner's
// project directory or into the Compose stack the runner rendered itself, and
// name no table, connector or host of the pipeline. A row that does name a
// pipeline resource (the source and connector drivers of U1-9) supplies its own
// scope: ScopeTables, ScopeConnector or ScopeField.
func EnvironmentSpecs() []Spec {
	return []Spec{
		{
			Name:     "file_write",
			Tier:     "T1",
			Layer:    "environment",
			Describe: "Write one non-executed UTF-8 text file under the runner's project directory.",
			Schema: objectSchema(map[string]any{
				"path":    map[string]any{"type": "string", "minLength": 1, "maxLength": 512},
				"content": map[string]any{"type": "string", "maxLength": maxFileWriteBytes},
			}, "path", "content"),
			Scope: ScopeFree,
			// No PreviewOp until the runner registers preview_file_write:
			// Preview answers "no preview" without a runner round trip.
		},
		{
			Name:     "file_delete",
			Tier:     "T1",
			Layer:    "environment",
			Describe: "Delete one regular file under the runner's project directory.",
			Schema: objectSchema(map[string]any{
				"path": map[string]any{"type": "string", "minLength": 1, "maxLength": 512},
			}, "path"),
			Scope: ScopeFree,
			// No PreviewOp until the runner registers preview_file_delete:
			// Preview answers "no preview" without a runner round trip.
		},
		{
			Name:     "compose_write",
			Tier:     "T1",
			Layer:    "environment",
			Describe: "Render a Compose stack from a typed template (kafka-kraft, connect, registry, source-postgres) and write it under the project directory.",
			Schema: objectSchema(map[string]any{
				"templates": map[string]any{
					"type": "array", "minItems": 1, "maxItems": 4,
					"items": map[string]any{"type": "string", "enum": []string{
						"kafka-kraft", "connect", "registry", "source-postgres",
					}},
				},
				"project":  map[string]any{"type": "string", "pattern": `^piperoom-[a-z0-9_-]{1,54}$`},
				"ports":    map[string]any{"type": "object", "maxProperties": 16, "additionalProperties": map[string]any{"type": "integer", "minimum": 1, "maximum": 65535}},
				"registry": map[string]any{"type": "boolean"},
				"file":     map[string]any{"type": "string", "maxLength": 512},
			}, "templates"),
			Scope: ScopeFree,
			// No PreviewOp until the runner registers preview_compose_write:
			// Preview answers "no preview" without a runner round trip.
		},
		{
			Name:     "properties_write",
			Tier:     "T1",
			Layer:    "environment",
			Describe: "Write the .env or a worker properties file from an allow-listed key set; a credential key takes a ${secret:name} reference, never a value.",
			Schema: objectSchema(map[string]any{
				"file": map[string]any{"type": "string", "enum": []string{".env", "worker.properties", "connect-distributed.properties"}},
				"entries": map[string]any{
					"type": "object", "maxProperties": 64,
					"additionalProperties": map[string]any{"type": "string", "maxLength": 4096},
				},
			}, "file", "entries"),
			Scope: ScopeFree,
			// No PreviewOp until the runner registers preview_properties_write:
			// Preview answers "no preview" without a runner round trip.
		},
		{
			Name:     "compose_up",
			Tier:     "T1",
			Layer:    "environment",
			Describe: "Start the rendered Compose stack in the background (docker compose up -d).",
			Schema:   stackSchema(),
			Scope:    ScopeFree,
			// compose_config renders the stack compose would run, so the
			// preview shows the effective services before they start.
			PreviewOp:     "compose_config",
			PreviewParams: fileOnlyParams,
			CheckParams:   checkStackParams,
			PreviewDecode: composeConfigPreview("up"),
			// The first up pulls the Kafka and Connect images.
			Timeout: longOpTimeout,
		},
		{
			Name:          "compose_restart",
			Tier:          "T1",
			Layer:         "environment",
			Describe:      "Restart the rendered Compose stack's services in place (volumes kept).",
			Schema:        stackSchema(),
			Scope:         ScopeFree,
			PreviewOp:     "compose_config",
			PreviewParams: fileOnlyParams,
			CheckParams:   checkStackParams,
			PreviewDecode: composeConfigPreview("restart"),
		},
		{
			Name:          "compose_down",
			Tier:          "T1",
			Layer:         "environment",
			Describe:      "Stop the rendered Compose stack; volumes are always kept.",
			Schema:        stackSchema(),
			Scope:         ScopeFree,
			PreviewOp:     "compose_config",
			PreviewParams: fileOnlyParams,
			CheckParams:   checkStackParams,
			PreviewDecode: composeConfigPreview("down"),
		},
		{
			Name:     "plugin_install",
			Tier:     "T1",
			Layer:    "kafka-connect",
			Describe: "Download one Debezium connector archive from an allow-listed host into the Connect plugin directory, verified against the pinned digest.",
			Schema: objectSchema(map[string]any{
				"release":   map[string]any{"type": "string", "minLength": 1, "maxLength": 32, "pattern": `^[0-9A-Za-z][0-9A-Za-z._-]{0,31}$`},
				"connector": map[string]any{"type": "string", "minLength": 1, "maxLength": 32, "pattern": `^[a-z][a-z0-9-]{0,31}$`},
			}, "release", "connector"),
			Scope: ScopeFree,
			// One archive download alone may take the runner's 5 minute
			// downloadTimeout.
			Timeout: longOpTimeout,
			// No PreviewOp until the runner registers preview_plugin_install:
			// Preview answers "no preview" without a runner round trip.
		},
	}
}

// maxFileWriteBytes mirrors the runner's file_write cap (256 KiB).
const maxFileWriteBytes = 256 << 10

// Register adds every environment action to reg.
func Register(reg *actions.Registry, deps Deps) error {
	return RegisterSpecs(reg, deps, EnvironmentSpecs())
}

// RegisterSpecs adds the actions of specs to reg. A spec New refuses, or a
// duplicate name, fails the whole registration: a setup stack with only part
// of its actions would propose steps the room cannot execute.
func RegisterSpecs(reg *actions.Registry, deps Deps, specs []Spec) error {
	if reg == nil {
		return errors.New("setupactions: a registry is required")
	}
	for _, spec := range specs {
		a, err := New(spec, deps)
		if err != nil {
			return err
		}
		if err := reg.Register(a); err != nil {
			return fmt.Errorf("setupactions: registering %s: %w", spec.Name, err)
		}
	}
	return nil
}

// stackSchema is the params of a Compose subcommand: the rendered file and the
// services to act on (none means the whole project).
func stackSchema() map[string]any {
	return objectSchema(map[string]any{
		"file": map[string]any{"type": "string", "maxLength": 512},
		"services": map[string]any{
			"type": "array", "maxItems": maxStackServices,
			"items": map[string]any{"type": "string", "minLength": 1, "maxLength": 63, "pattern": composeServicePattern},
		},
	})
}

// fileOnlyParams drops services for compose_config, which renders the whole
// project and refuses named services.
func fileOnlyParams(params json.RawMessage) (json.RawMessage, error) {
	var in struct {
		File string `json:"file"`
	}
	if len(params) > 0 {
		if err := json.Unmarshal(params, &in); err != nil {
			return nil, err
		}
	}
	return json.Marshal(struct {
		File string `json:"file,omitempty"`
	}{File: in.File})
}

// objectSchema builds a closed object schema with the given required list.
func objectSchema(props map[string]any, required ...string) map[string]any {
	if props == nil {
		props = map[string]any{}
	}
	if required == nil {
		required = []string{}
	}
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             required,
		"properties":           props,
	}
}
