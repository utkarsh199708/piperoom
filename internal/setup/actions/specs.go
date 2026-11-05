package setupactions

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/host"
)

// EnvironmentSpecs is the setup-only action catalogue of this package: the
// environment ops (docs/design/setup-agent.md §2.5) with their tiers, layers,
// bounded params schemas and preview ops. The runner owns the ops themselves
// (internal/setup/ops and internal/setup/targets/*); this table is the server's
// model-facing view of them, and the runner re-validates every param before it
// executes.
//
// The environment entries' scope is ScopeFree: these ops write only under the
// runner's project directory or into the Compose stack the runner rendered
// itself, and name no table, connector or host of the pipeline. A row that
// does name a pipeline resource supplies its own scope: ScopeTables,
// ScopeConnector or ScopeField (the Strimzi and managed connector writes of
// specs_targets.go check the connector against the pipeline's).
func EnvironmentSpecs() []Spec {
	specs := []Spec{
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
			Describe: "Render a Compose stack from typed templates (kafka-kraft, connect, registry, source-postgres, and sink-postgres: the dev warehouse a JDBC sink writes into) and write it under the project directory.",
			Schema: objectSchema(map[string]any{
				"templates": map[string]any{
					"type": "array", "minItems": 1, "maxItems": 5,
					"items": map[string]any{"type": "string", "enum": []string{
						"kafka-kraft", "connect", "registry", "source-postgres", "sink-postgres",
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
			Describe: "Write the .env or a worker properties file from an allow-listed key set; a credential key takes a ${secret:name} reference, never a value. On a host target the file may also be an absolute path outside the project directory named connect-distributed.properties or connect-standalone.properties (T2: the driver's approval).",
			Schema: objectSchema(map[string]any{
				"file": map[string]any{"type": "string", "minLength": 1, "maxLength": maxPropertiesPath, "pattern": propertiesFilePattern},
				"entries": map[string]any{
					"type": "object", "maxProperties": 64,
					"additionalProperties": map[string]any{"type": "string", "maxLength": 4096},
				},
			}, "file", "entries"),
			Scope: ScopeFree,
			// A file outside the project directory is T2 (host target, lead
			// ruling on U8a); the runner's catalogue raises the floor the
			// same way (ops.Registry.FloorFor over host's TierFor).
			CheckParams: checkPropertiesFile,
			TierFor:     propertiesWriteTier,
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
			Describe: "Download one Debezium connector archive (a source connector, or jdbc for the JDBC sink) from an allow-listed host into the Connect plugin directory, verified against the pinned digest.",
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
		// The host target's writes (row U8f). A runner whose room has another
		// target does not register these ops and refuses them as unknown; the
		// T0 host reads (service_status, properties_read, journal_tail) are
		// read tools (setuptools), not actions, because every entry here is a
		// write the agent may only propose.
		{
			Name:     "service_install",
			Tier:     "T2",
			Layer:    "environment",
			Describe: "Render the Kafka Connect worker service from the host driver's template and enable it: a piperoom-<name> systemd unit, or the Windows service. The unit's command and paths come from the template and detection, never from these params.",
			Schema: objectSchema(map[string]any{
				"name": map[string]any{"type": "string", "pattern": hostServiceNamePattern},
				"mode": map[string]any{"type": "string", "enum": []string{host.ModeDistributed, host.ModeStandalone}},
			}),
			Scope:       ScopeFree,
			CheckParams: checkServiceInstall,
			// No PreviewOp until the runner registers preview_service_install
			// (the host driver has the Preview; the catalogue does not expose
			// it as an op yet): Preview answers "no preview" without a runner
			// round trip.
		},
		{
			Name:     "service_restart",
			Tier:     "T2",
			Layer:    "environment",
			Describe: "Restart one piperoom service on the runner's host (systemctl restart of the unit, or the Windows service control manager).",
			Schema: objectSchema(map[string]any{
				"name": map[string]any{"type": "string", "pattern": hostServiceNamePattern},
			}),
			Scope:       ScopeFree,
			CheckParams: checkServiceName,
			// No PreviewOp until the runner registers preview_service_restart:
			// Preview answers "no preview" without a runner round trip.
		},
		// The embedded target's one write (row U6b): the reference project,
		// rendered from typed params under piperoom-reference/. A runner in
		// embedded mode refuses every other write; one that is not in
		// embedded mode does not register it and refuses it as unknown.
		{
			Name:     "reference_write",
			Tier:     "T1",
			Layer:    "environment",
			Describe: "Render the Debezium Engine reference project (Maven or Gradle, Postgres or MySQL) from typed params into piperoom-reference/ under the project directory, for the person to compare with their own build and run themselves. The runner never builds or runs it; the password is a ${secret:name} reference.",
			Schema:   referenceSchema(),
			Scope:    ScopeFree,
			CheckParams: func(params json.RawMessage) error {
				var p ops.ReferenceParams
				dec := json.NewDecoder(strings.NewReader(string(params)))
				dec.DisallowUnknownFields()
				if err := dec.Decode(&p); err != nil {
					return fmt.Errorf("reference_write params: %v", err)
				}
				return p.Embedded().Validate()
			},
			// No PreviewOp until the runner registers preview_reference_write:
			// Preview answers "no preview" without a runner round trip.
		},
	}
	// The Strimzi and managed targets' writes (specs_targets.go).
	return append(specs, targetSpecs()...)
}

// referenceSchema is reference_write's params schema: the runner op's own
// (ops.ReferenceParams), so the model sees exactly what the runner decodes.
func referenceSchema() map[string]any {
	return ops.ReferenceWriteSchema()
}

// hostServiceNamePattern mirrors the host driver's service name rule (host's
// serviceNameRE in unit.go): the NAME of piperoom-NAME.service. The runner
// re-checks it when it decodes the op's params.
const hostServiceNamePattern = `^[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?$`

var hostServiceNameRE = regexp.MustCompile(hostServiceNamePattern)

// hostServiceParams is the part of the host service ops' params this package
// checks; the runner decodes them strictly against the driver's own types.
type hostServiceParams struct {
	Name string `json:"name"`
	Mode string `json:"mode"`
}

// checkServiceName mirrors the host driver's service name rule for
// service_restart (and service_install through checkServiceInstall): the
// schema states the pattern for the model, this enforces it.
func checkServiceName(params json.RawMessage) error {
	var p hostServiceParams
	if err := json.Unmarshal(params, &p); err != nil {
		return fmt.Errorf("service params: %v", err)
	}
	if p.Name != "" && !hostServiceNameRE.MatchString(p.Name) {
		return fmt.Errorf("name is not a lowercase service name (the NAME of piperoom-NAME.service)")
	}
	return nil
}

// checkServiceInstall checks service_install's name and worker mode.
func checkServiceInstall(params json.RawMessage) error {
	if err := checkServiceName(params); err != nil {
		return err
	}
	var p hostServiceParams
	if err := json.Unmarshal(params, &p); err != nil {
		return fmt.Errorf("service params: %v", err)
	}
	switch p.Mode {
	case "", host.ModeDistributed, host.ModeStandalone:
		return nil
	default:
		return fmt.Errorf("mode %q is not %s or %s", clipText(p.Mode), host.ModeDistributed, host.ModeStandalone)
	}
}

// maxFileWriteBytes mirrors the runner's file_write cap (256 KiB).
const maxFileWriteBytes = 256 << 10

// maxPropertiesPath mirrors the host target's cap on one path (512 bytes).
const maxPropertiesPath = 512

// propertiesRootFiles are the files properties_write names by their
// project-root name: the Compose target's .env and worker.properties, and the
// host target's two worker files. The runner's target refuses the ones it
// does not write.
var propertiesRootFiles = map[string]bool{
	".env":                           true,
	"worker.properties":              true,
	"connect-distributed.properties": true,
	"connect-standalone.properties":  true,
}

// propertiesOutsideBases are the base names an absolute properties_write path
// may have: the host target's worker files (host.workerFiles).
var propertiesOutsideBases = map[string]bool{
	"connect-distributed.properties": true,
	"connect-standalone.properties":  true,
}

// propertiesFilePattern is the schema's statement of checkPropertiesFile for
// the model: a project-root name, or an absolute POSIX or drive path ending in
// a host worker file name.
const propertiesFilePattern = `^(\.env|worker\.properties|connect-(distributed|standalone)\.properties|(/|[A-Za-z]:[\\/]).*[\\/]connect-(distributed|standalone)\.properties)$`

// propertiesParams is the part of properties_write's params this package
// reads; the runner decodes them strictly.
type propertiesParams struct {
	File string `json:"file"`
}

// checkPropertiesFile mirrors the host target's path rule without I/O and
// without the runner's OS: the file is a project-root name, or an absolute
// path (POSIX, or drive-absolute Windows) that passes host.CheckServicePath,
// has no empty, dot or dot-dot component, and whose base name is a worker
// file. The runner re-checks it against the real file system (no symlinked
// component, not under the project directory, the parent exists).
func checkPropertiesFile(params json.RawMessage) error {
	var p propertiesParams
	if err := json.Unmarshal(params, &p); err != nil {
		return fmt.Errorf("properties_write params: %v", err)
	}
	if propertiesRootFiles[p.File] {
		return nil
	}
	if len(p.File) > maxPropertiesPath {
		return fmt.Errorf("file is longer than %d bytes", maxPropertiesPath)
	}
	if !absolutePropertiesPath(p.File) {
		return fmt.Errorf("file %q is neither a project-root properties file (%s) nor an absolute path", clipText(p.File), ".env, worker.properties, connect-distributed.properties, connect-standalone.properties")
	}
	if err := host.CheckServicePath(p.File); err != nil {
		return fmt.Errorf("file: %v", err)
	}
	// Below the root (the leading separator, after a drive letter): every
	// component is a plain name.
	rest := p.File
	if drivePathRE.MatchString(rest) {
		rest = rest[2:]
	}
	parts := strings.FieldsFunc(rest[1:], func(r rune) bool { return r == '/' || r == '\\' })
	if len(parts) < 2 {
		return fmt.Errorf("file %q names no directory outside the project", clipText(p.File))
	}
	if all := strings.Split(strings.ReplaceAll(rest[1:], `\`, "/"), "/"); len(all) != len(parts) {
		return fmt.Errorf("file %q has an empty component", clipText(p.File))
	}
	for _, c := range parts {
		if c == "." || c == ".." {
			return fmt.Errorf("file %q has a dot or dot-dot component", clipText(p.File))
		}
	}
	if base := parts[len(parts)-1]; !propertiesOutsideBases[base] {
		return fmt.Errorf("file %q outside the project directory must be named connect-distributed.properties or connect-standalone.properties", clipText(p.File))
	}
	return nil
}

// drivePathRE is a drive-absolute Windows path in either separator.
var drivePathRE = regexp.MustCompile(`^[A-Za-z]:[\\/]`)

// absolutePropertiesPath reports whether p has the shape of an absolute path
// on either OS, as the host target decides it (absoluteShape there).
func absolutePropertiesPath(p string) bool {
	return strings.HasPrefix(p, "/") || strings.HasPrefix(p, `\`) || drivePathRE.MatchString(p)
}

// propertiesWriteTier is properties_write's step tier: T1 for a project-root
// file (both targets), and for an absolute path the host target's own rule,
// host.PropertiesWriteTier (T2), so the room and the runner compute one tier
// from one function.
func propertiesWriteTier(params json.RawMessage) (string, error) {
	var p propertiesParams
	if err := json.Unmarshal(params, &p); err != nil {
		return "", fmt.Errorf("properties_write params: %v", err)
	}
	if propertiesRootFiles[p.File] && !propertiesOutsideBases[p.File] {
		// The Compose target's own files: host's rule does not name them.
		return protocol.TierT1, nil
	}
	return host.PropertiesWriteTier(params)
}

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
