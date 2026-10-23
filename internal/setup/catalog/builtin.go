package catalog

import (
	"encoding/json"

	"piperoom/internal/setup/plan"
)

// builtinSpecs is the U1 catalogue (design §2.5, "the catalogue at the end of
// U1"): the runner's base ops (internal/setup/ops), the Compose driver's ops
// (internal/setup/targets/compose) and the setup-only source ops over
// clients.SetupSourceWriter. Every runner op's tier, target, layer,
// description and schema is pinned to the runner's own declaration by
// TestBuiltinMatchesRunner; the source ops' params are the names the remote
// Source client binds a step to (internal/setup/remoteclients, source.go).
//
// Left out on purpose:
//   - secret_set: a secret value never travels in proposal params (they are
//     stored in the room log); it reaches the runner through
//     POST /api/rooms/{id}/setup/secret and the runner's terminal only.
//   - source_restart: no catalogue ships it yet.
var builtinSpecs = []Spec{
	// ---- T0, the runner's base ops ----
	{Name: "env_detect", Tier: plan.TierT0, Layer: layerEnv, Via: ViaSetup,
		Describe: "report OS, architecture, project files and PATH tools",
		Schema:   raw(`{"additionalProperties":false,"type":"object"}`)},
	{Name: "file_read", Tier: plan.TierT0, Layer: layerEnv, Via: ViaSetup,
		Describe: "read a UTF-8 text file under the project directory",
		Schema:   raw(`{"additionalProperties":false,"properties":{"path":{"minLength":1,"type":"string"}},"required":["path"],"type":"object"}`)},
	{Name: "dir_list", Tier: plan.TierT0, Layer: layerEnv, Via: ViaSetup,
		Describe: "list one directory under the project directory",
		Schema:   raw(`{"additionalProperties":false,"properties":{"path":{"type":"string"}},"type":"object"}`)},
	{Name: "jar_list", Tier: plan.TierT0, Layer: layerEnv, Via: ViaSetup,
		Describe: "inspect Debezium plugin directories for layout problems",
		Schema:   raw(`{"additionalProperties":false,"properties":{"path":{"type":"string"}},"type":"object"}`)},
	{Name: "java_version", Tier: plan.TierT0, Layer: layerEnv, Via: ViaSetup,
		Describe: "report the java on PATH and its version",
		Schema:   raw(`{"additionalProperties":false,"type":"object"}`)},
	{Name: "http_get", Tier: plan.TierT0, Layer: layerEnv, Via: ViaSetup,
		Describe: "GET or HEAD a URL on a host in the room's targets",
		Schema:   raw(`{"additionalProperties":false,"properties":{"method":{"enum":["GET","HEAD"],"type":"string"},"url":{"maxLength":2048,"minLength":1,"type":"string"}},"required":["url"],"type":"object"}`)},
	{Name: "net_probe", Tier: plan.TierT0, Layer: layerEnv, Via: ViaSetup,
		Describe: "probe DNS, TCP and TLS of a registered target from the runner",
		Schema:   raw(`{"additionalProperties":false,"properties":{"port":{"maximum":65535,"minimum":0,"type":"integer"},"target":{"minLength":1,"type":"string"},"tls":{"type":"boolean"}},"required":["target"],"type":"object"}`)},

	// ---- T1, the runner's base ops ----
	{Name: "file_write", Tier: plan.TierT1, Layer: layerEnv, Via: ViaSetup,
		Describe: "write a non-executed UTF-8 text file under the project directory",
		Schema:   raw(`{"additionalProperties":false,"properties":{"content":{"maxLength":262144,"type":"string"},"path":{"minLength":1,"type":"string"}},"required":["path","content"],"type":"object"}`)},
	{Name: "file_delete", Tier: plan.TierT1, Layer: layerEnv, Via: ViaSetup,
		Describe: "delete one regular file under the project directory",
		Schema:   raw(`{"additionalProperties":false,"properties":{"path":{"minLength":1,"type":"string"}},"required":["path"],"type":"object"}`)},

	// ---- T0, the Compose driver ----
	{Name: "docker_info", Tier: plan.TierT0, Target: targetCompose, Layer: layerEnv, Via: ViaSetup,
		Describe: "read the Docker client, daemon and Compose plugin versions and the daemon endpoint",
		Schema:   raw(`{"additionalProperties":false,"type":"object"}`)},
	{Name: "compose_ps", Tier: plan.TierT0, Target: targetCompose, Layer: layerEnv, Via: ViaSetup,
		Describe: "list the rendered stack's containers and their state",
		Schema:   raw(stackSchema)},
	{Name: "compose_config", Tier: plan.TierT0, Target: targetCompose, Layer: layerEnv, Via: ViaSetup,
		Describe: "render the stack as compose resolves it, with credential values masked",
		Schema:   raw(`{"additionalProperties":false,"properties":{"file":{"type":"string"}},"type":"object"}`)},
	{Name: "container_inspect", Tier: plan.TierT0, Target: targetCompose, Layer: layerEnv, Via: ViaSetup,
		Describe: "inspect one service's containers: state, mounts and environment variable names, never values",
		Schema:   raw(serviceSchema)},
	{Name: "container_logs", Tier: plan.TierT0, Target: targetCompose, Layer: layerEnv, Via: ViaSetup,
		Describe: "read the last 200 log lines of one service's container in the project (capped, redacted, newest last)",
		Schema:   raw(serviceSchema)},

	// ---- T1, the Compose driver ----
	{Name: "compose_write", Tier: plan.TierT1, Target: targetCompose, Layer: layerEnv, Via: ViaSetup,
		Describe: "render a Compose stack from a typed template and write it under the project directory",
		Schema:   raw(`{"additionalProperties":false,"properties":{"file":{"type":"string"},"ports":{"additionalProperties":{"maximum":65535,"minimum":1024,"type":"integer"},"maxProperties":8,"type":"object"},"project":{"pattern":"^piperoom-[a-z0-9_-]{1,54}$","type":"string"},"registry":{"type":"boolean"},"templates":{"items":{"enum":["kafka-kraft","connect","registry","source-postgres"],"type":"string"},"maxItems":4,"minItems":1,"type":"array"}},"required":["templates"],"type":"object"}`)},
	{Name: "properties_write", Tier: plan.TierT1, Target: targetCompose, Layer: layerEnv, Via: ViaSetup,
		Describe: "write an allow-listed properties file (.env or Kafka Connect worker properties)",
		Schema:   raw(`{"additionalProperties":false,"properties":{"entries":{"additionalProperties":{"type":"string"},"maxProperties":64,"minProperties":1,"type":"object"},"file":{"minLength":1,"type":"string"}},"required":["file","entries"],"type":"object"}`)},
	{Name: "compose_up", Tier: plan.TierT1, Target: targetCompose, Layer: layerEnv, Via: ViaSetup,
		Describe: "start the rendered Compose stack in the background",
		Schema:   raw(stackSchema)},
	{Name: "compose_restart", Tier: plan.TierT1, Target: targetCompose, Layer: layerEnv, Via: ViaSetup,
		Describe: "restart the services of the rendered Compose stack",
		Schema:   raw(stackSchema)},
	{Name: "compose_down", Tier: plan.TierT1, Target: targetCompose, Layer: layerEnv, Via: ViaSetup,
		Describe: "stop and remove the rendered stack's containers; volumes are kept",
		Schema:   raw(stackSchema)},
	{Name: "plugin_install", Tier: plan.TierT1, Target: targetCompose, Layer: "kafka-connect", Via: ViaSetup,
		Describe: "download one Debezium connector archive from an allowed host into the Connect plugin directory",
		Schema:   raw(`{"additionalProperties":false,"properties":{"connector":{"maxLength":32,"minLength":1,"pattern":"^[a-z][a-z0-9-]{0,31}$","type":"string"},"release":{"maxLength":32,"minLength":1,"pattern":"^[0-9A-Za-z][0-9A-Za-z._-]{0,31}$","type":"string"}},"required":["release","connector"],"type":"object"}`)},

	// ---- T2 and T3, the setup-only source ops (clients.SetupSourceWriter) ----
	{Name: "source_create_publication", Tier: plan.TierT2, Target: targetPostgres, Layer: layerPostgres, Via: ViaSetup,
		Describe:  "create a publication FOR TABLE the listed source tables",
		Schema:    raw(`{"additionalProperties":false,"properties":{"publication":{"pattern":"` + identPattern + `","type":"string"},"tables":{"items":{"pattern":"` + tablePattern + `","type":"string"},"maxItems":64,"minItems":1,"type":"array"}},"required":["publication","tables"],"type":"object"}`),
		TableKeys: []string{"tables"}},
	{Name: "source_create_canary_table", Tier: plan.TierT2, Target: targetPostgres, Layer: layerPostgres, Via: ViaSetup,
		Describe:  "create the verification ladder's canary table (idempotent); it must be one of the pipeline's tables",
		Schema:    raw(`{"additionalProperties":false,"properties":{"table":{"pattern":"` + tablePattern + `","type":"string"}},"required":["table"],"type":"object"}`),
		TableKeys: []string{"table"}},
	{Name: "source_insert_canary", Tier: plan.TierT2, Target: targetPostgres, Layer: layerPostgres, Via: ViaSetup,
		Describe:  "insert one canary row carrying the ladder runner's nonce",
		Schema:    raw(`{"additionalProperties":false,"properties":{"nonce":{"pattern":"^[0-9a-f]{32}$","type":"string"},"table":{"pattern":"` + tablePattern + `","type":"string"}},"required":["table","nonce"],"type":"object"}`),
		TableKeys: []string{"table"}},
	{Name: "source_create_read_role", Tier: plan.TierT2, Target: targetPostgres, Layer: layerPostgres, Via: ViaSetup,
		Describe:  "create the pipeline's read role with SELECT on the listed tables; the runner generates and keeps its password",
		Schema:    raw(`{"additionalProperties":false,"properties":{"role":{"pattern":"` + identPattern + `","type":"string"},"tables":{"items":{"pattern":"` + tablePattern + `","type":"string"},"maxItems":64,"minItems":1,"type":"array"}},"required":["role","tables"],"type":"object"}`),
		TableKeys: []string{"tables"}},
	{Name: "source_alter_system", Tier: plan.TierT3, Target: targetPostgres, Layer: layerPostgres, Via: ViaSetup,
		Describe: "ALTER SYSTEM one allow-listed setting (pending_restart in the preview); needs a maintenance window",
		Schema:   raw(`{"additionalProperties":false,"properties":{"name":{"enum":["wal_level","max_replication_slots","max_wal_senders","max_slot_wal_keep_size","wal_sender_timeout"],"type":"string"},"value":{"maxLength":64,"minLength":1,"type":"string"}},"required":["name","value"],"type":"object"}`)},
}

// Shared schema text and names of the table above.
const (
	layerEnv       = "environment"
	layerPostgres  = "postgres"
	targetCompose  = "compose"
	targetPostgres = "postgres"

	stackSchema   = `{"additionalProperties":false,"properties":{"file":{"type":"string"},"services":{"items":{"type":"string"},"maxItems":16,"type":"array"}},"type":"object"}`
	serviceSchema = `{"additionalProperties":false,"properties":{"file":{"type":"string"},"service":{"maxLength":63,"minLength":1,"type":"string"}},"required":["service"],"type":"object"}`

	// identPattern is a Postgres identifier the source client passes through
	// pgx.Identifier.Sanitize (AGENTS.md rule 6), lower case so it needs no
	// quoting to mean what it says.
	identPattern = `^[a-z_][a-z0-9_]{0,62}$`
	// tablePattern is schema.table, the room's setup table rule.
	tablePattern = `^[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*$`
)

func raw(s string) json.RawMessage { return json.RawMessage(s) }

// Builtin returns the U1 setup catalogue as a new Catalogue.
func Builtin() (*Catalogue, error) {
	return New(builtinSpecs...)
}
