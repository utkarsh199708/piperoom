package remoteclients

import (
	"piperoom/internal/setup/protocol"
)

// Op names, one per client method. The names are the catalogue's
// (internal/setup/ops) and the action names for the writes: a write op is the
// action the room's proposal step names (plan.OpConnectDeleteConnector,
// plan.OpSourceAlterSystem, ...), so the runner's StepAction mapping is the
// identity and a step's op and action cannot drift apart.
//
// docs/design/setup-agent.md §2.5 writes the read names with a dot
// ("connect.status"); the ops package requires snake_case names, so the dots
// are underscores here and the spelling below is the contract this package and
// the catalogue share.
const (
	// Kafka Connect readers (T0).
	OpConnectListConnectors = "connect_list_connectors"
	OpConnectStatus         = "connect_status"
	OpConnectConfig         = "connect_config"
	OpConnectValidateConfig = "connect_validate_config"
	OpConnectPluginConfig   = "connect_plugin_config"
	OpConnectPlugins        = "connect_plugins"
	OpConnectSourceOffsets  = "connect_source_offsets"
	OpConnectServerInfo     = "connect_server_info"

	// Kafka Connect writers (T2).
	OpConnectRestartTask        = "connect_restart_task"
	OpConnectRestartConnector   = "connect_restart_connector"
	OpConnectCreateConnector    = "connect_create_connector"
	OpConnectDeleteConnector    = "connect_delete_connector"
	OpConnectStopConnector      = "connect_stop_connector"
	OpConnectResetSourceOffsets = "connect_reset_source_offsets"
	OpConnectAlterSourceOffsets = "connect_alter_source_offsets"

	// Source Postgres readers (T0).
	OpSourceSettings         = "source_settings"
	OpSourceTableReplication = "source_table_replication"
	OpSourceRoleInfo         = "source_role_info"
	OpSourceBlockers         = "source_blockers"
	OpSourceReplicationSlots = "source_replication_slots"
	OpSourceRecentDDL        = "source_recent_ddl"
	OpSourceLongTransactions = "source_long_transactions"
	OpSourceRow              = "source_row"

	// Source Postgres writers. The Q action names are plan's canonical guard
	// names; the setup-only source ops are the design's §2.5 names.
	OpSourceSetReplicaIdentity   = "pg_set_replica_identity"
	OpSourceGrantSelect          = "source_grant_select"
	OpSourceCreateHeartbeatTable = "source_create_heartbeat_table"
	OpSourceEndSession           = "source_end_session"
	OpSourceCreatePublication    = "source_create_publication"
	OpSourceCreateCanaryTable    = "source_create_canary_table"
	OpSourceInsertCanary         = "source_insert_canary"
	OpSourceCreateReadRole       = "source_create_read_role"
	OpSourceAlterSystem          = "source_alter_system"

	// Kafka readers (T0).
	OpKafkaTopics         = "kafka_topics"
	OpKafkaTopicOffsets   = "kafka_topic_offsets"
	OpKafkaConsumerGroups = "kafka_consumer_groups"
	OpKafkaGroupLag       = "kafka_group_lag"
	OpKafkaFindByKey      = "kafka_find_by_key"
	OpKafkaFindByKeyScan  = "kafka_find_by_key_scan"

	// Schema Registry readers (T0).
	OpRegistrySubjects      = "registry_subjects"
	OpRegistryVersions      = "registry_versions"
	OpRegistrySchema        = "registry_schema"
	OpRegistryCompatibility = "registry_compatibility"
)

// OpSpec describes one op this package sends: the client method, the op name,
// the tier floor and the target kind the runner must have registered. The
// runner catalogue and the wiring read it; ops_test.go pins every method to
// exactly one spec.
type OpSpec struct {
	// Method is "Connect.Status", "Source.GrantSelect", ...
	Method string
	// Op is the catalogue name.
	Op string
	// Tier is the op's floor: protocol.TierT0 for reads, TierT2 for a Q
	// write, TierT3 for AlterSystem.
	Tier string
	// Target is the SetupTarget kind the op reaches ("connect", "postgres",
	// "kafka", "registry"), the kind the runner's class floor applies to.
	Target string
}

// specs is the package's whole operation surface, in declaration order.
var specs = []OpSpec{
	{"Connect.ListConnectors", OpConnectListConnectors, protocol.TierT0, "connect"},
	{"Connect.Status", OpConnectStatus, protocol.TierT0, "connect"},
	{"Connect.Config", OpConnectConfig, protocol.TierT0, "connect"},
	{"Connect.ValidateConfig", OpConnectValidateConfig, protocol.TierT0, "connect"},
	{"Connect.PluginConfigDef", OpConnectPluginConfig, protocol.TierT0, "connect"},
	{"Connect.Plugins", OpConnectPlugins, protocol.TierT0, "connect"},
	{"Connect.SourceOffsets", OpConnectSourceOffsets, protocol.TierT0, "connect"},
	{"Connect.ServerInfo", OpConnectServerInfo, protocol.TierT0, "connect"},
	{"Connect.RestartTask", OpConnectRestartTask, protocol.TierT2, "connect"},
	{"Connect.RestartConnector", OpConnectRestartConnector, protocol.TierT2, "connect"},
	{"Connect.CreateConnector", OpConnectCreateConnector, protocol.TierT2, "connect"},
	{"Connect.DeleteConnector", OpConnectDeleteConnector, protocol.TierT2, "connect"},
	{"Connect.StopConnector", OpConnectStopConnector, protocol.TierT2, "connect"},
	{"Connect.ResetSourceOffsets", OpConnectResetSourceOffsets, protocol.TierT2, "connect"},
	{"Connect.AlterSourceOffsets", OpConnectAlterSourceOffsets, protocol.TierT2, "connect"},
	{"Source.Settings", OpSourceSettings, protocol.TierT0, "postgres"},
	{"Source.TableReplication", OpSourceTableReplication, protocol.TierT0, "postgres"},
	{"Source.RoleInfo", OpSourceRoleInfo, protocol.TierT0, "postgres"},
	{"Source.Blockers", OpSourceBlockers, protocol.TierT0, "postgres"},
	{"Source.BlockersOnTables", OpSourceBlockers, protocol.TierT0, "postgres"},
	{"Source.ReplicationSlots", OpSourceReplicationSlots, protocol.TierT0, "postgres"},
	{"Source.RecentDDL", OpSourceRecentDDL, protocol.TierT0, "postgres"},
	{"Source.LongTransactions", OpSourceLongTransactions, protocol.TierT0, "postgres"},
	{"Source.Row", OpSourceRow, protocol.TierT0, "postgres"},
	{"Source.SetReplicaIdentity", OpSourceSetReplicaIdentity, protocol.TierT2, "postgres"},
	{"Source.GrantSelect", OpSourceGrantSelect, protocol.TierT2, "postgres"},
	{"Source.CreateHeartbeatTable", OpSourceCreateHeartbeatTable, protocol.TierT2, "postgres"},
	{"Source.EndSession", OpSourceEndSession, protocol.TierT2, "postgres"},
	{"Source.CreatePublication", OpSourceCreatePublication, protocol.TierT2, "postgres"},
	{"Source.CreateCanaryTable", OpSourceCreateCanaryTable, protocol.TierT2, "postgres"},
	{"Source.InsertCanary", OpSourceInsertCanary, protocol.TierT2, "postgres"},
	{"Source.CreateReadRole", OpSourceCreateReadRole, protocol.TierT2, "postgres"},
	{"Source.AlterSystem", OpSourceAlterSystem, protocol.TierT3, "postgres"},
	{"Kafka.Topics", OpKafkaTopics, protocol.TierT0, "kafka"},
	{"Kafka.TopicOffsets", OpKafkaTopicOffsets, protocol.TierT0, "kafka"},
	{"Kafka.ConsumerGroups", OpKafkaConsumerGroups, protocol.TierT0, "kafka"},
	{"Kafka.GroupLag", OpKafkaGroupLag, protocol.TierT0, "kafka"},
	{"Kafka.FindByKey", OpKafkaFindByKey, protocol.TierT0, "kafka"},
	{"Kafka.FindByKeyScan", OpKafkaFindByKeyScan, protocol.TierT0, "kafka"},
	{"Registry.Subjects", OpRegistrySubjects, protocol.TierT0, "registry"},
	{"Registry.Versions", OpRegistryVersions, protocol.TierT0, "registry"},
	{"Registry.Schema", OpRegistrySchema, protocol.TierT0, "registry"},
	{"Registry.Compatibility", OpRegistryCompatibility, protocol.TierT0, "registry"},
}

// Ops returns every op this package sends, in declaration order. It is a copy:
// callers cannot edit the table.
func Ops() []OpSpec { return append([]OpSpec(nil), specs...) }

// Error codes a failed op may carry in its result's Data ("error_code"). A
// real client's errors are sentinels the Q actions match with errors.Is, and
// the runner's OpResult has no reason field yet, so the runner catalogue puts
// one of these in Data for the failures a caller branches on. The PR carries a
// contract change request to add the field to protocol.OpResult.
const (
	ErrorCodeNotFound            = "not_found"
	ErrorCodeNotSupported        = "not_supported"
	ErrorCodeConnectorExists     = "connector_exists"
	ErrorCodeConnectorNotStopped = "connector_not_stopped"
	ErrorCodeRebalancing         = "rebalancing"
	ErrorCodeSessionChanged      = "session_changed"
	ErrorCodeSessionProtected    = "session_protected"
	ErrorCodeOutOfScope          = "out_of_scope"
)
