package owners

import (
	"fmt"
	"strings"

	"piperoom/internal/events"
)

// Provisional builds the pipeline a setup room is scoped to before it is
// registered (docs/design/setup-agent.md §3.2): kind cdc, the request's id,
// tenant, topic prefix, source tables and connector names, the request's
// target hosts as connections: entries with no credentials, and every layer
// unowned. The room passes it to tools.WithPipeline and to every action's
// CheckScope and Validate.
//
// The pipeline is deliberately not registered yet: no owners: block (so a
// top-level environment: owner written by discover never approves a setup
// step), no sink tables or consumer groups, and the connections: hold only the
// host addresses the person named. pipeline_register (A5) replaces the
// connections with the real file: references before writing the entry.
func Provisional(req events.SetupRequest) (Pipeline, error) {
	if strings.TrimSpace(req.PipelineID) == "" {
		return Pipeline{}, fmt.Errorf("owners: provisional: pipeline_id is required")
	}
	if strings.TrimSpace(req.TopicPrefix) == "" {
		return Pipeline{}, fmt.Errorf("owners: provisional: topic_prefix is required")
	}
	conns := Connections{}
	for _, t := range req.Targets {
		key := targetConnKey(t.Kind)
		if key == "" {
			continue
		}
		// No credentials: the value is the bare host the person typed. Load
		// would refuse a {value:} for these keys, but a provisional pipeline is
		// built in code and never written through Load; the overlap check reads
		// these values as the pipeline's hosts.
		conns[key] = ConnRef{Value: strings.TrimSpace(t.Address)}
	}
	return Pipeline{
		ID:           req.PipelineID,
		Title:        req.Title,
		Tenant:       req.Tenant,
		TopicPrefix:  req.TopicPrefix,
		SourceTables: req.Tables,
		Connectors:   req.Connectors,
		Connections:  conns,
		// Kind stays "" (the default: cdc), and Owners stays nil, so every
		// layer of the provisional pipeline is unowned.
	}, nil
}

// targetConnKey maps a SetupTarget kind to the connections: key that names the
// host it reaches. Kinds without a cdc connection key (kubernetes, aws,
// confluent, aiven, host) answer "" and are not written into connections:.
func targetConnKey(kind string) string {
	switch kind {
	case "postgres":
		return "source_db_url"
	case "kafka":
		return "kafka_brokers"
	case "connect":
		return "connect_url"
	case "registry":
		return "schema_registry_url"
	}
	return ""
}
