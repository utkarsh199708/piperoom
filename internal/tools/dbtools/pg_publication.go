package dbtools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
)

// Defaults for the Debezium publication name and connector role. dbtools.Deps
// has no Connect client, so the tool cannot read the live connector config; it
// assumes the dev connector's values and reports the connector-user source
// explicitly in its Data output. See dev/connectors/pg-orders-connector.json:
// database.user = "piperoom" and no publication.name, which Debezium defaults
// to "dbz_publication".
const (
	defaultPublication   = "dbz_publication"
	defaultConnectorRole = "piperoom"
)

// connectorUserSource names where defaultConnectorRole comes from, surfaced in
// Data so the agent and reviewers can see the assumption.
const connectorUserSource = "default (dev/connectors/pg-orders-connector.json database.user); dbtools.Deps has no Connect client to read the live config"

// publicationTablesClient is the local adapter for PublicationTables until the
// method lands on clients.SourceDB (contract change request in the PR): the
// real client (internal/clients/postgres) and the test fake implement it, and
// the tool reports a clear error when the injected client does not. This is
// the same pattern kafkatools uses for topicConfigAdmin.
type publicationTablesClient interface {
	PublicationTables(ctx context.Context, publication, role string, tables []string) ([]postgres.PublicationTable, error)
}

// pgPublicationTablesTool shows, for a pipeline, the Debezium publication and
// its tables, and whether the connector role can SELECT each source table.
func pgPublicationTablesTool(d Deps) tools.Tool {
	return &tool{
		name: "pg_publication_tables",
		desc: "For a pipeline, show the Debezium publication (its tables) and whether the connector role has SELECT on each pipeline source table. Use for connector-permission incidents.",
		schema: objSchema(map[string]any{
			"pipeline_id": strProp("Pipeline id from owners.yaml."),
		}, "pipeline_id"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				PipelineID string `json:"pipeline_id"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if input.PipelineID == "" {
				return tools.Result{}, fmt.Errorf("%w: pipeline_id is required", tools.ErrBadInput)
			}
			p, err := d.pipeline(input.PipelineID)
			if err != nil {
				return tools.Result{}, err
			}

			src, ok := d.Src.(publicationTablesClient)
			if !ok {
				return tools.Result{}, fmt.Errorf("dbtools: pg_publication_tables: source client does not support PublicationTables")
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			tables, err := src.PublicationTables(ctx, defaultPublication, defaultConnectorRole, p.SourceTables)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_publication_tables: %w", err)
			}

			summary, truncated := tools.Truncate(publicationSummary(defaultPublication, defaultConnectorRole, p.SourceTables, tables))
			return tools.Result{
				Summary:   summary,
				Data:      publicationData(defaultPublication, defaultConnectorRole, p.SourceTables, tables),
				Truncated: truncated,
			}, nil
		},
	}
}

// publicationSummary renders one line per pipeline source table with its
// publication membership and SELECT status, prefixed by the publication name
// and connector role.
func publicationSummary(pub, role string, sourceTables []string, tables []postgres.PublicationTable) string {
	byName := map[string]postgres.PublicationTable{}
	for _, t := range tables {
		byName[t.Schema+"."+t.Table] = t
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "publication %s (connector user %s)\n", pub, role)
	for _, st := range sourceTables {
		t, ok := byName[st]
		if !ok {
			t = splitPublicationName(st)
		}
		fmt.Fprintf(&sb, "%s: in_publication=%s, selectable=%s\n", st, yesNo(t.InPublication), yesNo(t.Selectable))
	}
	return sb.String()
}

// publicationData is the structured Data payload: the publication name, the
// connector user and where it came from, the publication's member tables, and
// the per-source-table membership + SELECT view.
func publicationData(pub, role string, sourceTables []string, tables []postgres.PublicationTable) map[string]any {
	byName := map[string]postgres.PublicationTable{}
	var members []postgres.PublicationTable
	for _, t := range tables {
		byName[t.Schema+"."+t.Table] = t
		if t.InPublication {
			members = append(members, t)
		}
	}
	var sources []postgres.PublicationTable
	for _, st := range sourceTables {
		t, ok := byName[st]
		if !ok {
			t = splitPublicationName(st)
		}
		sources = append(sources, t)
	}
	return map[string]any{
		"publication":           pub,
		"connector_user":        role,
		"connector_user_source": connectorUserSource,
		"publication_tables":    members,
		"source_tables":         sources,
	}
}

// splitPublicationName builds a PublicationTable from a "schema.table" string,
// used as a fallback when the client result is missing an entry (the client
// guarantees every source table appears, so this only guards a bad fake).
func splitPublicationName(s string) postgres.PublicationTable {
	schema, table, _ := strings.Cut(s, ".")
	return postgres.PublicationTable{Schema: schema, Table: table}
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}
