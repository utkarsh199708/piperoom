package dbtools

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// defaultPublication is the publication name Debezium creates when the source
// connector config sets no publication.name. It is only a fallback and is
// reported as publication_source:"default" in Data.
const defaultPublication = "dbz_publication"

// publicationTablesClient is the local adapter for PublicationTables until the
// method lands on the contract (contract change request in the PR): the real
// client (internal/clients/postgres) and the test fake implement it, and the
// tool reports a clear error when the injected client does not. This is the
// same pattern kafkatools uses for topicConfigAdmin.
type publicationTablesClient interface {
	PublicationTables(ctx context.Context, publication, role string, tables []string) ([]postgres.PublicationTable, error)
}

// publicationSourceTablesClient is implemented by source clients that read
// only the given tables of a publication in SQL and count its other members
// (internal/clients/postgres).
type publicationSourceTablesClient interface {
	PublicationSourceTables(ctx context.Context, publication, role string, tables []string) ([]postgres.PublicationTable, int, error)
}

// publicationRead returns the entries for tables and the number of the
// publication's other member tables.
type publicationRead func(ctx context.Context, publication, role string, tables []string) ([]postgres.PublicationTable, int, error)

// publicationReader picks the source client's publication read: the SQL
// filtered one when it has it, else the full member list filtered here. Either
// way the other members' names are dropped before any result is built.
func (d Deps) publicationReader() (publicationRead, error) {
	if c, ok := d.Src.(publicationSourceTablesClient); ok {
		return c.PublicationSourceTables, nil
	}
	c, ok := d.Src.(publicationTablesClient)
	if !ok {
		return nil, fmt.Errorf("dbtools: pg_publication_tables: source client does not support PublicationTables")
	}
	return func(ctx context.Context, publication, role string, tables []string) ([]postgres.PublicationTable, int, error) {
		all, err := c.PublicationTables(ctx, publication, role, tables)
		if err != nil {
			return nil, 0, err
		}
		var out []postgres.PublicationTable
		other := 0
		for _, t := range all {
			switch {
			case slices.Contains(tables, t.Schema+"."+t.Table):
				out = append(out, t)
			case t.InPublication:
				other++
			}
		}
		return out, other, nil
	}, nil
}

// connectorConfig is the Debezium source connector configuration resolved from
// Connect for a pipeline: the connector name, its database.user role, and the
// publication name it publishes to together with where that name came from.
type connectorConfig struct {
	name              string
	role              string
	publication       string
	publicationSource string
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
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			p, err := d.resolve(sc, input.PipelineID)
			if err != nil {
				return tools.Result{}, err
			}

			read, err := d.publicationReader()
			if err != nil {
				return tools.Result{}, err
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			conn, err := resolveConnector(ctx, p, d.Connect)
			if err != nil {
				return tools.Result{}, err
			}

			tables, other, err := read(ctx, conn.publication, conn.role, p.SourceTables)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_publication_tables: %w", err)
			}

			text := publicationSummary(conn.publication, conn.role, p.SourceTables, tables)
			if !sc.Unscoped() {
				if line := tools.NotShown(other, "tables in publication "+conn.publication); line != "" {
					text += line + "\n"
				}
			}
			summary, truncated := tools.Truncate(text)
			return tools.Result{
				Summary:   summary,
				Data:      publicationData(conn, p.SourceTables, tables),
				Truncated: truncated,
			}, nil
		},
	}
}

// resolveConnector reads the pipeline's Debezium Postgres source connector
// config via Connect and returns the connector role (database.user) and the
// publication name. The role has no default: a nil Connect client, a connector
// that cannot be found or read, or a config without database.user is an error
// naming the pipeline (and the connector where known). The publication falls
// back to defaultPublication, marked publicationSource "default", when the
// config has no publication.name.
func resolveConnector(ctx context.Context, p owners.Pipeline, connect clients.ConnectClient) (connectorConfig, error) {
	if connect == nil {
		return connectorConfig{}, fmt.Errorf("dbtools: pg_publication_tables: pipeline %q: Connect client is nil, cannot read the Debezium connector config", p.ID)
	}
	name, cfg, err := debeziumSourceConnector(ctx, connect, p.Connectors)
	if err != nil {
		return connectorConfig{}, fmt.Errorf("dbtools: pg_publication_tables: pipeline %q: %w", p.ID, err)
	}
	role := cfg["database.user"]
	if role == "" {
		return connectorConfig{}, fmt.Errorf("dbtools: pg_publication_tables: pipeline %q: connector %q config has no database.user", p.ID, name)
	}
	c := connectorConfig{
		name:        name,
		role:        role,
		publication: cfg["publication.name"],
	}
	if c.publication == "" {
		c.publication = defaultPublication
		c.publicationSource = "default"
	} else {
		c.publicationSource = "connector " + name + " config publication.name"
	}
	return c, nil
}

// debeziumSourceConnector returns the name and config of the pipeline's
// Debezium Postgres source connector. Selection rule: a connector is the source
// connector when its config's connector.class contains "PostgresConnector", or
// — for connectors that omit connector.class — when its config has
// database.server.name or database.user. The first matching connector wins; a
// pipeline with no matching connector is an error.
func debeziumSourceConnector(ctx context.Context, connect clients.ConnectClient, names []string) (string, map[string]string, error) {
	for _, name := range names {
		cfg, err := connect.Config(ctx, name)
		if err != nil {
			return "", nil, fmt.Errorf("read connector %q config: %w", name, err)
		}
		if strings.Contains(cfg["connector.class"], "PostgresConnector") {
			return name, cfg, nil
		}
		if _, ok := cfg["database.server.name"]; ok {
			return name, cfg, nil
		}
		if _, ok := cfg["database.user"]; ok {
			return name, cfg, nil
		}
	}
	return "", nil, fmt.Errorf("no Debezium Postgres source connector found among %v", names)
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

// publicationData is the structured Data payload: the publication name and its
// source, the connector name, the connector user and where it came from, the
// publication's member tables that are pipeline source tables, and the
// per-source-table membership + SELECT view.
func publicationData(c connectorConfig, sourceTables []string, tables []postgres.PublicationTable) map[string]any {
	byName := map[string]postgres.PublicationTable{}
	var members []postgres.PublicationTable
	for _, t := range tables {
		name := t.Schema + "." + t.Table
		byName[name] = t
		// Only the pipeline's own tables: a FOR ALL TABLES publication has
		// every table of the database as a member.
		if t.InPublication && slices.Contains(sourceTables, name) {
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
		"publication":           c.publication,
		"publication_source":    c.publicationSource,
		"connector":             c.name,
		"connector_user":        c.role,
		"connector_user_source": "connector " + c.name + " config database.user",
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
