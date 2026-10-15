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

// publicationInfoClient is implemented by source clients that can read the
// publication's owner and the connector role's attributes
// (internal/clients/postgres). It is optional: without it the tool gives the
// table facts only and says what it could not read. (Q1, QA G3)
type publicationInfoClient interface {
	PublicationInfo(ctx context.Context, publication, role string) (postgres.PublicationInfo, error)
}

// roleFacts is the outcome of the optional PublicationInfo read: the info, or
// why there is none. Both empty means the client cannot read it.
type roleFacts struct {
	info *postgres.PublicationInfo
	err  string
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
	// autocreate is publication.autocreate.mode as configured ("" = unset).
	// "filtered" makes Debezium run ALTER PUBLICATION ... SET TABLE at every
	// task start, which is what turns table ownership into a requirement.
	autocreate string
}

// pgPublicationTablesTool shows, for a pipeline, the Debezium publication and
// its tables, and whether the connector role can SELECT each source table.
func pgPublicationTablesTool(d Deps) tools.Tool {
	return &tool{
		name: "pg_publication_tables",
		desc: "For a pipeline, show the Debezium publication (its owner and tables) and, for the connector role, its REPLICATION and SUPERUSER attributes and, for each pipeline source table, " +
			"SELECT, the table's owner and whether the role is that owner or a member of it. Says plainly when the role neither owns the table nor is a member of its owner. " +
			"Use for connector-permission incidents, e.g. 'must be owner of table' in the Connect worker log while the task shows RUNNING.",
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

			facts := readRoleFacts(ctx, d.Src, conn)
			findings := publicationFindings(conn, p.SourceTables, tables, facts)

			text := publicationSummary(conn, p.SourceTables, tables, facts, findings)
			if !sc.Unscoped() {
				if line := tools.NotShown(other, "table in publication "+conn.publication, "tables in publication "+conn.publication); line != "" {
					text += line + "\n"
				}
			}
			summary, truncated := tools.Truncate(text)
			data := publicationData(conn, p.SourceTables, tables)
			if conn.autocreate != "" {
				data["autocreate_mode"] = conn.autocreate
			}
			if facts.info != nil {
				data["publication_owner"] = facts.info.Owner
				data["publication_info"] = *facts.info
			}
			if facts.err != "" {
				data["publication_info_error"] = facts.err
			}
			if len(findings) > 0 {
				data["findings"] = findings
			}
			return tools.Result{Summary: summary, Data: data, Truncated: truncated}, nil
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
		autocreate:  strings.TrimSpace(cfg["publication.autocreate.mode"]),
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

// readRoleFacts reads the publication's owner and the connector role's
// attributes when the source client can. A failure is kept as text, not
// returned: the table facts already read are still worth showing, and the
// summary says what is missing.
func readRoleFacts(ctx context.Context, src clients.SourceDB, c connectorConfig) roleFacts {
	ic, ok := src.(publicationInfoClient)
	if !ok {
		return roleFacts{}
	}
	info, err := ic.PublicationInfo(ctx, c.publication, c.role)
	if err != nil {
		return roleFacts{err: cutShort(err.Error(), 300)}
	}
	return roleFacts{info: &info}
}

// sourceTableFacts returns the entry for each pipeline source table, in order;
// a table the client left out is an empty entry.
func sourceTableFacts(sourceTables []string, tables []postgres.PublicationTable) []postgres.PublicationTable {
	byName := map[string]postgres.PublicationTable{}
	for _, t := range tables {
		byName[t.Schema+"."+t.Table] = t
	}
	out := make([]postgres.PublicationTable, 0, len(sourceTables))
	for _, st := range sourceTables {
		t, ok := byName[st]
		if !ok {
			t = splitPublicationName(st)
		}
		out = append(out, t)
	}
	return out
}

// publicationFindings says plainly what is wrong with the connector role's
// rights, one sentence each, most specific first. A finding starts with
// PROBLEM when the connector's settings make the missing right fatal, and with
// NOTE when it only matters if the connector goes on to change the publication.
// It states only what the catalogs showed: an unread fact is never a finding.
func publicationFindings(c connectorConfig, sourceTables []string, tables []postgres.PublicationTable, f roleFacts) []string {
	var out []string
	filtered := c.autocreate == "filtered"
	for i, t := range sourceTableFacts(sourceTables, tables) {
		// Owner is empty when the table is missing or the client did not
		// say; membership is true for the owner itself and for a superuser.
		if t.Owner == "" || t.RoleMemberOfOwner {
			continue
		}
		name := sourceTables[i]
		msg := fmt.Sprintf("%s: connector user %s is not the owner of this table (owner: %s) and is not a member of that role. Postgres lets only the owner of a table, or a member of the owner role, add it to a publication.",
			name, c.role, t.Owner)
		if filtered {
			out = append(out, "PROBLEM "+msg+" publication.autocreate.mode=filtered makes Debezium run ALTER PUBLICATION ... SET TABLE on every task start, so the task fails with \"must be owner of table\" while Connect can still report it RUNNING; read the worker log with connect_worker_log.")
		} else {
			out = append(out, "NOTE "+msg+" That matters only when the connector changes the publication (publication.autocreate.mode=filtered); a ready-made publication needs SELECT only.")
		}
	}
	i := f.info
	if i == nil {
		return out
	}
	if !i.RoleExists {
		return append(out, fmt.Sprintf("PROBLEM connector user %s does not exist as a role (pg_roles has no such name), so the connector cannot log in.", c.role))
	}
	if !i.RoleReplication && !i.RoleSuperuser {
		out = append(out, fmt.Sprintf("PROBLEM connector user %s has no REPLICATION attribute and is not a superuser, so it cannot start logical replication (on managed Postgres a role such as rds_replication can stand in).", c.role))
	}
	if filtered && i.Owner != "" && !i.RoleMemberOfOwner {
		out = append(out, fmt.Sprintf("PROBLEM publication %s is owned by %s; connector user %s is not that role and not a member of it, and filtered mode runs ALTER PUBLICATION, which only the owner may.",
			c.publication, i.Owner, c.role))
	}
	return out
}

// publicationSummary renders the publication and connector role, then one line
// per pipeline source table with its publication membership, SELECT status and
// owner, then the findings.
func publicationSummary(c connectorConfig, sourceTables []string, tables []postgres.PublicationTable, f roleFacts, findings []string) string {
	var sb strings.Builder
	if i := f.info; i != nil {
		fmt.Fprintf(&sb, "publication %s (owner %s; connector user %s: REPLICATION=%s, SUPERUSER=%s)\n",
			c.publication, i.Owner, c.role, yesNo(i.RoleReplication), yesNo(i.RoleSuperuser))
	} else {
		fmt.Fprintf(&sb, "publication %s (connector user %s)\n", c.publication, c.role)
	}
	for i, t := range sourceTableFacts(sourceTables, tables) {
		fmt.Fprintf(&sb, "%s: in_publication=%s, selectable=%s", sourceTables[i], yesNo(t.InPublication), yesNo(t.Selectable))
		if t.Owner != "" {
			fmt.Fprintf(&sb, ", owner=%s, role_member_of_owner=%s", t.Owner, yesNo(t.RoleMemberOfOwner))
		}
		sb.WriteString("\n")
	}
	for _, line := range findings {
		sb.WriteString(line + "\n")
	}
	if f.err != "" {
		fmt.Fprintf(&sb, "Publication owner and role attributes could not be read: %s\n", f.err)
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
